// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"google.golang.org/protobuf/proto"
)

// writeWalkTestSegment writes entries with metadata rows pointing at the
// segment, then finalizes it. Returns the segment and each key's offset.
func writeWalkTestSegment(t *testing.T, sm *segment.Manager, meta *metadata.MetaDB, entries map[string][]byte) (*segment.Segment, map[string]int64) {
	t.Helper()
	seg, err := sm.AcquireOpenSegmentWithReservation("walk-test", 0)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	offsets := make(map[string]int64, len(entries))
	for key, value := range entries {
		vm := &pb.ValueMessage{
			ValueType:   pb.ValueType_SEGMENT,
			ValueLength: int64(len(value)),
		}
		offset, err := seg.WriteEntry(key, bytes.NewReader(value), vm)
		require.NoError(t, err)
		offsets[key] = offset

		vm.SegmentPath = seg.Path()
		vm.SegmentOffset = offset
		row, err := proto.Marshal(vm)
		require.NoError(t, err)
		require.NoError(t, meta.Handle().Put(wo, keys.MakeMetadataKey(key), row))
	}
	require.NoError(t, sm.FinalizeSegment(seg))
	require.NoError(t, seg.Release("walk-test"))
	return seg, offsets
}

// The headline property of walk-gated recompaction: dead bytes with NO
// delete-index credit at all (the invisible-orphan case every historical
// credit bug produced) are derived from ground truth and reclaimed.
func TestWalkGatedRecompaction_ReclaimsWithoutAnyCredit(t *testing.T) {
	sr, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	entries := map[string][]byte{
		"live-1": bytes.Repeat([]byte("a"), 4096),
		"dead-1": bytes.Repeat([]byte("b"), 4096),
		"dead-2": bytes.Repeat([]byte("c"), 4096),
	}
	seg, _ := writeWalkTestSegment(t, sm, meta, entries)

	// Kill two entries by deleting their rows — deliberately writing NO
	// delete-index record, exactly what the historical credit bugs produced.
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, meta.Handle().Delete(wo, keys.MakeMetadataKey("dead-1")))
	require.NoError(t, meta.Handle().Delete(wo, keys.MakeMetadataKey("dead-2")))

	sr.minSegments = 1
	sr.fragThreshold = 0.5
	require.NoError(t, sr.RecompactFragmentedSegments(context.Background()))

	segments := sm.GetSegments()
	require.Len(t, segments, 1)
	assert.NotEqual(t, seg.Path(), segments[0].Path(), "orphaned dead bytes were not reclaimed")

	// The live entry survived and points at the new segment.
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	slice, err := meta.Handle().Get(ro, keys.MakeMetadataKey("live-1"))
	require.NoError(t, err)
	defer slice.Free()
	require.True(t, slice.Exists())
	var vm pb.ValueMessage
	require.NoError(t, proto.Unmarshal(slice.Data(), &vm))
	assert.Equal(t, segments[0].Path(), vm.SegmentPath)
}

// The inverse property: an inflated delete-index hint must NOT trigger
// recompaction of a fully live segment — the hint orders walks, the walk
// decides.
func TestWalkGatedRecompaction_IgnoresInflatedHint(t *testing.T) {
	sr, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	entries := map[string][]byte{
		"live-1": bytes.Repeat([]byte("a"), 4096),
		"live-2": bytes.Repeat([]byte("b"), 4096),
	}
	seg, _ := writeWalkTestSegment(t, sm, meta, entries)

	// Plant a wildly inflated credit: index claims everything is dead.
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	over := &pb.DeleteIndexEntry{DeletedEntries: 99, DeletedBytes: 1 << 30}
	overBytes, err := proto.Marshal(over)
	require.NoError(t, err)
	require.NoError(t, meta.Handle().Put(wo, keys.MakeDeleteIndexKey(seg.Path()), overBytes))

	sr.minSegments = 1
	sr.fragThreshold = 0.5
	require.NoError(t, sr.RecompactFragmentedSegments(context.Background()))

	segments := sm.GetSegments()
	require.Len(t, segments, 1)
	assert.Equal(t, seg.Path(), segments[0].Path(), "fully live segment was rewritten on a bogus hint")
}

// A truncated segment must abort the walk without recompacting: the iterator
// reads truncation as clean EOF, so the footer cross-check is what
// distinguishes damage from end-of-entries.
func TestWalk_AbortsOnTruncatedSegment(t *testing.T) {
	sr, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	entries := map[string][]byte{
		"k1": bytes.Repeat([]byte("a"), 4096),
		"k2": bytes.Repeat([]byte("b"), 4096),
	}
	seg, offsets := writeWalkTestSegment(t, sm, meta, entries)

	var lastOffset int64
	for _, off := range offsets {
		if off > lastOffset {
			lastOffset = off
		}
	}
	require.NoError(t, os.Truncate(seg.Path(), lastOffset+4))

	_, _, err := sr.walkSegmentLiveness(context.Background(), seg)
	require.Error(t, err, "walk of a truncated segment must abort")

	// And through the full loop: the damaged segment is left untouched.
	sr.minSegments = 1
	sr.fragThreshold = 0.1
	require.NoError(t, sr.RecompactFragmentedSegments(context.Background()))
	segments := sm.GetSegments()
	require.Len(t, segments, 1)
	assert.Equal(t, seg.Path(), segments[0].Path())
}

// The walk interval bounds re-walks: a segment derived clean is not walked
// again until the interval elapses, even when a stale hint keeps ranking it.
func TestWalk_IntervalBoundsRewalks(t *testing.T) {
	sr, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	for i := range 2 {
		_, _ = writeWalkTestSegment(t, sm, meta, map[string][]byte{
			fmt.Sprintf("k-%d", i): bytes.Repeat([]byte("x"), 4096),
		})
	}
	sr.minSegments = 1

	require.NoError(t, sr.RecompactFragmentedSegments(context.Background()))
	walkedOnce := len(sr.walkStates)
	require.Equal(t, 2, walkedOnce)

	first := make(map[string]any)
	for p, ts := range sr.walkStates {
		first[p] = ts
	}
	require.NoError(t, sr.RecompactFragmentedSegments(context.Background()))
	for p, ts := range sr.walkStates {
		assert.Equal(t, first[p], ts, "segment %s re-walked within the interval", p)
	}
}
