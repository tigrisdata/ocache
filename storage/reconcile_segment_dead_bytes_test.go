// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"fmt"
	"testing"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// The startup reconciliation must re-derive each finalized segment's dead bytes
// from the rows that still reference it, so credit the incremental path missed
// (overlapping same-key Puts) or never recorded (bytes orphaned before that path
// existed) still reaches the recompactor — without crediting segments whose
// entries are all live. Calling the method directly reproduces its runtime
// conditions exactly: it executes single-threaded before any background worker
// starts.
func TestStartupReconcile_DerivesSegmentDeadBytesFromLiveness(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	const entrySize = 4096
	payload := bytes.Repeat([]byte("x"), entrySize)

	// halfDead: 4 entries written, 2 still referenced by metadata.
	// allLive:  2 entries written, both still referenced.
	halfDead := writeFinalizedSegment(t, s, "halfdead", 4)
	allLive := writeFinalizedSegment(t, s, "live", 2)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	plant := func(key, segPath string) {
		row, err := proto.Marshal(&pb.ValueMessage{
			ValueType:   pb.ValueType_SEGMENT,
			SegmentPath: segPath,
			ValueLength: int64(len(payload)),
		})
		require.NoError(t, err)
		require.NoError(t, s.meta.Handle().Put(wo, keys.MakeMetadataKey(key), row))
	}
	plant("halfdead-0", halfDead)
	plant("halfdead-1", halfDead)
	plant("live-0", allLive)
	plant("live-1", allLive)

	// No delete-index rows exist: the two missing halfDead entries are exactly the
	// invisible dead bytes this reconciliation has to discover.
	entries, deadBytes, err := s.GetDeleteIndexStats(halfDead)
	require.NoError(t, err)
	require.Zero(t, entries)
	require.Zero(t, deadBytes)

	s.reconcileSegmentDeleteIndexAtStartup()

	entries, deadBytes, err = s.GetDeleteIndexStats(halfDead)
	require.NoError(t, err)
	require.Equal(t, int64(2), entries, "reconcile did not credit the unreferenced entries")
	require.Equal(t, int64(2*entrySize), deadBytes)

	entries, deadBytes, err = s.GetDeleteIndexStats(allLive)
	require.NoError(t, err)
	require.Zero(t, entries, "reconcile credited a fully live segment")
	require.Zero(t, deadBytes)

	// Idempotent: a second pass must not double-count what it already credited.
	s.reconcileSegmentDeleteIndexAtStartup()
	entries, deadBytes, err = s.GetDeleteIndexStats(halfDead)
	require.NoError(t, err)
	require.Equal(t, int64(2), entries, "reconcile is not idempotent")
	require.Equal(t, int64(2*entrySize), deadBytes)
}

// writeFinalizedSegment writes n entries of 4 KiB through the storage's segment
// manager and finalizes the segment, returning its path.
func writeFinalizedSegment(t *testing.T, s *Storage, prefix string, n int) string {
	t.Helper()
	payload := bytes.Repeat([]byte("x"), 4096)

	seg, err := s.segmentManager.AcquireOpenSegmentWithReservation(prefix, 0)
	require.NoError(t, err)
	for i := range n {
		vm := &pb.ValueMessage{ValueType: pb.ValueType_SEGMENT, ValueLength: int64(len(payload))}
		_, err := seg.WriteEntry(fmt.Sprintf("%s-%d", prefix, i), bytes.NewReader(payload), vm)
		require.NoError(t, err)
	}
	path := seg.Path()
	require.NoError(t, s.segmentManager.FinalizeSegment(seg))
	require.NoError(t, seg.Release(prefix))
	return path
}

// The startup pass runs single-threaded, so it can rewrite index rows
// absolutely: over-credit left by racing same-key Puts (each staging the same
// replaced value) must be withdrawn, not merely never worsened.
func TestStartupReconcile_WithdrawsOverCredit(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	const entrySize = 4096
	seg := writeFinalizedSegment(t, s, "seg", 4)

	// All four entries live.
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for i := range 4 {
		row, err := proto.Marshal(&pb.ValueMessage{
			ValueType:   pb.ValueType_SEGMENT,
			SegmentPath: seg,
			ValueLength: entrySize,
		})
		require.NoError(t, err)
		require.NoError(t, s.meta.Handle().Put(wo, keys.MakeMetadataKey(fmt.Sprintf("seg-%d", i)), row))
	}

	// Plant a bogus over-credit: 3 entries / 3*entrySize dead when nothing is.
	s.updateDeleteIndex(seg, entrySize)
	s.updateDeleteIndex(seg, entrySize)
	s.updateDeleteIndex(seg, entrySize)

	s.reconcileSegmentDeleteIndexAtStartup()

	entries, deadBytes, err := s.GetDeleteIndexStats(seg)
	require.NoError(t, err)
	require.Zero(t, entries, "over-credit was not withdrawn")
	require.Zero(t, deadBytes)
}

// Index rows for segments that no longer exist on disk are removed at startup:
// segment paths embed creation timestamps and are never reused, so such rows
// can only mislead and would otherwise persist forever.
func TestStartupReconcile_RemovesOrphanIndexRows(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	ghost := s.diskPath + "/segments/segment_424242.seg"
	s.updateDeleteIndex(ghost, 12345)
	entries, _, err := s.GetDeleteIndexStats(ghost)
	require.NoError(t, err)
	require.Equal(t, int64(1), entries)

	s.reconcileSegmentDeleteIndexAtStartup()

	entries, deadBytes, err := s.GetDeleteIndexStats(ghost)
	require.NoError(t, err)
	require.Zero(t, entries, "orphan delete-index row survived startup reconcile")
	require.Zero(t, deadBytes)
}
