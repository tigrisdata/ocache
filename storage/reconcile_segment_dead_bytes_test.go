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

// The metadata reconciliation must re-derive each finalized segment's dead bytes
// from the rows that still reference it, so credit the incremental path missed
// (overlapping same-key Puts) or never recorded (bytes orphaned before that path
// existed) still reaches the recompactor — without crediting segments whose
// entries are all live.
func TestReconcile_DerivesSegmentDeadBytesFromLiveness(t *testing.T) {
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

	s.cleaner.reconcileFromMetadata()

	entries, deadBytes, err = s.GetDeleteIndexStats(halfDead)
	require.NoError(t, err)
	require.Equal(t, int64(2), entries, "reconcile did not credit the unreferenced entries")
	require.Equal(t, int64(2*entrySize), deadBytes)

	entries, deadBytes, err = s.GetDeleteIndexStats(allLive)
	require.NoError(t, err)
	require.Zero(t, entries, "reconcile credited a fully live segment")
	require.Zero(t, deadBytes)

	// Idempotent: a second pass must not double-count what it already credited.
	s.cleaner.reconcileFromMetadata()
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
