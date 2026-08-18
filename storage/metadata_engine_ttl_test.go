// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// The metadata DB must not apply an engine-level TTL: RocksDB's TTL compaction
// filter drops EVERY key not rewritten within the window, including internal
// bookkeeping (per-segment delete-index records, FIFO eviction index, bucketed
// access keys) that must outlive entry TTLs. Entry expiry is owned by the
// cleaner. This test pins the regression: delete-index records written to a DB
// opened the way NewStorageWithConfig opens it survive an entry-TTL-sized wait
// plus a full compaction. With the old behavior (engine TTL = entry TTL) the
// record is silently dropped and the recompactor loses its only fragmentation
// signal, orphaning dead segment bytes forever.
func TestDeleteIndexSurvivesEntryTTL(t *testing.T) {
	dir := t.TempDir()

	// Mirror the NewStorageWithConfig call site: engine TTL disabled (0),
	// multiplex merge operator installed. The entry TTL the cache would use is
	// deliberately tiny so the wait below exceeds it.
	const entryTTLSeconds = 1
	db, err := metadata.NewMetaDB(dir, 0, merge.NewMultiplexOperator(), nil)
	require.NoError(t, err)
	defer db.Close()

	segPath := "/data/segments/segment_123.seg"
	diKey := keys.MakeDeleteIndexKey(segPath)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, db.Handle().Merge(wo, diKey, merge.MakeDeleteIndexOperand(1, 100)))
	require.NoError(t, db.Handle().Merge(wo, diKey, merge.MakeDeleteIndexOperand(1, 150)))

	// Outlive the entry TTL, then force a full compaction so any engine-level
	// TTL filter would run. The record must survive both.
	time.Sleep((entryTTLSeconds + 1) * time.Second)
	db.Handle().CompactRangeCF(db.Handle().GetDefaultColumnFamily(), grocksdb.Range{})

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	slice, err := db.Handle().Get(ro, diKey)
	require.NoError(t, err)
	defer slice.Free()
	require.True(t, slice.Exists(), "delete-index record was dropped by an engine-level TTL")

	var entry pb.DeleteIndexEntry
	require.NoError(t, proto.Unmarshal(slice.Data(), &entry))
	require.EqualValues(t, 2, entry.DeletedEntries)
	require.EqualValues(t, 250, entry.DeletedBytes)
}
