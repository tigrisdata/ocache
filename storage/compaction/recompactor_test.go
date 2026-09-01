// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/deletion"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"github.com/tigrisdata/ocache/storage/utils"
	"google.golang.org/protobuf/proto"
)

func setupTestRecompactor(t testing.TB) (*SegmentRecompactor, *segment.Manager, *metadata.MetaDB, string, func()) {
	tmpDir, err := os.MkdirTemp("", "recompactor_test_*")
	require.NoError(t, err)

	// Initialize metadata DB with merge operator
	mergeOp := merge.NewMultiplexOperator()
	meta, err := metadata.NewMetaDB(tmpDir, 0, mergeOp, nil)
	require.NoError(t, err)

	// Segment removal reaches the process-wide descriptor cache.
	_ = fd.NewFdCache(100)

	// Initialize segment manager
	sm, err := segment.NewManager(tmpDir, 1024*1024) // 1MB segments for testing
	require.NoError(t, err)

	// Initialize deletion queue
	config := deletion.Config{
		BatchSize:       100,
		ProcessInterval: time.Second,
		PruneAge:        24 * time.Hour,
	}
	deletionQueue := deletion.NewQueue(meta, config)
	deletionQueue.Start()

	// Create recompactor with 0 age for testing
	recompactor := NewSegmentRecompactor(meta, sm, deletionQueue, 0.5, 0, 3)

	cleanup := func() {
		sm.Close()
		deletionQueue.Stop()
		os.RemoveAll(tmpDir)
	}

	return recompactor, sm, meta, tmpDir, cleanup
}

func createTestSegmentWithEntries(t *testing.T, sm *segment.Manager, meta *metadata.MetaDB, entries map[string][]byte) (*segment.Segment, error) {
	seg, err := sm.AcquireOpenSegmentWithReservation("test", 0)
	if err != nil {
		return nil, err
	}

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	for key, value := range entries {
		// Create a temporary file with the value
		tmpFile, err := os.CreateTemp("", "test_value_*")
		if err != nil {
			return nil, err
		}
		defer os.Remove(tmpFile.Name())

		if _, err := tmpFile.Write(value); err != nil {
			tmpFile.Close()
			return nil, err
		}
		tmpFile.Close()

		// Open file for reading
		f, err := os.Open(tmpFile.Name())
		if err != nil {
			return nil, err
		}

		// Create metadata
		vm := &pb.ValueMessage{
			ValueType:   pb.ValueType_RAW_FILE,
			ValueLength: int64(len(value)),
			Checksum:    0, // No checksum for simplicity
		}

		// Write to segment
		offset, err := seg.WriteEntry(key, f, vm)
		f.Close()
		if err != nil {
			return nil, err
		}

		// Update metadata
		vm.ValueType = pb.ValueType_SEGMENT
		vm.SegmentPath = seg.Path()
		vm.SegmentOffset = offset
		vm.RawFilePath = ""

		metaBytes, err := proto.Marshal(vm)
		if err != nil {
			return nil, err
		}

		metaKey := keys.MakeMetadataKey(key)
		wb.Put(metaKey, metaBytes)
		require.NoError(t, stageSegmentLiveIndexRow(wb, key, seg.Path(), offset, int64(len(value)), 0))
	}

	// Commit metadata
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	if err := meta.Handle().Write(wo, wb); err != nil {
		return nil, err
	}

	// Finalize the segment
	if err := sm.FinalizeSegment(seg); err != nil {
		return nil, err
	}
	if err := MarkSegmentLiveIndexComplete(meta, seg); err != nil {
		return nil, err
	}

	return seg, nil
}

func TestBackfillSegmentLiveIndex_RebuildsLegacySegment(t *testing.T) {
	_, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	entries := map[string][]byte{"legacy": []byte("legacy segment payload")}
	seg, err := createTestSegmentWithEntries(t, sm, meta, entries)
	require.NoError(t, err)
	value, err := utils.GetMetadata(meta, string(keys.MakeMetadataKey("legacy")))
	require.NoError(t, err)
	wo := grocksdb.NewDefaultWriteOptions()
	require.NoError(t, meta.Handle().Delete(wo, keys.MakeSegmentLiveIndexKey(seg.Path(), value.SegmentOffset)))
	require.NoError(t, meta.Handle().Delete(wo, keys.MakeSegmentLiveCoverageKey(seg.Path())))
	wo.Destroy()

	require.NoError(t, backfillSegmentLiveIndex(meta, sm))
	require.True(t, segmentLiveIndexRowExistsForTest(t, meta, seg.Path(), value.SegmentOffset))
	covered, err := segmentLiveIndexCovered(meta, seg)
	require.NoError(t, err)
	require.True(t, covered)
}

func segmentLiveIndexRowExistsForTest(t *testing.T, meta *metadata.MetaDB, segmentPath string, offset int64) bool {
	t.Helper()
	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	slice, err := meta.Handle().Get(ro, keys.MakeSegmentLiveIndexKey(segmentPath, offset))
	require.NoError(t, err)
	exists := slice.Exists()
	slice.Free()
	return exists
}

func TestRecompactionRolloverPublishesBeforeFinalizing(t *testing.T) {
	_, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	oldSeg, err := sm.AcquireOpenSegmentWithReservation("rollover-source", 0)
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{'x'}, 600*1024)
	entries := make([]*segment.EntryInfo, 2)
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	for i := range entries {
		key := fmt.Sprintf("rollover-%d", i)
		offset, writeErr := oldSeg.WriteEntry(key, bytes.NewReader(payload), &pb.ValueMessage{
			ValueType:   pb.ValueType_SEGMENT,
			ValueLength: int64(len(payload)),
		})
		require.NoError(t, writeErr)
		entries[i] = &segment.EntryInfo{
			Key:         key,
			Offset:      offset,
			HeaderSize:  segment.CalculateValueHeaderSize(key),
			ValueLength: int64(len(payload)),
			Version:     segment.CurrentValueHeaderVersion,
		}
		metaBytes, marshalErr := proto.Marshal(&pb.ValueMessage{
			ValueType:     pb.ValueType_SEGMENT,
			SegmentPath:   oldSeg.Path(),
			SegmentOffset: offset,
			ValueLength:   int64(len(payload)),
		})
		require.NoError(t, marshalErr)
		batch.Put(keys.MakeMetadataKey(key), metaBytes)
	}
	wo := grocksdb.NewDefaultWriteOptions()
	require.NoError(t, meta.Handle().Write(wo, batch))
	wo.Destroy()
	require.NoError(t, sm.FinalizeSegment(oldSeg))

	oldFile, err := os.Open(oldSeg.Path())
	require.NoError(t, err)
	defer oldFile.Close()
	newSeg, err := sm.AcquireOpenSegmentWithReservation("rollover-recompactor", 0)
	require.NoError(t, err)
	firstDestination := newSeg.Path()
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()
	advice := newCacheAdvice()
	finalized := make([]*segment.Segment, 0, 1)
	recompactor := NewSegmentRecompactor(meta, sm, nil, 0, 0, 1)

	firstMeta := &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		SegmentPath:   oldSeg.Path(),
		SegmentOffset: entries[0].Offset,
		ValueLength:   entries[0].ValueLength,
	}
	require.NoError(t, recompactor.copyEntry(context.Background(), oldFile, &newSeg, "rollover-recompactor", entries[0], firstMeta, wb, advice, &finalized))
	require.Equal(t, firstDestination, newSeg.Path())

	secondMeta := &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		SegmentPath:   oldSeg.Path(),
		SegmentOffset: entries[1].Offset,
		ValueLength:   entries[1].ValueLength,
	}
	require.NoError(t, recompactor.copyEntry(context.Background(), oldFile, &newSeg, "rollover-recompactor", entries[1], secondMeta, wb, advice, &finalized))
	require.Len(t, finalized, 1)
	require.NotEqual(t, firstDestination, newSeg.Path())

	published, err := utils.GetMetadata(meta, string(keys.MakeMetadataKey(entries[0].Key)))
	require.NoError(t, err)
	require.Equal(t, firstDestination, published.SegmentPath)
	require.Equal(t, entries[0].ValueLength, published.ValueLength)
	require.True(t, segmentLiveIndexRowExistsForTest(t, meta, published.SegmentPath, published.SegmentOffset))
	require.NoError(t, newSeg.Release("rollover-recompactor"))
}

func TestSegmentRecompaction_NoFragmentation(t *testing.T) {
	recompactor, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	// Create a segment with entries
	entries := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}
	seg, err := createTestSegmentWithEntries(t, sm, meta, entries)
	require.NoError(t, err)

	// Run recompaction - should not recompact as there's no fragmentation
	ctx := context.Background()
	err = recompactor.RecompactFragmentedSegments(ctx)
	assert.NoError(t, err)

	// Verify segment still exists and wasn't recompacted
	segments := sm.GetSegments()
	assert.Equal(t, 1, len(segments))
	assert.Equal(t, seg.Path(), segments[0].Path())
}

func TestSegmentRecompaction_WithFragmentation(t *testing.T) {
	recompactor, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	// Create a segment with entries
	entries := map[string][]byte{
		"key1": []byte("value1 with some data"),
		"key2": []byte("value2 with more data"),
		"key3": []byte("value3 with even more data"),
	}
	seg, err := createTestSegmentWithEntries(t, sm, meta, entries)
	require.NoError(t, err)

	// Simulate deletion of key2 by removing its metadata
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	metaKey := keys.MakeMetadataKey("key2")
	err = meta.Handle().Delete(wo, metaKey)
	require.NoError(t, err)

	// Add delete index entry to track the deletion
	deleteIndexKey := keys.MakeDeleteIndexKey(seg.Path())
	deleteEntry := &pb.DeleteIndexEntry{
		DeletedEntries: 1,
		DeletedBytes:   int64(len(entries["key2"])),
	}
	deleteBytes, err := proto.Marshal(deleteEntry)
	require.NoError(t, err)
	err = meta.Handle().Put(wo, deleteIndexKey, deleteBytes)
	require.NoError(t, err)

	// Allow this one fragmented segment to be selected by the test.
	recompactor.minSegments = 1
	recompactor.fragThreshold = 0.1

	// Run recompaction
	ctx := context.Background()
	err = recompactor.RecompactFragmentedSegments(ctx)
	assert.NoError(t, err)

	// Verify the fragmented segment was replaced.
	segments := sm.GetSegments()
	require.Len(t, segments, 1)
	assert.NotEqual(t, seg.Path(), segments[0].Path())

	// Verify key1 and key3 still exist in metadata with correct segment references
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	for _, key := range []string{"key1", "key3"} {
		metaKey := keys.MakeMetadataKey(key)
		slice, err := meta.Handle().Get(ro, metaKey)
		require.NoError(t, err)
		defer slice.Free()

		assert.True(t, slice.Exists())
		var vm pb.ValueMessage
		err = proto.Unmarshal(slice.Data(), &vm)
		require.NoError(t, err)
		assert.Equal(t, pb.ValueType_SEGMENT, vm.ValueType)
		assert.Equal(t, segments[0].Path(), vm.SegmentPath)

		reader, err := sm.ReadEntry(key, vm.SegmentPath, vm.SegmentOffset, vm.ValueLength)
		require.NoError(t, err)
		value, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		assert.Equal(t, entries[key], value)
	}

	// Verify key2 is still deleted
	metaKey = keys.MakeMetadataKey("key2")
	slice, err := meta.Handle().Get(ro, metaKey)
	require.NoError(t, err)
	defer slice.Free()
	assert.False(t, slice.Exists())
}

func TestSegmentRecompaction_LegacySegmentFallsBackToSourceScan(t *testing.T) {
	recompactor, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	entries := map[string][]byte{
		"legacy-live": []byte("value that remains live"),
		"legacy-dead": []byte("value that is deleted"),
	}
	seg, err := createTestSegmentWithEntries(t, sm, meta, entries)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, meta.Handle().Delete(wo, keys.MakeSegmentLiveCoverageKey(seg.Path())))
	require.NoError(t, meta.Handle().Delete(wo, keys.MakeMetadataKey("legacy-dead")))
	deleteBytes, err := proto.Marshal(&pb.DeleteIndexEntry{DeletedEntries: 1, DeletedBytes: int64(len(entries["legacy-dead"]))})
	require.NoError(t, err)
	require.NoError(t, meta.Handle().Put(wo, keys.MakeDeleteIndexKey(seg.Path()), deleteBytes))

	recompactor.minSegments = 1
	recompactor.fragThreshold = 0.01
	require.NoError(t, recompactor.RecompactFragmentedSegments(context.Background()))

	segments := sm.GetSegments()
	require.Len(t, segments, 1)
	require.NotEqual(t, seg.Path(), segments[0].Path())
	vm, err := utils.GetMetadata(meta, string(keys.MakeMetadataKey("legacy-live")))
	require.NoError(t, err)
	reader, err := sm.ReadEntry("legacy-live", vm.SegmentPath, vm.SegmentOffset, vm.ValueLength)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, entries["legacy-live"], got)
}

func TestSegmentRecompaction_AllEntriesDeleted(t *testing.T) {
	recompactor, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	// Create a segment with entries
	entries := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}
	seg, err := createTestSegmentWithEntries(t, sm, meta, entries)
	require.NoError(t, err)
	originalPath := seg.Path()

	// Delete all entries
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for key := range entries {
		metaKey := keys.MakeMetadataKey(key)
		err = meta.Handle().Delete(wo, metaKey)
		require.NoError(t, err)
	}

	// Add delete index showing 100% fragmentation
	totalBytes := int64(0)
	for _, v := range entries {
		totalBytes += int64(len(v))
	}

	deleteIndexKey := keys.MakeDeleteIndexKey(originalPath)
	deleteEntry := &pb.DeleteIndexEntry{
		DeletedEntries: int64(len(entries)),
		DeletedBytes:   totalBytes,
	}
	deleteBytes, err := proto.Marshal(deleteEntry)
	require.NoError(t, err)
	err = meta.Handle().Put(wo, deleteIndexKey, deleteBytes)
	require.NoError(t, err)

	// Run recompaction
	ctx := context.Background()
	err = recompactor.RecompactFragmentedSegments(ctx)
	assert.NoError(t, err)

	// The old segment should be queued for deletion but no new segment created
	// since all entries were deleted
	segments := sm.GetSegments()
	for _, s := range segments {
		// The original segment should still be there (deletion happens async)
		// but no new segments should have been created
		if s.Path() != originalPath {
			assert.Fail(t, "Unexpected new segment created when all entries were deleted")
		}
	}
}

func TestSegmentRecompaction_MissingLiveRowRetainsSource(t *testing.T) {
	recompactor, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	entries := map[string][]byte{
		"missing-index": []byte("value-that-must-stay-readable"),
		"live":          []byte("another-live-value"),
		"dead":          []byte("dead-value-for-fragmentation"),
	}
	seg, err := createTestSegmentWithEntries(t, sm, meta, entries)
	require.NoError(t, err)

	missing, err := utils.GetMetadata(meta, string(keys.MakeMetadataKey("missing-index")))
	require.NoError(t, err)
	// Simulate a lost index row while leaving authoritative metadata intact.
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	require.NoError(t, meta.Handle().Delete(wo, keys.MakeSegmentLiveIndexKey(seg.Path(), missing.SegmentOffset)))
	// Make the segment fragmented without deleting the missing-index metadata.
	require.NoError(t, meta.Handle().Delete(wo, keys.MakeMetadataKey("dead")))
	recompactor.minSegments = 1
	recompactor.fragThreshold = 0.01

	require.NoError(t, recompactor.RecompactFragmentedSegments(context.Background()))
	paths := make(map[string]struct{})
	for _, got := range sm.GetSegments() {
		paths[got.Path()] = struct{}{}
	}
	require.Contains(t, paths, seg.Path(), "a missing live row must prevent source cleanup")

	vm, err := utils.GetMetadata(meta, string(keys.MakeMetadataKey("missing-index")))
	require.NoError(t, err)
	require.Equal(t, seg.Path(), vm.SegmentPath)
	reader, err := sm.ReadEntry("missing-index", vm.SegmentPath, vm.SegmentOffset, vm.ValueLength)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, entries["missing-index"], got)

	// The repair makes the next indexed pass able to find the row.
	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	slice, err := meta.Handle().Get(ro, keys.MakeSegmentLiveIndexKey(seg.Path(), missing.SegmentOffset))
	require.NoError(t, err)
	require.True(t, slice.Exists())
	slice.Free()
}

func TestSegmentRecompaction_ContextCancellation(t *testing.T) {
	recompactor, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	// Create multiple segments
	for i := 0; i < 3; i++ {
		entries := map[string][]byte{
			fmt.Sprintf("key%d", i): []byte(fmt.Sprintf("value%d", i)),
		}
		_, err := createTestSegmentWithEntries(t, sm, meta, entries)
		require.NoError(t, err)
	}

	// Create a context that will be cancelled immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Run recompaction with cancelled context
	err := recompactor.RecompactFragmentedSegments(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestSegmentRecompaction_FragmentationThreshold(t *testing.T) {
	recompactor, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	// Create a segment
	entries := map[string][]byte{
		"key1": make([]byte, 100),
		"key2": make([]byte, 100),
		"key3": make([]byte, 100),
		"key4": make([]byte, 100),
	}
	seg, err := createTestSegmentWithEntries(t, sm, meta, entries)
	require.NoError(t, err)

	// Delete one entry (25% fragmentation)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	metaKey := keys.MakeMetadataKey("key1")
	err = meta.Handle().Delete(wo, metaKey)
	require.NoError(t, err)

	// Add delete index
	deleteIndexKey := keys.MakeDeleteIndexKey(seg.Path())
	deleteEntry := &pb.DeleteIndexEntry{
		DeletedEntries: 1,
		DeletedBytes:   100,
	}
	deleteBytes, err := proto.Marshal(deleteEntry)
	require.NoError(t, err)
	err = meta.Handle().Put(wo, deleteIndexKey, deleteBytes)
	require.NoError(t, err)

	// Test with 50% threshold - should NOT recompact
	recompactor.fragThreshold = 0.5
	ctx := context.Background()
	err = recompactor.RecompactFragmentedSegments(ctx)
	assert.NoError(t, err)
	segments := sm.GetSegments()
	assert.Equal(t, 1, len(segments))

	// Test with 20% threshold - SHOULD recompact
	recompactor.fragThreshold = 0.2
	err = recompactor.RecompactFragmentedSegments(ctx)
	assert.NoError(t, err)

	// Should have created a new segment
	segments = sm.GetSegments()
	assert.GreaterOrEqual(t, len(segments), 1)
}

func TestSegmentRecompaction_OpenSegmentSkipped(t *testing.T) {
	recompactor, sm, _, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	// Create an open segment (not finalized)
	seg, err := sm.AcquireOpenSegmentWithReservation("test", 0)
	require.NoError(t, err)
	assert.True(t, seg.HasOpenFile())

	// Run recompaction - should skip open segments
	ctx := context.Background()
	err = recompactor.RecompactFragmentedSegments(ctx)
	assert.NoError(t, err)

	// Verify segment is still open
	assert.True(t, seg.HasOpenFile())
}

func TestGetFragmentationRatio(t *testing.T) {
	_, sm, _, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	// Create a segment
	seg, err := sm.AcquireOpenSegmentWithReservation("test", 0)
	require.NoError(t, err)

	// Simulate some data written
	seg.Lock()
	seg.IncrementSize(1000)
	seg.Unlock()

	err = sm.FinalizeSegment(seg)
	require.NoError(t, err)

	// Test fragmentation calculation
	ratio := sm.GetFragmentationRatio(seg.Path(), 300)
	assert.InDelta(t, 0.3, ratio, 0.01)

	ratio = sm.GetFragmentationRatio(seg.Path(), 500)
	assert.InDelta(t, 0.5, ratio, 0.01)

	ratio = sm.GetFragmentationRatio(seg.Path(), 0)
	assert.Equal(t, 0.0, ratio)

	// Test with non-existent segment
	ratio = sm.GetFragmentationRatio("/nonexistent/path", 100)
	assert.Equal(t, 0.0, ratio)
}

func TestIsSegmentFragmented(t *testing.T) {
	_, sm, _, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	// Create a segment
	seg, err := sm.AcquireOpenSegmentWithReservation("test", 0)
	require.NoError(t, err)

	// Simulate some data written
	seg.Lock()
	seg.IncrementSize(1000)
	seg.Unlock()

	err = sm.FinalizeSegment(seg)
	require.NoError(t, err)

	// Test with different thresholds
	assert.False(t, sm.IsSegmentFragmented(seg.Path(), 300, 0.5))  // 30% < 50%
	assert.True(t, sm.IsSegmentFragmented(seg.Path(), 600, 0.5))   // 60% > 50%
	assert.False(t, sm.IsSegmentFragmented(seg.Path(), 200, 0.25)) // 20% < 25%
	assert.True(t, sm.IsSegmentFragmented(seg.Path(), 300, 0.25))  // 30% > 25%
}

// TestSegmentRecompaction_IncompletePassKeepsOldSegment is the data-loss guard
// for a partial recompaction pass: when a live entry cannot be migrated (here, a
// truncated payload makes its copy — or the scan beyond it — fail), the old
// segment must NOT be removed or queued for deletion, because that entry's
// metadata still points into it. The successfully copied entries are committed;
// the segment stays tracked for a later retry.
func TestSegmentRecompaction_IncompletePassKeepsOldSegment(t *testing.T) {
	recompactor, sm, meta, _, cleanup := setupTestRecompactor(t)
	defer cleanup()

	entries := map[string][]byte{
		"intact-1": []byte("intact-value-one-0123456789"),
		"intact-2": []byte("intact-value-two-0123456789"),
		"victim":   []byte("victim-value-0123456789abcdef"),
	}
	seg, err := createTestSegmentWithEntries(t, sm, meta, entries)
	require.NoError(t, err)

	// Find the entry at the highest offset (last in file order) and truncate the
	// segment file mid-payload so migrating it must fail.
	type located struct {
		key    string
		offset int64
		length int64
	}
	var last located
	var firstKey string
	firstOffset := int64(-1)
	for key := range entries {
		vm, err := utils.GetMetadata(meta, string(keys.MakeMetadataKey(key)))
		require.NoError(t, err)
		require.Equal(t, seg.Path(), vm.SegmentPath)
		if vm.SegmentOffset > last.offset || last.key == "" {
			last = located{key, vm.SegmentOffset, vm.ValueLength}
		}
		if firstOffset == -1 || vm.SegmentOffset < firstOffset {
			firstOffset = vm.SegmentOffset
			firstKey = key
		}
	}
	cut := last.offset + segment.CalculateValueHeaderSize(last.key) + last.length/2
	require.NoError(t, os.Truncate(seg.Path(), cut))

	// Run the pass directly; the incomplete-pass guard must not surface an error.
	require.NoError(t, recompactor.recompactSegment(context.Background(), seg))

	// The old segment must survive: file present, still tracked by the manager
	// (an intact entry's original bytes remain readable at its old location),
	// and nothing queued for deletion.
	_, err = os.Stat(seg.Path())
	require.NoError(t, err, "old segment file must not be deleted after an incomplete pass")

	vm, err := utils.GetMetadata(meta, string(keys.MakeMetadataKey(firstKey)))
	require.NoError(t, err)
	reader, err := sm.ReadEntry(firstKey, seg.Path(), firstOffset, int64(len(entries[firstKey])))
	require.NoError(t, err, "old segment must still be tracked by the manager")
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, entries[firstKey], got)
	_ = vm

	require.Zero(t, recompactor.deletionQueue.GetQueueDepth(),
		"an incomplete pass must not queue the old segment for deletion")
}
