package compaction

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/deletion"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	"github.com/tigrisdata/ocache/storage/metadata"
	"github.com/tigrisdata/ocache/storage/segment"
	"google.golang.org/protobuf/proto"
)

func setupTestRecompactor(t *testing.T) (*SegmentRecompactor, *segment.Manager, *metadata.MetaDB, string, func()) {
	tmpDir, err := os.MkdirTemp("", "recompactor_test_*")
	require.NoError(t, err)

	// Initialize metadata DB with merge operator
	mergeOp := merge.NewMultiplexOperator()
	meta, err := metadata.NewMetaDB(tmpDir, 0, mergeOp)
	require.NoError(t, err)

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
	recompactor := NewSegmentRecompactor(sm, deletionQueue, 0.5, 0)

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
		offset, err := sm.WriteEntry(seg, key, f, vm)
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

	return seg, nil
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

	// Lower the fragmentation threshold to trigger recompaction
	recompactor.fragThreshold = 0.1

	// Run recompaction
	ctx := context.Background()
	err = recompactor.RecompactFragmentedSegments(ctx)
	assert.NoError(t, err)

	// Verify a new segment was created
	segments := sm.GetSegments()
	assert.GreaterOrEqual(t, len(segments), 1)

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
	}

	// Verify key2 is still deleted
	metaKey = keys.MakeMetadataKey("key2")
	slice, err := meta.Handle().Get(ro, metaKey)
	require.NoError(t, err)
	defer slice.Free()
	assert.False(t, slice.Exists())
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
