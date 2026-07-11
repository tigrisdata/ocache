// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	pb "github.com/tigrisdata/ocache/storage/proto"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestStorage_DeleteIndex_SegmentDeletion(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	// Create a key that simulates being stored in a segment
	key := "segment_key"
	segmentPath := "/data/segments/segment1.seg"
	valueSize := int64(1024)

	// Manually insert a metadata entry that points to a segment
	valueMsg := &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		SegmentPath:   segmentPath,
		SegmentOffset: 0,
		ValueLength:   valueSize,
	}
	data, err := proto.Marshal(valueMsg)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	err = s.meta.Handle().Put(wo, keys.MakeMetadataKey(key), data)
	require.NoError(t, err)

	// Delete the key
	err = s.DeleteKey(key)
	require.NoError(t, err)

	// Verify the key is deleted
	_, found, err := s.Get(key, 0, 0)
	assert.NoError(t, err)
	assert.False(t, found)

	// Check that delete index was updated
	deletedEntries, deletedBytes, err := s.GetDeleteIndexStats(segmentPath)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deletedEntries)
	assert.Equal(t, valueSize, deletedBytes)
}

func TestStorage_DeleteIndex_MultipleSegmentDeletions(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	segmentPath := "/data/segments/segment2.seg"
	numKeys := 5
	valueSize := int64(512)

	// Create multiple keys in the same segment
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		valueMsg := &pb.ValueMessage{
			ValueType:     pb.ValueType_SEGMENT,
			SegmentPath:   segmentPath,
			SegmentOffset: int64(i * int(valueSize)),
			ValueLength:   valueSize,
		}
		data, err := proto.Marshal(valueMsg)
		require.NoError(t, err)

		err = s.meta.Handle().Put(wo, keys.MakeMetadataKey(key), data)
		require.NoError(t, err)
	}

	// Delete some keys
	deletedKeys := 3
	for i := 0; i < deletedKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		err := s.DeleteKey(key)
		require.NoError(t, err)
	}

	// Check delete index stats
	deletedEntries, deletedBytes, err := s.GetDeleteIndexStats(segmentPath)
	assert.NoError(t, err)
	assert.Equal(t, int64(deletedKeys), deletedEntries)
	assert.Equal(t, valueSize*int64(deletedKeys), deletedBytes)

	// Verify remaining keys still exist
	for i := deletedKeys; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		// Note: We're testing that the metadata for non-deleted keys is preserved.
		// The actual segment file doesn't exist in this test since we're only
		// simulating segment storage by creating metadata entries.
		metaKey := keys.MakeMetadataKey(key)
		ro := grocksdb.NewDefaultReadOptions()
		defer ro.Destroy()
		slice, err := s.meta.Handle().Get(ro, metaKey)
		assert.NoError(t, err)
		assert.True(t, slice.Exists())
		slice.Free()
	}
}

func TestStorage_DeleteIndex_RawFileDeletion(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	// Create a key that simulates being stored in a raw file
	key := "raw_file_key"
	rawFilePath := "/data/files/file1.raw"
	valueSize := int64(2048)

	// Manually insert a metadata entry that points to a raw file
	valueMsg := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: rawFilePath,
		ValueLength: valueSize,
	}
	data, err := proto.Marshal(valueMsg)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	err = s.meta.Handle().Put(wo, keys.MakeMetadataKey(key), data)
	require.NoError(t, err)

	// Delete the key
	err = s.DeleteKey(key)
	require.NoError(t, err)

	// Verify the key is deleted
	_, found, err := s.Get(key, 0, 0)
	assert.NoError(t, err)
	assert.False(t, found)

	// Check that NO delete index entry was created (raw files don't use delete index)
	// Verify by checking that the delete index is empty
	stats, err := s.ListSegmentDeleteStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 0, "Raw file deletion should not create delete index entries")
}

func TestStorage_DeleteIndex_InlineDeletion(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	// Create a small inline key
	key := "inline_key"
	value := []byte("small value")

	err := s.Put(key, bytes.NewReader(value), 0)
	assert.NoError(t, err)

	// Delete the key
	err = s.DeleteKey(key)
	require.NoError(t, err)

	// Verify the key is deleted
	_, found, err := s.Get(key, 0, 0)
	assert.NoError(t, err)
	assert.False(t, found)

	// Check that NO delete index entries were created (inline values don't use delete index)
	stats, err := s.ListSegmentDeleteStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 0)
}

func TestStorage_ListSegmentDeleteStats(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	// Create keys in multiple segments
	segments := []struct {
		path      string
		numKeys   int
		numDelete int
		valueSize int64
	}{
		{"/data/segments/seg1.seg", 10, 5, 256},
		{"/data/segments/seg2.seg", 8, 3, 512},
		{"/data/segments/seg3.seg", 6, 6, 128}, // All deleted
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	for _, seg := range segments {
		// Create keys
		for i := 0; i < seg.numKeys; i++ {
			key := fmt.Sprintf("%s_key_%d", seg.path, i)
			valueMsg := &pb.ValueMessage{
				ValueType:     pb.ValueType_SEGMENT,
				SegmentPath:   seg.path,
				SegmentOffset: int64(i * int(seg.valueSize)),
				ValueLength:   seg.valueSize,
			}
			data, err := proto.Marshal(valueMsg)
			require.NoError(t, err)

			err = s.meta.Handle().Put(wo, keys.MakeMetadataKey(key), data)
			require.NoError(t, err)
		}

		// Delete some keys
		for i := 0; i < seg.numDelete; i++ {
			key := fmt.Sprintf("%s_key_%d", seg.path, i)
			err := s.DeleteKey(key)
			require.NoError(t, err)
		}
	}

	// Get all segment delete stats
	stats, err := s.ListSegmentDeleteStats()
	assert.NoError(t, err)
	assert.Len(t, stats, 3)

	// Verify stats for each segment
	statsMap := make(map[string]SegmentDeleteStats)
	for _, stat := range stats {
		statsMap[stat.SegmentPath] = stat
	}

	for _, seg := range segments {
		stat, ok := statsMap[seg.path]
		assert.True(t, ok, "Missing stats for segment %s", seg.path)
		assert.Equal(t, int64(seg.numDelete), stat.DeletedEntries)
		assert.Equal(t, seg.valueSize*int64(seg.numDelete), stat.DeletedBytes)
	}
}

func TestStorage_RemoveDeleteIndex(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	segmentPath := "/data/segments/segment_to_remove.seg"

	// Create and delete a key to create a delete index entry
	key := "key_to_delete"
	valueMsg := &pb.ValueMessage{
		ValueType:     pb.ValueType_SEGMENT,
		SegmentPath:   segmentPath,
		SegmentOffset: 0,
		ValueLength:   1024,
	}
	data, err := proto.Marshal(valueMsg)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	err = s.meta.Handle().Put(wo, keys.MakeMetadataKey(key), data)
	require.NoError(t, err)

	err = s.DeleteKey(key)
	require.NoError(t, err)

	// Verify delete index exists
	deletedEntries, deletedBytes, err := s.GetDeleteIndexStats(segmentPath)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deletedEntries)
	assert.Equal(t, int64(1024), deletedBytes)

	// Remove the delete index
	err = s.RemoveDeleteIndex(segmentPath)
	assert.NoError(t, err)

	// Verify delete index is gone
	deletedEntries, deletedBytes, err = s.GetDeleteIndexStats(segmentPath)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deletedEntries)
	assert.Equal(t, int64(0), deletedBytes)
}

func TestStorage_DeleteIndex_KeyFormats(t *testing.T) {
	// Test delete index key creation and extraction
	segmentPath := "/data/segments/test.seg"
	deleteKey := keys.MakeDeleteIndexKey(segmentPath)

	// Verify prefix
	assert.True(t, keys.IsDeleteIndexKey(deleteKey))
	assert.True(t, keys.IsInternalKey(deleteKey))

	// Verify extraction
	extractedPath := keys.ExtractSegmentPath(deleteKey)
	assert.Equal(t, segmentPath, extractedPath)

	// Test with empty path
	emptyKey := keys.MakeDeleteIndexKey("")
	assert.Equal(t, keys.DeleteIndexPrefix, string(emptyKey))
}

func TestStorage_DeleteIndex_ConcurrentDeletions(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	segmentPath := "/data/segments/concurrent.seg"
	numGoroutines := 10
	keysPerGoroutine := 20
	valueSize := int64(1024)

	// Create keys that will be deleted concurrently
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	for i := 0; i < numGoroutines*keysPerGoroutine; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i)
		valueMsg := &pb.ValueMessage{
			ValueType:     pb.ValueType_SEGMENT,
			SegmentPath:   segmentPath,
			SegmentOffset: int64(i) * valueSize,
			ValueLength:   valueSize,
		}
		data, err := proto.Marshal(valueMsg)
		require.NoError(t, err)

		err = s.meta.Handle().Put(wo, keys.MakeMetadataKey(key), data)
		require.NoError(t, err)
	}

	// Delete keys concurrently
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			start := goroutineID * keysPerGoroutine
			end := start + keysPerGoroutine

			for i := start; i < end; i++ {
				key := fmt.Sprintf("concurrent_key_%d", i)
				err := s.DeleteKey(key)
				require.NoError(t, err)
			}
		}(g)
	}

	wg.Wait()

	// Verify that all deletions were tracked correctly
	deletedEntries, deletedBytes, err := s.GetDeleteIndexStats(segmentPath)
	assert.NoError(t, err)

	expectedEntries := int64(numGoroutines * keysPerGoroutine)
	expectedBytes := expectedEntries * valueSize

	assert.Equal(t, expectedEntries, deletedEntries,
		"All concurrent deletions should be tracked atomically via merge operator")
	assert.Equal(t, expectedBytes, deletedBytes,
		"Total deleted bytes should match expected value")
}

func TestStorage_DeleteIndex_MergeOperatorAccumulation(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	segmentPath := "/data/segments/merge_test.seg"

	// Directly test merge operator functionality
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	deleteIndexKey := keys.MakeDeleteIndexKey(segmentPath)

	// Perform multiple merge operations directly
	for i := 0; i < 5; i++ {
		operand := merge.MakeDeleteIndexOperand(1, 2048)
		err := s.meta.Handle().Merge(wo, deleteIndexKey, operand)
		require.NoError(t, err)
	}

	// Read back the accumulated value
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	slice, err := s.meta.Handle().Get(ro, deleteIndexKey)
	require.NoError(t, err)
	defer slice.Free()

	var entry pb.DeleteIndexEntry
	err = proto.Unmarshal(slice.Data(), &entry)
	require.NoError(t, err)

	assert.Equal(t, int64(5), entry.DeletedEntries,
		"Merge operator should accumulate entries count")
	assert.Equal(t, int64(10240), entry.DeletedBytes,
		"Merge operator should accumulate bytes count")
}

func TestStorage_DeleteIndex_MergeOnExistingValue(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 8, 4096, 16*1024*1024, 1000, 1024*1024)
	defer cleanup()

	segmentPath := "/data/segments/existing_value.seg"
	deleteIndexKey := keys.MakeDeleteIndexKey(segmentPath)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	// First, put an existing value
	initialEntry := &pb.DeleteIndexEntry{
		DeletedEntries: 10,
		DeletedBytes:   20480,
	}
	data, err := proto.Marshal(initialEntry)
	require.NoError(t, err)

	err = s.meta.Handle().Put(wo, deleteIndexKey, data)
	require.NoError(t, err)

	// Now merge additional deletes
	operand := merge.MakeDeleteIndexOperand(5, 10240)
	err = s.meta.Handle().Merge(wo, deleteIndexKey, operand)
	require.NoError(t, err)

	// Read back and verify accumulation
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	slice, err := s.meta.Handle().Get(ro, deleteIndexKey)
	require.NoError(t, err)
	defer slice.Free()

	var entry pb.DeleteIndexEntry
	err = proto.Unmarshal(slice.Data(), &entry)
	require.NoError(t, err)

	assert.Equal(t, int64(15), entry.DeletedEntries,
		"Merge operator should add to existing entries count")
	assert.Equal(t, int64(30720), entry.DeletedBytes,
		"Merge operator should add to existing bytes count")
}
