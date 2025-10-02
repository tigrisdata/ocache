package storage

import (
	"bytes"
	"io"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// Helper function to create minimal storage config for testing corruption
func createCorruptionTestStorage(t *testing.T, tmpDir string) *Storage {
	config := &StorageConfig{
		DiskPath:         tmpDir,
		MaxDiskUsage:     100 * 1024 * 1024,
		InlineThreshold:  64 * 1024,
		CompactThreshold: 16 * 1024 * 1024,
		SegmentSize:      256 * 1024 * 1024,
		CleanupInterval:  0, // Disable cleanup for tests
	}
	stor, err := NewStorageWithConfig(config)
	require.NoError(t, err)
	return stor
}

func TestStorage_CorruptionHandling(t *testing.T) {
	t.Run("corrupted metadata returns error without deletion", func(t *testing.T) {
		tmpDir := t.TempDir()
		stor := createCorruptionTestStorage(t, tmpDir)

		key := "test-corrupted-key"
		value := []byte("test value")

		// Put a valid value first
		err := stor.Put(key, bytes.NewReader(value), 0)
		require.NoError(t, err)

		// Directly corrupt the metadata in RocksDB (using the correct metadata key format)
		metaKey := keys.MakeMetadataKey(key)
		corrupted := []byte("this is not a valid protobuf")
		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		err = stor.meta.Handle().Put(wo, metaKey, corrupted)
		require.NoError(t, err)

		// Try to get the corrupted key
		_, found, err := stor.Get(key, 0, -1)

		// Should return a corruption error
		assert.Error(t, err)
		assert.False(t, found)
		assert.True(t, storageErrors.IsCorruption(err), "Expected corruption error, got: %v", err)

		// Verify the key still exists in RocksDB (not deleted)
		ro := grocksdb.NewDefaultReadOptions()
		defer ro.Destroy()
		slice, err := stor.meta.Handle().Get(ro, metaKey)
		require.NoError(t, err)
		defer slice.Free()
		assert.Equal(t, corrupted, slice.Data(), "Corrupted data should still exist in database")
	})

	t.Run("unknown value type returns corruption error", func(t *testing.T) {
		tmpDir := t.TempDir()
		stor := createCorruptionTestStorage(t, tmpDir)

		key := "test-unknown-type"
		metaKey := keys.MakeMetadataKey(key)

		// Create a ValueMessage with an unknown type
		valueMsg := &pb.ValueMessage{
			ValueType: pb.ValueType(999), // Invalid/unknown type
			Data:      []byte("some data"),
		}

		// Marshal and store directly
		data, err := proto.Marshal(valueMsg)
		require.NoError(t, err)

		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		err = stor.meta.Handle().Put(wo, metaKey, data)
		require.NoError(t, err)

		// Try to get the key with unknown type
		_, found, err := stor.Get(key, 0, -1)

		// Should return a corruption error
		assert.Error(t, err)
		assert.False(t, found)
		assert.True(t, storageErrors.IsCorruption(err), "Expected corruption error for unknown type, got: %v", err)

		// Verify the key still exists (not deleted)
		ro := grocksdb.NewDefaultReadOptions()
		defer ro.Destroy()
		slice, err := stor.meta.Handle().Get(ro, metaKey)
		require.NoError(t, err)
		defer slice.Free()
		assert.NotEmpty(t, slice.Data(), "Data should still exist in database")
	})

	t.Run("valid data returns successfully", func(t *testing.T) {
		tmpDir := t.TempDir()
		stor := createCorruptionTestStorage(t, tmpDir)

		key := "test-valid-key"
		value := []byte("test value")

		// Put and get valid data
		err := stor.Put(key, bytes.NewReader(value), 0)
		require.NoError(t, err)

		reader, found, err := stor.Get(key, 0, -1)
		require.NoError(t, err)
		assert.True(t, found)

		// Read the data from the reader
		retrieved, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})

	t.Run("corruption error is not retryable", func(t *testing.T) {
		tmpDir := t.TempDir()
		stor := createCorruptionTestStorage(t, tmpDir)

		key := "test-non-retryable"
		metaKey := keys.MakeMetadataKey(key)

		// Corrupt the data directly
		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		err := stor.meta.Handle().Put(wo, metaKey, []byte("invalid protobuf"))
		require.NoError(t, err)

		// Get should return non-retryable corruption error
		_, _, err = stor.Get(key, 0, -1)
		require.Error(t, err)

		// Verify it's a non-retryable corruption error
		assert.True(t, storageErrors.IsCorruption(err))
		assert.False(t, storageErrors.IsRetryable(err))
	})
}

func TestStorage_IOErrorHandling(t *testing.T) {
	t.Run("segment read error returns retryable IO error", func(t *testing.T) {
		tmpDir := t.TempDir()
		stor := createCorruptionTestStorage(t, tmpDir)

		key := "test-segment-io-error"
		metaKey := keys.MakeMetadataKey(key)

		// Create a ValueMessage pointing to a non-existent segment file
		valueMsg := &pb.ValueMessage{
			ValueType:     pb.ValueType_SEGMENT,
			SegmentPath:   "/non/existent/segment.seg",
			SegmentOffset: 0,
			ValueLength:   100,
		}

		// Marshal and store
		data, err := proto.Marshal(valueMsg)
		require.NoError(t, err)

		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		err = stor.meta.Handle().Put(wo, metaKey, data)
		require.NoError(t, err)

		// Try to get the key - should fail with IO error
		_, found, err := stor.Get(key, 0, -1)

		// Should return an IO error that is retryable
		assert.Error(t, err)
		assert.False(t, found)

		// Check error type
		errType, ok := storageErrors.GetType(err)
		assert.True(t, ok, "Should be a StorageError")
		assert.Equal(t, storageErrors.TypeIO, errType, "Should be TypeIO error")
		assert.True(t, storageErrors.IsRetryable(err), "IO errors should be retryable")
		assert.False(t, storageErrors.IsCorruption(err), "Should not be classified as corruption")
	})

	t.Run("raw file read error returns retryable IO error", func(t *testing.T) {
		tmpDir := t.TempDir()
		stor := createCorruptionTestStorage(t, tmpDir)

		key := "test-file-io-error"
		metaKey := keys.MakeMetadataKey(key)

		// Create a ValueMessage pointing to a non-existent raw file
		valueMsg := &pb.ValueMessage{
			ValueType:   pb.ValueType_RAW_FILE,
			RawFilePath: "/non/existent/file.raw",
			ValueLength: 100,
		}

		// Marshal and store
		data, err := proto.Marshal(valueMsg)
		require.NoError(t, err)

		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		err = stor.meta.Handle().Put(wo, metaKey, data)
		require.NoError(t, err)

		// Try to get the key - should fail with IO error
		_, found, err := stor.Get(key, 0, -1)

		// Should return an IO error that is retryable
		assert.Error(t, err)
		assert.False(t, found)

		// Check error type
		errType, ok := storageErrors.GetType(err)
		assert.True(t, ok, "Should be a StorageError")
		assert.Equal(t, storageErrors.TypeIO, errType, "Should be TypeIO error")
		assert.True(t, storageErrors.IsRetryable(err), "IO errors should be retryable")
		assert.False(t, storageErrors.IsCorruption(err), "Should not be classified as corruption")
	})
}
