package storage

import (
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
)

func TestAccessUpdater_BasicUpdate(t *testing.T) {
	// Create storage without MaxDiskUsage to avoid conflicting accessUpdater
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create a standalone access updater for testing
	updater := newAccessUpdater(storage, 100, 10*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// Queue an update
	updater.UpdateNow("test-key-1")

	// Wait for the update to be processed
	time.Sleep(500 * time.Millisecond)

	// Verify the update was written to RocksDB
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Check that the secondary index was created
	bucketIndexKey := keys.MakeBucketedAccessIndexKey("test-key-1")
	slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice.Exists(), "Secondary index for test-key-1 should exist")

	// Verify the bucketed key exists
	bucketKey := slice.Data()
	// Make a copy of the key before freeing the slice (important!)
	bucketKeyCopy := make([]byte, len(bucketKey))
	copy(bucketKeyCopy, bucketKey)
	slice.Free()

	slice2, err := storage.meta.Handle().Get(ro, bucketKeyCopy)
	require.NoError(t, err)
	require.True(t, slice2.Exists(), "Bucketed key for test-key-1 should exist")
	slice2.Free()
}

func TestAccessUpdater_InBatchDeduplication(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater with longer interval to batch updates
	updater := newAccessUpdater(storage, 1000, 200*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// Queue multiple updates for the same key within one batch window
	firstUpdate := time.Now().Unix()
	for i := 0; i < 5; i++ {
		updater.Update("test-key-dedup", firstUpdate+int64(i))
		time.Sleep(10 * time.Millisecond)
	}

	// Also add updates for different keys
	updater.UpdateNow("test-key-other-1")
	updater.UpdateNow("test-key-other-2")

	// Wait for batch to flush
	time.Sleep(500 * time.Millisecond)

	// Verify only one update per key was written
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Check test-key-dedup (should have only the latest update)
	bucketIndexKey := keys.MakeBucketedAccessIndexKey("test-key-dedup")
	slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice.Exists())

	bucketKey := slice.Data()
	_, accessTime, err := keys.ParseBucketedAccessKey(bucketKey)
	require.NoError(t, err)

	// The access time should be the time of the first update
	assert.Equal(t, firstUpdate, accessTime.Unix())
	slice.Free()

	// Verify other keys were also written
	for _, key := range []string{"test-key-other-1", "test-key-other-2"} {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists())
		slice.Free()
	}
}

func TestAccessUpdater_BufferOverflow(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater with very small buffer
	updater := newAccessUpdater(storage, 2, 100*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// Try to queue more updates than buffer can hold
	// These should not block (best-effort)
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			updater.UpdateNow("test-key-overflow")
		}
		done <- true
	}()

	// Should complete quickly without blocking
	select {
	case <-done:
		// no blocking
	case <-time.After(250 * time.Millisecond):
		t.Fatal("UpdateNow blocked when buffer was full")
	}
}

func TestAccessUpdater_ExplicitFlush(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater with long interval
	updater := newAccessUpdater(storage, 100, 10*time.Second, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// Queue updates
	updater.UpdateNow("test-key-flush-1")
	updater.UpdateNow("test-key-flush-2")

	time.Sleep(100 * time.Millisecond)

	// Explicitly flush without waiting for interval
	updater.Flush()

	// Verify updates were written immediately
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	for _, key := range []string{"test-key-flush-1", "test-key-flush-2"} {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists(), "Key %s should exist after flush", key)
		slice.Free()
	}
}

func TestAccessUpdater_UpdatesOldBucketedEntry(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater
	updater := newAccessUpdater(storage, 100, 10*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// First update
	updater.UpdateNow("test-key-bucket-update")
	time.Sleep(500 * time.Millisecond)

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Get the first bucket key
	bucketIndexKey := keys.MakeBucketedAccessIndexKey("test-key-bucket-update")
	slice1, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice1.Exists(), "First bucketed entry for test-key-bucket-update should exist")
	firstBucketKey := make([]byte, len(slice1.Data()))
	copy(firstBucketKey, slice1.Data())
	slice1.Free()

	// Verify the first bucketed entry exists
	slice, err := storage.meta.Handle().Get(ro, firstBucketKey)
	require.NoError(t, err)
	require.True(t, slice.Exists())
	slice.Free()

	// Simulate time passing for one key
	updater.accessTimeLRU.Add("test-key-bucket-update", time.Now().Add(-6*time.Minute).Unix())

	// Second update should delete old entry and create new one
	updater.UpdateNow("test-key-bucket-update")
	time.Sleep(500 * time.Millisecond)

	// Get the new bucket key
	slice2, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice2.Exists())
	secondBucketKey := make([]byte, len(slice2.Data()))
	copy(secondBucketKey, slice2.Data())
	slice2.Free()

	// Keys should be different (different timestamps)
	t.Logf("First bucket key:  %s", string(firstBucketKey))
	t.Logf("Second bucket key: %s", string(secondBucketKey))
	assert.NotEqual(t, firstBucketKey, secondBucketKey, "Keys should be different after time-gated update")

	// Old bucketed entry should be deleted
	slice3, err := storage.meta.Handle().Get(ro, firstBucketKey)
	require.NoError(t, err)
	assert.False(t, slice3.Exists(), "Old bucketed entry should be deleted")
	slice3.Free()

	// New bucketed entry should exist
	slice4, err := storage.meta.Handle().Get(ro, secondBucketKey)
	require.NoError(t, err)
	assert.True(t, slice4.Exists(), "New bucketed entry should exist")
	slice4.Free()
}

func TestAccessUpdater_TimeGating(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater
	updater := newAccessUpdater(storage, 100, 10*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Update multiple keys
	testKeys := []string{"key1", "key2", "key3"}
	for _, key := range testKeys {
		updater.UpdateNow(key)
	}
	time.Sleep(500 * time.Millisecond)

	// Get the access times for the keys
	keyAccessTimes := make(map[string]time.Time)
	for _, key := range testKeys {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists(), "Key %s should exist (first pass)", key)
		bucketKey := slice.Data()

		_, accessTime, err := keys.ParseBucketedAccessKey(bucketKey)
		require.NoError(t, err)
		keyAccessTimes[key] = accessTime
		slice.Free()
	}

	// Try to update them again (should be gated)
	for _, key := range testKeys {
		updater.UpdateNow(key)
	}
	time.Sleep(500 * time.Millisecond)

	// Each key should have exactly one entry (second updates were gated)
	for _, key := range testKeys {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists(), "Key %s should exist (second pass)", key)
		bucketKey := slice.Data()

		_, accessTime, err := keys.ParseBucketedAccessKey(bucketKey)
		require.NoError(t, err)
		require.Equal(t, keyAccessTimes[key], accessTime, "Key %s should have the same access time (second pass)", key)
		slice.Free()
	}

	// Simulate time passing for one key
	updater.accessTimeLRU.Add("key2", time.Now().Add(-6*time.Minute).Unix())
	updater.UpdateNow("key2")
	time.Sleep(500 * time.Millisecond)

	// key2 should have a new timestamp, others should not
	bucketIndexKey := keys.MakeBucketedAccessIndexKey("key2")
	slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice.Exists(), "Key key2 should exist (third pass)")

	bucketKey := slice.Data()
	_, accessTime, err := keys.ParseBucketedAccessKey(bucketKey)
	require.NoError(t, err)

	// Should be recent
	assert.Greater(t, accessTime.Unix(), keyAccessTimes["key2"].Unix())
	slice.Free()
}

func TestAccessUpdater_StopFlushesRemaining(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater with long interval
	updater := newAccessUpdater(storage, 100, 10*time.Second, 5*time.Minute)
	updater.Start()

	// Queue updates
	updater.UpdateNow("test-key-stop-flush-1")
	updater.UpdateNow("test-key-stop-flush-2")

	// Stop should flush remaining updates
	updater.Stop()

	// Verify updates were written
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	for _, key := range []string{"test-key-stop-flush-1", "test-key-stop-flush-2"} {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists(), "Key %s should exist after stop", key)
		slice.Free()
	}
}
