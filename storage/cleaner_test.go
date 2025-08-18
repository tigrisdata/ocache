package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTTLCleanup(t *testing.T) {
	// Set short cleanup interval for testing
	os.Setenv("OCACHE_TEST_CLEANUP_INTERVAL", "100ms")
	defer os.Unsetenv("OCACHE_TEST_CLEANUP_INTERVAL")

	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "cleaner-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Initialize storage with short cleanup interval for testing
	InitStorageWithConfig(&StorageConfig{
		DiskPath:         tmpDir,
		TTL:              0,
		InlineThreshold:  100,
		CompactThreshold: 1024 * 1024,
		SegmentSize:      256 * 1024 * 1024,
		FdCacheSize:      100,
		MaxDiskUsage:     0,
	})
	defer CloseStorage()

	s := GetStorage()
	require.NotNil(t, s)
	require.NotNil(t, s.cleaner)

	// Add some keys with short TTL
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("ttl-key-%d", i)
		data := bytes.Repeat([]byte("a"), 50)
		err := s.Put(key, bytes.NewReader(data), 1) // 1 second TTL
		require.NoError(t, err)
	}

	// Add some keys without TTL
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("no-ttl-key-%d", i)
		data := bytes.Repeat([]byte("b"), 50)
		err := s.Put(key, bytes.NewReader(data), 0) // No TTL
		require.NoError(t, err)
	}

	// Verify all keys exist
	keys, err := s.ListKeys()
	require.NoError(t, err)
	assert.Equal(t, 10, len(keys))

	// Wait for TTL to expire and cleanup to run
	time.Sleep(2 * time.Second)

	// Check that expired keys are gone
	keys, err = s.ListKeys()
	require.NoError(t, err)
	assert.Equal(t, 5, len(keys))

	// Verify only non-TTL keys remain
	for _, key := range keys {
		assert.Contains(t, key, "no-ttl-key")
	}

	// Check cleaner stats
	cleaned, _ := s.CleanerStats()
	assert.GreaterOrEqual(t, cleaned, int64(5))
}

func TestLRUEviction(t *testing.T) {
	// Set short cleanup interval for testing
	os.Setenv("OCACHE_TEST_CLEANUP_INTERVAL", "100ms")
	defer os.Unsetenv("OCACHE_TEST_CLEANUP_INTERVAL")

	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "lru-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Initialize storage with disk usage limit
	maxDiskUsage := int64(1000) // 1KB limit
	InitStorageWithConfig(&StorageConfig{
		DiskPath:         tmpDir,
		TTL:              0,
		InlineThreshold:  100,
		CompactThreshold: 1024 * 1024,
		SegmentSize:      256 * 1024 * 1024,
		FdCacheSize:      100,
		MaxDiskUsage:     maxDiskUsage,
	})
	defer CloseStorage()

	s := GetStorage()
	require.NotNil(t, s)
	require.NotNil(t, s.cleaner)

	// Add keys with specific access times to ensure predictable LRU behavior
	baseTime := time.Now().Unix()

	// First batch - will be evicted (oldest access times)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("old-key-%d", i)
		data := bytes.Repeat([]byte("x"), 100) // 100 bytes each
		err := s.Put(key, bytes.NewReader(data), 0)
		require.NoError(t, err)

		// Set access times in the past, with keys 0-7 being oldest
		accessTime := baseTime - int64(100-i)
		s.SetAccessTime(key, accessTime)
	}

	// Flush access updates to ensure they're written to RocksDB
	s.FlushAccessUpdates()

	// Wait for initial size calculation to complete
	time.Sleep(200 * time.Millisecond)

	// Add new keys that should trigger eviction
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("new-key-%d", i)
		data := bytes.Repeat([]byte("y"), 100) // 100 bytes each
		err := s.Put(key, bytes.NewReader(data), 0)
		require.NoError(t, err)
	}

	// Wait for eviction to run (cleanup interval is 100ms, wait for multiple cycles)
	time.Sleep(1 * time.Second)

	// Check remaining keys
	keys, err := s.ListKeys()
	require.NoError(t, err)

	// With 1KB limit and 15 keys of 100 bytes each (1500 bytes total),
	// we need to evict at least 500 bytes to get under the limit
	// The cleaner targets 90% of max (900 bytes), so it needs to evict 600 bytes
	// This means at least 6 keys should be evicted

	// Verify that we have fewer keys than we started with
	assert.Less(t, len(keys), 15, "Expected some keys to be evicted")

	// Count how many old keys were evicted
	oldKeysEvicted := 0
	for i := 0; i < 10; i++ {
		found := false
		oldKey := fmt.Sprintf("old-key-%d", i)
		for _, key := range keys {
			if key == oldKey {
				found = true
				break
			}
		}
		if !found {
			oldKeysEvicted++
		}
	}

	// At least some old keys should be evicted (they have the oldest access times)
	assert.GreaterOrEqual(t, oldKeysEvicted, 5, "Expected at least 5 old keys to be evicted")

	// All new keys should still exist (they have the most recent access times)
	for i := 0; i < 5; i++ {
		found := false
		expectedKey := fmt.Sprintf("new-key-%d", i)
		for _, key := range keys {
			if key == expectedKey {
				found = true
				break
			}
		}
		assert.True(t, found, fmt.Sprintf("%s should not be evicted (most recent access time)", expectedKey))
	}

	// Check eviction stats
	_, evicted := s.CleanerStats()
	assert.Greater(t, evicted, int64(0))
}

func TestDiskUsageTracking(t *testing.T) {
	// Set short cleanup interval for testing
	os.Setenv("OCACHE_TEST_CLEANUP_INTERVAL", "100ms")
	defer os.Unsetenv("OCACHE_TEST_CLEANUP_INTERVAL")

	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "disk-usage-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Initialize storage with short cleanup interval for testing
	InitStorageWithConfig(&StorageConfig{
		DiskPath:         tmpDir,
		TTL:              0,
		InlineThreshold:  100,
		CompactThreshold: 1024 * 1024,
		SegmentSize:      256 * 1024 * 1024,
		FdCacheSize:      100,
		MaxDiskUsage:     0,
	})
	defer CloseStorage()

	s := GetStorage()
	require.NotNil(t, s)
	require.NotNil(t, s.cleaner)

	// Wait for cleaner initialization to complete
	s.cleaner.WaitForInitialization()

	// Add some data and verify size tracking
	totalSize := int64(0)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		dataSize := 100 + i*50 // Variable sizes
		data := bytes.Repeat([]byte("z"), dataSize)
		err := s.Put(key, bytes.NewReader(data), 0)
		require.NoError(t, err)
		totalSize += int64(dataSize)
	}

	// Wait a bit for size updates
	time.Sleep(100 * time.Millisecond)

	// Check tracked size
	trackedSize := s.cleaner.totalSize.Load()
	assert.Equal(t, totalSize, trackedSize)

	// Delete a key and verify size update
	s.DeleteKey("key-2")
	time.Sleep(100 * time.Millisecond)

	expectedSize := totalSize - 200 // key-2 had 200 bytes
	trackedSize = s.cleaner.totalSize.Load()
	assert.Equal(t, expectedSize, trackedSize)
}
