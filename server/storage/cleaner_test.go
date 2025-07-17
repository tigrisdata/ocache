package storage

import (
	"bytes"
	"fmt"
	"io"
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
	InitStorage(tmpDir, 0, 100, 1024*1024, 256*1024*1024, 100, 0)
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
	InitStorage(tmpDir, 0, 100, 1024*1024, 256*1024*1024, 100, maxDiskUsage)
	defer CloseStorage()

	s := GetStorage()
	require.NotNil(t, s)
	require.NotNil(t, s.cleaner)

	// Add keys that exceed memory limit
	// First batch - will be evicted (older access time)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("old-key-%d", i)
		data := bytes.Repeat([]byte("x"), 100) // 100 bytes each
		err := s.Put(key, bytes.NewReader(data), 0)
		require.NoError(t, err)
	}

	// Sleep to ensure different access times
	time.Sleep(100 * time.Millisecond)

	// Access some of the old keys to update their access time
	for i := 8; i < 10; i++ {
		key := fmt.Sprintf("old-key-%d", i)
		reader, found, err := s.Get(key)
		require.NoError(t, err)
		require.True(t, found)
		io.ReadAll(reader)
	}

	// Add new keys that should trigger eviction
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("new-key-%d", i)
		data := bytes.Repeat([]byte("y"), 100) // 100 bytes each
		err := s.Put(key, bytes.NewReader(data), 0)
		require.NoError(t, err)
	}

	// Wait for eviction to run
	time.Sleep(500 * time.Millisecond)

	// Check remaining keys
	keys, err := s.ListKeys()
	require.NoError(t, err)

	// Should have evicted oldest keys
	// old-key-8 and old-key-9 should still exist (recently accessed)
	foundOld8, foundOld9 := false, false
	for _, key := range keys {
		if key == "old-key-8" {
			foundOld8 = true
		}
		if key == "old-key-9" {
			foundOld9 = true
		}
		// old-key-0 through old-key-7 should be evicted
		for i := 0; i < 8; i++ {
			assert.NotEqual(t, fmt.Sprintf("old-key-%d", i), key)
		}
	}
	assert.True(t, foundOld8, "old-key-8 should not be evicted (recently accessed)")
	assert.True(t, foundOld9, "old-key-9 should not be evicted (recently accessed)")

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
	InitStorage(tmpDir, 0, 100, 1024*1024, 256*1024*1024, 100, 0)
	defer CloseStorage()

	s := GetStorage()
	require.NotNil(t, s)
	require.NotNil(t, s.cleaner)

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
