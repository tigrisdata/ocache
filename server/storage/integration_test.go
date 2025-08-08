package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCleanerIntegration tests the cleaner with various scenarios
func TestCleanerIntegration(t *testing.T) {
	// Set short cleanup interval for testing
	os.Setenv("OCACHE_TEST_CLEANUP_INTERVAL", "200ms")
	defer os.Unsetenv("OCACHE_TEST_CLEANUP_INTERVAL")

	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "integration-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Initialize storage with cleaner
	InitStorage(
		tmpDir,
		0,             // ttl
		1024,          // inline threshold (1KB)
		10*1024*1024,  // compact threshold (10MB)
		256*1024*1024, // segment size (256MB)
		100,           // fd cache size
		50*1024,       // max disk usage (50KB)
	)
	defer CloseStorage()

	s := GetStorage()
	require.NotNil(t, s)

	// Test 1: Add data with TTL and verify cleanup
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("ttl-%d", i)
		data := bytes.Repeat([]byte("a"), 500)      // 500 bytes each
		err := s.Put(key, bytes.NewReader(data), 1) // 1 second TTL
		require.NoError(t, err)
	}

	// Test 2: Add data without TTL (should be subject to LRU eviction)
	// We need to ensure different items have different access times
	// Since access time is stored with second precision, we need to either:
	// 1. Add a longer delay between items, or
	// 2. Manually set different access times for testing
	baseTime := time.Now().Unix()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("lru-%d", i)
		data := bytes.Repeat([]byte("b"), 1000)     // 1KB each
		err := s.Put(key, bytes.NewReader(data), 0) // No TTL
		require.NoError(t, err)
		
		// Set explicit access time for each key to ensure deterministic LRU ordering
		// Earlier items get older timestamps
		s.SetAccessTime(key, baseTime-int64(100-i))
	}
	
	// Flush pending access updates to ensure they're written before cleanup runs
	s.FlushAccessUpdates()

	// Wait for TTL cleanup and eviction to run
	// Need extra time for cleaner to initialize and run multiple times
	time.Sleep(5 * time.Second)

	// Check stats
	cleaned, evicted := s.CleanerStats()
	require.Greater(t, cleaned, int64(0), "Should have cleaned some TTL keys")

	// Verify TTL keys are cleaned up
	keys, err := s.ListKeys()
	require.NoError(t, err)

	ttlKeysFound := 0
	lruKeysFound := 0
	for _, key := range keys {
		if len(key) >= 3 && key[:3] == "ttl" {
			ttlKeysFound++
		}
		if len(key) >= 3 && key[:3] == "lru" {
			lruKeysFound++
		}
	}

	// All TTL keys should be gone
	require.Equal(t, 0, ttlKeysFound, "TTL keys should be cleaned up")

	// Due to disk usage limit (50KB) and each LRU key being 1KB,
	// we should have approximately 45-50 keys remaining (allowing for some overhead)
	require.Less(t, lruKeysFound, 60, "LRU eviction should have removed old keys")
	require.Greater(t, lruKeysFound, 30, "Should still have some keys remaining")

	// The remaining keys should be the most recently added ones
	// Since we set explicit access times, keys lru-90 to lru-99 should have the newest timestamps
	recentKeysFound := 0
	oldKeysFound := 0
	
	// Check recent keys (should mostly exist)
	for i := 90; i < 100; i++ {
		key := fmt.Sprintf("lru-%d", i)
		reader, found, err := s.Get(key)
		require.NoError(t, err)
		if found {
			recentKeysFound++
			reader.(*bytes.Reader).Reset(nil) // Close reader
		}
	}
	
	// Check old keys (should mostly be evicted)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("lru-%d", i)
		reader, found, err := s.Get(key)
		require.NoError(t, err)
		if found {
			oldKeysFound++
			reader.(*bytes.Reader).Reset(nil) // Close reader
		}
	}

	// Most recent keys should still exist (at least 8 out of 10)
	require.GreaterOrEqual(t, recentKeysFound, 8, "Recent keys should be retained")
	// Most old keys should be evicted (at most 2 out of 10)
	require.LessOrEqual(t, oldKeysFound, 2, "Old keys should be evicted")
	require.Greater(t, evicted, int64(0), "Should have evicted some LRU keys")

	t.Logf("Test completed: cleaned=%d, evicted=%d, remaining_keys=%d",
		cleaned, evicted, len(keys))
}
