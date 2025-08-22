package integration

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/stretchr/testify/require"
)

// Test_Cleaner_Integration tests the cleaner with various scenarios
func (s *CleanerSuite) Test_Cleaner_Integration() {
	// Test 1: Add data with TTL and verify cleanup
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("ttl-%d", i)
		data := bytes.Repeat([]byte("a"), 500)   // 500 bytes each
		err := s.Harness.PutObject(key, data, 1) // 1 second TTL
		require.NoError(s.T(), err)
	}

	// Test 2: Add data without TTL (should be subject to LRU eviction)
	// We need to ensure different items have different access times
	baseTime := time.Now().Unix()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("lru-%d", i)
		data := bytes.Repeat([]byte("b"), 1000)  // 1KB each
		err := s.Harness.PutObject(key, data, 0) // No TTL
		require.NoError(s.T(), err)

		// Set explicit access time for each key to ensure deterministic LRU ordering
		// Earlier items get older timestamps
		s.Harness.SetAccessTime(key, baseTime-int64(100-i))
	}

	// Flush pending access updates to ensure they're written before cleanup runs
	s.Harness.FlushAccessUpdates()

	// Wait for TTL cleanup and eviction to run
	// Need extra time for cleaner to initialize and run multiple times
	time.Sleep(5 * time.Second)

	// Check stats
	cleaned, evicted := s.Harness.Storage.CleanerStats()
	require.Greater(s.T(), cleaned, int64(0), "Should have cleaned some TTL keys")

	// Verify TTL keys are cleaned up
	keys, err := s.Harness.Storage.ListKeys()
	require.NoError(s.T(), err)

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
	require.Equal(s.T(), 0, ttlKeysFound, "TTL keys should be cleaned up")

	// Due to disk usage limit (50KB) and each LRU key being 1KB,
	// we should have approximately 45-50 keys remaining (allowing for some overhead)
	require.Less(s.T(), lruKeysFound, 60, "LRU eviction should have removed old keys")
	require.Greater(s.T(), lruKeysFound, 30, "Should still have some keys remaining")

	// The remaining keys should be the most recently added ones
	// Since we set explicit access times, keys lru-90 to lru-99 should have the newest timestamps
	recentKeysFound := 0
	oldKeysFound := 0

	// Check recent keys (should mostly exist)
	for i := 90; i < 100; i++ {
		key := fmt.Sprintf("lru-%d", i)
		reader, found, err := s.Harness.Storage.Get(key, 0, 0)
		require.NoError(s.T(), err)
		if found {
			recentKeysFound++
			// Important: Close the reader if it's a ReadCloser to release file descriptors
			if rc, ok := reader.(io.ReadCloser); ok {
				rc.Close()
			}
		}
	}

	// Check old keys (should mostly be evicted)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("lru-%d", i)
		reader, found, err := s.Harness.Storage.Get(key, 0, 0)
		require.NoError(s.T(), err)
		if found {
			oldKeysFound++
			// Important: Close the reader if it's a ReadCloser to release file descriptors
			if rc, ok := reader.(io.ReadCloser); ok {
				rc.Close()
			}
		}
	}

	// Most recent keys should still exist (at least 8 out of 10)
	require.GreaterOrEqual(s.T(), recentKeysFound, 8, "Recent keys should be retained")
	// Most old keys should be evicted (at most 2 out of 10)
	require.LessOrEqual(s.T(), oldKeysFound, 2, "Old keys should be evicted")
	require.Greater(s.T(), evicted, int64(0), "Should have evicted some LRU keys")

	s.T().Logf("Test completed: cleaned=%d, evicted=%d, remaining_keys=%d",
		cleaned, evicted, len(keys))
}

// Test_Cleaner_TTLWithLRU tests the interaction between TTL and LRU
func (s *CleanerSuite) Test_Cleaner_TTLWithLRU() {
	// Add mix of TTL and non-TTL data
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("mixed-%d", i)
		data := bytes.Repeat([]byte("x"), 2000) // 2KB each

		if i < 10 {
			// First 10 with TTL
			err := s.Harness.PutObject(key, data, 1) // 1 second TTL
			require.NoError(s.T(), err)
		} else {
			// Next 10 without TTL
			err := s.Harness.PutObject(key, data, 0) // No TTL
			require.NoError(s.T(), err)
		}
	}

	// Wait for TTL cleanup
	time.Sleep(3 * time.Second)

	// Check that TTL keys are removed
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("mixed-%d", i)
		_, err := s.Harness.GetObject(key)
		require.Error(s.T(), err, "TTL key %s should be removed", key)
	}

	// Check that non-TTL keys still exist
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("mixed-%d", i)
		data, err := s.Harness.GetObject(key)
		require.NoError(s.T(), err, "Non-TTL key %s should still exist", key)
		require.Len(s.T(), data, 2000)
	}
}

// Test_Cleaner_DiskUsageLimit tests LRU eviction based on disk usage
func (s *CleanerSuite) Test_Cleaner_DiskUsageLimit() {
	// Fill storage beyond disk limit
	// With 50KB limit and 2KB per object, we should have ~25 objects max
	baseTime := time.Now().Unix()
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("disk-%d", i)
		data := bytes.Repeat([]byte("d"), 2000)  // 2KB each
		err := s.Harness.PutObject(key, data, 0) // No TTL
		require.NoError(s.T(), err)

		// Set access time so older items get evicted first
		s.Harness.SetAccessTime(key, baseTime-int64(50-i))
	}

	// Flush and wait for eviction
	s.Harness.FlushAccessUpdates()
	time.Sleep(3 * time.Second)

	// Count remaining keys
	keys, err := s.Harness.Storage.ListKeys()
	require.NoError(s.T(), err)

	diskKeysFound := 0
	for _, key := range keys {
		if len(key) >= 4 && key[:4] == "disk" {
			diskKeysFound++
		}
	}

	// Should have evicted about half the keys to stay under limit
	require.Less(s.T(), diskKeysFound, 30, "Should have evicted keys to stay under disk limit")
	require.Greater(s.T(), diskKeysFound, 15, "Should still have some keys")

	// Verify newer keys are retained
	for i := 45; i < 50; i++ {
		key := fmt.Sprintf("disk-%d", i)
		_, err := s.Harness.GetObject(key)
		require.NoError(s.T(), err, "Recent key %s should be retained", key)
	}

	// Verify older keys are evicted
	evictedCount := 0
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("disk-%d", i)
		_, err := s.Harness.GetObject(key)
		if err != nil {
			evictedCount++
		}
	}
	require.Greater(s.T(), evictedCount, 3, "Most old keys should be evicted")
}
