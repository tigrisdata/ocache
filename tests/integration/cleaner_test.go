package integration

import (
	"bytes"
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
)

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
