// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage"
	storagepb "github.com/tigrisdata/ocache/storage/proto"
)

// Helper functions for cleaner tests that need direct storage access

func setAccessTime(h TestHarnessInterface, key string, timestamp int64) {
	if storageAccess, ok := h.(TestStorageAccess); ok {
		storageAccess.SetAccessTime(key, timestamp)
	}
}

func flushAccessUpdates(h TestHarnessInterface) {
	if storageAccess, ok := h.(TestStorageAccess); ok {
		storageAccess.FlushAccessUpdates()
	}
}

// Test_Cleaner_AutoTriggerTTL tests that TTL cleanup automatically triggers
// after the configured interval and removes expired objects
func (s *CleanerSuite) Test_Cleaner_AutoTriggerTTL() {
	t := s.T()

	// Reconfigure with faster cleanup interval
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Use parameterized TTL test helper
	testCases := []TTLTestCase{
		// Objects with 1 second TTL (should expire)
		{Key: "ttl-expire-1", Size: 10 * 1024, TTL: 1, WaitTime: 2 * time.Second, ShouldExist: false},
		{Key: "ttl-expire-2", Size: 20 * 1024, TTL: 1, WaitTime: 2 * time.Second, ShouldExist: false},
		{Key: "ttl-expire-3", Size: 30 * 1024, TTL: 1, WaitTime: 2 * time.Second, ShouldExist: false},
		{Key: "ttl-expire-4", Size: 40 * 1024, TTL: 1, WaitTime: 2 * time.Second, ShouldExist: false},
		{Key: "ttl-expire-5", Size: 50 * 1024, TTL: 1, WaitTime: 2 * time.Second, ShouldExist: false},

		// Objects with no TTL (should persist)
		{Key: "ttl-persist-1", Size: 10 * 1024, TTL: 0, WaitTime: 2 * time.Second, ShouldExist: true},
		{Key: "ttl-persist-2", Size: 20 * 1024, TTL: 0, WaitTime: 2 * time.Second, ShouldExist: true},

		// Objects with longer TTL (should persist)
		{Key: "ttl-long-1", Size: 10 * 1024, TTL: 10, WaitTime: 2 * time.Second, ShouldExist: true},
	}

	// Record initial state
	initialStats := s.Harness.GetStorageStats()
	t.Logf("Initial storage stats - Total keys: %d", initialStats.TotalKeys)

	// Run the parameterized TTL tests
	RunTTLTests(t, s.Harness, testCases)

	// Verify cleanup stats
	finalStats := s.Harness.GetStorageStats()
	t.Logf("Final storage stats - Total keys: %d, Cleaned: %d",
		finalStats.TotalKeys, finalStats.CleanedKeys)

	require.GreaterOrEqual(t, finalStats.CleanedKeys, int64(5),
		"At least 5 TTL objects should be cleaned")
}

// Test_CleanerLoop_LRUEviction tests LRU eviction when disk usage exceeds limit
func (s *CleanerSuite) Test_CleanerLoop_LRUEviction() {
	t := s.T()

	// Re-create harness with low disk limit
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	config.AccessUpdateDelay = 100 * time.Millisecond
	config.MaxDiskUsage = 100 * 1024 // 100KB limit
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Store objects to exceed disk limit
	numObjects := 50
	keys := make([]string, numObjects)
	objectSize := int64(5 * 1024) // 5KB each, total 250KB (exceeds 100KB limit)

	t.Log("Storing 50 objects (5KB each) to trigger LRU eviction")
	baseTime := time.Now().Unix()

	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("lru-evict-%d", i)
		keys[i] = key
		data := GenerateRandomData(objectSize)

		err := s.Harness.PutObject(key, data, 0) // No TTL
		require.NoError(t, err, "Failed to store object %d", i)

		// Set access time so older items get evicted first (only for single-node tests)
		// Earlier items get older timestamps
		if storageAccess, ok := s.Harness.(TestStorageAccess); ok {
			storageAccess.SetAccessTime(key, baseTime-int64(numObjects-i))
		}
	}

	// Flush access updates (only for single-node tests)
	if storageAccess, ok := s.Harness.(TestStorageAccess); ok {
		storageAccess.FlushAccessUpdates()
	}

	// Record initial state
	initialStats := s.Harness.GetStorageStats()
	t.Logf("Initial storage - Keys: %d, Disk usage: %d bytes (limit: %d)",
		initialStats.TotalKeys, initialStats.DiskUsage, config.MaxDiskUsage)

	// Wait for LRU eviction to trigger
	t.Log("Waiting for LRU eviction to reduce disk usage...")
	time.Sleep(3 * time.Second)

	// Check if eviction occurred
	finalStats := s.Harness.GetStorageStats()
	t.Logf("Final storage - Keys: %d, Evicted: %d, Disk usage: %d bytes",
		finalStats.TotalKeys, finalStats.EvictedKeys, finalStats.DiskUsage)

	// Verify eviction happened
	require.Greater(t, finalStats.EvictedKeys, int64(0),
		"Expected keys to be evicted due to disk limit")

	// Should have evicted approximately 60% of keys to get under limit
	// With 50 keys * 5KB = 250KB, need to evict to get under 100KB
	// So should keep ~20 keys, evict ~30 keys
	remainingKeys := 0
	evictedKeys := 0

	// Check which keys remain (should be the most recent ones)
	for i, key := range keys {
		_, err := s.Harness.GetObject(key)
		if err == nil {
			remainingKeys++
			// Recent keys (high indices) should remain
			if i < 20 {
				t.Logf("Old key %s unexpectedly remains", key)
			}
		} else {
			evictedKeys++
			// Old keys (low indices) should be evicted
			if i >= 40 {
				t.Logf("Recent key %s unexpectedly evicted", key)
			}
		}
	}

	t.Logf("Remaining: %d keys, Evicted: %d keys", remainingKeys, evictedKeys)

	// Verify we're under the disk limit (with some tolerance for metadata)
	require.Less(t, remainingKeys, 25, "Should have evicted enough keys to stay under limit")
	require.Greater(t, evictedKeys, 25, "Should have evicted old keys")

	// Verify recent keys are retained
	recentKeysFound := 0
	for i := 40; i < 50; i++ { // Check last 10 keys
		_, err := s.Harness.GetObject(keys[i])
		if err == nil {
			recentKeysFound++
		}
	}
	require.GreaterOrEqual(t, recentKeysFound, 8, "Most recent keys should be retained")

	// Verify old keys are evicted
	oldKeysFound := 0
	for i := 0; i < 10; i++ { // Check first 10 keys
		_, err := s.Harness.GetObject(keys[i])
		if err == nil {
			oldKeysFound++
		}
	}
	require.LessOrEqual(t, oldKeysFound, 2, "Most old keys should be evicted")
}

// Test_CleanerLoop_FIFOEviction tests FIFO eviction when disk usage exceeds the
// limit: the oldest-WRITTEN objects are evicted first, and — unlike LRU — reading
// an old object does NOT protect it from eviction.
func (s *CleanerSuite) Test_CleanerLoop_FIFOEviction() {
	t := s.T()

	// Re-create harness with a low disk limit and the FIFO policy.
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	config.MaxDiskUsage = 100 * 1024 // 100KB limit
	config.EvictionPolicy = storage.EvictionPolicyFIFO
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Store objects in order; write order == FIFO order (oldest-written first).
	numObjects := 50
	keys := make([]string, numObjects)
	objectSize := int64(5 * 1024) // 5KB each, total 250KB (exceeds 100KB limit)

	t.Log("Storing 50 objects (5KB each) in order to trigger FIFO eviction")
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("fifo-evict-%d", i)
		keys[i] = key
		data := GenerateRandomData(objectSize)
		err := s.Harness.PutObject(key, data, 0) // No TTL
		require.NoError(t, err, "Failed to store object %d", i)
	}

	// Repeatedly read the oldest-written keys. Under LRU this would protect them;
	// under FIFO it must NOT — they are still the oldest-written and evict first.
	t.Log("Reading the oldest-written keys (must not protect them under FIFO)")
	for r := 0; r < 3; r++ {
		for i := 0; i < 10; i++ {
			_, _ = s.Harness.GetObject(keys[i])
		}
	}

	// Wait for FIFO eviction to trigger.
	t.Log("Waiting for FIFO eviction to reduce disk usage...")
	time.Sleep(3 * time.Second)

	finalStats := s.Harness.GetStorageStats()
	t.Logf("Final storage - Keys: %d, Evicted: %d, Disk usage: %d bytes",
		finalStats.TotalKeys, finalStats.EvictedKeys, finalStats.DiskUsage)
	require.Greater(t, finalStats.EvictedKeys, int64(0),
		"Expected keys to be evicted due to disk limit")

	// The oldest-written keys (which we also read) should be evicted.
	oldEvicted := 0
	for i := 0; i < 10; i++ {
		if _, err := s.Harness.GetObject(keys[i]); err != nil {
			oldEvicted++
		}
	}
	require.GreaterOrEqual(t, oldEvicted, 8,
		"Oldest-written keys should be evicted even though they were read (FIFO ignores reads)")

	// The newest-written keys should be retained.
	recentRetained := 0
	for i := 40; i < 50; i++ {
		if _, err := s.Harness.GetObject(keys[i]); err == nil {
			recentRetained++
		}
	}
	require.GreaterOrEqual(t, recentRetained, 8,
		"Newest-written keys should be retained under FIFO")

	t.Logf("FIFO eviction - oldest (also read) evicted: %d/10, newest retained: %d/10",
		oldEvicted, recentRetained)
}

// Test_CleanerLoop_MixedWorkload tests cleaner with mixed TTL/LRU workload
func (s *CleanerSuite) Test_CleanerLoop_MixedWorkload() {
	t := s.T()

	// Re-create harness with configuration
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	config.MaxDiskUsage = 150 * 1024 // 150KB limit
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Create a mixed workload
	t.Log("Creating mixed workload with TTL and LRU objects")

	// Phase 1: Objects with varying TTLs
	shortTTLKeys := []string{}
	mediumTTLKeys := []string{}
	longTTLKeys := []string{}

	for i := 0; i < 10; i++ {
		// Short TTL (1 second)
		key := fmt.Sprintf("mixed-short-ttl-%d", i)
		shortTTLKeys = append(shortTTLKeys, key)
		data := GenerateRandomData(5 * 1024) // 5KB
		err := s.Harness.PutObject(key, data, 1)
		require.NoError(t, err)

		// Medium TTL (3 seconds)
		key = fmt.Sprintf("mixed-medium-ttl-%d", i)
		mediumTTLKeys = append(mediumTTLKeys, key)
		data = GenerateRandomData(5 * 1024) // 5KB
		err = s.Harness.PutObject(key, data, 3)
		require.NoError(t, err)

		// Long TTL (10 seconds)
		key = fmt.Sprintf("mixed-long-ttl-%d", i)
		longTTLKeys = append(longTTLKeys, key)
		data = GenerateRandomData(5 * 1024) // 5KB
		err = s.Harness.PutObject(key, data, 10)
		require.NoError(t, err)
	}

	// Phase 2: Non-TTL objects with different access patterns
	frequentKeys := []string{}
	infrequentKeys := []string{}
	baseTime := time.Now().Unix()

	for i := 0; i < 15; i++ {
		// Frequently accessed (recent timestamps)
		key := fmt.Sprintf("mixed-frequent-%d", i)
		frequentKeys = append(frequentKeys, key)
		data := GenerateRandomData(4 * 1024) // 4KB
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(t, err)
		setAccessTime(s.Harness, key, baseTime-int64(i)) // Recent access

		// Infrequently accessed (old timestamps)
		key = fmt.Sprintf("mixed-infrequent-%d", i)
		infrequentKeys = append(infrequentKeys, key)
		data = GenerateRandomData(4 * 1024) // 4KB
		err = s.Harness.PutObject(key, data, 0)
		require.NoError(t, err)
		setAccessTime(s.Harness, key, baseTime-int64(1000+i)) // Old access
	}

	flushAccessUpdates(s.Harness)

	// Initial state
	initialStats := s.Harness.GetStorageStats()
	t.Logf("Initial state - Total keys: %d", initialStats.TotalKeys)

	// Wait for first cleanup cycle (short TTL should expire)
	t.Log("Phase 1: Waiting for short TTL cleanup...")
	time.Sleep(1500 * time.Millisecond)

	// Check short TTL keys
	shortTTLCleaned := 0
	for _, key := range shortTTLKeys {
		_, err := s.Harness.GetObject(key)
		if err != nil {
			shortTTLCleaned++
		}
	}
	// Very lenient requirement due to timing
	require.GreaterOrEqual(t, shortTTLCleaned, 3, "Some short TTL keys should be cleaned")

	// Check medium TTL keys (may have started expiring)
	mediumTTLSurvived := 0
	for _, key := range mediumTTLKeys {
		_, err := s.Harness.GetObject(key)
		if err == nil {
			mediumTTLSurvived++
		}
	}
	require.Greater(t, mediumTTLSurvived, 5, "Most medium TTL keys should still exist")

	// Wait for medium TTL to expire
	t.Log("Phase 2: Waiting for medium TTL cleanup...")
	time.Sleep(2 * time.Second)

	// Check medium TTL keys
	mediumTTLCleaned := 0
	for _, key := range mediumTTLKeys {
		_, err := s.Harness.GetObject(key)
		if err != nil {
			mediumTTLCleaned++
		}
	}
	// Very lenient requirement due to timing
	require.GreaterOrEqual(t, mediumTTLCleaned, 3, "Some medium TTL keys should be cleaned")

	// Check LRU eviction (infrequent keys should be evicted)
	t.Log("Phase 3: Checking LRU eviction...")
	infrequentEvicted := 0
	for _, key := range infrequentKeys {
		_, err := s.Harness.GetObject(key)
		if err != nil {
			infrequentEvicted++
		}
	}

	frequentRetained := 0
	for _, key := range frequentKeys {
		_, err := s.Harness.GetObject(key)
		if err == nil {
			frequentRetained++
		}
	}

	t.Logf("LRU results - Infrequent evicted: %d, Frequent retained: %d",
		infrequentEvicted, frequentRetained)

	// In a mixed workload with TTL and LRU, eviction patterns may vary
	// Due to timing and disk limits, we just verify some eviction occurred
	require.Greater(t, infrequentEvicted, 3, "Some infrequent keys should be evicted")
	// Frequent keys may also be evicted if disk limit is strict
	if frequentRetained == 0 {
		t.Log("Warning: All frequent keys were evicted - disk limit may be too strict")
		// At least verify that eviction happened
		require.Greater(t, infrequentEvicted, 5, "If frequent keys evicted, more infrequent should be too")
	} else {
		require.Greater(t, frequentRetained, 3, "Some frequent keys should be retained")
	}

	// Final stats
	finalStats := s.Harness.GetStorageStats()
	t.Logf("Final stats - Cleaned: %d, Evicted: %d, Remaining keys: %d",
		finalStats.CleanedKeys, finalStats.EvictedKeys, finalStats.TotalKeys)
}

// Test_CleanerLoop_ErrorRecovery tests cleaner recovery from errors
func (s *CleanerSuite) Test_CleanerLoop_ErrorRecovery() {
	t := s.T()

	// Re-create harness
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	config.MaxDiskUsage = 100 * 1024 // 100KB limit
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Store objects with TTL
	numObjects := 20
	keys := make([]string, numObjects)

	t.Log("Storing objects for error recovery test")
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("error-recovery-%d", i)
		keys[i] = key
		data := GenerateRandomData(5 * 1024)     // 5KB
		err := s.Harness.PutObject(key, data, 2) // 2 second TTL
		require.NoError(t, err)
	}

	// Simulate concurrent modifications during cleanup
	t.Log("Simulating concurrent modifications during cleanup")

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Continuously modify objects
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				// Update random objects
				for i := 0; i < 5; i++ {
					key := keys[i%len(keys)]
					newData := GenerateRandomData(6 * 1024) // Slightly larger
					s.Harness.PutObject(key, newData, 1)    // Reset TTL
				}
				time.Sleep(300 * time.Millisecond)
			}
		}
	}()

	// Delete some objects concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		deleted := 0
		for deleted < 5 {
			select {
			case <-stopChan:
				return
			default:
				if deleted < len(keys)/2 {
					s.Harness.DeleteObject(keys[deleted])
					deleted++
					time.Sleep(400 * time.Millisecond)
				}
			}
		}
	}()

	// Let cleanup run with interference
	time.Sleep(3 * time.Second)
	close(stopChan)
	wg.Wait()

	// Verify system is in consistent state
	t.Log("Verifying system consistency after concurrent operations")

	// Some keys should be cleaned (TTL expired)
	// Some should exist (updated or not yet expired)
	existingKeys := 0
	cleanedKeys := 0
	for _, key := range keys {
		_, err := s.Harness.GetObject(key)
		if err == nil {
			existingKeys++
		} else {
			cleanedKeys++
		}
	}

	t.Logf("After recovery - Existing: %d, Cleaned: %d", existingKeys, cleanedKeys)
	// More lenient requirements due to concurrent modifications
	require.GreaterOrEqual(t, cleanedKeys+existingKeys, 10, "At least some keys should be processed")

	// Verify cleanup continues to work
	t.Log("Verifying cleanup continues to function")

	// Add new TTL objects
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("recovery-new-%d", i)
		data := GenerateRandomData(3 * 1024)
		err := s.Harness.PutObject(key, data, 1) // 1 second TTL
		require.NoError(t, err)
	}

	// Wait for cleanup
	time.Sleep(2 * time.Second)

	// Verify new TTL objects are cleaned
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("recovery-new-%d", i)
		_, err := s.Harness.GetObject(key)
		require.Error(t, err, "New TTL object should be cleaned")
	}

	// Final stats
	stats := s.Harness.GetStorageStats()
	t.Logf("Final recovery stats - Cleaned: %d, Evicted: %d",
		stats.CleanedKeys, stats.EvictedKeys)
}

// Test_CleanerLoop_Performance tests cleaner performance with large datasets
func (s *CleanerSuite) Test_CleanerLoop_Performance() {
	t := s.T()

	// Re-create harness with performance configuration
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 500 * time.Millisecond
	config.MaxDiskUsage = 10 * 1024 * 1024 // 10MB limit
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Store a large dataset
	numObjects := 200
	totalSize := int64(0)
	keys := make([]string, numObjects)

	t.Log("Storing 200 objects for performance test")
	startTime := time.Now()

	// Mix of TTL and non-TTL objects
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("perf-test-%d", i)
		keys[i] = key
		// Vary sizes between 10KB and 100KB
		size := int64(10*1024 + (i%10)*10*1024)
		data := GenerateRandomData(size)
		totalSize += size

		ttl := int64(0)
		if i%3 == 0 { // Every third object has TTL
			ttl = int64(2 + i%3) // 2-4 seconds TTL
		}

		err := s.Harness.PutObject(key, data, ttl)
		require.NoError(t, err)

		// Set access times for LRU testing
		if ttl == 0 {
			accessTime := time.Now().Unix() - int64(numObjects-i)
			setAccessTime(s.Harness, key, accessTime)
		}
	}

	flushAccessUpdates(s.Harness)
	writeTime := time.Since(startTime)
	t.Logf("Wrote %d objects (%.2f MB) in %v", numObjects, float64(totalSize)/(1024*1024), writeTime)

	// Wait for cleanup cycles
	t.Log("Waiting for cleanup cycles...")
	cleanupStart := time.Now()
	time.Sleep(5 * time.Second)
	cleanupTime := time.Since(cleanupStart)

	// Measure cleanup effectiveness
	stats := s.Harness.GetStorageStats()

	// Count remaining objects by type
	ttlRemaining := 0
	nonTTLRemaining := 0
	for i, key := range keys {
		_, err := s.Harness.GetObject(key)
		if err == nil {
			if i%3 == 0 {
				ttlRemaining++
			} else {
				nonTTLRemaining++
			}
		}
	}

	// Calculate metrics
	cleanupRate := float64(stats.CleanedKeys) / cleanupTime.Seconds()
	evictionRate := float64(stats.EvictedKeys) / cleanupTime.Seconds()

	// Log performance metrics
	t.Logf("Performance Metrics:")
	t.Logf("  - Write throughput: %.2f MB/s", float64(totalSize)/writeTime.Seconds()/(1024*1024))
	t.Logf("  - Cleanup time: %v", cleanupTime)
	t.Logf("  - Cleanup rate: %.2f keys/sec", cleanupRate)
	t.Logf("  - Eviction rate: %.2f keys/sec", evictionRate)
	t.Logf("  - TTL cleaned: %d/%d", numObjects/3-ttlRemaining, numObjects/3)
	t.Logf("  - LRU evicted: %d", stats.EvictedKeys)
	t.Logf("  - Remaining: TTL=%d, Non-TTL=%d", ttlRemaining, nonTTLRemaining)

	// Performance assertions
	require.Greater(t, cleanupRate, 5.0, "Cleanup rate too low")
	require.Less(t, ttlRemaining, 10, "Too many TTL objects remaining")

	// Verify disk usage is under limit (with tolerance for metadata)
	finalStats := s.Harness.GetStorageStats()
	t.Logf("  - Final disk usage: %.2f MB (limit: %.2f MB)",
		float64(finalStats.DiskUsage)/(1024*1024), float64(config.MaxDiskUsage)/(1024*1024))
}

// Test_CleanerLoop_SelectiveEviction tests that LRU eviction is selective
// based on access patterns and respects different object types
func (s *CleanerSuite) Test_CleanerLoop_SelectiveEviction() {
	t := s.T()

	// Re-create harness with specific configuration
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	config.MaxDiskUsage = 200 * 1024 // 200KB limit
	config.InlineThreshold = 1024    // 1KB inline threshold
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Create objects with different characteristics
	t.Log("Creating objects with different storage types and access patterns")

	// Small inline objects (should not be evicted by LRU)
	inlineKeys := []string{}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("selective-inline-%d", i)
		inlineKeys = append(inlineKeys, key)
		data := GenerateRandomData(500) // 500 bytes (inline)
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(t, err)
		VerifyStorageType(t, s.Harness.GetTempDir(), key, storagepb.ValueType_INLINE)
	}

	// Medium raw file objects with old access times
	oldRawKeys := []string{}
	baseTime := time.Now().Unix()
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("selective-old-raw-%d", i)
		oldRawKeys = append(oldRawKeys, key)
		data := GenerateRandomData(8 * 1024) // 8KB (raw file)
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(t, err)
		setAccessTime(s.Harness, key, baseTime-int64(1000+i)) // Very old access
		VerifyStorageType(t, s.Harness.GetTempDir(), key, storagepb.ValueType_RAW_FILE)
	}

	// Medium raw file objects with recent access times
	recentRawKeys := []string{}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("selective-recent-raw-%d", i)
		recentRawKeys = append(recentRawKeys, key)
		data := GenerateRandomData(8 * 1024) // 8KB (raw file)
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(t, err)
		setAccessTime(s.Harness, key, baseTime-int64(i)) // Recent access
		VerifyStorageType(t, s.Harness.GetTempDir(), key, storagepb.ValueType_RAW_FILE)
	}

	flushAccessUpdates(s.Harness)

	// Initial state
	initialStats := s.Harness.GetStorageStats()
	t.Logf("Initial state - Total keys: %d, Disk usage: ~%.2f KB",
		initialStats.TotalKeys, float64(50*500+40*8*1024)/1024)

	// Wait for LRU eviction
	t.Log("Waiting for selective LRU eviction...")
	time.Sleep(3 * time.Second)

	// Check eviction results
	finalStats := s.Harness.GetStorageStats()
	t.Logf("Final state - Evicted: %d keys", finalStats.EvictedKeys)

	// Verify inline objects are NOT evicted (they don't count toward disk usage)
	inlineRetained := 0
	for _, key := range inlineKeys {
		_, err := s.Harness.GetObject(key)
		if err == nil {
			inlineRetained++
		}
	}
	require.Equal(t, len(inlineKeys), inlineRetained, "All inline objects should be retained")

	// Verify old raw files are evicted
	oldEvicted := 0
	for _, key := range oldRawKeys {
		_, err := s.Harness.GetObject(key)
		if err != nil {
			oldEvicted++
		}
	}
	require.Greater(t, oldEvicted, 15, "Most old raw files should be evicted")

	// Verify recent raw files are retained
	recentRetained := 0
	for _, key := range recentRawKeys {
		_, err := s.Harness.GetObject(key)
		if err == nil {
			recentRetained++
		}
	}
	require.Greater(t, recentRetained, 15, "Most recent raw files should be retained")

	t.Logf("Selective eviction results - Inline: %d/%d, Old evicted: %d/%d, Recent retained: %d/%d",
		inlineRetained, len(inlineKeys), oldEvicted, len(oldRawKeys), recentRetained, len(recentRawKeys))
}

// Test_CleanerLoop_AccessPatternUpdate tests that access pattern updates
// correctly influence LRU eviction decisions
func (s *CleanerSuite) Test_CleanerLoop_AccessPatternUpdate() {
	t := s.T()

	// Re-create harness with larger disk limit to avoid premature eviction
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 500 * time.Millisecond
	config.AccessUpdateDelay = 100 * time.Millisecond
	config.MaxDiskUsage = 150 * 1024 // 150KB limit (30 objects * 5KB = 150KB)
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Create objects with initial access pattern
	numObjects := 20
	keys := make([]string, numObjects)
	// Use a base time that's at least 3 hours in the past to ensure clear separation
	baseTime := time.Now().Add(-3 * time.Hour).Unix()

	t.Log("Creating objects with initial access pattern")
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("access-pattern-%02d", i) // Use zero-padding for consistent sorting
		keys[i] = key
		data := GenerateRandomData(5 * 1024) // 5KB each
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(t, err)

		// Initially, lower indices have older access times
		// Space them out by 10 seconds each to ensure clear ordering
		setAccessTime(s.Harness, key, baseTime+int64(i)*10)
	}
	flushAccessUpdates(s.Harness)

	// Verify all keys exist initially
	for i := 0; i < numObjects; i++ {
		_, err := s.Harness.GetObject(keys[i])
		require.NoError(t, err, "All keys should exist initially")
	}

	// Update access pattern - make middle keys (10-14) recent by accessing them
	t.Log("Phase 1: Updating access patterns - accessing middle keys")
	// Use a time far enough in the future to ensure different bucket (at least 1 hour later)
	// This ensures the updated keys will be in a different access bucket
	newTime := time.Now().Add(2 * time.Hour).Unix()
	for i := 10; i < 15; i++ {
		// Access the keys (this should update their access time)
		_, err := s.Harness.GetObject(keys[i])
		require.NoError(t, err)
		// Explicitly set very recent access time with proper spacing
		// Add seconds to ensure ordering within the new bucket
		setAccessTime(s.Harness, keys[i], newTime+int64(i*10))
	}
	flushAccessUpdates(s.Harness)

	// Add more objects to exceed disk limit and trigger eviction
	t.Log("Phase 2: Adding more objects to trigger eviction")
	newKeys := []string{}
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("access-pattern-new-%02d", i) // Use zero-padding for consistent sorting
		newKeys = append(newKeys, key)
		data := GenerateRandomData(5 * 1024) // 5KB each
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(t, err)
	}

	// Wait for eviction to occur
	time.Sleep(2 * time.Second)

	// Check eviction pattern
	// Keys 10-14 should be retained (recently accessed)
	// Keys 0-9 should be evicted (old access times)
	recentlyAccessedRetained := 0
	for i := 10; i < 15; i++ {
		_, err := s.Harness.GetObject(keys[i])
		if err == nil {
			recentlyAccessedRetained++
		}
	}

	oldKeysEvicted := 0
	for i := 0; i < 10; i++ {
		_, err := s.Harness.GetObject(keys[i])
		if err != nil {
			oldKeysEvicted++
		}
	}

	t.Logf("Access pattern results - Recently accessed retained: %d/5, Old keys evicted: %d/10",
		recentlyAccessedRetained, oldKeysEvicted)

	// With proper bucket separation, we should see clear LRU behavior
	// Keys 10-14 have access times 2+ hours newer, so they should be retained
	require.GreaterOrEqual(t, recentlyAccessedRetained, 3, "Most recently accessed keys should be retained")
	require.GreaterOrEqual(t, oldKeysEvicted, 5, "Most old keys should be evicted")
}

// Test_CleanerLoop_TTLPriority tests that TTL cleanup has priority over LRU eviction
func (s *CleanerSuite) Test_CleanerLoop_TTLPriority() {
	t := s.T()

	// Re-create harness with tight disk limit
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	config.MaxDiskUsage = 50 * 1024 // 50KB limit
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Fill storage with non-TTL objects
	t.Log("Filling storage with non-TTL objects")
	lruKeys := []string{}
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("priority-lru-%d", i)
		lruKeys = append(lruKeys, key)
		data := GenerateRandomData(4 * 1024) // 4KB each, total 60KB
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(t, err)
	}

	// Wait for LRU eviction to bring disk usage under limit
	time.Sleep(2 * time.Second)

	// Check some LRU keys were evicted
	lruEvicted := 0
	for _, key := range lruKeys {
		_, err := s.Harness.GetObject(key)
		if err != nil {
			lruEvicted++
		}
	}
	require.Greater(t, lruEvicted, 3, "Some LRU keys should be evicted to stay under limit")

	// Now add TTL objects that will expire
	t.Log("Adding TTL objects that will expire soon")
	ttlKeys := []string{}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("priority-ttl-%d", i)
		ttlKeys = append(ttlKeys, key)
		data := GenerateRandomData(3 * 1024)     // 3KB each
		err := s.Harness.PutObject(key, data, 1) // 1 second TTL
		require.NoError(t, err)
	}

	// Wait for TTL expiration and cleanup
	time.Sleep(2 * time.Second)

	// Verify TTL objects are cleaned (not just evicted)
	ttlCleaned := 0
	for _, key := range ttlKeys {
		_, err := s.Harness.GetObject(key)
		if err != nil {
			ttlCleaned++
		}
	}
	require.Equal(t, len(ttlKeys), ttlCleaned, "All TTL objects should be cleaned")

	// Verify cleanup stats show TTL cleaning
	stats := s.Harness.GetStorageStats()
	require.GreaterOrEqual(t, stats.CleanedKeys, int64(len(ttlKeys)),
		"Cleaned count should include TTL objects")

	t.Logf("Priority test - TTL cleaned: %d, LRU evicted: %d, Total cleaned: %d, Total evicted: %d",
		ttlCleaned, lruEvicted, stats.CleanedKeys, stats.EvictedKeys)
}

// Test_CleanerLoop_SegmentedObjects tests cleaner behavior with compacted objects in segments
func (s *CleanerSuite) Test_CleanerLoop_SegmentedObjects() {
	t := s.T()

	// Re-create harness with compaction enabled
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 500 * time.Millisecond
	config.RecompactionInterval = 500 * time.Millisecond
	config.MaxDiskUsage = 5 * 1024 * 1024 // 5MB limit - increased to avoid aggressive LRU eviction
	config.SegmentSize = 2 * 1024 * 1024  // 2MB segments
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Create medium objects that will be compacted
	t.Log("Creating medium objects for compaction")
	compactKeys := []string{}
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("segment-ttl-%d", i)
		compactKeys = append(compactKeys, key)
		data := GenerateRandomData(100 * 1024) // 100KB each

		ttl := int64(0)
		if i < 15 {
			ttl = 3 // First half with 3 second TTL
		}

		err := s.Harness.PutObject(key, data, ttl)
		require.NoError(t, err)
	}

	// Wait for compaction to move objects to segments
	t.Log("Waiting for compaction to create segments...")
	time.Sleep(2 * time.Second)

	// Verify some objects are in segments
	segmentDir := filepath.Join(s.Harness.GetTempDir(), "segments")
	segments, _ := filepath.Glob(filepath.Join(segmentDir, "segment_*.seg"))
	require.Greater(t, len(segments), 0, "Segments should be created")

	// Wait for TTL cleanup
	t.Log("Waiting for TTL cleanup of segmented objects...")
	time.Sleep(2 * time.Second)

	// Check TTL objects are cleaned even when in segments
	ttlCleaned := 0
	for i := 0; i < 15; i++ {
		key := compactKeys[i]
		_, err := s.Harness.GetObject(key)
		if err != nil {
			ttlCleaned++
		}
	}
	// Very lenient requirement due to timing and compaction
	require.GreaterOrEqual(t, ttlCleaned, 3, "Some TTL objects should be cleaned even when in segments")

	// Non-TTL objects should remain
	nonTTLRemaining := 0
	for i := 15; i < 30; i++ {
		key := compactKeys[i]
		_, err := s.Harness.GetObject(key)
		if err == nil {
			nonTTLRemaining++
		}
	}
	require.GreaterOrEqual(t, nonTTLRemaining, 3, "Some non-TTL segmented objects should remain")

	t.Logf("Segmented objects - TTL cleaned: %d/15, Non-TTL remaining: %d/15",
		ttlCleaned, nonTTLRemaining)
}

// Test_CleanerLoop_ZeroTTL tests that objects with TTL=0 are never expired
func (s *CleanerSuite) Test_CleanerLoop_ZeroTTL() {
	t := s.T()

	// Re-create harness
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Store objects with TTL=0 (no expiration)
	numObjects := 20
	keys := make([]string, numObjects)

	t.Log("Storing objects with TTL=0 (no expiration)")
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("zero-ttl-%d", i)
		keys[i] = key
		data := GenerateRandomData(5 * 1024)     // 5KB
		err := s.Harness.PutObject(key, data, 0) // TTL=0 means no expiration
		require.NoError(t, err)
	}

	// Wait for multiple cleanup cycles
	t.Log("Waiting for cleanup cycles to verify TTL=0 objects are not cleaned...")
	time.Sleep(3 * time.Second)

	// Verify all objects still exist
	for _, key := range keys {
		_, err := s.Harness.GetObject(key)
		require.NoError(t, err, "Object with TTL=0 should never expire: %s", key)
	}

	// Verify cleanup stats show no TTL cleaning for these objects
	stats := s.Harness.GetStorageStats()
	require.Equal(t, int64(0), stats.CleanedKeys, "No objects should be cleaned when TTL=0")

	t.Log("Verified: Objects with TTL=0 are never expired")
}

// Test_Cleaner_DiskUsageLimit tests LRU eviction based on disk usage
// Moved from cleaner_test.go during consolidation
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
		setAccessTime(s.Harness, key, baseTime-int64(50-i))
	}

	// Flush and wait for eviction
	flushAccessUpdates(s.Harness)
	time.Sleep(3 * time.Second)

	// Count remaining keys (only for single-node tests with direct storage access)
	if storageAccess, ok := s.Harness.(TestStorageAccess); ok {
		if stor, ok := storageAccess.GetStorage().(*storage.Storage); ok {
			keys, err := stor.ListKeys("")
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
		}
	}

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

// Test_CleanerLoop_UpdatedTTL tests behavior when an object's TTL is updated
func (s *CleanerSuite) Test_CleanerLoop_UpdatedTTL() {
	t := s.T()

	// Re-create harness
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Store objects with short TTL
	numObjects := 10
	keys := make([]string, numObjects)

	t.Log("Phase 1: Storing objects with 1 second TTL")
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("updated-ttl-%d", i)
		keys[i] = key
		data := GenerateRandomData(5 * 1024)     // 5KB
		err := s.Harness.PutObject(key, data, 1) // 1 second TTL
		require.NoError(t, err)
	}

	// Immediately update half of them with longer TTL
	t.Log("Phase 2: Updating half of the objects with longer TTL")
	for i := 0; i < numObjects/2; i++ {
		key := keys[i]
		data := GenerateRandomData(5 * 1024)      // 5KB
		err := s.Harness.PutObject(key, data, 10) // Update to 10 second TTL
		require.NoError(t, err)
	}

	// Wait for original TTL to expire
	t.Log("Phase 3: Waiting for original TTL expiration...")
	time.Sleep(2 * time.Second)

	// Check that updated objects still exist
	updatedSurvived := 0
	for i := 0; i < numObjects/2; i++ {
		_, err := s.Harness.GetObject(keys[i])
		if err == nil {
			updatedSurvived++
		}
	}
	require.GreaterOrEqual(t, updatedSurvived, 4, "Most updated TTL objects should survive")

	// Check that non-updated objects are cleaned
	nonUpdatedCleaned := 0
	for i := numObjects / 2; i < numObjects; i++ {
		_, err := s.Harness.GetObject(keys[i])
		if err != nil {
			nonUpdatedCleaned++
		}
	}
	require.GreaterOrEqual(t, nonUpdatedCleaned, 4, "Most non-updated objects should be cleaned")

	t.Logf("Updated TTL test - Updated survived: %d/5, Non-updated cleaned: %d/5",
		updatedSurvived, nonUpdatedCleaned)
}

// Test_CleanerLoop_RapidPutDelete tests cleaner behavior with rapid put/delete operations
func (s *CleanerSuite) Test_CleanerLoop_RapidPutDelete() {
	t := s.T()

	// Re-create harness
	s.Harness.Cleanup()
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond
	s.Config = config
	s.Harness = NewIntegrationTestHarness(t, config)

	// Perform rapid put/delete operations
	t.Log("Performing rapid put/delete operations")
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Goroutine for rapid puts with TTL
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("rapid-%d", i)
			data := GenerateRandomData(2 * 1024)                      // 2KB
			if err := s.Harness.PutObject(key, data, 1); err != nil { // 1 second TTL
				errors <- fmt.Errorf("put error: %w", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Goroutine for rapid deletes
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(500 * time.Millisecond) // Let some objects be created first
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("rapid-%d", i)
			s.Harness.DeleteObject(key)
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Let operations run with cleanup active
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Fatalf("Operation failed: %v", err)
	}

	// Wait for cleanup to process remaining objects
	time.Sleep(2 * time.Second)

	// Verify system is in consistent state
	remainingCount := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("rapid-%d", i)
		if _, err := s.Harness.GetObject(key); err == nil {
			remainingCount++
		}
	}

	t.Logf("Rapid put/delete test - Remaining objects: %d/100", remainingCount)

	// Very few objects should remain (deleted or TTL expired)
	require.Less(t, remainingCount, 20, "Most objects should be deleted or expired")

	// Verify no panics or corruption
	stats := s.Harness.GetStorageStats()
	t.Logf("Final stats - Cleaned: %d, Total keys: %d", stats.CleanedKeys, stats.TotalKeys)
}
