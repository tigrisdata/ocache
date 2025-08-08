package integration

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
)

// Test_SmallObject_BasicFlow tests basic operations with small objects
func (s *SmallObjectSuite) Test_SmallObject_BasicFlow() {
	testCases := []struct {
		name string
		size int64
		desc string
	}{
		{"1-byte", 1, "Minimum size object"},
		{"1KB", 1024, "1KB object"},
		{"32KB", 32 * 1024, "32KB object"},
		{"63KB", 63 * 1024, "Just under threshold"},
		{"64KB-exact", 64 * 1024, "Exactly at threshold"},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			// Generate test data
			key := fmt.Sprintf("small-basic-%s", tc.name)
			data := GenerateRandomData(tc.size)

			// Store the object
			err := s.Harness.PutObject(key, data, 0)
			require.NoError(s.T(), err, "Failed to put %s", tc.desc)

			// Verify it's stored inline
			VerifyStorageType(s.T(), s.Harness.TempDir, key, pb.ValueType_INLINE)

			// Retrieve and verify data integrity
			retrieved, err := s.Harness.GetObject(key)
			require.NoError(s.T(), err, "Failed to get %s", tc.desc)
			VerifyDataIntegrity(s.T(), data, retrieved)

			// Verify no raw files or segments created
			VerifyNoRawFiles(s.T(), s.Harness.TempDir)
			VerifySegmentsExist(s.T(), s.Harness.TempDir, 0)

			// Delete the object
			err = s.Harness.DeleteObject(key)
			require.NoError(s.T(), err, "Failed to delete %s", tc.desc)

			// Verify deletion
			VerifyKeyNotExists(s.T(), s.Harness.Storage, key)
		})
	}
}

// Test_SmallObject_TTLExpiration tests TTL functionality for small objects
func (s *SmallObjectSuite) Test_SmallObject_TTLExpiration() {
	// Create objects with short TTL
	ttlObjects := GenerateObjectsWithTTL(10, 1024, 1*time.Second)

	// Create objects without TTL (permanent)
	permanentObjects := GenerateSmallObjects(5)

	// Store TTL objects
	var ttlKeys []string
	for _, obj := range ttlObjects {
		key := fmt.Sprintf("ttl-%s", obj.Key)
		ttlKeys = append(ttlKeys, key)
		err := s.Harness.PutObject(key, obj.Data, int64(obj.TTL.Seconds()))
		require.NoError(s.T(), err)
	}

	// Store permanent objects
	var permanentKeys []string
	for _, obj := range permanentObjects {
		key := fmt.Sprintf("permanent-%s", obj.Key)
		permanentKeys = append(permanentKeys, key)
		err := s.Harness.PutObject(key, obj.Data, 0)
		require.NoError(s.T(), err)
	}

	// Verify all objects exist initially
	for _, key := range ttlKeys {
		VerifyKeyExists(s.T(), s.Harness.Storage, key)
	}
	for _, key := range permanentKeys {
		VerifyKeyExists(s.T(), s.Harness.Storage, key)
	}

	// Wait for TTL expiration and cleanup
	// TTL objects have 1 second TTL, wait a bit longer for cleanup to run
	time.Sleep(4 * time.Second)

	// Verify TTL objects are cleaned up
	VerifyTTLCleanup(s.T(), s.Harness.Storage, ttlKeys, permanentKeys)

	// Check cleaner stats
	cleaned, _ := s.Harness.Storage.CleanerStats()
	assert.GreaterOrEqual(s.T(), cleaned, int64(len(ttlKeys)),
		"Should have cleaned at least %d TTL keys", len(ttlKeys))
}

// Test_SmallObject_LRUEviction tests LRU eviction for small objects
func (s *SmallObjectSuite) Test_SmallObject_LRUEviction() {
	// Configure with low max disk usage to trigger LRU
	s.Harness.Cleanup()
	s.Config.MaxDiskUsage = 50 * 1024 // 50KB limit
	s.Harness = NewIntegrationTestHarness(s.T(), s.Config)

	// Add 100 objects of 1KB each (total 100KB, exceeds 50KB limit)
	baseTime := time.Now().Unix()
	objects := GenerateSequentialKeys("lru", 1, 100, 1024)

	for i, obj := range objects {
		err := s.Harness.PutObject(obj.Key, obj.Data, 0)
		require.NoError(s.T(), err)

		// Set explicit access times (older objects have older times)
		s.Harness.SetAccessTime(obj.Key, baseTime-int64(100-i))
	}

	// Flush access updates to ensure they're written
	s.Harness.FlushAccessUpdates()

	// Wait for LRU eviction to run
	time.Sleep(3 * time.Second)

	// Verify LRU eviction occurred
	keys, err := s.Harness.Storage.ListKeys()
	require.NoError(s.T(), err)

	// Should have approximately 45-50 keys remaining (50KB / 1KB per key)
	assert.Less(s.T(), len(keys), 60, "LRU eviction should have removed old keys")
	assert.Greater(s.T(), len(keys), 30, "Should still have some keys remaining")

	// Verify the remaining keys are the most recently accessed ones
	for _, key := range keys {
		// Keys should be from the higher range (more recent access times)
		var keyNum int
		fmt.Sscanf(key, "lru-%d", &keyNum)
		assert.Greater(s.T(), keyNum, 40, "Older keys should be evicted first")
	}

	// Check eviction stats
	_, evicted := s.Harness.Storage.CleanerStats()
	assert.Greater(s.T(), evicted, int64(30), "Should have evicted at least 30 keys")
}

// Test_SmallObject_Concurrent tests concurrent operations on small objects
func (s *SmallObjectSuite) Test_SmallObject_Concurrent() {
	numGoroutines := 10
	objectsPerGoroutine := 100
	var wg sync.WaitGroup

	// Track errors
	errors := make(chan error, numGoroutines*objectsPerGoroutine)

	// Concurrent writes
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < objectsPerGoroutine; i++ {
				// Random size between 1B and 63KB
				size := int64(1 + (goroutineID*1000+i)%63000)
				key := fmt.Sprintf("concurrent-g%d-i%d", goroutineID, i)
				data := GenerateRandomData(size)

				if err := s.Harness.PutObject(key, data, 0); err != nil {
					errors <- fmt.Errorf("write failed for %s: %w", key, err)
				}
			}
		}(g)
	}

	// Wait for writes to complete
	wg.Wait()
	close(errors)

	// Check for errors
	var writeErrors []error
	for err := range errors {
		writeErrors = append(writeErrors, err)
	}
	require.Empty(s.T(), writeErrors, "Concurrent writes should not fail")

	// Concurrent reads
	readErrors := make(chan error, numGoroutines*objectsPerGoroutine)
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < objectsPerGoroutine; i++ {
				key := fmt.Sprintf("concurrent-g%d-i%d", goroutineID, i)

				if _, err := s.Harness.GetObject(key); err != nil {
					readErrors <- fmt.Errorf("read failed for %s: %w", key, err)
				}
			}
		}(g)
	}

	// Wait for reads to complete
	wg.Wait()
	close(readErrors)

	// Check for read errors
	var readErrorList []error
	for err := range readErrors {
		readErrorList = append(readErrorList, err)
	}
	require.Empty(s.T(), readErrorList, "Concurrent reads should not fail")

	// Verify all objects are stored inline
	VerifyObjectStorageDistribution(s.T(), s.Harness.TempDir,
		numGoroutines*objectsPerGoroutine, 0, 0)

	// Verify no raw files or segments
	VerifyNoRawFiles(s.T(), s.Harness.TempDir)
	VerifySegmentsExist(s.T(), s.Harness.TempDir, 0)
}

// Test_SmallObject_EdgeCases tests edge cases for small objects
func (s *SmallObjectSuite) Test_SmallObject_EdgeCases() {
	testCases := []struct {
		name string
		key  string
		data []byte
		desc string
	}{
		{
			name: "empty-value",
			key:  "empty",
			data: []byte{},
			desc: "Empty value (0 bytes)",
		},
		{
			name: "single-byte",
			key:  "single",
			data: []byte{0x42},
			desc: "Single byte value",
		},
		{
			name: "null-bytes",
			key:  "null-bytes",
			data: bytes.Repeat([]byte{0x00}, 1024),
			desc: "Data with null bytes",
		},
		{
			name: "all-ones",
			key:  "all-ones",
			data: bytes.Repeat([]byte{0xFF}, 1024),
			desc: "Data with all ones",
		},
		{
			name: "unicode-data",
			key:  "unicode",
			data: GenerateUnicodeData(1024),
			desc: "Unicode text data",
		},
		{
			name: "binary-data",
			key:  "binary",
			data: GenerateBinaryData(1024),
			desc: "Binary data with mixed patterns",
		},
		{
			name: "compressible",
			key:  "compressible",
			data: GenerateCompressibleData(10240),
			desc: "Highly compressible data",
		},
		{
			name: "max-key-length",
			key:  string(bytes.Repeat([]byte("k"), 256)), // Long key
			data: []byte("test"),
			desc: "Maximum key length",
		},
		{
			name: "special-chars-key",
			key:  "key-with-!@#$%^&*()_+-=[]{}|;:',.<>?/`~",
			data: []byte("test"),
			desc: "Key with special characters",
		},
		{
			name: "exactly-threshold",
			key:  "exactly-64kb",
			data: GenerateRandomData(64 * 1024),
			desc: "Exactly at 64KB threshold",
		},
		{
			name: "just-below-threshold",
			key:  "just-below-64kb",
			data: GenerateRandomData(64*1024 - 1),
			desc: "Just below 64KB threshold",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			// Store the object
			err := s.Harness.PutObject(tc.key, tc.data, 0)
			require.NoError(s.T(), err, "Failed to put %s", tc.desc)

			// Verify it's stored inline
			if len(tc.data) <= int(s.Config.InlineThreshold) {
				VerifyStorageType(s.T(), s.Harness.TempDir, tc.key, pb.ValueType_INLINE)
			}

			// Retrieve and verify
			retrieved, err := s.Harness.GetObject(tc.key)
			require.NoError(s.T(), err, "Failed to get %s", tc.desc)

			// Verify data integrity
			assert.Equal(s.T(), len(tc.data), len(retrieved),
				"Size mismatch for %s", tc.desc)
			assert.Equal(s.T(), tc.data, retrieved,
				"Data mismatch for %s", tc.desc)

			// Clean up
			err = s.Harness.DeleteObject(tc.key)
			require.NoError(s.T(), err, "Failed to delete %s", tc.desc)
		})
	}
}

// Test_SmallObject_UpdateExisting tests updating existing small objects
func (s *SmallObjectSuite) Test_SmallObject_UpdateExisting() {
	key := "update-test"

	// Store initial data
	initialData := GenerateRandomData(1024)
	err := s.Harness.PutObject(key, initialData, 0)
	require.NoError(s.T(), err)

	// Verify initial data
	retrieved, err := s.Harness.GetObject(key)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), initialData, retrieved)

	// Update with new data (different size)
	updatedData := GenerateRandomData(2048)
	err = s.Harness.PutObject(key, updatedData, 0)
	require.NoError(s.T(), err)

	// Verify updated data
	retrieved, err = s.Harness.GetObject(key)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), updatedData, retrieved)

	// Update with larger data still within inline threshold
	largerData := GenerateRandomData(32 * 1024)
	err = s.Harness.PutObject(key, largerData, 0)
	require.NoError(s.T(), err)

	// Verify it's still inline
	VerifyStorageType(s.T(), s.Harness.TempDir, key, pb.ValueType_INLINE)

	retrieved, err = s.Harness.GetObject(key)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), largerData, retrieved)
}

// Test_SmallObject_MixedTTL tests objects with mixed TTL values
func (s *SmallObjectSuite) Test_SmallObject_MixedTTL() {
	// Objects with different TTLs
	ttlGroups := []struct {
		prefix string
		count  int
		ttl    time.Duration
	}{
		{"ttl-1s", 5, 1 * time.Second},
		{"ttl-2s", 5, 2 * time.Second},
		{"ttl-5s", 5, 5 * time.Second},
		{"no-ttl", 5, 0}, // No expiration
	}

	allKeys := make(map[string]time.Duration)

	// Store objects with different TTLs
	for _, group := range ttlGroups {
		for i := 0; i < group.count; i++ {
			key := fmt.Sprintf("%s-%d", group.prefix, i)
			data := GenerateRandomData(1024)

			ttlSeconds := int64(0)
			if group.ttl > 0 {
				ttlSeconds = int64(group.ttl.Seconds())
			}

			err := s.Harness.PutObject(key, data, ttlSeconds)
			require.NoError(s.T(), err)
			allKeys[key] = group.ttl
		}
	}

	// Wait and check TTL expiration at different intervals
	checkIntervals := []time.Duration{
		1500 * time.Millisecond, // After 1.5s: 1s TTL expired
		1000 * time.Millisecond, // After 2.5s: 2s TTL expired
		3000 * time.Millisecond, // After 5.5s: 5s TTL expired
	}

	for i, interval := range checkIntervals {
		time.Sleep(interval)

		// Check which keys should be expired
		keys, err := s.Harness.Storage.ListKeys()
		require.NoError(s.T(), err)

		for key, ttl := range allKeys {
			totalElapsed := time.Duration(0)
			for j := 0; j <= i; j++ {
				totalElapsed += checkIntervals[j]
			}

			if ttl > 0 && totalElapsed > ttl {
				// Should be expired
				found := false
				for _, k := range keys {
					if k == key {
						found = true
						break
					}
				}
				assert.False(s.T(), found, "Key %s should be expired after %v", key, totalElapsed)
			}
		}
	}

	// Final check: only no-TTL objects should remain
	keys, err := s.Harness.Storage.ListKeys()
	require.NoError(s.T(), err)

	remainingCount := 0
	for _, key := range keys {
		if ttl, exists := allKeys[key]; exists && ttl == 0 {
			remainingCount++
		}
	}
	assert.Equal(s.T(), 5, remainingCount, "Only no-TTL objects should remain")
}
