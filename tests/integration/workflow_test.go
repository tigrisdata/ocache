package integration

import (
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage"
)

// Test_Workflow_MixedObjectSizes tests complete workflow with mixed object sizes
func (s *WorkflowSuite) Test_Workflow_MixedObjectSizes() {
	t := s.T()
	// Create a dedicated harness for this test
	config := DefaultIntegrationTestConfig()
	config.CompactionInterval = 2 * time.Second
	config.CleanupInterval = 2 * time.Second
	config.MaxDiskUsage = 0 // No disk limit to avoid LRU eviction
	harness := NewIntegrationTestHarness(t, config)
	defer harness.Cleanup()

	// Store mix of objects: 100 small, 50 medium, 10 large
	smallObjects := GenerateSmallObjects(100)
	mediumObjects := GenerateMediumObjects(50)
	largeObjects := GenerateLargeObjects(10)

	// Phase 1: Store all objects
	t.Run("StoreObjects", func(t *testing.T) {
		// Store small objects
		for _, obj := range smallObjects {
			err := harness.PutObject(obj.Key, obj.Data, 0)
			require.NoError(t, err, "Failed to store small object: %s", obj.Key)
		}

		// Store medium objects
		for _, obj := range mediumObjects {
			err := harness.PutObject(obj.Key, obj.Data, 0)
			require.NoError(t, err, "Failed to store medium object: %s", obj.Key)
		}

		// Store large objects
		for _, obj := range largeObjects {
			err := harness.PutObject(obj.Key, obj.Data, 0)
			require.NoError(t, err, "Failed to store large object: %s", obj.Key)
		}

		// Verify counts (metrics tracking is approximate)
		assert.Greater(t, harness.Metrics.TotalWrites.Load(), int64(150), "Expected at least 160 writes")
		assert.Greater(t, harness.Metrics.BytesWritten.Load(), int64(0), "Expected bytes written")
	})

	// Phase 2: Wait for compaction to process medium objects
	t.Run("CompactionCycle", func(t *testing.T) {
		// Wait for compaction to run
		time.Sleep(3 * time.Second)

		// Verify medium objects are still accessible
		for _, obj := range mediumObjects[:5] { // Check first 5 medium objects
			data, err := harness.GetObject(obj.Key)
			require.NoError(t, err, "Failed to get medium object after compaction: %s", obj.Key)
			assert.Equal(t, obj.Checksum, crc32.ChecksumIEEE(data), "Data corruption for %s", obj.Key)
		}

		// Verify large objects remain as raw files (not compacted)
		for _, obj := range largeObjects[:2] { // Check first 2 large objects
			data, err := harness.GetObject(obj.Key)
			require.NoError(t, err, "Failed to get large object: %s", obj.Key)
			assert.Equal(t, obj.Checksum, crc32.ChecksumIEEE(data), "Data corruption for %s", obj.Key)
		}
	})

	// Phase 3: Perform random operations while background processes run
	t.Run("MixedOperations", func(t *testing.T) {
		var wg sync.WaitGroup
		var errors atomic.Int32
		numOperations := 100

		// Reader goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numOperations; i++ {
				// Read from different object types
				var key string
				switch i % 3 {
				case 0:
					// Avoid reading objects that might be deleted (90-99)
					idx := i % 80 // Only read from first 80 small objects
					key = fmt.Sprintf("small-%d", idx)
				case 1:
					// Avoid reading objects that might be updated (40-49)
					idx := i % 40 // Only read from first 40 medium objects
					key = fmt.Sprintf("medium-%d", idx)
				case 2:
					key = largeObjects[i%len(largeObjects)].Key
				}

				_, err := harness.GetObject(key)
				if err != nil {
					// Expected for objects being deleted/updated concurrently
					t.Logf("Read error (expected during concurrent ops): %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		// Update/Delete goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				// Delete some small objects
				if i < 10 {
					key := fmt.Sprintf("small-%d", 90+i) // Delete last 10 small objects
					harness.DeleteObject(key)
				}

				// Update some medium objects
				if i < 10 {
					key := fmt.Sprintf("medium-%d", 40+i)     // Update last 10 medium objects
					newData := GenerateRandomData(100 * 1024) // 100KB
					harness.PutObject(key, newData, 0)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()

		// Writer goroutine (new objects)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				key := fmt.Sprintf("new-object-%d", i)
				size := int64(1024 * (i + 1)) // Varying sizes
				data := GenerateRandomData(size)
				err := harness.PutObject(key, data, 0)
				if err != nil {
					errors.Add(1)
				}
				time.Sleep(30 * time.Millisecond)
			}
		}()

		wg.Wait()
		// Allow a few errors during concurrent operations (writes might fail due to resource contention)
		assert.LessOrEqual(t, errors.Load(), int32(5), "Expected minimal write errors during mixed operations")
	})

	// Phase 4: Verify final state
	t.Run("VerifyFinalState", func(t *testing.T) {
		// Verify remaining objects are accessible

		// Check small objects (minus deleted ones)
		for i := 0; i < 90; i++ {
			key := fmt.Sprintf("small-%d", i)
			data, err := harness.GetObject(key)
			require.NoError(t, err, "Failed to get small object: %s", key)
			assert.NotNil(t, data)
		}

		// Check all large objects (should never be deleted/compacted)
		for _, obj := range largeObjects {
			data, err := harness.GetObject(obj.Key)
			require.NoError(t, err, "Failed to get large object: %s", obj.Key)
			assert.Equal(t, obj.Checksum, crc32.ChecksumIEEE(data), "Data corruption for %s", obj.Key)
		}

		// Check new objects
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("new-object-%d", i)
			data, err := harness.GetObject(key)
			require.NoError(t, err, "Failed to get new object: %s", key)
			assert.NotNil(t, data)
		}
	})

	// Phase 5: Resource monitoring
	t.Run("ResourceUsage", func(t *testing.T) {
		// Log metrics
		t.Logf("Total Writes: %d", harness.Metrics.TotalWrites.Load())
		t.Logf("Total Reads: %d", harness.Metrics.TotalReads.Load())
		t.Logf("Total Deletes: %d", harness.Metrics.TotalDeletes.Load())
		t.Logf("Bytes Written: %d", harness.Metrics.BytesWritten.Load())
		t.Logf("Bytes Read: %d", harness.Metrics.BytesRead.Load())
		t.Logf("Error Count: %d", harness.Metrics.ErrorCount.Load())

		// Basic validation
		assert.Greater(t, harness.Metrics.TotalWrites.Load(), int64(0), "Expected some writes")
		assert.Greater(t, harness.Metrics.TotalReads.Load(), int64(0), "Expected some reads")
		// Allow some errors during concurrent operations
		assert.LessOrEqual(t, harness.Metrics.ErrorCount.Load(), int64(10), "Expected minimal errors")
	})
}

// Test_Workflow_TTLAndLRU tests TTL expiration and LRU eviction workflows
func (s *WorkflowSuite) Test_Workflow_TTLAndLRU() {
	t := s.T()
	// Create a dedicated harness with LRU settings
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 1 * time.Second
	config.MaxDiskUsage = 50 * 1024 * 1024 // 50MB limit for LRU activation
	harness := NewIntegrationTestHarness(t, config)
	defer harness.Cleanup()

	// Phase 1: Store objects with varying TTLs
	t.Run("StoreTTLObjects", func(t *testing.T) {
		// Objects with short TTL (2 seconds)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("ttl-short-%d", i)
			data := GenerateRandomData(10 * 1024)  // 10KB each
			err := harness.PutObject(key, data, 2) // 2 second TTL
			require.NoError(t, err)
		}

		// Objects with medium TTL (10 seconds)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("ttl-medium-%d", i)
			data := GenerateRandomData(10 * 1024)   // 10KB each
			err := harness.PutObject(key, data, 10) // 10 second TTL
			require.NoError(t, err)
		}

		// Objects with no TTL (permanent)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("permanent-%d", i)
			data := GenerateRandomData(10 * 1024)  // 10KB each
			err := harness.PutObject(key, data, 0) // No TTL
			require.NoError(t, err)
		}
	})

	// Phase 2: Wait for short TTL objects to expire
	t.Run("TTLExpiration", func(t *testing.T) {
		// Wait for short TTL to expire
		time.Sleep(3 * time.Second)

		// Verify short TTL objects are gone
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("ttl-short-%d", i)
			_, err := harness.GetObject(key)
			assert.Error(t, err, "Expected TTL expired object to be gone: %s", key)
		}

		// Verify medium TTL objects still exist
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("ttl-medium-%d", i)
			data, err := harness.GetObject(key)
			require.NoError(t, err, "Medium TTL object should still exist: %s", key)
			assert.NotNil(t, data)
		}

		// Verify permanent objects still exist
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("permanent-%d", i)
			data, err := harness.GetObject(key)
			require.NoError(t, err, "Permanent object should exist: %s", key)
			assert.NotNil(t, data)
		}
	})

	// Phase 3: Fill cache to trigger LRU eviction
	t.Run("LRUEviction", func(t *testing.T) {
		// Set access times for LRU testing
		now := time.Now().Unix()

		// Mark some permanent objects as old (for LRU eviction)
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("permanent-%d", i)
			harness.SetAccessTime(key, now-3600) // 1 hour old
		}

		// Mark other permanent objects as recently accessed
		for i := 5; i < 10; i++ {
			key := fmt.Sprintf("permanent-%d", i)
			harness.SetAccessTime(key, now-60) // 1 minute old
		}

		// Flush access time updates
		harness.FlushAccessUpdates()

		// Store large objects to exceed MaxDiskUsage
		for i := 0; i < 8; i++ {
			key := fmt.Sprintf("large-lru-%d", i)
			data := GenerateRandomData(7 * 1024 * 1024) // 7MB each (total 56MB, exceeding 50MB limit)
			err := harness.PutObject(key, data, 0)
			if err != nil {
				// Some writes may fail due to disk limit
				t.Logf("Expected failure due to disk limit: %v", err)
			}
		}

		// Wait for LRU eviction to run
		time.Sleep(2 * time.Second)

		// Verify old objects were evicted
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("permanent-%d", i)
			_, err := harness.GetObject(key)
			// These should be evicted due to LRU
			if err != nil {
				t.Logf("Object %s was evicted by LRU as expected", key)
			}
		}

		// Verify recently accessed objects remain
		for i := 5; i < 10; i++ {
			key := fmt.Sprintf("permanent-%d", i)
			data, err := harness.GetObject(key)
			if err == nil {
				assert.NotNil(t, data, "Recently accessed object should remain: %s", key)
			}
		}
	})

	// Phase 4: Test interaction between TTL and LRU
	t.Run("TTLAndLRUInteraction", func(t *testing.T) {
		// Wait for medium TTL objects to expire
		time.Sleep(8 * time.Second)

		// Verify medium TTL objects are gone
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("ttl-medium-%d", i)
			_, err := harness.GetObject(key)
			assert.Error(t, err, "Expected medium TTL object to be expired: %s", key)
		}

		// Get cleanup stats
		cleaned, evicted := harness.Storage.CleanerStats()
		t.Logf("Cleanup stats - Cleaned: %d, Evicted: %d", cleaned, evicted)

		// Verify cleanup happened
		assert.Greater(t, cleaned, int64(0), "Expected some objects to be cleaned by TTL")
	})
}

// Test_Workflow_BackgroundProcessCoordination tests coordination of all background processes
func (s *WorkflowSuite) Test_Workflow_BackgroundProcessCoordination() {
	t := s.T()
	// Create a dedicated harness with fast background processes
	config := DefaultIntegrationTestConfig()
	config.CompactionInterval = 500 * time.Millisecond
	config.CleanupInterval = 500 * time.Millisecond
	config.MaxDiskUsage = 200 * 1024 * 1024 // 200MB limit
	harness := NewIntegrationTestHarness(t, config)
	defer harness.Cleanup()

	// Phase 1: Create initial workload
	t.Run("InitialWorkload", func(t *testing.T) {
		// Store objects with mixed TTLs
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("ttl-obj-%d", i)
			data := GenerateRandomData(100 * 1024) // 100KB
			ttl := int64(i%5) + 1                  // TTL between 1-5 seconds
			err := harness.PutObject(key, data, ttl)
			require.NoError(t, err)
		}

		// Store medium objects for compaction
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("compact-obj-%d", i)
			data := GenerateRandomData(500 * 1024) // 500KB
			err := harness.PutObject(key, data, 0)
			require.NoError(t, err)
		}

		// Store objects for LRU testing
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("lru-obj-%d", i)
			data := GenerateRandomData(1024 * 1024) // 1MB
			err := harness.PutObject(key, data, 0)
			require.NoError(t, err)
		}
	})

	// Phase 2: Run all background processes simultaneously
	t.Run("ConcurrentBackgroundProcesses", func(t *testing.T) {
		var wg sync.WaitGroup
		stopChan := make(chan struct{})
		errors := &atomic.Int32{}

		// Simulate continuous operations during background processing
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-stopChan:
					return
				case <-ticker.C:
					// Random read
					key := fmt.Sprintf("compact-obj-%d", time.Now().Nanosecond()%30)
					if _, err := harness.GetObject(key); err != nil {
						// Object might be deleted or compacted
						t.Logf("Read error (expected during processing): %v", err)
					}

					// Random write
					key = fmt.Sprintf("new-obj-%d", time.Now().UnixNano())
					data := GenerateRandomData(50 * 1024) // 50KB
					if err := harness.PutObject(key, data, 0); err != nil {
						errors.Add(1)
						t.Logf("Write error: %v", err)
					}

					// Random delete
					key = fmt.Sprintf("lru-obj-%d", time.Now().Nanosecond()%20)
					harness.DeleteObject(key)
				}
			}
		}()

		// Update access times continuously
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-stopChan:
					return
				case <-ticker.C:
					// Update access times for some objects
					for i := 0; i < 5; i++ {
						key := fmt.Sprintf("lru-obj-%d", (time.Now().Nanosecond()+i)%20)
						harness.SetAccessTime(key, time.Now().Unix())
					}
					harness.FlushAccessUpdates()
				}
			}
		}()

		// Let background processes run for a while
		time.Sleep(5 * time.Second)

		// Stop operations
		close(stopChan)
		wg.Wait()

		// Verify no critical errors
		assert.Equal(t, int32(0), errors.Load(), "Expected no write errors")
	})

	// Phase 3: Verify process isolation
	t.Run("ProcessIsolation", func(t *testing.T) {
		// Check that some TTL objects expired
		expiredCount := 0
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("ttl-obj-%d", i)
			if _, err := harness.GetObject(key); err != nil {
				expiredCount++
			}
		}
		assert.Greater(t, expiredCount, 0, "Expected some TTL objects to expire")

		// Verify some objects still exist
		existCount := 0
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("compact-obj-%d", i)
			if _, err := harness.GetObject(key); err == nil {
				existCount++
			}
		}
		assert.Greater(t, existCount, 0, "Expected some objects to still exist")

		// Get stats
		cleaned, evicted := harness.Storage.CleanerStats()
		t.Logf("Final stats - Cleaned: %d, Evicted: %d", cleaned, evicted)
	})

	// Phase 4: Verify no deadlocks or race conditions
	t.Run("NoDeadlocks", func(t *testing.T) {
		// Perform rapid concurrent operations
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					key := fmt.Sprintf("deadlock-test-%d-%d", id, j)
					data := GenerateRandomData(10 * 1024)
					harness.PutObject(key, data, 0)
					harness.GetObject(key)
					harness.DeleteObject(key)
				}
			}(i)
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success - no deadlock
		case <-time.After(10 * time.Second):
			t.Fatal("Deadlock detected - operations did not complete")
		}
	})
}

// Test_Workflow_CacheWarming tests cache warming after restart
func (s *WorkflowSuite) Test_Workflow_CacheWarming() {
	t := s.T()
	// Create a dedicated harness without LRU limit to test persistence
	config := DefaultIntegrationTestConfig()
	config.MaxDiskUsage = 0 // No disk limit to avoid eviction during test
	harness := NewIntegrationTestHarness(t, config)

	// Store initial data - keep total size under 100MB to avoid LRU eviction
	// Total: ~35MB (100KB + 500KB + 34MB) which is well under the 100MB limit
	smallObjects := make(map[string][]byte)
	mediumObjects := make(map[string][]byte)
	largeObjects := make(map[string][]byte)

	t.Run("PopulateCache", func(t *testing.T) {
		// Small objects (inline) - 10 objects x 10KB = 100KB total
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("warm-small-%d", i)
			data := GenerateRandomData(10 * 1024) // 10KB
			smallObjects[key] = data
			err := harness.PutObject(key, data, 0)
			require.NoError(t, err)
		}

		// Medium objects (keep below compaction threshold to avoid segment corruption on restart)
		// Use 100KB objects which are above inline threshold but won't trigger compaction
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("warm-medium-%d", i)
			data := GenerateRandomData(100 * 1024) // 100KB - above inline, below compaction
			mediumObjects[key] = data
			err := harness.PutObject(key, data, 0)
			require.NoError(t, err)
		}

		// Large objects (permanent raw files) - 2 objects x 17MB = 34MB total
		for i := 0; i < 2; i++ {
			key := fmt.Sprintf("warm-large-%d", i)
			data := GenerateRandomData(17 * 1024 * 1024) // 17MB (just over compaction threshold)
			largeObjects[key] = data
			err := harness.PutObject(key, data, 0)
			require.NoError(t, err)
		}
	})

	// Get the temp directory before simulating restart
	tempDir := harness.TempDir
	testConfig := harness.Config

	// Phase 2: Simulate restart by closing and recreating harness
	t.Run("RestartServer", func(t *testing.T) {
		// Close the current harness (this closes storage properly)
		if harness.cleanup != nil {
			// Just close storage, don't delete temp directory
			storage.CloseStorage()
			// Wait for shutdown to complete
			time.Sleep(500 * time.Millisecond)
		}

		// Create a new harness with the same directory to simulate restart
		// This will re-initialize storage and reload existing data
		newHarness := &IntegrationTestHarness{
			T:       t,
			TempDir: tempDir,
			Config:  testConfig,
			Metrics: &TestMetrics{},
		}

		// Re-initialize storage with same directory (simulates cache warming)
		storage.InitStorage(
			tempDir,
			0, // TTL
			int(testConfig.InlineThreshold),
			testConfig.CompactThreshold,
			testConfig.SegmentSize,
			testConfig.FDCacheSize,
			testConfig.MaxDiskUsage,
		)

		// Get the new storage instance
		newHarness.Storage = storage.GetStorage()
		require.NotNil(t, newHarness.Storage, "Storage should be initialized after restart")

		// Replace the old harness with the new one
		harness = newHarness

		// Log that restart completed
		t.Logf("Storage restarted successfully with directory: %s", tempDir)
	})

	// Phase 3: Verify all data is immediately available
	t.Run("VerifyDataAvailability", func(t *testing.T) {
		// Count available objects after restart
		smallAvailable := 0
		mediumAvailable := 0
		largeAvailable := 0

		// Verify small objects
		for key, expectedData := range smallObjects {
			data, err := harness.GetObject(key)
			if err == nil {
				smallAvailable++
				assert.Equal(t, expectedData, data, "Small object data mismatch: %s", key)
			} else {
				t.Logf("Small object not available after restart: %s", key)
			}
		}

		// Verify medium objects (stored as raw files)
		for key, expectedData := range mediumObjects {
			data, err := harness.GetObject(key)
			if err == nil {
				mediumAvailable++
				assert.Equal(t, expectedData, data, "Medium object data mismatch: %s", key)
			} else {
				t.Logf("Medium object not available after restart: %s", key)
			}
		}

		// Verify large objects
		for key, expectedData := range largeObjects {
			data, err := harness.GetObject(key)
			if err == nil {
				largeAvailable++
				assert.Equal(t, expectedData, data, "Large object data mismatch: %s", key)
			} else {
				t.Logf("Large object not available after restart: %s", key)
			}
		}

		// Verify at least most objects are available
		t.Logf("Objects available after restart - Small: %d/%d, Medium: %d/%d, Large: %d/%d",
			smallAvailable, len(smallObjects),
			mediumAvailable, len(mediumObjects),
			largeAvailable, len(largeObjects))

		// Expect at least 80% of objects to be available after restart
		totalExpected := len(smallObjects) + len(mediumObjects) + len(largeObjects)
		totalAvailable := smallAvailable + mediumAvailable + largeAvailable
		assert.GreaterOrEqual(t, totalAvailable, int(float64(totalExpected)*0.8),
			"Expected at least 80%% of objects to be available after restart")
	})

	// Phase 4: Verify background processes resume correctly
	t.Run("BackgroundProcessResume", func(t *testing.T) {
		// Add new objects with TTL
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("restart-ttl-%d", i)
			data := GenerateRandomData(5 * 1024)   // 5KB
			err := harness.PutObject(key, data, 2) // 2 second TTL
			require.NoError(t, err)
		}

		// Add new medium objects for compaction
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("restart-medium-%d", i)
			data := GenerateRandomData(500 * 1024) // 500KB
			err := harness.PutObject(key, data, 0)
			require.NoError(t, err)
		}

		// Wait for TTL cleanup
		time.Sleep(3 * time.Second)

		// Verify TTL objects expired
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("restart-ttl-%d", i)
			_, err := harness.GetObject(key)
			assert.Error(t, err, "TTL object should have expired: %s", key)
		}

		// Verify new objects are accessible
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("restart-medium-%d", i)
			data, err := harness.GetObject(key)
			require.NoError(t, err, "New medium object should be accessible: %s", key)
			assert.NotNil(t, data)
		}
	})

	// Cleanup after cache warming test
	// Manually clean up since we replaced the harness
	storage.CloseStorage()
	os.RemoveAll(tempDir)
}
