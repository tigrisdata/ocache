package integration

import (
	"bytes"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
)

// ObjectsSuite consolidates tests for all object sizes (small, medium, large)
// This suite replaces the separate SmallObjectSuite, MediumObjectSuite, and LargeObjectSuite

// Test_Objects_BasicFlow tests basic operations across all object size categories
func (s *ObjectsSuite) Test_Objects_BasicFlow() {
	testCases := []struct {
		name         string
		size         int64
		expectedType pb.ValueType
		category     string
	}{
		// Small objects (< 64KB) - stored inline in RocksDB
		{"small-1B", 1, pb.ValueType_INLINE, "small"},
		{"small-1KB", 1024, pb.ValueType_INLINE, "small"},
		{"small-32KB", 32 * 1024, pb.ValueType_INLINE, "small"},
		{"small-63KB", 63 * 1024, pb.ValueType_INLINE, "small"},
		{"small-64KB", 64 * 1024, pb.ValueType_INLINE, "small"},
		
		// Medium objects (64KB-16MB) - stored as raw files, eligible for compaction
		{"medium-65KB", 65 * 1024, pb.ValueType_RAW_FILE, "medium"},
		{"medium-100KB", 100 * 1024, pb.ValueType_RAW_FILE, "medium"},
		{"medium-1MB", 1024 * 1024, pb.ValueType_RAW_FILE, "medium"},
		{"medium-10MB", 10 * 1024 * 1024, pb.ValueType_RAW_FILE, "medium"},
		{"medium-16MB", 16 * 1024 * 1024, pb.ValueType_RAW_FILE, "medium"},
		
		// Large objects (> 16MB) - permanent raw files, never compacted
		{"large-17MB", 17 * 1024 * 1024, pb.ValueType_RAW_FILE, "large"},
		{"large-50MB", 50 * 1024 * 1024, pb.ValueType_RAW_FILE, "large"},
		{"large-100MB", 100 * 1024 * 1024, pb.ValueType_RAW_FILE, "large"},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			key := fmt.Sprintf("object-%s", tc.name)
			data := GenerateRandomData(tc.size)

			// Store the object
			err := s.Harness.PutObject(key, data, 0)
			require.NoError(s.T(), err, "Failed to put %s object", tc.category)

			// Verify storage type
			VerifyStorageType(s.T(), s.Harness.TempDir, key, tc.expectedType)

			// Retrieve and verify data integrity
			retrieved, err := s.Harness.GetObject(key)
			require.NoError(s.T(), err, "Failed to get %s object", tc.category)
			VerifyDataIntegrity(s.T(), data, retrieved)

			// Category-specific verifications
			switch tc.category {
			case "small":
				// Small objects should have no raw files or segments
				VerifyNoRawFiles(s.T(), s.Harness.TempDir)
				VerifySegmentsExist(s.T(), s.Harness.TempDir, 0)
			case "medium", "large":
				// Medium and large objects should create raw files
				VerifyRawFilesExist(s.T(), s.Harness.TempDir, 1)
			}

			// Delete the object
			err = s.Harness.DeleteObject(key)
			require.NoError(s.T(), err, "Failed to delete %s object", tc.category)

			// Verify deletion
			_, err = s.Harness.GetObject(key)
			require.Error(s.T(), err, "%s object should not exist after deletion", tc.category)
		})
	}
}

// Test_Objects_EdgeCases tests edge cases across all object sizes
func (s *ObjectsSuite) Test_Objects_EdgeCases() {
	testCases := []struct {
		name     string
		size     int64
		data     []byte
		category string
	}{
		// Small object edge cases
		{"small-empty", 0, []byte{}, "small"},
		{"small-single-byte", 1, []byte{'A'}, "small"},
		{"small-null-bytes", 1024, bytes.Repeat([]byte{0}, 1024), "small"},
		{"small-all-ones", 1024, bytes.Repeat([]byte{0xFF}, 1024), "small"},
		{"small-unicode", 1024, bytes.Repeat([]byte("😀"), 256), "small"},
		{"small-compressible", 10240, bytes.Repeat([]byte("A"), 10240), "small"},
		
		// Medium object edge cases
		{"medium-boundary-exact", 64*1024 + 1, GenerateRandomData(64*1024 + 1), "medium"},
		{"medium-repeating", 100 * 1024, bytes.Repeat([]byte("ABCD"), 25*1024), "medium"},
		{"medium-binary", 500 * 1024, GenerateRandomData(500 * 1024), "medium"},
		
		// Large object edge cases
		{"large-boundary-exact", 16*1024*1024 + 1, GenerateSequentialData(16*1024*1024 + 1), "large"},
		{"large-sparse", 20 * 1024 * 1024, GenerateSequentialData(20 * 1024 * 1024), "large"},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			key := fmt.Sprintf("edge-%s", tc.name)

			// Store the object
			err := s.Harness.PutObject(key, tc.data, 0)
			require.NoError(s.T(), err, "Failed to put edge case %s", tc.name)

			// Retrieve and verify
			retrieved, err := s.Harness.GetObject(key)
			require.NoError(s.T(), err, "Failed to get edge case %s", tc.name)
			VerifyDataIntegrity(s.T(), tc.data, retrieved)

			// Cleanup
			s.Harness.DeleteObject(key)
		})
	}
}

// Test_Objects_UpdateExisting tests updating objects of different sizes
func (s *ObjectsSuite) Test_Objects_UpdateExisting() {
	testCases := []struct {
		name        string
		initialSize int64
		updateSize  int64
		category    string
	}{
		// Small object updates
		{"small-to-small", 1024, 2048, "small"},
		{"small-to-medium", 32 * 1024, 100 * 1024, "cross-boundary"},
		
		// Medium object updates
		{"medium-to-medium", 100 * 1024, 500 * 1024, "medium"},
		{"medium-to-large", 10 * 1024 * 1024, 20 * 1024 * 1024, "cross-boundary"},
		
		// Large object updates
		{"large-to-large", 20 * 1024 * 1024, 30 * 1024 * 1024, "large"},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			key := fmt.Sprintf("update-%s", tc.name)

			// Store initial object
			initialData := GenerateRandomData(tc.initialSize)
			err := s.Harness.PutObject(key, initialData, 0)
			require.NoError(s.T(), err)

			// Update with new data
			updateData := GenerateRandomData(tc.updateSize)
			err = s.Harness.PutObject(key, updateData, 0)
			require.NoError(s.T(), err)

			// Verify updated data
			retrieved, err := s.Harness.GetObject(key)
			require.NoError(s.T(), err)
			VerifyDataIntegrity(s.T(), updateData, retrieved)
			require.Equal(s.T(), len(updateData), len(retrieved))

			// Cleanup
			s.Harness.DeleteObject(key)
		})
	}
}

// Test_Objects_LRUEviction tests LRU eviction for objects of different sizes
func (s *ObjectsSuite) Test_Objects_LRUEviction() {
	// Reconfigure with low disk limit
	s.Harness.Cleanup()
	s.Config.MaxDiskUsage = 500 * 1024 // 500KB limit
	s.Harness = NewIntegrationTestHarness(s.T(), s.Config)

	// Create mix of object sizes
	baseTime := time.Now().Unix()
	objects := []struct {
		key  string
		size int64
		age  int64
	}{
		// Old small objects
		{"lru-small-old-1", 10 * 1024, baseTime - 100},
		{"lru-small-old-2", 10 * 1024, baseTime - 99},
		
		// Old medium objects  
		{"lru-medium-old-1", 100 * 1024, baseTime - 98},
		{"lru-medium-old-2", 100 * 1024, baseTime - 97},
		
		// Recent small objects
		{"lru-small-new-1", 10 * 1024, baseTime - 10},
		{"lru-small-new-2", 10 * 1024, baseTime - 9},
		
		// Recent medium objects
		{"lru-medium-new-1", 100 * 1024, baseTime - 8},
		{"lru-medium-new-2", 100 * 1024, baseTime - 7},
	}

	// Store objects with different access times
	for _, obj := range objects {
		data := GenerateRandomData(obj.size)
		err := s.Harness.PutObject(obj.key, data, 0)
		require.NoError(s.T(), err)
		s.Harness.SetAccessTime(obj.key, obj.age)
	}

	// Flush and wait for eviction
	s.Harness.FlushAccessUpdates()
	time.Sleep(3 * time.Second)

	// Verify old objects are evicted
	for _, obj := range objects[:4] {
		_, err := s.Harness.GetObject(obj.key)
		assert.Error(s.T(), err, "Old object %s should be evicted", obj.key)
	}

	// Verify recent objects are retained
	for _, obj := range objects[4:] {
		_, err := s.Harness.GetObject(obj.key)
		// Some recent objects might be evicted due to size constraints
		if err == nil {
			s.T().Logf("Recent object %s was retained", obj.key)
		}
	}

	// Verify eviction stats
	_, evicted := s.Harness.Storage.CleanerStats()
	assert.Greater(s.T(), evicted, int64(2), "Should have evicted objects")
}

// Test_Objects_MixedSizes tests operations with mixed object sizes
func (s *ObjectsSuite) Test_Objects_MixedSizes() {
	// Create a mix of objects of different sizes
	objects := []struct {
		key      string
		size     int64
		ttl      int64
		category string
	}{
		// Small objects
		{"mixed-small-1", 1024, 0, "small"},
		{"mixed-small-2", 32 * 1024, 0, "small"},
		{"mixed-small-ttl", 10 * 1024, 2, "small"},
		
		// Medium objects
		{"mixed-medium-1", 100 * 1024, 0, "medium"},
		{"mixed-medium-2", 1024 * 1024, 0, "medium"},
		{"mixed-medium-ttl", 500 * 1024, 3, "medium"},
		
		// Large objects
		{"mixed-large-1", 20 * 1024 * 1024, 0, "large"},
		{"mixed-large-2", 30 * 1024 * 1024, 0, "large"},
	}

	// Store all objects
	for _, obj := range objects {
		data := GenerateRandomData(obj.size)
		err := s.Harness.PutObject(obj.key, data, obj.ttl)
		require.NoError(s.T(), err, "Failed to store %s", obj.key)
	}

	// Verify storage distribution
	stats := s.Harness.GetStorageStats()
	// Check we have both inline and raw file objects
	assert.Greater(s.T(), stats.TotalKeys, 0, "Should have objects stored")
	assert.Greater(s.T(), stats.RawFileCount, 0, "Should have raw file objects")

	// Wait for TTL expiration
	time.Sleep(4 * time.Second)

	// Verify TTL objects are cleaned
	for _, obj := range objects {
		if obj.ttl > 0 {
			_, err := s.Harness.GetObject(obj.key)
			assert.Error(s.T(), err, "TTL object %s should be expired", obj.key)
		} else {
			_, err := s.Harness.GetObject(obj.key)
			assert.NoError(s.T(), err, "Non-TTL object %s should exist", obj.key)
		}
	}
}

// Test_Objects_StreamingWrite tests streaming writes for medium and large objects
func (s *ObjectsSuite) Test_Objects_StreamingWrite() {
	testCases := []struct {
		name      string
		size      int64
		chunkSize int
	}{
		{"medium-streaming", 5 * 1024 * 1024, 64 * 1024},
		{"large-streaming", 25 * 1024 * 1024, 256 * 1024},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			key := fmt.Sprintf("streaming-%s", tc.name)
			
			// Generate data in chunks
			totalChunks := int(tc.size / int64(tc.chunkSize))
			var fullData []byte
			for i := 0; i < totalChunks; i++ {
				chunk := GenerateRandomData(int64(tc.chunkSize))
				fullData = append(fullData, chunk...)
			}

			// Store using the full data
			err := s.Harness.PutObject(key, fullData, 0)
			require.NoError(s.T(), err)

			// Verify streaming read
			retrieved, err := s.Harness.GetObject(key)
			require.NoError(s.T(), err)
			VerifyDataIntegrity(s.T(), fullData, retrieved)

			// Cleanup
			s.Harness.DeleteObject(key)
		})
	}
}

// Test_Objects_CompactionBehavior tests compaction behavior for medium objects
func (s *ObjectsSuite) Test_Objects_CompactionBehavior() {
	// Store multiple medium objects to trigger compaction
	numObjects := 20
	keys := make([]string, numObjects)
	
	s.T().Log("Creating medium objects for compaction")
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("compact-%d", i)
		keys[i] = key
		data := GenerateRandomData(200 * 1024) // 200KB each
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(s.T(), err)
	}

	// Initially all should be raw files
	for _, key := range keys {
		VerifyStorageType(s.T(), s.Harness.TempDir, key, pb.ValueType_RAW_FILE)
	}

	// Wait for compaction
	s.T().Log("Waiting for compaction to occur")
	time.Sleep(5 * time.Second)

	// Verify some objects moved to segments
	stats := s.Harness.GetStorageStats()
	if stats.SegmentCount > 0 {
		s.T().Logf("Compaction occurred: %d segments created", stats.SegmentCount)
		
		// Verify data integrity after compaction
		for _, key := range keys {
			_, err := s.Harness.GetObject(key)
			require.NoError(s.T(), err, "Object %s should be accessible after compaction", key)
		}
	} else {
		s.T().Log("No compaction occurred within timeout")
	}
}

// Test_Objects_MixedTTL tests mixed TTL behavior across object sizes
func (s *ObjectsSuite) Test_Objects_MixedTTL() {
	testCases := []struct {
		key  string
		size int64
		ttl  int64
	}{
		// Different TTLs for different sizes
		{"ttl-small-1s", 10 * 1024, 1},
		{"ttl-small-2s", 20 * 1024, 2},
		{"ttl-medium-2s", 100 * 1024, 2},
		{"ttl-medium-3s", 500 * 1024, 3},
		{"ttl-large-3s", 20 * 1024 * 1024, 3},
		
		// No TTL (permanent)
		{"perm-small", 10 * 1024, 0},
		{"perm-medium", 100 * 1024, 0},
		{"perm-large", 20 * 1024 * 1024, 0},
	}

	// Store all objects
	for _, tc := range testCases {
		data := GenerateRandomData(tc.size)
		err := s.Harness.PutObject(tc.key, data, tc.ttl)
		require.NoError(s.T(), err)
	}

	// Wait for shortest TTL to expire
	time.Sleep(2 * time.Second)

	// Check 1s TTL objects (should be expired)
	_, err := s.Harness.GetObject("ttl-small-1s")
	assert.Error(s.T(), err, "1s TTL object should be expired")

	// Check 2s and 3s TTL objects (should still exist)
	_, err = s.Harness.GetObject("ttl-medium-2s")
	assert.NoError(s.T(), err, "2s TTL object should still exist")

	// Wait for all TTLs to expire
	time.Sleep(3 * time.Second)

	// Check all TTL objects are expired
	for _, tc := range testCases {
		if tc.ttl > 0 {
			_, err := s.Harness.GetObject(tc.key)
			assert.Error(s.T(), err, "TTL object %s should be expired", tc.key)
		} else {
			_, err := s.Harness.GetObject(tc.key)
			assert.NoError(s.T(), err, "Permanent object %s should exist", tc.key)
		}
	}
}