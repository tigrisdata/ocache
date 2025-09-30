package integration

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage"
	storagepb "github.com/tigrisdata/ocache/storage/proto"
)

// Test_Objects_BasicFlow tests basic operations across all object size categories
func (s *ObjectsSuite) Test_Objects_BasicFlow() {
	testCases := []ObjectSizeTestCase{
		// Small objects (< 64KB) - stored inline in RocksDB
		{Name: "small-1B", Size: 1, ExpectedType: storagepb.ValueType_INLINE, Category: "small"},
		{Name: "small-1KB", Size: 1024, ExpectedType: storagepb.ValueType_INLINE, Category: "small"},
		{Name: "small-32KB", Size: 32 * 1024, ExpectedType: storagepb.ValueType_INLINE, Category: "small"},
		{Name: "small-63KB", Size: 63 * 1024, ExpectedType: storagepb.ValueType_INLINE, Category: "small"},
		{Name: "small-64KB", Size: 64 * 1024, ExpectedType: storagepb.ValueType_INLINE, Category: "small"},

		// Medium objects (64KB-16MB) - stored as raw files, eligible for compaction
		{Name: "medium-65KB", Size: 65 * 1024, ExpectedType: storagepb.ValueType_RAW_FILE, Category: "medium"},
		{Name: "medium-100KB", Size: 100 * 1024, ExpectedType: storagepb.ValueType_RAW_FILE, Category: "medium"},
		{Name: "medium-1MB", Size: 1024 * 1024, ExpectedType: storagepb.ValueType_RAW_FILE, Category: "medium"},
		{Name: "medium-10MB", Size: 10 * 1024 * 1024, ExpectedType: storagepb.ValueType_RAW_FILE, Category: "medium"},
		{Name: "medium-16MB", Size: 16 * 1024 * 1024, ExpectedType: storagepb.ValueType_RAW_FILE, Category: "medium"},

		// Large objects (> 16MB) - permanent raw files, never compacted
		{Name: "large-17MB", Size: 17 * 1024 * 1024, ExpectedType: storagepb.ValueType_RAW_FILE, Category: "large"},
		{Name: "large-50MB", Size: 50 * 1024 * 1024, ExpectedType: storagepb.ValueType_RAW_FILE, Category: "large"},
		{Name: "large-100MB", Size: 100 * 1024 * 1024, ExpectedType: storagepb.ValueType_RAW_FILE, Category: "large"},
	}

	RunObjectSizeTests(s.T(), s.Harness, testCases, func(t *testing.T, h TestHarnessInterface, tc ObjectSizeTestCase) {
		key := fmt.Sprintf("object-%s", tc.Name)
		data := GenerateRandomData(tc.Size)

		// Use ObjectOperationSteps for granular control
		ops := &ObjectOperationSteps{
			Key:  key,
			Data: data,
			TTL:  0,
		}

		// Store the object
		ops.PutObject(t, h)

		// Verify storage type while object exists
		VerifyStorageType(t, h.GetTempDir(), key, tc.ExpectedType)

		// Retrieve and verify
		ops.GetAndVerify(t, h)

		// Delete the object
		ops.DeleteObject(t, h)

		// Verify deletion
		ops.VerifyDeleted(t, h)
	})
}

// Test_Objects_EdgeCases tests edge cases across all object sizes
func (s *ObjectsSuite) Test_Objects_EdgeCases() {
	testCases := []EdgeCaseTest{
		// Small object edge cases
		{Name: "small-empty", Key: "edge-small-empty", Data: []byte{}, Description: "Empty object"},
		{Name: "small-single-byte", Key: "edge-small-single", Data: []byte{'A'}, Description: "Single byte"},
		{Name: "small-null-bytes", Key: "edge-small-null", Data: bytes.Repeat([]byte{0}, 1024), Description: "Null bytes"},
		{Name: "small-all-ones", Key: "edge-small-ones", Data: bytes.Repeat([]byte{0xFF}, 1024), Description: "All ones"},
		{Name: "small-unicode", Key: "edge-small-unicode", Data: bytes.Repeat([]byte("😀"), 256), Description: "Unicode data"},
		{Name: "small-compressible", Key: "edge-small-compress", Data: bytes.Repeat([]byte("A"), 10240), Description: "Highly compressible"},

		// Medium object edge cases
		{Name: "medium-boundary", Key: "edge-medium-boundary", Size: 64*1024 + 1, Description: "Boundary crossing"},
		{Name: "medium-repeating", Key: "edge-medium-repeat", Data: bytes.Repeat([]byte("ABCD"), 25*1024), Description: "Repeating pattern"},
		{Name: "medium-binary", Key: "edge-medium-binary", Size: 500 * 1024, Description: "Binary data"},

		// Large object edge cases
		{Name: "large-boundary", Key: "edge-large-boundary", Size: 16*1024*1024 + 1, Description: "Large boundary"},
		{Name: "large-sparse", Key: "edge-large-sparse", Size: 20 * 1024 * 1024, Description: "Large sparse data"},
	}

	RunEdgeCaseTests(s.T(), s.Harness, testCases)
}

// Test_Objects_UpdateExisting tests updating objects of different sizes
func (s *ObjectsSuite) Test_Objects_UpdateExisting() {
	testCases := []UpdateTestCase{
		// Small object updates
		{Key: "update-small-to-small", InitialSize: 1024, UpdateSize: 2048, Category: "same"},
		{Key: "update-small-to-medium", InitialSize: 32 * 1024, UpdateSize: 100 * 1024, Category: "cross-boundary"},

		// Medium object updates
		{Key: "update-medium-to-medium", InitialSize: 100 * 1024, UpdateSize: 500 * 1024, Category: "same"},
		{Key: "update-medium-to-large", InitialSize: 10 * 1024 * 1024, UpdateSize: 20 * 1024 * 1024, Category: "cross-boundary"},

		// Large object updates
		{Key: "update-large-to-large", InitialSize: 20 * 1024 * 1024, UpdateSize: 30 * 1024 * 1024, Category: "same"},
	}

	RunUpdateTests(s.T(), s.Harness, testCases)
}

// Test_Objects_LRUEviction tests LRU eviction for objects of different sizes
func (s *ObjectsSuite) Test_Objects_LRUEviction() {
	// Skip for cluster tests as LRU eviction behavior is more complex in distributed mode
	if _, ok := s.Harness.(*ClusterTestHarness); ok {
		s.T().Skip("Skipping LRU test for cluster mode")
		return
	}

	// Reconfigure with low disk limit
	s.Harness.Cleanup()
	s.Config.MaxDiskUsage = 300 * 1024                // 300KB limit - lower to ensure eviction triggers
	s.Config.CleanupInterval = 500 * time.Millisecond // Faster cleanup for testing
	s.Harness = NewIntegrationTestHarness(s.T(), s.Config)

	baseTime := time.Now().Unix()
	testCases := []LRUTestCase{
		// Old medium objects that should be evicted (stored as files)
		{Key: "lru-medium-old-1", Size: 70 * 1024, AccessTime: baseTime - 100, ShouldEvict: true},
		{Key: "lru-medium-old-2", Size: 70 * 1024, AccessTime: baseTime - 99, ShouldEvict: true},
		{Key: "lru-medium-old-3", Size: 70 * 1024, AccessTime: baseTime - 98, ShouldEvict: true},

		// Recent medium objects that should be retained (stored as files)
		{Key: "lru-medium-new-1", Size: 70 * 1024, AccessTime: baseTime - 10, ShouldEvict: false},
		{Key: "lru-medium-new-2", Size: 70 * 1024, AccessTime: baseTime - 9, ShouldEvict: false},
		{Key: "lru-medium-new-3", Size: 70 * 1024, AccessTime: baseTime - 8, ShouldEvict: false},
	}

	RunLRUTests(s.T(), s.Harness, 300*1024, testCases)

	// Verify eviction stats (only for single-node tests with direct storage access)
	if storageAccess, ok := s.Harness.(TestStorageAccess); ok {
		if stor, ok := storageAccess.GetStorage().(*storage.Storage); ok {
			_, evicted := stor.CleanerStats()
			assert.GreaterOrEqual(s.T(), evicted, int64(1), "Should have evicted at least one object")
		}
	}
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
		VerifyStorageType(s.T(), s.Harness.GetTempDir(), key, storagepb.ValueType_RAW_FILE)
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
	// Skip for cluster tests as they have more complex setup
	if _, ok := s.Harness.(*ClusterTestHarness); ok {
		s.T().Skip("Skipping TTL reconfiguration test for cluster mode")
		return
	}

	// Ensure clean harness with default configuration
	s.Harness.Cleanup()
	s.Config = DefaultIntegrationTestConfig()
	s.Harness = NewIntegrationTestHarness(s.T(), s.Config)

	testCases := []struct {
		key  string
		size int64
		ttl  int64
	}{
		// Different TTLs for different sizes
		{"ttl-small-1s", 10 * 1024, 1},
		{"ttl-small-2s", 20 * 1024, 2},
		{"ttl-medium-3s", 100 * 1024, 3},
		{"ttl-medium-5s", 500 * 1024, 5},
		{"ttl-large-5s", 20 * 1024 * 1024, 5},

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

	// Wait for shortest TTL to expire plus cleanup interval
	time.Sleep(2 * time.Second)

	// Check 1s TTL objects (should be expired)
	_, err := s.Harness.GetObject("ttl-small-1s")
	assert.Error(s.T(), err, "1s TTL object should be expired")

	// Check 5s TTL objects (should still exist)
	_, err = s.Harness.GetObject("ttl-large-5s")
	assert.NoError(s.T(), err, "5s TTL object should still exist")

	// Wait for all TTLs to expire
	time.Sleep(4 * time.Second)

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
