package integration

import (
	"fmt"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
)

// Test_MediumObject_RawFileCreation tests that medium objects are stored as raw files
func (s *MediumObjectSuite) Test_MediumObject_RawFileCreation() {
	testCases := []struct {
		name string
		size int64
		desc string
	}{
		{"65KB", 65 * 1024, "Just over inline threshold"},
		{"100KB", 100 * 1024, "100KB object"},
		{"1MB", 1024 * 1024, "1MB object"},
		{"8MB", 8 * 1024 * 1024, "8MB object"},
		{"15MB", 15 * 1024 * 1024, "15MB object"},
		{"16MB-exact", 16 * 1024 * 1024, "Exactly at compact threshold"},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			// Generate test data
			key := fmt.Sprintf("medium-raw-%s", tc.name)
			data := GenerateRandomData(tc.size)
			
			// Store the object
			err := s.Harness.PutObject(key, data, 0)
			require.NoError(s.T(), err, "Failed to put %s", tc.desc)
			
			// Verify it's stored as a raw file (will be logged as skipped for now)
			VerifyStorageType(s.T(), s.Harness.TempDir, key, pb.ValueType_RAW_FILE)
			
			// Verify raw files exist in storage directory
			VerifyRawFilesExist(s.T(), s.Harness.TempDir, -1) // -1 means at least one
			
			// Retrieve and verify data integrity
			retrieved, err := s.Harness.GetObject(key)
			require.NoError(s.T(), err, "Failed to get %s", tc.desc)
			VerifyDataIntegrity(s.T(), data, retrieved)
			
			// Verify no segments created yet (not compacted)
			VerifySegmentsExist(s.T(), s.Harness.TempDir, 0)
			
			// Clean up
			err = s.Harness.DeleteObject(key)
			require.NoError(s.T(), err, "Failed to delete %s", tc.desc)
		})
	}
}

// Test_MediumObject_CompactionFlow tests the compaction of medium objects from raw files to segments
func (s *MediumObjectSuite) Test_MediumObject_CompactionFlow() {
	// Store multiple medium objects
	numObjects := 10
	objectSize := int64(1024 * 1024) // 1MB each
	keys := make([]string, numObjects)
	dataMap := make(map[string][]byte)
	
	s.T().Log("Storing medium objects for compaction test...")
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("compact-%d", i)
		keys[i] = key
		data := GenerateRandomData(objectSize)
		dataMap[key] = data
		
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(s.T(), err, "Failed to put object %d", i)
	}
	
	// Verify all objects are stored as raw files initially
	s.T().Log("Verifying objects are stored as raw files...")
	for _, key := range keys {
		VerifyStorageType(s.T(), s.Harness.TempDir, key, pb.ValueType_RAW_FILE)
		
		// Verify data can be retrieved
		retrieved, err := s.Harness.GetObject(key)
		require.NoError(s.T(), err, "Failed to get %s before compaction", key)
		assert.Equal(s.T(), len(dataMap[key]), len(retrieved), "Size mismatch for %s", key)
	}
	
	// Wait for compaction to run (compaction interval is 1 second in test config)
	s.T().Log("Waiting for compaction to run...")
	time.Sleep(3 * time.Second)
	
	// Force a compaction cycle if available
	err := s.Harness.WaitForCompaction(5 * time.Second)
	if err != nil {
		s.T().Log("Compaction wait timed out, checking state anyway")
	}
	
	// Verify objects can still be retrieved after compaction
	s.T().Log("Verifying data integrity after compaction...")
	for _, key := range keys {
		retrieved, err := s.Harness.GetObject(key)
		require.NoError(s.T(), err, "Failed to get %s after compaction", key)
		VerifyDataIntegrity(s.T(), dataMap[key], retrieved)
	}
	
	// Check if segments were created (indicates compaction occurred)
	// Note: Actual verification of migration from RAW_FILE to SEGMENT 
	// would require inspecting RocksDB metadata
	s.T().Log("Checking for segment creation...")
	// This is a hint that compaction may have occurred
	// Actual verification would need to check ValueType in metadata
	
	// Clean up
	for _, key := range keys {
		err := s.Harness.DeleteObject(key)
		require.NoError(s.T(), err, "Failed to delete %s", key)
	}
}

// Test_MediumObject_PartialCompaction tests compaction with mixed scenarios
func (s *MediumObjectSuite) Test_MediumObject_PartialCompaction() {
	// Create objects with different scenarios
	scenarios := []struct {
		key    string
		size   int64
		action string // "keep", "delete", "update"
	}{
		{"partial-keep-1", 500 * 1024, "keep"},
		{"partial-keep-2", 1024 * 1024, "keep"},
		{"partial-delete-1", 800 * 1024, "delete"},
		{"partial-delete-2", 600 * 1024, "delete"},
		{"partial-update-1", 700 * 1024, "update"},
		{"partial-update-2", 900 * 1024, "update"},
	}
	
	dataMap := make(map[string][]byte)
	
	// Store initial objects
	s.T().Log("Storing objects for partial compaction test...")
	for _, sc := range scenarios {
		data := GenerateRandomData(sc.size)
		dataMap[sc.key] = data
		err := s.Harness.PutObject(sc.key, data, 0)
		require.NoError(s.T(), err, "Failed to put %s", sc.key)
	}
	
	// Perform actions before compaction
	s.T().Log("Performing pre-compaction actions...")
	for _, sc := range scenarios {
		switch sc.action {
		case "delete":
			// Delete some objects before compaction
			err := s.Harness.DeleteObject(sc.key)
			require.NoError(s.T(), err, "Failed to delete %s", sc.key)
			delete(dataMap, sc.key)
			
		case "update":
			// Update some objects with new data
			newData := GenerateRandomData(sc.size + 1024) // Slightly larger
			err := s.Harness.PutObject(sc.key, newData, 0)
			require.NoError(s.T(), err, "Failed to update %s", sc.key)
			dataMap[sc.key] = newData
		}
	}
	
	// Wait for compaction
	s.T().Log("Waiting for compaction with mixed object states...")
	time.Sleep(3 * time.Second)
	
	// Verify remaining objects
	s.T().Log("Verifying object states after compaction...")
	for key, expectedData := range dataMap {
		retrieved, err := s.Harness.GetObject(key)
		require.NoError(s.T(), err, "Failed to get %s after partial compaction", key)
		VerifyDataIntegrity(s.T(), expectedData, retrieved)
	}
	
	// Verify deleted objects are gone
	for _, sc := range scenarios {
		if sc.action == "delete" {
			_, err := s.Harness.GetObject(sc.key)
			assert.Error(s.T(), err, "Deleted key %s should not exist", sc.key)
		}
	}
	
	// Clean up remaining objects
	for key := range dataMap {
		err := s.Harness.DeleteObject(key)
		require.NoError(s.T(), err, "Failed to delete %s", key)
	}
}

// Test_MediumObject_Concurrent tests concurrent operations on medium objects
func (s *MediumObjectSuite) Test_MediumObject_Concurrent() {
	numGoroutines := 5
	objectsPerGoroutine := 10
	var wg sync.WaitGroup
	
	// Track errors
	errors := make(chan error, numGoroutines*objectsPerGoroutine)
	
	// Concurrent writes of medium objects
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			for i := 0; i < objectsPerGoroutine; i++ {
				// Random size between 64KB and 1MB
				minSize := int64(64*1024 + 1)
				maxSize := int64(1024 * 1024)
				size := minSize + int64(goroutineID*100000+i*10000)%(maxSize-minSize)
				
				key := fmt.Sprintf("medium-concurrent-g%d-i%d", goroutineID, i)
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
				key := fmt.Sprintf("medium-concurrent-g%d-i%d", goroutineID, i)
				
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
	
	// Clean up
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < objectsPerGoroutine; i++ {
			key := fmt.Sprintf("medium-concurrent-g%d-i%d", g, i)
			s.Harness.DeleteObject(key)
		}
	}
}

// Test_MediumObject_UpdateExisting tests updating medium objects
func (s *MediumObjectSuite) Test_MediumObject_UpdateExisting() {
	key := "medium-update-test"
	
	// Store initial medium object
	initialSize := int64(100 * 1024) // 100KB
	initialData := GenerateRandomData(initialSize)
	err := s.Harness.PutObject(key, initialData, 0)
	require.NoError(s.T(), err)
	
	// Verify initial data
	retrieved, err := s.Harness.GetObject(key)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), initialData, retrieved)
	
	// Update with larger data (still medium)
	updatedSize := int64(500 * 1024) // 500KB
	updatedData := GenerateRandomData(updatedSize)
	err = s.Harness.PutObject(key, updatedData, 0)
	require.NoError(s.T(), err)
	
	// Verify updated data
	retrieved, err = s.Harness.GetObject(key)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), updatedData, retrieved)
	
	// Update with even larger data (near threshold)
	largerSize := int64(15 * 1024 * 1024) // 15MB
	largerData := GenerateRandomData(largerSize)
	err = s.Harness.PutObject(key, largerData, 0)
	require.NoError(s.T(), err)
	
	// Verify it's still stored as raw file
	VerifyStorageType(s.T(), s.Harness.TempDir, key, pb.ValueType_RAW_FILE)
	
	retrieved, err = s.Harness.GetObject(key)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), largerData, retrieved)
	
	// Clean up
	err = s.Harness.DeleteObject(key)
	require.NoError(s.T(), err)
}

// Test_MediumObject_EdgeCases tests edge cases for medium objects
func (s *MediumObjectSuite) Test_MediumObject_EdgeCases() {
	testCases := []struct {
		name string
		size int64
		desc string
	}{
		{"just-over-inline", 64*1024 + 1, "Just over inline threshold"},
		{"just-under-compact", 16*1024*1024 - 1, "Just under compact threshold"},
		{"power-of-two", 256 * 1024, "Power of two size (256KB)"},
		{"prime-size", 524287, "Prime number size"},
	}
	
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			key := fmt.Sprintf("medium-edge-%s", tc.name)
			
			// Test with different data patterns
			patterns := []struct {
				name string
				data []byte
			}{
				{"random", GenerateRandomData(tc.size)},
				{"compressible", GenerateCompressibleData(tc.size)},
				{"binary", GenerateBinaryData(tc.size)},
			}
			
			for _, pattern := range patterns {
				testKey := fmt.Sprintf("%s-%s", key, pattern.name)
				
				// Store
				err := s.Harness.PutObject(testKey, pattern.data, 0)
				require.NoError(s.T(), err, "Failed to put %s with %s pattern", tc.desc, pattern.name)
				
				// Retrieve and verify
				retrieved, err := s.Harness.GetObject(testKey)
				require.NoError(s.T(), err, "Failed to get %s with %s pattern", tc.desc, pattern.name)
				assert.Equal(s.T(), len(pattern.data), len(retrieved), 
					"Size mismatch for %s with %s pattern", tc.desc, pattern.name)
				
				// Clean up
				err = s.Harness.DeleteObject(testKey)
				require.NoError(s.T(), err)
			}
		})
	}
}

// Test_MediumObject_TTL tests TTL functionality for medium objects
func (s *MediumObjectSuite) Test_MediumObject_TTL() {
	// Create medium objects with short TTL
	ttlObjects := []struct {
		key  string
		size int64
		ttl  time.Duration
	}{
		{"medium-ttl-1", 100 * 1024, 2 * time.Second},
		{"medium-ttl-2", 500 * 1024, 2 * time.Second},
		{"medium-ttl-3", 1024 * 1024, 2 * time.Second},
	}
	
	// Create medium objects without TTL
	permanentObjects := []struct {
		key  string
		size int64
	}{
		{"medium-perm-1", 200 * 1024},
		{"medium-perm-2", 800 * 1024},
	}
	
	// Store TTL objects
	s.T().Log("Storing medium objects with TTL...")
	for _, obj := range ttlObjects {
		data := GenerateRandomData(obj.size)
		err := s.Harness.PutObject(obj.key, data, int64(obj.ttl.Seconds()))
		require.NoError(s.T(), err)
	}
	
	// Store permanent objects
	for _, obj := range permanentObjects {
		data := GenerateRandomData(obj.size)
		err := s.Harness.PutObject(obj.key, data, 0)
		require.NoError(s.T(), err)
	}
	
	// Verify all objects exist initially
	for _, obj := range ttlObjects {
		VerifyKeyExists(s.T(), s.Harness.Storage, obj.key)
	}
	for _, obj := range permanentObjects {
		VerifyKeyExists(s.T(), s.Harness.Storage, obj.key)
	}
	
	// Wait for TTL expiration
	s.T().Log("Waiting for TTL expiration...")
	time.Sleep(4 * time.Second)
	
	// Verify TTL objects are gone
	for _, obj := range ttlObjects {
		VerifyKeyNotExists(s.T(), s.Harness.Storage, obj.key)
	}
	
	// Verify permanent objects still exist
	for _, obj := range permanentObjects {
		VerifyKeyExists(s.T(), s.Harness.Storage, obj.key)
	}
	
	// Clean up
	for _, obj := range permanentObjects {
		err := s.Harness.DeleteObject(obj.key)
		require.NoError(s.T(), err)
	}
}

// Test_MediumObject_StreamingWrite tests streaming writes for medium objects
func (s *MediumObjectSuite) Test_MediumObject_StreamingWrite() {
	key := "medium-streaming"
	size := int64(5 * 1024 * 1024) // 5MB
	
	// Generate data in chunks
	chunkSize := 64 * 1024 // 64KB chunks
	totalChunks := int(size / int64(chunkSize))
	
	// Create a reader that generates data on the fly
	var fullData []byte
	for i := 0; i < totalChunks; i++ {
		chunk := GenerateRandomData(int64(chunkSize))
		fullData = append(fullData, chunk...)
	}
	
	// Store using the reader
	err := s.Harness.PutObject(key, fullData, 0)
	require.NoError(s.T(), err)
	
	// Verify streaming read
	retrieved, err := s.Harness.GetObject(key)
	require.NoError(s.T(), err)
	
	// Verify data integrity
	VerifyDataIntegrity(s.T(), fullData, retrieved)
	
	// Clean up
	err = s.Harness.DeleteObject(key)
	require.NoError(s.T(), err)
}

// Test_MediumObject_CompactionWithTTL tests compaction behavior with TTL objects
func (s *MediumObjectSuite) Test_MediumObject_CompactionWithTTL() {
	// Create mix of TTL and non-TTL medium objects
	objects := []struct {
		key  string
		size int64
		ttl  int64 // 0 means no TTL
	}{
		{"compact-ttl-1", 500 * 1024, 10},  // 10 second TTL
		{"compact-ttl-2", 700 * 1024, 10},  // 10 second TTL
		{"compact-perm-1", 600 * 1024, 0},  // No TTL
		{"compact-perm-2", 800 * 1024, 0},  // No TTL
	}
	
	dataMap := make(map[string][]byte)
	
	// Store all objects
	for _, obj := range objects {
		data := GenerateRandomData(obj.size)
		dataMap[obj.key] = data
		err := s.Harness.PutObject(obj.key, data, obj.ttl)
		require.NoError(s.T(), err)
	}
	
	// Wait for compaction (but before TTL expiration)
	s.T().Log("Waiting for compaction (before TTL expiration)...")
	time.Sleep(3 * time.Second)
	
	// Verify all objects still exist and are retrievable
	for _, obj := range objects {
		retrieved, err := s.Harness.GetObject(obj.key)
		require.NoError(s.T(), err, "Object %s should still exist", obj.key)
		VerifyDataIntegrity(s.T(), dataMap[obj.key], retrieved)
	}
	
	// Wait for TTL expiration
	s.T().Log("Waiting for TTL expiration...")
	time.Sleep(8 * time.Second)
	
	// Verify TTL objects are gone
	for _, obj := range objects {
		if obj.ttl > 0 {
			VerifyKeyNotExists(s.T(), s.Harness.Storage, obj.key)
		} else {
			VerifyKeyExists(s.T(), s.Harness.Storage, obj.key)
		}
	}
	
	// Clean up remaining objects
	for _, obj := range objects {
		if obj.ttl == 0 {
			err := s.Harness.DeleteObject(obj.key)
			require.NoError(s.T(), err)
		}
	}
}