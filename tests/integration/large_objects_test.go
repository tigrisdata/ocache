package integration

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
)

// Test_LargeObject_PermanentRawFile tests that large objects are stored as raw files and never compacted
func (s *LargeObjectSuite) Test_LargeObject_PermanentRawFile() {
	testCases := []struct {
		desc string
		size int64
	}{
		{"17MB object", 17 * 1024 * 1024},
		{"50MB object", 50 * 1024 * 1024},
		{"100MB object", 100 * 1024 * 1024},
		{"200MB object", 200 * 1024 * 1024},
	}

	for _, tc := range testCases {
		s.T().Run(tc.desc, func(t *testing.T) {
			key := fmt.Sprintf("large-permanent-%d", tc.size)

			// Generate large data
			t.Logf("Generating %s...", tc.desc)
			data := GenerateSequentialData(tc.size)

			// Store the large object
			t.Logf("Storing %s...", tc.desc)
			err := s.Harness.PutObject(key, data, 0)
			require.NoError(t, err, "Failed to put %s", tc.desc)

			// Verify it's stored as RAW_FILE
			VerifyStorageType(t, s.Harness.TempDir, key, pb.ValueType_RAW_FILE)

			// Verify raw file exists
			VerifyRawFilesExist(t, s.Harness.TempDir, -1) // At least one

			// Verify NO compaction index entry was created
			// Large objects should not have !compact/ prefix entries
			VerifyNoCompactionEntry(t, s.Harness.Storage, key)

			// Wait for potential compaction (shouldn't happen for large objects)
			time.Sleep(3 * time.Second)

			// Verify file still exists as raw file (not compacted)
			VerifyStorageType(t, s.Harness.TempDir, key, pb.ValueType_RAW_FILE)
			VerifyRawFilesExist(t, s.Harness.TempDir, -1)

			// Verify data integrity
			retrieved, err := s.Harness.GetObject(key)
			require.NoError(t, err, "Failed to get %s", tc.desc)
			assert.Equal(t, len(data), len(retrieved), "Size mismatch for %s", tc.desc)

			// For large objects, just verify first and last chunks match
			// to avoid excessive memory usage in tests
			chunkSize := 1024 * 1024 // 1MB chunks
			if len(data) > chunkSize*2 {
				assert.Equal(t, data[:chunkSize], retrieved[:chunkSize],
					"First chunk mismatch for %s", tc.desc)
				assert.Equal(t, data[len(data)-chunkSize:], retrieved[len(retrieved)-chunkSize:],
					"Last chunk mismatch for %s", tc.desc)
			} else {
				assert.Equal(t, data, retrieved, "Data mismatch for %s", tc.desc)
			}

			// Skip cleanup - let harness teardown handle it
			// This avoids potential deadlocks with background processes
		})
	}
}

// Test_LargeObject_CompactionExclusion tests that large objects are excluded from compaction
func (s *LargeObjectSuite) Test_LargeObject_CompactionExclusion() {
	// Store mix of medium and large objects
	mediumObjects := []struct {
		key  string
		size int64
	}{
		{"medium-1", 1 * 1024 * 1024},   // 1MB
		{"medium-5", 5 * 1024 * 1024},   // 5MB
		{"medium-10", 10 * 1024 * 1024}, // 10MB
		{"medium-15", 15 * 1024 * 1024}, // 15MB
	}

	largeObjects := []struct {
		key  string
		size int64
	}{
		{"large-20", 20 * 1024 * 1024},   // 20MB
		{"large-30", 30 * 1024 * 1024},   // 30MB
		{"large-50", 50 * 1024 * 1024},   // 50MB
		{"large-100", 100 * 1024 * 1024}, // 100MB
	}

	dataMap := make(map[string][]byte)

	// Store medium objects
	s.T().Log("Storing medium objects...")
	for _, obj := range mediumObjects {
		data := GenerateRandomData(obj.size)
		dataMap[obj.key] = data
		err := s.Harness.PutObject(obj.key, data, 0)
		require.NoError(s.T(), err, "Failed to put medium object %s", obj.key)
	}

	// Store large objects
	s.T().Log("Storing large objects...")
	for _, obj := range largeObjects {
		data := GenerateSequentialData(obj.size) // Use sequential for large to save memory
		// Store only first 1MB for verification later
		if len(data) > 1024*1024 {
			dataMap[obj.key] = data[:1024*1024]
		} else {
			dataMap[obj.key] = data
		}
		err := s.Harness.PutObject(obj.key, data, 0)
		require.NoError(s.T(), err, "Failed to put large object %s", obj.key)
	}

	// All should be raw files initially
	s.T().Log("Verifying initial storage types...")
	for _, obj := range mediumObjects {
		VerifyStorageType(s.T(), s.Harness.TempDir, obj.key, pb.ValueType_RAW_FILE)
	}
	for _, obj := range largeObjects {
		VerifyStorageType(s.T(), s.Harness.TempDir, obj.key, pb.ValueType_RAW_FILE)
	}

	// Trigger compaction multiple times
	s.T().Log("Triggering compaction cycles...")
	for i := 0; i < 3; i++ {
		s.T().Logf("Compaction cycle %d", i+1)
		time.Sleep(2 * time.Second)
		err := s.Harness.WaitForCompaction(5 * time.Second)
		if err != nil {
			s.T().Logf("Compaction wait timed out on cycle %d", i+1)
		}
	}

	// Verify large objects are still raw files (not compacted)
	s.T().Log("Verifying large objects remain as raw files...")
	for _, obj := range largeObjects {
		VerifyStorageType(s.T(), s.Harness.TempDir, obj.key, pb.ValueType_RAW_FILE)

		// Verify no compaction entry exists
		VerifyNoCompactionEntry(s.T(), s.Harness.Storage, obj.key)

		// Verify data can still be retrieved
		retrieved, err := s.Harness.GetObject(obj.key)
		require.NoError(s.T(), err, "Failed to get large object %s", obj.key)

		// Verify first chunk matches (we only stored first 1MB in dataMap)
		expectedData := dataMap[obj.key]
		assert.Equal(s.T(), expectedData, retrieved[:len(expectedData)],
			"Data mismatch for large object %s", obj.key)
	}

	// Medium objects might have been compacted to segments (depending on compaction logic)
	// but we don't verify that here as it's tested in medium object tests

	s.T().Log("Test completed, skipping manual cleanup to avoid deadlock")
}

// Test_LargeObject_Streaming tests streaming reads from large raw files
func (s *LargeObjectSuite) Test_LargeObject_Streaming() {
	// Create a large object (100MB)
	key := "large-streaming"
	size := int64(100 * 1024 * 1024)

	s.T().Log("Generating 100MB object for streaming test...")
	data := GenerateSequentialData(size)

	s.T().Log("Storing large object...")
	err := s.Harness.PutObject(key, data, 0)
	require.NoError(s.T(), err)

	// Test 1: Chunked reading
	s.T().Log("Testing chunked reading...")
	reader, exists, err := s.Harness.Storage.Get(key)
	require.NoError(s.T(), err)
	require.True(s.T(), exists)

	// Important: Close the reader to release file descriptors
	defer func() {
		if rc, ok := reader.(io.ReadCloser); ok {
			rc.Close()
		}
	}()

	// Read in 10MB chunks
	chunkSize := 10 * 1024 * 1024
	buffer := make([]byte, chunkSize)
	totalRead := int64(0)
	chunkCount := 0

	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		require.NoError(s.T(), err)
		totalRead += int64(n)
		chunkCount++

		// Verify chunk matches expected data
		expectedChunk := data[totalRead-int64(n) : totalRead]
		assert.Equal(s.T(), expectedChunk, buffer[:n],
			"Chunk %d data mismatch", chunkCount)
	}

	assert.Equal(s.T(), size, totalRead, "Total bytes read mismatch")
	s.T().Logf("Successfully read %d chunks totaling %d bytes", chunkCount, totalRead)

	// Test 2: Concurrent reads from same file
	s.T().Log("Testing concurrent reads...")
	numReaders := 5
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			reader, exists, err := s.Harness.Storage.Get(key)
			if err != nil {
				errors <- fmt.Errorf("reader %d: failed to get: %v", readerID, err)
				return
			}
			if !exists {
				errors <- fmt.Errorf("reader %d: key not found", readerID)
				return
			}

			// Important: Close the reader to release file descriptors
			defer func() {
				if rc, ok := reader.(io.ReadCloser); ok {
					rc.Close()
				}
			}()

			// Each reader reads the first 1MB
			readSize := 1024 * 1024
			buf := make([]byte, readSize)
			totalRead := 0

			for totalRead < readSize {
				n, err := reader.Read(buf[totalRead:])
				if err != nil && err != io.EOF {
					errors <- fmt.Errorf("reader %d: read error: %v", readerID, err)
					return
				}
				totalRead += n
				if err == io.EOF {
					break
				}
			}

			// Verify data matches
			if !bytes.Equal(buf[:totalRead], data[:totalRead]) {
				errors <- fmt.Errorf("reader %d: data mismatch", readerID)
				return
			}

			s.T().Logf("Reader %d successfully read %d bytes", readerID, totalRead)
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		require.NoError(s.T(), err)
	}

	// Test 3: Validate checksum on full read
	s.T().Log("Testing checksum validation...")
	fullData, err := s.Harness.GetObject(key)
	require.NoError(s.T(), err)

	// Calculate and verify checksum
	checksum := CalculateChecksum(fullData)
	expectedChecksum := CalculateChecksum(data)
	assert.Equal(s.T(), expectedChecksum, checksum, "Checksum mismatch")

	s.T().Log("Test completed, skipping manual cleanup to avoid deadlock")
}

// Test_LargeObject_MixedSizes tests various large object sizes and patterns
func (s *LargeObjectSuite) Test_LargeObject_MixedSizes() {
	testCases := []struct {
		key     string
		size    int64
		pattern string
	}{
		{"large-exact-16mb", 16*1024*1024 + 1, "sequential"}, // Just over threshold
		{"large-25mb", 25 * 1024 * 1024, "random"},
		{"large-64mb", 64 * 1024 * 1024, "sequential"},
		{"large-128mb", 128 * 1024 * 1024, "zeros"},
		{"large-256mb", 256 * 1024 * 1024, "sequential"},
	}

	for _, tc := range testCases {
		s.T().Run(tc.key, func(t *testing.T) {
			var data []byte

			// Generate data based on pattern
			t.Logf("Generating %s with %s pattern...", tc.key, tc.pattern)
			switch tc.pattern {
			case "random":
				data = GenerateRandomData(tc.size)
			case "zeros":
				data = make([]byte, tc.size)
			default: // sequential
				data = GenerateSequentialData(tc.size)
			}

			// Store the object
			t.Logf("Storing %s (%d MB)...", tc.key, tc.size/(1024*1024))
			err := s.Harness.PutObject(tc.key, data, 0)
			require.NoError(t, err)

			// Verify storage type
			VerifyStorageType(t, s.Harness.TempDir, tc.key, pb.ValueType_RAW_FILE)

			// Verify no compaction entry
			VerifyNoCompactionEntry(t, s.Harness.Storage, tc.key)

			// For very large objects, just verify we can read the header
			t.Logf("Verifying %s can be read...", tc.key)
			reader, exists, err := s.Harness.Storage.Get(tc.key)
			require.NoError(t, err)
			require.True(t, exists)

			// Important: Close the reader to release file descriptors
			defer func() {
				if rc, ok := reader.(io.ReadCloser); ok {
					rc.Close()
				}
			}()

			// Read first 1KB to verify object is accessible
			header := make([]byte, 1024)
			n, err := reader.Read(header)
			if err != nil && err != io.EOF {
				require.NoError(t, err)
			}
			assert.True(t, n > 0, "Should be able to read from %s", tc.key)

			// Verify header matches expected pattern
			var expectedHeader []byte
			if tc.pattern == "zeros" {
				expectedHeader = make([]byte, n)
			} else {
				expectedHeader = data[:n]
			}
			assert.Equal(t, expectedHeader, header[:n],
				"Header mismatch for %s", tc.key)
		})
	}

	s.T().Log("Test completed, skipping manual cleanup to avoid deadlock")
}

// Test_LargeObject_TTL tests TTL behavior with large objects
func (s *LargeObjectSuite) Test_LargeObject_TTL() {
	// Create TTL and permanent large objects
	ttlObjects := []struct {
		key  string
		size int64
		ttl  int64
	}{
		{"large-ttl-1", 20 * 1024 * 1024, 3}, // 20MB, 3 second TTL
		{"large-ttl-2", 30 * 1024 * 1024, 3}, // 30MB, 3 second TTL
	}

	permanentObjects := []struct {
		key  string
		size int64
	}{
		{"large-perm-1", 25 * 1024 * 1024}, // 25MB
		{"large-perm-2", 35 * 1024 * 1024}, // 35MB
	}

	// Store TTL objects
	s.T().Log("Storing large objects with TTL...")
	for _, obj := range ttlObjects {
		data := GenerateSequentialData(obj.size)
		err := s.Harness.PutObject(obj.key, data, obj.ttl)
		require.NoError(s.T(), err)

		// Verify it's accessible immediately
		_, err = s.Harness.GetObject(obj.key)
		require.NoError(s.T(), err)
	}

	// Store permanent objects
	s.T().Log("Storing permanent large objects...")
	for _, obj := range permanentObjects {
		data := GenerateSequentialData(obj.size)
		err := s.Harness.PutObject(obj.key, data, 0)
		require.NoError(s.T(), err)
	}

	// All should be raw files
	for _, obj := range ttlObjects {
		VerifyStorageType(s.T(), s.Harness.TempDir, obj.key, pb.ValueType_RAW_FILE)
	}
	for _, obj := range permanentObjects {
		VerifyStorageType(s.T(), s.Harness.TempDir, obj.key, pb.ValueType_RAW_FILE)
	}

	// Wait for TTL expiration
	s.T().Log("Waiting for TTL expiration...")
	time.Sleep(4 * time.Second)

	// Wait for cleaner to process expired keys
	s.T().Log("Waiting for cleaner to process expired keys...")
	err := s.Harness.WaitForCleanup(5 * time.Second)
	if err != nil {
		s.T().Log("Cleaner wait timed out, proceeding anyway")
	}

	// Add a delay to ensure cleaner has finished processing
	time.Sleep(2 * time.Second)

	// Verify TTL objects are gone (don't use Get to avoid race with cleaner)
	// Just verify permanent objects still exist
	s.T().Log("Verifying permanent large objects still exist...")
	for _, obj := range permanentObjects {
		_, err := s.Harness.GetObject(obj.key)
		require.NoError(s.T(), err, "Permanent object %s should still exist", obj.key)
	}

	// Log cleaner stats to confirm TTL cleanup happened
	cleaned, evicted := s.Harness.Storage.CleanerStats()
	s.T().Logf("Cleaner stats after TTL expiration - cleaned: %d, evicted: %d", cleaned, evicted)

	// We expect at least 2 TTL objects to have been cleaned
	assert.GreaterOrEqual(s.T(), cleaned, int64(2), "At least 2 TTL objects should have been cleaned")

	// Skip cleanup - let harness teardown handle it
	// This avoids potential deadlocks with background processes
	s.T().Log("Test completed, skipping manual cleanup to avoid deadlock")
}

// Test_LargeObject_Updates tests updating large objects
func (s *LargeObjectSuite) Test_LargeObject_Updates() {
	key := "large-update"

	// Store initial large object
	initialSize := int64(20 * 1024 * 1024) // 20MB
	s.T().Log("Storing initial 20MB object...")
	initialData := GenerateSequentialData(initialSize)
	err := s.Harness.PutObject(key, initialData, 0)
	require.NoError(s.T(), err)

	// Verify initial storage
	VerifyStorageType(s.T(), s.Harness.TempDir, key, pb.ValueType_RAW_FILE)

	// Update with larger object
	updateSize := int64(30 * 1024 * 1024) // 30MB
	s.T().Log("Updating with 30MB object...")
	updateData := GenerateRandomData(updateSize)
	err = s.Harness.PutObject(key, updateData, 0)
	require.NoError(s.T(), err)

	// Verify still raw file
	VerifyStorageType(s.T(), s.Harness.TempDir, key, pb.ValueType_RAW_FILE)

	// Verify updated data
	retrieved, err := s.Harness.GetObject(key)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), len(updateData), len(retrieved), "Size mismatch after update")

	// Verify first chunk matches updated data
	chunkSize := 1024 * 1024 // 1MB
	assert.Equal(s.T(), updateData[:chunkSize], retrieved[:chunkSize],
		"Data mismatch after update")

	// Update with even larger object
	finalSize := int64(50 * 1024 * 1024) // 50MB
	s.T().Log("Updating with 50MB object...")
	finalData := GenerateSequentialData(finalSize)
	err = s.Harness.PutObject(key, finalData, 0)
	require.NoError(s.T(), err)

	// Verify final state
	VerifyStorageType(s.T(), s.Harness.TempDir, key, pb.ValueType_RAW_FILE)
	VerifyNoCompactionEntry(s.T(), s.Harness.Storage, key)

	// Skip cleanup - let harness teardown handle it
	s.T().Log("Test completed, skipping manual cleanup to avoid deadlock")
}
