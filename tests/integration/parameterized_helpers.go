package integration

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
)

// Common error types for testing
var (
	ErrKeyNotFound = errors.New("key not found")
)

// IsNotFoundError checks if an error indicates a key was not found
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	// Check for our sentinel error or string pattern
	return errors.Is(err, ErrKeyNotFound) || strings.Contains(err.Error(), "not found")
}

// TestCase represents a parameterized test case
type TestCase struct {
	Name        string
	Description string
	Setup       func(t *testing.T, h *IntegrationTestHarness)
	Execute     func(t *testing.T, h *IntegrationTestHarness)
	Verify      func(t *testing.T, h *IntegrationTestHarness)
	Cleanup     func(t *testing.T, h *IntegrationTestHarness)
	Skip        bool
	SkipReason  string
}

// ObjectSizeTestCase represents a test case for object size operations
type ObjectSizeTestCase struct {
	Name         string
	Size         int64
	ExpectedType pb.ValueType
	Category     string // "small", "medium", "large"
	Data         []byte
	TTL          int64
}

// RunParameterizedTest runs a parameterized test with proper setup/teardown
func RunParameterizedTest(t *testing.T, testName string, cases []TestCase) {
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Skip {
				t.Skip(tc.SkipReason)
			}

			// Create test harness with default config
			config := DefaultIntegrationTestConfig()
			harness := NewIntegrationTestHarness(t, config)
			defer harness.Cleanup()

			// Run setup if provided
			if tc.Setup != nil {
				tc.Setup(t, harness)
			}

			// Execute the test
			if tc.Execute != nil {
				tc.Execute(t, harness)
			}

			// Verify results
			if tc.Verify != nil {
				tc.Verify(t, harness)
			}

			// Run cleanup if provided
			if tc.Cleanup != nil {
				tc.Cleanup(t, harness)
			}
		})
	}
}

// RunObjectSizeTests runs parameterized tests for object size operations
func RunObjectSizeTests(t *testing.T, harness *IntegrationTestHarness, cases []ObjectSizeTestCase, 
	operation func(t *testing.T, h *IntegrationTestHarness, tc ObjectSizeTestCase)) {
	
	for _, tc := range cases {
		// Don't create nested t.Run when called from suite context
		operation(t, harness, tc)
	}
}

// StandardObjectOperations performs standard put/get/delete operations on an object
func StandardObjectOperations(t *testing.T, h *IntegrationTestHarness, key string, data []byte, ttl int64) {
	// Store the object
	err := h.PutObject(key, data, ttl)
	require.NoError(t, err, "Failed to put object with key %s", key)

	// Retrieve and verify
	retrieved, err := h.GetObject(key)
	require.NoError(t, err, "Failed to get object with key %s", key)
	VerifyDataIntegrity(t, data, retrieved)

	// Delete the object
	err = h.DeleteObject(key)
	require.NoError(t, err, "Failed to delete object with key %s", key)

	// Verify deletion
	_, err = h.GetObject(key)
	require.Error(t, err, "Object with key %s should not exist after deletion", key)
}

// ConcurrentTestConfig defines configuration for concurrent tests
type ConcurrentTestConfig struct {
	NumWorkers      int
	NumOperations   int
	ObjectSizeMin   int64
	ObjectSizeMax   int64
	ReadWeight      int // Weight for read operations (0-100)
	WriteWeight     int // Weight for write operations (0-100)
	DeleteWeight    int // Weight for delete operations (0-100)
	VerifyIntegrity bool
}

// RunConcurrentTest runs a parameterized concurrent test
func RunConcurrentTest(t *testing.T, h *IntegrationTestHarness, config ConcurrentTestConfig) {
	require.Equal(t, 100, config.ReadWeight+config.WriteWeight+config.DeleteWeight, 
		"Operation weights must sum to 100")

	type operation struct {
		opType string // "read", "write", "delete"
		key    string
		data   []byte
	}

	operations := make(chan operation, config.NumOperations)
	results := make(chan error, config.NumOperations)

	// Generate operations
	go func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < config.NumOperations; i++ {
			op := operation{}
			
			// Determine operation type based on weights
			randVal := r.Intn(100)
			if randVal < config.ReadWeight {
				op.opType = "read"
				op.key = fmt.Sprintf("concurrent-key-%d", i%10) // Reuse some keys
			} else if randVal < config.ReadWeight+config.WriteWeight {
				op.opType = "write"
				op.key = fmt.Sprintf("concurrent-key-%d", i)
				// Handle edge case where min equals max
				var size int64
				if config.ObjectSizeMax > config.ObjectSizeMin {
					size = config.ObjectSizeMin + int64(r.Intn(int(config.ObjectSizeMax-config.ObjectSizeMin)))
				} else {
					size = config.ObjectSizeMin
				}
				op.data = GenerateRandomData(size)
			} else {
				op.opType = "delete"
				op.key = fmt.Sprintf("concurrent-key-%d", i%10) // Delete some existing keys
			}
			
			operations <- op
		}
		close(operations)
	}()

	// Start workers
	for w := 0; w < config.NumWorkers; w++ {
		go func() {
			for op := range operations {
				var err error
				switch op.opType {
				case "read":
					_, err = h.GetObject(op.key)
					// Ignore not found errors for reads
					if IsNotFoundError(err) {
						err = nil
					}
				case "write":
					err = h.PutObject(op.key, op.data, 0)
				case "delete":
					err = h.DeleteObject(op.key)
				}
				results <- err
			}
		}()
	}

	// Collect results
	successCount := 0
	errorCount := 0
	for i := 0; i < config.NumOperations; i++ {
		err := <-results
		if err == nil {
			successCount++
		} else {
			errorCount++
			t.Logf("Operation error: %v", err)
		}
	}

	assert.Greater(t, successCount, config.NumOperations*8/10, 
		"At least 80%% of operations should succeed: %d/%d", successCount, config.NumOperations)
}

// TTLTestCase represents a TTL test scenario
type TTLTestCase struct {
	Key         string
	Size        int64
	TTL         int64
	WaitTime    time.Duration
	ShouldExist bool
}

// RunTTLTests runs parameterized TTL expiration tests
func RunTTLTests(t *testing.T, h *IntegrationTestHarness, cases []TTLTestCase) {
	// Store all objects
	for _, tc := range cases {
		data := GenerateRandomData(tc.Size)
		err := h.PutObject(tc.Key, data, tc.TTL)
		require.NoError(t, err, "Failed to store object %s with TTL %d", tc.Key, tc.TTL)
	}

	// Find max wait time
	maxWait := time.Duration(0)
	for _, tc := range cases {
		if tc.WaitTime > maxWait {
			maxWait = tc.WaitTime
		}
	}

	// Wait for TTL expiration
	time.Sleep(maxWait)

	// Verify objects
	for _, tc := range cases {
		_, err := h.GetObject(tc.Key)
		if tc.ShouldExist {
			assert.NoError(t, err, "Object %s should still exist", tc.Key)
		} else {
			assert.Error(t, err, "Object %s should be expired", tc.Key)
		}
	}
}

// LRUTestCase represents an LRU eviction test scenario  
type LRUTestCase struct {
	Key        string
	Size       int64
	AccessTime int64 // Unix timestamp
	ShouldEvict bool
}

// RunLRUTests runs parameterized LRU eviction tests
func RunLRUTests(t *testing.T, h *IntegrationTestHarness, maxDiskUsage int64, cases []LRUTestCase) {
	// Note: The test should already have a properly configured harness with disk limit set

	// Store all objects
	for _, tc := range cases {
		data := GenerateRandomData(tc.Size)
		err := h.PutObject(tc.Key, data, 0)
		require.NoError(t, err, "Failed to store object %s", tc.Key)
		
		// Set access time
		h.SetAccessTime(tc.Key, tc.AccessTime)
	}

	// Flush access updates and wait for eviction
	h.FlushAccessUpdates()
	time.Sleep(5 * time.Second) // Give more time for eviction to occur

	// Verify eviction
	for _, tc := range cases {
		_, err := h.GetObject(tc.Key)
		if tc.ShouldEvict {
			assert.Error(t, err, "Object %s should be evicted", tc.Key)
		} else {
			assert.NoError(t, err, "Object %s should be retained", tc.Key)
		}
	}
}

// UpdateTestCase represents an update operation test case
type UpdateTestCase struct {
	Key         string
	InitialSize int64
	UpdateSize  int64
	Category    string // "same", "cross-boundary"
}

// RunUpdateTests runs parameterized update tests
func RunUpdateTests(t *testing.T, h *IntegrationTestHarness, cases []UpdateTestCase) {
	for _, tc := range cases {
		// Store initial object
		initialData := GenerateRandomData(tc.InitialSize)
		err := h.PutObject(tc.Key, initialData, 0)
		require.NoError(t, err, "Failed to store initial object for %s", tc.Key)

		// Update with new data
		updateData := GenerateRandomData(tc.UpdateSize)
		err = h.PutObject(tc.Key, updateData, 0)
		require.NoError(t, err, "Failed to update object %s", tc.Key)

		// Verify updated data
		retrieved, err := h.GetObject(tc.Key)
		require.NoError(t, err, "Failed to retrieve updated object %s", tc.Key)
		VerifyDataIntegrity(t, updateData, retrieved)

		// Cleanup
		h.DeleteObject(tc.Key)
	}
}

// EdgeCaseTest represents an edge case test scenario
type EdgeCaseTest struct {
	Name        string
	Key         string
	Data        []byte
	Size        int64
	Description string
}

// RunEdgeCaseTests runs parameterized edge case tests
func RunEdgeCaseTests(t *testing.T, h *IntegrationTestHarness, cases []EdgeCaseTest) {
	for _, tc := range cases {
		// If Data is nil, generate it based on Size
		data := tc.Data
		if data == nil && tc.Size > 0 {
			data = GenerateRandomData(tc.Size)
		}

		// Store the object
		err := h.PutObject(tc.Key, data, 0)
		require.NoError(t, err, "Failed to store edge case: %s", tc.Description)

		// Retrieve and verify
		retrieved, err := h.GetObject(tc.Key)
		require.NoError(t, err, "Failed to retrieve edge case: %s", tc.Description)
		VerifyDataIntegrity(t, data, retrieved)

		// Cleanup
		h.DeleteObject(tc.Key)
	}
}

// CompactionTestCase represents a compaction test scenario
type CompactionTestCase struct {
	Name              string
	NumObjects        int
	ObjectSize        int64
	WaitForCompaction time.Duration
	ExpectSegments    bool
	ExpectRawFiles    bool
}

// RunCompactionTests runs parameterized compaction tests
func RunCompactionTests(t *testing.T, h *IntegrationTestHarness, cases []CompactionTestCase) {
	for _, tc := range cases {
		// Create objects
		keys := make([]string, tc.NumObjects)
		for i := 0; i < tc.NumObjects; i++ {
			keys[i] = fmt.Sprintf("compact-%s-%d", tc.Name, i)
			data := GenerateRandomData(tc.ObjectSize)
			err := h.PutObject(keys[i], data, 0)
			require.NoError(t, err, "Failed to create object %s", keys[i])
		}

		// Wait for compaction
		if tc.WaitForCompaction > 0 {
			time.Sleep(tc.WaitForCompaction)
		}

		// Verify storage state
		if tc.ExpectSegments {
			VerifySegmentsExist(t, h.TempDir, 1)
		}
		
		if !tc.ExpectRawFiles {
			// Check that raw files have been compacted
			// Note: This is a simplified check
			stats := h.GetStorageStats()
			assert.Less(t, stats.RawFileCount, tc.NumObjects,
				"Raw files should be compacted")
		}

		// Verify data integrity for all objects
		for _, key := range keys {
			_, err := h.GetObject(key)
			assert.NoError(t, err, "Object %s should still be accessible", key)
		}
	}
}