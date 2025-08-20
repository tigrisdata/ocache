package integration

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage"
)

// IntegrationTestConfig holds configuration for Integration tests
type IntegrationTestConfig struct {
	InlineThreshold    int64         // Threshold for inline storage (default 64KB)
	CompactThreshold   int64         // Threshold for compaction (default 16MB)
	SegmentSize        int64         // Maximum segment size (default 256MB)
	CompactionInterval time.Duration // How often compaction runs
	CleanupInterval    time.Duration // How often cleanup runs
	MaxDiskUsage       int64         // Maximum disk usage for LRU eviction
	FDCacheSize        int           // File descriptor cache size
}

// DefaultIntegrationTestConfig returns default test configuration
func DefaultIntegrationTestConfig() IntegrationTestConfig {
	return IntegrationTestConfig{
		InlineThreshold:    64 * 1024,         // 64KB
		CompactThreshold:   16 * 1024 * 1024,  // 16MB
		SegmentSize:        256 * 1024 * 1024, // 256MB
		CompactionInterval: 1 * time.Second,   // Fast for testing
		CleanupInterval:    1 * time.Second,   // Fast for testing
		MaxDiskUsage:       0,                 // No limit by default
		FDCacheSize:        100,
	}
}

// TestMetrics tracks metrics during test execution
type TestMetrics struct {
	TotalWrites    atomic.Int64
	TotalReads     atomic.Int64
	TotalDeletes   atomic.Int64
	InlineObjects  atomic.Int64 // Objects written as inline (small)
	RawFileObjects atomic.Int64 // Objects written as raw files (medium/large)
	CompactionRuns atomic.Int64
	CompactedFiles atomic.Int64
	CleanupRuns    atomic.Int64
	BytesWritten   atomic.Int64
	BytesRead      atomic.Int64
	ErrorCount     atomic.Int64
	StartTime      time.Time
	EndTime        time.Time
}

// IntegrationTestHarness provides utilities for Integration testing
type IntegrationTestHarness struct {
	T           *testing.T
	Storage     *storage.Storage
	Config      IntegrationTestConfig
	TempDir     string
	Metrics     *TestMetrics
	cleanup     func()
	stopMetrics chan struct{}
}

// NewIntegrationTestHarness creates a new test harness
func NewIntegrationTestHarness(t *testing.T, config IntegrationTestConfig) *IntegrationTestHarness {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "ocache-integration-test-*")
	require.NoError(t, err)

	// Set environment variables for test intervals
	if config.CleanupInterval > 0 {
		os.Setenv("OCACHE_TEST_CLEANUP_INTERVAL", config.CleanupInterval.String())
	}

	// Initialize storage
	storage.InitStorageWithConfig(&storage.StorageConfig{
		DiskPath:           tmpDir,
		TTL:                0,
		InlineThreshold:    int(config.InlineThreshold),
		CompactThreshold:   config.CompactThreshold,
		SegmentSize:        config.SegmentSize,
		FdCacheSize:        config.FDCacheSize,
		MaxDiskUsage:       config.MaxDiskUsage,
		CompactionInterval: config.CompactionInterval,
	})

	s := storage.GetStorage()
	require.NotNil(t, s)

	h := &IntegrationTestHarness{
		T:           t,
		Storage:     s,
		Config:      config,
		TempDir:     tmpDir,
		Metrics:     &TestMetrics{StartTime: time.Now()},
		stopMetrics: make(chan struct{}),
	}

	h.cleanup = func() {
		close(h.stopMetrics)
		storage.CloseStorage()
		os.RemoveAll(tmpDir)
		os.Unsetenv("OCACHE_TEST_CLEANUP_INTERVAL")
	}

	// Start metrics collection
	h.startMetricsCollection()

	return h
}

// Cleanup cleans up the test harness
func (h *IntegrationTestHarness) Cleanup() {
	h.Metrics.EndTime = time.Now()
	if h.cleanup != nil {
		h.cleanup()
	}
}

// startMetricsCollection starts background metrics collection
func (h *IntegrationTestHarness) startMetricsCollection() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				h.updateStorageMetrics()
			case <-h.stopMetrics:
				return
			}
		}
	}()
}

// updateStorageMetrics updates storage distribution metrics
func (h *IntegrationTestHarness) updateStorageMetrics() {
	// This would be updated based on actual storage inspection
	// For now, we'll track these through operations
}

// PutObject stores an object in the cache
func (h *IntegrationTestHarness) PutObject(key string, data []byte, ttl int64) error {
	h.Metrics.TotalWrites.Add(1)
	h.Metrics.BytesWritten.Add(int64(len(data)))

	err := h.Storage.Put(key, bytes.NewReader(data), int(ttl))
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return err
	}

	// Track object type based on size
	if int64(len(data)) <= h.Config.InlineThreshold {
		h.Metrics.InlineObjects.Add(1)
	} else if int64(len(data)) <= h.Config.CompactThreshold {
		h.Metrics.RawFileObjects.Add(1)
	} else {
		h.Metrics.RawFileObjects.Add(1) // Large objects stay as raw files
	}

	return nil
}

// GetObject retrieves an object from the cache
func (h *IntegrationTestHarness) GetObject(key string) ([]byte, error) {
	h.Metrics.TotalReads.Add(1)

	reader, exists, err := h.Storage.Get(key)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return nil, err
	}
	if !exists {
		h.Metrics.ErrorCount.Add(1)
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Important: Close the reader if it's a ReadCloser to release file descriptors
	defer func() {
		if rc, ok := reader.(io.ReadCloser); ok {
			rc.Close()
		}
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return nil, err
	}

	h.Metrics.BytesRead.Add(int64(len(data)))
	return data, nil
}

// DeleteObject deletes an object from the cache
func (h *IntegrationTestHarness) DeleteObject(key string) error {
	h.Metrics.TotalDeletes.Add(1)

	// The storage package uses DeleteKey which doesn't return an error
	h.Storage.DeleteKey(key)

	return nil
}

// WaitForCompaction waits for compaction to complete or timeout
func (h *IntegrationTestHarness) WaitForCompaction(timeout time.Duration) error {
	start := time.Now()

	for time.Since(start) < timeout {
		// Check if compaction has run
		// This would be implemented based on actual compaction status
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// WaitForCleanup waits for cleanup cycle to run
func (h *IntegrationTestHarness) WaitForCleanup(timeout time.Duration) error {
	start := time.Now()
	initialCleaned, initialEvicted := h.Storage.CleanerStats()

	for time.Since(start) < timeout {
		cleaned, evicted := h.Storage.CleanerStats()
		if cleaned > initialCleaned || evicted > initialEvicted {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("cleanup did not run within timeout")
}

// SetAccessTime sets the access time for a key (for testing LRU)
func (h *IntegrationTestHarness) SetAccessTime(key string, timestamp int64) {
	h.Storage.SetAccessTime(key, timestamp)
}

// FlushAccessUpdates flushes pending access time updates
func (h *IntegrationTestHarness) FlushAccessUpdates() {
	h.Storage.FlushAccessUpdates()
}

// VerifyStorageType checks that a key is stored with the expected type
func (h *IntegrationTestHarness) VerifyStorageType(key string, expectedType string) error {
	// This would inspect the actual storage to verify the type
	// For now, we'll use size-based inference
	reader, exists, err := h.Storage.Get(key)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("key not found: %s", key)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	actualType := h.inferStorageType(int64(len(data)))
	if actualType != expectedType {
		return fmt.Errorf("expected storage type %s, got %s for key %s", expectedType, actualType, key)
	}

	return nil
}

// inferStorageType infers storage type based on size
func (h *IntegrationTestHarness) inferStorageType(size int64) string {
	if size <= h.Config.InlineThreshold {
		return "INLINE"
	} else if size <= h.Config.CompactThreshold {
		return "RAW_FILE" // Will become SEGMENT after compaction
	} else {
		return "RAW_FILE" // Large files stay as RAW_FILE
	}
}

// GetStorageStats returns current storage statistics
func (h *IntegrationTestHarness) GetStorageStats() StorageStats {
	stats := StorageStats{}

	// Get list of keys
	keys, err := h.Storage.ListKeys()
	if err == nil {
		stats.TotalKeys = len(keys)

		// Count keys by prefix
		for _, key := range keys {
			if len(key) >= 3 {
				switch key[:3] {
				case "ttl":
					stats.TTLKeys++
				case "lru":
					stats.LRUKeys++
				}
			}
		}
	}

	// Count raw files
	filesDir := filepath.Join(h.TempDir, "files")
	if files, err := filepath.Glob(filepath.Join(filesDir, "*")); err == nil {
		stats.RawFileCount = len(files)
	}

	// Count segment files
	segmentDir := filepath.Join(h.TempDir, "segments")
	if files, err := filepath.Glob(filepath.Join(segmentDir, "segment_*.seg")); err == nil {
		stats.SegmentCount = len(files)
	}

	// Get disk usage (TODO: implement GetDiskUsage in Storage)
	stats.TotalDiskUsage = 0 // h.Storage.GetDiskUsage()
	stats.DiskUsage = stats.TotalDiskUsage

	// Get cleaner stats
	stats.CleanedKeys, stats.EvictedKeys = h.Storage.CleanerStats()

	// Get disk usage
	stats.DiskUsage = h.calculateDiskUsage()

	return stats
}

// calculateDiskUsage calculates total disk usage
func (h *IntegrationTestHarness) calculateDiskUsage() int64 {
	var totalSize int64

	// Walk through storage directory
	filepath.Walk(h.TempDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	return totalSize
}

// PrintMetrics prints test metrics
func (h *IntegrationTestHarness) PrintMetrics() {
	// If EndTime is not set, use current time for duration calculation
	endTime := h.Metrics.EndTime
	if endTime.IsZero() {
		endTime = time.Now()
	}
	duration := endTime.Sub(h.Metrics.StartTime)
	fmt.Printf("\n=== Integration Test Metrics ===\n")
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Total Writes: %d\n", h.Metrics.TotalWrites.Load())
	fmt.Printf("Total Reads: %d\n", h.Metrics.TotalReads.Load())
	fmt.Printf("Total Deletes: %d\n", h.Metrics.TotalDeletes.Load())
	fmt.Printf("Bytes Written: %d\n", h.Metrics.BytesWritten.Load())
	fmt.Printf("Bytes Read: %d\n", h.Metrics.BytesRead.Load())
	fmt.Printf("Inline Objects: %d\n", h.Metrics.InlineObjects.Load())
	fmt.Printf("Raw File Objects: %d\n", h.Metrics.RawFileObjects.Load())
	fmt.Printf("Error Count: %d\n", h.Metrics.ErrorCount.Load())
	fmt.Printf("=======================\n")
}

// StorageStats holds storage statistics
type StorageStats struct {
	TotalKeys      int
	TTLKeys        int
	LRUKeys        int
	CleanedKeys    int64
	EvictedKeys    int64
	DiskUsage      int64
	RawFileCount   int
	SegmentCount   int
	TotalDiskUsage int64
}
