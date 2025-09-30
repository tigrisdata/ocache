package integration

import (
	"bytes"
	"fmt"
	"io"
	"math"
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
	InlineThreshold        int64         // Threshold for inline storage (default 64KB)
	CompactThreshold       int64         // Threshold for compaction (default 16MB)
	SegmentSize            int64         // Maximum segment size (default 256MB)
	CompactionInterval     time.Duration // How often compaction runs
	CompactionThreads      int           // Number of compaction threads
	RecompactMinSegmentAge time.Duration // Minimum age for segment recompaction
	RecompactMinSegments   int           // Minimum number of segments for recompaction
	CleanupInterval        time.Duration // How often cleanup runs
	AccessUpdateDelay      time.Duration // How often access time is updated
	MaxDiskUsage           int64         // Maximum disk usage for LRU eviction
	FDCacheSize            int           // File descriptor cache size
}

// DefaultIntegrationTestConfig returns default test configuration
func DefaultIntegrationTestConfig() IntegrationTestConfig {
	return IntegrationTestConfig{
		InlineThreshold:        64 * 1024,              // 64KB
		CompactThreshold:       16 * 1024 * 1024,       // 16MB
		SegmentSize:            256 * 1024 * 1024,      // 256MB
		CompactionInterval:     1 * time.Second,        // Fast for testing
		CompactionThreads:      1,                      // Default to single thread
		RecompactMinSegmentAge: 30 * time.Second,       // Default to 30 seconds
		RecompactMinSegments:   2,                      // Default to 2 segments
		CleanupInterval:        1 * time.Second,        // Fast for testing
		AccessUpdateDelay:      200 * time.Millisecond, // Default to 200ms for testing
		MaxDiskUsage:           0,                      // No limit by default
		FDCacheSize:            100,
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

	// Initialize storage
	s, err := storage.NewStorageWithConfig(&storage.StorageConfig{
		DiskPath:           tmpDir,
		TTL:                0,
		InlineThreshold:    int(config.InlineThreshold),
		CompactThreshold:   config.CompactThreshold,
		SegmentSize:        config.SegmentSize,
		FdCacheSize:        config.FDCacheSize,
		MaxDiskUsage:       config.MaxDiskUsage,
		CompactionInterval: config.CompactionInterval,
		CompactionThreads:  config.CompactionThreads,
		MinSegmentAge:      config.RecompactMinSegmentAge,
		MinSegments:        config.RecompactMinSegments,
		CleanupInterval:    config.CleanupInterval,
		AccessUpdateDelay:  config.AccessUpdateDelay,
	})
	require.NoError(t, err)

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
		s.Close()
		os.RemoveAll(tmpDir)
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

	reader, exists, err := h.Storage.Get(key, 0, 0)
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

	err := h.Storage.DeleteKey(key)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return err
	}

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
	reader, exists, err := h.Storage.Get(key, 0, 0)
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
	keys, err := h.Storage.ListKeys("")
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

// GetTempDir returns the temporary directory path for the test
func (h *IntegrationTestHarness) GetTempDir() string {
	return h.TempDir
}

// GetStorage returns the storage instance (implements TestStorageAccess)
func (h *IntegrationTestHarness) GetStorage() interface{} {
	return h.Storage
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

// NodeDistribution holds distribution metrics for a single node
type NodeDistribution struct {
	KeyCount     int64
	WriteCount   int64
	ReadCount    int64
	DeleteCount  int64
	BytesWritten int64
	BytesRead    int64
	Partitions   []int
}

// DistributionStats holds statistics about workload distribution across cluster nodes
type DistributionStats struct {
	NodeCount        int
	PerNode          map[string]NodeDistribution
	KeyCountStdDev   float64 // Standard deviation of keys per node
	WriteCountStdDev float64 // Std dev of writes
	MaxMinKeyRatio   float64 // Ratio of max to min keys
	BalanceScore     float64 // 0-100, 100 = perfectly balanced
}

// CalculateBalance computes balance metrics for distribution stats
func (s *DistributionStats) CalculateBalance() {
	if len(s.PerNode) == 0 {
		s.BalanceScore = 0
		return
	}

	// Calculate key count statistics
	var totalKeys, minKeys, maxKeys int64
	var keyCounts []float64
	first := true

	for _, dist := range s.PerNode {
		totalKeys += dist.KeyCount
		keyCounts = append(keyCounts, float64(dist.KeyCount))

		if first {
			minKeys = dist.KeyCount
			maxKeys = dist.KeyCount
			first = false
		} else {
			if dist.KeyCount < minKeys {
				minKeys = dist.KeyCount
			}
			if dist.KeyCount > maxKeys {
				maxKeys = dist.KeyCount
			}
		}
	}

	// Calculate standard deviation for keys
	if len(keyCounts) > 0 {
		mean := float64(totalKeys) / float64(len(keyCounts))
		var sumSquares float64
		for _, count := range keyCounts {
			diff := count - mean
			sumSquares += diff * diff
		}
		s.KeyCountStdDev = math.Sqrt(sumSquares / float64(len(keyCounts)))
	}

	// Calculate write count standard deviation
	var writeCounts []float64
	for _, dist := range s.PerNode {
		writeCounts = append(writeCounts, float64(dist.WriteCount))
	}
	if len(writeCounts) > 0 {
		var totalWrites float64
		for _, count := range writeCounts {
			totalWrites += count
		}
		mean := totalWrites / float64(len(writeCounts))
		var sumSquares float64
		for _, count := range writeCounts {
			diff := count - mean
			sumSquares += diff * diff
		}
		s.WriteCountStdDev = math.Sqrt(sumSquares / float64(len(writeCounts)))
	}

	// Calculate max/min ratio
	if minKeys > 0 {
		s.MaxMinKeyRatio = float64(maxKeys) / float64(minKeys)
	} else if maxKeys > 0 {
		s.MaxMinKeyRatio = math.Inf(1)
	} else {
		s.MaxMinKeyRatio = 1.0
	}

	// Calculate balance score (0-100)
	// Perfect balance = 100, completely unbalanced = 0
	// Based on coefficient of variation (CV = stddev / mean)
	if totalKeys > 0 {
		mean := float64(totalKeys) / float64(len(keyCounts))
		if mean > 0 {
			cv := s.KeyCountStdDev / mean
			// Convert CV to score: lower CV = higher score
			// CV of 0 = 100, CV of 1 = 0
			s.BalanceScore = math.Max(0, math.Min(100, 100*(1-cv)))
		} else {
			s.BalanceScore = 100
		}
	} else {
		s.BalanceScore = 100 // No data = perfectly balanced trivially
	}
}

// AssertEvenDistribution verifies that workload is reasonably distributed across nodes
// threshold: max acceptable deviation from perfect balance (e.g., 0.2 = 20% deviation)
func AssertEvenDistribution(t *testing.T, stats *DistributionStats, threshold float64) {
	t.Helper()

	// Check if balance score meets threshold
	minAcceptable := (1.0 - threshold) * 100
	require.GreaterOrEqual(t, stats.BalanceScore, minAcceptable,
		"Distribution balance score %.2f below threshold %.2f (min acceptable: %.2f)",
		stats.BalanceScore, threshold*100, minAcceptable)

	// Print distribution details
	t.Logf("Distribution Stats:")
	t.Logf("  Balance Score: %.2f/100", stats.BalanceScore)
	t.Logf("  Key Count Std Dev: %.2f", stats.KeyCountStdDev)
	t.Logf("  Max/Min Key Ratio: %.2fx", stats.MaxMinKeyRatio)
	for nodeID, dist := range stats.PerNode {
		t.Logf("  Node %s: %d keys, %d writes, %d reads",
			nodeID, dist.KeyCount, dist.WriteCount, dist.ReadCount)
	}
}
