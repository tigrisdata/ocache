package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPattern represents a common test pattern that can be reused
type TestPattern interface {
	Name() string
	Setup(t *testing.T, h *IntegrationTestHarness)
	Execute(t *testing.T, h *IntegrationTestHarness)
	Verify(t *testing.T, h *IntegrationTestHarness)
	Cleanup(t *testing.T, h *IntegrationTestHarness)
}

// MixedWorkloadPattern tests mixed read/write/delete operations
type MixedWorkloadPattern struct {
	NumWorkers    int
	NumOperations int
	ObjectSizes   []int64
	TTLRange      [2]int64 // Min and max TTL
}

func (p *MixedWorkloadPattern) Name() string {
	return "MixedWorkload"
}

func (p *MixedWorkloadPattern) Setup(t *testing.T, h *IntegrationTestHarness) {
	// Pre-populate some objects for reads
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("mixed-init-%d", i)
		data := GenerateRandomData(p.ObjectSizes[i%len(p.ObjectSizes)])
		err := h.PutObject(key, data, 0)
		require.NoError(t, err)
	}
}

func (p *MixedWorkloadPattern) Execute(t *testing.T, h *IntegrationTestHarness) {
	var wg sync.WaitGroup
	errors := make(chan error, p.NumOperations)
	
	for w := 0; w < p.NumWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for i := 0; i < p.NumOperations/p.NumWorkers; i++ {
				op := i % 3
				key := fmt.Sprintf("mixed-%d-%d", workerID, i)
				
				switch op {
				case 0: // Write
					size := p.ObjectSizes[i%len(p.ObjectSizes)]
					data := GenerateRandomData(size)
					ttl := p.TTLRange[0] + int64(i)%(p.TTLRange[1]-p.TTLRange[0]+1)
					err := h.PutObject(key, data, ttl)
					if err != nil {
						errors <- fmt.Errorf("write failed: %w", err)
					}
				case 1: // Read
					readKey := fmt.Sprintf("mixed-%d-%d", workerID, i-1)
					_, err := h.GetObject(readKey)
					// Ignore not found errors
					if err != nil && !IsNotFoundError(err) {
						errors <- fmt.Errorf("read failed: %w", err)
					}
				case 2: // Delete
					deleteKey := fmt.Sprintf("mixed-%d-%d", workerID, i-2)
					err := h.DeleteObject(deleteKey)
					if err != nil {
						errors <- fmt.Errorf("delete failed: %w", err)
					}
				}
			}
		}(w)
	}
	
	wg.Wait()
	close(errors)
	
	// Check for errors
	errorCount := 0
	for err := range errors {
		errorCount++
		t.Logf("Operation error: %v", err)
	}
	
	assert.Less(t, errorCount, p.NumOperations/10, "Less than 10%% of operations should fail")
}

func (p *MixedWorkloadPattern) Verify(t *testing.T, h *IntegrationTestHarness) {
	// Verify storage stats
	stats := h.GetStorageStats()
	assert.Greater(t, stats.TotalKeys, 0, "Should have objects in storage")
}

func (p *MixedWorkloadPattern) Cleanup(t *testing.T, h *IntegrationTestHarness) {
	// Cleanup is handled by test harness
}

// CompactionPattern tests compaction behavior
type CompactionPattern struct {
	NumObjects        int
	ObjectSize        int64
	CompactionDelay   time.Duration
	ExpectedSegments  int
}

func (p *CompactionPattern) Name() string {
	return "Compaction"
}

func (p *CompactionPattern) Setup(t *testing.T, h *IntegrationTestHarness) {
	// Create objects eligible for compaction
	for i := 0; i < p.NumObjects; i++ {
		key := fmt.Sprintf("compact-%d", i)
		data := GenerateRandomData(p.ObjectSize)
		err := h.PutObject(key, data, 0)
		require.NoError(t, err)
	}
}

func (p *CompactionPattern) Execute(t *testing.T, h *IntegrationTestHarness) {
	// Wait for compaction to occur
	time.Sleep(p.CompactionDelay)
}

func (p *CompactionPattern) Verify(t *testing.T, h *IntegrationTestHarness) {
	// Verify segments were created
	if p.ExpectedSegments > 0 {
		VerifySegmentsExist(t, h.TempDir, p.ExpectedSegments)
	}
	
	// Verify all objects are still accessible
	for i := 0; i < p.NumObjects; i++ {
		key := fmt.Sprintf("compact-%d", i)
		_, err := h.GetObject(key)
		assert.NoError(t, err, "Object %s should be accessible after compaction", key)
	}
}

func (p *CompactionPattern) Cleanup(t *testing.T, h *IntegrationTestHarness) {
	// Cleanup handled by harness
}

// TTLExpirationPattern tests TTL expiration
type TTLExpirationPattern struct {
	Objects []struct {
		Key  string
		Size int64
		TTL  int64
	}
	WaitTime time.Duration
}

func (p *TTLExpirationPattern) Name() string {
	return "TTLExpiration"
}

func (p *TTLExpirationPattern) Setup(t *testing.T, h *IntegrationTestHarness) {
	// Create objects with various TTLs
	for _, obj := range p.Objects {
		data := GenerateRandomData(obj.Size)
		err := h.PutObject(obj.Key, data, obj.TTL)
		require.NoError(t, err)
	}
}

func (p *TTLExpirationPattern) Execute(t *testing.T, h *IntegrationTestHarness) {
	// Wait for TTL expiration
	time.Sleep(p.WaitTime)
}

func (p *TTLExpirationPattern) Verify(t *testing.T, h *IntegrationTestHarness) {
	// Verify expired and non-expired objects
	for _, obj := range p.Objects {
		_, err := h.GetObject(obj.Key)
		if obj.TTL > 0 && obj.TTL < int64(p.WaitTime.Seconds()) {
			assert.Error(t, err, "Object %s should be expired", obj.Key)
		} else {
			assert.NoError(t, err, "Object %s should still exist", obj.Key)
		}
	}
}

func (p *TTLExpirationPattern) Cleanup(t *testing.T, h *IntegrationTestHarness) {
	// Cleanup handled by TTL
}

// StreamingPattern tests streaming operations
type StreamingPattern struct {
	ObjectSize   int64
	ChunkSize    int
	NumChunks    int
	Concurrent   bool
}

func (p *StreamingPattern) Name() string {
	return "Streaming"
}

func (p *StreamingPattern) Setup(t *testing.T, h *IntegrationTestHarness) {
	// Nothing to setup
}

func (p *StreamingPattern) Execute(t *testing.T, h *IntegrationTestHarness) {
	if p.Concurrent {
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				key := fmt.Sprintf("stream-concurrent-%d", id)
				p.executeStreamingWrite(t, h, key)
			}(i)
		}
		wg.Wait()
	} else {
		key := "stream-single"
		p.executeStreamingWrite(t, h, key)
	}
}

func (p *StreamingPattern) executeStreamingWrite(t *testing.T, h *IntegrationTestHarness, key string) {
	data := GenerateRandomData(p.ObjectSize)
	
	// Simulate streaming write (store in chunks)
	// For now, just use regular PutObject as streaming is not implemented in harness
	err := h.PutObject(key, data, 0)
	require.NoError(t, err)
	
	// Verify streaming read
	VerifyStreamingRead(t, h.Storage, key, p.ObjectSize)
}

func (p *StreamingPattern) Verify(t *testing.T, h *IntegrationTestHarness) {
	// Verification done in Execute
}

func (p *StreamingPattern) Cleanup(t *testing.T, h *IntegrationTestHarness) {
	// Cleanup handled by harness
}

// RunTestPattern executes a test pattern
func RunTestPattern(t *testing.T, h *IntegrationTestHarness, pattern TestPattern) {
	t.Run(pattern.Name(), func(t *testing.T) {
		pattern.Setup(t, h)
		pattern.Execute(t, h)
		pattern.Verify(t, h)
		pattern.Cleanup(t, h)
	})
}

// BatchTestRunner runs multiple test patterns in sequence or parallel
type BatchTestRunner struct {
	Patterns []TestPattern
	Parallel bool
}

func (r *BatchTestRunner) Run(t *testing.T, h *IntegrationTestHarness) {
	if r.Parallel {
		// Use t.Run for parallel test execution to avoid race conditions
		// as testing.T is not thread-safe
		for _, pattern := range r.Patterns {
			pattern := pattern // capture loop variable
			t.Run(pattern.Name(), func(t *testing.T) {
				t.Parallel()
				RunTestPattern(t, h, pattern)
			})
		}
	} else {
		for _, pattern := range r.Patterns {
			RunTestPattern(t, h, pattern)
		}
	}
}

// PerformanceMetrics tracks performance during test execution
type PerformanceMetrics struct {
	StartTime      time.Time
	EndTime        time.Time
	OperationCount atomic.Int64
	BytesWritten   atomic.Int64
	BytesRead      atomic.Int64
	Errors         atomic.Int64
}

func (m *PerformanceMetrics) Start() {
	m.StartTime = time.Now()
}

func (m *PerformanceMetrics) Stop() {
	m.EndTime = time.Now()
}

func (m *PerformanceMetrics) Duration() time.Duration {
	return m.EndTime.Sub(m.StartTime)
}

func (m *PerformanceMetrics) OpsPerSecond() float64 {
	duration := m.Duration().Seconds()
	if duration == 0 {
		return 0
	}
	return float64(m.OperationCount.Load()) / duration
}

func (m *PerformanceMetrics) Throughput() float64 {
	duration := m.Duration().Seconds()
	if duration == 0 {
		return 0
	}
	totalBytes := m.BytesWritten.Load() + m.BytesRead.Load()
	return float64(totalBytes) / duration / (1024 * 1024) // MB/s
}

func (m *PerformanceMetrics) ErrorRate() float64 {
	total := m.OperationCount.Load()
	if total == 0 {
		return 0
	}
	return float64(m.Errors.Load()) / float64(total) * 100
}

func (m *PerformanceMetrics) Report(t *testing.T) {
	t.Logf("Performance Metrics:")
	t.Logf("  Duration: %v", m.Duration())
	t.Logf("  Operations: %d", m.OperationCount.Load())
	t.Logf("  Ops/sec: %.2f", m.OpsPerSecond())
	t.Logf("  Throughput: %.2f MB/s", m.Throughput())
	t.Logf("  Error rate: %.2f%%", m.ErrorRate())
	t.Logf("  Bytes written: %d", m.BytesWritten.Load())
	t.Logf("  Bytes read: %d", m.BytesRead.Load())
}

// CommonTestScenarios provides pre-configured test scenarios
var CommonTestScenarios = struct {
	SmallObjects   []ObjectSizeTestCase
	MediumObjects  []ObjectSizeTestCase
	LargeObjects   []ObjectSizeTestCase
	BoundaryObjects []ObjectSizeTestCase
}{
	SmallObjects: []ObjectSizeTestCase{
		{Name: "1B", Size: 1, Category: "small"},
		{Name: "1KB", Size: 1024, Category: "small"},
		{Name: "32KB", Size: 32 * 1024, Category: "small"},
		{Name: "64KB", Size: 64 * 1024, Category: "small"},
	},
	MediumObjects: []ObjectSizeTestCase{
		{Name: "100KB", Size: 100 * 1024, Category: "medium"},
		{Name: "1MB", Size: 1024 * 1024, Category: "medium"},
		{Name: "10MB", Size: 10 * 1024 * 1024, Category: "medium"},
		{Name: "16MB", Size: 16 * 1024 * 1024, Category: "medium"},
	},
	LargeObjects: []ObjectSizeTestCase{
		{Name: "20MB", Size: 20 * 1024 * 1024, Category: "large"},
		{Name: "50MB", Size: 50 * 1024 * 1024, Category: "large"},
		{Name: "100MB", Size: 100 * 1024 * 1024, Category: "large"},
	},
	BoundaryObjects: []ObjectSizeTestCase{
		{Name: "64KB-1", Size: 64*1024 - 1, Category: "small"},
		{Name: "64KB+1", Size: 64*1024 + 1, Category: "medium"},
		{Name: "16MB-1", Size: 16*1024*1024 - 1, Category: "medium"},
		{Name: "16MB+1", Size: 16*1024*1024 + 1, Category: "large"},
	},
}