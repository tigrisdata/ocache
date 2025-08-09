package ycsb

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// MetricsCollector collects detailed metrics during benchmark execution
type MetricsCollector struct {
	mu sync.RWMutex
	
	// Per-operation latencies
	opLatencies map[OpType][]time.Duration
	
	// Throughput time series (timestamp -> ops/sec)
	throughputSeries []ThroughputPoint
	
	// Error categorization
	errorsByType map[string]int
	errorsByOp   map[OpType]int
	
	// Histogram buckets for latency distribution
	latencyBuckets []HistogramBucket
	
	// Start time for time series
	startTime time.Time
}

// ThroughputPoint represents throughput at a point in time
type ThroughputPoint struct {
	Time      time.Duration // Time since start
	OpsPerSec float64
	OpType    OpType
}

// HistogramBucket represents a latency histogram bucket
type HistogramBucket struct {
	Min   time.Duration
	Max   time.Duration
	Count int
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		opLatencies:      make(map[OpType][]time.Duration),
		errorsByType:     make(map[string]int),
		errorsByOp:       make(map[OpType]int),
		throughputSeries: make([]ThroughputPoint, 0),
		startTime:        time.Now(),
		latencyBuckets:   initializeHistogramBuckets(),
	}
}

// initializeHistogramBuckets creates logarithmic buckets for latency histogram
func initializeHistogramBuckets() []HistogramBucket {
	buckets := []HistogramBucket{
		{0, 100 * time.Microsecond, 0},
		{100 * time.Microsecond, 250 * time.Microsecond, 0},
		{250 * time.Microsecond, 500 * time.Microsecond, 0},
		{500 * time.Microsecond, 1 * time.Millisecond, 0},
		{1 * time.Millisecond, 2 * time.Millisecond, 0},
		{2 * time.Millisecond, 5 * time.Millisecond, 0},
		{5 * time.Millisecond, 10 * time.Millisecond, 0},
		{10 * time.Millisecond, 20 * time.Millisecond, 0},
		{20 * time.Millisecond, 50 * time.Millisecond, 0},
		{50 * time.Millisecond, 100 * time.Millisecond, 0},
		{100 * time.Millisecond, 200 * time.Millisecond, 0},
		{200 * time.Millisecond, 500 * time.Millisecond, 0},
		{500 * time.Millisecond, 1 * time.Second, 0},
		{1 * time.Second, time.Duration(math.MaxInt64), 0}, // > 1s
	}
	return buckets
}

// RecordOperation records a single operation's metrics
func (mc *MetricsCollector) RecordOperation(opType OpType, latency time.Duration, err error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	// Record per-operation latency
	mc.opLatencies[opType] = append(mc.opLatencies[opType], latency)
	
	// Update histogram buckets
	for i := range mc.latencyBuckets {
		if latency >= mc.latencyBuckets[i].Min && latency < mc.latencyBuckets[i].Max {
			mc.latencyBuckets[i].Count++
			break
		}
	}
	
	// Record error if present
	if err != nil {
		errStr := err.Error()
		mc.errorsByType[errStr]++
		mc.errorsByOp[opType]++
	}
}

// RecordThroughput records throughput at current time
func (mc *MetricsCollector) RecordThroughput(opsPerSec float64, opType OpType) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	point := ThroughputPoint{
		Time:      time.Since(mc.startTime),
		OpsPerSec: opsPerSec,
		OpType:    opType,
	}
	mc.throughputSeries = append(mc.throughputSeries, point)
}

// GetPerOperationStats returns statistics for each operation type
func (mc *MetricsCollector) GetPerOperationStats() map[OpType]OperationStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	stats := make(map[OpType]OperationStats)
	
	for opType, latencies := range mc.opLatencies {
		if len(latencies) == 0 {
			continue
		}
		
		// Sort latencies for percentile calculation
		sorted := make([]time.Duration, len(latencies))
		copy(sorted, latencies)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i] < sorted[j]
		})
		
		stats[opType] = OperationStats{
			Count:       len(latencies),
			ErrorCount:  mc.errorsByOp[opType],
			MinLatency:  sorted[0],
			MaxLatency:  sorted[len(sorted)-1],
			AvgLatency:  calculateAverage(latencies),
			P50Latency:  percentile(sorted, 0.50),
			P95Latency:  percentile(sorted, 0.95),
			P99Latency:  percentile(sorted, 0.99),
			P999Latency: percentile(sorted, 0.999),
		}
	}
	
	return stats
}

// OperationStats holds statistics for a single operation type
type OperationStats struct {
	Count       int
	ErrorCount  int
	MinLatency  time.Duration
	MaxLatency  time.Duration
	AvgLatency  time.Duration
	P50Latency  time.Duration
	P95Latency  time.Duration
	P99Latency  time.Duration
	P999Latency time.Duration
}

// GetHistogram returns the latency histogram
func (mc *MetricsCollector) GetHistogram() []HistogramBucket {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	result := make([]HistogramBucket, len(mc.latencyBuckets))
	copy(result, mc.latencyBuckets)
	return result
}

// GetErrorBreakdown returns detailed error categorization
func (mc *MetricsCollector) GetErrorBreakdown() (byType map[string]int, byOp map[OpType]int) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	// Return copies
	byType = make(map[string]int)
	for k, v := range mc.errorsByType {
		byType[k] = v
	}
	
	byOp = make(map[OpType]int)
	for k, v := range mc.errorsByOp {
		byOp[k] = v
	}
	
	return byType, byOp
}

// GetThroughputSeries returns the throughput time series
func (mc *MetricsCollector) GetThroughputSeries() []ThroughputPoint {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	result := make([]ThroughputPoint, len(mc.throughputSeries))
	copy(result, mc.throughputSeries)
	return result
}

// calculateAverage calculates the average of durations
func calculateAverage(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations))
}

// RenderHistogram creates an ASCII histogram visualization
func RenderHistogram(buckets []HistogramBucket) string {
	if len(buckets) == 0 {
		return "No data"
	}
	
	// Find max count for scaling
	maxCount := 0
	totalCount := 0
	for _, b := range buckets {
		if b.Count > maxCount {
			maxCount = b.Count
		}
		totalCount += b.Count
	}
	
	if maxCount == 0 {
		return "No data"
	}
	
	const maxBarWidth = 40
	var result strings.Builder
	
	result.WriteString("Latency Distribution:\n")
	result.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	
	for _, bucket := range buckets {
		if bucket.Count == 0 {
			continue
		}
		
		// Format bucket range
		var label string
		if bucket.Max == time.Duration(math.MaxInt64) {
			label = fmt.Sprintf("> %s", formatDuration(bucket.Min))
		} else {
			label = fmt.Sprintf("%s - %s", formatDuration(bucket.Min), formatDuration(bucket.Max))
		}
		
		// Calculate bar width
		barWidth := int(float64(bucket.Count) / float64(maxCount) * maxBarWidth)
		if barWidth == 0 && bucket.Count > 0 {
			barWidth = 1
		}
		
		// Calculate percentage
		percentage := float64(bucket.Count) / float64(totalCount) * 100
		
		// Create bar
		bar := strings.Repeat("█", barWidth)
		
		// Format line
		result.WriteString(fmt.Sprintf("%-20s │ %-40s %6d (%5.1f%%)\n",
			label, bar, bucket.Count, percentage))
	}
	
	return result.String()
}

// formatDuration formats a duration for display
func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.0fμs", float64(d.Microseconds()))
	} else if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// ThroughputSummary provides summary of throughput over time
type ThroughputSummary struct {
	MinThroughput     float64
	MaxThroughput     float64
	AvgThroughput     float64
	StdDevThroughput  float64
	ThroughputTrend   string // "increasing", "decreasing", "stable"
}

// AnalyzeThroughput analyzes throughput time series
func AnalyzeThroughput(series []ThroughputPoint) ThroughputSummary {
	if len(series) == 0 {
		return ThroughputSummary{}
	}
	
	var min, max, sum float64
	min = math.MaxFloat64
	
	for _, point := range series {
		if point.OpsPerSec < min {
			min = point.OpsPerSec
		}
		if point.OpsPerSec > max {
			max = point.OpsPerSec
		}
		sum += point.OpsPerSec
	}
	
	avg := sum / float64(len(series))
	
	// Calculate standard deviation
	var varianceSum float64
	for _, point := range series {
		diff := point.OpsPerSec - avg
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(len(series)))
	
	// Determine trend
	trend := "stable"
	if len(series) > 10 {
		// Compare first and last quartile averages
		quartileSize := len(series) / 4
		var firstSum, lastSum float64
		
		for i := 0; i < quartileSize; i++ {
			firstSum += series[i].OpsPerSec
			lastSum += series[len(series)-quartileSize+i].OpsPerSec
		}
		
		firstAvg := firstSum / float64(quartileSize)
		lastAvg := lastSum / float64(quartileSize)
		
		if lastAvg > firstAvg*1.1 {
			trend = "increasing"
		} else if lastAvg < firstAvg*0.9 {
			trend = "decreasing"
		}
	}
	
	return ThroughputSummary{
		MinThroughput:    min,
		MaxThroughput:    max,
		AvgThroughput:    avg,
		StdDevThroughput: stdDev,
		ThroughputTrend:  trend,
	}
}