// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func expectedOperationStats(durations []time.Duration, errorCount int) OperationStats {
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	return OperationStats{
		Count:       len(durations),
		ErrorCount:  errorCount,
		MinLatency:  sorted[0],
		MaxLatency:  sorted[len(sorted)-1],
		AvgLatency:  calculateAverage(durations),
		P50Latency:  percentile(sorted, 0.50),
		P95Latency:  percentile(sorted, 0.95),
		P99Latency:  percentile(sorted, 0.99),
		P999Latency: percentile(sorted, 0.999),
	}
}

func TestGetPerOperationStatsPreservesStatistics(t *testing.T) {
	collector := NewMetricsCollector()
	readLatencies := []time.Duration{
		4 * time.Millisecond,
		1 * time.Millisecond,
		3 * time.Millisecond,
		2 * time.Millisecond,
	}
	for i, latency := range readLatencies {
		var err error
		if i == 1 {
			err = errMetricsTest
		}
		collector.RecordOperation(OpRead, latency, err)
	}
	collector.RecordOperation(OpUpdate, 8*time.Millisecond, nil)

	want := map[OpType]OperationStats{
		OpRead:   expectedOperationStats(readLatencies, 1),
		OpUpdate: expectedOperationStats([]time.Duration{8 * time.Millisecond}, 0),
	}
	if got := collector.GetPerOperationStats(); !reflect.DeepEqual(got, want) {
		t.Fatalf("GetPerOperationStats() = %#v, want %#v", got, want)
	}
	if got := collector.GetPerOperationStats(); !reflect.DeepEqual(got, want) {
		t.Fatalf("second GetPerOperationStats() = %#v, want %#v", got, want)
	}

	readLatencies = append(readLatencies, 500*time.Microsecond)
	collector.RecordOperation(OpRead, 500*time.Microsecond, nil)
	got := collector.GetPerOperationStats()[OpRead]
	wantRead := expectedOperationStats(readLatencies, 1)
	if !reflect.DeepEqual(got, wantRead) {
		t.Fatalf("GetPerOperationStats()[OpRead] after append = %#v, want %#v", got, wantRead)
	}
}

var errMetricsTest = metricsTestError("test error")

type metricsTestError string

func (e metricsTestError) Error() string { return string(e) }

func TestMetricsCollectorConcurrentRecordingAndStats(t *testing.T) {
	const (
		writers       = 4
		recordsPerRun = 100
		readers       = 2
	)

	collector := NewMetricsCollector()
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(writers + readers)
	for writer := range writers {
		go func(writer int) {
			defer wg.Done()
			<-start
			for i := range recordsPerRun {
				collector.RecordOperation(OpType(writer%int(OpNum)), time.Duration(i+writer)*time.Nanosecond, nil)
			}
		}(writer)
	}
	for range readers {
		go func() {
			defer wg.Done()
			<-start
			for range recordsPerRun {
				collector.GetPerOperationStats()
			}
		}()
	}

	close(start)
	wg.Wait()

	stats := collector.GetPerOperationStats()
	count := 0
	for _, operationStats := range stats {
		count += operationStats.Count
	}
	if count != writers*recordsPerRun {
		t.Fatalf("recorded count = %d, want %d", count, writers*recordsPerRun)
	}
}

func BenchmarkGetPerOperationStats(b *testing.B) {
	for _, testCase := range []struct {
		name  string
		total int
	}{
		{name: "1K", total: 1 << 10},
		{name: "100K", total: 100 << 10},
		{name: "1M", total: 1 << 20},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			readCount := testCase.total / 2
			updateCount := testCase.total - readCount
			readFixture := descendingLatencies(readCount)
			updateFixture := descendingLatencies(updateCount)

			collector := NewMetricsCollector()
			collector.opLatencies[OpRead] = append([]time.Duration(nil), readFixture...)
			collector.opLatencies[OpUpdate] = append([]time.Duration(nil), updateFixture...)

			b.ReportAllocs()
			for b.Loop() {
				// Restore the collector to the same unsorted post-recording state
				// before each report without including setup in the measurement.
				b.StopTimer()
				copy(collector.opLatencies[OpRead], readFixture)
				copy(collector.opLatencies[OpUpdate], updateFixture)
				b.StartTimer()

				stats := collector.GetPerOperationStats()
				if len(stats) != 2 {
					b.Fatalf("GetPerOperationStats() returned %d operation types, want 2", len(stats))
				}
			}
		})
	}
}

func BenchmarkRunYCSBFinalReport(b *testing.B) {
	disablePtermOutput(b)
	_, addr := startYCSBReadServer(b)

	for _, testCase := range []struct {
		name   string
		numOps int
	}{
		{name: "1K", numOps: 1 << 10},
		{name: "100K", numOps: 100 << 10},
		{name: "1M", numOps: 1 << 20},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			cfg := readOnlyYCSBConfig(addr, 1)
			cfg.NumOps = testCase.numOps
			cfg.Concurrency = 8
			cfg.ConnectionPoolSize = 8

			originalStats := getPerOperationStatsForReport
			var statsDuration time.Duration
			var statsCount int
			getPerOperationStatsForReport = func(metrics *MetricsCollector) map[OpType]OperationStats {
				started := time.Now()
				stats := metrics.GetPerOperationStats()
				statsDuration += time.Since(started)
				statsCount++
				return stats
			}
			defer func() { getPerOperationStatsForReport = originalStats }()

			for b.Loop() {
				result, err := RunYCSBWithContext(context.Background(), cfg)
				if err != nil {
					b.Fatal(err)
				}
				if result.Errors != 0 {
					b.Fatalf("RunYCSBWithContext reported %d errors", result.Errors)
				}
			}
			if statsCount == 0 {
				b.Fatal("RunYCSBWithContext did not collect final-report statistics")
			}
			b.ReportMetric(float64(statsDuration)/float64(statsCount), "stats-ns/op")
		})
	}
}

// BenchmarkDisplayFinalResultsWithMetrics isolates the final report after a
// completed read-only YCSB run. The fixture is restored to its unsorted,
// post-recording state outside the timed report so each iteration has the same
// work as the first report.
func BenchmarkDisplayFinalResultsWithMetrics(b *testing.B) {
	disablePtermOutput(b)

	for _, testCase := range []struct {
		name   string
		numOps int
	}{
		{name: "1K", numOps: 1 << 10},
		{name: "100K", numOps: 100 << 10},
		{name: "1M", numOps: 1 << 20},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			cfg := YCSBConfig{NumOps: testCase.numOps, ValueSize: 1, Workload: "C"}
			result := Result{Ops: testCase.numOps, Duration: time.Second}
			totalOps := make([]int, OpNum)
			totalOps[OpRead] = testCase.numOps

			fixture := descendingLatencies(testCase.numOps)
			collector := NewMetricsCollector()
			collector.opLatencies[OpRead] = append([]time.Duration(nil), fixture...)

			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				copy(collector.opLatencies[OpRead], fixture)
				b.StartTimer()

				DisplayFinalResultsWithMetrics(cfg, result, totalOps, collector)
			}
		})
	}
}

func descendingLatencies(count int) []time.Duration {
	latencies := make([]time.Duration, count)
	for i := range latencies {
		latencies[i] = time.Duration(count-i) * time.Microsecond
	}
	return latencies
}
