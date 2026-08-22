// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"
)

func TestGetPerOperationStatsMatchesSortedReference(t *testing.T) {
	boundarySizes := []int{1, 2, 3, 4, 5, 20, 21, 99, 100, 101, 999, 1000, 1001, 1002}
	testCases := []struct {
		name       string
		latencies  func(int) map[OpType][]time.Duration
		errorCount map[OpType]int
	}{
		{
			name: "one-operation",
			latencies: func(count int) map[OpType][]time.Duration {
				return map[OpType][]time.Duration{
					OpRead: metricsTestDurations(count, int64(count)),
				}
			},
			errorCount: map[OpType]int{OpRead: 3},
		},
		{
			name: "mixed-operation",
			latencies: func(count int) map[OpType][]time.Duration {
				return map[OpType][]time.Duration{
					OpRead:   orderedMetricsTestDurations(count),
					OpUpdate: metricsTestDurations(count+1, int64(count)*17),
				}
			},
			errorCount: map[OpType]int{OpRead: 3, OpUpdate: 5},
		},
	}

	for _, sampleCount := range boundarySizes {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s/samples=%d", tc.name, sampleCount), func(t *testing.T) {
				original := tc.latencies(sampleCount)
				collector := &MetricsCollector{
					opLatencies: cloneOperationLatencies(original),
					errorsByOp:  tc.errorCount,
				}

				want := make(map[OpType]OperationStats, len(original))
				for opType, latencies := range original {
					want[opType] = sortedReferenceStats(latencies, tc.errorCount[opType])
				}

				got := collector.GetPerOperationStats()
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("GetPerOperationStats() = %#v, want %#v", got, want)
				}

				for opType, wantLatencies := range original {
					if gotLatencies := collector.opLatencies[opType]; !slices.Equal(gotLatencies, wantLatencies) {
						t.Fatalf("GetPerOperationStats mutated %s latencies: got %v, want %v", opNames[opType], gotLatencies, wantLatencies)
					}
				}
			})
		}
	}
}

func cloneOperationLatencies(source map[OpType][]time.Duration) map[OpType][]time.Duration {
	clone := make(map[OpType][]time.Duration, len(source))
	for opType, latencies := range source {
		clone[opType] = slices.Clone(latencies)
	}
	return clone
}

func sortedReferenceStats(latencies []time.Duration, errorCount int) OperationStats {
	sorted := slices.Clone(latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	return OperationStats{
		Count:       len(latencies),
		ErrorCount:  errorCount,
		MinLatency:  sorted[0],
		MaxLatency:  sorted[len(sorted)-1],
		AvgLatency:  referenceAverage(latencies),
		P50Latency:  referencePercentile(sorted, 0.50),
		P95Latency:  referencePercentile(sorted, 0.95),
		P99Latency:  referencePercentile(sorted, 0.99),
		P999Latency: referencePercentile(sorted, 0.999),
	}
}

func referenceAverage(durations []time.Duration) time.Duration {
	var sum time.Duration
	for _, duration := range durations {
		sum += duration
	}
	return sum / time.Duration(len(durations))
}

func referencePercentile(sorted []time.Duration, p float64) time.Duration {
	pos := p * float64(len(sorted)-1)
	lo := int(pos)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}

	frac := pos - float64(lo)
	loValue := float64(sorted[lo])
	hiValue := float64(sorted[hi])
	return time.Duration(loValue + frac*(hiValue-loValue))
}

func metricsTestDurations(count int, seed int64) []time.Duration {
	values := make([]time.Duration, count)
	edgeCases := []time.Duration{
		time.Duration(-1 << 63),
		-1,
		0,
		1,
		time.Duration(1<<63 - 1),
		-500 * time.Millisecond,
		500 * time.Millisecond,
	}
	rng := rand.New(rand.NewSource(seed))

	for i := range values {
		if i < len(edgeCases) {
			values[i] = edgeCases[i]
			continue
		}

		switch {
		case i%7 == 0:
			values[i] = 500 * time.Microsecond
		case i%5 == 0:
			values[i] = -500 * time.Microsecond
		default:
			value := time.Duration(rng.Int63())
			if i%2 == 0 {
				value = -value
			}
			values[i] = value
		}
	}
	return values
}

func orderedMetricsTestDurations(count int) []time.Duration {
	values := make([]time.Duration, count)
	for i := range values {
		values[i] = time.Duration(i-count/2) * time.Microsecond
	}
	return values
}
