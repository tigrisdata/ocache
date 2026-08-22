// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkRunYCSBWithContextFinalReport(b *testing.B) {
	disablePtermOutput(b)
	_, addr := startYCSBReadServer(b)

	for _, sampleCount := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("samples=%d", sampleCount), func(b *testing.B) {
			for _, workload := range []struct {
				name string
				mix  string
			}{
				{name: "one-operation", mix: "C"},
				{name: "mixed-operation", mix: "A"},
			} {
				b.Run(workload.name, func(b *testing.B) {
					cfg := readOnlyYCSBConfig(addr, 1)
					cfg.ConnectionPoolSize = 1
					cfg.Concurrency = 1
					cfg.NumOps = sampleCount
					cfg.Workload = workload.mix

					b.ResetTimer()
					var finalReportDuration time.Duration
					for b.Loop() {
						started := time.Now()
						result, err := RunYCSBWithContext(context.Background(), cfg)
						if err != nil {
							b.Fatal(err)
						}
						if result.Errors != 0 {
							b.Fatalf("RunYCSBWithContext reported %d errors", result.Errors)
						}

						// Result.Duration stops before DisplayFinalResultsWithMetrics.
						finalReportDuration += time.Since(started) - result.Duration
					}
					// The ordinary ns/op value includes the whole YCSB run. Emit only the
					// completion metric outside Result.Duration for this benchmark.
					b.ReportMetric(0, "ns/op")
					b.ReportMetric(float64(finalReportDuration)/float64(b.N), "report-ns/op")
				})
			}
		})
	}
}
