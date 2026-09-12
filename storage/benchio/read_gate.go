// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_benchmark

// Package benchio provides optional benchmark-only payload I/O controls.
package benchio

import "io"

// SetReadRateLimitForBenchmark is a no-op outside benchmark builds.
func SetReadRateLimitForBenchmark(_ int64) func() {
	return func() {}
}

// WaitForReadBudget is a no-op outside benchmark builds.
func WaitForReadBudget(_ int) error {
	return nil
}

// WaitForReadBudgetCancelable is the context-free build's no-op equivalent of
// the benchmark read gate. Production reads use their own cancellation
// boundary; this helper exists so benchmark wrappers share one interface.
func WaitForReadBudgetCancelable(_ <-chan struct{}, _ int) error {
	return nil
}

// WrapPayloadReaderForBenchmark leaves readers unchanged outside benchmark builds.
func WrapPayloadReaderForBenchmark(reader io.Reader) io.Reader {
	return reader
}

// WrapPayloadReaderAtForBenchmark leaves random-access readers unchanged outside benchmark builds.
func WrapPayloadReaderAtForBenchmark(reader io.ReaderAt) io.ReaderAt {
	return reader
}

// ResetPayloadStatsForBenchmark is a no-op outside benchmark builds.
func ResetPayloadStatsForBenchmark() {}

// MarkPayloadCancellationForBenchmark is a no-op outside benchmark builds.
func MarkPayloadCancellationForBenchmark() {}

// RecordListRowForBenchmark is a no-op outside benchmark builds.
func RecordListRowForBenchmark() {}

// RecordPayloadBytesForBenchmark is a no-op outside benchmark builds.
func RecordPayloadBytesForBenchmark(_ int) {}

// BeginListScanForBenchmark returns a no-op cleanup outside benchmark builds.
func BeginListScanForBenchmark() func() {
	return func() {}
}

// RecordPayloadReaderOpenedForBenchmark is a no-op outside benchmark builds.
func RecordPayloadReaderOpenedForBenchmark() {}

// RecordPayloadReaderClosedForBenchmark is a no-op outside benchmark builds.
func RecordPayloadReaderClosedForBenchmark() {}

// PostCancellationRowsForBenchmark returns zero outside benchmark builds.
func PostCancellationRowsForBenchmark() int64 { return 0 }

// PostCancellationBytesForBenchmark returns zero outside benchmark builds.
func PostCancellationBytesForBenchmark() int64 { return 0 }

// ActiveListScansForBenchmark returns zero outside benchmark builds.
func ActiveListScansForBenchmark() int64 { return 0 }

// ActivePayloadReadersForBenchmark returns zero outside benchmark builds.
func ActivePayloadReadersForBenchmark() int64 { return 0 }
