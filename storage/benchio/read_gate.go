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

// WrapPayloadReaderForBenchmark leaves readers unchanged outside benchmark builds.
func WrapPayloadReaderForBenchmark(reader io.Reader) io.Reader {
	return reader
}

// WrapPayloadReaderAtForBenchmark leaves random-access readers unchanged outside benchmark builds.
func WrapPayloadReaderAtForBenchmark(reader io.ReaderAt) io.ReaderAt {
	return reader
}
