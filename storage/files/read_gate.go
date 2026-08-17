// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_benchmark

package files

import "io"

func wrapReadForBenchmark(reader io.ReadCloser) io.ReadCloser {
	return reader
}
