// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_benchmark

package files

import (
	"fmt"
	"io"

	"github.com/tigrisdata/ocache/storage/benchio"
)

type benchmarkReadCloser struct {
	io.ReadCloser
	seeker io.Seeker
}

func (r *benchmarkReadCloser) Read(p []byte) (int, error) {
	if err := benchio.WaitForReadBudget(len(p)); err != nil {
		return 0, err
	}
	return r.ReadCloser.Read(p)
}

func (r *benchmarkReadCloser) Seek(offset int64, whence int) (int64, error) {
	if r.seeker == nil {
		return 0, fmt.Errorf("benchmark-gated reader is not seekable")
	}
	return r.seeker.Seek(offset, whence)
}

func wrapReadForBenchmark(reader io.ReadCloser) io.ReadCloser {
	readerWithSeek, _ := reader.(io.Seeker)
	return &benchmarkReadCloser{ReadCloser: reader, seeker: readerWithSeek}
}
