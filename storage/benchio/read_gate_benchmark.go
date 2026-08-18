// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_benchmark

package benchio

import (
	"context"
	"io"
	"sync"

	"golang.org/x/time/rate"
)

const readGateBurst = 64 * 1024

var readGate struct {
	sync.RWMutex
	limiter *rate.Limiter
}

// SetReadRateLimitForBenchmark installs one shared payload-read budget for the
// benchmark process. It returns a function that restores the prior budget.
func SetReadRateLimitForBenchmark(bytesPerSecond int64) func() {
	readGate.Lock()
	previous := readGate.limiter
	if bytesPerSecond > 0 {
		readGate.limiter = rate.NewLimiter(rate.Limit(bytesPerSecond), readGateBurst)
	} else {
		readGate.limiter = nil
	}
	readGate.Unlock()

	return func() {
		readGate.Lock()
		readGate.limiter = previous
		readGate.Unlock()
	}
}

// WaitForReadBudget admits a payload read in bounded chunks so every raw-file
// and segment reader in the benchmark shares the same throughput cap.
func WaitForReadBudget(bytes int) error {
	readGate.RLock()
	limiter := readGate.limiter
	readGate.RUnlock()
	if limiter == nil {
		return nil
	}

	for bytes > 0 {
		chunk := bytes
		if chunk > readGateBurst {
			chunk = readGateBurst
		}
		if err := limiter.WaitN(context.Background(), chunk); err != nil {
			return err
		}
		bytes -= chunk
	}
	return nil
}

type benchmarkPayloadReader struct {
	reader io.Reader
}

func (r benchmarkPayloadReader) Read(p []byte) (int, error) {
	if err := WaitForReadBudget(len(p)); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

// WrapPayloadReaderForBenchmark charges direct compaction payload reads to the
// same benchmark-only lane used by foreground raw-file and segment readers.
func WrapPayloadReaderForBenchmark(reader io.Reader) io.Reader {
	return benchmarkPayloadReader{reader: reader}
}

type benchmarkPayloadReaderAt struct {
	reader io.ReaderAt
}

func (r benchmarkPayloadReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if err := WaitForReadBudget(len(p)); err != nil {
		return 0, err
	}
	return r.reader.ReadAt(p, offset)
}

// WrapPayloadReaderAtForBenchmark charges direct random-access compaction
// payload reads to the shared benchmark lane.
func WrapPayloadReaderAtForBenchmark(reader io.ReaderAt) io.ReaderAt {
	return benchmarkPayloadReaderAt{reader: reader}
}
