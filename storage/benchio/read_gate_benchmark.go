// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_benchmark

package benchio

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const readGateBurst = 64 * 1024

type readBlock struct {
	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

var readGate struct {
	sync.RWMutex
	limiter *rate.Limiter
	block   *readBlock
}

var benchmarkReadStats struct {
	canceled        atomic.Bool
	postCancelRows  atomic.Int64
	postCancelBytes atomic.Int64
	activeScans     atomic.Int64
	activeReaders   atomic.Int64
}

// ResetPayloadStatsForBenchmark clears the counters for one measured list page.
func ResetPayloadStatsForBenchmark() {
	benchmarkReadStats.canceled.Store(false)
	benchmarkReadStats.postCancelRows.Store(0)
	benchmarkReadStats.postCancelBytes.Store(0)
	benchmarkReadStats.activeScans.Store(0)
	benchmarkReadStats.activeReaders.Store(0)
}

// MarkPayloadCancellationForBenchmark marks the request cancellation boundary.
func MarkPayloadCancellationForBenchmark() {
	benchmarkReadStats.canceled.Store(true)
}

// RecordListRowForBenchmark records rows visited after cancellation.
func RecordListRowForBenchmark() {
	if benchmarkReadStats.canceled.Load() {
		benchmarkReadStats.postCancelRows.Add(1)
	}
}

// RecordPayloadBytesForBenchmark records payload bytes returned after cancellation.
func RecordPayloadBytesForBenchmark(bytes int) {
	if benchmarkReadStats.canceled.Load() {
		benchmarkReadStats.postCancelBytes.Add(int64(bytes))
	}
}

// BeginListScanForBenchmark tracks the lifetime of a storage list iterator.
func BeginListScanForBenchmark() func() {
	benchmarkReadStats.activeScans.Add(1)
	return func() { benchmarkReadStats.activeScans.Add(-1) }
}

// RecordPayloadReaderOpenedForBenchmark tracks a foreground payload reader.
func RecordPayloadReaderOpenedForBenchmark() {
	benchmarkReadStats.activeReaders.Add(1)
}

// RecordPayloadReaderClosedForBenchmark tracks a closed foreground payload reader.
func RecordPayloadReaderClosedForBenchmark() {
	benchmarkReadStats.activeReaders.Add(-1)
}

func PostCancellationRowsForBenchmark() int64 {
	return benchmarkReadStats.postCancelRows.Load()
}

func PostCancellationBytesForBenchmark() int64 {
	return benchmarkReadStats.postCancelBytes.Load()
}

func ActiveListScansForBenchmark() int64 {
	return benchmarkReadStats.activeScans.Load()
}

func ActivePayloadReadersForBenchmark() int64 {
	return benchmarkReadStats.activeReaders.Load()
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

// BlockPayloadReadsForBenchmark holds every foreground payload read at the
// read boundary until Release is called or the reader's interrupt channel is
// closed. It returns the first-read notification, a release function, and a
// restore function for the previous gate state.
func BlockPayloadReadsForBenchmark() (started <-chan struct{}, release func(), restore func()) {
	block := &readBlock{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	readGate.Lock()
	previous := readGate.block
	readGate.block = block
	readGate.Unlock()

	release = func() {
		block.releaseOnce.Do(func() { close(block.release) })
	}
	restore = func() {
		readGate.Lock()
		if readGate.block == block {
			readGate.block = previous
		}
		readGate.Unlock()
	}
	return block.started, release, restore
}

// WaitForReadBudget admits a payload read in bounded chunks so every raw-file
// and segment reader in the benchmark shares the same throughput cap.
func WaitForReadBudget(bytes int) error {
	return WaitForReadBudgetCancelable(nil, bytes)
}

// WaitForReadBudgetCancelable waits at the benchmark payload boundary while
// also allowing the private list reader to wake it through cancellation.
func WaitForReadBudgetCancelable(cancel <-chan struct{}, bytes int) error {
	readGate.RLock()
	block := readGate.block
	limiter := readGate.limiter
	readGate.RUnlock()

	if block != nil {
		block.startOnce.Do(func() { close(block.started) })
		if cancel == nil {
			<-block.release
		} else {
			select {
			case <-block.release:
			case <-cancel:
				return context.Canceled
			}
		}
	}
	if limiter == nil {
		return nil
	}

	for bytes > 0 {
		chunk := bytes
		if chunk > readGateBurst {
			chunk = readGateBurst
		}

		reservation := limiter.ReserveN(time.Now(), chunk)
		if !reservation.OK() {
			return fmt.Errorf("read budget reservation exceeds burst")
		}
		delay := reservation.Delay()
		if delay > 0 {
			timer := time.NewTimer(delay)
			if cancel == nil {
				<-timer.C
			} else {
				select {
				case <-timer.C:
				case <-cancel:
					if !timer.Stop() {
						<-timer.C
					}
					reservation.CancelAt(time.Now())
					return context.Canceled
				}
			}
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
