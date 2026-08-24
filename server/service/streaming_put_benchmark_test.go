// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
)

const (
	streamedPutFirstChunkSize   = stor.DefaultInlineThreshold + 1
	streamedPutBenchmarkWorkers = 4
)

// benchmarkPutStream drives CacheService.Put through its ordinary local
// streaming path. Its first request crosses Storage's inline threshold; the
// second request is consumed from the io.PipeReader created by handleLocalPut.
type benchmarkPutStream struct {
	pb.CacheService_PutServer
	ctx                context.Context
	requests           []*pb.PutRequest
	next               int
	secondChunkReached chan<- struct{}
	releaseSecondChunk <-chan struct{}
	response           *pb.PutResponse
}

func (s *benchmarkPutStream) Context() context.Context {
	return s.ctx
}

func (s *benchmarkPutStream) Recv() (*pb.PutRequest, error) {
	if s.next == len(s.requests) {
		return nil, io.EOF
	}
	if s.next == 1 && s.secondChunkReached != nil {
		s.secondChunkReached <- struct{}{}
		<-s.releaseSecondChunk
	}
	request := s.requests[s.next]
	s.next++
	return request, nil
}

func (s *benchmarkPutStream) SendAndClose(response *pb.PutResponse) error {
	s.response = response
	return nil
}

func newBenchmarkPutStream(ctx context.Context, key string, firstChunk, remainder []byte) *benchmarkPutStream {
	return newBlockedBenchmarkPutStream(ctx, key, firstChunk, remainder, nil, nil)
}

func newBlockedBenchmarkPutStream(
	ctx context.Context,
	key string,
	firstChunk, remainder []byte,
	secondChunkReached chan<- struct{},
	releaseSecondChunk <-chan struct{},
) *benchmarkPutStream {
	return &benchmarkPutStream{
		ctx:                ctx,
		secondChunkReached: secondChunkReached,
		releaseSecondChunk: releaseSecondChunk,
		requests: []*pb.PutRequest{
			{Key: key, Data: firstChunk},
			{Data: remainder},
		},
	}
}

func newStreamedPutBenchmarkStorage(tb testing.TB) *stor.Storage {
	tb.Helper()

	// Mirror the supported server defaults, including normal background
	// compaction, rather than disabling maintenance for the benchmark.
	storage, err := stor.NewStorageWithConfig(&stor.StorageConfig{
		DiskPath:                 tb.TempDir(),
		TTL:                      stor.DefaultTTL,
		InlineThreshold:          stor.DefaultInlineThreshold,
		CompactThreshold:         stor.DefaultCompactThreshold,
		SegmentSize:              stor.DefaultSegmentSize,
		FdCacheSize:              stor.DefaultFdCacheSize,
		MaxDiskUsage:             stor.DefaultMaxDiskUsage,
		CompactionThreads:        stor.DefaultCompactionThreads,
		FragThreshold:            stor.DefaultFragmentationThreshold,
		MinSegmentAge:            stor.DefaultMinSegmentAgeForRecompaction,
		MinSegments:              stor.DefaultMinSegmentsBeforeRecompaction,
		DisableRecompaction:      stor.DefaultRecompactionDisabled,
		CleanupInterval:          stor.DefaultTTLCleanupInterval,
		RecoveryWorkers:          stor.DefaultRecoveryWorkers,
		DeleteBatchSize:          stor.DefaultDeleteBatchSize,
		EvictionPolicy:           stor.DefaultEvictionPolicy,
		CompactionBytesPerSecond: stor.DefaultCompactionBytesPerSecond,
		MetadataCacheSize:        stor.DefaultMetadataCacheSize,
		MetadataBackgroundJobs:   stor.DefaultMetadataBackgroundJobs,
	})
	if err != nil {
		tb.Fatalf("create storage: %v", err)
	}
	tb.Cleanup(func() { storage.Close() })
	return storage
}

// measureLiveHeapGrowth holds a fixed concurrent first-write batch at the
// pipe's second chunk. At that point the raw-file copy has acquired its buffer,
// so the sample observes the copy's in-flight live heap without depending on
// the benchmark framework's adaptive iteration count.
func measureLiveHeapGrowth(
	tb testing.TB,
	service *CacheService,
	ctx context.Context,
	name string,
	firstChunk, remainder []byte,
) int64 {
	tb.Helper()

	workerCount := streamedPutBenchmarkWorkers
	start := make(chan struct{})
	secondChunkReached := make(chan struct{}, workerCount)
	releaseSecondChunk := make(chan struct{})
	errs := make(chan error, workerCount)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for worker := range workerCount {
		go func(worker int) {
			defer workers.Done()
			<-start

			key := fmt.Sprintf("streamed-put-%s-live-heap-%d", name, worker)
			stream := newBlockedBenchmarkPutStream(
				ctx, key, firstChunk, remainder, secondChunkReached, releaseSecondChunk,
			)
			if err := service.Put(stream); err != nil {
				errs <- fmt.Errorf("CacheService.Put(%q): %w", key, err)
				return
			}
			if stream.response == nil || !stream.response.Success {
				errs <- fmt.Errorf("CacheService.Put(%q) response = %#v, want success", key, stream.response)
				return
			}
			errs <- nil
		}(worker)
	}

	// Settle existing pool entries and goroutine setup before starting the batch.
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	close(start)
	for range workerCount {
		<-secondChunkReached
	}

	// The copies remain blocked, so these collections retain their live buffers
	// while dropping unrelated temporary allocations.
	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	close(releaseSecondChunk)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			tb.Fatal(err)
		}
	}

	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

func medianDuration(values []time.Duration) time.Duration {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[len(values)/2]
}

func verifyStreamedPutContent(tb testing.TB, storage *stor.Storage, key string, want []byte) {
	tb.Helper()

	reader, found, err := storage.Get(key, 0, 0)
	if err != nil {
		tb.Fatalf("Get(%q): %v", key, err)
	}
	if !found {
		tb.Fatalf("Get(%q) did not find the value", key)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		tb.Fatalf("read %q: %v", key, err)
	}
	if closer, ok := reader.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			tb.Fatalf("close %q: %v", key, err)
		}
	}
	if !bytes.Equal(got, want) {
		tb.Fatalf("Get(%q) returned different data", key)
	}
}

// BenchmarkCacheServicePutLocalStreamFirstWrite measures concurrent local
// streamed Puts that take Storage's raw-file branch. Between timed batches the
// fixture deletes the previous batch's keys, so every timed Put sees an absent
// key and covers the first-write path without timing cleanup.
func BenchmarkCacheServicePutLocalStreamFirstWrite(b *testing.B) {
	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(previousLogLevel) })

	ctx := context.Background()

	for _, tc := range []struct {
		name             string
		size             int
		reportThroughput bool
	}{
		{name: "threshold-plus-2", size: streamedPutFirstChunkSize + 1, reportThroughput: true},
		// Keep allocation and latency evidence separate from the aggregate
		// throughput series, whose file-system component can dominate this
		// threshold-sized copy.
		{name: "threshold-plus-2-allocation", size: streamedPutFirstChunkSize + 1},
		{name: "1MiB", size: 1 << 20, reportThroughput: true},
		{name: "64MiB-plus-1-sync", size: stor.DefaultCompactThreshold + 1, reportThroughput: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			storage := newStreamedPutBenchmarkStorage(b)
			service := NewCacheService(nil, storage)
			content := bytes.Repeat([]byte("x"), tc.size)
			firstChunk := content[:streamedPutFirstChunkSize]
			remainder := content[streamedPutFirstChunkSize:]
			workerCount := streamedPutBenchmarkWorkers
			keys := make([]string, workerCount)
			for worker := range workerCount {
				keys[worker] = fmt.Sprintf("streamed-put-%s-%d", tc.name, worker)
			}

			type putResult struct {
				key      string
				duration time.Duration
				err      error
			}
			jobs := make(chan string, workerCount)
			results := make(chan putResult, workerCount)
			var workers sync.WaitGroup
			workers.Add(workerCount)
			for range workerCount {
				go func() {
					defer workers.Done()
					for key := range jobs {
						stream := newBenchmarkPutStream(ctx, key, firstChunk, remainder)
						start := time.Now()
						err := service.Put(stream)
						duration := time.Since(start)
						if err != nil {
							results <- putResult{key: key, duration: duration, err: err}
							continue
						}
						if stream.response == nil || !stream.response.Success {
							results <- putResult{
								key:      key,
								duration: duration,
								err:      fmt.Errorf("response = %#v, want success", stream.response),
							}
							continue
						}
						results <- putResult{key: key, duration: duration}
					}
				}()
			}
			defer func() {
				close(jobs)
				workers.Wait()
			}()

			if tc.reportThroughput {
				b.SetBytes(int64(len(content)))
			}
			b.ReportAllocs()
			// Keep the live-heap probe independent of adaptive b.N calibration.
			liveHeapGrowth := measureLiveHeapGrowth(b, service, ctx, tc.name, firstChunk, remainder)
			runtime.GC()
			b.ResetTimer()

			var gcPauseNs uint64
			var totalBatchElapsed time.Duration
			putLatencies := make([]time.Duration, 0, b.N)
			for puts := 0; puts < b.N; {
				batchSize := min(workerCount, b.N-puts)
				var beforeTimedPuts runtime.MemStats
				runtime.ReadMemStats(&beforeTimedPuts)
				batchStart := time.Now()
				for worker := range batchSize {
					jobs <- keys[worker]
				}
				lastKey := ""
				for range batchSize {
					result := <-results
					lastKey = result.key
					putLatencies = append(putLatencies, result.duration)
					if result.err != nil {
						b.Errorf("CacheService.Put(%q): %v", result.key, result.err)
					}
				}
				batchElapsed := time.Since(batchStart)
				totalBatchElapsed += batchElapsed
				b.StopTimer()
				var afterTimedPuts runtime.MemStats
				runtime.ReadMemStats(&afterTimedPuts)
				gcPauseNs += afterTimedPuts.PauseTotalNs - beforeTimedPuts.PauseTotalNs

				if puts+batchSize == b.N {
					verifyStreamedPutContent(b, storage, lastKey, content)
				}
				for worker := range batchSize {
					if err := storage.DeleteKey(keys[worker]); err != nil {
						b.Fatalf("DeleteKey(%q): %v", keys[worker], err)
					}
				}
				puts += batchSize
				if puts < b.N {
					b.StartTimer()
				}
			}

			b.ReportMetric(float64(gcPauseNs)/float64(b.N), "gc-pause-ns/op")
			// Report p50 service latency. Throughput variants report sustained
			// batch throughput separately.
			b.ReportMetric(float64(medianDuration(putLatencies)), "ns/op")
			if tc.reportThroughput {
				b.ReportMetric(float64(b.N*len(content))*1e3/float64(totalBatchElapsed), "MB/s")
			}
			// Use Go heap-page units for the in-flight live-heap metric. The
			// 32 KiB scratch allocation at issue would span multiple pages.
			const heapPageSize = 8 << 10
			liveHeapPages := int64(0)
			if liveHeapGrowth > 0 {
				liveHeapPages = (liveHeapGrowth + heapPageSize - 1) / heapPageSize
			}
			b.ReportMetric(float64(liveHeapPages), "live-heap-pages")
		})
	}
}
