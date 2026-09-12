// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_benchmark && !ocache_topology_benchmark

package service

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
	"github.com/tigrisdata/ocache/storage/benchio"
)

const (
	canceledListBenchmarkCount     = 1000
	canceledListBenchmarkValueSize = 128 * 1024
)

type canceledListBenchmarkEnvironment struct {
	storage *stor.Storage
	service *CacheService
	prefix  string
}

func newCanceledListBenchmarkEnvironment(tb testing.TB, count, valueSize int) *canceledListBenchmarkEnvironment {
	tb.Helper()

	storage, err := stor.NewStorageWithConfig(&stor.StorageConfig{
		DiskPath:            tb.TempDir(),
		InlineThreshold:     1,
		CompactThreshold:    4 * 1024,
		SegmentSize:         256 * 1024 * 1024,
		CleanupInterval:     time.Hour,
		DisableRecompaction: true,
	})
	require.NoError(tb, err)

	env := &canceledListBenchmarkEnvironment{
		storage: storage,
		service: NewCacheService(nil, storage),
		prefix:  "list-values-cancel-benchmark-",
	}
	value := bytes.Repeat([]byte("x"), valueSize)
	for i := 0; i < count; i++ {
		response, putErr := env.service.PutObject(context.Background(), &pb.PutRequest{
			Key:  fmt.Sprintf("%s%04d", env.prefix, i),
			Data: value,
		})
		require.NoError(tb, putErr)
		require.True(tb, response.Success)
	}
	return env
}

func (env *canceledListBenchmarkEnvironment) close() {
	if env.storage != nil {
		env.storage.Close()
		env.storage = nil
	}
}

func TestCacheServiceListWithValuesCancellationReleasesBlockedRead(t *testing.T) {
	quietCacheServiceBenchmarkLogs(t)
	env := newCanceledListBenchmarkEnvironment(t, 2, canceledListBenchmarkValueSize)
	defer env.close()

	started, release, restore := benchio.BlockPayloadReadsForBenchmark()
	defer func() {
		release()
		restore()
	}()

	type result struct {
		response *pb.ListWithValuesResponse
		err      error
	}
	resultCh := make(chan result, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		response, err := env.service.ListWithValues(ctx, &pb.ListRequest{
			Prefix: env.prefix,
			Limit:  2,
		})
		resultCh <- result{response: response, err: err}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("ListWithValues did not reach the payload read")
	}
	cancel()

	select {
	case result := <-resultCh:
		require.ErrorIs(t, result.err, context.Canceled)
		require.Nil(t, result.response)
	case <-time.After(2 * time.Second):
		t.Fatal("ListWithValues remained blocked after cancellation")
	}
}

type canceledListBenchmarkStats struct {
	cancelToExitNanos         int64
	rowsAfterCancel           int64
	bytesAfterCancel          int64
	activeScansAfterCancel    int64
	activeHandlersAfterCancel int64
	activeReadersAfterCancel  int64
	handlerExitedBeforeGate   int64
	operations                int64
}

// BenchmarkCacheServiceListWithValuesCancellation measures the public list
// operation after its first raw-file payload read has started. The benchmark
// records cancellation delay, post-cancel scan work, and ownership state while
// a gate holds the first payload read.
func BenchmarkCacheServiceListWithValuesCancellation(b *testing.B) {
	quietCacheServiceBenchmarkLogs(b)
	env := newCanceledListBenchmarkEnvironment(b, canceledListBenchmarkCount, canceledListBenchmarkValueSize)
	defer env.close()

	req := &pb.ListRequest{Prefix: env.prefix, Limit: canceledListBenchmarkCount}
	var stats canceledListBenchmarkStats
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		stats.operations++
		benchio.ResetPayloadStatsForBenchmark()
		started, release, restore := benchio.BlockPayloadReadsForBenchmark()
		ctx, cancel := context.WithCancel(context.Background())
		ready := make(chan struct{})
		cancelled := make(chan time.Time, 1)
		activeStats := make(chan canceledListBenchmarkStats, 1)
		postCancelStats := make(chan canceledListBenchmarkStats, 1)
		handlerDone := make(chan struct{})
		released := make(chan struct{})
		go func() {
			close(ready)
			<-started
			cancelAt := time.Now()
			benchio.MarkPayloadCancellationForBenchmark()
			cancel()
			cancelled <- cancelAt
			time.Sleep(5 * time.Millisecond)
			activeStats <- canceledListBenchmarkStats{
				activeScansAfterCancel:    benchio.ActiveListScansForBenchmark(),
				activeHandlersAfterCancel: benchio.ActiveListHandlersForBenchmark(),
				activeReadersAfterCancel:  benchio.ActivePayloadReadersForBenchmark(),
			}
			time.Sleep(5 * time.Millisecond)
			release()
			close(released)
			<-handlerDone
			postCancelStats <- canceledListBenchmarkStats{
				rowsAfterCancel:  benchio.PostCancellationRowsForBenchmark(),
				bytesAfterCancel: benchio.PostCancellationBytesForBenchmark(),
			}
		}()
		<-ready

		b.StartTimer()
		_, _ = env.service.ListWithValues(ctx, req)
		b.StopTimer()
		close(handlerDone)

		cancelAt := <-cancelled
		stats.cancelToExitNanos += time.Since(cancelAt).Nanoseconds()
		select {
		case <-released:
		default:
			stats.handlerExitedBeforeGate++
		}
		active := <-activeStats
		stats.activeScansAfterCancel += active.activeScansAfterCancel
		stats.activeHandlersAfterCancel += active.activeHandlersAfterCancel
		stats.activeReadersAfterCancel += active.activeReadersAfterCancel
		<-released
		postCancel := <-postCancelStats
		stats.rowsAfterCancel += postCancel.rowsAfterCancel
		stats.bytesAfterCancel += postCancel.bytesAfterCancel
		restore()
		cancel()
		b.StartTimer()
	}
	b.StopTimer()

	operations := float64(stats.operations)
	b.ReportMetric(float64(stats.cancelToExitNanos)/operations, "cancel-to-exit-ns/op")
	b.ReportMetric(float64(stats.rowsAfterCancel)/operations, "rows-after-cancel/op")
	b.ReportMetric(float64(stats.bytesAfterCancel)/operations, "bytes-after-cancel/op")
	b.ReportMetric(float64(stats.activeScansAfterCancel)/operations, "active-list-scans-after-cancel/op")
	b.ReportMetric(float64(stats.activeHandlersAfterCancel)/operations, "active-list-handlers-after-cancel/op")
	b.ReportMetric(float64(stats.activeReadersAfterCancel)/operations, "active-payload-readers-after-cancel/op")
	b.ReportMetric(float64(stats.handlerExitedBeforeGate)/operations, "handler-exit-before-gate/op")
}

// BenchmarkCacheServiceListWithValuesRawLive is the live-context guard for
// the same spilled-value consumer path. It checks that opening a private
// interruptible descriptor does not silently transfer material cost to
// successful raw-file pages.
func BenchmarkCacheServiceListWithValuesRawLive(b *testing.B) {
	quietCacheServiceBenchmarkLogs(b)
	env := newCanceledListBenchmarkEnvironment(b, 100, canceledListBenchmarkValueSize)
	defer env.close()

	req := &pb.ListRequest{Prefix: env.prefix, Limit: 100}
	b.ReportAllocs()
	for b.Loop() {
		ctx, cancel := context.WithCancel(context.Background())
		response, err := env.service.ListWithValues(ctx, req)
		cancel()
		if err != nil {
			b.Fatal(err)
		}
		consumeListWithValuesBenchmarkResponse(b, response, 100, canceledListBenchmarkValueSize)
	}
}
