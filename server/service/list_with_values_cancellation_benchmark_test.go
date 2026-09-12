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

const canceledListBenchmarkCount = 1000

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

// BenchmarkCacheServiceListWithValuesCancellation measures the public list
// operation after its first raw-file payload read has started. The benchmark
// gate holds that read so the cancellation boundary is deterministic. The
// base path is released after a short hold; the changed path interrupts its
// private descriptor and returns as soon as cancellation is observed.
func BenchmarkCacheServiceListWithValuesCancellation(b *testing.B) {
	quietCacheServiceBenchmarkLogs(b)
	env := newCanceledListBenchmarkEnvironment(b, canceledListBenchmarkCount, 8*1024)
	defer env.close()

	req := &pb.ListRequest{Prefix: env.prefix, Limit: canceledListBenchmarkCount}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		started, release, restore := benchio.BlockPayloadReadsForBenchmark()
		ctx, cancel := context.WithCancel(context.Background())
		ready := make(chan struct{})
		finished := make(chan struct{})
		go func() {
			close(ready)
			<-started
			cancel()
			time.Sleep(5 * time.Millisecond)
			release()
			close(finished)
		}()
		<-ready

		b.StartTimer()
		_, _ = env.service.ListWithValues(ctx, req)
		b.StopTimer()

		<-finished
		restore()
		cancel()
		b.StartTimer()
	}
	b.StopTimer()
}

// BenchmarkCacheServiceListWithValuesRawLive is the live-context guard for
// the same spilled-value consumer path. It checks that opening a private
// interruptible descriptor does not silently transfer material cost to
// successful raw-file pages.
func BenchmarkCacheServiceListWithValuesRawLive(b *testing.B) {
	quietCacheServiceBenchmarkLogs(b)
	env := newCanceledListBenchmarkEnvironment(b, 100, 8*1024)
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
		consumeListWithValuesBenchmarkResponse(b, response, 100, 8*1024)
	}
}
