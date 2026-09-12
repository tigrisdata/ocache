// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_benchmark && !ocache_topology_benchmark

package service

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
	"github.com/tigrisdata/ocache/storage/benchio"
	"google.golang.org/grpc"
)

const (
	canceledListBenchmarkCount     = 1000
	canceledListBenchmarkValueSize = 128 * 1024
)

type canceledListBenchmarkEnvironment struct {
	storage      *stor.Storage
	peerStorage  *stor.Storage
	service      *CacheService
	peerServer   *grpc.Server
	peerListener net.Listener
	closeRing    func()
	prefix       string
}

func newCanceledListBenchmarkEnvironment(tb testing.TB, count, valueSize int) *canceledListBenchmarkEnvironment {
	tb.Helper()

	newStorage := func() *stor.Storage {
		storage, err := stor.NewStorageWithConfig(&stor.StorageConfig{
			DiskPath:            tb.TempDir(),
			InlineThreshold:     1,
			CompactThreshold:    4 * 1024,
			SegmentSize:         256 * 1024 * 1024,
			CleanupInterval:     time.Hour,
			DisableRecompaction: true,
		})
		require.NoError(tb, err)
		return storage
	}

	storage := newStorage()
	peerStorage := newStorage()
	ringManager, closeRing, err := ring.NewTopologyBenchmarkManager(2, 128)
	require.NoError(tb, err)
	coord := coordinator.NewBenchmarkCoordinator(ringManager, "member-0")

	peerListener, err := net.Listen("tcp", "127.0.0.1:10001")
	require.NoError(tb, err)
	peerServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(128*1024*1024),
		grpc.MaxSendMsgSize(128*1024*1024),
	)
	pb.RegisterCacheServiceServer(peerServer, NewCacheService(nil, peerStorage))
	go func() { _ = peerServer.Serve(peerListener) }()

	env := &canceledListBenchmarkEnvironment{
		storage:      storage,
		peerStorage:  peerStorage,
		service:      NewCacheService(coord, storage),
		peerServer:   peerServer,
		peerListener: peerListener,
		closeRing:    closeRing,
		prefix:       "list-values-cancel-benchmark-",
	}
	value := bytes.Repeat([]byte("x"), valueSize)
	for i := 0; i < count; i++ {
		for _, nodeStorage := range []*stor.Storage{storage, peerStorage} {
			key := fmt.Sprintf("%s%04d", env.prefix, i)
			if nodeStorage == peerStorage {
				key = fmt.Sprintf("%speer-%04d", env.prefix, i)
			} else {
				key = fmt.Sprintf("%slocal-%04d", env.prefix, i)
			}
			require.NoError(tb, nodeStorage.Put(key, bytes.NewReader(value), 0))
		}
	}
	return env
}

func (env *canceledListBenchmarkEnvironment) close() {
	if env.peerServer != nil {
		env.peerServer.Stop()
		env.peerServer = nil
	}
	if env.peerListener != nil {
		_ = env.peerListener.Close()
		env.peerListener = nil
	}
	if env.storage != nil {
		env.storage.Close()
		env.storage = nil
	}
	if env.peerStorage != nil {
		env.peerStorage.Close()
		env.peerStorage = nil
	}
	if env.closeRing != nil {
		env.closeRing()
		env.closeRing = nil
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

// waitForCanceledListBenchmarkFanout waits until both cluster branches have
// reached their first payload reader before cancellation is recorded.
func waitForCanceledListBenchmarkFanout() bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if benchio.ActiveListScansForBenchmark() >= 2 &&
			benchio.ActiveListHandlersForBenchmark() >= 2 &&
			benchio.ActivePayloadReadersForBenchmark() >= 2 {
			return true
		}
		time.Sleep(100 * time.Microsecond)
	}
	return false
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
		fanoutReady := make(chan bool, 1)
		activeStats := make(chan canceledListBenchmarkStats, 1)
		postCancelStats := make(chan canceledListBenchmarkStats, 1)
		handlerDone := make(chan struct{})
		released := make(chan struct{})
		go func() {
			close(ready)
			<-started
			fanoutReady <- waitForCanceledListBenchmarkFanout()
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
		fanoutReached := <-fanoutReady
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
		if !fanoutReached {
			b.Fatal("cluster cancellation workload did not reach both handlers, scans, and payload readers")
		}
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

func consumeClusterListWithValuesBenchmarkResponse(b *testing.B, response *pb.ListWithValuesResponse, count, valueSize int, expectMore bool) []string {
	b.Helper()
	if response == nil {
		b.Fatal("ListWithValues returned a nil response")
	}
	if len(response.Entries) != count {
		b.Fatalf("ListWithValues returned %d entries, want %d", len(response.Entries), count)
	}
	if response.HasMore != expectMore {
		b.Fatalf("ListWithValues returned unexpected has_more: got=%v want=%v", response.HasMore, expectMore)
	}
	if (response.ContinuationToken != "") != expectMore {
		b.Fatalf("ListWithValues returned unexpected continuation: has_more=%v token=%q", response.HasMore, response.ContinuationToken)
	}

	keys := make([]string, 0, len(response.Entries))
	var totalBytes int
	previousKey := ""
	for _, entry := range response.Entries {
		if entry == nil {
			b.Fatal("ListWithValues returned a nil entry")
		}
		if previousKey != "" && entry.Key <= previousKey {
			b.Fatalf("ListWithValues returned keys out of order: %q after %q", entry.Key, previousKey)
		}
		previousKey = entry.Key
		keys = append(keys, entry.Key)
		if entry.ValueOmitted || entry.ValueLength != int64(valueSize) || len(entry.Value) != valueSize {
			b.Fatalf("ListWithValues returned an invalid value: key=%q omitted=%v length=%d bytes=%d", entry.Key, entry.ValueOmitted, entry.ValueLength, len(entry.Value))
		}
		totalBytes += len(entry.Value)
	}
	if totalBytes != count*valueSize {
		b.Fatalf("ListWithValues returned %d value bytes, want %d", totalBytes, count*valueSize)
	}
	return keys
}

// BenchmarkCacheServiceListWithValuesRawLive is the live-context guard for
// the same spilled-value consumer path. It checks that opening a private
// interruptible descriptor does not silently transfer material cost to
// successful raw-file pages while preserving cluster ordering and cursors.
func BenchmarkCacheServiceListWithValuesRawLive(b *testing.B) {
	quietCacheServiceBenchmarkLogs(b)
	env := newCanceledListBenchmarkEnvironment(b, 100, canceledListBenchmarkValueSize)
	defer env.close()

	req := &pb.ListRequest{Prefix: env.prefix, Limit: 100}
	b.ReportAllocs()
	for b.Loop() {
		ctx, cancel := context.WithCancel(context.Background())
		response, err := env.service.ListWithValues(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		firstKeys := consumeClusterListWithValuesBenchmarkResponse(b, response, 100, canceledListBenchmarkValueSize, true)
		response, err = env.service.ListWithValues(ctx, &pb.ListRequest{
			Prefix:            env.prefix,
			Limit:             100,
			ContinuationToken: response.ContinuationToken,
		})
		cancel()
		if err != nil {
			b.Fatal(err)
		}
		secondKeys := consumeClusterListWithValuesBenchmarkResponse(b, response, 100, canceledListBenchmarkValueSize, false)
		if firstKeys[len(firstKeys)-1] >= secondKeys[0] {
			b.Fatalf("cluster continuation moved backwards: first=%q second=%q", firstKeys[len(firstKeys)-1], secondKeys[0])
		}
		seen := make(map[string]struct{}, len(firstKeys))
		for _, key := range firstKeys {
			seen[key] = struct{}{}
		}
		for _, key := range secondKeys {
			if _, exists := seen[key]; exists {
				b.Fatalf("cluster continuation repeated key %q", key)
			}
		}
	}
}
