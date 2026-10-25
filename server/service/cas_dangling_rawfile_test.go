// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage"
	"github.com/tigrisdata/ocache/storage/fd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	danglingCASInlineThreshold  = 1024
	danglingCASCompactThreshold = 4 * 1024
	danglingCASLargeValueSize   = 8 * 1024
)

type danglingCASServiceEnvironment struct {
	store   *storage.Storage
	service *CacheService
	disk    string
}

func newDanglingCASServiceEnvironment(tb testing.TB) *danglingCASServiceEnvironment {
	tb.Helper()

	disk := tb.TempDir()
	store, err := storage.NewStorageWithConfig(&storage.StorageConfig{
		DiskPath:            disk,
		InlineThreshold:     danglingCASInlineThreshold,
		CompactThreshold:    danglingCASCompactThreshold,
		SegmentSize:         256 * 1024 * 1024,
		DisableRecompaction: true,
		CleanupInterval:     time.Hour,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() { store.Close() })

	return &danglingCASServiceEnvironment{
		store:   store,
		service: NewCacheService(nil, store),
		disk:    disk,
	}
}

func (env *danglingCASServiceEnvironment) rawFilePath(tb testing.TB) string {
	tb.Helper()

	entries, err := os.ReadDir(filepath.Join(env.disk, "files"))
	require.NoError(tb, err)
	var path string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		require.Empty(tb, path, "fixture expects one raw file")
		path = filepath.Join(env.disk, "files", entry.Name())
	}
	require.NotEmpty(tb, path, "fixture did not create a raw file")
	return path
}

func (env *danglingCASServiceEnvironment) putDanglingLarge(
	tb testing.TB,
	key string,
	expected uint64,
	value []byte,
) uint64 {
	tb.Helper()

	version, err := env.store.PutIfVersion(key, bytes.NewReader(value), 0, expected)
	require.NoError(tb, err)
	path := env.rawFilePath(tb)
	require.NoError(tb, os.Remove(path))
	fd.GetFdCache().Remove(path)
	return version
}

type getWithVersionCapture struct {
	pb.CacheService_GetStreamWithVersionServer
	ctx       context.Context
	responses []*pb.GetWithVersionResponse
}

func (s *getWithVersionCapture) Context() context.Context { return s.ctx }

func (s *getWithVersionCapture) Send(response *pb.GetWithVersionResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func TestCacheService_GetStreamWithVersion_DanglingLargeRawFile_Recovery(t *testing.T) {
	env := newDanglingCASServiceEnvironment(t)
	key := "dangling-large-stream"
	value := bytes.Repeat([]byte("x"), danglingCASLargeValueSize)
	_, err := env.store.PutIfVersion(key, bytes.NewReader(value), 0, 0)
	require.NoError(t, err)
	path := env.rawFilePath(t)
	require.NoError(t, os.Remove(path))
	fd.GetFdCache().Remove(path)

	stream := &getWithVersionCapture{ctx: context.Background()}
	err = env.service.GetStreamWithVersion(&pb.GetRequest{Key: key}, stream)
	if err != nil || len(stream.responses) != 1 || stream.responses[0].Found || stream.responses[0].Version == 0 {
		t.Fatalf(
			"property: local versioned stream must return found=false with a nonzero token after a dangling large raw file (responses %d, error %v)",
			len(stream.responses), err,
		)
	}
}

// BenchmarkCacheServiceGetStreamWithVersionDanglingLargeRawFile measures the
// local streaming handler's terminal response for one unchanged missing-file
// fault. The base source returns Unavailable without a header; recovered source
// returns a usable absence header. Setup is excluded from the measured call.
func BenchmarkCacheServiceGetStreamWithVersionDanglingLargeRawFile(b *testing.B) {
	quietCacheServiceBenchmarkLogs(b)
	env := newDanglingCASServiceEnvironment(b)
	value := bytes.Repeat([]byte("x"), danglingCASLargeValueSize)
	key := "dangling-large-stream-benchmark"
	var expected uint64
	b.ResetTimer()

	for b.Loop() {
		b.StopTimer()
		createdVersion := env.putDanglingLarge(b, key, expected, value)
		stream := &getWithVersionCapture{ctx: context.Background()}
		b.StartTimer()
		err := env.service.GetStreamWithVersion(&pb.GetRequest{Key: key}, stream)
		b.StopTimer()

		if err != nil {
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.Unavailable || st.Message() != "temporary I/O error" || len(stream.responses) != 0 {
				b.Fatalf(
					"unexpected terminal response for dangling large raw file: status=%v responses=%d error=%v",
					st, len(stream.responses), err,
				)
			}
			// The unchanged source exhausts its retry budget without a header.
			expected = createdVersion
		} else {
			if len(stream.responses) != 1 || stream.responses[0].Found || stream.responses[0].Version == 0 {
				b.Fatalf("unexpected successful terminal response: responses=%d", len(stream.responses))
			}
			// Recovery leaves an unstamped purge tombstone; the next iteration
			// starts from absence again, with a fresh raw file.
			expected = 0
		}
		b.StartTimer()
	}
}
