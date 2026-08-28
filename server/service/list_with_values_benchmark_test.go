// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

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
)

type cacheServiceListWithValuesBenchmarkEnvironment struct {
	storage *stor.Storage
	service *CacheService
	prefix  string
}

func newCacheServiceListWithValuesBenchmarkEnvironment(tb testing.TB, count, valueSize int) *cacheServiceListWithValuesBenchmarkEnvironment {
	tb.Helper()

	storage, err := stor.NewStorageWithConfig(&stor.StorageConfig{
		DiskPath:            tb.TempDir(),
		InlineThreshold:     stor.DefaultInlineThreshold,
		CleanupInterval:     time.Hour,
		DisableRecompaction: true,
	})
	require.NoError(tb, err)

	env := &cacheServiceListWithValuesBenchmarkEnvironment{
		storage: storage,
		service: NewCacheService(nil, storage),
		prefix:  "list-values-benchmark-",
	}
	value := bytes.Repeat([]byte("x"), valueSize)
	for i := 0; i < count; i++ {
		_, err := env.service.PutObject(context.Background(), &pb.PutRequest{
			Key:  fmt.Sprintf("%s%04d", env.prefix, i),
			Data: value,
		})
		require.NoError(tb, err)
	}

	return env
}

func (env *cacheServiceListWithValuesBenchmarkEnvironment) close() {
	if env.storage != nil {
		env.storage.Close()
		env.storage = nil
	}
}

func consumeListWithValuesBenchmarkResponse(b *testing.B, response *pb.ListWithValuesResponse, count, valueSize int) {
	b.Helper()
	if response == nil {
		b.Fatal("ListWithValues returned a nil response")
	}
	if response.HasMore || response.ContinuationToken != "" {
		b.Fatalf("ListWithValues returned an unexpected continuation: has_more=%v token=%q", response.HasMore, response.ContinuationToken)
	}
	if len(response.Entries) != count {
		b.Fatalf("ListWithValues returned %d entries, want %d", len(response.Entries), count)
	}

	totalBytes := 0
	for _, entry := range response.Entries {
		if entry == nil {
			b.Fatal("ListWithValues returned a nil entry")
		}
		if entry.ValueOmitted || entry.ValueLength != int64(valueSize) || len(entry.Value) != valueSize {
			b.Fatalf("ListWithValues returned an invalid inline value: omitted=%v length=%d bytes=%d", entry.ValueOmitted, entry.ValueLength, len(entry.Value))
		}
		if entry.Value[0] != 'x' || entry.Value[len(entry.Value)-1] != 'x' {
			b.Fatal("ListWithValues returned an unexpected value")
		}
		totalBytes += len(entry.Value)
	}
	if totalBytes != count*valueSize {
		b.Fatalf("ListWithValues returned %d value bytes, want %d", totalBytes, count*valueSize)
	}
}

func BenchmarkCacheServiceListWithValues(b *testing.B) {
	quietCacheServiceBenchmarkLogs(b)

	cases := []struct {
		name      string
		count     int
		valueSize int
	}{
		{name: "Inline4KiB/Count1", count: 1, valueSize: 4 * 1024},
		{name: "Inline4KiB/Count100", count: 100, valueSize: 4 * 1024},
		{name: "Inline4KiB/Count1000", count: 1000, valueSize: 4 * 1024},
		{name: "Inline64KiB/Count1", count: 1, valueSize: 64 * 1024},
		{name: "Inline64KiB/Count100", count: 100, valueSize: 64 * 1024},
		{name: "Inline64KiB/Count1000", count: 1000, valueSize: 64 * 1024},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			env := newCacheServiceListWithValuesBenchmarkEnvironment(b, tc.count, tc.valueSize)
			defer env.close()

			req := &pb.ListRequest{
				Prefix: env.prefix,
				Limit:  int32(tc.count),
			}
			b.ReportAllocs()
			for b.Loop() {
				response, err := env.service.ListWithValues(context.Background(), req)
				if err != nil {
					b.Fatal(err)
				}
				consumeListWithValuesBenchmarkResponse(b, response, tc.count, tc.valueSize)
			}
		})
	}
}

func TestCacheService_ListWithValuesRetainsInlineData(t *testing.T) {
	storage, err := stor.NewStorageWithConfig(&stor.StorageConfig{
		DiskPath:            t.TempDir(),
		InlineThreshold:     stor.DefaultInlineThreshold,
		CleanupInterval:     time.Hour,
		DisableRecompaction: true,
	})
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			storage.Close()
		}
	})

	service := NewCacheService(nil, storage)
	want := map[string][]byte{
		"list-retain-4k":  bytes.Repeat([]byte("a"), 4*1024),
		"list-retain-64k": bytes.Repeat([]byte("b"), 64*1024),
	}
	for key, value := range want {
		response, err := service.PutObject(context.Background(), &pb.PutRequest{Key: key, Data: value})
		require.NoError(t, err)
		require.True(t, response.Success)
	}

	response, err := service.ListWithValues(context.Background(), &pb.ListRequest{
		Prefix: "list-retain-",
		Limit:  int32(len(want)),
	})
	require.NoError(t, err)
	require.False(t, response.HasMore)
	require.Empty(t, response.ContinuationToken)
	require.Len(t, response.Entries, len(want))

	// The storage iterator is closed before ListWithValues returns. Close the
	// storage too before checking the response to ensure returned bytes are not
	// backed by RocksDB iterator memory.
	closed = true
	storage.Close()
	for _, entry := range response.Entries {
		require.Contains(t, want, entry.Key)
		require.False(t, entry.ValueOmitted)
		require.Equal(t, int64(len(want[entry.Key])), entry.ValueLength)
		require.Equal(t, want[entry.Key], entry.Value)
	}
}
