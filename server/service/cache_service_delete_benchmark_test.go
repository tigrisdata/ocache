// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage"
)

const cacheServiceDeleteInlineSize = storage.DefaultInlineThreshold

type cacheServiceDeleteEnvironment struct {
	storage *storage.Storage
	service *CacheService
	disk    string
}

func newCacheServiceDeleteEnvironment(tb testing.TB, compactThreshold int64) *cacheServiceDeleteEnvironment {
	tb.Helper()

	disk := tb.TempDir()
	s, err := storage.NewStorageWithConfig(&storage.StorageConfig{
		DiskPath:            disk,
		InlineThreshold:     storage.DefaultInlineThreshold,
		CompactThreshold:    compactThreshold,
		DisableRecompaction: true,
	})
	require.NoError(tb, err)

	return &cacheServiceDeleteEnvironment{
		storage: s,
		service: NewCacheService(nil, s),
		disk:    disk,
	}
}

func (env *cacheServiceDeleteEnvironment) close() {
	if env.storage != nil {
		env.storage.Close()
		env.storage = nil
	}
}

func quietCacheServiceBenchmarkLogs(tb testing.TB) {
	tb.Helper()
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	tb.Cleanup(func() {
		zerolog.SetGlobalLevel(level)
	})
}

func (env *cacheServiceDeleteEnvironment) put(tb testing.TB, key string, value []byte) {
	tb.Helper()
	response, err := env.service.PutObject(context.Background(), &pb.PutRequest{
		Key:  key,
		Data: value,
	})
	require.NoError(tb, err)
	require.True(tb, response.Success)
}

func (env *cacheServiceDeleteEnvironment) delete(tb testing.TB, key string) {
	tb.Helper()
	response, err := env.service.Delete(context.Background(), &pb.DeleteRequest{Key: key})
	require.NoError(tb, err)
	require.True(tb, response.Success)
}

func (env *cacheServiceDeleteEnvironment) rawFiles(tb testing.TB) map[string]struct{} {
	tb.Helper()
	entries, err := os.ReadDir(filepath.Join(env.disk, "files"))
	require.NoError(tb, err)

	files := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			files[filepath.Join(env.disk, "files", entry.Name())] = struct{}{}
		}
	}
	return files
}

// putSegment writes an ordinary compaction-eligible raw value and waits for its
// source file to be removed. The compactor only queues that source after its
// metadata merge commits, so this synchronizes with the RAW_FILE-to-SEGMENT
// transition instead of the earlier segment-file append.
func (env *cacheServiceDeleteEnvironment) putSegment(tb testing.TB, key string, value []byte) {
	tb.Helper()
	before := env.rawFiles(tb)
	env.put(tb, key, value)

	var source string
	for path := range env.rawFiles(tb) {
		if _, existed := before[path]; !existed {
			source = path
			break
		}
	}
	if source == "" {
		// The compactor and deletion queue completed before the directory read.
		return
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := os.Stat(source)
		if os.IsNotExist(err) {
			return
		}
		require.NoError(tb, err)
		if time.Now().After(deadline) {
			tb.Fatalf("compaction did not publish a segment for %q", key)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (env *cacheServiceDeleteEnvironment) deletedSegmentStats(tb testing.TB) (entries, bytes int64) {
	tb.Helper()
	stats, err := env.storage.ListSegmentDeleteStats()
	require.NoError(tb, err)
	for _, stat := range stats {
		entries += stat.DeletedEntries
		bytes += stat.DeletedBytes
	}
	return entries, bytes
}

func TestCacheServiceDeleteInlineAtDefaultThreshold(t *testing.T) {
	env := newCacheServiceDeleteEnvironment(t, storage.DefaultInlineThreshold)
	defer env.close()

	value := bytes.Repeat([]byte("i"), cacheServiceDeleteInlineSize)
	env.put(t, "inline-at-default-threshold", value)
	env.delete(t, "inline-at-default-threshold")

	_, found, err := env.storage.Get("inline-at-default-threshold", 0, 0)
	require.NoError(t, err)
	require.False(t, found)
}

func TestCacheServiceDeleteSegment(t *testing.T) {
	env := newCacheServiceDeleteEnvironment(t, 0)
	defer env.close()

	value := bytes.Repeat([]byte("s"), cacheServiceDeleteInlineSize+1)
	for i := range 3 {
		key := fmt.Sprintf("segment-delete-%d", i)
		env.putSegment(t, key, value)
		env.delete(t, key)

		entries, deletedBytes := env.deletedSegmentStats(t)
		require.Equal(t, int64(i+1), entries)
		require.Equal(t, int64(i+1)*int64(len(value)), deletedBytes)
	}
}

func TestCacheServicePutObjectOverwriteSegment(t *testing.T) {
	env := newCacheServiceDeleteEnvironment(t, 0)
	defer env.close()

	value := bytes.Repeat([]byte("s"), cacheServiceDeleteInlineSize+1)
	for i := range 3 {
		key := fmt.Sprintf("segment-overwrite-%d", i)
		env.putSegment(t, key, value)
		env.put(t, key, value)

		entries, deletedBytes := env.deletedSegmentStats(t)
		require.Equal(t, int64(i+1), entries)
		require.Equal(t, int64(i+1)*int64(len(value)), deletedBytes)
	}
}

func BenchmarkCacheServiceDelete(b *testing.B) {
	quietCacheServiceBenchmarkLogs(b)

	cases := []struct {
		name             string
		valueSize        int
		compactThreshold int64
		segment          bool
	}{
		{name: "Inline64B", valueSize: 64, compactThreshold: storage.DefaultInlineThreshold},
		{name: "Inline4KiB", valueSize: 4 * 1024, compactThreshold: storage.DefaultInlineThreshold},
		{name: "Inline64KiB", valueSize: cacheServiceDeleteInlineSize, compactThreshold: storage.DefaultInlineThreshold},
		{name: "RawFile64KiBPlus1", valueSize: cacheServiceDeleteInlineSize + 1, compactThreshold: storage.DefaultInlineThreshold},
		{name: "Segment64KiBPlus1", valueSize: cacheServiceDeleteInlineSize + 1, segment: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			env := newCacheServiceDeleteEnvironment(b, tc.compactThreshold)
			defer env.close()

			value := bytes.Repeat([]byte("x"), tc.valueSize)
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				b.StopTimer()
				key := fmt.Sprintf("delete-%d", i)
				if tc.segment {
					env.putSegment(b, key, value)
				} else {
					env.put(b, key, value)
				}
				req := &pb.DeleteRequest{Key: key}
				b.StartTimer()

				response, err := env.service.Delete(context.Background(), req)
				if err != nil {
					b.Fatal(err)
				}
				if !response.Success {
					b.Fatalf("Delete(%q) returned unsuccessful response", key)
				}

				if tc.segment {
					b.StopTimer()
					entries, deletedBytes := env.deletedSegmentStats(b)
					if entries != int64(i+1) || deletedBytes != int64(i+1)*int64(len(value)) {
						b.Fatalf("Delete(%q) did not credit its segment: entries=%d bytes=%d", key, entries, deletedBytes)
					}
					b.StartTimer()
				}
			}
		})
	}
}

func BenchmarkCacheServicePutObjectOverwrite(b *testing.B) {
	quietCacheServiceBenchmarkLogs(b)

	cases := []struct {
		name             string
		valueSize        int
		compactThreshold int64
		segment          bool
	}{
		{name: "Inline64KiB", valueSize: cacheServiceDeleteInlineSize, compactThreshold: storage.DefaultInlineThreshold},
		{name: "RawFile64KiBPlus1", valueSize: cacheServiceDeleteInlineSize + 1, compactThreshold: storage.DefaultInlineThreshold},
		{name: "Segment64KiBPlus1", valueSize: cacheServiceDeleteInlineSize + 1, segment: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			env := newCacheServiceDeleteEnvironment(b, tc.compactThreshold)
			defer env.close()

			value := bytes.Repeat([]byte("x"), tc.valueSize)
			key := "overwrite"
			var req *pb.PutRequest
			if !tc.segment {
				env.put(b, key, value)
				req = &pb.PutRequest{Key: key, Data: value}
			}

			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				if tc.segment {
					b.StopTimer()
					key = fmt.Sprintf("segment-overwrite-%d", i)
					env.putSegment(b, key, value)
					req = &pb.PutRequest{Key: key, Data: value}
					b.StartTimer()
				}

				response, err := env.service.PutObject(context.Background(), req)
				if err != nil {
					b.Fatal(err)
				}
				if !response.Success {
					b.Fatal("PutObject returned unsuccessful response")
				}

				if tc.segment {
					b.StopTimer()
					entries, deletedBytes := env.deletedSegmentStats(b)
					if entries != int64(i+1) || deletedBytes != int64(i+1)*int64(len(value)) {
						b.Fatalf("PutObject(%q) did not credit its segment: entries=%d bytes=%d", key, entries, deletedBytes)
					}
					b.StartTimer()
				}
			}
		})
	}
}
