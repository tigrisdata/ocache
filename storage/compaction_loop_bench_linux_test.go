//go:build linux

// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"fmt"
	"runtime"
	"syscall"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

const (
	benchmarkRecompactionTargetCount       = 2
	benchmarkRecompactionLiveEntryCount    = 384
	benchmarkRecompactionDeletedEntryCount = 432
	benchmarkRecompactionEntrySize         = 256 * 1024
	benchmarkRecompactionLoopInterval      = time.Millisecond
)

// BenchmarkRecompactionLoopSectionReaders measures the ordinary timer-driven
// recompaction loop with a one-millisecond test interval. The target segments
// each contain 384 live and 432 deleted 256 KiB entries, so the default
// fragmentation and segment-count gates select it after the age signal is set.
// Segment creation and metadata preparation happen before b.Loop; the timed
// operation includes the cadence wait, the loop's scan, live-entry rewrite, and
// publication. Use -test.benchtime=1x because one pass consumes the prepared
// fragmented segments.
func BenchmarkRecompactionLoopSectionReaders(b *testing.B) {
	benchmarkRecompactionLoop(b, benchmarkRecompactionLoopInterval, true, true, true)
}

// BenchmarkRecompactionLoopSectionReadersTiming measures the ordinary timer-driven
// loop while keeping the reported result focused on elapsed time and workload
// shape. The detailed allocation and GC diagnostics remain in the companion
// benchmark above; this row avoids making unstable process-wide counters part of
// the timing comparison.
func BenchmarkRecompactionLoopSectionReadersTiming(b *testing.B) {
	benchmarkRecompactionLoop(b, benchmarkRecompactionLoopInterval, false, false, false)
}

// BenchmarkRecompactionLoopDefaultInterval runs the same ordinary loop with
// the production one-minute interval. The benchmark timer includes the cadence
// wait and the asynchronous rewrite, so the standard and custom metrics cover
// the complete operation. Use -test.benchtime=1x because one pass consumes the
// prepared segments.
func BenchmarkRecompactionLoopDefaultInterval(b *testing.B) {
	benchmarkRecompactionLoop(b, DefaultRecompactionInterval, true, true, true)
}

// BenchmarkRecompactionLoopDefaultIntervalTiming measures the same ordinary
// loop at the supported production cadence while reporting only elapsed time
// and workload shape. The detailed diagnostics remain in the companion
// benchmark above.
func BenchmarkRecompactionLoopDefaultIntervalTiming(b *testing.B) {
	benchmarkRecompactionLoop(b, DefaultRecompactionInterval, false, false, false)
}

func benchmarkRecompactionLoop(b *testing.B, interval time.Duration, reportCPU, reportGC, reportAllocs bool) {
	b.Helper()

	liveValue := bytes.Repeat([]byte("l"), benchmarkRecompactionEntrySize)
	deletedValue := bytes.Repeat([]byte("d"), benchmarkRecompactionEntrySize)

	env := newRecompactionSetupBenchmark(b)
	defer env.close()
	liveKeys, deletedKeys, oldPaths := prepareRecompactionLoopSegment(b, env, liveValue, deletedValue)
	startRecompactionBenchmark(b, env, interval)
	runtime.GC()
	var memoryBefore runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	cpuBefore := benchmarkProcessCPUTime(b)
	if reportAllocs {
		b.ReportAllocs()
	}
	elapsedBefore := time.Now()

	for b.Loop() {
		// Leave a short scheduling margin for the timer-driven worker. Keep the
		// cadence wait inside the benchmark timer so a rewrite that starts before
		// the margin ends cannot be omitted from the standard metrics.
		time.Sleep(interval + 100*time.Millisecond)
		waitForRecompactedValues(b, env.storage, liveKeys, oldPaths, interval)
	}
	elapsedNanos := time.Since(elapsedBefore).Nanoseconds()

	cpuAfter := benchmarkProcessCPUTime(b)
	var memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryAfter)
	env.compactor.Close()
	for _, key := range liveKeys {
		requireStoredValue(b, env.storage, key, liveValue)
	}
	for _, key := range deletedKeys {
		if _, exists := benchmarkValueMessage(b, env.storage, key); exists {
			b.Fatalf("recompaction restored deleted metadata for %s", key)
		}
	}

	// Keep the workload shape and cadence visible in native benchmark output.
	// The standard timer and these counters cover the cadence wait and rewrite.
	operations := float64(b.N)
	if reportCPU {
		b.ReportMetric(float64(cpuAfter-cpuBefore)/operations, "recompaction-cpu-ns/op")
		b.ReportMetric(float64(elapsedNanos)/operations, "recompaction-elapsed-ns/op")
	}
	if reportGC {
		b.ReportMetric(float64(memoryAfter.TotalAlloc-memoryBefore.TotalAlloc)/operations, "recompaction-gc-alloc-bytes/op")
		b.ReportMetric(float64(memoryAfter.NumGC-memoryBefore.NumGC)/operations, "recompaction-gc-cycles/op")
		b.ReportMetric(float64(memoryAfter.PauseTotalNs-memoryBefore.PauseTotalNs)/operations, "recompaction-gc-pause-ns/op")
	}
	b.ReportMetric(float64(len(liveKeys)), "live-entries/op")
	b.ReportMetric(float64(interval.Milliseconds()), "recompaction-interval-ms")
}

func benchmarkProcessCPUTime(b *testing.B) int64 {
	b.Helper()

	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	return usage.Utime.Sec*int64(time.Second) + usage.Utime.Usec*int64(time.Microsecond) +
		usage.Stime.Sec*int64(time.Second) + usage.Stime.Usec*int64(time.Microsecond)
}

func prepareRecompactionLoopSegment(b *testing.B, env *compactionServingBenchmark, liveValue, deletedValue []byte) ([]string, []string, []string) {
	b.Helper()

	liveKeys := make([]string, 0, benchmarkRecompactionTargetCount*benchmarkRecompactionLiveEntryCount)
	deletedKeys := make([]string, 0, benchmarkRecompactionTargetCount*benchmarkRecompactionDeletedEntryCount)
	oldPaths := make([]string, 0, benchmarkRecompactionTargetCount)
	for targetIndex := 0; targetIndex < benchmarkRecompactionTargetCount; targetIndex++ {
		liveStart := len(liveKeys)
		entries := make([]benchmarkSegmentEntry, 0, benchmarkRecompactionLiveEntryCount+benchmarkRecompactionDeletedEntryCount)
		for i := 0; i < benchmarkRecompactionLiveEntryCount; i++ {
			key := fmt.Sprintf("recompaction-loop-%d-live-%d", targetIndex, i)
			liveKeys = append(liveKeys, key)
			entries = append(entries, benchmarkSegmentEntry{key: key, value: liveValue})
		}
		liveEnd := len(liveKeys)
		deletedStart := len(deletedKeys)
		for i := 0; i < benchmarkRecompactionDeletedEntryCount; i++ {
			key := fmt.Sprintf("recompaction-loop-%d-deleted-%d", targetIndex, i)
			deletedKeys = append(deletedKeys, key)
			entries = append(entries, benchmarkSegmentEntry{key: key, value: deletedValue})
		}
		deletedEnd := len(deletedKeys)

		target := createBenchmarkSegment(b, env, entries)
		oldPath := target.Path()
		agedPath := ageBenchmarkSegment(b, env, target, liveKeys[liveStart])
		updateRecompactionLoopPaths(b, env, liveKeys[liveStart+1:liveEnd], oldPath, agedPath)

		writeOptions := grocksdb.NewDefaultWriteOptions()
		for _, key := range deletedKeys[deletedStart:deletedEnd] {
			if err := env.meta.Handle().Delete(writeOptions, keys.MakeMetadataKey(key)); err != nil {
				writeOptions.Destroy()
				b.Fatal(err)
			}
		}
		deleteIndex := &pb.DeleteIndexEntry{
			DeletedEntries: int64(deletedEnd - deletedStart),
			DeletedBytes:   int64((deletedEnd - deletedStart) * len(deletedValue)),
		}
		deleteIndexBytes, err := proto.Marshal(deleteIndex)
		if err != nil {
			writeOptions.Destroy()
			b.Fatal(err)
		}
		if err := env.meta.Handle().Put(writeOptions, keys.MakeDeleteIndexKey(agedPath), deleteIndexBytes); err != nil {
			writeOptions.Destroy()
			b.Fatal(err)
		}
		writeOptions.Destroy()
		oldPaths = append(oldPaths, agedPath)
	}

	// Keep a closed control segment so the production minimum-segment gate is
	// exercised. It has no delete index and is skipped after its liveness walk.
	createBenchmarkSegment(b, env, []benchmarkSegmentEntry{{
		key:   "recompaction-loop-control",
		value: liveValue,
	}})

	return liveKeys, deletedKeys, oldPaths
}

func updateRecompactionLoopPaths(b *testing.B, env *compactionServingBenchmark, keysToUpdate []string, oldPath, agedPath string) {
	b.Helper()
	if len(keysToUpdate) == 0 {
		return
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	for _, key := range keysToUpdate {
		value, exists := benchmarkValueMessage(b, env.storage, key)
		if !exists || value.ValueType != pb.ValueType_SEGMENT || value.SegmentPath != oldPath {
			b.Fatalf("%s was not stored in %s", key, oldPath)
		}
		value.SegmentPath = agedPath
		valueBytes, err := proto.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		batch.Put(keys.MakeMetadataKey(key), valueBytes)
	}
	writeOptions := grocksdb.NewDefaultWriteOptions()
	defer writeOptions.Destroy()
	if err := env.meta.Handle().Write(writeOptions, batch); err != nil {
		b.Fatal(err)
	}
}

func waitForRecompactedValues(b *testing.B, storage *Storage, keys []string, oldPaths []string, interval time.Duration) {
	b.Helper()

	oldPathSet := make(map[string]struct{}, len(oldPaths))
	for _, oldPath := range oldPaths {
		oldPathSet[oldPath] = struct{}{}
	}
	deadline := time.Now().Add(interval + 10*time.Second)
	for {
		complete := true
		for _, key := range keys {
			if recompactedValueFromPaths(b, storage, key, oldPathSet) == nil {
				complete = false
				break
			}
		}
		if complete {
			return
		}
		if time.Now().After(deadline) {
			b.Fatal("segmentRecompactionLoop did not replace every live entry at the configured interval")
		}
		time.Sleep(time.Millisecond)
	}
}

func recompactedValueFromPaths(b *testing.B, storage *Storage, key string, oldPaths map[string]struct{}) *pb.ValueMessage {
	b.Helper()

	value, exists := benchmarkValueMessage(b, storage, key)
	if exists && value.ValueType == pb.ValueType_SEGMENT {
		if _, old := oldPaths[value.SegmentPath]; !old {
			return value
		}
	}
	return nil
}
