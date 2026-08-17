//go:build linux

// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
	"unsafe"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/tigrisdata/ocache/storage/compaction"
	"github.com/tigrisdata/ocache/storage/deletion"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/files"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	benchmarkServingValueSize = 8 * 1024 * 1024
	benchmarkServingKeyCount  = 8
	benchmarkServingReadSize  = 64 * 1024
)

var benchmarkCompactionRawFileSizes = []int{
	DefaultInlineThreshold + 1,
	512 * 1024,
	4 * 1024 * 1024,
	int(DefaultCompactThreshold),
	int(DefaultCompactThreshold),
	int(DefaultCompactThreshold),
}

type compactionServingBenchmark struct {
	storage        *Storage
	meta           *metadata.MetaDB
	segmentManager *segment.Manager
	compactor      *compaction.Compactor
	close          func()
}

type benchmarkRawFile struct {
	key  string
	size int64
	fill byte
	path string
}

type benchmarkServingFile struct {
	key           string
	segmentPath   string
	segmentOffset int64
}

type benchmarkServingMetrics struct {
	readMissPages int64
	readRounds    int64
	p95           time.Duration
	p99           time.Duration
}

// BenchmarkFileCompactionServingCache runs the ordinary background
// fileCompactionLoop at the production compact threshold and segment size. It
// warms a 64 MiB serving segment, then reads it while the loop moves values at
// the inline boundary, 512 KiB, 4 MiB, and three 64 MiB maximum-size values.
// That batch makes the raw input and target output materially larger than the
// serving set, creating page-cache pressure on a memory-constrained host.
func BenchmarkFileCompactionServingCache(b *testing.B) {
	inputs := benchmarkRawFileInputs(benchmarkCompactionRawFileSizes)
	servingValue := bytes.Repeat([]byte("s"), benchmarkServingValueSize)

	var (
		compactionPages int64
		servingMetrics  benchmarkServingMetrics
	)
	for b.Loop() {
		pages, serving := runFileCompactionServingScenario(b, inputs, servingValue)
		compactionPages += pages
		servingMetrics.readMissPages += serving.readMissPages
		servingMetrics.p95 += serving.p95
		servingMetrics.p99 += serving.p99
	}

	reportServingBenchmarkMetrics(b, compactionPages, servingMetrics, "compaction-resident-pages/op")
}

func runFileCompactionServingScenario(b *testing.B, inputs []benchmarkRawFile, servingValue []byte) (int64, benchmarkServingMetrics) {
	env := newFileCompactionServingBenchmark(b)
	defer env.close()

	servingFiles := prepareServingWorkingSet(b, env, servingValue)
	for i := range inputs {
		input := &inputs[i]
		if err := env.storage.Put(input.key, &benchmarkByteReader{remaining: input.size, fill: input.fill}, 0); err != nil {
			b.Fatal(err)
		}
		value, exists := benchmarkValueMessage(b, env.storage, input.key)
		if !exists || value.ValueType != pb.ValueType_RAW_FILE {
			b.Fatalf("%s was not stored as a raw file", input.key)
		}
		input.path = value.RawFilePath
	}

	warmServingWorkingSet(b, env.storage, servingFiles)
	env.compactor.Start()
	servingPaths := benchmarkServingPaths(servingFiles)
	waitForCompactionOutput(b, env.segmentManager, servingPaths, func() bool {
		return compactedValues(b, env.storage, inputs)
	}, "fileCompactionLoop")
	serving := measureServingReadsWhileCompacting(b, env.storage, servingFiles, func() bool {
		return compactedValues(b, env.storage, inputs)
	})
	waitForCompactedValues(b, env.storage, inputs)
	env.compactor.Close()

	compactionPages := countRawFilePages(b, inputs)
	for _, seg := range env.segmentManager.GetSegments() {
		if _, isServing := servingPaths[seg.Path()]; isServing {
			continue
		}
		pages, err := residentPageCount(seg.Path(), seg.GetSize())
		if err != nil {
			b.Fatal(err)
		}
		compactionPages += int64(pages)
	}

	for _, input := range inputs {
		requireStoredRawFile(b, env.storage, input)
	}

	return compactionPages, serving
}

// BenchmarkRecompactionServingCache uses Storage's timer-driven recompaction
// loop with its production defaults. It gives the closed test segment an aged
// filename, which is the production age signal, before starting the default
// loop so the two-hour eligibility gate is exercised without sleeping.
func BenchmarkRecompactionServingCache(b *testing.B) {
	servingValue := bytes.Repeat([]byte("s"), benchmarkServingValueSize)
	// Keep the source below the default 256 MiB segment size while making the
	// live copy long enough for serving reads to overlap it on fast disks.
	liveValue := bytes.Repeat([]byte("l"), 127*1024*1024)
	deletedValue := bytes.Repeat([]byte("d"), 128*1024*1024)

	var (
		recompactionPages int64
		servingMetrics    benchmarkServingMetrics
	)
	for b.Loop() {
		pages, serving := runRecompactionServingScenario(b, servingValue, liveValue, deletedValue)
		recompactionPages += pages
		servingMetrics.readMissPages += serving.readMissPages
		servingMetrics.p95 += serving.p95
		servingMetrics.p99 += serving.p99
	}

	reportServingBenchmarkMetrics(b, recompactionPages, servingMetrics, "recompaction-output-resident-pages/op")
}

func runRecompactionServingScenario(b *testing.B, servingValue, liveValue, deletedValue []byte) (int64, benchmarkServingMetrics) {
	env := newRecompactionSetupBenchmark(b)
	defer env.close()

	servingFiles := prepareServingWorkingSet(b, env, servingValue)

	const (
		liveKey    = "recompaction-live"
		deletedKey = "recompaction-deleted"
	)
	oldSegment := createBenchmarkSegment(b, env, []benchmarkSegmentEntry{
		{key: liveKey, value: liveValue},
		{key: deletedKey, value: deletedValue},
	})
	oldPath := ageBenchmarkSegment(b, env, oldSegment, liveKey)

	writeOptions := grocksdb.NewDefaultWriteOptions()
	if err := env.meta.Handle().Delete(writeOptions, keys.MakeMetadataKey(deletedKey)); err != nil {
		writeOptions.Destroy()
		b.Fatal(err)
	}
	deleteIndex := &pb.DeleteIndexEntry{
		DeletedEntries: 1,
		DeletedBytes:   int64(len(deletedValue)),
	}
	deleteIndexBytes, err := proto.Marshal(deleteIndex)
	if err != nil {
		writeOptions.Destroy()
		b.Fatal(err)
	}
	if err := env.meta.Handle().Put(writeOptions, keys.MakeDeleteIndexKey(oldPath), deleteIndexBytes); err != nil {
		writeOptions.Destroy()
		b.Fatal(err)
	}
	writeOptions.Destroy()

	startDefaultRecompactionBenchmark(b, env)
	warmServingWorkingSet(b, env.storage, servingFiles)
	if err := evictBenchmarkFile(oldPath); err != nil {
		b.Fatal(err)
	}

	servingPaths := benchmarkServingPaths(servingFiles)
	waitForRecompactionOutput(b, env.segmentManager, servingPaths, func() bool {
		return recompactedValue(b, env.storage, liveKey, oldPath) != nil
	})
	serving := measureServingReadsWhileCompacting(b, env.storage, servingFiles, func() bool {
		return recompactedValue(b, env.storage, liveKey, oldPath) != nil
	})
	compacted := waitForRecompactedValue(b, env.storage, liveKey, oldPath)
	newSegment := env.segmentManager.GetSegmentByPath(compacted.SegmentPath)
	if newSegment == nil {
		b.Fatal("recompaction published an unknown segment")
	}
	newPages, err := residentPageCount(newSegment.Path(), newSegment.GetSize())
	if err != nil {
		b.Fatal(err)
	}

	requireStoredValue(b, env.storage, liveKey, liveValue)
	if _, exists := benchmarkValueMessage(b, env.storage, deletedKey); exists {
		b.Fatal("recompaction restored deleted metadata")
	}

	return int64(newPages), serving
}

func newFileCompactionServingBenchmark(b *testing.B) *compactionServingBenchmark {
	b.Helper()

	tmpDir := b.TempDir()
	meta, err := metadata.NewMetaDB(tmpDir, 0, merge.NewMultiplexOperator(), nil)
	if err != nil {
		b.Fatal(err)
	}
	fdCache := fd.NewFdCache(100)
	segmentManager, err := segment.NewManager(tmpDir, DefaultSegmentSize)
	if err != nil {
		meta.Close()
		b.Fatal(err)
	}
	fileManager, err := files.NewFileManager(tmpDir, DefaultCompactThreshold)
	if err != nil {
		segmentManager.Close()
		meta.Close()
		b.Fatal(err)
	}
	deletionQueue := deletion.NewQueue(meta, benchmarkDeletionQueueConfig())
	storage := &Storage{
		meta:             meta,
		diskPath:         tmpDir,
		inlineThreshold:  DefaultInlineThreshold,
		compactThreshold: DefaultCompactThreshold,
		segmentManager:   segmentManager,
		fileManager:      fileManager,
		fdCache:          fdCache,
		deletionQueue:    deletionQueue,
		cleaner:          &Cleaner{},
	}
	storage.cleaner.storage = storage
	compactor := compaction.NewCompactorWithConfig(&compaction.CompactorConfig{
		MetaDB:         meta,
		FileManager:    fileManager,
		SegmentManager: segmentManager,
		DeletionQueue:  deletionQueue,
	})
	storage.compactor = compactor

	return &compactionServingBenchmark{
		storage:        storage,
		meta:           meta,
		segmentManager: segmentManager,
		compactor:      compactor,
		close: func() {
			compactor.Close()
			segmentManager.Close()
			meta.Close()
		},
	}
}

func newRecompactionSetupBenchmark(b *testing.B) *compactionServingBenchmark {
	b.Helper()

	storage, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:            b.TempDir(),
		DisableRecompaction: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	return &compactionServingBenchmark{
		storage:        storage,
		meta:           storage.meta,
		segmentManager: storage.segmentManager,
		compactor:      storage.compactor,
		close:          storage.Close,
	}
}

func startDefaultRecompactionBenchmark(b *testing.B, env *compactionServingBenchmark) {
	b.Helper()

	env.compactor.Close()
	compactor := compaction.NewCompactorWithConfig(&compaction.CompactorConfig{
		MetaDB:               env.meta,
		FileManager:          env.storage.fileManager,
		SegmentManager:       env.segmentManager,
		DeletionQueue:        env.storage.deletionQueue,
		CompactionThreads:    DefaultCompactionThreads,
		EnableRecompaction:   true,
		FragThreshold:        DefaultFragmentationThreshold,
		MinSegmentAge:        DefaultMinSegmentAgeForRecompaction,
		MinSegments:          DefaultMinSegmentsBeforeRecompaction,
		RecompactionInterval: DefaultRecompactionInterval,
	})
	env.compactor = compactor
	env.storage.compactor = compactor
	compactor.Start()
}

func benchmarkDeletionQueueConfig() deletion.Config {
	return deletion.Config{
		BatchSize:       1000,
		ProcessInterval: time.Hour,
		PruneAge:        24 * time.Hour,
	}
}

func benchmarkRawFileInputs(sizes []int) []benchmarkRawFile {
	inputs := make([]benchmarkRawFile, 0, len(sizes))
	for index, size := range sizes {
		inputs = append(inputs, benchmarkRawFile{
			key:  fmt.Sprintf("compaction-%d-%d", index, size),
			size: int64(size),
			fill: byte(index + 1),
		})
	}
	return inputs
}

func prepareServingWorkingSet(b *testing.B, env *compactionServingBenchmark, value []byte) []benchmarkServingFile {
	b.Helper()

	entries := make([]benchmarkSegmentEntry, 0, benchmarkServingKeyCount)
	for n := 0; n < benchmarkServingKeyCount; n++ {
		entries = append(entries, benchmarkSegmentEntry{
			key:   fmt.Sprintf("serving-%d", n),
			value: value,
		})
	}
	seg := createBenchmarkSegment(b, env, entries)
	if err := evictBenchmarkFile(seg.Path()); err != nil {
		b.Fatal(err)
	}

	files := make([]benchmarkServingFile, 0, benchmarkServingKeyCount)
	for n := 0; n < benchmarkServingKeyCount; n++ {
		key := fmt.Sprintf("serving-%d", n)
		value, exists := benchmarkValueMessage(b, env.storage, key)
		if !exists || value.ValueType != pb.ValueType_SEGMENT || value.SegmentPath != seg.Path() {
			b.Fatalf("%s was not stored in the serving segment", key)
		}
		files = append(files, benchmarkServingFile{
			key:           key,
			segmentPath:   seg.Path(),
			segmentOffset: value.SegmentOffset,
		})
	}
	return files
}

func warmServingWorkingSet(b *testing.B, storage *Storage, files []benchmarkServingFile) {
	b.Helper()
	for _, file := range files {
		n, err := readStoredValue(storage, file.key, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
		if n != benchmarkServingValueSize {
			b.Fatalf("warmed %d bytes for %s, want %d", n, file.key, benchmarkServingValueSize)
		}
	}
}

// measureServingReadsWhileCompacting samples each serving range only while
// complete is false. It counts nonresident pages in the exact payload range
// immediately before its Storage.Get. The segment reader seeks directly to
// that range and the matching post-read mincore check confirms the range was
// populated, so the count records serving read-miss pages rather than a
// whole-segment residency proxy.
func measureServingReadsWhileCompacting(b *testing.B, storage *Storage, files []benchmarkServingFile, complete func() bool) benchmarkServingMetrics {
	b.Helper()

	metrics := benchmarkServingMetrics{}
	latencies := make([]time.Duration, 0, len(files))
	deadline := time.Now().Add(10 * time.Second)
	for !complete() {
		for n, file := range files {
			offset := int64((n * benchmarkServingReadSize) % (benchmarkServingValueSize - benchmarkServingReadSize))
			payloadOffset := file.segmentOffset + segment.CalculateValueHeaderSize(file.key) + offset
			residentBefore, pages, err := pageResidentCounts(file.segmentPath, payloadOffset, benchmarkServingReadSize)
			if err != nil {
				b.Fatal(err)
			}

			started := time.Now()
			read, err := readStoredValue(storage, file.key, offset, offset+benchmarkServingReadSize-1)
			latencies = append(latencies, time.Since(started))
			if err != nil {
				b.Fatal(err)
			}
			if read != benchmarkServingReadSize {
				b.Fatalf("served %d bytes for %s, want %d", read, file.key, benchmarkServingReadSize)
			}

			residentAfter, pagesAfter, err := pageResidentCounts(file.segmentPath, payloadOffset, benchmarkServingReadSize)
			if err != nil {
				b.Fatal(err)
			}
			if pagesAfter != pages || residentAfter != pagesAfter {
				b.Fatalf("serving read did not populate every page for %s", file.key)
			}
			metrics.readMissPages += int64(pages - residentBefore)
		}
		metrics.readRounds++
		if time.Now().After(deadline) {
			b.Fatal("compaction did not complete while serving reads were sampled")
		}
	}
	if metrics.readRounds == 0 {
		b.Fatal("compaction completed before any serving reads overlapped it")
	}

	metrics.p95 = benchmarkPercentile(latencies, 95)
	metrics.p99 = benchmarkPercentile(latencies, 99)
	return metrics
}

func reportServingBenchmarkMetrics(b *testing.B, residentPages int64, serving benchmarkServingMetrics, residentMetric string) {
	b.Helper()
	operations := float64(b.N)
	b.ReportMetric(float64(residentPages)/operations, residentMetric)
	b.ReportMetric(float64(serving.readMissPages)/operations, "serving-read-miss-pages-during-compaction/op")
	b.ReportMetric(float64(serving.p95.Nanoseconds())/operations, "serving-read-p95-during-compaction-ns")
	b.ReportMetric(float64(serving.p99.Nanoseconds())/operations, "serving-read-p99-during-compaction-ns")
}

func waitForCompactedValues(b *testing.B, storage *Storage, inputs []benchmarkRawFile) {
	b.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for !compactedValues(b, storage, inputs) {
		if time.Now().After(deadline) {
			b.Fatal("fileCompactionLoop did not migrate all raw files")
		}
		time.Sleep(time.Millisecond)
	}
}

func compactedValues(b *testing.B, storage *Storage, inputs []benchmarkRawFile) bool {
	b.Helper()

	for _, input := range inputs {
		value, exists := benchmarkValueMessage(b, storage, input.key)
		if !exists || value.ValueType != pb.ValueType_SEGMENT {
			return false
		}
	}
	return true
}

func waitForRecompactedValue(b *testing.B, storage *Storage, key, oldPath string) *pb.ValueMessage {
	b.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if value := recompactedValue(b, storage, key, oldPath); value != nil {
			return value
		}
		if time.Now().After(deadline) {
			b.Fatal("segmentRecompactionLoop did not replace the fragmented segment")
		}
		time.Sleep(time.Millisecond)
	}
}

func recompactedValue(b *testing.B, storage *Storage, key, oldPath string) *pb.ValueMessage {
	b.Helper()

	value, exists := benchmarkValueMessage(b, storage, key)
	if exists && value.ValueType == pb.ValueType_SEGMENT && value.SegmentPath != oldPath {
		return value
	}
	return nil
}

func waitForCompactionOutput(b *testing.B, segmentManager *segment.Manager, servingPaths map[string]struct{}, complete func() bool, loopName string) {
	b.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, seg := range segmentManager.GetOpenSegments() {
			if _, serving := servingPaths[seg.Path()]; !serving && !complete() {
				return
			}
		}
		if complete() {
			b.Fatalf("%s completed before serving reads could overlap its output", loopName)
		}
		if time.Now().After(deadline) {
			b.Fatalf("%s did not create an output segment", loopName)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForRecompactionOutput ignores the idle, zero-length segment held by the
// file compactor. Recompaction may reuse that segment, so growth beyond its
// initial size is the observable start of the timer-driven copy.
func waitForRecompactionOutput(b *testing.B, segmentManager *segment.Manager, servingPaths map[string]struct{}, complete func() bool) {
	b.Helper()

	initialSizes := make(map[string]int64)
	for _, seg := range segmentManager.GetOpenSegments() {
		initialSizes[seg.Path()] = seg.GetSize()
	}

	deadline := time.Now().Add(DefaultRecompactionInterval + 10*time.Second)
	for {
		for _, seg := range segmentManager.GetOpenSegments() {
			if _, serving := servingPaths[seg.Path()]; serving || seg.GetSize() <= initialSizes[seg.Path()] {
				continue
			}
			if !complete() {
				return
			}
		}
		if complete() {
			b.Fatal("segmentRecompactionLoop completed before serving reads could overlap its output")
		}
		if time.Now().After(deadline) {
			b.Fatal("segmentRecompactionLoop did not create output at the default interval")
		}
		time.Sleep(time.Millisecond)
	}
}

func countRawFilePages(b *testing.B, files []benchmarkRawFile) int64 {
	b.Helper()

	var pages int64
	for _, file := range files {
		count, err := residentPageCount(file.path, file.size)
		if err != nil {
			b.Fatal(err)
		}
		pages += int64(count)
	}
	return pages
}

func benchmarkServingPaths(files []benchmarkServingFile) map[string]struct{} {
	paths := make(map[string]struct{})
	for _, file := range files {
		paths[file.segmentPath] = struct{}{}
	}
	return paths
}

// ageBenchmarkSegment gives a closed benchmark segment a filename older than
// the default eligibility window. Recompaction derives segment age from this
// production filename timestamp. Re-registering the closed segment under the
// renamed path preserves its known size without shortening the configured age
// gate.
func ageBenchmarkSegment(b *testing.B, env *compactionServingBenchmark, seg *segment.Segment, liveKey string) string {
	b.Helper()

	oldPath := seg.Path()
	agedPath := filepath.Join(filepath.Dir(oldPath), fmt.Sprintf("segment_%d.seg", time.Now().Add(-DefaultMinSegmentAgeForRecompaction-time.Second).UnixNano()))
	if err := os.Rename(oldPath, agedPath); err != nil {
		b.Fatal(err)
	}
	renamedSegment := env.segmentManager.RemoveSegment(oldPath)
	if renamedSegment == nil {
		b.Fatalf("segment manager did not contain %s", oldPath)
	}
	env.segmentManager.RegisterSegment(agedPath, renamedSegment.GetNumEntries(), renamedSegment.GetSize())

	value, exists := benchmarkValueMessage(b, env.storage, liveKey)
	if !exists || value.ValueType != pb.ValueType_SEGMENT || value.SegmentPath != oldPath {
		b.Fatalf("%s was not stored in %s", liveKey, oldPath)
	}
	value.SegmentPath = agedPath
	valueBytes, err := proto.Marshal(value)
	if err != nil {
		b.Fatal(err)
	}
	writeOptions := grocksdb.NewDefaultWriteOptions()
	defer writeOptions.Destroy()
	if err := env.meta.Handle().Put(writeOptions, keys.MakeMetadataKey(liveKey), valueBytes); err != nil {
		b.Fatal(err)
	}
	return agedPath
}

func createBenchmarkSegment(b *testing.B, env *compactionServingBenchmark, entries []benchmarkSegmentEntry) *segment.Segment {
	b.Helper()

	seg, err := env.segmentManager.AcquireOpenSegmentWithReservation("compaction-serving-cache", 0)
	if err != nil {
		b.Fatal(err)
	}
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	for _, entry := range entries {
		value := &pb.ValueMessage{
			ValueType:   pb.ValueType_SEGMENT,
			ValueLength: int64(len(entry.value)),
		}
		offset, err := seg.WriteEntry(entry.key, bytes.NewReader(entry.value), value)
		if err != nil {
			b.Fatal(err)
		}
		value.SegmentPath = seg.Path()
		value.SegmentOffset = offset
		valueBytes, err := proto.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		batch.Put(keys.MakeMetadataKey(entry.key), valueBytes)
	}
	writeOptions := grocksdb.NewDefaultWriteOptions()
	if err := env.meta.Handle().Write(writeOptions, batch); err != nil {
		writeOptions.Destroy()
		b.Fatal(err)
	}
	writeOptions.Destroy()
	if err := env.segmentManager.FinalizeSegment(seg); err != nil {
		b.Fatal(err)
	}
	return seg
}

type benchmarkSegmentEntry struct {
	key   string
	value []byte
}

func benchmarkValueMessage(b *testing.B, storage *Storage, key string) (*pb.ValueMessage, bool) {
	b.Helper()

	readOptions := grocksdb.NewDefaultReadOptions()
	defer readOptions.Destroy()
	slice, err := storage.meta.Handle().Get(readOptions, keys.MakeMetadataKey(key))
	if err != nil {
		b.Fatal(err)
	}
	defer slice.Free()
	if !slice.Exists() {
		return nil, false
	}
	value := &pb.ValueMessage{}
	if err := proto.Unmarshal(slice.Data(), value); err != nil {
		b.Fatal(err)
	}
	return value, true
}

type benchmarkByteReader struct {
	remaining int64
	fill      byte
}

func (r *benchmarkByteReader) Read(buffer []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := int64(len(buffer))
	if n > r.remaining {
		n = r.remaining
	}
	for i := range buffer[:n] {
		buffer[i] = r.fill
	}
	r.remaining -= n
	return int(n), nil
}

func requireStoredRawFile(b *testing.B, storage *Storage, input benchmarkRawFile) {
	b.Helper()

	reader, found, err := storage.Get(input.key, 0, 0)
	if err != nil {
		b.Fatal(err)
	}
	if !found {
		b.Fatalf("%s was not found", input.key)
	}
	buffer := make([]byte, 64*1024)
	var read int64
	for {
		n, readErr := reader.Read(buffer)
		for _, value := range buffer[:n] {
			if value != input.fill {
				_ = closeReader(reader)
				b.Fatalf("stored value for %s differs", input.key)
			}
		}
		read += int64(n)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = closeReader(reader)
			b.Fatal(readErr)
		}
	}
	if err := closeReader(reader); err != nil {
		b.Fatal(err)
	}
	if read != input.size {
		b.Fatalf("stored %d bytes for %s, want %d", read, input.key, input.size)
	}
}

func requireStoredValue(b *testing.B, storage *Storage, key string, want []byte) {
	b.Helper()

	reader, found, err := storage.Get(key, 0, 0)
	if err != nil {
		b.Fatal(err)
	}
	if !found {
		b.Fatalf("%s was not found", key)
	}
	got, err := io.ReadAll(reader)
	if closeErr := closeReader(reader); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		b.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		b.Fatalf("stored value for %s differs", key)
	}
}

func readStoredValue(storage *Storage, key string, start, end int64) (int64, error) {
	reader, found, err := storage.Get(key, start, end)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("%s was not found", key)
	}
	read, err := io.Copy(io.Discard, reader)
	if closeErr := closeReader(reader); closeErr != nil && err == nil {
		err = closeErr
	}
	return read, err
}

func closeReader(reader io.Reader) error {
	if closer, ok := reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func evictBenchmarkFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > 0 {
		_ = unix.Fadvise(int(file.Fd()), 0, info.Size(), unix.FADV_DONTNEED)
	}
	return nil
}

func TestPageResidentCountsRange(t *testing.T) {
	pageSize := int64(os.Getpagesize())
	path := filepath.Join(t.TempDir(), "pages")
	if err := os.WriteFile(path, make([]byte, 2*pageSize), 0o600); err != nil {
		t.Fatal(err)
	}

	resident, pages, err := pageResidentCounts(path, pageSize-1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if pages != 2 {
		t.Fatalf("counted %d pages, want 2", pages)
	}
	if resident < 0 || resident > pages {
		t.Fatalf("counted %d resident pages out of %d", resident, pages)
	}
}

func residentPageCount(path string, length int64) (int, error) {
	resident, _, err := pageResidentCounts(path, 0, length)
	return resident, err
}

// pageResidentCounts uses mincore without touching the file mapping. It counts
// only pages overlapping [offset, offset+length), even when the range begins
// or ends in the middle of a page.
func pageResidentCounts(path string, offset, length int64) (int, int, error) {
	if offset < 0 {
		return 0, 0, fmt.Errorf("negative file range offset: %s", path)
	}
	if length <= 0 {
		return 0, 0, nil
	}
	end := offset + length
	if end < offset {
		return 0, 0, fmt.Errorf("file range overflow: %s", path)
	}

	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	if end > info.Size() {
		return 0, 0, fmt.Errorf("file range exceeds size: %s", path)
	}

	pageSize := int64(os.Getpagesize())
	mappingOffset := offset - offset%pageSize
	mappingLength := end - mappingOffset
	if mappingLength > int64(int(^uint(0)>>1)) {
		return 0, 0, fmt.Errorf("file range too large to map: %s", path)
	}
	mapping, err := unix.Mmap(int(file.Fd()), mappingOffset, int(mappingLength), unix.PROT_NONE, unix.MAP_SHARED)
	if err != nil {
		return 0, 0, err
	}
	defer unix.Munmap(mapping)

	pageSizeInt := int(pageSize)
	residency := make([]byte, (len(mapping)+pageSizeInt-1)/pageSizeInt)
	_, _, errno := unix.Syscall(
		unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&mapping[0])),
		uintptr(len(mapping)),
		uintptr(unsafe.Pointer(&residency[0])),
	)
	if errno != 0 {
		return 0, 0, errno
	}

	firstPage := int((offset - mappingOffset) / pageSize)
	lastPage := int((end - 1 - mappingOffset) / pageSize)
	resident := 0
	for _, state := range residency[firstPage : lastPage+1] {
		if state&1 != 0 {
			resident++
		}
	}
	return resident, lastPage - firstPage + 1, nil
}

func benchmarkPercentile(samples []time.Duration, percentile int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	index := int(math.Ceil(float64(percentile)*float64(len(samples))/100)) - 1
	if index < 0 {
		index = 0
	}
	return samples[index]
}
