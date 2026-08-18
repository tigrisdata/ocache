// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/service"
	"github.com/tigrisdata/ocache/storage"
	"github.com/tigrisdata/ocache/storage/benchio"
)

const (
	servingInlineSize               = storage.DefaultInlineThreshold
	servingSegmentSize              = 2 * 1024 * 1024
	servingRawSize                  = storage.DefaultCompactThreshold + 1
	servingRangeSize                = 64 * 1024
	servingRoundInterval            = 10 * time.Millisecond
	benchmarkInlineReadsPerRound    = 4
	benchmarkMinimumInlineSamples   = 1000
	benchmarkReadBudget             = 96 * 1024 * 1024
	benchmarkServingObservationTime = 6 * time.Second
	benchmarkBacklogDrainTimeout    = 45 * time.Second
	benchmarkCompactionCycles       = 5
)

// Each source is above the inline boundary and at or below the compaction
// threshold, so every input enters the ordinary file-compaction path.
var compactionServingInputSizes = []int64{
	storage.DefaultInlineThreshold + 1,
	512 * 1024,
	4 * 1024 * 1024,
	storage.DefaultCompactThreshold,
}

// repeatReader supplies deterministic payloads without retaining the benchmark
// workload in memory.
type repeatReader struct {
	remaining int64
	fill      byte
}

func (r *repeatReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}

	n := int64(len(p))
	if n > r.remaining {
		n = r.remaining
	}
	for i := range p[:n] {
		p[i] = r.fill
	}
	r.remaining -= n
	return int(n), nil
}

// benchmarkGetStream verifies the payload while discarding it, so CacheService
// follows its normal streaming path without retaining each response.
type benchmarkGetStream struct {
	pb.CacheService_GetServer
	ctx   context.Context
	fill  byte
	bytes int64
}

func (s *benchmarkGetStream) Context() context.Context {
	return s.ctx
}

func (s *benchmarkGetStream) Send(response *pb.GetResponse) error {
	for _, value := range response.Data {
		if value != s.fill {
			return fmt.Errorf("unexpected payload for CacheService.Get")
		}
	}
	s.bytes += int64(len(response.Data))
	return nil
}

type getOperation struct {
	name  string
	key   string
	start int64
	end   int64
	bytes int64
	fill  byte
}

type compactionServingEnvironment struct {
	storage *storage.Storage
	service *service.CacheService
	disk    string

	inlineFull   getOperation
	rawRange     getOperation
	segmentRange getOperation
	rawFull      getOperation
	segmentFull  getOperation
}

func newCompactionServingEnvironment(tb testing.TB) *compactionServingEnvironment {
	tb.Helper()

	disk := tb.TempDir()
	// Use the supported Storage defaults: a 64 KiB inline threshold, a 64 MiB
	// compaction threshold, 256 MiB segments, one worker, and enabled
	// recompaction. This workload creates no eligible fragmented segment, so the
	// measured background copy is the ordinary file-compaction path.
	s, err := storage.NewStorageWithConfig(&storage.StorageConfig{DiskPath: disk})
	if err != nil {
		tb.Fatalf("create storage: %v", err)
	}

	env := &compactionServingEnvironment{
		storage: s,
		service: service.NewCacheService(nil, s),
		disk:    disk,
		inlineFull: getOperation{
			name: "inline-read", key: "serving-inline", end: servingInlineSize - 1,
			bytes: servingInlineSize, fill: 'i',
		},
		rawRange: getOperation{
			name: "raw-range-read", key: "serving-raw", start: servingRangeSize,
			end: servingRangeSize + servingRangeSize - 1, bytes: servingRangeSize, fill: 'r',
		},
		segmentRange: getOperation{
			name: "segment-range-read", key: "serving-segment", start: servingRangeSize,
			end: servingRangeSize + servingRangeSize - 1, bytes: servingRangeSize, fill: 's',
		},
		rawFull: getOperation{
			name: "raw-full-read", key: "serving-raw", bytes: servingRawSize, fill: 'r',
		},
		segmentFull: getOperation{
			name: "segment-full-read", key: "serving-segment", bytes: servingSegmentSize, fill: 's',
		},
	}

	env.put(tb, env.inlineFull.key, servingInlineSize, env.inlineFull.fill)
	env.put(tb, env.rawRange.key, servingRawSize, env.rawRange.fill)
	env.put(tb, env.segmentRange.key, servingSegmentSize, env.segmentRange.fill)
	waitForCompactionDrain(tb, disk, 1, 1, 10*time.Second)

	for _, op := range []getOperation{env.inlineFull, env.rawRange, env.segmentRange} {
		if _, err := env.get(op); err != nil {
			env.close()
			tb.Fatalf("warm %s: %v", op.name, err)
		}
	}

	return env
}

func (env *compactionServingEnvironment) close() {
	if env.storage != nil {
		env.storage.Close()
		env.storage = nil
	}
}

func (env *compactionServingEnvironment) put(tb testing.TB, key string, size int64, fill byte) {
	tb.Helper()
	if err := env.storage.Put(key, &repeatReader{remaining: size, fill: fill}, 0); err != nil {
		tb.Fatalf("put %q: %v", key, err)
	}
}

func (env *compactionServingEnvironment) get(op getOperation) (time.Duration, error) {
	stream := &benchmarkGetStream{ctx: context.Background(), fill: op.fill}
	started := time.Now()
	err := env.service.Get(&pb.GetRequest{
		Key:   op.key,
		Start: op.start,
		End:   op.end,
	}, stream)
	elapsed := time.Since(started)
	if err != nil {
		return elapsed, err
	}
	if stream.bytes != op.bytes {
		return elapsed, fmt.Errorf("read %d bytes, want %d", stream.bytes, op.bytes)
	}
	return elapsed, nil
}

func dataFileCount(disk, directory string) (int, error) {
	entries, err := os.ReadDir(filepath.Join(disk, directory))
	if err != nil {
		return 0, err
	}

	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			count++
		}
	}
	return count, nil
}

func rawFileCount(disk string) (int, error) {
	return dataFileCount(disk, "files")
}

func segmentFileCount(disk string) (int, error) {
	return dataFileCount(disk, "segments")
}

func waitForCompactionDrain(tb testing.TB, disk string, expectedRawFiles, minSegmentFiles int, timeout time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for {
		rawFiles, err := rawFileCount(disk)
		if err != nil {
			tb.Fatalf("count raw files: %v", err)
		}
		segmentFiles, err := segmentFileCount(disk)
		if err != nil {
			tb.Fatalf("count segment files: %v", err)
		}
		if rawFiles == expectedRawFiles && segmentFiles >= minSegmentFiles {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf(
				"compaction did not drain to %d raw files and %d segments before %s; have %d raw files and %d segments",
				expectedRawFiles, minSegmentFiles, timeout, rawFiles, segmentFiles,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (env *compactionServingEnvironment) populateCompactionBacklog(tb testing.TB) (int64, []getOperation) {
	tb.Helper()

	var bytes int64
	inputs := make([]getOperation, 0, benchmarkCompactionCycles*len(compactionServingInputSizes))
	for cycle := 0; cycle < benchmarkCompactionCycles; cycle++ {
		for sizeIndex, size := range compactionServingInputSizes {
			key := fmt.Sprintf("compaction-%d-%d", cycle, sizeIndex)
			fill := byte('a' + sizeIndex)
			env.put(tb, key, size, fill)
			inputs = append(inputs, getOperation{
				name: fmt.Sprintf("compacted-%s", key), key: key, bytes: size, fill: fill,
			})
			bytes += size
		}
	}
	return bytes, inputs
}

func TestCompactionServingGetSchedule(t *testing.T) {
	env := newCompactionServingEnvironment(t)
	defer env.close()

	const compactedSize = 2 * 1024 * 1024
	env.put(t, "compaction-test", compactedSize, 'c')
	for _, op := range []getOperation{env.inlineFull, env.rawRange, env.segmentRange, env.rawFull, env.segmentFull} {
		if _, err := env.get(op); err != nil {
			t.Fatalf("read %s while compaction drains: %v", op.name, err)
		}
	}
	waitForCompactionDrain(t, env.disk, 1, 1, 10*time.Second)
	if _, err := env.get(getOperation{
		name: "compacted-read", key: "compaction-test", bytes: compactedSize, fill: 'c',
	}); err != nil {
		t.Fatalf("read compacted value: %v", err)
	}
}

type latencyRecorder struct {
	mu      sync.Mutex
	samples map[string][]time.Duration
	err     error
}

func newLatencyRecorder() *latencyRecorder {
	return &latencyRecorder{samples: make(map[string][]time.Duration)}
}

func (r *latencyRecorder) add(name string, latency time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		if r.err == nil {
			r.err = fmt.Errorf("%s: %w", name, err)
		}
		return
	}
	r.samples[name] = append(r.samples[name], latency)
}

func (r *latencyRecorder) snapshot() (map[string][]time.Duration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	samples := make(map[string][]time.Duration, len(r.samples))
	for name, values := range r.samples {
		samples[name] = append([]time.Duration(nil), values...)
	}
	return samples, r.err
}

func (env *compactionServingEnvironment) serveUntilStopped(start <-chan struct{}, stop <-chan struct{}, recorder *latencyRecorder, done *sync.WaitGroup) {
	defer done.Done()
	<-start

	for round := 0; ; round++ {
		// The short inline shape is repeated so its p99 is sampled throughout
		// the same active compaction window as the disk-backed serving shapes.
		operations := make([]getOperation, 0, benchmarkInlineReadsPerRound+4)
		for range benchmarkInlineReadsPerRound {
			operations = append(operations, env.inlineFull)
		}
		operations = append(operations, env.rawRange, env.segmentRange)
		// A full raw read per client covers the largest serving shape without
		// continuously consuming the lane that the compaction cap is meant to
		// reserve for foreground traffic.
		if round == 0 {
			operations = append(operations, env.rawFull)
		}
		if round%64 == 0 {
			operations = append(operations, env.segmentFull)
		}

		for _, op := range operations {
			select {
			case <-stop:
				return
			default:
			}

			latency, err := env.get(op)
			recorder.add(op.name, latency, err)
			if err != nil {
				return
			}
		}

		select {
		case <-stop:
			return
		case <-time.After(servingRoundInterval):
		}
	}
}

func percentile(values []time.Duration, percent int) time.Duration {
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := (len(sorted)*percent + 99) / 100
	return sorted[index-1]
}

func runCacheServiceGetDuringCompaction(b *testing.B) {
	b.Helper()

	// The storage workers are deliberately chatty at the test logger's debug
	// level. Keep their diagnostic output out of the benchmark process so the
	// measured lane is payload I/O, not a pipe to the test runner. env.close
	// stops those workers before the process logger is restored.
	previousLogger := zlog.Logger
	zlog.Logger = zerolog.Nop()
	defer func() { zlog.Logger = previousLogger }()

	env := newCompactionServingEnvironment(b)
	defer env.close()
	// The benchmark-only gate is a shared payload-read lane. It admits direct
	// raw/segment compaction source reads and raw/segment CacheService.Get reads;
	// segment writes remain outside it, matching the source-read product limit.
	restoreReadGate := benchio.SetReadRateLimitForBenchmark(benchmarkReadBudget)
	defer restoreReadGate()

	start := make(chan struct{})
	stop := make(chan struct{})
	recorder := newLatencyRecorder()
	var readers sync.WaitGroup
	readers.Add(2)
	for range 2 {
		go env.serveUntilStopped(start, stop, recorder, &readers)
	}

	drainStarted := time.Now()
	compactionBytes, compactedInputs := env.populateCompactionBacklog(b)
	close(start)

	// Record serving tails over an equal, active compaction interval on both
	// sides. The raw-file check prevents a fast baseline from including
	// post-drain reads in its samples.
	time.Sleep(benchmarkServingObservationTime)
	rawFiles, err := rawFileCount(env.disk)
	if err != nil {
		b.Fatalf("count raw files during serving observation: %v", err)
	}
	if rawFiles <= 1 {
		b.Fatal("compaction drained before the serving observation completed")
	}
	close(stop)
	readers.Wait()

	waitForCompactionDrain(b, env.disk, 1, 2, benchmarkBacklogDrainTimeout)
	drainElapsed := time.Since(drainStarted)

	// Confirm each migrated payload remains readable after the source files are
	// deleted, so the measured drain ends with published, intact segment data.
	for _, input := range compactedInputs {
		if _, err := env.get(input); err != nil {
			b.Fatalf("read %s after compaction: %v", input.name, err)
		}
	}

	samples, err := recorder.snapshot()
	if err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{
		env.inlineFull.name,
		env.rawRange.name,
		env.segmentRange.name,
		env.rawFull.name,
		env.segmentFull.name,
	} {
		values := samples[name]
		if len(values) == 0 {
			b.Fatalf("no %s samples while compaction drained", name)
		}
		if name == env.inlineFull.name && len(values) < benchmarkMinimumInlineSamples {
			b.Fatalf("got %d inline samples, want at least %d", len(values), benchmarkMinimumInlineSamples)
		}
		b.ReportMetric(float64(percentile(values, 95).Microseconds()), name+"-p95-us")
		b.ReportMetric(float64(percentile(values, 99).Microseconds()), name+"-p99-us")
	}
	inlineP99 := percentile(samples[env.inlineFull.name], 99)
	if inlineP99 >= time.Millisecond {
		b.Fatalf("inline p99 %s exceeds 1ms", inlineP99)
	}
	// This is the repository's small-object p99 target. Keep the raw value
	// above for diagnosis and publish the checked bound for the contract.
	b.ReportMetric(1, "inline-read-p99-under-1ms")
	b.ReportMetric(float64(compactionBytes)/(1024*1024)/drainElapsed.Seconds(), "compaction-MB/s")
	b.ReportMetric(float64(drainElapsed.Milliseconds()), "backlog-drain-ms")
	// waitForCompactionDrain fails at the fixed bound. Publish the successful
	// bound explicitly so the contract guards the intentional drain tradeoff.
	b.ReportMetric(1, "backlog-drain-under-45s")
}

func BenchmarkCacheServiceGetDuringCompaction(b *testing.B) {
	// Fix CPU scheduling capacity for paired runs; Storage settings above remain
	// at their supported defaults.
	previousProcs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(previousProcs)

	for b.Loop() {
		runCacheServiceGetDuringCompaction(b)
	}
}
