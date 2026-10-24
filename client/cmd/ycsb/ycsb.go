// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pterm/pterm"
	cacheclient "github.com/tigrisdata/ocache/client"
)

type OpType int

const (
	OpRead OpType = iota
	OpUpdate
	// OpCAS is one guarded read-modify-write: GetWithVersion, then PutIfVersion
	// against the version just read. This is the write-coordination pattern CAS
	// exists for (issue #254). Losing the race (a VersionMismatchError) is the
	// expected outcome under contention and is counted as a mismatch, not an
	// error; the mismatch rate is the contention signal the benchmark reports.
	OpCAS
	OpNum
)

// StreamingThreshold defines the size threshold (4MB) above which YCSB writes automatically use streaming.
const StreamingThreshold = 4 * 1024 * 1024 // 4MB

var opNames = []string{"read", "update", "cas"}

// WorkloadSpec defines the operation mix for a workload.
type WorkloadSpec struct {
	Weights [OpNum]float64 // Fraction for each op type
}

var WorkloadPresets = map[string]WorkloadSpec{
	"A": {Weights: [OpNum]float64{0.5, 0.5}},   // 50% read, 50% update
	"B": {Weights: [OpNum]float64{0.95, 0.05}}, // 95% read, 5% update
	"C": {Weights: [OpNum]float64{1.0, 0}},     // 100% read
}

func ParseWorkload(s string) (WorkloadSpec, error) {
	if preset, ok := WorkloadPresets[strings.ToUpper(s)]; ok {
		return preset, nil
	}
	// Custom: e.g. "read=70,update=30"
	var ws WorkloadSpec
	var sum float64
	for part := range strings.SplitSeq(s, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return ws, fmt.Errorf("invalid workload part: %q", part)
		}
		var opIdx int
		switch strings.ToLower(kv[0]) {
		case "read":
			opIdx = int(OpRead)
		case "update":
			opIdx = int(OpUpdate)
		case "cas":
			opIdx = int(OpCAS)
		default:
			return ws, fmt.Errorf("unknown op: %q", kv[0])
		}
		var v float64
		fmt.Sscanf(kv[1], "%f", &v)
		ws.Weights[opIdx] = v
		sum += v
	}
	if sum == 0 {
		return ws, fmt.Errorf("all weights zero")
	}
	for i := range ws.Weights {
		ws.Weights[i] /= sum
	}
	return ws, nil
}

type YCSBConfig struct {
	Addr               string        // Address of the cache service (host:port or comma-separated)
	ConnMode           string        // Connection mode: auto, simple, or cluster
	TopologyRefresh    time.Duration // Topology refresh interval (cluster mode only)
	ConnectionPoolSize int           // Number of connections per address (default: 4)
	NumKeys            int           // Number of unique keys to use in the benchmark
	ValueSize          int           // Size of each value in bytes
	NumOps             int           // Total number of operations to perform
	Concurrency        int           // Number of concurrent workers
	Workload           string        // Workload type or custom mix (e.g. "A", "B", "read=70,update=30")
	Seed               int64         // Seed for random number generation (for reproducibility)
	NoProgress         bool          // Disable progress output during benchmark
	ForceStreaming     bool          // Force streaming for writes regardless of size; reads always stream
}

type Result struct {
	Ops       int
	Duration  time.Duration
	Errors    int
	Latencies []time.Duration // All operation latencies

	// Guarded read-modify-write (OpCAS) outcomes. A mismatch is a lost race —
	// the expected outcome under contention — and is never counted in Errors.
	CASAttempts   int
	CASWins       int
	CASMismatches int
}

// workerResult is what each worker hands back when it finishes or aborts.
type workerResult struct {
	errors        int
	latencies     []time.Duration
	opCounts      []int
	casWins       int
	casMismatches int
}

// hashKey generates a consistent string key from a key number using FNV-1a 64-bit hash.
func hashKey(keyNum int) string {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	n := uint64(keyNum)
	for range 8 {
		h ^= n & 0xff
		h *= prime64
		n >>= 8
	}
	return fmt.Sprintf("user%x", h)
}

// casKey names the CAS-guarded twin of key number keyNum. CAS keys live in
// their own namespace so a key is never both plain-written and CAS-guarded
// (an unsupported mix); they are created with put-if-absent at preload.
func casKey(keyNum int) string {
	return "cas-" + hashKey(keyNum)
}

// generateValue returns a random byte slice of the given size using the provided rng.
func generateValue(rng *rand.Rand, size int) []byte {
	val := make([]byte, size)
	for i := range val {
		val[i] = byte(rng.Intn(256))
	}
	return val
}

// preloadKeys inserts NumKeys random key-value pairs into the cache service.
// isConnectionError checks if an error is a connection-level error
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "refused") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "unavailable") ||
		strings.Contains(errStr, "transport")
}

func preloadKeys(ctx context.Context, cfg YCSBConfig, ws WorkloadSpec, rng *rand.Rand) error {
	// Create client for preloading
	addrs := strings.Split(cfg.Addr, ",")
	for i, a := range addrs {
		addrs[i] = strings.TrimSpace(a)
	}

	config := &cacheclient.ClientConfig{
		Addrs:              addrs,
		Mode:               cacheclient.ConnectionMode(cfg.ConnMode),
		RefreshInterval:    cfg.TopologyRefresh,
		ConnectionPoolSize: cfg.ConnectionPoolSize,
	}

	client, err := cacheclient.NewWithConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create client for preload: %w", err)
	}
	defer client.Close()

	// Use pterm spinner for preloading only if progress is enabled
	var spinner *pterm.SpinnerPrinter
	if !cfg.NoProgress {
		spinner, _ = pterm.DefaultSpinner.
			WithText(fmt.Sprintf("Preloading %d keys...", cfg.NumKeys)).
			Start()
	}

	// Each key index does one preload put per enabled namespace (plain and/or
	// CAS), so the summary and the failure threshold are sized by puts, not keys.
	plainEnabled := ws.Weights[OpRead] > 0 || ws.Weights[OpUpdate] > 0
	casEnabled := ws.Weights[OpCAS] > 0
	expectedPuts := 0
	if plainEnabled {
		expectedPuts += cfg.NumKeys
	}
	if casEnabled {
		expectedPuts += cfg.NumKeys
	}

	useStreaming := cfg.ForceStreaming || cfg.ValueSize > StreamingThreshold
	workerCount := max(1, min(cfg.Concurrency, max(expectedPuts, 1)))

	// The preload runs the configured concurrency: one deterministic producer
	// generates keys and values in the same order as before (so the RNG state
	// after preload is unchanged), a bounded worker pool performs the puts, and
	// one collector owns the accounting and spinner output. The job queue is
	// bounded so values are released as their put completes instead of holding
	// the whole corpus in memory.
	type preloadJob struct {
		seq   int // generation order, for a stable "first error"
		key   string
		value []byte
		cas   bool
	}
	type preloadResult struct {
		seq int
		key string
		err error
	}
	jobs := make(chan preloadJob, workerCount)
	results := make(chan preloadResult, workerCount)

	var workerWg sync.WaitGroup
	workerWg.Add(workerCount)
	for range workerCount {
		go func() {
			defer workerWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					if ctx.Err() != nil {
						return
					}
					// Plain read/update keys and CAS keys live in disjoint namespaces
					// so a key is never both plain-written and CAS-guarded (an
					// unsupported mix). CAS keys are created with put-if-absent; a
					// mismatch there means the key survived from an earlier run and
					// is already CAS-owned, which is fine.
					var err error
					switch {
					case job.cas && useStreaming:
						_, err = client.PutStreamIfVersion(ctx, job.key, bytes.NewReader(job.value), 0, 0)
					case job.cas:
						_, err = client.PutIfVersion(ctx, job.key, job.value, 0, 0)
					case useStreaming:
						err = client.PutStream(ctx, job.key, bytes.NewReader(job.value), 0)
					default:
						err = client.Put(ctx, job.key, job.value, 0)
					}
					if outcome, _ := ClassifyCASResult(err); outcome == CASMismatch {
						err = nil // already present from an earlier run: fine
					}
					results <- preloadResult{seq: job.seq, key: job.key, err: err}
				}
			}
		}()
	}

	// Completion order is independent of generation order, so keep the
	// lowest-sequence error for the same first-error message the serial loop
	// produced.
	var collectWg sync.WaitGroup
	collectWg.Add(1)
	successCount, totalErrors, completedCount := 0, 0, 0
	firstErrorSeq := -1
	var firstError error
	go func() {
		defer collectWg.Done()
		for result := range results {
			completedCount++
			if result.err != nil {
				totalErrors++
				if firstErrorSeq < 0 || result.seq < firstErrorSeq {
					firstErrorSeq = result.seq
					firstError = fmt.Errorf("key %s: %w", result.key, result.err)
				}
			} else {
				successCount++
			}
			if spinner != nil && (completedCount == 1 || completedCount%100 == 0) {
				spinner.UpdateText(fmt.Sprintf("Preloading keys: %d/%d (errors: %d)",
					completedCount, expectedPuts, totalErrors))
			}
		}
	}()

	cancelled := false
	seq := 0
produce:
	for i := range cfg.NumKeys {
		for _, p := range []struct {
			enabled bool
			key     string
			cas     bool
		}{
			{plainEnabled, hashKey(i), false},
			{casEnabled, casKey(i), true},
		} {
			if !p.enabled {
				continue
			}
			// Check for cancellation before generating the next owned value.
			if ctx.Err() != nil {
				cancelled = true
				break produce
			}
			val := generateValue(rng, cfg.ValueSize)
			select {
			case <-ctx.Done():
				cancelled = true
				break produce
			case jobs <- preloadJob{seq: seq, key: p.key, value: val, cas: p.cas}:
				seq++
			}
		}
	}

	close(jobs)
	workerWg.Wait()
	close(results)
	collectWg.Wait()

	if cancelled || ctx.Err() != nil {
		if spinner != nil {
			spinner.Warning(fmt.Sprintf("Preload cancelled after %d/%d puts", completedCount, expectedPuts))
		}
		return ctx.Err()
	}

	if totalErrors > 0 {
		if spinner != nil {
			spinner.Warning(fmt.Sprintf("Preloaded %d/%d puts (%d errors)",
				successCount, expectedPuts, totalErrors))
		}
		if totalErrors > expectedPuts/10 { // If more than 10% failed, consider it a failure
			if firstError != nil {
				return fmt.Errorf("preload failed with %d errors, first error: %w", totalErrors, firstError)
			}
		}
	} else {
		if spinner != nil {
			spinner.Success(fmt.Sprintf("Preloaded %d keys", cfg.NumKeys))
		}
	}
	return nil
}

// pickOp selects an operation type based on the provided weights and a random number generator.
// It uses a cumulative distribution to select an operation based on the weights.
func pickOp(weights [OpNum]float64, rng *rand.Rand) OpType {
	x := rng.Float64()
	acc := 0.0
	for i, w := range weights {
		acc += w
		if x < acc {
			return OpType(i)
		}
	}
	return OpType(OpNum - 1)
}

// percentile returns the p-th percentile value from a sorted slice of durations.
// Uses linear interpolation between closest ranks for more accurate results.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	pos := p * float64(len(sorted)-1)
	lo := int(pos)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	loVal := float64(sorted[lo])
	hiVal := float64(sorted[hi])
	return time.Duration(loVal + frac*(hiVal-loVal))
}

func RunYCSB(cfg YCSBConfig) (Result, error) {
	return RunYCSBWithContext(context.Background(), cfg)
}

func RunYCSBWithContext(ctx context.Context, cfg YCSBConfig) (Result, error) {
	if cfg.Concurrency < 1 {
		return Result{}, fmt.Errorf("Concurrency must be at least 1")
	}

	// Create a cancellable context for the entire benchmark
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rng := rand.New(rand.NewSource(cfg.Seed))
	ws, err := ParseWorkload(cfg.Workload)
	if err != nil {
		return Result{}, err
	}
	// Preload keys with context
	if err := preloadKeys(ctx, cfg, ws, rng); err != nil {
		return Result{}, err
	}

	// Create clients for workers
	addrs := strings.Split(cfg.Addr, ",")
	for i, a := range addrs {
		addrs[i] = strings.TrimSpace(a)
	}

	// Create one client for the benchmark
	config := &cacheclient.ClientConfig{
		Addrs:              addrs,
		Mode:               cacheclient.ConnectionMode(cfg.ConnMode),
		RefreshInterval:    cfg.TopologyRefresh,
		ConnectionPoolSize: cfg.ConnectionPoolSize,
	}

	mainClient, err := cacheclient.NewWithConfig(config)
	if err != nil {
		return Result{}, fmt.Errorf("failed to create client: %w", err)
	}
	defer mainClient.Close()

	// For workers, we'll use the same client since it has internal pooling
	var workerClients []*cacheclient.Client
	for i := 0; i < cfg.Concurrency; i++ {
		workerClients = append(workerClients, mainClient)
	}

	// Create metrics collector
	metricsCollector := NewMetricsCollector()

	// Create pterm progress reporter only if progress is enabled
	var progressReporter *PtermProgressReporter
	if !cfg.NoProgress {
		progressReporter = NewPtermProgressReporter(cfg.NumOps)
		if err := progressReporter.Start(); err != nil {
			return Result{}, fmt.Errorf("failed to start progress reporter: %w", err)
		}
		defer progressReporter.Stop()
	}

	// Channel for aggregate throughput tracking
	throughputCh := make(chan struct {
		ops    int
		opType OpType
	}, 1000) // Buffered to avoid blocking workers

	// Start goroutine to track aggregate throughput
	var throughputWg sync.WaitGroup
	throughputWg.Add(1)
	go func() {
		defer throughputWg.Done()
		lastTime := time.Now()
		opsInInterval := 0
		opTypeCounts := make(map[OpType]int)

		for {
			select {
			case <-ctx.Done():
				return // Exit on context cancellation
			case update, ok := <-throughputCh:
				if !ok {
					return // Channel closed
				}
				opsInInterval += update.ops
				opTypeCounts[update.opType] += update.ops

				// Record aggregate throughput every 100ms
				if time.Since(lastTime) >= 100*time.Millisecond {
					if opsInInterval > 0 {
						throughput := float64(opsInInterval) / time.Since(lastTime).Seconds()
						// Record overall aggregate throughput
						metricsCollector.RecordThroughput(throughput, OpNum) // OpNum as sentinel for aggregate

						// Also record per-operation type throughput
						for opType, count := range opTypeCounts {
							if count > 0 {
								opThroughput := float64(count) / time.Since(lastTime).Seconds()
								metricsCollector.RecordThroughput(opThroughput, opType)
							}
						}

						opsInInterval = 0
						opTypeCounts = make(map[OpType]int)
						lastTime = time.Now()
					}
				}
			}
		}
	}()

	// Write transport selection is fixed for the duration of a run.
	useStreamingWrites := cfg.ForceStreaming || cfg.ValueSize > StreamingThreshold

	var wg sync.WaitGroup
	opsPerWorker := cfg.NumOps / cfg.Concurrency
	resultCh := make(chan workerResult, cfg.Concurrency)
	t0 := time.Now()
	for i := range cfg.Concurrency {
		wg.Add(1)
		seed := rng.Int63()        // Each goroutine gets its own seed
		client := workerClients[i] // Assign dedicated client to each worker
		go func(workerID int, seed int64, c *cacheclient.Client, reporter *PtermProgressReporter, metrics *MetricsCollector, throughputCh chan<- struct {
			ops    int
			opType OpType
		}, NoProgress bool,
		) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(seed))
			errCount := 0
			// Pre-allocate latencies slice with exact capacity to avoid reallocation
			latencies := make([]time.Duration, 0, opsPerWorker)
			opCounts := make([]int, OpNum) // Track count for each op type
			casWins, casMismatches := 0, 0 // OpCAS outcomes; a mismatch is a lost race, not an error

			for opIdx := 0; opIdx < opsPerWorker; opIdx++ {
				// Check for context cancellation
				select {
				case <-ctx.Done():
					// Context cancelled, report partial results
					resultCh <- workerResult{errCount, latencies, opCounts, casWins, casMismatches}
					return
				default:
				}

				keyNum := localRng.Intn(cfg.NumKeys)
				op := pickOp(ws.Weights, localRng)
				// CAS uses its own key namespace (preloaded with put-if-absent) so a
				// key is never both plain-written and CAS-guarded, an unsupported mix.
				k := hashKey(keyNum)
				if op == OpCAS {
					k = casKey(keyNum)
				}
				start := time.Now()
				var opErr error

				// Use context with timeout for individual operations
				opCtx, opCancel := context.WithTimeout(ctx, 5*time.Second)

				switch op {
				case OpRead:
					// YCSB does not consume read values, so drain chunks to io.Discard
					// rather than assemble a result slice.
					opErr = c.GetStream(opCtx, k, io.Discard)
				case OpUpdate:
					val := generateValue(localRng, cfg.ValueSize)
					if useStreamingWrites {
						// Use streaming for writes
						opErr = c.PutStream(opCtx, k, bytes.NewReader(val), 0)
					} else {
						opErr = c.Put(opCtx, k, val, 0)
					}
				case OpCAS:
					// Guarded read-modify-write: read the current version (draining
					// the value), then write only if it is unchanged. Reads always
					// stream; the write uses the same transport selection as an update.
					ver, found, rerr := c.GetStreamWithVersion(opCtx, k, io.Discard)
					if rerr != nil {
						opErr = rerr
					} else {
						if !found {
							ver = 0 // absent: recreate with put-if-absent
						}
						val := generateValue(localRng, cfg.ValueSize)
						var perr error
						if useStreamingWrites {
							_, perr = c.PutStreamIfVersion(opCtx, k, bytes.NewReader(val), 0, ver)
						} else {
							_, perr = c.PutIfVersion(opCtx, k, val, 0, ver)
						}
						// Classification lives in the metrics layer; the worker only counts.
						var outcome CASOutcome
						outcome, opErr = ClassifyCASResult(perr)
						switch outcome {
						case CASWin:
							casWins++
						case CASMismatch:
							casMismatches++ // lost the race: contention, not an error
						}
					}
				}
				opCancel()
				latency := time.Since(start)

				// Check for connection errors and abort worker if connection is lost
				if opErr != nil && isConnectionError(opErr) {
					// Log critical error and exit worker (only if progress is enabled)
					if !NoProgress {
						pterm.Error.Printf("Worker %d: Connection failed: %v\n", workerID, opErr)
					}
					// Count remaining operations as errors
					remainingOps := opsPerWorker - opIdx - 1
					resultCh <- workerResult{errCount + remainingOps + 1, latencies, opCounts, casWins, casMismatches}
					return
				}

				// Report to progress tracker if enabled
				if reporter != nil {
					reporter.RecordOp(op, latency, opErr)
				}

				// Record in metrics collector
				metrics.RecordOperation(op, latency, opErr)

				// Send operation to throughput tracker
				select {
				case throughputCh <- struct {
					ops    int
					opType OpType
				}{1, op}:
				default:
					// Channel is full, skip this update to avoid blocking
				}

				// Keep local stats for final report
				if opErr != nil {
					errCount++
				}
				latencies = append(latencies, latency)
				opCounts[op]++
			}
			resultCh <- workerResult{errCount, latencies, opCounts, casWins, casMismatches}
		}(i, seed, client, progressReporter, metricsCollector, throughputCh, cfg.NoProgress)
	}
	wg.Wait()
	close(throughputCh) // Stop the throughput tracking goroutine
	throughputWg.Wait() // Wait for throughput goroutine to finish
	dur := time.Since(t0)

	totalErr := 0
	allLatencies := make([]time.Duration, 0, cfg.NumOps)
	totalOps := make([]int, OpNum)
	casWins, casMismatches := 0, 0
	for range cfg.Concurrency {
		res := <-resultCh
		totalErr += res.errors
		allLatencies = append(allLatencies, res.latencies...)
		for i := range int(OpNum) {
			totalOps[i] += res.opCounts[i]
		}
		casWins += res.casWins
		casMismatches += res.casMismatches
	}
	slices.Sort(allLatencies)
	result := Result{
		Ops: cfg.NumOps, Duration: dur, Errors: totalErr, Latencies: allLatencies,
		CASAttempts: totalOps[OpCAS], CASWins: casWins, CASMismatches: casMismatches,
	}

	// Display final results using pterm with enhanced metrics
	DisplayFinalResultsWithMetrics(cfg, result, totalOps, metricsCollector)

	return result, nil
}
