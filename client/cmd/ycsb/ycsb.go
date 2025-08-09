package ycsb

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pterm/pterm"
	cacheclient "github.com/tigrisdata/ocache/client"
)

type OpType int

const (
	OpRead OpType = iota
	OpUpdate
	OpNum
)

var opNames = []string{"read", "update"}

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
	Addr        string // Address of the cache service (host:port)
	NumKeys     int    // Number of unique keys to use in the benchmark
	ValueSize   int    // Size of each value in bytes
	NumOps      int    // Total number of operations to perform
	Concurrency int    // Number of concurrent workers
	Workload    string // Workload type or custom mix (e.g. "A", "B", "read=70,update=30")
	Seed        int64  // Seed for random number generation (for reproducibility)
}

type Result struct {
	Ops       int
	Duration  time.Duration
	Errors    int
	Latencies []time.Duration // All operation latencies
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

func preloadKeys(ctx context.Context, cfg YCSBConfig, rng *rand.Rand) error {
	// Use a small pool for preloading to avoid overwhelming the server
	poolSize := 4
	if cfg.Concurrency < poolSize {
		poolSize = cfg.Concurrency
	}
	pool, err := cacheclient.NewConnectionPool(cfg.Addr, poolSize)
	if err != nil {
		return fmt.Errorf("failed to create connection pool for preload: %w", err)
	}
	defer pool.Close()

	// Use pterm spinner for preloading
	spinner, _ := pterm.DefaultSpinner.
		WithText(fmt.Sprintf("Preloading %d keys...", cfg.NumKeys)).
		Start()
	
	var preloadErrors int32
	var successCount int32
	errorCh := make(chan error, 100)
	
	for i := range cfg.NumKeys {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			spinner.Warning(fmt.Sprintf("Preload cancelled after %d/%d keys", i, cfg.NumKeys))
			return ctx.Err()
		default:
		}
		
		k := hashKey(i)
		val := generateValue(rng, cfg.ValueSize)
		err := pool.Execute(ctx, func(ctx context.Context, c *cacheclient.Client) error {
			return c.Put(ctx, k, val, 0)
		})
		if err != nil {
			atomic.AddInt32(&preloadErrors, 1)
			select {
			case errorCh <- fmt.Errorf("key %s: %w", k, err):
			default: // Don't block on error channel
			}
		} else {
			atomic.AddInt32(&successCount, 1)
		}
		
		if i%100 == 0 {
			spinner.UpdateText(fmt.Sprintf("Preloading keys: %d/%d (errors: %d)", 
				i+1, cfg.NumKeys, atomic.LoadInt32(&preloadErrors)))
		}
	}
	
	close(errorCh)
	
	// Collect sample of errors
	var sampleErrors []error
	for err := range errorCh {
		if len(sampleErrors) < 5 {
			sampleErrors = append(sampleErrors, err)
		}
	}
	
	totalErrors := atomic.LoadInt32(&preloadErrors)
	if totalErrors > 0 {
		spinner.Warning(fmt.Sprintf("Preloaded %d/%d keys (%d errors)", 
			atomic.LoadInt32(&successCount), cfg.NumKeys, totalErrors))
		if int(totalErrors) > cfg.NumKeys/10 { // If more than 10% failed, consider it a failure
			if len(sampleErrors) > 0 {
				return fmt.Errorf("preload failed with %d errors, first error: %w", totalErrors, sampleErrors[0])
			}
		}
	} else {
		spinner.Success(fmt.Sprintf("Preloaded %d keys", cfg.NumKeys))
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
	if err := preloadKeys(ctx, cfg, rng); err != nil {
		return Result{}, err
	}

	// Create connection pool with size equal to concurrency
	// This ensures each worker can have its own connection without contention
	pool, err := cacheclient.NewConnectionPool(cfg.Addr, cfg.Concurrency)
	if err != nil {
		return Result{}, fmt.Errorf("failed to create connection pool: %w", err)
	}
	defer pool.Close()

	// Get all clients from the pool to distribute one per worker
	clients := pool.GetAll()

	// Create metrics collector
	metricsCollector := NewMetricsCollector()
	
	// Create pterm progress reporter
	progressReporter := NewPtermProgressReporter(cfg.NumOps)
	if err := progressReporter.Start(); err != nil {
		return Result{}, fmt.Errorf("failed to start progress reporter: %w", err)
	}
	defer progressReporter.Stop()

	// Channel for aggregate throughput tracking
	throughputCh := make(chan struct {
		ops int
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

	var wg sync.WaitGroup
	opsPerWorker := cfg.NumOps / cfg.Concurrency
	resultCh := make(chan struct {
		errors    int
		latencies []time.Duration
		opCounts  []int
	}, cfg.Concurrency)
	t0 := time.Now()
	for i := range cfg.Concurrency {
		wg.Add(1)
		seed := rng.Int63() // Each goroutine gets its own seed
		client := clients[i] // Assign dedicated client to each worker
		go func(workerID int, seed int64, c *cacheclient.Client, reporter *PtermProgressReporter, metrics *MetricsCollector, throughputCh chan<- struct{ops int; opType OpType}) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(seed))
			errCount := 0
			// Pre-allocate latencies slice with exact capacity to avoid reallocation
			latencies := make([]time.Duration, 0, opsPerWorker)
			opCounts := make([]int, OpNum) // Track count for each op type
			
			for opIdx := 0; opIdx < opsPerWorker; opIdx++ {
				// Check for context cancellation
				select {
				case <-ctx.Done():
					// Context cancelled, report partial results
					resultCh <- struct {
						errors    int
						latencies []time.Duration
						opCounts  []int
					}{errCount, latencies, opCounts}
					return
				default:
				}
				
				keyNum := localRng.Intn(cfg.NumKeys)
				k := hashKey(keyNum)
				op := pickOp(ws.Weights, localRng)
				start := time.Now()
				var opErr error
				
				// Use context with timeout for individual operations
				opCtx, opCancel := context.WithTimeout(ctx, 5*time.Second)
				switch op {
				case OpRead:
					_, opErr = c.Get(opCtx, k)
				case OpUpdate:
					val := generateValue(localRng, cfg.ValueSize)
					opErr = c.Put(opCtx, k, val, 0)
				}
				opCancel()
				latency := time.Since(start)
				
				// Check for connection errors and abort worker if connection is lost
				if opErr != nil && isConnectionError(opErr) {
					// Log critical error and exit worker
					pterm.Error.Printf("Worker %d: Connection failed: %v\n", workerID, opErr)
					// Count remaining operations as errors
					remainingOps := opsPerWorker - opIdx - 1
					resultCh <- struct {
						errors    int
						latencies []time.Duration
						opCounts  []int
					}{errCount + remainingOps + 1, latencies, opCounts}
					return
				}
				
				// Report to progress tracker
				reporter.RecordOp(op, latency, opErr)
				
				// Record in metrics collector
				metrics.RecordOperation(op, latency, opErr)
				
				// Send operation to throughput tracker
				select {
				case throughputCh <- struct{ops int; opType OpType}{1, op}:
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
			resultCh <- struct {
				errors    int
				latencies []time.Duration
				opCounts  []int
			}{errCount, latencies, opCounts}
		}(i, seed, client, progressReporter, metricsCollector, throughputCh)
	}
	wg.Wait()
	close(throughputCh) // Stop the throughput tracking goroutine
	throughputWg.Wait() // Wait for throughput goroutine to finish
	dur := time.Since(t0)
	
	// Stop progress reporter before processing results
	progressReporter.Stop()
	
	totalErr := 0
	allLatencies := make([]time.Duration, 0, cfg.NumOps)
	totalOps := make([]int, OpNum)
	for range cfg.Concurrency {
		res := <-resultCh
		totalErr += res.errors
		allLatencies = append(allLatencies, res.latencies...)
		for i := range int(OpNum) {
			totalOps[i] += res.opCounts[i]
		}
	}
	slices.Sort(allLatencies)
	result := Result{Ops: cfg.NumOps, Duration: dur, Errors: totalErr, Latencies: allLatencies}

	// Display final results using pterm with enhanced metrics
	DisplayFinalResultsWithMetrics(cfg, result, totalOps, metricsCollector)


	return result, nil
}
