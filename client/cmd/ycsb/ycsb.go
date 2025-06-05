package ycsb

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	cacheclient "github.com/tigrisdata/cache_service/client"
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
func preloadKeys(cfg YCSBConfig, rng *rand.Rand) {
	c, _ := cacheclient.New(cfg.Addr)
	for i := range cfg.NumKeys {
		k := hashKey(i)
		val := generateValue(rng, cfg.ValueSize)
		_ = c.Put(context.Background(), k, val, 0)
	}
	c.Close()
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
	if cfg.Concurrency < 1 {
		return Result{}, fmt.Errorf("Concurrency must be at least 1")
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	ws, err := ParseWorkload(cfg.Workload)
	if err != nil {
		return Result{}, err
	}
	// Preload keys
	preloadKeys(cfg, rng)

	var wg sync.WaitGroup
	opsPerWorker := cfg.NumOps / cfg.Concurrency
	resultCh := make(chan struct {
		errors    int
		latencies []time.Duration
		opCounts  []int
	}, cfg.Concurrency)
	t0 := time.Now()
	for range cfg.Concurrency {
		wg.Add(1)
		seed := rng.Int63() // Each goroutine gets its own seed
		go func(seed int64) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(seed))
			c, err := cacheclient.New(cfg.Addr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to connect to cache service: %v\n", err)
				return
			}
			errCount := 0
			latencies := make([]time.Duration, 0, opsPerWorker)
			opCounts := make([]int, OpNum) // Track count for each op type
			for range opsPerWorker {
				keyNum := localRng.Intn(cfg.NumKeys)
				k := hashKey(keyNum)
				op := pickOp(ws.Weights, localRng)
				start := time.Now()
				switch op {
				case OpRead:
					_, err := c.Get(context.Background(), k)
					if err != nil {
						errCount++
					}
				case OpUpdate:
					val := generateValue(localRng, cfg.ValueSize)
					err := c.Put(context.Background(), k, val, 0)
					if err != nil {
						errCount++
					}
				}
				latencies = append(latencies, time.Since(start))
				opCounts[op]++
			}
			c.Close()
			resultCh <- struct {
				errors    int
				latencies []time.Duration
				opCounts  []int
			}{errCount, latencies, opCounts}
		}(seed)
	}
	wg.Wait()
	dur := time.Since(t0)
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

	// Print latency percentiles
	fmt.Printf("Latency percentiles (ms): P50=%.2f, P95=%.2f, P99=%.2f, Max=%.2f\n",
		float64(percentile(allLatencies, 0.50))/float64(time.Millisecond),
		float64(percentile(allLatencies, 0.95))/float64(time.Millisecond),
		float64(percentile(allLatencies, 0.99))/float64(time.Millisecond),
		float64(percentile(allLatencies, 1.00))/float64(time.Millisecond),
	)

	// Compute ops/sec for each operation type
	fmt.Println("Ops/sec by operation type:")
	for i := range int(OpNum) {
		count := totalOps[i]
		fmt.Printf("  %s: %.2f\n", opNames[i], float64(count)/dur.Seconds())
	}
	// Print total operations executed
	fmt.Println("Total ops by operation type:")
	for i := range int(OpNum) {
		count := totalOps[i]
		fmt.Printf("  %s: %d\n", opNames[i], count)
	}

	return result, nil
}
