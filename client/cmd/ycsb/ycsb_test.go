// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pterm/pterm"
	cacheclient "github.com/tigrisdata/ocache/client"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ycsbReadWorkers    = 4
	ycsbReadKeys       = 1
	ycsbReadOperations = 64
)

type ycsbReadServer struct {
	pb.UnimplementedCacheServiceServer

	mu            sync.RWMutex
	values        map[string][]byte
	versions      map[string]uint64 // CAS version per key; 0 = absent
	putDelay      time.Duration     // simulated per-put latency, honouring ctx
	getCalls      atomic.Int64
	responseBytes atomic.Int64
	casBumps      atomic.Int64 // successful PutObjectIfVersion applications
}

func newYCSBReadServer() *ycsbReadServer {
	return &ycsbReadServer{values: make(map[string][]byte), versions: make(map[string]uint64)}
}

func (s *ycsbReadServer) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.putDelay > 0 {
		timer := time.NewTimer(s.putDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	data := append([]byte(nil), req.Data...)

	s.mu.Lock()
	s.values[req.Key] = data
	s.mu.Unlock()

	return &pb.PutResponse{Success: true}, nil
}

func (s *ycsbReadServer) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	s.mu.RLock()
	data, ok := s.values[req.Key]
	s.mu.RUnlock()
	if !ok {
		return status.Error(codes.NotFound, "key not found")
	}

	for len(data) > 0 {
		chunkSize := min(len(data), cacheclient.DefaultBufferSize)
		chunk := data[:chunkSize]
		if err := stream.Send(&pb.GetResponse{Data: chunk}); err != nil {
			return err
		}
		s.responseBytes.Add(int64(len(chunk)))
		data = data[chunkSize:]
	}
	s.getCalls.Add(1)

	return nil
}

// GetStreamWithVersion serves the versioned read the CAS workload uses: version
// and found in the first message, then the value bytes.
func (s *ycsbReadServer) GetStreamWithVersion(req *pb.GetRequest, stream pb.CacheService_GetStreamWithVersionServer) error {
	s.mu.RLock()
	data, ok := s.values[req.Key]
	ver := s.versions[req.Key]
	s.mu.RUnlock()
	if !ok {
		return stream.Send(&pb.GetWithVersionResponse{Found: false})
	}
	if err := stream.Send(&pb.GetWithVersionResponse{Version: ver, Found: true}); err != nil {
		return err
	}
	for len(data) > 0 {
		chunkSize := min(len(data), cacheclient.DefaultBufferSize)
		if err := stream.Send(&pb.GetWithVersionResponse{Data: data[:chunkSize]}); err != nil {
			return err
		}
		data = data[chunkSize:]
	}
	return nil
}

// PutObjectIfVersion applies the write only when expected_version matches the
// key's current version (0 = absent), mirroring storage's contract: a lost race
// is reported in-band with the current version, never as a transport error.
func (s *ycsbReadServer) PutObjectIfVersion(_ context.Context, req *pb.PutIfVersionRequest) (*pb.PutIfVersionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.versions[req.Key]
	if cur != req.ExpectedVersion {
		return &pb.PutIfVersionResponse{Success: false, CurrentVersion: cur}, nil
	}
	s.values[req.Key] = append([]byte(nil), req.Data...)
	s.versions[req.Key] = cur + 1
	s.casBumps.Add(1)
	return &pb.PutIfVersionResponse{Success: true, NewVersion: cur + 1}, nil
}

func startYCSBReadServer(tb testing.TB) (*ycsbReadServer, string) {
	tb.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	cacheServer := newYCSBReadServer()
	pb.RegisterCacheServiceServer(grpcServer, cacheServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	tb.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	return cacheServer, listener.Addr().String()
}

// preloadTestServer wraps ycsbReadServer to inject per-key put failures and
// observe put concurrency for the preload tests.
type preloadTestServer struct {
	*ycsbReadServer

	putFailures       map[string]error
	putCalls          atomic.Int64
	activePuts        atomic.Int64
	maxPutConcurrency atomic.Int64
}

func (s *preloadTestServer) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.putCalls.Add(1)
	active := s.activePuts.Add(1)
	defer s.activePuts.Add(-1)
	for {
		maxActive := s.maxPutConcurrency.Load()
		if active <= maxActive || s.maxPutConcurrency.CompareAndSwap(maxActive, active) {
			break
		}
	}

	if err, ok := s.putFailures[req.Key]; ok {
		return nil, err
	}
	return s.ycsbReadServer.PutObject(ctx, req)
}

func startPreloadTestServer(tb testing.TB, putDelay time.Duration, putFailures map[string]error) (*preloadTestServer, string) {
	tb.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	cacheServer := &preloadTestServer{
		ycsbReadServer: newYCSBReadServer(),
		putFailures:    putFailures,
	}
	cacheServer.ycsbReadServer.putDelay = putDelay
	pb.RegisterCacheServiceServer(grpcServer, cacheServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	tb.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	return cacheServer, listener.Addr().String()
}

// plainWorkload is a read-only spec: the preload writes the plain namespace only.
func plainWorkload() WorkloadSpec {
	return WorkloadSpec{Weights: [OpNum]float64{OpRead: 1}}
}

func readOnlyYCSBConfig(addr string, valueSize int) YCSBConfig {
	return YCSBConfig{
		Addr:               addr,
		ConnMode:           string(cacheclient.ModeSimple),
		ConnectionPoolSize: ycsbReadWorkers,
		NumKeys:            ycsbReadKeys,
		ValueSize:          valueSize,
		NumOps:             ycsbReadOperations,
		Concurrency:        ycsbReadWorkers,
		Workload:           "C",
		Seed:               1,
		NoProgress:         true,
		ForceStreaming:     false,
	}
}

func disablePtermOutput(tb testing.TB) {
	tb.Helper()

	output := pterm.Output
	pterm.Output = false
	tb.Cleanup(func() {
		pterm.Output = output
	})
}

func BenchmarkPreloadYCSBKeys(b *testing.B) {
	disablePtermOutput(b)
	cacheServer, addr := startYCSBReadServer(b)
	cacheServer.putDelay = time.Millisecond
	cfg := YCSBConfig{
		Addr:               addr,
		ConnMode:           string(cacheclient.ModeSimple),
		ConnectionPoolSize: 8,
		NumKeys:            64,
		ValueSize:          100,
		NumOps:             64,
		Concurrency:        8,
		Workload:           "C",
		Seed:               1,
		NoProgress:         true,
	}

	for b.Loop() {
		result, err := RunYCSBWithContext(context.Background(), cfg)
		if err != nil {
			b.Fatal(err)
		}
		if result.Errors != 0 {
			b.Fatalf("RunYCSBWithContext reported %d errors", result.Errors)
		}
	}
}

// TestPreloadKeysPreservesValuesAndRNG: the parallel preload writes the same
// key/value mapping as the serial one, leaves the RNG in the same state, and
// actually runs puts concurrently, bounded by the configured concurrency.
func TestPreloadKeysPreservesValuesAndRNG(t *testing.T) {
	disablePtermOutput(t)
	const (
		seed        = int64(42)
		numKeys     = 16
		valueSize   = 32
		concurrency = 4
	)
	cacheServer, addr := startPreloadTestServer(t, 5*time.Millisecond, nil)
	cfg := YCSBConfig{
		Addr:               addr,
		ConnMode:           string(cacheclient.ModeSimple),
		ConnectionPoolSize: concurrency,
		NumKeys:            numKeys,
		ValueSize:          valueSize,
		Concurrency:        concurrency,
		NoProgress:         true,
	}

	wantRNG := rand.New(rand.NewSource(seed))
	wantValues := make(map[string][]byte, numKeys)
	for i := range numKeys {
		wantValues[hashKey(i)] = generateValue(wantRNG, valueSize)
	}
	wantNextSeed := wantRNG.Int63()

	gotRNG := rand.New(rand.NewSource(seed))
	if err := preloadKeys(context.Background(), cfg, plainWorkload(), gotRNG); err != nil {
		t.Fatal(err)
	}

	if got := cacheServer.maxPutConcurrency.Load(); got < 2 {
		t.Fatalf("maximum concurrent puts = %d, want at least 2", got)
	}
	if got := cacheServer.maxPutConcurrency.Load(); got > concurrency {
		t.Fatalf("maximum concurrent puts = %d, want at most %d", got, concurrency)
	}
	cacheServer.mu.RLock()
	defer cacheServer.mu.RUnlock()
	if got, want := len(cacheServer.values), numKeys; got != want {
		t.Fatalf("preloaded values = %d, want %d", got, want)
	}
	for key, want := range wantValues {
		if got := cacheServer.values[key]; !bytes.Equal(got, want) {
			t.Errorf("value for %q changed", key)
		}
	}
	if got := gotRNG.Int63(); got != wantNextSeed {
		t.Errorf("RNG state after preload = %d, want %d", got, wantNextSeed)
	}
}

// TestPreloadKeysPreservesErrorThreshold: the 10% failure threshold and the
// "first error" message are unchanged by parallel completion order.
func TestPreloadKeysPreservesErrorThreshold(t *testing.T) {
	disablePtermOutput(t)
	for _, tc := range []struct {
		name        string
		failedKeys  []int
		wantFailure bool
	}{
		{name: "at threshold", failedKeys: []int{0}, wantFailure: false},
		{name: "over threshold", failedKeys: []int{0, 1}, wantFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failures := make(map[string]error, len(tc.failedKeys))
			for _, keyNum := range tc.failedKeys {
				failures[hashKey(keyNum)] = errors.New("injected preload failure")
			}
			cacheServer, addr := startPreloadTestServer(t, 0, failures)
			cfg := YCSBConfig{
				Addr:               addr,
				ConnMode:           string(cacheclient.ModeSimple),
				ConnectionPoolSize: 4,
				NumKeys:            10,
				ValueSize:          8,
				Concurrency:        4,
				NoProgress:         true,
			}

			err := preloadKeys(context.Background(), cfg, plainWorkload(), rand.New(rand.NewSource(1)))
			if tc.wantFailure {
				if err == nil {
					t.Fatal("preload succeeded, want threshold error")
				}
				if !strings.Contains(err.Error(), "key "+hashKey(tc.failedKeys[0])) {
					t.Errorf("error = %v, want first failed key %q", err, hashKey(tc.failedKeys[0]))
				}
			} else if err != nil {
				t.Fatalf("preload failed at threshold: %v", err)
			}
			if got := cacheServer.putCalls.Load(); got != int64(cfg.NumKeys) {
				t.Errorf("put calls = %d, want %d", got, cfg.NumKeys)
			}
		})
	}
}

// TestPreloadKeysWaitsForWorkersOnCancellation: cancelling the preload returns
// context.Canceled only after every in-flight put has finished, and no put is
// started afterwards.
func TestPreloadKeysWaitsForWorkersOnCancellation(t *testing.T) {
	disablePtermOutput(t)
	cacheServer, addr := startPreloadTestServer(t, 100*time.Millisecond, nil)
	cfg := YCSBConfig{
		Addr:               addr,
		ConnMode:           string(cacheclient.ModeSimple),
		ConnectionPoolSize: 4,
		NumKeys:            100,
		ValueSize:          8,
		Concurrency:        4,
		NoProgress:         true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- preloadKeys(ctx, cfg, plainWorkload(), rand.New(rand.NewSource(1)))
	}()

	deadline := time.Now().Add(time.Second)
	for cacheServer.activePuts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if cacheServer.activePuts.Load() == 0 {
		t.Fatal("preload did not start a put")
	}
	cancel()

	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("preload error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("preload did not return after cancellation")
	}
	if got := cacheServer.activePuts.Load(); got != 0 {
		t.Fatalf("active puts after preload returned = %d, want 0", got)
	}
	calls := cacheServer.putCalls.Load()
	time.Sleep(20 * time.Millisecond)
	if got := cacheServer.putCalls.Load(); got != calls {
		t.Fatalf("put calls continued after preload returned: started at %d, now %d", calls, got)
	}
}

func TestRunYCSBReadOnlyDrainsResponses(t *testing.T) {
	disablePtermOutput(t)
	cacheServer, addr := startYCSBReadServer(t)
	cfg := readOnlyYCSBConfig(addr, 2*cacheclient.DefaultBufferSize+1)

	result, err := RunYCSBWithContext(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Errors != 0 {
		t.Fatalf("RunYCSBWithContext reported %d errors", result.Errors)
	}

	if got, want := cacheServer.getCalls.Load(), int64(cfg.NumOps); got != want {
		t.Errorf("Get calls = %d, want %d", got, want)
	}
	if got, want := cacheServer.responseBytes.Load(), int64(cfg.NumOps*cfg.ValueSize); got != want {
		t.Errorf("response bytes = %d, want %d", got, want)
	}
}

func BenchmarkRunYCSBReadOnly(b *testing.B) {
	disablePtermOutput(b)

	for _, tc := range []struct {
		name      string
		valueSize int
	}{
		{name: "64KiB", valueSize: 64 * 1024},
		{name: "256KiB", valueSize: 256 * 1024},
		{name: "1MiB", valueSize: 1024 * 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			_, addr := startYCSBReadServer(b)
			cfg := readOnlyYCSBConfig(addr, tc.valueSize)

			b.ReportAllocs()
			b.SetBytes(int64(cfg.NumOps * cfg.ValueSize))
			for b.Loop() {
				result, err := RunYCSBWithContext(context.Background(), cfg)
				if err != nil {
					b.Fatal(err)
				}
				if result.Errors != 0 {
					b.Fatalf("RunYCSBWithContext reported %d errors", result.Errors)
				}
			}
		})
	}
}

func casYCSBConfig(addr string, workers, keys, ops int) YCSBConfig {
	return YCSBConfig{
		Addr:               addr,
		ConnMode:           string(cacheclient.ModeSimple),
		ConnectionPoolSize: workers,
		NumKeys:            keys,
		ValueSize:          64,
		NumOps:             ops,
		Concurrency:        workers,
		Workload:           "cas=100",
		Seed:               1,
		NoProgress:         true,
	}
}

// TestRunYCSBCASAccountsWinsAndMismatches runs a fully contended guarded
// read-modify-write workload (4 workers, 1 key) and checks the accounting
// invariants that make the CAS numbers trustworthy: every attempt is either a
// win or a mismatch, a lost race is never reported as an error, and the server
// applied exactly one version bump per reported win (plus one per preloaded key).
func TestRunYCSBCASAccountsWinsAndMismatches(t *testing.T) {
	disablePtermOutput(t)
	cacheServer, addr := startYCSBReadServer(t)
	cfg := casYCSBConfig(addr, 4, 1, 64)

	result, err := RunYCSBWithContext(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Errors != 0 {
		t.Fatalf("RunYCSBWithContext reported %d errors; a lost CAS race must not be an error", result.Errors)
	}
	if result.CASAttempts != cfg.NumOps {
		t.Errorf("CAS attempts = %d, want %d", result.CASAttempts, cfg.NumOps)
	}
	if got := result.CASWins + result.CASMismatches; got != result.CASAttempts {
		t.Errorf("wins+mismatches = %d, want attempts %d", got, result.CASAttempts)
	}
	if got, want := cacheServer.casBumps.Load(), int64(cfg.NumKeys+result.CASWins); got != want {
		t.Errorf("server version bumps = %d, want preload %d + wins %d = %d", got, cfg.NumKeys, result.CASWins, want)
	}
}

// TestRunYCSBCASSingleWorkerNeverMismatches: with one worker there is no race,
// so every attempt must win and the mismatch count must be exactly zero.
func TestRunYCSBCASSingleWorkerNeverMismatches(t *testing.T) {
	disablePtermOutput(t)
	cacheServer, addr := startYCSBReadServer(t)
	cfg := casYCSBConfig(addr, 1, 3, 30)

	result, err := RunYCSBWithContext(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Errors != 0 || result.CASMismatches != 0 {
		t.Fatalf("errors=%d mismatches=%d, want 0/0 with a single worker", result.Errors, result.CASMismatches)
	}
	if result.CASWins != cfg.NumOps {
		t.Errorf("wins = %d, want %d", result.CASWins, cfg.NumOps)
	}
	if got, want := cacheServer.casBumps.Load(), int64(cfg.NumKeys+cfg.NumOps); got != want {
		t.Errorf("server version bumps = %d, want %d", got, want)
	}
}
