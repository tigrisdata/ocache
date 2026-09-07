// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

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
	getCalls      atomic.Int64
	responseBytes atomic.Int64
	casBumps      atomic.Int64 // successful PutObjectIfVersion applications
}

func newYCSBReadServer() *ycsbReadServer {
	return &ycsbReadServer{values: make(map[string][]byte), versions: make(map[string]uint64)}
}

func (s *ycsbReadServer) PutObject(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
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
