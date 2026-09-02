// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"context"
	"net"
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
	putDelay      time.Duration
	getCalls      atomic.Int64
	responseBytes atomic.Int64
}

func newYCSBReadServer() *ycsbReadServer {
	return &ycsbReadServer{values: make(map[string][]byte)}
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
