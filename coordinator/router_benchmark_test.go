// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
)

type benchmarkDialGate struct {
	mu      sync.Mutex
	started chan<- struct{}
	release <-chan struct{}
	target  string
}

func (g *benchmarkDialGate) set(started chan<- struct{}, release <-chan struct{}, target string) {
	g.mu.Lock()
	g.started = started
	g.release = release
	g.target = target
	g.mu.Unlock()
}

func (g *benchmarkDialGate) dial(ctx context.Context, address string) (net.Conn, error) {
	if address == "blocked-node" {
		g.mu.Lock()
		started := g.started
		release := g.release
		target := g.target
		g.mu.Unlock()
		started <- struct{}{}
		select {
		case <-release:
			return (&net.Dialer{}).DialContext(ctx, "tcp", target)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return (&net.Dialer{}).DialContext(ctx, "tcp", address)
}

// BenchmarkRouterHealthyRouteDuringPeerDial measures a warmed route to one
// owner while another owner's first connection attempt is blocked. The blocked
// dial is a fixed 5ms control; it is released by a timer so the base
// implementation completes rather than hanging the benchmark. Setup, dial
// release, and the blocked route are outside the timed healthy lookup. The
// comparison command uses -test.benchtime=1x so each sample measures one
// controlled contention episode.
func BenchmarkRouterHealthyRouteDuringPeerDial(b *testing.B) {
	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(previousLogLevel) })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterCacheServiceServer(server, &mockRouterCacheService{})
	go func() {
		if err := server.Serve(listener); err != nil {
			b.Logf("benchmark server stopped: %v", err)
		}
	}()
	b.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	mockRing := newMockRing("local-node")
	mockRing.AddNode("local-node", "unused", "unused")
	mockRing.AddNode("healthy-node", "unused", listener.Addr().String())
	mockRing.AddNode("blocked-node", "unused", "blocked-node")
	mockRing.SetKeyOwner("healthy-key", "healthy-node")
	mockRing.SetKeyOwner("blocked-key", "blocked-node")

	gate := &benchmarkDialGate{}
	config := DefaultRouterConfig()
	config.MaxRetries = 0
	config.ConnectionTimeout = time.Second
	config.CircuitBreakerThreshold = 1 << 30
	config.GRPCDialOptions = []grpc.DialOption{grpc.WithContextDialer(gate.dial)}
	router := NewRouterWithConfig(mockRing, "local-node", config)
	b.Cleanup(func() { _ = router.Close() })

	if _, err := router.Route("healthy-key"); err != nil {
		b.Fatalf("warm healthy route: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		dialStarted := make(chan struct{}, 1)
		releaseDial := make(chan struct{})
		gate.set(dialStarted, releaseDial, listener.Addr().String())

		blockedDone := make(chan error, 1)
		go func() {
			_, err := router.Route("blocked-key")
			blockedDone <- err
		}()
		select {
		case <-dialStarted:
		case <-time.After(time.Second):
			b.Fatal("blocked dial did not start")
		}

		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseDial) }) }
		releaseTimer := time.AfterFunc(5*time.Millisecond, release)
		b.StartTimer()
		if _, err := router.Route("healthy-key"); err != nil {
			b.StopTimer()
			if releaseTimer.Stop() {
				release()
			}
			b.Fatalf("healthy route: %v", err)
		}
		b.StopTimer()
		if releaseTimer.Stop() {
			release()
		}
		if err := <-blockedDone; err != nil {
			b.Fatalf("blocked route: %v", err)
		}
		router.RemoveClient("blocked-node")
	}
}
