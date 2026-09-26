// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// blockingDialer never completes a connection to blockAddr: it holds the dial
// until its context ends, the shape of a peer that is mid-restart and
// blackholing SYNs. Every other address connects normally.
func blockingDialer(blockAddr string) grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		if addr == blockAddr {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	})
}

func dialTestConfig(connTimeout time.Duration, opts ...grpc.DialOption) *RouterConfig {
	cfg := DefaultRouterConfig()
	cfg.ConnectionTimeout = connTimeout
	cfg.MaxRetries = 0
	cfg.GRPCDialOptions = opts
	return cfg
}

const stuckAddr = "stuck.invalid:1"

// TestRouter_SlowDialDoesNotBlockOtherNodes pins the head-of-line fix: while a
// dial to one unreachable node is in flight, routing to a healthy node must
// complete immediately instead of waiting out that dial's timeout behind the
// router-wide lock.
func TestRouter_SlowDialDoesNotBlockOtherNodes(t *testing.T) {
	mockRing := newMockRing("local-node")
	cacheAddr, clusterAddr, clusterSrv, cacheSrv, _, _ := startMockRouterServer(t, "healthy-node")
	defer clusterSrv.Stop()
	defer cacheSrv.Stop()
	mockRing.AddNode("healthy-node", clusterAddr, cacheAddr)
	mockRing.AddNode("stuck-node", "localhost:1", stuckAddr)
	mockRing.SetKeyOwner("healthy-key", "healthy-node")
	mockRing.SetKeyOwner("stuck-key", "stuck-node")

	const connTimeout = 3 * time.Second
	router := NewRouterWithConfig(mockRing, "local-node", dialTestConfig(connTimeout, blockingDialer(stuckAddr)))
	defer router.Close()

	stuckDone := make(chan error, 1)
	go func() {
		_, err := router.Route("stuck-key")
		stuckDone <- err
	}()
	// The stuck node's state is registered before its dial starts, so its
	// appearance in the stats means the dial is now in flight.
	require.Eventually(t, func() bool {
		_, ok := router.GetConnectionStats()["stuck-node"]
		return ok
	}, time.Second, 5*time.Millisecond, "stuck dial never started")

	start := time.Now()
	client, err := router.Route("healthy-key")
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.Less(t, time.Since(start), connTimeout/2,
		"routing to a healthy node waited on another node's dial")

	select {
	case err := <-stuckDone:
		assert.Error(t, err)
	case <-time.After(2 * connTimeout):
		t.Fatal("stuck dial never timed out")
	}
}

// TestRouter_CancelledContextAbortsDial: a caller that gives up mid-dial gets
// its own context error back promptly, and the abandoned dial is not held
// against the node's circuit breaker.
func TestRouter_CancelledContextAbortsDial(t *testing.T) {
	mockRing := newMockRing("local-node")
	mockRing.AddNode("stuck-node", "localhost:1", stuckAddr)
	mockRing.SetKeyOwner("stuck-key", "stuck-node")

	cfg := dialTestConfig(5*time.Second, blockingDialer(stuckAddr))
	cfg.CircuitBreakerThreshold = 1
	router := NewRouterWithConfig(mockRing, "local-node", cfg)
	defer router.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := router.RouteContext(ctx, "stuck-key")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second, "dial outlived the caller's context")

	stats := router.GetConnectionStats()["stuck-node"]
	assert.Equal(t, int32(0), stats.FailureCount, "an abandoned dial is not a node failure")
	assert.False(t, stats.CircuitOpen)
}

// TestRouter_CancelledContextStopsRetryBackoff: the retry sleep between
// attempts ends when the caller's context does.
func TestRouter_CancelledContextStopsRetryBackoff(t *testing.T) {
	mockRing := newMockRing("local-node")
	mockRing.AddNode("dead-node", "localhost:1", "localhost:1") // nothing listens here
	mockRing.SetKeyOwner("dead-key", "dead-node")

	cfg := dialTestConfig(100 * time.Millisecond)
	cfg.MaxRetries = 3
	cfg.InitialRetryBackoff = 10 * time.Second
	cfg.MaxRetryBackoff = 10 * time.Second
	router := NewRouterWithConfig(mockRing, "local-node", cfg)
	defer router.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := router.RouteContext(ctx, "dead-key")
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), 2*time.Second, "retry backoff outlived the caller's context")
}
