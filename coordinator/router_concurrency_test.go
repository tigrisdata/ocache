// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

func TestRouter_UnrelatedRouteNotBlockedByDial(t *testing.T) {
	mockRing := newMockRing("local-node")
	healthyAddr, _, clusterServer, healthyServer, _, _ := startMockRouterServer(t, "healthy-node")
	defer clusterServer.Stop()
	defer healthyServer.Stop()
	mockRing.AddNode("local-node", "unused", "unused")
	mockRing.AddNode("healthy-node", "unused", healthyAddr)
	mockRing.AddNode("blocked-node", "unused", "blocked-node")
	mockRing.SetKeyOwner("healthy-key", "healthy-node")
	mockRing.SetKeyOwner("blocked-key", "blocked-node")

	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	var dialStartOnce sync.Once
	config := DefaultRouterConfig()
	config.MaxRetries = 0
	config.ConnectionTimeout = time.Second
	config.GRPCDialOptions = []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		if address == "blocked-node" {
			dialStartOnce.Do(func() { close(dialStarted) })
			select {
			case <-releaseDial:
				return (&net.Dialer{}).DialContext(ctx, "tcp", healthyAddr)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	})}
	router := NewRouterWithConfig(mockRing, "local-node", config)
	defer router.Close()

	_, err := router.Route("healthy-key")
	require.NoError(t, err)

	blockedDone := make(chan error, 1)
	go func() {
		_, err := router.Route("blocked-key")
		blockedDone <- err
	}()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("blocked dial did not start")
	}

	healthyDone := make(chan error, 1)
	go func() {
		_, err := router.Route("healthy-key")
		healthyDone <- err
	}()
	select {
	case err := <-healthyDone:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("healthy route waited for an unrelated blocked dial")
	}

	close(releaseDial)
	select {
	case err := <-blockedDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("blocked route did not finish after release")
	}
}

func TestRouter_CoalescesSameNodeDials(t *testing.T) {
	mockRing := newMockRing("local-node")
	cacheAddr, _, clusterServer, cacheServer, _, _ := startMockRouterServer(t, "remote-node")
	defer clusterServer.Stop()
	defer cacheServer.Stop()
	mockRing.AddNode("local-node", "unused", "unused")
	mockRing.AddNode("remote-node", "unused", cacheAddr)
	mockRing.SetKeyOwner("remote-key", "remote-node")

	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	var dialStartOnce sync.Once
	var dialCount atomic.Int32
	config := DefaultRouterConfig()
	config.MaxRetries = 0
	config.GRPCDialOptions = []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		if address == cacheAddr {
			dialCount.Add(1)
			dialStartOnce.Do(func() { close(dialStarted) })
			select {
			case <-releaseDial:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	})}
	router := NewRouterWithConfig(mockRing, "local-node", config)
	defer router.Close()

	const callers = 16
	start := make(chan struct{})
	results := make(chan error, callers)
	var workers sync.WaitGroup
	workers.Add(callers)
	for range callers {
		go func() {
			defer workers.Done()
			<-start
			_, err := router.Route("remote-key")
			results <- err
		}()
	}
	close(start)

	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("shared dial did not start")
	}
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int32(1), dialCount.Load(), "same-node callers should share one dial")
	close(releaseDial)
	workers.Wait()
	for range callers {
		require.NoError(t, <-results)
	}
}

func TestRouter_ReportsConnectingDuringReconnect(t *testing.T) {
	mockRing := newMockRing("local-node")
	cacheAddr, _, clusterServer, cacheServer, _, _ := startMockRouterServer(t, "remote-node")
	defer clusterServer.Stop()
	defer cacheServer.Stop()
	mockRing.AddNode("local-node", "unused", "unused")
	mockRing.AddNode("remote-node", "unused", cacheAddr)
	mockRing.SetKeyOwner("remote-key", "remote-node")

	secondDialStarted := make(chan struct{})
	releaseSecondDial := make(chan struct{})
	var secondDialOnce sync.Once
	var dialCount atomic.Int32
	var blockDial atomic.Bool
	config := DefaultRouterConfig()
	config.MaxRetries = 0
	config.GRPCDialOptions = []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		if address == cacheAddr {
			dialCount.Add(1)
			if blockDial.Load() {
				secondDialOnce.Do(func() { close(secondDialStarted) })
				select {
				case <-releaseSecondDial:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	})}
	router := NewRouterWithConfig(mockRing, "local-node", config)
	defer router.Close()

	_, err := router.Route("remote-key")
	require.NoError(t, err)
	require.Equal(t, int32(1), dialCount.Load())
	blockDial.Store(true)

	router.mu.RLock()
	state := router.clients["remote-node"]
	var oldConn *grpc.ClientConn
	if state != nil {
		oldConn = state.conn
	}
	router.mu.RUnlock()
	require.NotNil(t, state)
	require.NotNil(t, oldConn)
	require.NoError(t, oldConn.Close())

	reconnectDone := make(chan error, 1)
	go func() {
		_, err := router.Route("remote-key")
		reconnectDone <- err
	}()
	select {
	case <-secondDialStarted:
	case <-time.After(time.Second):
		t.Fatal("reconnect dial did not start")
	}

	stats := router.GetConnectionStats()
	require.Equal(t, connectivity.Connecting.String(), stats["remote-node"].State)

	close(releaseSecondDial)
	select {
	case err := <-reconnectDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reconnect did not finish after release")
	}
}

func TestRouter_RemoveClientCancelsDialAndInvalidatesResult(t *testing.T) {
	mockRing := newMockRing("local-node")
	mockRing.AddNode("local-node", "unused", "unused")
	mockRing.AddNode("remote-node", "unused", "blocked-node")
	mockRing.SetKeyOwner("remote-key", "remote-node")

	dialStarted := make(chan struct{})
	var dialStartOnce sync.Once
	config := DefaultRouterConfig()
	config.MaxRetries = 0
	config.GRPCDialOptions = []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		if address == "blocked-node" {
			dialStartOnce.Do(func() { close(dialStarted) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	})}
	router := NewRouterWithConfig(mockRing, "local-node", config)
	defer router.Close()

	routeDone := make(chan error, 1)
	go func() {
		_, err := router.Route("remote-key")
		routeDone <- err
	}()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}

	router.RemoveClient("remote-node")
	select {
	case err := <-routeDone:
		require.ErrorIs(t, err, ErrMaxRetriesExceeded)
	case <-time.After(time.Second):
		t.Fatal("removing a node did not release dial waiters")
	}
	require.NotContains(t, router.GetConnectionStats(), "remote-node")
}

func TestRouter_RefreshConnectionsCancelsInactiveDial(t *testing.T) {
	mockRing := newMockRing("local-node")
	mockRing.AddNode("local-node", "unused", "unused")
	mockRing.AddNode("remote-node", "unused", "blocked-node")
	mockRing.SetKeyOwner("remote-key", "remote-node")

	dialStarted := make(chan struct{})
	var dialStartOnce sync.Once
	config := DefaultRouterConfig()
	config.MaxRetries = 0
	config.GRPCDialOptions = []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		if address == "blocked-node" {
			dialStartOnce.Do(func() { close(dialStarted) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	})}
	router := NewRouterWithConfig(mockRing, "local-node", config)
	defer router.Close()

	routeDone := make(chan error, 1)
	go func() {
		_, err := router.Route("remote-key")
		routeDone <- err
	}()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}

	mockRing.RemoveNode("remote-node")
	router.RefreshConnections()
	select {
	case err := <-routeDone:
		require.ErrorIs(t, err, ErrMaxRetriesExceeded)
	case <-time.After(time.Second):
		t.Fatal("refresh did not release inactive dial waiters")
	}
	require.NotContains(t, router.GetConnectionStats(), "remote-node")
}

func TestRouter_CloseCancelsDial(t *testing.T) {
	mockRing := newMockRing("local-node")
	mockRing.AddNode("local-node", "unused", "unused")
	mockRing.AddNode("remote-node", "unused", "blocked-node")
	mockRing.SetKeyOwner("remote-key", "remote-node")

	dialStarted := make(chan struct{})
	var dialStartOnce sync.Once
	config := DefaultRouterConfig()
	config.MaxRetries = 0
	config.GRPCDialOptions = []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
		if address == "blocked-node" {
			dialStartOnce.Do(func() { close(dialStarted) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	})}
	router := NewRouterWithConfig(mockRing, "local-node", config)

	routeDone := make(chan error, 1)
	go func() {
		_, err := router.Route("remote-key")
		routeDone <- err
	}()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- router.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("router close waited for the dial timeout")
	}
	select {
	case err := <-routeDone:
		require.ErrorIs(t, err, ErrMaxRetriesExceeded)
	case <-time.After(time.Second):
		t.Fatal("close did not release dial waiters")
	}
	require.Empty(t, router.GetConnectionStats())
}
