// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const listValuesTestPrefix = "list-values-shard-"

type listValuesRPCControl struct {
	fail    atomic.Bool
	block   atomic.Bool
	calls   atomic.Int64
	entered chan struct{}
}

func newListValuesRPCControl() *listValuesRPCControl {
	return &listValuesRPCControl{entered: make(chan struct{}, 1)}
}

func (c *listValuesRPCControl) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod != pb.CacheService_ListLocalWithValues_FullMethodName {
		return handler(ctx, req)
	}

	c.calls.Add(1)
	if c.block.Load() {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if c.fail.Load() {
		return nil, status.Error(codes.Unavailable, "injected ListLocalWithValues failure")
	}
	return handler(ctx, req)
}

type listValuesTestNode struct {
	id          string
	clusterAddr string
	coordinator *coordinator.Coordinator
	service     *CacheService
	server      *grpc.Server
	listener    net.Listener
	serveDone   chan struct{}
	stopServer  sync.Once
}

func newListValuesTestNode(t *testing.T, id string, seeds []string, ready bool) (*listValuesTestNode, *listValuesRPCControl) {
	t.Helper()

	clusterAddr := freeListValuesClusterAddr(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	storage := setupTestStorage(t)
	control := newListValuesRPCControl()
	routerConfig := coordinator.DefaultRouterConfig()
	routerConfig.ConnectionTimeout = 200 * time.Millisecond
	routerConfig.MaxRetries = 0
	coord, err := coordinator.New(&coordinator.Config{
		Enabled:      true,
		MyNodeID:     id,
		ClusterAddr:  clusterAddr,
		ListenAddr:   listener.Addr().String(),
		Seeds:        seeds,
		DiskPath:     t.TempDir(),
		RouterConfig: routerConfig,
		Registerer:   prometheus.NewRegistry(),
		LifecyclerConfig: ring.LifecyclerConfig{
			NumTokens:            32,
			ObservePeriod:        0,
			MinReadyDuration:     0,
			UnregisterOnShutdown: true,
			// RF=3 allows two ACTIVE nodes to serve while the unready test observer remains JOINING.
			RingConfig: ring.Config{
				HeartbeatPeriod:   100 * time.Millisecond,
				HeartbeatTimeout:  10 * time.Second,
				ReplicationFactor: 3,
			},
		},
	})
	if err != nil {
		listener.Close()
		t.Fatalf("create coordinator %s: %v", id, err)
	}

	service := NewCacheService(coord, storage)
	server := grpc.NewServer(grpc.UnaryInterceptor(control.intercept))
	pb.RegisterCacheServiceServer(server, service)
	clusterpb.RegisterClusterServiceServer(server, coord)

	ctx, cancel := context.WithCancel(context.Background())
	if err := coord.Start(ctx); err != nil {
		server.Stop()
		listener.Close()
		cancel()
		_ = coord.Stop()
		t.Fatalf("start coordinator %s: %v", id, err)
	}

	node := &listValuesTestNode{
		id:          id,
		clusterAddr: clusterAddr,
		coordinator: coord,
		service:     service,
		server:      server,
		listener:    listener,
		serveDone:   make(chan struct{}),
	}
	go func() {
		defer close(node.serveDone)
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		_ = coord.Stop()
		node.stopGRPC()
		cancel()
	})

	if ready {
		coord.MarkReady()
		readyCtx, readyCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer readyCancel()
		if err := coord.WaitReady(readyCtx); err != nil {
			t.Fatalf("wait for coordinator %s readiness: %v", id, err)
		}
	}

	return node, control
}

func (n *listValuesTestNode) stopGRPC() {
	n.stopServer.Do(func() {
		n.server.Stop()
		_ = n.listener.Close()
		<-n.serveDone
	})
}

func freeListValuesClusterAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.LocalAddr().(*net.UDPAddr).Port
	require.NoError(t, listener.Close())
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

func waitForListValuesNodes(t *testing.T, coord *coordinator.Coordinator, want ...string) {
	t.Helper()

	wantNodes := make(map[string]bool, len(want))
	for _, id := range want {
		wantNodes[id] = true
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		nodes := coord.GetRing().GetActiveNodes()
		if len(nodes) == len(wantNodes) {
			found := make(map[string]bool, len(nodes))
			for _, node := range nodes {
				found[node.ID] = true
			}
			if mapsEqual(found, wantNodes) {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	var activeIDs []string
	for _, node := range coord.GetRing().GetActiveNodes() {
		activeIDs = append(activeIDs, node.ID)
	}
	t.Fatalf("active nodes = %v, want %v", activeIDs, want)
}

func mapsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func TestCacheService_ListWithValuesRequiresEveryActiveShard(t *testing.T) {
	// The observer is deliberately not active, so its all-failure request reaches both active shards remotely.
	observer, _ := newListValuesTestNode(t, "list-values-observer", nil, false)

	response, err := observer.service.ListWithValues(context.Background(), &pb.ListRequest{
		Prefix: listValuesTestPrefix,
		Limit:  2,
	})
	if err == nil || err.Error() != "no active nodes in cluster" || response != nil {
		t.Errorf("ListWithValues no-active-node request = (%v, %v), want the existing no-active-nodes error", response, err)
	}

	nodeA, controlA := newListValuesTestNode(t, "list-values-node-a", []string{observer.clusterAddr}, true)
	nodeB, controlB := newListValuesTestNode(t, "list-values-node-b", []string{observer.clusterAddr, nodeA.clusterAddr}, true)
	waitForListValuesNodes(t, nodeA.coordinator, nodeA.id, nodeB.id)
	waitForListValuesNodes(t, observer.coordinator, nodeA.id, nodeB.id)

	localValue := []byte("local value")
	peerValue := []byte("peer value")
	require.NoError(t, nodeA.service.Operations().PutLocal(context.Background(), listValuesTestPrefix+"a", bytes.NewReader(localValue), 0))
	require.NoError(t, nodeB.service.Operations().PutLocal(context.Background(), listValuesTestPrefix+"z", bytes.NewReader(peerValue), 0))

	response, err = nodeA.service.ListWithValues(context.Background(), &pb.ListRequest{
		Prefix: listValuesTestPrefix,
		Limit:  2,
	})
	if err != nil {
		t.Errorf("ListWithValues healthy shards returned an error: %v", err)
	} else if response == nil {
		t.Error("ListWithValues healthy shards returned a nil response")
	} else {
		got := make(map[string][]byte, len(response.Entries))
		for _, entry := range response.Entries {
			if entry != nil {
				got[entry.Key] = entry.Value
			}
		}
		if len(response.Entries) != 2 || len(got) != 2 || !bytes.Equal(got[listValuesTestPrefix+"a"], localValue) || !bytes.Equal(got[listValuesTestPrefix+"z"], peerValue) || response.Entries[0].Key != listValuesTestPrefix+"a" || response.Entries[1].Key != listValuesTestPrefix+"z" || response.HasMore || response.ContinuationToken != "" {
			t.Errorf("ListWithValues healthy-shard page = %+v, want both sorted values in a terminal page", response)
		}
	}

	controlB.fail.Store(true)
	beforePeerCalls := controlB.calls.Load()
	response, err = nodeA.service.ListWithValues(context.Background(), &pb.ListRequest{
		Prefix: listValuesTestPrefix,
		Limit:  1,
	})
	if controlB.calls.Load() == beforePeerCalls {
		t.Fatalf("ListLocalWithValues RPC was not reached for node %s", nodeB.id)
	}
	if err == nil {
		t.Errorf("ListWithValues returned successful page %+v after active peer %s failed ListLocalWithValues", response, nodeB.id)
	}
	if response != nil {
		t.Errorf("ListWithValues returned response %+v with peer failure %v", response, err)
	}
	controlB.fail.Store(false)

	controlA.fail.Store(true)
	controlB.fail.Store(true)
	beforeACalls := controlA.calls.Load()
	beforePeerCalls = controlB.calls.Load()
	response, err = observer.service.ListWithValues(context.Background(), &pb.ListRequest{
		Prefix: listValuesTestPrefix,
		Limit:  2,
	})
	if controlA.calls.Load() == beforeACalls || controlB.calls.Load() == beforePeerCalls {
		t.Fatal("ListLocalWithValues RPCs were not reached on both active shards")
	}
	if err == nil {
		t.Errorf("ListWithValues returned successful page with %d entries, has_more=%t, token=%q after all active shard ListLocalWithValues RPCs failed", len(response.GetEntries()), response.GetHasMore(), response.GetContinuationToken())
	}
	if response != nil {
		t.Errorf("ListWithValues returned response %+v with all-shard failure %v", response, err)
	}
	controlA.fail.Store(false)
	controlB.fail.Store(false)

	controlB.block.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	type listResult struct {
		response *pb.ListWithValuesResponse
		err      error
	}
	resultCh := make(chan listResult, 1)
	go func() {
		response, err := nodeA.service.ListWithValues(ctx, &pb.ListRequest{
			Prefix: listValuesTestPrefix,
			Limit:  2,
		})
		resultCh <- listResult{response: response, err: err}
	}()
	select {
	case <-controlB.entered:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("ListLocalWithValues RPC did not reach the blocking peer")
	}
	select {
	case result := <-resultCh:
		if !errors.Is(result.err, context.Canceled) || result.response != nil {
			t.Errorf("ListWithValues canceled request = (%v, %v), want nil response and context.Canceled", result.response, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListWithValues did not return after caller cancellation")
	}
	controlB.block.Store(false)

	nodeA.coordinator.GetRouter().RemoveClient(nodeB.id)
	nodeB.stopGRPC()
	beforePeerCalls = controlB.calls.Load()
	response, err = nodeA.service.ListWithValues(context.Background(), &pb.ListRequest{
		Prefix: listValuesTestPrefix,
		Limit:  1,
	})
	if controlB.calls.Load() != beforePeerCalls {
		t.Fatal("ListLocalWithValues RPC unexpectedly reached the stopped peer")
	}
	if err == nil {
		t.Errorf("ListWithValues returned successful page %+v after client acquisition for active peer %s failed", response, nodeB.id)
	}
	if response != nil {
		t.Errorf("ListWithValues returned response %+v with peer client-acquisition failure %v", response, err)
	}
}
