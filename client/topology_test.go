// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestTopologyRefreshLoop_PeriodicUpdate verifies automatic refresh
func TestTopologyRefreshLoop_PeriodicUpdate(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Initial topology
	topology1 := setupSimpleTopology([]string{server.address})
	server.cacheService.SetClusterTopology(topology1)

	// Create client with short refresh interval
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{server.address},
		Mode:            ModeCluster,
		RefreshInterval: 100 * time.Millisecond, // Short interval for testing
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Initial epoch should be 1
	assert.Equal(t, uint64(1), client.GetTopologyEpoch())

	// Update topology with higher epoch
	topology2 := setupSimpleTopology([]string{server.address})
	topology2.Epoch = 2
	server.cacheService.SetClusterTopology(topology2)

	// Wait for refresh with eventual consistency check
	assert.Eventually(t, func() bool {
		return client.GetTopologyEpoch() == uint64(2)
	}, 500*time.Millisecond, 50*time.Millisecond, "Epoch should be updated to 2")

	// Update again
	topology3 := setupSimpleTopology([]string{server.address})
	topology3.Epoch = 3
	server.cacheService.SetClusterTopology(topology3)

	// Wait for another refresh with eventual consistency check
	assert.Eventually(t, func() bool {
		return client.GetTopologyEpoch() == uint64(3)
	}, 500*time.Millisecond, 50*time.Millisecond, "Epoch should be updated to 3")

	// Verify multiple calls to GetTopology
	assert.Greater(t, server.cacheService.getTopologyCallCount.Load(), int32(2))
}

// TestUpdateTopology_RingUpdate verifies ring updates correctly
func TestUpdateTopology_RingUpdate(t *testing.T) {
	// Create two servers
	server1, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server1.Stop()

	server2, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server2.Stop()

	// Initial topology with one server
	topology1 := setupSimpleTopology([]string{server1.address})
	server1.cacheService.SetClusterTopology(topology1)

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server1.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Initial state
	assert.Len(t, client.GetConnectedNodes(), 1)
	assert.Equal(t, uint64(1), client.GetTopologyEpoch())

	// Update topology to include both servers
	topology2 := setupSimpleTopology([]string{server1.address, server2.address})
	topology2.Epoch = 2
	server1.cacheService.SetClusterTopology(topology2)

	// Manually trigger topology update
	if cc, ok := client.CacheClient.(*ClusterClient); ok {
		newTopology, err := cc.FetchTopology()
		require.NoError(t, err)
		err = cc.UpdateTopology(newTopology)
		require.NoError(t, err)
	} else {
		t.Skip("Test requires ClusterClient")
	}

	// Verify update
	assert.Len(t, client.GetConnectedNodes(), 2)
	assert.Equal(t, uint64(2), client.GetTopologyEpoch())
	assert.True(t, client.HasRing())
}

// TestUpdateTopology_PoolManagement verifies pools added/removed
func TestUpdateTopology_PoolManagement(t *testing.T) {
	// Create three servers
	servers := make([]*testServer, 3)
	addresses := make([]string, 3)
	for i := 0; i < 3; i++ {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()
		servers[i] = server
		addresses[i] = server.address
	}

	// Initial topology with two servers
	topology1 := setupSimpleTopology(addresses[:2])
	servers[0].cacheService.SetClusterTopology(topology1)

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{servers[0].address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Initial state - 2 connections
	if cc, ok := client.CacheClient.(*ClusterClient); ok {
		assert.Equal(t, 2, cc.GetConnectionCount())
	} else {
		t.Skip("Test requires ClusterClient")
	}
	assert.Len(t, client.GetConnectedNodes(), 2)

	// Add third server
	topology2 := setupSimpleTopology(addresses)
	topology2.Epoch = 2
	servers[0].cacheService.SetClusterTopology(topology2)

	// Update topology
	if cc, ok := client.CacheClient.(*ClusterClient); ok {
		newTopology, err := cc.FetchTopology()
		require.NoError(t, err)
		err = cc.UpdateTopology(newTopology)
		require.NoError(t, err)
		// Should have 3 connections now
		assert.Equal(t, 3, cc.GetConnectionCount())
	}
	assert.Len(t, client.GetConnectedNodes(), 3)

	// Remove second server (mark as inactive)
	topology3 := setupSimpleTopology(addresses)
	topology3.Epoch = 3
	topology3.Nodes[1].Status = clusterpb.NodeStatus_NODE_STATUS_DOWN
	servers[0].cacheService.SetClusterTopology(topology3)

	// Update topology
	if cc, ok := client.CacheClient.(*ClusterClient); ok {
		newTopology, err := cc.FetchTopology()
		require.NoError(t, err)
		err = cc.UpdateTopology(newTopology)
		require.NoError(t, err)
	}

	// Should have 2 active pools (server 0 and 2)
	connectedNodes := client.GetConnectedNodes()
	assert.Len(t, connectedNodes, 2)
	assert.Contains(t, connectedNodes, addresses[0])
	assert.Contains(t, connectedNodes, addresses[2])
}

// TestTopology_RetiresRemovedPoolWhenNewMemberDialFails ensures a partial
// refresh does not keep pools for members removed by the attempted topology.
func TestTopology_RetiresRemovedPoolWhenNewMemberDialFails(t *testing.T) {
	servers := make([]*testServer, 2)
	addresses := make([]string, 2)
	for i := range servers {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()
		servers[i] = server
		addresses[i] = server.address
	}

	initial := setupSimpleTopology(addresses)
	servers[0].cacheService.SetClusterTopology(initial)
	badAddr := "127.0.0.1:1"

	client, err := NewClusterClient(&ClientConfig{
		Addrs:              []string{addresses[0]},
		ConnectionPoolSize: 1,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(50 * time.Millisecond),
		},
	})
	require.NoError(t, err)
	defer client.Close()
	require.ElementsMatch(t, addresses, client.GetConnectedNodes())

	updated := setupSimpleTopology([]string{addresses[0], badAddr})
	updated.Epoch = 2
	require.NoError(t, client.UpdateTopology(updated))

	assert.Equal(t, []string{addresses[0]}, client.GetConnectedNodes())
	assert.Equal(t, uint64(2), client.GetTopologyEpoch())
}

// TestTopology_RetriesFailedPoolAtSameEpoch ensures a transient pool dial
// failure is retried even when the topology epoch does not change.
func TestTopology_RetriesFailedPoolAtSameEpoch(t *testing.T) {
	servers := make([]*testServer, 2)
	addresses := make([]string, 2)
	for i := range servers {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()
		servers[i] = server
		addresses[i] = server.address
	}

	initial := setupSimpleTopology([]string{addresses[0]})
	servers[0].cacheService.SetClusterTopology(initial)
	var attempts atomic.Int32
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		if addr == addresses[1] && attempts.Add(1) == 1 {
			return nil, fmt.Errorf("transient dial failure")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}

	client, err := NewClusterClient(&ClientConfig{
		Addrs:              []string{addresses[0]},
		ConnectionPoolSize: 1,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
			grpc.WithBlock(),
			grpc.FailOnNonTempDialError(true),
			grpc.WithTimeout(100 * time.Millisecond),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	updated := setupSimpleTopology(addresses)
	updated.Epoch = 2
	require.NoError(t, client.UpdateTopology(updated))
	require.Equal(t, int32(1), attempts.Load())
	assert.Equal(t, []string{addresses[0]}, client.GetConnectedNodes())

	require.NoError(t, client.UpdateTopology(updated))
	assert.Equal(t, int32(2), attempts.Load())
	assert.ElementsMatch(t, addresses, client.GetConnectedNodes())
	assert.Equal(t, uint64(2), client.GetTopologyEpoch())
}

// TestTopology_ConcurrentReads tests concurrent read operations during topology changes
func TestTopology_ConcurrentReads(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Initial topology
	topology := setupSimpleTopology([]string{server.address})
	server.cacheService.SetClusterTopology(topology)

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{server.address},
		Mode:            ModeCluster,
		RefreshInterval: 50 * time.Millisecond,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Prepare test data
	testKey := "concurrent-read-test"
	server.cacheService.data[testKey] = []byte("test-value")

	ctx := context.Background()
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	errors := make(chan error, 100)

	// Concurrent topology updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		epoch := uint64(2)
		for i := 0; i < 10; i++ {
			select {
			case <-stopCh:
				return
			default:
				newTopology := setupSimpleTopology([]string{server.address})
				newTopology.Epoch = epoch
				server.cacheService.SetClusterTopology(newTopology)
				epoch++
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	// Concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				select {
				case <-stopCh:
					return
				default:
					_, err := client.Get(ctx, testKey)
					if err != nil {
						errors <- err
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}

	// Let it run briefly
	time.Sleep(250 * time.Millisecond)
	close(stopCh)
	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		if err != nil && !isTransientError(err) {
			errorCount++
		}
	}

	assert.Less(t, errorCount, 5, "Too many errors during concurrent reads")

	// Client should still be functional
	data, err := client.Get(ctx, testKey)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-value"), data)
}

// TestTopology_ConcurrentWrites tests concurrent write operations during topology changes
func TestTopology_ConcurrentWrites(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Initial topology
	topology := setupSimpleTopology([]string{server.address})
	server.cacheService.SetClusterTopology(topology)

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{server.address},
		Mode:            ModeCluster,
		RefreshInterval: 50 * time.Millisecond,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	successWrites := int32(0)

	// Concurrent topology updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		epoch := uint64(2)
		for i := 0; i < 10; i++ {
			select {
			case <-stopCh:
				return
			default:
				newTopology := setupSimpleTopology([]string{server.address})
				newTopology.Epoch = epoch
				server.cacheService.SetClusterTopology(newTopology)
				epoch++
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	// Concurrent writes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("write-key-%d", id)
			for j := 0; j < 10; j++ {
				select {
				case <-stopCh:
					return
				default:
					err := client.Put(ctx, key, []byte("value"), 0)
					if err == nil {
						atomic.AddInt32(&successWrites, 1)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(i)
	}

	// Let it run briefly
	time.Sleep(250 * time.Millisecond)
	close(stopCh)
	wg.Wait()

	// Should have successful writes
	assert.Greater(t, atomic.LoadInt32(&successWrites), int32(20), "Should have many successful writes")
}

// TestTopology_NodeFailure verifies handling of node failures
func TestTopology_NodeFailure(t *testing.T) {
	// Create three servers
	servers := make([]*testServer, 3)
	addresses := make([]string, 3)
	for i := 0; i < 3; i++ {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		defer server.Stop()
		servers[i] = server
		addresses[i] = server.address
	}

	// Initial topology with all servers active
	topology := setupSimpleTopology(addresses)
	for _, server := range servers {
		server.cacheService.SetClusterTopology(topology)
	}

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{servers[0].address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// All nodes should be connected
	assert.Len(t, client.GetConnectedNodes(), 3)

	// Simulate node 1 failure
	servers[1].Stop()

	// Update topology to reflect node 1 is down
	// Create topology with only 2 active nodes (exclude node 1)
	activeAddresses := []string{addresses[0], addresses[2]}
	topology2 := setupSimpleTopology(activeAddresses)
	// Restore original node list but mark node-1 as DOWN
	topology2.Nodes = []*clusterpb.NodeInfo{
		{Id: "node-0", Address: addresses[0], ListenAddress: addresses[0], Status: clusterpb.NodeStatus_NODE_STATUS_ACTIVE},
		{Id: "node-1", Address: addresses[1], ListenAddress: addresses[1], Status: clusterpb.NodeStatus_NODE_STATUS_DOWN},
		{Id: "node-2", Address: addresses[2], ListenAddress: addresses[2], Status: clusterpb.NodeStatus_NODE_STATUS_ACTIVE},
	}
	// Keep only tokens for active nodes (node-0 and node-2 from setupSimpleTopology)
	// The setupSimpleTopology was called with 2 addresses, so it created tokens for node-0 and node-1
	// We need to rename node-1 to node-2 in the token list
	for _, nt := range topology2.RingConfig.NodeTokens {
		if nt.NodeId == "node-1" {
			nt.NodeId = "node-2"
		}
	}
	topology2.Epoch = 2
	servers[0].cacheService.SetClusterTopology(topology2)
	servers[2].cacheService.SetClusterTopology(topology2)

	// Fetch and update topology
	if cc, ok := client.CacheClient.(*ClusterClient); ok {
		newTopology, err := cc.FetchTopology()
		require.NoError(t, err)
		err = cc.UpdateTopology(newTopology)
		require.NoError(t, err)
	}

	// Only 2 nodes should be connected
	connectedNodes := client.GetConnectedNodes()
	assert.Len(t, connectedNodes, 2)
	assert.NotContains(t, connectedNodes, addresses[1])

	// Operations should still work with remaining nodes
	ctx := context.Background()
	err = client.Put(ctx, "test-key", []byte("test-value"), 0)
	assert.NoError(t, err)
}

// TestTopology_TokenReassignment verifies token ownership changes between nodes
func TestTopology_TokenReassignment(t *testing.T) {
	// Create two servers
	server1, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server1.Stop()

	server2, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server2.Stop()

	// Initial topology - all tokens on server1
	topology1 := &clusterpb.ClusterTopology{
		Epoch: 1,
		Nodes: []*clusterpb.NodeInfo{
			{
				Id:            "node-0",
				Address:       server1.address,
				ListenAddress: server1.address,
				Status:        clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
			},
			{
				Id:            "node-1",
				Address:       server2.address,
				ListenAddress: server2.address,
				Status:        clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
			},
		},
		RingConfig: &clusterpb.RingConfig{
			ReplicationFactor: 1,
			NodeTokens: []*clusterpb.NodeTokens{
				{
					NodeId: "node-0",
					Tokens: []uint32{0, 1000000000, 2000000000, 3000000000},
				},
			},
		},
	}

	server1.cacheService.SetClusterTopology(topology1)

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs: []string{server1.address},
		Mode:  ModeCluster,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// All keys should route to node-0 initially
	nodeID, err := client.GetNodeIDForKey("test-key")
	require.NoError(t, err)
	assert.Equal(t, "node-0", nodeID)

	// Rebalance - give some tokens to node-1
	topology2 := &clusterpb.ClusterTopology{
		Epoch: 2,
		Nodes: topology1.Nodes,
		RingConfig: &clusterpb.RingConfig{
			ReplicationFactor: 1,
			NodeTokens: []*clusterpb.NodeTokens{
				{
					NodeId: "node-0",
					Tokens: []uint32{0, 1000000000},
				},
				{
					NodeId: "node-1",
					Tokens: []uint32{2000000000, 3000000000},
				},
			},
		},
	}

	server1.cacheService.SetClusterTopology(topology2)

	// Update topology
	if cc, ok := client.CacheClient.(*ClusterClient); ok {
		newTopology, err := cc.FetchTopology()
		require.NoError(t, err)
		err = cc.UpdateTopology(newTopology)
		require.NoError(t, err)
	}

	// Verify the ring has been updated
	assert.True(t, client.HasRing())
	assert.Equal(t, uint64(2), client.GetTopologyEpoch())
}

// TestTopology_NoRoutingGapWhileAddingNode ensures the refresh path does not
// expose a new ring while its member pool is still being constructed.
func TestTopology_NoRoutingGapWhileAddingNode(t *testing.T) {
	servers := make([]*testServer, 3)
	addresses := make([]string, 3)
	for i := range servers {
		server, err := newTestServerWithAddr()
		require.NoError(t, err)
		servers[i] = server
		addresses[i] = server.address
		defer server.Stop()
	}

	initial := setupSimpleTopology(addresses[:2])
	added := setupSimpleTopology(addresses)
	added.Epoch = 2
	for _, server := range servers {
		server.cacheService.SetClusterTopology(initial)
	}

	var block atomic.Bool
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDial := func() {
		releaseOnce.Do(func() { close(release) })
	}
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		if addr == addresses[2] && block.Load() {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}

	client, err := NewClusterClient(&ClientConfig{
		Addrs:              []string{addresses[0]},
		ConnectionPoolSize: 1,
		RefreshInterval:    2 * time.Millisecond,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
			grpc.WithBlock(),
		},
	})
	require.NoError(t, err)
	defer func() {
		block.Store(false)
		releaseDial()
		if client != nil {
			_ = client.Close()
		}
	}()

	newRing := NewTokenRing()
	nodeTokens := make(map[string][]uint32)
	nodeAddresses := make(map[string]string)
	for _, node := range added.Nodes {
		nodeAddresses[node.Id] = node.ListenAddress
	}
	for _, nodeTokensForNode := range added.RingConfig.NodeTokens {
		nodeTokens[nodeTokensForNode.NodeId] = nodeTokensForNode.Tokens
	}
	newRing.Update(nodeTokens, nodeAddresses)

	var key string
	for i := 0; i < 100000; i++ {
		candidate := string(rune(i))
		oldAddr, oldErr := client.topology.GetNodeForKey(candidate)
		newAddr, newErr := newRing.GetNodeForKey(candidate)
		if oldErr == nil && newErr == nil && oldAddr != addresses[2] && newAddr == addresses[2] {
			key = candidate
			break
		}
	}
	require.NotEmpty(t, key, "no key moved to added member")

	block.Store(true)
	for _, server := range servers {
		server.cacheService.SetClusterTopology(added)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		releaseDial()
		_ = client.Close()
		t.Fatal("refresh did not start blocked dial")
	}

	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, routeErr := client.Route(key)
		if routeErr != nil {
			releaseDial()
			_ = client.Close()
			t.Fatalf("route failed while new pool was staging: %v", routeErr)
		}
	}

	releaseDial()
	block.Store(false)
	deadline = time.Now().Add(time.Second)
	for client.GetTopologyEpoch() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	assert.Equal(t, uint64(2), client.GetTopologyEpoch())
}

// TestTopology_WaitsForReplacementBeforeRoutingRemovedOwner ensures a route
// does not use a removed pool while its replacement is being staged.
func TestTopology_WaitsForReplacementBeforeRoutingRemovedOwner(t *testing.T) {
	oldServer, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer oldServer.Stop()
	newServer, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer newServer.Stop()

	initial := setupSimpleTopology([]string{oldServer.address})
	oldServer.cacheService.SetClusterTopology(initial)
	var block atomic.Bool
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		if addr == newServer.address && block.Load() {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}

	client, err := NewClusterClient(&ClientConfig{
		Addrs:              []string{oldServer.address},
		ConnectionPoolSize: 1,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
			grpc.WithBlock(),
		},
	})
	require.NoError(t, err)
	defer func() {
		block.Store(false)
		select {
		case <-release:
		default:
			close(release)
		}
		_ = client.Close()
	}()

	block.Store(true)
	updated := setupSimpleTopology([]string{newServer.address})
	updated.Epoch = 2
	updateDone := make(chan error, 1)
	go func() { updateDone <- client.UpdateTopology(updated) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("replacement dial did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	getDone := make(chan error, 1)
	go func() {
		_, routeErr := client.Get(ctx, "replacement-route-key")
		getDone <- routeErr
	}()
	listDone := make(chan error, 1)
	go func() {
		_, _, _, routeErr := client.ListPage(ctx, "", 1, "")
		listDone <- routeErr
	}()
	select {
	case routeErr := <-getDone:
		t.Fatalf("keyed operation returned before its context expired: %v", routeErr)
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case routeErr := <-listDone:
		t.Fatalf("round-robin operation returned before its context expired: %v", routeErr)
	case <-time.After(20 * time.Millisecond):
	}
	require.ErrorIs(t, <-getDone, context.DeadlineExceeded)
	require.ErrorIs(t, <-listDone, context.DeadlineExceeded)

	close(release)
	require.NoError(t, <-updateDone)
	conn, routeErr := client.Route("replacement-route-key")
	require.NoError(t, routeErr)
	require.NotNil(t, conn)
	assert.Equal(t, newServer.address, conn.address)
}

// TestTopology_CloseCancelsBlockedPoolStaging ensures Close interrupts a
// blocking replacement dial instead of waiting for an unreachable member.
func TestTopology_CloseCancelsBlockedPoolStaging(t *testing.T) {
	oldServer, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer oldServer.Stop()
	newServer, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer newServer.Stop()

	initial := setupSimpleTopology([]string{oldServer.address})
	oldServer.cacheService.SetClusterTopology(initial)
	var block atomic.Bool
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDial := func() {
		releaseOnce.Do(func() { close(release) })
	}
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		if addr == newServer.address && block.Load() {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}

	client, err := NewClusterClient(&ClientConfig{
		Addrs:              []string{oldServer.address},
		ConnectionPoolSize: 1,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
			grpc.WithBlock(),
		},
	})
	require.NoError(t, err)
	defer func() {
		block.Store(false)
		releaseDial()
		_ = client.Close()
	}()

	block.Store(true)
	updated := setupSimpleTopology([]string{newServer.address})
	updated.Epoch = 2
	updateDone := make(chan error, 1)
	go func() { updateDone <- client.UpdateTopology(updated) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("replacement dial did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close() }()
	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		releaseDial()
		t.Fatal("Close waited for the blocked replacement dial")
	}
	select {
	case updateErr := <-updateDone:
		require.NoError(t, updateErr)
	case <-time.After(time.Second):
		t.Fatal("topology update did not stop after Close")
	}
}

// TestTopology_RoutingRetryCancelsBlockedPoolStaging ensures a routing-error
// retry stops a replacement dial when the operation context expires.
func TestTopology_RoutingRetryCancelsBlockedPoolStaging(t *testing.T) {
	oldServer, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer oldServer.Stop()
	newServer, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer newServer.Stop()

	initial := setupSimpleTopology([]string{oldServer.address})
	oldServer.cacheService.SetClusterTopology(initial)

	var block atomic.Bool
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDial := func() {
		releaseOnce.Do(func() { close(release) })
	}
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		if addr == newServer.address && block.Load() {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}

	client, err := NewClusterClient(&ClientConfig{
		Addrs:              []string{oldServer.address},
		ConnectionPoolSize: 1,
		RefreshInterval:    time.Hour,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
			grpc.WithBlock(),
		},
	})
	require.NoError(t, err)
	defer func() {
		block.Store(false)
		releaseDial()
		_ = client.Close()
	}()

	block.Store(true)
	updated := setupSimpleTopology([]string{oldServer.address, newServer.address})
	updated.Epoch = 2
	oldServer.cacheService.SetClusterTopology(updated)
	oldServer.cacheService.getError = status.Error(codes.Unavailable, "routing error")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	operationDone := make(chan error, 1)
	go func() {
		_, err := client.Get(ctx, "routing-retry-cancellation-key")
		operationDone <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("routing retry did not start replacement dial")
	}

	select {
	case err := <-operationDone:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("routing retry waited past the operation deadline")
	}

	releaseDial()
	assert.Equal(t, uint64(1), client.GetTopologyEpoch())
	assert.Equal(t, []string{oldServer.address}, client.GetConnectedNodes())
}

// TestTopology_RoutingRetryCancelsWhileBackgroundStages ensures an operation
// deadline also interrupts waiting for a concurrent background refresh.
func TestTopology_RoutingRetryCancelsWhileBackgroundStages(t *testing.T) {
	oldServer, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer oldServer.Stop()
	newServer, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer newServer.Stop()

	initial := setupSimpleTopology([]string{oldServer.address})
	oldServer.cacheService.SetClusterTopology(initial)

	var block atomic.Bool
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDial := func() {
		releaseOnce.Do(func() { close(release) })
	}
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		if addr == newServer.address && block.Load() {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}

	client, err := NewClusterClient(&ClientConfig{
		Addrs:              []string{oldServer.address},
		ConnectionPoolSize: 1,
		RefreshInterval:    time.Hour,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
			grpc.WithBlock(),
		},
	})
	require.NoError(t, err)
	defer func() {
		block.Store(false)
		releaseDial()
		_ = client.Close()
	}()

	block.Store(true)
	updated := setupSimpleTopology([]string{oldServer.address, newServer.address})
	updated.Epoch = 2
	oldServer.cacheService.SetClusterTopology(updated)
	oldServer.cacheService.getError = status.Error(codes.Unavailable, "routing error")

	backgroundDone := make(chan error, 1)
	go func() {
		backgroundDone <- client.UpdateTopology(updated)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not start replacement dial")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	operationDone := make(chan error, 1)
	go func() {
		_, err := client.Get(ctx, "background-refresh-cancellation-key")
		operationDone <- err
	}()

	select {
	case err := <-operationDone:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("routing retry waited for background staging after its deadline")
	}

	block.Store(false)
	releaseDial()
	require.NoError(t, <-backgroundDone)
	assert.Equal(t, uint64(2), client.GetTopologyEpoch())
	assert.ElementsMatch(t, []string{oldServer.address, newServer.address}, client.GetConnectedNodes())
}

// TestTopology_RetainsReachablePoolWhenMemberDialFails ensures a failed member
// does not discard pools which were constructed for reachable members.
func TestTopology_RetainsReachablePoolWhenMemberDialFails(t *testing.T) {
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	badAddr := "127.0.0.1:1"
	topology := setupSimpleTopology([]string{server.address, badAddr})
	server.cacheService.SetClusterTopology(topology)

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		if addr == badAddr {
			return nil, fmt.Errorf("dial disabled for test member")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}
	client, err := NewClusterClient(&ClientConfig{
		Addrs:              []string{server.address},
		ConnectionPoolSize: 1,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
			grpc.WithBlock(),
			grpc.FailOnNonTempDialError(true),
			grpc.WithTimeout(50 * time.Millisecond),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	assert.Equal(t, []string{server.address}, client.GetConnectedNodes())
	var key string
	for i := 0; i < 100000; i++ {
		candidate := fmt.Sprintf("reachable-member-key-%d", i)
		addr, routeErr := client.topology.GetNodeForKey(candidate)
		if routeErr == nil && addr == server.address {
			key = candidate
			break
		}
	}
	require.NotEmpty(t, key, "no key owned by reachable member")
	_, err = client.Route(key)
	assert.NoError(t, err)
}

// isTransientError checks if an error is transient (expected during topology changes)
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "rpc error: code = Canceled desc = grpc: the client connection is closing" ||
		errStr == "no available connections"
}
