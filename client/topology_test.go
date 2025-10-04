package cacheclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	topology2 := setupSimpleTopology(addresses)
	topology2.Epoch = 2
	topology2.Nodes[1].Status = clusterpb.NodeStatus_NODE_STATUS_DOWN
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

// TestTopology_PartitionReassignment verifies partition ownership changes
func TestTopology_PartitionReassignment(t *testing.T) {
	// Create two servers
	server1, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server1.Stop()

	server2, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server2.Stop()

	// Initial topology - all partitions on server1
	topology1 := &clusterpb.ClusterTopology{
		Epoch: 1,
		Nodes: []*clusterpb.NodeInfo{
			{
				Id:            "node-0",
				Address:       server1.address,
				ListenAddress: server1.address, // For tests, use the same address for both cluster and listen
				Status:        clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
			},
			{
				Id:            "node-1",
				Address:       server2.address,
				ListenAddress: server2.address, // For tests, use the same address for both cluster and listen
				Status:        clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
			},
		},
		RingConfig: &clusterpb.RingConfig{
			PartitionCount:    10,
			ReplicationFactor: 20,
			Load:              1.25,
		},
		PartitionOwners: make([]*clusterpb.PartitionOwner, 0, 10),
	}

	// All partitions initially on node-0
	for i := int32(0); i < 10; i++ {
		topology1.PartitionOwners = append(topology1.PartitionOwners, &clusterpb.PartitionOwner{
			PartitionId: i,
			NodeId:      "node-0",
		})
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

	// Verify initial partition ownership
	for i := int32(0); i < 10; i++ {
		assert.Equal(t, "node-0", client.GetPartitionOwnerID(i))
	}

	// Rebalance - move half partitions to node-1
	topology2 := &clusterpb.ClusterTopology{
		Epoch:           2,
		Nodes:           topology1.Nodes,
		RingConfig:      topology1.RingConfig,
		PartitionOwners: make([]*clusterpb.PartitionOwner, 0, 10),
	}

	for i := int32(0); i < 10; i++ {
		nodeId := "node-0"
		if i >= 5 {
			nodeId = "node-1"
		}
		topology2.PartitionOwners = append(topology2.PartitionOwners, &clusterpb.PartitionOwner{
			PartitionId: i,
			NodeId:      nodeId,
		})
	}

	server1.cacheService.SetClusterTopology(topology2)

	// Update topology
	if cc, ok := client.CacheClient.(*ClusterClient); ok {
		newTopology, err := cc.FetchTopology()
		require.NoError(t, err)
		err = cc.UpdateTopology(newTopology)
		require.NoError(t, err)
	}

	// Verify partition reassignment
	for i := int32(0); i < 5; i++ {
		assert.Equal(t, "node-0", client.GetPartitionOwnerID(i))
	}
	for i := int32(5); i < 10; i++ {
		assert.Equal(t, "node-1", client.GetPartitionOwnerID(i))
	}
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
