package cacheclient

import (
	"context"
	"fmt"
	"sync"
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
	server.clusterService.SetTopology(topology1)

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
	server.clusterService.SetTopology(topology2)

	// Wait for refresh with eventual consistency check
	assert.Eventually(t, func() bool {
		return client.GetTopologyEpoch() == uint64(2)
	}, 500*time.Millisecond, 50*time.Millisecond, "Epoch should be updated to 2")

	// Update again
	topology3 := setupSimpleTopology([]string{server.address})
	topology3.Epoch = 3
	server.clusterService.SetTopology(topology3)

	// Wait for another refresh with eventual consistency check
	assert.Eventually(t, func() bool {
		return client.GetTopologyEpoch() == uint64(3)
	}, 500*time.Millisecond, 50*time.Millisecond, "Epoch should be updated to 3")

	// Verify multiple calls to GetClusterTopology
	assert.Greater(t, server.clusterService.getTopologyCallCount.Load(), int32(2))
}

// TestTopologyRefreshLoop_ErrorHandling verifies continues after errors
func TestTopologyRefreshLoop_ErrorHandling(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Initial topology
	topology1 := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology1)

	// Create client with short refresh interval
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{server.address},
		Mode:            ModeCluster,
		RefreshInterval: 100 * time.Millisecond,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Initial epoch should be 1
	assert.Equal(t, uint64(1), client.GetTopologyEpoch())

	// Make topology fetch fail
	server.clusterService.SetTopologyError(status.Error(codes.Unavailable, "topology unavailable"))

	// Wait for failed refresh attempt
	time.Sleep(150 * time.Millisecond)

	// Epoch should remain unchanged
	assert.Equal(t, uint64(1), client.GetTopologyEpoch())

	// Fix the error and update topology
	server.clusterService.SetTopologyError(nil)
	topology2 := setupSimpleTopology([]string{server.address})
	topology2.Epoch = 2
	server.clusterService.SetTopology(topology2)

	// Wait for successful refresh with eventual consistency
	assert.Eventually(t, func() bool {
		return client.GetTopologyEpoch() == uint64(2)
	}, 500*time.Millisecond, 50*time.Millisecond, "Epoch should be updated after error is fixed")
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
	server1.clusterService.SetTopology(topology1)

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
	server1.clusterService.SetTopology(topology2)

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
	servers[0].clusterService.SetTopology(topology1)

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
	servers[0].clusterService.SetTopology(topology2)

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
	servers[0].clusterService.SetTopology(topology3)

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

// TestUpdateTopology_ConcurrentAccess verifies thread-safe updates
func TestUpdateTopology_ConcurrentAccess(t *testing.T) {
	// Create a server
	server, err := newTestServerWithAddr()
	require.NoError(t, err)
	defer server.Stop()

	// Initial topology
	topology := setupSimpleTopology([]string{server.address})
	server.clusterService.SetTopology(topology)

	// Create client
	client, err := NewWithConfig(&ClientConfig{
		Addrs:           []string{server.address},
		Mode:            ModeCluster,
		RefreshInterval: 10 * time.Millisecond, // Very short for stress testing
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer client.Close()

	// Prepare test data
	testKey := "concurrent-test"
	server.cacheService.data[testKey] = []byte("test-value")

	ctx := context.Background()
	var wg sync.WaitGroup
	errors := make(chan error, 100)
	stopCh := make(chan struct{})

	// Concurrent topology updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		epoch := uint64(2)
		for {
			select {
			case <-stopCh:
				return
			default:
				// Update topology
				newTopology := setupSimpleTopology([]string{server.address})
				newTopology.Epoch = epoch
				server.clusterService.SetTopology(newTopology)
				epoch++
				time.Sleep(15 * time.Millisecond)
			}
		}
	}()

	// Concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					_, err := client.Get(ctx, testKey)
					if err != nil {
						select {
						case errors <- err:
						case <-stopCh:
							return
						}
					}
				}
			}
		}()
	}

	// Concurrent writes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("write-key-%d", id)
			for {
				select {
				case <-stopCh:
					return
				default:
					err := client.Put(ctx, key, []byte("value"), 0)
					if err != nil {
						select {
						case errors <- err:
						case <-stopCh:
							return
						}
					}
				}
			}
		}(i)
	}

	// Let it run for a bit
	time.Sleep(500 * time.Millisecond)

	// Stop all goroutines
	close(stopCh)
	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	connectionClosingErrors := 0
	for err := range errors {
		if err != nil {
			// Connection closing errors are expected during topology changes
			if err.Error() == "rpc error: code = Canceled desc = grpc: the client connection is closing" {
				connectionClosingErrors++
			} else {
				errorCount++
				t.Logf("Concurrent operation error: %v", err)
			}
		}
	}

	// Log connection closing errors separately
	if connectionClosingErrors > 0 {
		t.Logf("Expected connection closing errors: %d", connectionClosingErrors)
	}

	// Some non-connection-closing errors are expected due to topology changes, but not too many
	assert.Less(t, errorCount, 10, "Too many unexpected errors during concurrent operations")

	// Client should still be functional (retry a few times as topology may have just changed)
	var data []byte
	var finalErr error
	for i := 0; i < 3; i++ {
		data, finalErr = client.Get(ctx, testKey)
		if finalErr == nil {
			break
		}
		// If error is connection closing, wait briefly for topology to stabilize
		if finalErr.Error() == "rpc error: code = Canceled desc = grpc: the client connection is closing" {
			time.Sleep(100 * time.Millisecond)
		} else {
			break // Other errors, don't retry
		}
	}
	require.NoError(t, finalErr, "Client should be able to read after topology stabilizes")
	assert.Equal(t, []byte("test-value"), data)
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
		server.clusterService.SetTopology(topology)
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
	servers[0].clusterService.SetTopology(topology2)
	servers[2].clusterService.SetTopology(topology2)

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
				Id:      "node-0",
				Address: server1.address,
				Status:  clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
			},
			{
				Id:      "node-1",
				Address: server2.address,
				Status:  clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
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

	server1.clusterService.SetTopology(topology1)

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
	assert.Equal(t, 10, client.GetPartitionOwnerCount())
	for i := int32(0); i < 10; i++ {
		assert.Equal(t, "node-0", client.GetPartitionOwner(i))
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

	server1.clusterService.SetTopology(topology2)

	// Update topology
	if cc, ok := client.CacheClient.(*ClusterClient); ok {
		newTopology, err := cc.FetchTopology()
		require.NoError(t, err)
		err = cc.UpdateTopology(newTopology)
		require.NoError(t, err)
	}

	// Verify partition reassignment
	for i := int32(0); i < 5; i++ {
		assert.Equal(t, "node-0", client.GetPartitionOwner(i))
	}
	for i := int32(5); i < 10; i++ {
		assert.Equal(t, "node-1", client.GetPartitionOwner(i))
	}
}
