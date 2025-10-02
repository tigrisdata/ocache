package coordinator

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"google.golang.org/grpc"
)

func TestCoordinator_New(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9090",
		ListenAddr:         "localhost:8090",
		RingPartitionCount: 16384,
		HeartbeatInterval:  5,
		FailureThreshold:   3,
	}

	coord, err := New(config)
	require.NoError(t, err)
	assert.NotNil(t, coord)
	assert.Equal(t, "test-node", coord.GetLocalNodeID())
	assert.NotNil(t, coord.GetRing())
	assert.NotNil(t, coord.GetRouter())
}

// TestCoordinator_ConfigValidation verifies coordinator config validation
func TestCoordinator_ConfigValidation(t *testing.T) {
	// Test missing listen address
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:7000",
		ListenAddr:         "", // Missing listen address
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "listen address is required")
	assert.Nil(t, coord)

	// Test missing cluster address
	config2 := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "", // Invalid cluster address
		ListenAddr:         "localhost:9000",
		RingPartitionCount: 1024,
	}

	coord2, err := New(config2)
	assert.Error(t, err)
	assert.Nil(t, coord2)
}

// TestCoordinator_StartWithInvalidAddress tests invalid address validation
func TestCoordinator_StartWithInvalidAddress(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "invalid-address",
		RingPartitionCount: 1024,
	}

	// Should fail during New() with address validation
	coord, err := New(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cluster address")
	assert.Nil(t, coord)
}

// TestCoordinator_PortAlreadyInUse tests behavior when port is already in use
func TestCoordinator_PortAlreadyInUse(t *testing.T) {
	// Start first coordinator
	config1 := &Config{
		Enabled:            true,
		MyNodeID:           "node1",
		ClusterAddr:        "localhost:9324",
		ListenAddr:         "localhost:9325",
		RingPartitionCount: 1024,
	}

	coord1, err := New(config1)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = coord1.Start(ctx)
	require.NoError(t, err)
	defer coord1.Stop()

	// Try to start second coordinator on same port
	config2 := &Config{
		Enabled:            true,
		MyNodeID:           "node2",
		ClusterAddr:        "localhost:9324", // Same port
		ListenAddr:         "localhost:9325",
		RingPartitionCount: 1024,
	}

	coord2, err := New(config2)
	require.NoError(t, err)

	err = coord2.Start(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to listen")
}

func TestCoordinator_JoinAndHeartbeat(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9091",
		ListenAddr:         "localhost:8091",
		RingPartitionCount: 1024,
		HeartbeatInterval:  1,
		FailureThreshold:   2,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Test Join RPC
	ctx := context.Background()
	joinReq := &clusterpb.JoinRequest{
		NodeId:        "new-node",
		Address:       "localhost:9092",
		ListenAddress: "localhost:8092",
	}

	joinResp, err := coord.Join(ctx, joinReq)
	require.NoError(t, err)
	assert.True(t, joinResp.Success)
	assert.Equal(t, uint64(2), joinResp.Epoch)

	// Verify node was added
	nodes := coord.GetRing().GetAllNodes()
	assert.Len(t, nodes, 2)

	// Find the new node and verify addresses
	for _, node := range nodes {
		if node.ID == "new-node" {
			assert.Equal(t, "localhost:9092", node.Address, "Cluster address should be stored")
			assert.Equal(t, "localhost:8092", node.ListenAddress, "Listen address should be stored")
		}
	}

	// Test Heartbeat RPC
	hbReq := &clusterpb.HeartbeatRequest{
		NodeId: "new-node",
		Epoch:  2,
	}

	hbResp, err := coord.Heartbeat(ctx, hbReq)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), hbResp.Epoch)

	// Verify heartbeat was recorded
	coord.mu.RLock()
	lastHb, exists := coord.lastHeartbeat["new-node"]
	coord.mu.RUnlock()
	assert.True(t, exists)
	assert.WithinDuration(t, time.Now(), lastHb, 2*time.Second)
}

// TestCoordinator_AddressSeparation verifies cluster and listen addresses are properly separated
func TestCoordinator_NodeAddresses(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "coordinator-node",
		ClusterAddr:        "localhost:7001",
		ListenAddr:         "localhost:9001",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Verify self node has both addresses
	nodes := coord.GetRing().GetAllNodes()
	require.Len(t, nodes, 1)
	assert.Equal(t, "coordinator-node", nodes[0].ID)
	assert.Equal(t, "localhost:7001", nodes[0].Address)
	assert.Equal(t, "localhost:9001", nodes[0].ListenAddress)

	// Add multiple nodes with different addresses
	ctx := context.Background()

	// Node 1
	joinReq1 := &clusterpb.JoinRequest{
		NodeId:        "node1",
		Address:       "localhost:7002",
		ListenAddress: "localhost:9002",
	}
	joinResp1, err := coord.Join(ctx, joinReq1)
	require.NoError(t, err)
	assert.True(t, joinResp1.Success)

	// Node 2
	joinReq2 := &clusterpb.JoinRequest{
		NodeId:        "node2",
		Address:       "localhost:7003",
		ListenAddress: "localhost:9003",
	}
	joinResp2, err := coord.Join(ctx, joinReq2)
	require.NoError(t, err)
	assert.True(t, joinResp2.Success)

	// Verify all nodes have correct addresses
	allNodes := coord.GetRing().GetAllNodes()
	assert.Len(t, allNodes, 3)

	addressMap := make(map[string]struct {
		clusterAddr string
		listenAddr  string
	})

	for _, node := range allNodes {
		addressMap[node.ID] = struct {
			clusterAddr string
			listenAddr  string
		}{
			clusterAddr: node.Address,
			listenAddr:  node.ListenAddress,
		}
	}

	// Verify each node has correct addresses
	assert.Equal(t, "localhost:7001", addressMap["coordinator-node"].clusterAddr)
	assert.Equal(t, "localhost:9001", addressMap["coordinator-node"].listenAddr)
	assert.Equal(t, "localhost:7002", addressMap["node1"].clusterAddr)
	assert.Equal(t, "localhost:9002", addressMap["node1"].listenAddr)
	assert.Equal(t, "localhost:7003", addressMap["node2"].clusterAddr)
	assert.Equal(t, "localhost:9003", addressMap["node2"].listenAddr)

	// Test GetClusterState returns correct addresses
	state, err := coord.GetClusterState(ctx, &clusterpb.Empty{})
	require.NoError(t, err)
	assert.Len(t, state.Nodes, 3)

	for _, node := range state.Nodes {
		if node.Id == "node1" {
			assert.Equal(t, "localhost:7002", node.Address)
			assert.Equal(t, "localhost:9002", node.ListenAddress)
		}
	}

	// Test GetClusterTopology returns correct addresses
	topology, err := coord.GetClusterTopology(ctx, &clusterpb.Empty{})
	require.NoError(t, err)
	assert.Len(t, topology.Nodes, 3)

	for _, node := range topology.Nodes {
		if node.Id == "node2" {
			assert.Equal(t, "localhost:7003", node.Address)
			assert.Equal(t, "localhost:9003", node.ListenAddress)
		}
	}
}

// TestCoordinator_RequiresBothAddresses verifies that nodes must provide both addresses
func TestCoordinator_RequiresBothAddresses(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "coordinator-node",
		ClusterAddr:        "localhost:7100",
		ListenAddr:         "localhost:9100",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Test 1: Join with empty listen address should fail
	joinReq := &clusterpb.JoinRequest{
		NodeId:        "node-without-listen",
		Address:       "localhost:7101",
		ListenAddress: "", // Empty listen address
	}

	_, err = coord.Join(ctx, joinReq)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid join request: missing required fields")

	// Test 2: Join with empty cluster address should fail
	joinReq2 := &clusterpb.JoinRequest{
		NodeId:        "node-without-cluster",
		Address:       "", // Empty cluster address
		ListenAddress: "localhost:9101",
	}

	_, err = coord.Join(ctx, joinReq2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid join request: missing required fields")

	// Test 3: Verify no nodes were added
	nodes := coord.GetRing().GetAllNodes()
	assert.Len(t, nodes, 1) // Only coordinator node
	assert.Equal(t, "coordinator-node", nodes[0].ID)
}

// TestCoordinator_JoinClusterFailure tests that node starts as bootstrap node if it can't sync with any nodes
func TestCoordinator_JoinClusterFailure(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:19200",
		ListenAddr:         "localhost:19201",
		Nodes:              []string{"invalid-node1:9999", "invalid-node2:9999"}, // Invalid nodes
		RingPartitionCount: 10,
		HeartbeatInterval:  1,
		FailureThreshold:   2,
		SyncTimeout:        1,
	}

	coord, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, coord)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should not fail if we can't sync with any nodes
	// we will start as bootstrap node
	err = coord.Start(ctx)
	assert.NoError(t, err)

	// verify we are a bootstrap node
	nodes := coord.GetRing().GetAllNodes()
	assert.Len(t, nodes, 1) // Only coordinator node
	assert.Equal(t, "test-node", nodes[0].ID)
}

// TestCoordinator_JoinClusterWithNodes tests joining cluster with nodes
func TestCoordinator_JoinClusterWithNodes(t *testing.T) {
	// Start a node
	nodeConfig := &Config{
		Enabled:            true,
		MyNodeID:           "node1",
		ClusterAddr:        "localhost:9301",
		ListenAddr:         "localhost:8301",
		RingPartitionCount: 1024,
	}

	node, err := New(nodeConfig)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = node.Start(ctx)
	require.NoError(t, err)
	defer node.Stop()

	// Wait for node to be ready
	time.Sleep(100 * time.Millisecond)

	// Start a new node
	newNodeConfig := &Config{
		Enabled:            true,
		MyNodeID:           "node2",
		ClusterAddr:        "localhost:9302",
		ListenAddr:         "localhost:8302",
		Nodes:              []string{"localhost:9301"},
		RingPartitionCount: 1024,
	}

	node2, err := New(newNodeConfig)
	require.NoError(t, err)

	err = node2.Start(ctx)
	require.NoError(t, err)
	defer node2.Stop()

	// Verify both nodes see each other
	seenNodes := node.GetRing().GetAllNodes()
	assert.Len(t, seenNodes, 2)

	seenNodes2 := node2.GetRing().GetAllNodes()
	assert.Len(t, seenNodes2, 2)
}

func TestCoordinator_Leave(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9093",
		ListenAddr:         "localhost:8093",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Add a node
	_, err = coord.Join(ctx, &clusterpb.JoinRequest{
		NodeId:        "leaving-node",
		Address:       "localhost:9094",
		ListenAddress: "localhost:8094",
	})
	require.NoError(t, err)

	// Leave the cluster
	leaveResp, err := coord.Leave(ctx, &clusterpb.LeaveRequest{
		NodeId: "leaving-node",
	})
	require.NoError(t, err)
	assert.True(t, leaveResp.Success)

	// Verify node was removed
	nodes := coord.GetRing().GetAllNodes()
	assert.Len(t, nodes, 1)
	assert.Equal(t, "test-node", nodes[0].ID)
}

// TestCoordinator_HeartbeatWithEpochMismatch tests heartbeat with epoch mismatch
func TestCoordinator_HeartbeatWithEpochMismatch(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9303",
		ListenAddr:         "localhost:8303",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Send heartbeat with newer epoch
	hbReq := &clusterpb.HeartbeatRequest{
		NodeId: "remote-node",
		Epoch:  999,
	}

	hbResp, err := coord.Heartbeat(ctx, hbReq)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), hbResp.Epoch) // Local epoch should be 1
}

// TestCoordinator_VerifyLastHeartbeat tests heartbeat timeout detection
func TestCoordinator_VerifyLastHeartbeat(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9306",
		ListenAddr:         "localhost:8306",
		RingPartitionCount: 1024,
		HeartbeatInterval:  1,
		FailureThreshold:   2,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Add a node
	ctx := context.Background()
	_, err = coord.Join(ctx, &clusterpb.JoinRequest{
		NodeId:        "timeout-node",
		Address:       "localhost:9307",
		ListenAddress: "localhost:8307",
	})
	require.NoError(t, err)

	// Set last heartbeat to old time
	coord.mu.Lock()
	coord.lastHeartbeat["timeout-node"] = time.Now().Add(-10 * time.Second)
	coord.mu.Unlock()

	// Run verification
	coord.verifyLastHeartbeat()

	// Verify node is marked down by checking all nodes
	nodes := coord.GetRing().GetAllNodes()
	var timeoutNode *NodeInfo
	for _, n := range nodes {
		if n.ID == "timeout-node" {
			timeoutNode = n
			break
		}
	}
	require.NotNil(t, timeoutNode, "timeout-node should exist")
	assert.Equal(t, NodeStatusDown, timeoutNode.Status)
}

func TestCoordinator_GetClusterState(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9095",
		ListenAddr:         "localhost:8095",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Add some nodes
	_, err = coord.Join(ctx, &clusterpb.JoinRequest{
		NodeId:        "node1",
		Address:       "localhost:9096",
		ListenAddress: "localhost:8096",
	})
	require.NoError(t, err)

	_, err = coord.Join(ctx, &clusterpb.JoinRequest{
		NodeId:        "node2",
		Address:       "localhost:9097",
		ListenAddress: "localhost:8097",
	})
	require.NoError(t, err)

	// Get cluster state
	state, err := coord.GetClusterState(ctx, &clusterpb.Empty{})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), state.Epoch)
	assert.Len(t, state.Nodes, 3)

	// Verify all nodes are present
	nodeIDs := make(map[string]bool)
	for _, node := range state.Nodes {
		nodeIDs[node.Id] = true
	}
	assert.True(t, nodeIDs["test-node"])
	assert.True(t, nodeIDs["node1"])
	assert.True(t, nodeIDs["node2"])
}

func TestCoordinator_GetClusterTopology(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9095",
		ListenAddr:         "localhost:8095",
		RingPartitionCount: 128, // Use smaller count for testing
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Add some nodes
	_, err = coord.Join(ctx, &clusterpb.JoinRequest{
		NodeId:        "node1",
		Address:       "localhost:9096",
		ListenAddress: "localhost:8096",
	})
	require.NoError(t, err)

	_, err = coord.Join(ctx, &clusterpb.JoinRequest{
		NodeId:        "node2",
		Address:       "localhost:9097",
		ListenAddress: "localhost:8097",
	})
	require.NoError(t, err)

	// Get cluster topology
	topology, err := coord.GetClusterTopology(ctx, &clusterpb.Empty{})
	require.NoError(t, err)

	// Verify basic topology info
	assert.Equal(t, uint64(3), topology.Epoch)
	assert.Len(t, topology.Nodes, 3)

	// Verify ring configuration
	assert.NotNil(t, topology.RingConfig)
	assert.Equal(t, int32(128), topology.RingConfig.PartitionCount)
	assert.Equal(t, int32(100), topology.RingConfig.ReplicationFactor)
	assert.Equal(t, 1.25, topology.RingConfig.Load)

	// Verify partition ownership
	assert.NotEmpty(t, topology.PartitionOwners)
	assert.LessOrEqual(t, len(topology.PartitionOwners), 128) // Should not exceed partition count

	// Verify all partitions are owned
	partitionMap := make(map[int32]string)
	for _, owner := range topology.PartitionOwners {
		partitionMap[owner.PartitionId] = owner.NodeId
	}

	// Check that partition IDs are in valid range
	for partID := range partitionMap {
		assert.GreaterOrEqual(t, partID, int32(0))
		assert.Less(t, partID, int32(128))
	}

	// Verify that owners are valid nodes
	validNodes := map[string]bool{
		"test-node": true,
		"node1":     true,
		"node2":     true,
	}
	for _, nodeID := range partitionMap {
		assert.True(t, validNodes[nodeID], "Invalid node ID in partition ownership: %s", nodeID)
	}
}

func TestCoordinator_FailureDetection(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9098",
		ListenAddr:         "localhost:8098",
		RingPartitionCount: 1024,
		HeartbeatInterval:  1,
		FailureThreshold:   2,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Add a node
	ctx := context.Background()
	_, err = coord.Join(ctx, &clusterpb.JoinRequest{
		NodeId:        "failing-node",
		Address:       "localhost:9099",
		ListenAddress: "localhost:8099",
	})
	require.NoError(t, err)

	// Simulate node failure by recording multiple failures
	coord.recordFailure("failing-node")
	assert.Equal(t, 1, coord.failureCount["failing-node"])

	coord.recordFailure("failing-node")
	assert.Equal(t, 2, coord.failureCount["failing-node"])

	// After threshold, node should be marked down (but not in ring yet)
	coord.recordFailure("failing-node")
	assert.Equal(t, 3, coord.failureCount["failing-node"])

	clusterState, err := coord.GetClusterState(ctx, &clusterpb.Empty{})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), clusterState.Epoch)
	assert.Len(t, clusterState.Nodes, 2)

	var failingNode *clusterpb.NodeInfo
	for _, node := range clusterState.Nodes {
		if node.Id == "failing-node" {
			failingNode = node
			break
		}
	}

	assert.Equal(t, "failing-node", failingNode.Id)
	assert.Equal(t, clusterpb.NodeStatus_NODE_STATUS_DOWN, failingNode.Status)
}

// TestCoordinator_GetLocalNodeID tests getting local node ID
func TestCoordinator_GetLocalNodeID(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "my-special-node",
		ClusterAddr:        "localhost:9325",
		ListenAddr:         "localhost:8325",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	assert.Equal(t, "my-special-node", coord.GetLocalNodeID())
}

func TestCoordinator_IsLocal(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "local-node",
		ClusterAddr:        "localhost:9099",
		ListenAddr:         "localhost:8099",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Add another node
	ctx := context.Background()
	_, err = coord.Join(ctx, &clusterpb.JoinRequest{
		NodeId:        "remote-node",
		Address:       "localhost:9100",
		ListenAddress: "localhost:8100",
	})
	require.NoError(t, err)

	// Test key locality
	// Some keys should be local, some remote
	localCount := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		if coord.IsLocal(key) {
			localCount++
		}
	}

	// With 2 nodes, roughly 50% should be local
	assert.Greater(t, localCount, 20)
	assert.Less(t, localCount, 80)
}

// TestCoordinator_GetNodeForKey tests key routing
func TestCoordinator_GetNodeForKey(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9308",
		ListenAddr:         "localhost:8308",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Add more nodes
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err = coord.Join(ctx, &clusterpb.JoinRequest{
			NodeId:        fmt.Sprintf("node-%d", i),
			Address:       fmt.Sprintf("localhost:930%d", 9+i),
			ListenAddress: fmt.Sprintf("localhost:830%d", 8+i),
		})
		require.NoError(t, err)
	}

	// Test key distribution
	nodeDistribution := make(map[string]int)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		node, err := coord.GetNodeForKey(key)
		require.NoError(t, err)
		nodeDistribution[node.ID]++
	}

	// Verify distribution (should be roughly even)
	assert.Equal(t, 4, len(nodeDistribution)) // 4 nodes total
	for nodeID, count := range nodeDistribution {
		t.Logf("Node %s got %d keys", nodeID, count)
		assert.Greater(t, count, 5) // At least some keys
		assert.Less(t, count, 50)   // Not all keys
	}
}

// TestCoordinator_ConcurrentOperations tests concurrent join/leave operations
func TestCoordinator_ConcurrentOperations(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9313",
		ListenAddr:         "localhost:8313",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()
	done := make(chan bool)

	// Concurrent joins
	for i := 0; i < 10; i++ {
		go func(id int) {
			_, err := coord.Join(ctx, &clusterpb.JoinRequest{
				NodeId:        fmt.Sprintf("node-%d", id),
				Address:       fmt.Sprintf("localhost:931%d", 4+id),
				ListenAddress: fmt.Sprintf("localhost:831%d", 3+id),
			})
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all joins
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all nodes joined
	nodes := coord.GetRing().GetAllNodes()
	assert.Equal(t, 11, len(nodes)) // test-node + 10 new nodes

	// Concurrent leaves
	for i := 0; i < 5; i++ {
		go func(id int) {
			_, err := coord.Leave(ctx, &clusterpb.LeaveRequest{
				NodeId: fmt.Sprintf("node-%d", id),
			})
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all leaves
	for i := 0; i < 5; i++ {
		<-done
	}

	// Verify nodes left
	nodes = coord.GetRing().GetAllNodes()
	assert.Equal(t, 6, len(nodes)) // test-node + 5 remaining nodes
}

// TestCoordinator_RouteError tests routing when no nodes are available
func TestCoordinator_RouteError(t *testing.T) {
	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9326",
		ListenAddr:         "localhost:8326",
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Mark local node as down
	err = coord.GetRing().UpdateNodeStatus("test-node", NodeStatusDown)
	require.NoError(t, err)

	// Try to route a key - should fail as no nodes are available
	_, err = coord.Route("some-key")
	assert.Error(t, err)
}

// MockGRPCServer creates a mock gRPC server for testing
func createMockClusterServer(t *testing.T, addr string) (*grpc.Server, func()) {
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	server := grpc.NewServer()

	// Register a mock cluster service
	clusterpb.RegisterClusterServiceServer(server, &mockClusterService{})

	go func() {
		if err := server.Serve(lis); err != nil {
			// Server stopped
		}
	}()

	return server, func() {
		server.GracefulStop()
	}
}

type mockClusterService struct {
	clusterpb.UnimplementedClusterServiceServer
}

func (m *mockClusterService) GetClusterState(ctx context.Context, req *clusterpb.Empty) (*clusterpb.ClusterState, error) {
	return &clusterpb.ClusterState{
		Epoch: 1,
		Nodes: []*clusterpb.NodeInfo{
			{
				Id:      "mock-node",
				Address: "localhost:9999",
				Status:  clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
			},
		},
	}, nil
}

func (m *mockClusterService) Join(ctx context.Context, req *clusterpb.JoinRequest) (*clusterpb.JoinResponse, error) {
	return &clusterpb.JoinResponse{
		Success: true,
		Epoch:   1,
	}, nil
}

// TestCoordinator_SyncWithNodeSuccess tests successful sync with node
func TestCoordinator_SyncWithNodeSuccess(t *testing.T) {
	// Create mock node server
	_, cleanup := createMockClusterServer(t, "localhost:9327")
	defer cleanup()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	config := &Config{
		Enabled:            true,
		MyNodeID:           "test-node",
		ClusterAddr:        "localhost:9328",
		ListenAddr:         "localhost:8328",
		Nodes:              []string{"localhost:9327"},
		RingPartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Should succeed with mock node
	err = coord.Start(ctx)
	require.NoError(t, err)
	defer coord.Stop()

	// Verify node from node was added
	nodes := coord.GetRing().GetAllNodes()
	foundMockNode := false
	for _, node := range nodes {
		if node.ID == "mock-node" {
			foundMockNode = true
			break
		}
	}
	assert.True(t, foundMockNode, "Mock node from node should be in ring")
}
