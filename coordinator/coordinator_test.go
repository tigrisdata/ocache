package coordinator

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
)

func TestCoordinator_New(t *testing.T) {
	config := &Config{
		Enabled:           true,
		MyNodeID:          "test-node",
		ClusterAddr:       "localhost:9090",
		PartitionCount:    16384,
		HeartbeatInterval: 5,
		FailureThreshold:  3,
	}

	coord, err := New(config)
	require.NoError(t, err)
	assert.NotNil(t, coord)
	assert.Equal(t, "test-node", coord.GetLocalNodeID())
	assert.NotNil(t, coord.GetRing())
	assert.NotNil(t, coord.GetRouter())
}

// TestCoordinator_StartWithInvalidAddress tests invalid address validation
func TestCoordinator_StartWithInvalidAddress(t *testing.T) {
	config := &Config{
		Enabled:        true,
		MyNodeID:       "test-node",
		ClusterAddr:    "invalid-address",
		PartitionCount: 1024,
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
		Enabled:        true,
		MyNodeID:       "node1",
		ClusterAddr:    "localhost:9324",
		PartitionCount: 1024,
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
		Enabled:        true,
		MyNodeID:       "node2",
		ClusterAddr:    "localhost:9324", // Same port
		PartitionCount: 1024,
	}

	coord2, err := New(config2)
	require.NoError(t, err)

	err = coord2.Start(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to listen")
}

func TestCoordinator_JoinAndHeartbeat(t *testing.T) {
	config := &Config{
		Enabled:           true,
		MyNodeID:          "test-node",
		ClusterAddr:       "localhost:9091",
		PartitionCount:    1024,
		HeartbeatInterval: 1,
		FailureThreshold:  2,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Test Join RPC
	ctx := context.Background()
	joinReq := &pb.JoinRequest{
		NodeId:  "new-node",
		Address: "localhost:9092",
	}

	joinResp, err := coord.Join(ctx, joinReq)
	require.NoError(t, err)
	assert.True(t, joinResp.Success)
	assert.Equal(t, uint64(2), joinResp.Epoch)

	// Verify node was added
	nodes := coord.GetRing().GetAllNodes()
	assert.Len(t, nodes, 2)

	// Test Heartbeat RPC
	hbReq := &pb.HeartbeatRequest{
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

// TestCoordinator_JoinClusterFailure tests that joinCluster returns error when all nodes fail
func TestCoordinator_JoinClusterFailure(t *testing.T) {
	config := &Config{
		Enabled:           true,
		MyNodeID:          "test-node",
		ClusterAddr:       "localhost:19200",
		Nodes:             []string{"invalid-node1:9999", "invalid-node2:9999"}, // Invalid nodes
		PartitionCount:    10,
		HeartbeatInterval: 1,
		FailureThreshold:  2,
	}

	coord, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, coord)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should fail because we can't sync with any nodes
	err = coord.Start(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to sync with any node")
}

// TestCoordinator_JoinClusterWithNodes tests joining cluster with nodes
func TestCoordinator_JoinClusterWithNodes(t *testing.T) {
	// Start a node
	nodeConfig := &Config{
		Enabled:        true,
		MyNodeID:       "node1",
		ClusterAddr:    "localhost:9301",
		PartitionCount: 1024,
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
		Enabled:        true,
		MyNodeID:       "node2",
		ClusterAddr:    "localhost:9302",
		Nodes:          []string{"localhost:9301"},
		PartitionCount: 1024,
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
		Enabled:        true,
		MyNodeID:       "test-node",
		ClusterAddr:    "localhost:9093",
		PartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Add a node
	_, err = coord.Join(ctx, &pb.JoinRequest{
		NodeId:  "leaving-node",
		Address: "localhost:9094",
	})
	require.NoError(t, err)

	// Leave the cluster
	leaveResp, err := coord.Leave(ctx, &pb.LeaveRequest{
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
		Enabled:        true,
		MyNodeID:       "test-node",
		ClusterAddr:    "localhost:9303",
		PartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Send heartbeat with newer epoch
	hbReq := &pb.HeartbeatRequest{
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
		Enabled:           true,
		MyNodeID:          "test-node",
		ClusterAddr:       "localhost:9306",
		PartitionCount:    1024,
		HeartbeatInterval: 1,
		FailureThreshold:  2,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Add a node
	ctx := context.Background()
	_, err = coord.Join(ctx, &pb.JoinRequest{
		NodeId:  "timeout-node",
		Address: "localhost:9307",
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
		Enabled:        true,
		MyNodeID:       "test-node",
		ClusterAddr:    "localhost:9095",
		PartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Add some nodes
	_, err = coord.Join(ctx, &pb.JoinRequest{
		NodeId:  "node1",
		Address: "localhost:9096",
	})
	require.NoError(t, err)

	_, err = coord.Join(ctx, &pb.JoinRequest{
		NodeId:  "node2",
		Address: "localhost:9097",
	})
	require.NoError(t, err)

	// Get cluster state
	state, err := coord.GetClusterState(ctx, &pb.Empty{})
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

func TestCoordinator_FailureDetection(t *testing.T) {
	config := &Config{
		Enabled:           true,
		MyNodeID:          "test-node",
		ClusterAddr:       "localhost:9098",
		PartitionCount:    1024,
		HeartbeatInterval: 1,
		FailureThreshold:  2,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Add a node
	ctx := context.Background()
	_, err = coord.Join(ctx, &pb.JoinRequest{
		NodeId:  "failing-node",
		Address: "localhost:9099",
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

	clusterState, err := coord.GetClusterState(ctx, &pb.Empty{})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), clusterState.Epoch)
	assert.Len(t, clusterState.Nodes, 2)

	var failingNode *pb.NodeInfo
	for _, node := range clusterState.Nodes {
		if node.Id == "failing-node" {
			failingNode = node
			break
		}
	}

	assert.Equal(t, "failing-node", failingNode.Id)
	assert.Equal(t, pb.NodeStatus_NODE_STATUS_DOWN, failingNode.Status)
}

// TestCoordinator_GetLocalNodeID tests getting local node ID
func TestCoordinator_GetLocalNodeID(t *testing.T) {
	config := &Config{
		Enabled:        true,
		MyNodeID:       "my-special-node",
		ClusterAddr:    "localhost:9325",
		PartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	assert.Equal(t, "my-special-node", coord.GetLocalNodeID())
}

func TestCoordinator_IsLocal(t *testing.T) {
	config := &Config{
		Enabled:        true,
		MyNodeID:       "local-node",
		ClusterAddr:    "localhost:9099",
		PartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Add another node
	ctx := context.Background()
	_, err = coord.Join(ctx, &pb.JoinRequest{
		NodeId:  "remote-node",
		Address: "localhost:9100",
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
		Enabled:        true,
		MyNodeID:       "test-node",
		ClusterAddr:    "localhost:9308",
		PartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	// Add more nodes
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err = coord.Join(ctx, &pb.JoinRequest{
			NodeId:  fmt.Sprintf("node-%d", i),
			Address: fmt.Sprintf("localhost:930%d", 9+i),
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
		Enabled:        true,
		MyNodeID:       "test-node",
		ClusterAddr:    "localhost:9313",
		PartitionCount: 1024,
	}

	coord, err := New(config)
	require.NoError(t, err)

	ctx := context.Background()
	done := make(chan bool)

	// Concurrent joins
	for i := 0; i < 10; i++ {
		go func(id int) {
			_, err := coord.Join(ctx, &pb.JoinRequest{
				NodeId:  fmt.Sprintf("node-%d", id),
				Address: fmt.Sprintf("localhost:931%d", 4+id),
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
			_, err := coord.Leave(ctx, &pb.LeaveRequest{
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
		Enabled:        true,
		MyNodeID:       "test-node",
		ClusterAddr:    "localhost:9326",
		PartitionCount: 1024,
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
	pb.RegisterClusterServiceServer(server, &mockClusterService{})

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
	pb.UnimplementedClusterServiceServer
}

func (m *mockClusterService) GetClusterState(ctx context.Context, req *pb.Empty) (*pb.ClusterState, error) {
	return &pb.ClusterState{
		Epoch: 1,
		Nodes: []*pb.NodeInfo{
			{
				Id:      "mock-node",
				Address: "localhost:9999",
				Status:  pb.NodeStatus_NODE_STATUS_ACTIVE,
			},
		},
	}, nil
}

func (m *mockClusterService) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	return &pb.JoinResponse{
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
		Enabled:        true,
		MyNodeID:       "test-node",
		ClusterAddr:    "localhost:9328",
		Nodes:          []string{"localhost:9327"},
		PartitionCount: 1024,
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
