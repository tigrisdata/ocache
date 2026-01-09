package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tigrisdata/ocache/coordinator"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"github.com/tigrisdata/ocache/coordinator/ring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CoordinatorTestNode represents a coordinator node for testing
type CoordinatorTestNode struct {
	NodeID      string
	Coordinator *coordinator.Coordinator
	grpcServer  *grpc.Server
	ClusterConn *grpc.ClientConn
	ClusterSvc  clusterpb.ClusterServiceClient
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
	stopped     bool
}

// Stop stops the coordinator node
func (n *CoordinatorTestNode) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stopped {
		return nil
	}

	// Close the cluster client connection first (before stopping coordinator)
	if n.ClusterConn != nil {
		n.ClusterConn.Close()
	}

	// Stop the coordinator gracefully - this will:
	// 1. Stop ring manager (unregister from ring, broadcast leave)
	// 2. Stop memberlist KV
	// The coordinator handles its own shutdown order correctly.
	if n.Coordinator != nil {
		if err := n.Coordinator.Stop(); err != nil {
			return err
		}
	}

	// Stop gRPC server after coordinator (it might still be handling requests during unregister)
	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
	}

	// Cancel context last - this is just for cleanup, not for triggering shutdown
	if n.cancel != nil {
		n.cancel()
	}

	n.stopped = true
	return nil
}

// IsRunning checks if the node is running
func (n *CoordinatorTestNode) IsRunning() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return !n.stopped
}

// CoordinatorTestHarness provides a simplified harness for testing coordinator clustering
type CoordinatorTestHarness struct {
	T         *testing.T
	Nodes     map[string]*CoordinatorTestNode
	BasePort  int
	NodeCount int
	mu        sync.RWMutex
}

// NewCoordinatorTestHarness creates a new coordinator test harness
func NewCoordinatorTestHarness(t *testing.T, nodeCount int) *CoordinatorTestHarness {
	return &CoordinatorTestHarness{
		T:         t,
		Nodes:     make(map[string]*CoordinatorTestNode),
		BasePort:  27000, // Use high port range
		NodeCount: nodeCount,
	}
}

// StartNode starts a coordinator node
func (h *CoordinatorTestHarness) StartNode(nodeIndex int) (*CoordinatorTestNode, error) {
	nodeID := fmt.Sprintf("test-node-%d", nodeIndex+1)
	memberlistPort := h.BasePort + nodeIndex                 // Memberlist gossip port
	listenPort := h.BasePort + 1000 + nodeIndex              // gRPC service port (cache + cluster RPCs)
	clusterAddr := fmt.Sprintf("0.0.0.0:%d", memberlistPort) // Memberlist requires IP, not hostname
	listenAddr := fmt.Sprintf("localhost:%d", listenPort)

	// Create temporary directory for this node
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("ocache-cluster-test-node-%d-*", nodeIndex))
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Build seed list (memberlist addresses of other nodes)
	var seeds []string
	for i := 0; i < h.NodeCount; i++ {
		seedPort := h.BasePort + i
		// Seeds use 127.0.0.1 so nodes can reach each other
		seedAddr := fmt.Sprintf("127.0.0.1:%d", seedPort)
		seeds = append(seeds, seedAddr)
	}

	// Create coordinator config with dskit ring
	config := &coordinator.Config{
		Enabled:     true,
		MyNodeID:    nodeID,
		ClusterAddr: clusterAddr, // For memberlist gossip
		ListenAddr:  listenAddr,  // For gRPC (cache ops + cluster topology)
		Seeds:       seeds,
		DiskPath:    tmpDir,
		LifecyclerConfig: ring.LifecyclerConfig{
			NumTokens:            128,                    // Fewer tokens for faster testing
			ObservePeriod:        100 * time.Millisecond, // Very fast observe for testing
			MinReadyDuration:     0,                      // No minimum ready duration for testing
			UnregisterOnShutdown: true,                   // Remove from ring on shutdown for clean departure
			RingConfig: ring.Config{
				HeartbeatPeriod:  100 * time.Millisecond, // Fast heartbeat for testing
				HeartbeatTimeout: 10 * time.Second,       // Longer timeout to account for gossip delay
			},
		},
		Registerer: prometheus.NewRegistry(), // Use fresh registry to avoid duplicate registration
	}

	// Create coordinator
	coord, err := coordinator.New(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordinator: %w", err)
	}

	// Create node context
	ctx, cancel := context.WithCancel(context.Background())

	// Start coordinator
	if err := coord.Start(ctx); err != nil {
		coord.Stop()
		cancel()
		return nil, fmt.Errorf("failed to start coordinator: %w", err)
	}

	// Start gRPC server with ClusterService on ListenAddr
	grpcServer := grpc.NewServer()
	clusterpb.RegisterClusterServiceServer(grpcServer, coord)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		coord.Stop()
		cancel()
		return nil, fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	// Start gRPC server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			// Server stopped - this is expected during shutdown
		}
	}()

	// Give the server a moment to start accepting connections
	time.Sleep(100 * time.Millisecond)

	// Create cluster client connection to ListenAddr (where ClusterService is registered)
	// Use a context with timeout instead of deprecated grpc.WithTimeout
	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dialCancel()

	conn, err := grpc.DialContext(dialCtx, listenAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		grpcServer.GracefulStop()
		coord.Stop()
		cancel()
		return nil, fmt.Errorf("failed to connect to cluster service: %w", err)
	}

	node := &CoordinatorTestNode{
		NodeID:      nodeID,
		Coordinator: coord,
		grpcServer:  grpcServer,
		ClusterConn: conn,
		ClusterSvc:  clusterpb.NewClusterServiceClient(conn),
		ctx:         ctx,
		cancel:      cancel,
	}

	h.mu.Lock()
	h.Nodes[nodeID] = node
	h.mu.Unlock()

	return node, nil
}

// StartAllNodes starts all nodes
func (h *CoordinatorTestHarness) StartAllNodes() error {
	for i := 0; i < h.NodeCount; i++ {
		if _, err := h.StartNode(i); err != nil {
			return fmt.Errorf("failed to start node %d: %w", i, err)
		}

		// Add delay between starts
		if i < h.NodeCount-1 {
			time.Sleep(200 * time.Millisecond)
		}
	}

	return nil
}

// StopAllNodes stops all nodes with proper cleanup
func (h *CoordinatorTestHarness) StopAllNodes() {
	h.mu.Lock()
	defer h.mu.Unlock()

	var wg sync.WaitGroup
	for nodeID, node := range h.Nodes {
		wg.Add(1)
		go func(id string, n *CoordinatorTestNode) {
			defer wg.Done()
			if err := n.Stop(); err != nil {
				h.T.Logf("Failed to stop node %s: %v", id, err)
			}
		}(nodeID, node)
	}

	// Wait for all nodes to stop with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All nodes stopped successfully
	case <-time.After(10 * time.Second):
		h.T.Log("Timeout waiting for nodes to stop")
	}

	// Clear the nodes map
	h.Nodes = make(map[string]*CoordinatorTestNode)
}

// GetTopology gets topology from a node
func (h *CoordinatorTestHarness) GetTopology(nodeID string) (*clusterpb.ClusterTopology, error) {
	h.mu.RLock()
	node, exists := h.Nodes[nodeID]
	h.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return node.ClusterSvc.GetClusterTopology(ctx, &clusterpb.Empty{})
}

// GetClusterState gets cluster state from a node
func (h *CoordinatorTestHarness) GetClusterState(nodeID string) (*clusterpb.ClusterState, error) {
	h.mu.RLock()
	node, exists := h.Nodes[nodeID]
	h.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return node.ClusterSvc.GetClusterState(ctx, &clusterpb.Empty{})
}

// WaitForConvergence waits for topology to converge
func (h *CoordinatorTestHarness) WaitForConvergence(timeout time.Duration) error {
	start := time.Now()

	for {
		if time.Since(start) > timeout {
			return fmt.Errorf("topology did not converge within %v", timeout)
		}

		if h.IsConverged() {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// IsConverged checks if all nodes have the same view
func (h *CoordinatorTestHarness) IsConverged() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.Nodes) == 0 {
		return false
	}

	// Check expected node count
	expectedNodes := 0
	expectedNodeIDs := make(map[string]struct{})
	for nodeID, node := range h.Nodes {
		if node.IsRunning() {
			expectedNodes++
			expectedNodeIDs[nodeID] = struct{}{}
		}
	}

	if expectedNodes == 0 {
		return false
	}

	// Check each node's view
	for nodeID, node := range h.Nodes {
		if !node.IsRunning() {
			continue
		}

		topology, err := h.GetTopology(nodeID)
		if err != nil {
			return false
		}

		// Check node count
		if len(topology.Nodes) != expectedNodes {
			return false
		}

		// Check that all expected nodes are present
		foundNodeIDs := make(map[string]struct{})
		for _, topologyNode := range topology.Nodes {
			foundNodeIDs[topologyNode.Id] = struct{}{}
		}

		for expectedID := range expectedNodeIDs {
			if _, found := foundNodeIDs[expectedID]; !found {
				return false
			}
		}

		// Check that all nodes are ACTIVE
		for _, topologyNode := range topology.Nodes {
			if topologyNode.Status != clusterpb.NodeStatus_NODE_STATUS_ACTIVE {
				return false
			}
		}
	}

	return true
}

// Cleanup cleans up the test harness
func (h *CoordinatorTestHarness) Cleanup() {
	h.StopAllNodes()
}
