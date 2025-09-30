package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tigrisdata/ocache/common/hash"
	"github.com/tigrisdata/ocache/coordinator"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CoordinatorTestNode represents a coordinator node for testing
type CoordinatorTestNode struct {
	NodeID      string
	Coordinator *coordinator.Coordinator
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

	if n.cancel != nil {
		n.cancel()
	}

	if n.Coordinator != nil {
		if err := n.Coordinator.Stop(); err != nil {
			return err
		}
	}

	if n.ClusterConn != nil {
		n.ClusterConn.Close()
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
	clusterPort := h.BasePort + nodeIndex
	listenPort := h.BasePort + 1000 + nodeIndex // Service port
	clusterAddr := fmt.Sprintf("localhost:%d", clusterPort)
	listenAddr := fmt.Sprintf("localhost:%d", listenPort)

	// Build seed list - for first node, use empty seeds
	// For subsequent nodes, use addresses of existing nodes
	var seeds []string
	h.mu.RLock()
	numExistingNodes := len(h.Nodes)
	h.mu.RUnlock()

	if numExistingNodes > 0 {
		// Add addresses of other nodes (they may or may not be running)
		for i := 0; i < h.NodeCount; i++ {
			seedPort := h.BasePort + i
			seedAddr := fmt.Sprintf("localhost:%d", seedPort)
			if seedAddr != clusterAddr {
				seeds = append(seeds, seedAddr)
			}
		}
	}
	// For the first node, seeds will be empty and it will bootstrap itself

	// Create coordinator config
	config := &coordinator.Config{
		Enabled:            true,
		MyNodeID:           nodeID,
		ClusterAddr:        clusterAddr,
		ListenAddr:         listenAddr,
		Nodes:              seeds,
		RingPartitionCount: hash.DefaultPartitionCount,
		HeartbeatInterval:  1, // 1 second for faster testing
		FailureThreshold:   3,
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
		// Even though Start failed, we need to call Stop() to cleanup
		// any resources that were successfully initialized (like the gRPC server)
		coord.Stop() // This is safe even if Start() partially failed
		cancel()
		return nil, fmt.Errorf("failed to start coordinator: %w", err)
	}

	// Wait for startup
	time.Sleep(500 * time.Millisecond)

	// Create cluster client connection
	conn, err := grpc.Dial(clusterAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		coord.Stop()
		cancel()
		return nil, fmt.Errorf("failed to connect to cluster service: %w", err)
	}

	node := &CoordinatorTestNode{
		NodeID:      nodeID,
		Coordinator: coord,
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

	var referenceTopology *clusterpb.ClusterTopology
	var referenceNodeID string

	// Get reference topology
	for nodeID, node := range h.Nodes {
		if !node.IsRunning() {
			continue
		}

		topology, err := h.GetTopology(nodeID)
		if err != nil {
			return false
		}

		referenceTopology = topology
		referenceNodeID = nodeID
		break
	}

	if referenceTopology == nil {
		return false
	}

	// Check expected node count
	expectedNodes := 0
	for _, node := range h.Nodes {
		if node.IsRunning() {
			expectedNodes++
		}
	}

	if len(referenceTopology.Nodes) != expectedNodes {
		return false
	}

	// Compare with other nodes
	for nodeID, node := range h.Nodes {
		if !node.IsRunning() || nodeID == referenceNodeID {
			continue
		}

		topology, err := h.GetTopology(nodeID)
		if err != nil {
			return false
		}

		// Check epoch and node count
		if topology.Epoch != referenceTopology.Epoch {
			return false
		}

		if len(topology.Nodes) != len(referenceTopology.Nodes) {
			return false
		}
	}

	return true
}

// Cleanup cleans up the test harness
func (h *CoordinatorTestHarness) Cleanup() {
	h.StopAllNodes()
}
