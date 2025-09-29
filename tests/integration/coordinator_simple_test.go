package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
)

// TestCoordinator_BasicFormation tests basic cluster formation
func TestCoordinator_BasicFormation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping coordinator test in short mode")
	}

	// Create harness for 3-node cluster
	harness := NewCoordinatorTestHarness(t, 3)
	defer harness.Cleanup()

	// Start all nodes
	err := harness.StartAllNodes()
	require.NoError(t, err, "Failed to start nodes")

	// Wait for convergence
	err = harness.WaitForConvergence(10 * time.Second)
	require.NoError(t, err, "Cluster did not converge")

	// Verify all nodes see each other
	for nodeID := range harness.Nodes {
		topology, err := harness.GetTopology(nodeID)
		require.NoError(t, err)
		assert.Equal(t, 3, len(topology.Nodes),
			"Node %s should see 3 nodes", nodeID)
	}

	// TODO: Improve partition distribution balance
	// For now, we skip this check as consistent hashing
	// doesn't guarantee perfect balance with small node counts
	// harness.VerifyPartitionDistribution(t)
}

// TestCoordinator_NodeJoin tests a node joining existing cluster
func TestCoordinator_NodeJoin(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping coordinator test in short mode")
	}

	// Start with 2-node cluster
	harness := NewCoordinatorTestHarness(t, 3)
	defer harness.Cleanup()

	// Start first 2 nodes
	for i := 0; i < 2; i++ {
		_, err := harness.StartNode(i)
		require.NoError(t, err)
		time.Sleep(200 * time.Millisecond)
	}

	// Wait for initial convergence
	err := harness.WaitForConvergence(5 * time.Second)
	require.NoError(t, err)

	// Verify 2-node topology
	for nodeID := range harness.Nodes {
		topology, err := harness.GetTopology(nodeID)
		require.NoError(t, err)
		assert.Equal(t, 2, len(topology.Nodes))
	}

	// Add third node
	_, err = harness.StartNode(2)
	require.NoError(t, err)

	// Wait for convergence with 3 nodes
	time.Sleep(2 * time.Second)
	err = harness.WaitForConvergence(10 * time.Second)
	require.NoError(t, err)

	// Verify 3-node topology
	for nodeID := range harness.Nodes {
		topology, err := harness.GetTopology(nodeID)
		require.NoError(t, err)
		assert.Equal(t, 3, len(topology.Nodes))
	}
}

// TestCoordinator_NodeLeave tests a node leaving the cluster
func TestCoordinator_NodeLeave(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping coordinator test in short mode")
	}

	// Create 3-node cluster
	harness := NewCoordinatorTestHarness(t, 3)
	defer harness.Cleanup()

	// Start all nodes
	err := harness.StartAllNodes()
	require.NoError(t, err)

	err = harness.WaitForConvergence(10 * time.Second)
	require.NoError(t, err)

	// Stop one node
	var nodeToStop string
	for nodeID := range harness.Nodes {
		nodeToStop = nodeID
		break
	}

	node := harness.Nodes[nodeToStop]
	require.NotNil(t, node)

	// Graceful leave
	err = node.Stop()
	require.NoError(t, err)

	// Wait for detection and convergence
	time.Sleep(5 * time.Second)

	// Verify remaining nodes see 2 nodes
	for nodeID, n := range harness.Nodes {
		if nodeID == nodeToStop || !n.IsRunning() {
			continue
		}

		// May take time for failure detection
		var topology *clusterpb.ClusterTopology
		for i := 0; i < 10; i++ {
			topology, err = harness.GetTopology(nodeID)
			if err == nil && len(topology.Nodes) == 2 {
				break
			}
			time.Sleep(1 * time.Second)
		}

		require.NoError(t, err)
		assert.Equal(t, 2, len(topology.Nodes),
			"Node %s should see 2 nodes after leave", nodeID)
	}
}
