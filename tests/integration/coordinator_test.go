package integration

import (
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
)

// Test_Coordinator_BasicFormation tests basic cluster formation using CoordinatorSuite
func (s *CoordinatorSuite) Test_Coordinator_BasicFormation() {
	// Start all nodes
	err := s.harness.StartAllNodes()
	require.NoError(s.T(), err, "Failed to start nodes")

	// Wait for convergence
	err = s.harness.WaitForConvergence(10 * time.Second)
	require.NoError(s.T(), err, "Cluster did not converge")

	// Verify all nodes see each other
	for nodeID := range s.harness.Nodes {
		topology, err := s.harness.GetTopology(nodeID)
		require.NoError(s.T(), err)
		assert.Equal(s.T(), 3, len(topology.Nodes),
			"Node %s should see 3 nodes", nodeID)
	}
}

// Test_Coordinator_NodeJoin tests a node joining existing cluster using CoordinatorSuite
func (s *CoordinatorSuite) Test_Coordinator_NodeJoin() {
	// Start first 2 nodes
	for i := 0; i < 2; i++ {
		_, err := s.harness.StartNode(i)
		require.NoError(s.T(), err)
		time.Sleep(200 * time.Millisecond)
	}

	// Wait for initial convergence
	err := s.harness.WaitForConvergence(5 * time.Second)
	require.NoError(s.T(), err)

	// Verify 2-node topology
	for nodeID := range s.harness.Nodes {
		topology, err := s.harness.GetTopology(nodeID)
		require.NoError(s.T(), err)
		assert.Equal(s.T(), 2, len(topology.Nodes))
	}

	// Add third node
	_, err = s.harness.StartNode(2)
	require.NoError(s.T(), err)

	// Wait for convergence with 3 nodes
	time.Sleep(2 * time.Second)
	err = s.harness.WaitForConvergence(10 * time.Second)
	require.NoError(s.T(), err)

	// Verify 3-node topology
	for nodeID := range s.harness.Nodes {
		topology, err := s.harness.GetTopology(nodeID)
		require.NoError(s.T(), err)
		assert.Equal(s.T(), 3, len(topology.Nodes))
	}
}

// Test_Coordinator_NodeLeave tests a node leaving the cluster using CoordinatorSuite
func (s *CoordinatorSuite) Test_Coordinator_NodeLeave() {
	// Start all nodes
	err := s.harness.StartAllNodes()
	require.NoError(s.T(), err)

	err = s.harness.WaitForConvergence(10 * time.Second)
	require.NoError(s.T(), err)

	// Stop one node
	var nodeToStop string
	for nodeID := range s.harness.Nodes {
		nodeToStop = nodeID
		break
	}

	node := s.harness.Nodes[nodeToStop]
	require.NotNil(s.T(), node)

	// Graceful leave
	err = node.Stop()
	require.NoError(s.T(), err)

	// Verify remaining nodes detect the stopped node as down
	// Note: Failure detection runs every 10 seconds, and requires 3 missed heartbeats (at 1s intervals)
	// So we need to wait at least 10-13 seconds for detection
	// The stopped node will remain in the topology but marked as "Down"
	for nodeID, n := range s.harness.Nodes {
		if nodeID == nodeToStop || !n.IsRunning() {
			continue
		}

		// Wait for failure detection (runs every 10s) plus some buffer
		var topology *clusterpb.ClusterTopology
		detected := false
		for i := 0; i < 20; i++ {
			topology, err = s.harness.GetTopology(nodeID)
			require.NoError(s.T(), err)

			// Check if the stopped node is marked as down
			for _, node := range topology.Nodes {
				if node.Id == nodeToStop && node.Status == clusterpb.NodeStatus_NODE_STATUS_DOWN {
					detected = true
					break
				}
			}

			if detected {
				break
			}
			time.Sleep(1 * time.Second)
		}

		require.True(s.T(), detected, "Node %s failed to detect stopped node %s as down within timeout", nodeID, nodeToStop)

		// Verify topology still has 3 nodes but one is marked as down
		assert.Equal(s.T(), 3, len(topology.Nodes), "Node %s should still see 3 nodes in topology", nodeID)

		// Count active nodes
		activeCount := 0
		for _, node := range topology.Nodes {
			if node.Status == clusterpb.NodeStatus_NODE_STATUS_ACTIVE {
				activeCount++
			}
		}
		assert.Equal(s.T(), 2, activeCount, "Node %s should see 2 active nodes", nodeID)
	}
}
