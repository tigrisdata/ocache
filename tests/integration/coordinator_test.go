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

	// Verify remaining nodes receive leave broadcast and remove the node
	// With graceful departure, the node is completely removed (not marked DOWN)
	// This should happen very quickly (< 1s via Leave broadcast)
	for nodeID, n := range s.harness.Nodes {
		if nodeID == nodeToStop || !n.IsRunning() {
			continue
		}

		// Wait for graceful departure to propagate (should be fast)
		var topology *clusterpb.ClusterTopology
		nodeRemoved := false
		for i := 0; i < 10; i++ {
			topology, err = s.harness.GetTopology(nodeID)
			require.NoError(s.T(), err)

			// Check if the stopped node is completely removed from topology
			nodeFound := false
			for _, node := range topology.Nodes {
				if node.Id == nodeToStop {
					nodeFound = true
					break
				}
			}

			if !nodeFound {
				nodeRemoved = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}

		require.True(s.T(), nodeRemoved, "Node %s failed to remove stopped node %s within timeout", nodeID, nodeToStop)

		// Verify topology now has only 2 nodes (departed node completely removed)
		assert.Equal(s.T(), 2, len(topology.Nodes), "Node %s should see only 2 nodes after graceful departure", nodeID)

		// Verify all remaining nodes are active
		for _, node := range topology.Nodes {
			assert.Equal(s.T(), clusterpb.NodeStatus_NODE_STATUS_ACTIVE, node.Status,
				"All remaining nodes should be active, got status %v for node %s", node.Status, node.Id)
		}
	}
}

// Test_Coordinator_GracefulNodeDeparture tests that graceful departure removes node quickly
func (s *CoordinatorSuite) Test_Coordinator_GracefulNodeDeparture() {
	// Start all 3 nodes
	err := s.harness.StartAllNodes()
	require.NoError(s.T(), err, "Failed to start nodes")

	// Wait for convergence
	err = s.harness.WaitForConvergence(10 * time.Second)
	require.NoError(s.T(), err, "Cluster did not converge")

	// Verify all nodes see each other (3 nodes total)
	for nodeID := range s.harness.Nodes {
		topology, err := s.harness.GetTopology(nodeID)
		require.NoError(s.T(), err)
		assert.Equal(s.T(), 3, len(topology.Nodes),
			"Node %s should see 3 nodes before departure", nodeID)
	}

	// Select a node to stop gracefully
	var nodeToStop string
	var remainingNodes []string
	for nodeID := range s.harness.Nodes {
		if nodeToStop == "" {
			nodeToStop = nodeID
		} else {
			remainingNodes = append(remainingNodes, nodeID)
		}
	}

	require.NotEmpty(s.T(), nodeToStop, "Should have a node to stop")
	require.Len(s.T(), remainingNodes, 2, "Should have 2 remaining nodes")

	node := s.harness.Nodes[nodeToStop]
	require.NotNil(s.T(), node)

	s.T().Logf("Stopping node %s gracefully", nodeToStop)

	// Record time before stopping
	startTime := time.Now()

	// Gracefully stop the node (this should trigger announceLeave)
	err = node.Stop()
	require.NoError(s.T(), err)

	// Verify remaining nodes quickly detect the departure
	// With graceful departure, this should happen in < 1 second
	// (not 10-20 seconds like passive detection)
	for _, nodeID := range remainingNodes {
		n := s.harness.Nodes[nodeID]
		if !n.IsRunning() {
			continue
		}

		s.T().Logf("Checking if node %s detected departure of %s", nodeID, nodeToStop)

		var topology *clusterpb.ClusterTopology
		nodeRemoved := false

		// Check every 100ms for up to 5 seconds
		// Graceful departure should complete within ~500ms
		for i := 0; i < 50; i++ {
			topology, err = s.harness.GetTopology(nodeID)
			require.NoError(s.T(), err)

			// Check if the stopped node is completely removed from ring
			nodeFound := false
			for _, topologyNode := range topology.Nodes {
				if topologyNode.Id == nodeToStop {
					nodeFound = true
					break
				}
			}

			if !nodeFound {
				nodeRemoved = true
				elapsed := time.Since(startTime)
				s.T().Logf("Node %s detected departure of %s in %v", nodeID, nodeToStop, elapsed)
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		require.True(s.T(), nodeRemoved,
			"Node %s should have removed departed node %s from ring", nodeID, nodeToStop)

		// Verify topology now has only 2 nodes
		assert.Equal(s.T(), 2, len(topology.Nodes),
			"Node %s should see only 2 nodes after graceful departure", nodeID)

		// Verify both remaining nodes are active
		for _, topologyNode := range topology.Nodes {
			assert.Equal(s.T(), clusterpb.NodeStatus_NODE_STATUS_ACTIVE, topologyNode.Status,
				"All remaining nodes should be active")
		}
	}

	// Verify graceful departure was fast (< 2 seconds)
	totalElapsed := time.Since(startTime)
	s.T().Logf("Total time for graceful departure detection: %v", totalElapsed)
	assert.Less(s.T(), totalElapsed, 2*time.Second,
		"Graceful departure should be detected quickly (not 10-20s like passive detection)")
}
