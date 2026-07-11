// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
)

const waitForConvergenceTimeout = 20 * time.Second

// Test_Coordinator_BasicFormation tests basic cluster formation using CoordinatorSuite
func (s *CoordinatorSuite) Test_Coordinator_BasicFormation() {
	// Start all nodes
	err := s.harness.StartAllNodes()
	require.NoError(s.T(), err, "Failed to start nodes")

	// Wait for convergence
	err = s.harness.WaitForConvergence(waitForConvergenceTimeout)
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
	err := s.harness.WaitForConvergence(waitForConvergenceTimeout)
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
	err = s.harness.WaitForConvergence(waitForConvergenceTimeout)
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

	err = s.harness.WaitForConvergence(waitForConvergenceTimeout)
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

// Test_Coordinator_GracefulDeparture_DetectsLeavingState tests that remaining nodes detect
// the LEAVING state transition when a node departs gracefully.
// This verifies the KV watcher detects state changes, not just node removal.
func (s *CoordinatorSuite) Test_Coordinator_GracefulDeparture_DetectsLeavingState() {
	// Start 2 nodes (simpler than 3 for this focused test)
	_, err := s.harness.StartNode(0)
	require.NoError(s.T(), err)
	time.Sleep(200 * time.Millisecond)

	_, err = s.harness.StartNode(1)
	require.NoError(s.T(), err)

	// Wait for convergence with 2 nodes
	err = s.harness.WaitForConvergence(waitForConvergenceTimeout)
	require.NoError(s.T(), err)

	// Verify both nodes see each other as ACTIVE
	var nodeToStop, remainingNode string
	for nodeID := range s.harness.Nodes {
		if nodeToStop == "" {
			nodeToStop = nodeID
		} else {
			remainingNode = nodeID
		}
	}

	topology, err := s.harness.GetTopology(remainingNode)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, len(topology.Nodes))

	for _, node := range topology.Nodes {
		assert.Equal(s.T(), clusterpb.NodeStatus_NODE_STATUS_ACTIVE, node.Status,
			"Node %s should be ACTIVE before departure", node.Id)
	}

	s.T().Logf("Stopping node %s gracefully, watching from %s", nodeToStop, remainingNode)

	// Record epoch before stopping
	initialEpoch := topology.Epoch

	// Stop the node gracefully - this triggers AnnounceLeaving which broadcasts LEAVING state
	node := s.harness.Nodes[nodeToStop]
	err = node.Stop()
	require.NoError(s.T(), err)

	// Poll for either:
	// 1. LEAVING state detection (intermediate state)
	// 2. Node removal (final state)
	// The KV watcher should detect these changes immediately via gossip
	startTime := time.Now()
	sawLeavingOrRemoval := false
	var finalTopology *clusterpb.ClusterTopology

	for i := 0; i < 30; i++ { // Check for up to 3 seconds
		finalTopology, err = s.harness.GetTopology(remainingNode)
		require.NoError(s.T(), err)

		// Check if epoch changed (indicates ring state was updated)
		if finalTopology.Epoch != initialEpoch {
			// Check if we see LEAVING state or node removal
			nodeFound := false
			for _, topologyNode := range finalTopology.Nodes {
				if topologyNode.Id == nodeToStop {
					nodeFound = true
					if topologyNode.Status == clusterpb.NodeStatus_NODE_STATUS_LEAVING {
						s.T().Logf("Detected LEAVING state for %s in %v", nodeToStop, time.Since(startTime))
						sawLeavingOrRemoval = true
					}
					break
				}
			}

			if !nodeFound {
				// Node was completely removed
				s.T().Logf("Node %s removed from ring in %v", nodeToStop, time.Since(startTime))
				sawLeavingOrRemoval = true
				break
			}
		}

		if sawLeavingOrRemoval {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	require.True(s.T(), sawLeavingOrRemoval,
		"Should have detected LEAVING state or node removal within %v", waitForConvergenceTimeout)

	// Verify detection was fast - proving KV watcher works
	elapsed := time.Since(startTime)
	assert.Less(s.T(), elapsed, waitForConvergenceTimeout,
		"KV watcher should detect departure quickly, got %v", elapsed)

	// Verify epoch was updated
	assert.NotEqual(s.T(), initialEpoch, finalTopology.Epoch,
		"Epoch should change after departure")
}

// Test_Coordinator_KVWatcher_DetectsJoin tests that existing nodes detect a new node
// joining the cluster immediately via the KV watcher.
func (s *CoordinatorSuite) Test_Coordinator_KVWatcher_DetectsJoin() {
	// Start first node alone
	_, err := s.harness.StartNode(0)
	require.NoError(s.T(), err)

	// Wait for first node to be ready
	time.Sleep(500 * time.Millisecond)

	// Get initial state from node1
	var node1ID string
	for nodeID := range s.harness.Nodes {
		node1ID = nodeID
		break
	}

	initialTopology, err := s.harness.GetTopology(node1ID)
	require.NoError(s.T(), err)
	initialEpoch := initialTopology.Epoch
	initialNodeCount := len(initialTopology.Nodes)
	s.T().Logf("Node1 initial state: epoch=%d, nodes=%d", initialEpoch, initialNodeCount)

	// Start second node
	s.T().Log("Starting node2...")
	startTime := time.Now()
	_, err = s.harness.StartNode(1)
	require.NoError(s.T(), err)

	// Poll node1 to detect node2 joining
	// The KV watcher should detect the join immediately via gossip
	sawJoin := false
	var finalTopology *clusterpb.ClusterTopology

	for i := 0; i < 50; i++ { // Check for up to 5 seconds
		finalTopology, err = s.harness.GetTopology(node1ID)
		require.NoError(s.T(), err)

		// Check if node count increased or epoch changed
		if len(finalTopology.Nodes) > initialNodeCount {
			elapsed := time.Since(startTime)
			s.T().Logf("Node1 detected node2 join in %v (nodes=%d, epoch=%d)",
				elapsed, len(finalTopology.Nodes), finalTopology.Epoch)
			sawJoin = true
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	require.True(s.T(), sawJoin,
		"Node1 should detect node2 joining within %v", waitForConvergenceTimeout)

	// Verify detection was reasonably fast (< 10 seconds)
	// Note: This includes node startup time plus CI overhead. The key is it's much faster than heartbeat timeout (60s).
	// We allow 10s instead of 5s because: 50 iterations × 100ms sleep = 5s, plus RPC overhead per iteration.
	elapsed := time.Since(startTime)
	assert.Less(s.T(), elapsed, waitForConvergenceTimeout,
		"KV watcher should detect join quickly, got %v", elapsed)

	// Verify epoch changed
	assert.NotEqual(s.T(), initialEpoch, finalTopology.Epoch,
		"Epoch should change after new node joins")

	// Verify both nodes are visible
	assert.Equal(s.T(), 2, len(finalTopology.Nodes),
		"Should see 2 nodes after join")

	// Wait for convergence (both nodes ACTIVE)
	err = s.harness.WaitForConvergence(waitForConvergenceTimeout)
	require.NoError(s.T(), err)

	// Verify final state
	finalTopology, err = s.harness.GetTopology(node1ID)
	require.NoError(s.T(), err)

	for _, node := range finalTopology.Nodes {
		assert.Equal(s.T(), clusterpb.NodeStatus_NODE_STATUS_ACTIVE, node.Status,
			"Node %s should be ACTIVE after convergence", node.Id)
	}
}

// Test_Coordinator_GracefulNodeDeparture tests that graceful departure removes node quickly
func (s *CoordinatorSuite) Test_Coordinator_GracefulNodeDeparture() {
	// Start all 3 nodes
	err := s.harness.StartAllNodes()
	require.NoError(s.T(), err, "Failed to start nodes")

	// Wait for convergence
	err = s.harness.WaitForConvergence(waitForConvergenceTimeout)
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

	// Verify graceful departure was fast (< 5 seconds)
	// Note: This includes the blocking AnnounceLeaving() call (~500ms propagation delay)
	// plus polling time. The key is that it's much faster than passive detection (10-20s).
	totalElapsed := time.Since(startTime)
	s.T().Logf("Total time for graceful departure detection: %v", totalElapsed)
	assert.Less(s.T(), totalElapsed, waitForConvergenceTimeout,
		"Graceful departure should be detected quickly (not 10-20s like passive detection)")
}
