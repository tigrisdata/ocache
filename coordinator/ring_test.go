package coordinator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRing_AddNode(t *testing.T) {
	ring, err := NewRing(1024, "local")
	require.NoError(t, err)

	// Add first node
	err = ring.AddNode("node1", "localhost:7000", "localhost:9000")
	assert.NoError(t, err)

	// Verify node was added
	nodes := ring.GetAllNodes()
	assert.Len(t, nodes, 1)
	assert.Equal(t, "node1", nodes[0].ID)
	assert.Equal(t, "localhost:9000", nodes[0].Address)
	assert.Equal(t, NodeStatusActive, nodes[0].Status)
	assert.True(t, nodes[0].Available)

	// Add second node
	err = ring.AddNode("node2", "localhost:7001", "localhost:9001")
	assert.NoError(t, err)

	nodes = ring.GetAllNodes()
	assert.Len(t, nodes, 2)

	// Try adding duplicate node
	err = ring.AddNode("node1", "localhost:7000", "localhost:9000")
	assert.Error(t, err)
}

func TestRing_RemoveNode(t *testing.T) {
	ring, err := NewRing(1024, "local")
	require.NoError(t, err)

	// Add nodes
	ring.AddNode("node1", "localhost:7000", "localhost:9000")
	ring.AddNode("node2", "localhost:7001", "localhost:9001")

	// Remove node
	err = ring.RemoveNode("node1")
	assert.NoError(t, err)

	nodes := ring.GetAllNodes()
	assert.Len(t, nodes, 1)
	assert.Equal(t, "node2", nodes[0].ID)

	// Try removing non-existent node
	err = ring.RemoveNode("node3")
	assert.Error(t, err)
}

func TestRing_GetNode(t *testing.T) {
	ring, err := NewRing(1024, "local")
	require.NoError(t, err)

	// Add nodes
	ring.AddNode("node1", "localhost:7000", "localhost:9000")
	ring.AddNode("node2", "localhost:7001", "localhost:9001")
	ring.AddNode("node3", "localhost:7002", "localhost:9002")

	// Test key distribution
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	nodeDistribution := make(map[string]int)

	for _, key := range keys {
		node, err := ring.GetNode(key)
		assert.NoError(t, err)
		assert.NotNil(t, node)
		nodeDistribution[node.ID]++
	}

	// Verify keys are distributed across nodes
	assert.True(t, len(nodeDistribution) > 1, "Keys should be distributed across multiple nodes")
}

func TestRing_IsLocal(t *testing.T) {
	ring, err := NewRing(1024, "node1")
	require.NoError(t, err)

	// Add nodes
	ring.AddNode("node1", "localhost:7000", "localhost:9000")
	ring.AddNode("node2", "localhost:7001", "localhost:9001")

	// Test various keys
	localCount := 0
	remoteCount := 0

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		if ring.IsLocal(key) {
			localCount++
		} else {
			remoteCount++
		}
	}

	// Both should have some keys
	assert.Greater(t, localCount, 0, "Should have some local keys")
	assert.Greater(t, remoteCount, 0, "Should have some remote keys")
}

func TestRing_UpdateNodeStatus(t *testing.T) {
	ring, err := NewRing(1024, "local")
	require.NoError(t, err)

	// Add node
	ring.AddNode("node1", "localhost:7000", "localhost:9000")

	// Update status to down
	err = ring.UpdateNodeStatus("node1", NodeStatusDown)
	assert.NoError(t, err)

	// Node should not be available
	assert.False(t, ring.IsNodeAvailable("node1"))

	// Try to get node for key - should fail because node is unavailable
	_, err = ring.GetNode("somekey")
	assert.Error(t, err, "Should not return unavailable node")

	// But GetPrimaryNode should still return it
	primary, err := ring.GetPrimaryNode("somekey")
	assert.NoError(t, err)
	assert.Equal(t, "node1", primary.ID)
	assert.False(t, primary.Available)

	// Update back to active
	err = ring.UpdateNodeStatus("node1", NodeStatusActive)
	assert.NoError(t, err)

	// Node should be available again
	assert.True(t, ring.IsNodeAvailable("node1"))

	// Should work now
	node, err := ring.GetNode("somekey")
	assert.NoError(t, err)
	assert.Equal(t, "node1", node.ID)
	assert.True(t, node.Available)
}

func TestRing_GetClosestN(t *testing.T) {
	ring, err := NewRing(1024, "local")
	require.NoError(t, err)

	// Add nodes
	ring.AddNode("node1", "localhost:7000", "localhost:9000")
	ring.AddNode("node2", "localhost:7001", "localhost:9001")
	ring.AddNode("node3", "localhost:7002", "localhost:9002")

	// Get closest nodes - requesting 2 when we have 3
	nodes, err := ring.GetClosestN("testkey", 2)
	assert.NoError(t, err)
	assert.Len(t, nodes, 2)

	// Request exactly the number of nodes available
	nodes, err = ring.GetClosestN("testkey", 3)
	assert.NoError(t, err)
	assert.Len(t, nodes, 3)
}

func TestRing_EpochIncrement(t *testing.T) {
	ring, err := NewRing(1024, "local")
	require.NoError(t, err)

	initialMembershipEpoch := ring.GetEpoch()

	// Add node - should increment both epochs
	ring.AddNode("node1", "localhost:7000", "localhost:9000")
	assert.Greater(t, ring.GetEpoch(), initialMembershipEpoch)

	membershipEpoch1 := ring.GetEpoch()

	// Update status - should ONLY increment status epoch, NOT membership
	ring.UpdateNodeStatus("node1", NodeStatusDown)
	assert.Equal(t, ring.GetEpoch(), membershipEpoch1, "Membership epoch should not change on status update")

	// Update status again
	ring.UpdateNodeStatus("node1", NodeStatusActive)
	assert.Equal(t, ring.GetEpoch(), membershipEpoch1, "Membership epoch should still not change")

	// Remove node - should increment both epochs
	ring.RemoveNode("node1")
	assert.Greater(t, ring.GetEpoch(), membershipEpoch1, "Membership epoch should increment on remove")
}

func TestRing_GetNextAvailableNode(t *testing.T) {
	ring, err := NewRing(1024, "local")
	require.NoError(t, err)

	// Add multiple nodes
	ring.AddNode("node1", "localhost:7000", "localhost:9000")
	ring.AddNode("node2", "localhost:7001", "localhost:9001")
	ring.AddNode("node3", "localhost:7002", "localhost:9002")

	// Mark primary node as down
	primary, err := ring.GetPrimaryNode("testkey")
	assert.NoError(t, err)
	ring.UpdateNodeStatus(primary.ID, NodeStatusDown)

	// GetNode should fail
	_, err = ring.GetNode("testkey")
	assert.Error(t, err)

	// But GetNextAvailableNode should find an alternative
	next, err := ring.GetNextAvailableNode("testkey")
	assert.NoError(t, err)
	assert.NotEqual(t, primary.ID, next.ID)
	assert.True(t, next.Available)
}

func TestRing_AvailableNodes(t *testing.T) {
	ring, err := NewRing(1024, "local")
	require.NoError(t, err)

	// Add nodes
	ring.AddNode("node1", "localhost:7000", "localhost:9000")
	ring.AddNode("node2", "localhost:7001", "localhost:9001")
	ring.AddNode("node3", "localhost:7002", "localhost:9002")

	// All should be available initially
	available := ring.GetAvailableNodes()
	assert.Len(t, available, 3)

	// Mark one as down
	ring.UpdateNodeStatus("node2", NodeStatusDown)
	available = ring.GetAvailableNodes()
	assert.Len(t, available, 2)

	// Mark another as joining (not available)
	ring.UpdateNodeStatus("node3", NodeStatusJoining)
	available = ring.GetAvailableNodes()
	assert.Len(t, available, 1)
	assert.Equal(t, "node1", available[0].ID)
}
