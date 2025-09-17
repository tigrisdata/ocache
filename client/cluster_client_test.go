package cacheclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
)

func TestClusterClient_PartitionRouting(t *testing.T) {
	// Create a test topology
	topology := &clusterpb.ClusterTopology{
		Epoch: 1,
		Nodes: []*clusterpb.NodeInfo{
			{Id: "node1", Address: "localhost:9001", Status: clusterpb.NodeStatus_NODE_STATUS_ACTIVE},
			{Id: "node2", Address: "localhost:9002", Status: clusterpb.NodeStatus_NODE_STATUS_ACTIVE},
			{Id: "node3", Address: "localhost:9003", Status: clusterpb.NodeStatus_NODE_STATUS_ACTIVE},
		},
		RingConfig: &clusterpb.RingConfig{
			PartitionCount:    128,
			ReplicationFactor: 20,
			Load:              1.25,
		},
		PartitionOwners: []*clusterpb.PartitionOwner{},
	}

	// Generate partition ownership (simplified for testing)
	for i := 0; i < 128; i++ {
		nodeIdx := i % 3
		nodeID := topology.Nodes[nodeIdx].Id
		topology.PartitionOwners = append(topology.PartitionOwners, &clusterpb.PartitionOwner{
			PartitionId: int32(i),
			NodeId:      nodeID,
		})
	}

	// Create cluster client
	config := &ClusterClientConfig{
		PoolSizePerNode:         3,
		TopologyRefreshInterval: 30 * time.Second,
	}
	c := &ClusterClient{
		clients:         make(map[string]*ConnectionPool),
		partitionOwners: make(map[int32]string),
		config:          config,
		stopCh:          make(chan struct{}),
	}

	// Update topology
	err := c.updateTopology(topology)
	require.NoError(t, err)

	// Verify ring was created
	assert.NotNil(t, c.ring)
	assert.Equal(t, uint64(1), c.topologyEpoch)

	// Test partition calculation is consistent
	testKeys := []string{"key1", "key2", "key3", "test-key", "another-key"}
	for _, key := range testKeys {
		partition1 := c.getPartitionForKey(key)
		partition2 := c.getPartitionForKey(key)
		assert.Equal(t, partition1, partition2, "Partition should be consistent for same key")
		assert.GreaterOrEqual(t, partition1, int32(0))
		assert.Less(t, partition1, int32(128))
	}

	// Test node assignment
	for _, key := range testKeys {
		partition := c.getPartitionForKey(key)
		nodeID := c.getNodeForPartition(partition)
		assert.NotEmpty(t, nodeID, "Should have node for partition")
		assert.Contains(t, []string{"node1", "node2", "node3"}, nodeID)
	}

	// Test GetNodeForKey
	for _, key := range testKeys {
		nodeID, err := c.GetNodeForKey(key)
		require.NoError(t, err)
		assert.NotEmpty(t, nodeID)
		assert.Contains(t, []string{"node1", "node2", "node3"}, nodeID)
	}
}

func TestClusterClient_TopologyUpdate(t *testing.T) {
	config := &ClusterClientConfig{
		PoolSizePerNode:         3,
		TopologyRefreshInterval: 30 * time.Second,
	}
	c := &ClusterClient{
		clients:         make(map[string]*ConnectionPool),
		partitionOwners: make(map[int32]string),
		config:          config,
		stopCh:          make(chan struct{}),
	}

	// Initial topology
	topology1 := &clusterpb.ClusterTopology{
		Epoch: 1,
		Nodes: []*clusterpb.NodeInfo{
			{Id: "node1", Address: "localhost:9001", Status: clusterpb.NodeStatus_NODE_STATUS_ACTIVE},
		},
		RingConfig: &clusterpb.RingConfig{
			PartitionCount:    64,
			ReplicationFactor: 10,
			Load:              1.25,
		},
		PartitionOwners: []*clusterpb.PartitionOwner{},
	}

	for i := 0; i < 64; i++ {
		topology1.PartitionOwners = append(topology1.PartitionOwners, &clusterpb.PartitionOwner{
			PartitionId: int32(i),
			NodeId:      "node1",
		})
	}

	err := c.updateTopology(topology1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), c.topologyEpoch)
	assert.Len(t, c.partitionOwners, 64)

	// Update with same epoch (should be ignored)
	err = c.updateTopology(topology1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), c.topologyEpoch)

	// Update with new topology
	topology2 := &clusterpb.ClusterTopology{
		Epoch: 2,
		Nodes: []*clusterpb.NodeInfo{
			{Id: "node1", Address: "localhost:9001", Status: clusterpb.NodeStatus_NODE_STATUS_ACTIVE},
			{Id: "node2", Address: "localhost:9002", Status: clusterpb.NodeStatus_NODE_STATUS_ACTIVE},
		},
		RingConfig: &clusterpb.RingConfig{
			PartitionCount:    64,
			ReplicationFactor: 10,
			Load:              1.25,
		},
		PartitionOwners: []*clusterpb.PartitionOwner{},
	}

	// Redistribute partitions
	for i := 0; i < 64; i++ {
		nodeID := "node1"
		if i%2 == 0 {
			nodeID = "node2"
		}
		topology2.PartitionOwners = append(topology2.PartitionOwners, &clusterpb.PartitionOwner{
			PartitionId: int32(i),
			NodeId:      nodeID,
		})
	}

	err = c.updateTopology(topology2)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), c.topologyEpoch)

	// Verify partition redistribution
	evenPartitions := 0
	oddPartitions := 0
	for i := 0; i < 64; i++ {
		nodeID := c.getNodeForPartition(int32(i))
		if i%2 == 0 {
			assert.Equal(t, "node2", nodeID)
			evenPartitions++
		} else {
			assert.Equal(t, "node1", nodeID)
			oddPartitions++
		}
	}
	assert.Equal(t, 32, evenPartitions)
	assert.Equal(t, 32, oddPartitions)
}

func TestClusterClient_IsRoutingError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "non-grpc error",
			err:      context.Canceled,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRoutingError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
