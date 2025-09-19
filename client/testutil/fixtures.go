package testutil

import (
	"fmt"

	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
)

// Test data sizes
const (
	SmallDataSize  = 1024        // 1KB
	MediumDataSize = 64 * 1024   // 64KB
	LargeDataSize  = 1024 * 1024 // 1MB
)

// GenerateTestData generates test data of specified size
func GenerateTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

// StandardTestKeys returns a set of standard test keys
func StandardTestKeys() []string {
	return []string{
		"test-key-1",
		"test-key-2",
		"test-key-3",
		"test-key-alpha",
		"test-key-beta",
		"test-key-gamma",
	}
}

// CreateSimpleTopology creates a basic topology for testing
func CreateSimpleTopology(addresses []string, epoch uint64) *clusterpb.ClusterTopology {
	nodes := make([]*clusterpb.NodeInfo, len(addresses))
	partitionOwners := make([]*clusterpb.PartitionOwner, 0)
	
	for i, addr := range addresses {
		nodes[i] = &clusterpb.NodeInfo{
			Id:      fmt.Sprintf("node-%d", i),
			Address: addr,
			Status:  clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
		}
	}
	
	// Distribute partitions among nodes
	partitionCount := int32(10)
	for i := int32(0); i < partitionCount; i++ {
		nodeIdx := i % int32(len(nodes))
		partitionOwners = append(partitionOwners, &clusterpb.PartitionOwner{
			PartitionId: i,
			NodeId:      nodes[nodeIdx].Id,
		})
	}
	
	return &clusterpb.ClusterTopology{
		Epoch: epoch,
		Nodes: nodes,
		RingConfig: &clusterpb.RingConfig{
			PartitionCount:    partitionCount,
			ReplicationFactor: 20,
			Load:              1.25,
		},
		PartitionOwners: partitionOwners,
	}
}

// CreateTopologyWithDownNodes creates a topology with some nodes marked as down
func CreateTopologyWithDownNodes(addresses []string, downIndices []int, epoch uint64) *clusterpb.ClusterTopology {
	topology := CreateSimpleTopology(addresses, epoch)
	
	for _, idx := range downIndices {
		if idx < len(topology.Nodes) {
			topology.Nodes[idx].Status = clusterpb.NodeStatus_NODE_STATUS_DOWN
		}
	}
	
	return topology
}

// StandardErrorMessages returns common error messages for testing
func StandardErrorMessages() map[string]string {
	return map[string]string{
		"routing":    "routing error",
		"network":    "network error",
		"timeout":    "operation timeout",
		"notfound":   "key not found",
		"permission": "permission denied",
		"internal":   "internal error",
	}
}

// TestDataSet represents a set of test data with different sizes
type TestDataSet struct {
	Small  []byte
	Medium []byte
	Large  []byte
}

// NewTestDataSet creates a new test data set
func NewTestDataSet() *TestDataSet {
	return &TestDataSet{
		Small:  GenerateTestData(SmallDataSize),
		Medium: GenerateTestData(MediumDataSize),
		Large:  GenerateTestData(LargeDataSize),
	}
}

// KeyValuePairs generates a map of test key-value pairs
func KeyValuePairs(prefix string, count int) map[string][]byte {
	pairs := make(map[string][]byte)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		value := []byte(fmt.Sprintf("value-%d", i))
		pairs[key] = value
	}
	return pairs
}