package integration

import (
	"fmt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CoordinatorObjectsSuite uses the same tests as ObjectsSuite since they share
// the TestHarnessInterface. The tests are defined in objects_test.go.

// Test_Objects_BasicFlow - inherited from ObjectsSuite via embedding
func (s *ClusterSuite) Test_Objects_BasicFlow() {
	// Call the ObjectsSuite implementation
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_BasicFlow()
}

// Test_Objects_EdgeCases - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_EdgeCases() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_EdgeCases()
}

// Test_Objects_UpdateExisting - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_UpdateExisting() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_UpdateExisting()
}

// Test_Objects_MixedSizes - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_MixedSizes() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_MixedSizes()
}

// Test_Objects_StreamingWrite - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_StreamingWrite() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_StreamingWrite()
}

// Test_Objects_CompactionBehavior - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_CompactionBehavior() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_CompactionBehavior()
}

// Note: Test_Objects_LRUEviction and Test_Objects_MixedTTL are automatically
// skipped in cluster mode by the checks in objects_test.go

// Test_WorkloadDistribution_Basic verifies basic workload distribution across cluster nodes
func (s *ClusterSuite) Test_WorkloadDistribution_Basic() {
	clusterHarness, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	// Write 1000 keys with predictable pattern
	keyCount := 1000
	keys := make([]string, keyCount)
	data := GenerateRandomData(1024) // 1KB objects

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("dist-key-%04d", i)
		keys[i] = key
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(s.T(), err)
	}

	// Verify distribution is reasonably even (within 20% deviation)
	stats := clusterHarness.GetDistributionStats()
	AssertEvenDistribution(s.T(), stats, 0.2)

	// Verify keys are on correct nodes according to consistent hashing
	distribution, err := clusterHarness.VerifyKeyDistribution(keys)
	require.NoError(s.T(), err)

	// Each node should have roughly keyCount/nodeCount keys (with some variance)
	expectedPerNode := keyCount / clusterHarness.NodeCount
	variance := expectedPerNode / 3 // Allow 33% variance

	for nodeID, nodeKeys := range distribution {
		count := len(nodeKeys)
		s.T().Logf("Node %s owns %d keys (expected ~%d)", nodeID, count, expectedPerNode)
		assert.InDelta(s.T(), expectedPerNode, count, float64(variance),
			"Node %s has %d keys, expected ~%d (±%d)", nodeID, count, expectedPerNode, variance)
	}

	// Verify all keys are accounted for
	totalKeysInDistribution := 0
	for _, nodeKeys := range distribution {
		totalKeysInDistribution += len(nodeKeys)
	}
	assert.Equal(s.T(), keyCount, totalKeysInDistribution, "All keys should be accounted for")
}

// Test_WorkloadDistribution_PartitionMapping verifies partition distribution across nodes
func (s *ClusterSuite) Test_WorkloadDistribution_PartitionMapping() {
	clusterHarness, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	partitions := clusterHarness.GetPartitionDistribution()
	require.NotEmpty(s.T(), partitions, "Should have partition distribution")

	// With 3 nodes, each should own approximately 5461 partitions (16384/3)
	expectedPerNode := 16384 / clusterHarness.NodeCount
	variance := expectedPerNode / 5 // Allow 20% variance

	s.T().Logf("Partition distribution (total 16384 partitions):")
	for nodeID, parts := range partitions {
		count := parts[0] // We store estimated count
		s.T().Logf("  Node %s: %d partitions (expected ~%d)", nodeID, count, expectedPerNode)
		assert.InDelta(s.T(), expectedPerNode, count, float64(variance),
			"Node %s partition count should be balanced", nodeID)
	}

	// All nodes should be present
	assert.Equal(s.T(), clusterHarness.NodeCount, len(partitions),
		"All nodes should be in partition distribution")
}

// Test_WorkloadDistribution_ReadBalance verifies read operations are balanced
func (s *ClusterSuite) Test_WorkloadDistribution_ReadBalance() {
	clusterHarness, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	// Write 500 keys
	keyCount := 500
	keys := make([]string, keyCount)
	data := GenerateRandomData(2048) // 2KB objects

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("read-key-%04d", i)
		keys[i] = key
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(s.T(), err)
	}

	// Read all keys multiple times
	readRounds := 3
	for round := 0; round < readRounds; round++ {
		for _, key := range keys {
			_, err := s.Harness.GetObject(key)
			require.NoError(s.T(), err)
		}
	}

	// Verify read operations are distributed
	stats := clusterHarness.GetDistributionStats()

	s.T().Logf("Read distribution after %d read rounds:", readRounds)
	totalReads := int64(0)
	for nodeID, dist := range stats.PerNode {
		s.T().Logf("  Node %s: %d reads", nodeID, dist.ReadCount)
		totalReads += dist.ReadCount
	}

	// Total reads should match expected (keys * readRounds)
	expectedTotalReads := int64(keyCount * readRounds)
	assert.Equal(s.T(), expectedTotalReads, totalReads,
		"Total reads across all nodes should match expected")

	// Each node should handle roughly equal reads (based on key distribution)
	// With consistent hashing, reads should follow write distribution
	AssertEvenDistribution(s.T(), stats, 0.25) // Allow 25% variance for reads
}

// Test_WorkloadDistribution_MixedOperations verifies balanced mixed workload
func (s *ClusterSuite) Test_WorkloadDistribution_MixedOperations() {
	clusterHarness, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	// Perform mixed operations
	keyCount := 300
	data := GenerateRandomData(1024)

	// Write operations
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("mixed-key-%04d", i)
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(s.T(), err)
	}

	// Read operations
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("mixed-key-%04d", i)
		_, err := s.Harness.GetObject(key)
		require.NoError(s.T(), err)
	}

	// Delete some operations
	deleteCount := keyCount / 3
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("mixed-key-%04d", i)
		err := s.Harness.DeleteObject(key)
		require.NoError(s.T(), err)
	}

	// Verify balanced distribution of all operation types
	stats := clusterHarness.GetDistributionStats()

	s.T().Logf("Mixed workload distribution:")
	for nodeID, dist := range stats.PerNode {
		s.T().Logf("  Node %s: %d writes, %d reads, %d deletes, %d keys",
			nodeID, dist.WriteCount, dist.ReadCount, dist.DeleteCount, dist.KeyCount)
	}

	// Verify overall balance
	AssertEvenDistribution(s.T(), stats, 0.25)

	// Verify total operations
	totalWrites := int64(0)
	totalReads := int64(0)
	totalDeletes := int64(0)
	for _, dist := range stats.PerNode {
		totalWrites += dist.WriteCount
		totalReads += dist.ReadCount
		totalDeletes += dist.DeleteCount
	}

	assert.Equal(s.T(), int64(keyCount), totalWrites, "Total writes should match")
	assert.Equal(s.T(), int64(keyCount), totalReads, "Total reads should match")
	assert.Equal(s.T(), int64(deleteCount), totalDeletes, "Total deletes should match")
}
