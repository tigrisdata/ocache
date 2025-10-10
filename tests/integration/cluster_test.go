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

// Test_ClusterList_BasicOperation verifies List operation aggregates keys across all cluster nodes
func (s *ClusterSuite) Test_ClusterList_BasicOperation() {
	clusterHarness, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	// Write keys distributed across nodes
	keyCount := 300
	keyPrefix := "list-test-"
	data := GenerateRandomData(1024) // 1KB objects

	expectedKeys := make(map[string]bool)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%04d", keyPrefix, i)
		expectedKeys[key] = true
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(s.T(), err)
	}

	s.T().Logf("Created %d keys with prefix '%s'", keyCount, keyPrefix)

	// Verify keys are distributed across nodes
	distribution, err := clusterHarness.VerifyKeyDistribution(keysFromMap(expectedKeys))
	require.NoError(s.T(), err)

	nodeCount := len(distribution)
	require.Greater(s.T(), nodeCount, 1, "Keys should be distributed across multiple nodes")

	s.T().Logf("Keys distributed across %d nodes:", nodeCount)
	for nodeID, keys := range distribution {
		s.T().Logf("  Node %s: %d keys", nodeID, len(keys))
	}

	// List all keys with prefix
	listedKeys, err := s.Harness.List(keyPrefix)
	require.NoError(s.T(), err)

	// Verify all keys were returned
	assert.Len(s.T(), listedKeys, keyCount, "Should list all keys from all nodes")

	// Verify each expected key is in the result
	listedKeyMap := make(map[string]bool)
	for _, key := range listedKeys {
		listedKeyMap[key] = true
	}

	for expectedKey := range expectedKeys {
		assert.True(s.T(), listedKeyMap[expectedKey], "Expected key %s should be in list results", expectedKey)
	}

	// Verify no extra keys
	for listedKey := range listedKeyMap {
		assert.True(s.T(), expectedKeys[listedKey], "Listed key %s should be in expected keys", listedKey)
	}

	// Verify keys are in sorted order
	for i := 1; i < len(listedKeys); i++ {
		assert.LessOrEqual(s.T(), listedKeys[i-1], listedKeys[i],
			"Keys should be in sorted order: %s should be <= %s", listedKeys[i-1], listedKeys[i])
	}

	s.T().Logf("Successfully listed %d keys across %d nodes in sorted order", len(listedKeys), nodeCount)
}

// Test_ClusterList_Pagination verifies pagination works correctly across cluster nodes
func (s *ClusterSuite) Test_ClusterList_Pagination() {
	_, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	// Write 2000 keys to force pagination (default page size is 1000)
	keyCount := 2000
	keyPrefix := "page-test-"
	data := GenerateRandomData(512)

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%05d", keyPrefix, i)
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(s.T(), err)
	}

	s.T().Logf("Created %d keys for pagination test", keyCount)

	// Use pagination to get all keys
	var allKeys []string
	continuationToken := ""
	pageCount := 0

	for {
		keys, token, hasMore, err := s.Harness.ListPage(keyPrefix, 1000, continuationToken)
		require.NoError(s.T(), err)

		pageCount++
		s.T().Logf("Page %d: received %d keys, hasMore=%v", pageCount, len(keys), hasMore)

		allKeys = append(allKeys, keys...)

		if !hasMore {
			break
		}

		require.NotEmpty(s.T(), token, "Continuation token should not be empty when hasMore=true")
		continuationToken = token
	}

	// Verify we got all keys
	assert.Len(s.T(), allKeys, keyCount, "Should receive all keys across pages")

	// Verify keys are globally sorted across pages
	for i := 1; i < len(allKeys); i++ {
		assert.LessOrEqual(s.T(), allKeys[i-1], allKeys[i],
			"Keys should be in sorted order across pages: %s should be <= %s", allKeys[i-1], allKeys[i])
	}

	s.T().Logf("Successfully paginated through %d keys in %d pages, maintaining global sort order", keyCount, pageCount)
}

// Test_ClusterList_EmptyPrefix verifies List with empty prefix returns all keys
func (s *ClusterSuite) Test_ClusterList_EmptyPrefix() {
	_, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	// Write some keys
	keys := []string{"alpha-1", "beta-2", "gamma-3", "delta-4"}
	data := GenerateRandomData(512)

	for _, key := range keys {
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(s.T(), err)
	}

	// List all keys (empty prefix)
	allKeys, err := s.Harness.List("")
	require.NoError(s.T(), err)

	// Should contain at least our keys (may contain more from other tests)
	s.T().Logf("Listed %d total keys with empty prefix", len(allKeys))

	for _, expectedKey := range keys {
		assert.Contains(s.T(), allKeys, expectedKey, "All keys should be listed with empty prefix")
	}
}

// Helper function to convert map keys to slice
func keysFromMap(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
