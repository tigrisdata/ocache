package integration

import (
	"fmt"
	"time"

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

// Test_WorkloadDistribution_TokenMapping verifies token distribution across nodes
func (s *ClusterSuite) Test_WorkloadDistribution_TokenMapping() {
	clusterHarness, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	tokens := clusterHarness.GetTokenDistribution()
	require.NotEmpty(s.T(), tokens, "Should have token distribution")

	// Each node should own 128 tokens (test harness uses fewer tokens for faster testing)
	// Production uses 512 tokens per node (DefaultNumTokens)
	expectedPerNode := 128
	variance := expectedPerNode / 5

	s.T().Logf("Token distribution (expected ~%d tokens per node):", expectedPerNode)
	for nodeID, count := range tokens {
		s.T().Logf("  Node %s: %d tokens", nodeID, count)
		assert.InDelta(s.T(), expectedPerNode, count, float64(variance),
			"Node %s token count should be balanced", nodeID)
	}

	// All nodes should be present
	assert.Equal(s.T(), clusterHarness.NodeCount, len(tokens),
		"All nodes should be in token distribution")
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

// Test_NodeFailure_RoutingContinuity verifies that when a node goes down,
// the cluster client continues to route traffic to the remaining nodes.
// NOTE: With ReplicationFactor=1, keys on the stopped node are LOST (not replicated).
// This test verifies routing works, NOT data availability after node failure.
func (s *ClusterSuite) Test_NodeFailure_RoutingContinuity() {
	clusterHarness, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	// Write keys distributed across all nodes
	keyCount := 300
	data := GenerateRandomData(1024)
	keys := make([]string, keyCount)

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("failover-key-%04d", i)
		keys[i] = key
		err := s.Harness.PutObject(key, data, 0)
		require.NoError(s.T(), err)
	}

	// Verify initial distribution and track which keys are on which node
	initialDist, err := clusterHarness.VerifyKeyDistribution(keys)
	require.NoError(s.T(), err)

	// Pick a node to stop
	var nodeToStop string
	for nodeID := range clusterHarness.Nodes {
		nodeToStop = nodeID
		break
	}

	// Track which keys are on the stopped node vs surviving nodes
	keysOnStoppedNode := make(map[string]bool)
	keysOnSurvivingNodes := make(map[string]bool)
	for nodeID, nodeKeys := range initialDist {
		for _, key := range nodeKeys {
			if nodeID == nodeToStop {
				keysOnStoppedNode[key] = true
			} else {
				keysOnSurvivingNodes[key] = true
			}
		}
	}

	s.T().Logf("Initial key distribution:")
	for nodeID, nodeKeys := range initialDist {
		s.T().Logf("  Node %s: %d keys", nodeID, len(nodeKeys))
	}
	s.T().Logf("Keys on node to stop (%s): %d", nodeToStop, len(keysOnStoppedNode))
	s.T().Logf("Keys on surviving nodes: %d", len(keysOnSurvivingNodes))

	// Stop the node
	s.T().Logf("Stopping node %s", nodeToStop)
	node := clusterHarness.Nodes[nodeToStop]
	err = node.Stop()
	require.NoError(s.T(), err)

	// Wait for cluster to detect the node departure
	time.Sleep(2 * time.Second)

	// Verify keys on surviving nodes are still accessible
	survivingKeySuccessCount := 0
	for key := range keysOnSurvivingNodes {
		_, err := s.Harness.GetObject(key)
		if err == nil {
			survivingKeySuccessCount++
		}
	}

	// All keys on surviving nodes should be accessible
	assert.Equal(s.T(), len(keysOnSurvivingNodes), survivingKeySuccessCount,
		"All keys on surviving nodes should be accessible")
	s.T().Logf("Keys on surviving nodes accessible: %d/%d",
		survivingKeySuccessCount, len(keysOnSurvivingNodes))

	// Verify new writes succeed to surviving nodes
	newWriteCount := 100
	newWriteSuccessCount := 0
	for i := 0; i < newWriteCount; i++ {
		key := fmt.Sprintf("post-failure-key-%04d", i)
		err := s.Harness.PutObject(key, data, 0)
		if err == nil {
			newWriteSuccessCount++
		}
	}

	// All new writes should succeed (routed to surviving nodes)
	assert.Equal(s.T(), newWriteCount, newWriteSuccessCount,
		"All new writes should succeed on surviving nodes")
	s.T().Logf("New writes succeeded: %d/%d", newWriteSuccessCount, newWriteCount)

	// Verify we can read back the new keys
	newReadSuccessCount := 0
	for i := 0; i < newWriteCount; i++ {
		key := fmt.Sprintf("post-failure-key-%04d", i)
		_, err := s.Harness.GetObject(key)
		if err == nil {
			newReadSuccessCount++
		}
	}
	assert.Equal(s.T(), newWriteSuccessCount, newReadSuccessCount,
		"All newly written keys should be readable")
	s.T().Logf("New keys readable: %d/%d", newReadSuccessCount, newWriteCount)
}

// Test_NodeFailure_TrafficContinuity verifies that online read/write traffic
// continues to work when a node is brought down in the cluster.
// This test focuses on NEW traffic after node failure to avoid the issue
// of keys lost on the failed node (RF=1 means no replication).
func (s *ClusterSuite) Test_NodeFailure_TrafficContinuity() {
	clusterHarness, ok := s.Harness.(*ClusterTestHarness)
	if !ok {
		s.T().Skip("Test requires ClusterTestHarness")
	}

	data := GenerateRandomData(1024)

	// Phase 1: Stop one node
	var nodeToStop string
	for nodeID := range clusterHarness.Nodes {
		nodeToStop = nodeID
		break
	}

	s.T().Logf("Phase 1: Stopping node %s", nodeToStop)
	node := clusterHarness.Nodes[nodeToStop]
	err := node.Stop()
	require.NoError(s.T(), err)

	// Wait for cluster to detect the node departure
	time.Sleep(2 * time.Second)

	// Phase 2: Write new keys after node failure (should all succeed)
	keyCount := 200
	writeSuccessCount := 0
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("post-failure-traffic-%04d", i)
		err := s.Harness.PutObject(key, data, 0)
		if err == nil {
			writeSuccessCount++
		}
	}

	// All writes should succeed (routed to surviving nodes)
	assert.Equal(s.T(), keyCount, writeSuccessCount,
		"All writes should succeed after node failure")
	s.T().Logf("Phase 2: %d/%d writes succeeded after node failure",
		writeSuccessCount, keyCount)

	// Phase 3: Read back all written keys
	readSuccessCount := 0
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("post-failure-traffic-%04d", i)
		_, err := s.Harness.GetObject(key)
		if err == nil {
			readSuccessCount++
		}
	}

	// All reads should succeed (keys written to surviving nodes)
	assert.Equal(s.T(), writeSuccessCount, readSuccessCount,
		"All written keys should be readable")
	s.T().Logf("Phase 3: %d/%d keys readable after node failure",
		readSuccessCount, keyCount)

	// Phase 4: Mixed read/write workload to verify ongoing traffic
	mixedSuccessCount := 0
	for i := 0; i < 100; i++ {
		// Write
		writeKey := fmt.Sprintf("mixed-traffic-%04d", i)
		if err := s.Harness.PutObject(writeKey, data, 0); err == nil {
			mixedSuccessCount++
			// Read back immediately
			if _, err := s.Harness.GetObject(writeKey); err == nil {
				mixedSuccessCount++
			}
		}
	}

	// Mixed operations should mostly succeed
	assert.GreaterOrEqual(s.T(), mixedSuccessCount, 180, // At least 90% success
		"Mixed read/write traffic should continue working")
	s.T().Logf("Phase 4: %d/200 mixed operations succeeded", mixedSuccessCount)
}

// Helper function to convert map keys to slice
func keysFromMap(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
