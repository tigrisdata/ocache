// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	serveroperations "github.com/tigrisdata/ocache/server/operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

	// Verify distribution is reasonably even
	// With 128 tokens per node (test harness), allow 40% deviation (min score 60)
	stats := clusterHarness.GetDistributionStats()
	AssertEvenDistribution(s.T(), stats, 0.40)

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
	// With 128 tokens per node (test harness), allow 40% deviation (min score 60)
	AssertEvenDistribution(s.T(), stats, 0.40)
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
	// With 128 tokens per node (test harness), allow 40% deviation (min score 60)
	AssertEvenDistribution(s.T(), stats, 0.40)

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

type clusterListPaginationFixture struct {
	nodeA  *ClusterServerNode
	nodeB  *ClusterServerNode
	nodeC  *ClusterServerNode
	client pb.CacheServiceClient
}

func newClusterListPaginationFixture(t *testing.T) *clusterListPaginationFixture {
	return newClusterListPaginationFixtureWithInterceptor(t, nil)
}

func newClusterListPaginationFixtureWithInterceptor(
	t *testing.T,
	interceptorFactory func(string, grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor,
) *clusterListPaginationFixture {
	return newClusterListPaginationFixtureWithNodeCount(t, 2, interceptorFactory)
}

func newClusterListPaginationFixtureWithNodeCount(
	t *testing.T,
	nodeCount int,
	interceptorFactory func(string, grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor,
) *clusterListPaginationFixture {
	t.Helper()

	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = time.Hour
	config.RecompactionInterval = time.Hour
	harness := NewClusterTestHarness(t, nodeCount, config)
	harness.unaryInterceptorFactory = interceptorFactory
	t.Cleanup(harness.Cleanup)
	if err := harness.StartAllNodes(); err != nil {
		t.Fatalf("start %d-node cluster: %v", nodeCount, err)
	}

	nodeIDs := make([]string, 0, len(harness.Nodes))
	for nodeID := range harness.Nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	require.Len(t, nodeIDs, nodeCount)

	nodeA := harness.Nodes[nodeIDs[0]]
	nodeB := harness.Nodes[nodeIDs[1]]
	var nodeC *ClusterServerNode
	if nodeCount > 2 {
		nodeC = harness.Nodes[nodeIDs[2]]
	}
	return &clusterListPaginationFixture{
		nodeA:  nodeA,
		nodeB:  nodeB,
		nodeC:  nodeC,
		client: pb.NewCacheServiceClient(nodeA.ClusterConn),
	}
}

func listFailureInterceptorFactory(
	targetNodeID string,
	failList *atomic.Bool,
	failListWithValues *atomic.Bool,
	failureCount *atomic.Int32,
) func(string, grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(nodeID string, next grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
		return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			shouldFail := false
			if nodeID == targetNodeID {
				switch info.FullMethod {
				case pb.CacheService_ListLocal_FullMethodName:
					shouldFail = failList.CompareAndSwap(true, false)
				case pb.CacheService_ListLocalWithValues_FullMethodName:
					shouldFail = failListWithValues.CompareAndSwap(true, false)
				}
			}
			if shouldFail {
				failureCount.Add(1)
				return nil, status.Error(codes.Unavailable, "temporary list RPC failure")
			}
			return next(ctx, req, info, handler)
		}
	}
}

func newLargeListPaginationClient(t *testing.T, node *ClusterServerNode) pb.CacheServiceClient {
	t.Helper()

	dialCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, node.ListenAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024)),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return pb.NewCacheServiceClient(conn)
}

func putClusterListFixtureValue(t *testing.T, node *ClusterServerNode, key string) []byte {
	t.Helper()
	value := []byte("value-" + key)
	// Write directly to each node's real store so the two shards have fixed,
	// deliberately interleaved key sets throughout the pagination exercise.
	require.NoError(t, node.Storage.Put(key, bytes.NewReader(value), 0))
	return value
}

func checkClusterPaginationPages(
	t *testing.T,
	ctx context.Context,
	client pb.CacheServiceClient,
	scenario, prefix, initialToken, startKey, previousKey string,
	previousKeySet bool,
	firstLimit, continuationLimit int,
	want []string,
	values map[string][]byte,
	violation func(format string, args ...interface{}),
) {
	t.Helper()

	var got []string
	lastKey := previousKey
	hasLastKey := previousKeySet
	seenKeys := make(map[string]struct{}, len(want)+1)
	if previousKeySet {
		seenKeys[previousKey] = struct{}{}
	}
	seenTokens := make(map[string]struct{})
	if initialToken != "" {
		seenTokens[initialToken] = struct{}{}
	}
	token := initialToken
	complete := false

	for page := 0; page < len(want)+1; page++ {
		pageLimit := firstLimit
		if page > 0 {
			pageLimit = continuationLimit
		}
		response, err := client.ListWithValues(ctx, &pb.ListRequest{
			Prefix:            prefix,
			StartKey:          startKey,
			Limit:             int32(pageLimit),
			ContinuationToken: token,
		})
		require.NoErrorf(t, err, "%s page %d RPC", scenario, page+1)
		require.NotNilf(t, response, "%s page %d response", scenario, page+1)
		if len(response.Entries) > pageLimit {
			violation("page %d returned %d entries for limit %d", page+1, len(response.Entries), pageLimit)
		}
		for _, entry := range response.Entries {
			if entry == nil {
				violation("page %d returned a nil entry", page+1)
				continue
			}
			wantValue, exists := values[entry.Key]
			if !exists || !bytes.Equal(entry.Value, wantValue) {
				violation("key %q returned an unexpected value", entry.Key)
			}
			if _, exists := seenKeys[entry.Key]; exists {
				violation("key %q was repeated", entry.Key)
			}
			if hasLastKey && entry.Key <= lastKey {
				violation("key %q did not follow %q in strict global order", entry.Key, lastKey)
			}
			seenKeys[entry.Key] = struct{}{}
			lastKey = entry.Key
			hasLastKey = true
			got = append(got, entry.Key)
		}

		if !response.HasMore {
			if response.ContinuationToken != "" {
				violation("final page returned a continuation token")
			}
			complete = true
			break
		}
		if len(got) >= len(want) {
			violation("HasMore remained true after all expected keys")
		}
		if response.ContinuationToken == "" {
			violation("page %d reported HasMore without a continuation token", page+1)
			break
		}
		if _, exists := seenTokens[response.ContinuationToken]; exists {
			violation("continuation token repeated after page %d", page+1)
			break
		}
		seenTokens[response.ContinuationToken] = struct{}{}
		token = response.ContinuationToken
		startKey = ""
	}

	if !complete {
		violation("pagination did not reach HasMore=false")
	}
	if len(got) != len(want) {
		violation("returned %d keys, want %d", len(got), len(want))
		return
	}
	for i := range want {
		if got[i] != want[i] {
			violation("key at page order index %d differed", i)
			return
		}
	}
}

func testClusterListWithValuesPagination(t *testing.T) {
	testClusterListWithValuesPaginationPages(t)
}

func testClusterListWithValuesPaginationPages(t *testing.T) {
	fixture := newClusterListPaginationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	values := make(map[string][]byte)
	for _, key := range []string{"a", "z"} {
		values[key] = putClusterListFixtureValue(t, fixture.nodeA, key)
	}
	for _, key := range []string{"b", "c"} {
		values[key] = putClusterListFixtureValue(t, fixture.nodeB, key)
	}
	checkPages := func(scenario, prefix, initialToken, startKey, previousKey string, previousKeySet bool, firstLimit, continuationLimit int, want []string) {
		t.Helper()
		violation := func(format string, args ...interface{}) {
			t.Errorf("%s: %s", scenario, fmt.Sprintf(format, args...))
		}
		checkClusterPaginationPages(t, ctx, fixture.client, scenario, prefix, initialToken, startKey, previousKey, previousKeySet, firstLimit, continuationLimit, want, values, violation)
	}

	want := []string{"a", "b", "c", "z"}
	checkPages("fresh token", "", "", "", "", false, 1, 1, want)

	previousGlobalBytes, err := json.Marshal(struct {
		NodeCursors map[string]string `json:"node_cursors"`
		Prefix      string            `json:"prefix"`
	}{
		NodeCursors: map[string]string{"": "a"},
	})
	require.NoError(t, err)
	previousGlobalToken := base64.StdEncoding.EncodeToString(previousGlobalBytes)
	checkPages(
		"previous global cursor token", "", previousGlobalToken, "", "a", true, 1, 1,
		[]string{"b", "c", "z"},
	)

	legacyBytes, err := json.Marshal(struct {
		NodeCursors map[string]string `json:"node_cursors"`
		Prefix      string            `json:"prefix"`
	}{
		NodeCursors: map[string]string{fixture.nodeA.NodeID: "a"},
	})
	require.NoError(t, err)
	legacyToken := base64.StdEncoding.EncodeToString(legacyBytes)
	checkPages("legacy per-node token", "", legacyToken, "", "a", true, 1, 1, []string{"b", "c", "z"})
	checkPages("plain start key", "", "", "a", "a", true, 1, 1, []string{"b", "c", "z"})
	checkPages("plain continuation token", "", "a", "", "a", true, 1, 1, []string{"b", "c", "z"})

	// Duplicate keys on different shards are returned once, including when a
	// larger page consumes more than one key from each shard.
	values["b"] = putClusterListFixtureValue(t, fixture.nodeA, "b")
	values["a"] = putClusterListFixtureValue(t, fixture.nodeB, "a")
	checkPages("duplicate keys", "", "", "", "", false, 1, 1, want)
	checkPages("duplicate keys with larger pages", "", "", "", "", false, 2, 2, want)
	checkPages("legacy token with duplicate keys", "", legacyToken, "", "a", true, 1, 1, []string{"b", "c", "z"})

	legacyShapedBytes, err := json.Marshal(struct {
		NodeCursors map[string]string `json:"node_cursors"`
		Prefix      string            `json:"prefix"`
	}{
		NodeCursors: map[string]string{"": "zzzz"},
	})
	require.NoError(t, err)
	legacyShapedStartKey := base64.StdEncoding.EncodeToString(legacyShapedBytes)
	values[legacyShapedStartKey] = putClusterListFixtureValue(t, fixture.nodeA, legacyShapedStartKey)
	response, err := fixture.client.ListWithValues(ctx, &pb.ListRequest{StartKey: legacyShapedStartKey, Limit: 10})
	require.NoError(t, err)
	require.Len(t, response.Entries, 1)
	require.Equal(t, "z", response.Entries[0].Key)
	require.False(t, response.HasMore)

	for _, key := range []string{"p:a", "p:z"} {
		values[key] = putClusterListFixtureValue(t, fixture.nodeA, key)
	}
	for _, key := range []string{"p:b", "p:c"} {
		values[key] = putClusterListFixtureValue(t, fixture.nodeB, key)
	}
	checkPages("nonempty prefix token", "p:", "", "", "", false, 1, 1, []string{"p:a", "p:b", "p:c", "p:z"})

	previousPartialBytes, err := json.Marshal(struct {
		NodeCursors map[string]string `json:"node_cursors"`
	}{
		NodeCursors: map[string]string{"": "p:", fixture.nodeA.NodeID: "p:a"},
	})
	require.NoError(t, err)
	previousPartialToken := base64.StdEncoding.EncodeToString(previousPartialBytes)
	checkPages(
		"previous partial token without top-level prefix", "p:", previousPartialToken, "", "p:a", true, 1, 1,
		[]string{"p:b", "p:c", "p:z"},
	)

	values["\x00"] = putClusterListFixtureValue(t, fixture.nodeB, "\x00")
	values[""] = putClusterListFixtureValue(t, fixture.nodeA, "")
	firstEmptyPage, err := fixture.client.ListWithValues(ctx, &pb.ListRequest{Limit: 1})
	require.NoError(t, err)
	require.Len(t, firstEmptyPage.Entries, 1)
	require.Empty(t, firstEmptyPage.Entries[0].Key)
	require.True(t, firstEmptyPage.HasMore)
	emptyTokenBytes, err := base64.StdEncoding.DecodeString(firstEmptyPage.ContinuationToken)
	require.NoError(t, err)
	var emptyCursorToken serveroperations.ContinuationToken
	require.NoError(t, json.Unmarshal(emptyTokenBytes, &emptyCursorToken))
	require.Equal(t, "global-empty-v1", emptyCursorToken.CursorMode)
	require.NotNil(t, emptyCursorToken.CursorPrefix)
	require.Empty(t, *emptyCursorToken.CursorPrefix)
	var oldReaderEmptyToken struct {
		Prefix string `json:"prefix"`
	}
	require.NoError(t, json.Unmarshal(emptyTokenBytes, &oldReaderEmptyToken))
	require.NotEqual(t, "", oldReaderEmptyToken.Prefix, "legacy readers must reject rather than replay an empty cursor")
	allKeys := []string{"", "\x00", "a", "b", "c", legacyShapedStartKey, "p:a", "p:b", "p:c", "p:z", "z"}
	checkPages("empty key", "", "", "", "", false, 1, 1, allKeys)

	legacyEmptyBytes, err := json.Marshal(struct {
		NodeCursors map[string]string `json:"node_cursors"`
		Prefix      string            `json:"prefix"`
	}{
		NodeCursors: map[string]string{fixture.nodeA.NodeID: ""},
	})
	require.NoError(t, err)
	legacyEmptyToken := base64.StdEncoding.EncodeToString(legacyEmptyBytes)
	checkPages("legacy empty-key token", "", legacyEmptyToken, "", "", true, 1, 1, allKeys[1:])
}

// testClusterListLegacyReaderCompatibility models the base coordinator's
// two-field token decoder and per-node cursor lookup, then calls the real
// node-local RPCs. It is a token-contract test, not an old-server process test.
func testClusterListLegacyReaderCompatibility(t *testing.T) {
	var failList atomic.Bool
	var failListWithValues atomic.Bool
	var failureCount atomic.Int32
	fixture := newClusterListPaginationFixtureWithInterceptor(
		t,
		listFailureInterceptorFactory("cluster-node-2", &failList, &failListWithValues, &failureCount),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const prefix = "legacy-reader:"
	values := make(map[string][]byte, 4)
	for _, key := range []string{prefix + "a", prefix + "z"} {
		values[key] = putClusterListFixtureValue(t, fixture.nodeA, key)
	}
	for _, key := range []string{prefix + "b", prefix + "c"} {
		values[key] = putClusterListFixtureValue(t, fixture.nodeB, key)
	}

	newPage := func(withValues bool, token string) ([]string, string, bool) {
		t.Helper()
		if withValues {
			response, err := fixture.client.ListWithValues(ctx, &pb.ListRequest{
				Prefix:            prefix,
				Limit:             1,
				ContinuationToken: token,
			})
			require.NoError(t, err)
			keys := make([]string, 0, len(response.Entries))
			for _, entry := range response.Entries {
				require.NotNil(t, entry)
				require.Equal(t, values[entry.Key], entry.Value)
				keys = append(keys, entry.Key)
			}
			return keys, response.ContinuationToken, response.HasMore
		}
		response, err := fixture.client.List(ctx, &pb.ListRequest{
			Prefix:            prefix,
			Limit:             1,
			ContinuationToken: token,
		})
		require.NoError(t, err)
		return response.Keys, response.ContinuationToken, response.HasMore
	}

	for _, withValues := range []bool{false, true} {
		name := "List"
		if withValues {
			name = "ListWithValues"
		}
		t.Run(name, func(t *testing.T) {
			token := ""
			for page, want := range []string{prefix + "a", prefix + "b"} {
				keys, nextToken, hasMore := newPage(withValues, token)
				require.Equal(t, []string{want}, keys)
				require.True(t, hasMore)
				require.NotEmpty(t, nextToken)
				if page == 0 {
					require.Equal(t, prefix+"b", listNextKeyUsingLegacyToken(t, ctx, fixture, values, prefix, nextToken, withValues))
				}
				token = nextToken
			}
			require.Equal(t, prefix+"c", listNextKeyUsingLegacyToken(t, ctx, fixture, values, prefix, token, withValues))
		})
	}

	for _, withValues := range []bool{false, true} {
		name := "partial List"
		fail := &failList
		if withValues {
			name = "partial ListWithValues"
			fail = &failListWithValues
		}
		t.Run(name, func(t *testing.T) {
			fail.Store(true)
			keys, token, hasMore := newPage(withValues, "")
			require.Equal(t, []string{prefix + "a"}, keys)
			require.True(t, hasMore)
			require.NotEmpty(t, token)
			require.Equal(t, prefix+"b", listNextKeyUsingLegacyToken(t, ctx, fixture, values, prefix, token, withValues))
		})
	}
	require.Equal(t, int32(2), failureCount.Load())
}

// The base coordinator decoded only NodeCursors and Prefix, then selected each
// node's StartKey by its real ID. This mirrors that read path over live RPCs.
func listNextKeyUsingLegacyToken(
	t *testing.T,
	ctx context.Context,
	fixture *clusterListPaginationFixture,
	values map[string][]byte,
	prefix, token string,
	withValues bool,
) string {
	t.Helper()

	decoded, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err)
	var legacyToken struct {
		NodeCursors map[string]string `json:"node_cursors"`
		Prefix      string            `json:"prefix"`
	}
	require.NoError(t, json.Unmarshal(decoded, &legacyToken))
	require.Equal(t, prefix, legacyToken.Prefix)

	var candidates []string
	for _, node := range []*ClusterServerNode{fixture.nodeA, fixture.nodeB} {
		client := pb.NewCacheServiceClient(node.ClusterConn)
		startKey := legacyToken.NodeCursors[node.NodeID]
		request := &pb.ListRequest{Prefix: legacyToken.Prefix, StartKey: startKey, Limit: 1}
		if withValues {
			response, err := client.ListLocalWithValues(ctx, request)
			require.NoError(t, err)
			for _, entry := range response.Entries {
				require.NotNil(t, entry)
				require.Equal(t, values[entry.Key], entry.Value)
				candidates = append(candidates, entry.Key)
			}
			continue
		}
		response, err := client.ListLocal(ctx, request)
		require.NoError(t, err)
		candidates = append(candidates, response.Keys...)
	}
	sort.Strings(candidates)
	require.NotEmpty(t, candidates)
	return candidates[0]
}

const emptyCursorCapabilityHeaderName = "x-ocache-empty-start-key-cursor-capability"

type stripEmptyCursorCapabilityStream struct {
	grpc.ServerTransportStream
}

func (s stripEmptyCursorCapabilityStream) SetHeader(md metadata.MD) error {
	return s.ServerTransportStream.SetHeader(withoutEmptyCursorCapability(md))
}

func (s stripEmptyCursorCapabilityStream) SendHeader(md metadata.MD) error {
	return s.ServerTransportStream.SendHeader(withoutEmptyCursorCapability(md))
}

func withoutEmptyCursorCapability(md metadata.MD) metadata.MD {
	filtered := md.Copy()
	filtered.Delete(emptyCursorCapabilityHeaderName)
	return filtered
}

func emptyCursorCapabilityMissingOnNode(targetNodeID string) func(string, grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(nodeID string, next grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
		return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			if nodeID == targetNodeID && (info.FullMethod == pb.CacheService_ListLocal_FullMethodName || info.FullMethod == pb.CacheService_ListLocalWithValues_FullMethodName) {
				if stream := grpc.ServerTransportStreamFromContext(ctx); stream != nil {
					ctx = grpc.NewContextWithServerTransportStream(ctx, stripEmptyCursorCapabilityStream{ServerTransportStream: stream})
				}
			}
			return next(ctx, req, info, handler)
		}
	}
}

func testClusterListEmptyCursorCompatibilityGate(t *testing.T) {
	fixture := newClusterListPaginationFixtureWithInterceptor(t, emptyCursorCapabilityMissingOnNode("cluster-node-2"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	putClusterListFixtureValue(t, fixture.nodeA, "")
	putClusterListFixtureValue(t, fixture.nodeA, "z")
	putClusterListFixtureValue(t, fixture.nodeB, "\x00")
	putClusterListFixtureValue(t, fixture.nodeB, "a")

	for _, withValues := range []bool{false, true} {
		name := "List"
		if withValues {
			name = "ListWithValues"
		}
		t.Run(name, func(t *testing.T) {
			if withValues {
				response, err := fixture.client.ListWithValues(ctx, &pb.ListRequest{Limit: 1})
				require.Error(t, err)
				require.Nil(t, response)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				return
			}
			response, err := fixture.client.List(ctx, &pb.ListRequest{Limit: 1})
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
}

func testClusterListRawStartKey(t *testing.T) {
	fixture := newClusterListPaginationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, key := range []string{"a", "z"} {
		putClusterListFixtureValue(t, fixture.nodeA, key)
	}
	for _, key := range []string{"b", "c"} {
		putClusterListFixtureValue(t, fixture.nodeB, key)
	}
	legacyShapedBytes, err := json.Marshal(struct {
		NodeCursors map[string]string `json:"node_cursors"`
		Prefix      string            `json:"prefix"`
	}{
		NodeCursors: map[string]string{"": "zzzz"},
	})
	require.NoError(t, err)
	rawStartKey := base64.StdEncoding.EncodeToString(legacyShapedBytes)
	putClusterListFixtureValue(t, fixture.nodeA, rawStartKey)

	response, err := fixture.client.List(ctx, &pb.ListRequest{StartKey: rawStartKey, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{"z"}, response.Keys)
	require.False(t, response.HasMore)
	require.Empty(t, response.ContinuationToken)
}

func testClusterListPartialFailureRecovery(t *testing.T) {
	var failList atomic.Bool
	var failListWithValues atomic.Bool
	var failureCount atomic.Int32
	fixture := newClusterListPaginationFixtureWithInterceptor(
		t,
		listFailureInterceptorFactory("cluster-node-2", &failList, &failListWithValues, &failureCount),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	values := make(map[string][]byte, 5)
	for _, key := range []string{"m", "x", "z"} {
		values[key] = putClusterListFixtureValue(t, fixture.nodeA, key)
	}
	for _, key := range []string{"m", "n", "o"} {
		values[key] = putClusterListFixtureValue(t, fixture.nodeB, key)
	}

	listPage := func(withValues bool, token string, limit int32) ([]string, string, bool) {
		t.Helper()
		if withValues {
			response, err := fixture.client.ListWithValues(ctx, &pb.ListRequest{Limit: limit, ContinuationToken: token})
			require.NoError(t, err)
			keys := make([]string, 0, len(response.Entries))
			for _, entry := range response.Entries {
				require.NotNil(t, entry)
				require.Equal(t, values[entry.Key], entry.Value)
				keys = append(keys, entry.Key)
			}
			return keys, response.ContinuationToken, response.HasMore
		}
		response, err := fixture.client.List(ctx, &pb.ListRequest{Limit: limit, ContinuationToken: token})
		require.NoError(t, err)
		return response.Keys, response.ContinuationToken, response.HasMore
	}

	for _, withValues := range []bool{true, false} {
		name := "List"
		if withValues {
			name = "ListWithValues"
			failListWithValues.Store(true)
		} else {
			failList.Store(true)
		}
		t.Run(name, func(t *testing.T) {
			failuresBefore := failureCount.Load()
			token := ""
			var got []string
			complete := false
			for page := 0; page < 10; page++ {
				limit := int32(2)
				if page == 1 {
					limit = 1
				}
				keys, nextToken, hasMore := listPage(withValues, token, limit)
				got = append(got, keys...)
				if page == 0 {
					require.Equal(t, []string{"m", "x"}, keys)
					require.True(t, hasMore)
					require.NotEmpty(t, nextToken)
					require.Equal(t, failuresBefore+1, failureCount.Load(), "one peer page RPC should fail")
				} else if page == 1 {
					require.Equal(t, []string{"m"}, keys)
				}
				if !hasMore {
					complete = true
					break
				}
				require.NotEmpty(t, nextToken)
				token = nextToken
			}
			require.True(t, complete, "pagination should finish after the peer recovers")
			seen := make(map[string]bool, len(got))
			for _, key := range got {
				seen[key] = true
			}
			for _, key := range []string{"m", "n", "o", "x", "z"} {
				require.True(t, seen[key], "recovery omitted key %q", key)
			}
			require.Equal(t, failuresBefore+1, failureCount.Load())
		})
	}
}

func testClusterListPartialCursorAtResponseLimit(t *testing.T) {
	oldLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	defer zerolog.SetGlobalLevel(oldLogLevel)

	var failList atomic.Bool
	var failListWithValues atomic.Bool
	var failureCount atomic.Int32
	fixture := newClusterListPaginationFixtureWithInterceptor(
		t,
		listFailureInterceptorFactory("cluster-node-2", &failList, &failListWithValues, &failureCount),
	)
	client := newLargeListPaginationClient(t, fixture.nodeA)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const valueSize = 1 << 20
	largeValue := bytes.Repeat([]byte{'v'}, valueSize)
	lastValue := bytes.Repeat([]byte{'v'}, 1_046_196)
	for i := 0; i < 127; i++ {
		key := fmt.Sprintf("m%03d", i)
		require.NoError(t, fixture.nodeA.Storage.Put(key, bytes.NewReader(largeValue), 0))
	}
	require.NoError(t, fixture.nodeA.Storage.Put("m127", bytes.NewReader(lastValue), 0))
	require.NoError(t, fixture.nodeA.Storage.Put("z", bytes.NewReader([]byte("z")), 0))
	for _, key := range []string{"a", "b"} {
		require.NoError(t, fixture.nodeB.Storage.Put(key, bytes.NewReader([]byte(key)), 0))
	}

	failListWithValues.Store(true)
	first, err := client.ListWithValues(ctx, &pb.ListRequest{Limit: 128})
	require.NoError(t, err)
	require.Len(t, first.Entries, 128)
	require.Equal(t, "m000", first.Entries[0].Key)
	require.Equal(t, "m127", first.Entries[len(first.Entries)-1].Key)
	require.True(t, first.HasMore)
	require.NotEmpty(t, first.ContinuationToken)
	require.Equal(t, int32(1), failureCount.Load())

	const maxGRPCMessageSize = 128 << 20
	responseSize := proto.Size(first)
	require.LessOrEqual(t, responseSize, maxGRPCMessageSize)
	require.GreaterOrEqual(t, responseSize, maxGRPCMessageSize-4096, "fixture should exercise a near-limit page")

	second, err := client.ListWithValues(ctx, &pb.ListRequest{Limit: 128, ContinuationToken: first.ContinuationToken})
	require.NoError(t, err)
	got := make([]string, len(second.Entries))
	for i, entry := range second.Entries {
		require.NotNil(t, entry)
		got[i] = entry.Key
	}
	require.Equal(t, []string{"a", "b", "z"}, got)
	require.False(t, second.HasMore)
}

func testClusterListContinuationTokenSize(t *testing.T) {
	oldLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	defer zerolog.SetGlobalLevel(oldLogLevel)

	var failList atomic.Bool
	var failListWithValues atomic.Bool
	var failureCount atomic.Int32
	fixture := newClusterListPaginationFixtureWithNodeCount(
		t,
		3,
		listFailureInterceptorFactory("cluster-node-2", &failList, &failListWithValues, &failureCount),
	)
	client := newLargeListPaginationClient(t, fixture.nodeA)
	// A high-entropy cursor can make a public page plus its token exceed the
	// gRPC message limit even though the key and page each fit by themselves.
	const highEntropyKeySize = 72 << 20
	const prefix = "oversized/"
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
	keyBytes := make([]byte, highEntropyKeySize)
	highEntropyPrefix := []byte(prefix + "z-a/")
	copy(keyBytes, highEntropyPrefix)
	random := rand.New(rand.NewSource(20260622))
	for i := len(highEntropyPrefix); i < len(keyBytes); i++ {
		keyBytes[i] = alphabet[random.Intn(len(alphabet))]
	}
	highEntropyKey := string(keyBytes)
	keyBytes = nil
	lastSmallKey := ""
	for i := 0; i < serveroperations.MaxListLimit-1; i++ {
		key := fmt.Sprintf("%sa%04d", prefix, i)
		lastSmallKey = key
		require.NoError(t, fixture.nodeA.Storage.Put(key, bytes.NewReader([]byte("v")), 0))
	}
	tailKey := prefix + "zz"
	require.NoError(t, fixture.nodeA.Storage.Put(highEntropyKey, bytes.NewReader([]byte("v")), 0))
	require.NoError(t, fixture.nodeA.Storage.Put(tailKey, bytes.NewReader([]byte("v")), 0))

	highEntropyCtx, cancelHighEntropy := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelHighEntropy()
	for _, withValues := range []bool{false, true} {
		failuresBefore := failureCount.Load()
		var err error
		if withValues {
			failListWithValues.Store(true)
			_, err = client.ListWithValues(highEntropyCtx, &pb.ListRequest{Prefix: prefix, StartKey: lastSmallKey, Limit: 1})
		} else {
			failList.Store(true)
			_, err = client.List(highEntropyCtx, &pb.ListRequest{Prefix: prefix, StartKey: lastSmallKey, Limit: 1})
		}
		require.Equal(t, codes.ResourceExhausted, status.Code(err), "public list must fail rather than return HasMore without a cursor")
		require.Equal(t, failuresBefore+1, failureCount.Load(), "one peer page RPC should fail")
	}

	// The embedded operation has no outer gRPC page response. It keeps the
	// continuation internally and must not truncate its all-keys result.
	ops := serveroperations.New(fixture.nodeA.Storage, fixture.nodeA.Coordinator)
	failuresBefore := failureCount.Load()
	failList.Store(true)
	got, err := ops.List(highEntropyCtx, prefix)
	require.NoError(t, err)
	require.Equal(t, failuresBefore+1, failureCount.Load(), "one peer page RPC should fail")
	wantAll := make([]string, 0, serveroperations.MaxListLimit+1)
	for i := 0; i < serveroperations.MaxListLimit-1; i++ {
		wantAll = append(wantAll, fmt.Sprintf("%sa%04d", prefix, i))
	}
	wantAll = append(wantAll, highEntropyKey, tailKey)
	require.Len(t, got, len(wantAll))
	for i, want := range wantAll {
		if got[i] != want {
			t.Fatalf("List returned an unexpected key at index %d (got %d bytes, want %d bytes)", i, len(got[i]), len(want))
		}
	}
}
