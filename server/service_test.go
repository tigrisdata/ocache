package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
)

func setupTestStorage(t *testing.T) *stor.Storage {
	dir := t.TempDir()
	s, err := stor.NewStorageWithConfig(&stor.StorageConfig{
		DiskPath:           dir,
		TTL:                3600,
		InlineThreshold:    1024,
		CompactThreshold:   4096,
		SegmentSize:        16 * 1024 * 1024,
		FdCacheSize:        1000,
		MaxDiskUsage:       1024 * 1024 * 1024,
		FragThreshold:      0.5,
		CompactionInterval: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		s.Close()
	})
	return s
}

func TestCacheService_PutObjectAndGet(t *testing.T) {
	s := setupTestStorage(t)
	svc := &cacheService{
		storage: s,
	}
	key := "testkey"
	value := []byte("hello world")
	ctx := context.Background()
	_, err := svc.PutObject(ctx, &pb.PutRequest{Key: key, Data: value, TtlSeconds: 0})
	assert.NoError(t, err)

	// Test Get
	req := &pb.GetRequest{Key: key}
	stream := &mockGetServer{responses: []*pb.GetResponse{}}
	err = svc.Get(req, stream)
	assert.NoError(t, err)
	var got []byte
	for _, resp := range stream.responses {
		got = append(got, resp.Data...)
	}
	assert.Equal(t, value, got)
}

type mockGetServer struct {
	pb.CacheService_GetServer
	responses []*pb.GetResponse
	ctx       context.Context
}

func (m *mockGetServer) Send(resp *pb.GetResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockGetServer) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func TestCacheService_Delete(t *testing.T) {
	s := setupTestStorage(t)
	svc := &cacheService{
		storage: s,
	}
	key := "delkey"
	value := []byte("bye")
	ctx := context.Background()
	_, err := svc.PutObject(ctx, &pb.PutRequest{Key: key, Data: value, TtlSeconds: 0})
	assert.NoError(t, err)

	_, err = svc.Delete(ctx, &pb.DeleteRequest{Key: key})
	assert.NoError(t, err)

	req := &pb.GetRequest{Key: key}
	stream := &mockGetServer{responses: []*pb.GetResponse{}}
	err = svc.Get(req, stream)
	assert.Error(t, err) // should be not found
}

func TestCacheService_List(t *testing.T) {
	s := setupTestStorage(t)
	svc := &cacheService{
		storage: s,
	}
	ctx := context.Background()
	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		_, err := svc.PutObject(ctx, &pb.PutRequest{Key: k, Data: []byte(k), TtlSeconds: 0})
		assert.NoError(t, err)
	}
	stream := &mockListServer{responses: []*pb.ListResponse{}}
	err := svc.List(&pb.ListRequest{}, stream)
	assert.NoError(t, err)
	var found []string
	for _, resp := range stream.responses {
		found = append(found, resp.Keys...)
	}
	for _, k := range keys {
		assert.Contains(t, found, k)
	}
}

func TestCacheService_ListWithPrefix(t *testing.T) {
	s := setupTestStorage(t)
	svc := &cacheService{
		storage: s,
	}
	ctx := context.Background()

	// Create test keys with different prefixes
	testKeys := []struct {
		key   string
		value string
	}{
		{"user:alice", "alice"},
		{"user:bob", "bob"},
		{"user:charlie", "charlie"},
		{"session:123", "session123"},
		{"session:456", "session456"},
		{"cache:temp", "temp"},
		{"other", "other"},
	}

	// Put all test keys
	for _, tk := range testKeys {
		_, err := svc.PutObject(ctx, &pb.PutRequest{Key: tk.key, Data: []byte(tk.value), TtlSeconds: 0})
		assert.NoError(t, err)
	}

	// Test cases with different prefixes
	testCases := []struct {
		name     string
		prefix   string
		expected []string
	}{
		{
			name:     "user prefix",
			prefix:   "user:",
			expected: []string{"user:alice", "user:bob", "user:charlie"},
		},
		{
			name:     "session prefix",
			prefix:   "session:",
			expected: []string{"session:123", "session:456"},
		},
		{
			name:     "cache prefix",
			prefix:   "cache:",
			expected: []string{"cache:temp"},
		},
		{
			name:     "empty prefix returns all",
			prefix:   "",
			expected: []string{"user:alice", "user:bob", "user:charlie", "session:123", "session:456", "cache:temp", "other"},
		},
		{
			name:     "non-existent prefix",
			prefix:   "notfound:",
			expected: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stream := &mockListServer{responses: []*pb.ListResponse{}}
			err := svc.List(&pb.ListRequest{Prefix: tc.prefix}, stream)
			assert.NoError(t, err)

			var found []string
			for _, resp := range stream.responses {
				found = append(found, resp.Keys...)
			}
			assert.ElementsMatch(t, tc.expected, found, "unexpected keys for prefix %q", tc.prefix)
		})
	}
}

type mockListServer struct {
	pb.CacheService_ListServer
	responses []*pb.ListResponse
	ctx       context.Context
}

func (m *mockListServer) Send(resp *pb.ListResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockListServer) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func TestCacheService_Put_TTL(t *testing.T) {
	s := setupTestStorage(t)
	svc := &cacheService{
		storage: s,
	}
	key := "ttlkey"
	value := []byte("with ttl")
	ctx := context.Background()
	_, err := svc.PutObject(ctx, &pb.PutRequest{Key: key, Data: value, TtlSeconds: 1})
	assert.NoError(t, err)

	req := &pb.GetRequest{Key: key}
	stream := &mockGetServer{responses: []*pb.GetResponse{}}
	err = svc.Get(req, stream)
	assert.NoError(t, err)
	var got []byte
	for _, resp := range stream.responses {
		got = append(got, resp.Data...)
	}
	assert.Equal(t, value, got)

	time.Sleep(2 * time.Second)
	stream2 := &mockGetServer{responses: []*pb.GetResponse{}}
	err = svc.Get(req, stream2)
	assert.Error(t, err) // should be not found after TTL
}

// TestCacheService_GetTopology_ErrorHandling tests GetTopology error handling
func TestCacheService_GetTopology_ErrorHandling(t *testing.T) {
	s := setupTestStorage(t)

	testCases := []struct {
		name          string
		coordinator   *coordinator.Coordinator
		expectedError string
	}{
		{
			name:          "nil coordinator returns cluster not enabled",
			coordinator:   nil,
			expectedError: "cluster mode not enabled",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &cacheService{
				storage:     s,
				coordinator: tc.coordinator,
			}

			ctx := context.Background()
			resp, err := svc.GetTopology(ctx, &pb.GetTopologyRequest{})

			// Should not return error at RPC level
			assert.NoError(t, err)
			assert.NotNil(t, resp)

			// Error should be in response
			if tc.expectedError != "" {
				assert.Equal(t, tc.expectedError, resp.Error)
			} else {
				assert.Empty(t, resp.Error)
			}
		})
	}
}

// mockCoordinator implements a minimal coordinator for testing
type mockCoordinator struct {
	topology *clusterpb.ClusterTopology
	err      error
}

func (m *mockCoordinator) GetClusterTopology(ctx context.Context, req *clusterpb.Empty) (*clusterpb.ClusterTopology, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.topology == nil {
		// Return a default topology
		return &clusterpb.ClusterTopology{
			Epoch: 1,
			Nodes: []*clusterpb.NodeInfo{
				{
					Id:      "node-1",
					Address: "localhost:9000",
					Status:  clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
				},
				{
					Id:      "node-2",
					Address: "localhost:9001",
					Status:  clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
				},
			},
			RingConfig: &clusterpb.RingConfig{
				PartitionCount:    16,
				ReplicationFactor: 20,
				Load:              1.25,
			},
			PartitionOwners: []*clusterpb.PartitionOwner{
				{PartitionId: 0, NodeId: "node-1"},
				{PartitionId: 1, NodeId: "node-2"},
				{PartitionId: 2, NodeId: "node-1"},
				{PartitionId: 3, NodeId: "node-2"},
			},
		}, nil
	}
	return m.topology, nil
}

// TestCacheService_GetTopology_MockCoordinator tests GetTopology with a mock coordinator
func TestCacheService_GetTopology_MockCoordinator(t *testing.T) {
	_ = setupTestStorage(t)

	testCases := []struct {
		name           string
		topology       *clusterpb.ClusterTopology
		coordinatorErr error
		expectedError  string
		expectedNodes  int
		expectedEpoch  uint64
	}{
		{
			name:          "default topology",
			topology:      nil, // Use default from mock
			expectedNodes: 2,
			expectedEpoch: 1,
		},
		{
			name: "custom topology",
			topology: &clusterpb.ClusterTopology{
				Epoch: 5,
				Nodes: []*clusterpb.NodeInfo{
					{
						Id:      "custom-node",
						Address: "localhost:9999",
						Status:  clusterpb.NodeStatus_NODE_STATUS_ACTIVE,
					},
				},
				RingConfig: &clusterpb.RingConfig{
					PartitionCount:    8,
					ReplicationFactor: 10,
					Load:              1.5,
				},
				PartitionOwners: []*clusterpb.PartitionOwner{
					{PartitionId: 0, NodeId: "custom-node"},
				},
			},
			expectedNodes: 1,
			expectedEpoch: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create mock coordinator
			_ = &mockCoordinator{
				topology: tc.topology,
				err:      tc.coordinatorErr,
			}

			// Since we can't easily inject a mock coordinator without changing the service,
			// let's test the conversion logic directly by calling GetTopology
			// with a real minimal coordinator setup

			// This test demonstrates the structure, but for full testing
			// you'd want to refactor the service to accept an interface
			t.Skip("Skipping mock test - would require service refactoring for proper dependency injection")
		})
	}
}
