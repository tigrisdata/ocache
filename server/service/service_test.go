package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
)

func setupTestStorage(t *testing.T) *stor.Storage {
	dir := t.TempDir()
	s, err := stor.NewStorageWithConfig(&stor.StorageConfig{
		DiskPath:         dir,
		TTL:              3600,
		InlineThreshold:  1024,
		CompactThreshold: 4096,
		SegmentSize:      16 * 1024 * 1024,
		FdCacheSize:      1000,
		MaxDiskUsage:     1024 * 1024 * 1024,
		FragThreshold:    0.5,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		s.Close()
	})
	return s
}

func TestCacheService_PutObjectAndGet(t *testing.T) {
	s := setupTestStorage(t)
	svc := &CacheService{
		storage: s,
	}
	key := "testkey"
	value := []byte("hello world")
	ctx := context.Background()
	_, err := svc.PutObject(ctx, &pb.PutRequest{Key: key, Data: value, TtlSeconds: 0})
	assert.NoError(t, err)

	// Test Get (end=-1 means read to EOF)
	req := &pb.GetRequest{Key: key, End: -1}
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
	svc := &CacheService{
		storage: s,
	}
	key := "delkey"
	value := []byte("bye")
	ctx := context.Background()
	_, err := svc.PutObject(ctx, &pb.PutRequest{Key: key, Data: value, TtlSeconds: 0})
	assert.NoError(t, err)

	_, err = svc.Delete(ctx, &pb.DeleteRequest{Key: key})
	assert.NoError(t, err)

	req := &pb.GetRequest{Key: key, End: -1}
	stream := &mockGetServer{responses: []*pb.GetResponse{}}
	err = svc.Get(req, stream)
	assert.Error(t, err) // should be not found
}

func TestCacheService_List(t *testing.T) {
	s := setupTestStorage(t)
	svc := &CacheService{
		storage: s,
	}
	ctx := context.Background()
	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		_, err := svc.PutObject(ctx, &pb.PutRequest{Key: k, Data: []byte(k), TtlSeconds: 0})
		assert.NoError(t, err)
	}

	resp, err := svc.List(ctx, &pb.ListRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	found := resp.Keys
	for _, k := range keys {
		assert.Contains(t, found, k)
	}

	// Verify keys are sorted
	for i := 1; i < len(found); i++ {
		assert.LessOrEqual(t, found[i-1], found[i], "Keys should be in sorted order")
	}
}

func TestCacheService_ListWithPrefix(t *testing.T) {
	s := setupTestStorage(t)
	svc := &CacheService{
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
			resp, err := svc.List(ctx, &pb.ListRequest{Prefix: tc.prefix})
			assert.NoError(t, err)
			assert.NotNil(t, resp)

			found := resp.Keys
			assert.ElementsMatch(t, tc.expected, found, "unexpected keys for prefix %q", tc.prefix)

			// Verify keys are sorted
			for i := 1; i < len(found); i++ {
				assert.LessOrEqual(t, found[i-1], found[i], "Keys should be in sorted order for prefix %q", tc.prefix)
			}
		})
	}
}

func TestCacheService_List_Pagination(t *testing.T) {
	s := setupTestStorage(t)
	svc := &CacheService{
		storage: s,
	}
	ctx := context.Background()

	// Create 50 keys to test pagination
	keyCount := 50
	for i := 0; i < keyCount; i++ {
		key := string('a'+rune(i/26)) + string('a'+rune(i%26))
		_, err := svc.PutObject(ctx, &pb.PutRequest{Key: key, Data: []byte(key), TtlSeconds: 0})
		assert.NoError(t, err)
	}

	// Test pagination with limit
	resp, err := svc.List(ctx, &pb.ListRequest{Limit: 10})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.LessOrEqual(t, len(resp.Keys), 10, "Should respect limit")

	// Verify sorted order
	for i := 1; i < len(resp.Keys); i++ {
		assert.LessOrEqual(t, resp.Keys[i-1], resp.Keys[i], "Keys should be sorted")
	}

	// If there are more keys, continuation token should be present
	if resp.HasMore {
		assert.NotEmpty(t, resp.ContinuationToken, "Continuation token should be present when hasMore=true")

		// Get next page
		resp2, err := svc.List(ctx, &pb.ListRequest{
			Limit:             10,
			ContinuationToken: resp.ContinuationToken,
		})
		assert.NoError(t, err)
		assert.NotNil(t, resp2)

		// Verify no overlap between pages
		firstPageKeys := make(map[string]bool)
		for _, k := range resp.Keys {
			firstPageKeys[k] = true
		}
		for _, k := range resp2.Keys {
			assert.False(t, firstPageKeys[k], "Pages should not overlap: key %s in both pages", k)
		}

		// Verify continuation of sort order across pages
		if len(resp.Keys) > 0 && len(resp2.Keys) > 0 {
			lastKeyPage1 := resp.Keys[len(resp.Keys)-1]
			firstKeyPage2 := resp2.Keys[0]
			assert.Less(t, lastKeyPage1, firstKeyPage2, "Sort order should continue across pages")
		}
	}
}

func TestCacheService_ListLocal(t *testing.T) {
	s := setupTestStorage(t)
	svc := &CacheService{
		storage: s,
	}
	ctx := context.Background()

	// Create test keys
	keys := []string{"local-a", "local-b", "local-c"}
	for _, k := range keys {
		_, err := svc.PutObject(ctx, &pb.PutRequest{Key: k, Data: []byte(k), TtlSeconds: 0})
		assert.NoError(t, err)
	}

	// Test ListLocal (now single-response, not streaming)
	resp, err := svc.ListLocal(ctx, &pb.ListRequest{Prefix: "local-"})
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	found := resp.Keys

	// Verify all keys are present
	assert.ElementsMatch(t, keys, found)

	// Verify keys are sorted
	for i := 1; i < len(found); i++ {
		assert.LessOrEqual(t, found[i-1], found[i], "Keys should be in sorted order")
	}
}

func TestCacheService_Put_TTL(t *testing.T) {
	s := setupTestStorage(t)
	svc := &CacheService{
		storage: s,
	}
	key := "ttlkey"
	value := []byte("with ttl")
	ctx := context.Background()
	_, err := svc.PutObject(ctx, &pb.PutRequest{Key: key, Data: value, TtlSeconds: 1})
	assert.NoError(t, err)

	req := &pb.GetRequest{Key: key, End: -1}
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
			svc := &CacheService{
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
