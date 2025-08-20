package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
)

func setupTestStorage(t *testing.T) {
	dir := t.TempDir()
	stor.InitStorageWithConfig(&stor.StorageConfig{
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
}

func TestCacheService_PutObjectAndGet(t *testing.T) {
	setupTestStorage(t)
	svc := &cacheService{}
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
}

func (m *mockGetServer) Send(resp *pb.GetResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func TestCacheService_Delete(t *testing.T) {
	setupTestStorage(t)
	svc := &cacheService{}
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
	setupTestStorage(t)
	svc := &cacheService{}
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

type mockListServer struct {
	pb.CacheService_ListServer
	responses []*pb.ListResponse
}

func (m *mockListServer) Send(resp *pb.ListResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func TestCacheService_Put_TTL(t *testing.T) {
	setupTestStorage(t)
	svc := &cacheService{}
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
