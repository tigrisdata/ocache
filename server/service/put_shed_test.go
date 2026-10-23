// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// putStream feeds a fixed sequence of PutRequests and captures the response.
type putStream struct {
	grpc.ServerStream
	ctx  context.Context
	reqs []*pb.PutRequest
	resp *pb.PutResponse
}

func (p *putStream) Context() context.Context { return p.ctx }
func (p *putStream) Recv() (*pb.PutRequest, error) {
	if len(p.reqs) == 0 {
		return nil, io.EOF
	}
	r := p.reqs[0]
	p.reqs = p.reqs[1:]
	return r, nil
}
func (p *putStream) SendAndClose(r *pb.PutResponse) error { p.resp = r; return nil }

func setupShedStorage(t *testing.T) *stor.Storage {
	s, err := stor.NewStorageWithConfig(&stor.StorageConfig{
		DiskPath:        t.TempDir(),
		TTL:             3600,
		InlineThreshold: 512,
		MaxDiskUsage:    4096,
		ShedDiskRatio:   1.0,
		CleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// A streaming put flagged sheddable is refused at the owner with
// ResourceExhausted in the response code; unflagged puts still land.
func TestCacheService_Put_ShedOverBudget(t *testing.T) {
	s := setupShedStorage(t)
	svc := NewCacheService(nil, s)
	ctx := context.Background()

	fill := &putStream{ctx: ctx, reqs: []*pb.PutRequest{{Key: "fill", Data: make([]byte, 8192)}}}
	require.NoError(t, svc.Put(fill))
	require.True(t, fill.resp.Success, "plain put over the cap must land: %+v", fill.resp)

	shed := &putStream{ctx: ctx, reqs: []*pb.PutRequest{
		{Key: "shed", ShedOverBudget: true, Data: make([]byte, 600)},
		{Data: make([]byte, 600)},
	}}
	require.NoError(t, svc.Put(shed))
	require.False(t, shed.resp.Success)
	require.Equal(t, int32(codes.ResourceExhausted), shed.resp.Code, "resp=%+v", shed.resp)

	// Unary path carries the same code.
	resp, err := svc.PutObject(ctx, &pb.PutRequest{Key: "shed2", ShedOverBudget: true, Data: make([]byte, 600)})
	require.NoError(t, err)
	require.False(t, resp.Success)
	require.Equal(t, int32(codes.ResourceExhausted), resp.Code)

	// Metadata-style write without the flag still succeeds over budget.
	resp, err = svc.PutObject(ctx, &pb.PutRequest{Key: "meta", Data: []byte("marker")})
	require.NoError(t, err)
	require.True(t, resp.Success)
}
