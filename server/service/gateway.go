// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"context"

	"github.com/tigrisdata/ocache/coordinator"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
)

// gatewayUnaryForwardLimit keeps the HTTP gateway's direct owner call on the
// bounded unary path. Larger bodies continue through the local service, whose
// existing Operations.PutBytes path forwards them with the streaming RPC.
const gatewayUnaryForwardLimit = 4 * 1024 * 1024

type gatewayRouter interface {
	Resolve(key string) (*ring.NodeInfo, error)
	RouteToNode(nodeID string) (pb.CacheServiceClient, error)
	GetLocalNodeID() string
}

// gatewayCacheServiceClient keeps the generated gateway on its normal local
// client for every method except the eligible unary PutObject path. Embedding
// the generated client means new and streaming methods retain their existing
// transport and handler behavior.
type gatewayCacheServiceClient struct {
	pb.CacheServiceClient
	router gatewayRouter
}

func newGatewayCacheServiceClient(local pb.CacheServiceClient, coord *coordinator.Coordinator) *gatewayCacheServiceClient {
	var router gatewayRouter
	if coord != nil {
		router = coord
	}
	return &gatewayCacheServiceClient{
		CacheServiceClient: local,
		router:             router,
	}
}

func (c *gatewayCacheServiceClient) PutObject(ctx context.Context, req *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
	if c.router == nil || req == nil || req.Key == "" || len(req.Data) > gatewayUnaryForwardLimit {
		return c.CacheServiceClient.PutObject(ctx, req, opts...)
	}
	// Let the local gRPC interceptor preserve its transport-level response for
	// requests that have already exceeded the forwarding limit.
	if err := coordinator.CheckHopCount(coordinator.ExtractRequestMetadata(ctx).HopCount); err != nil {
		return c.CacheServiceClient.PutObject(ctx, req, opts...)
	}

	owner, err := c.router.Resolve(req.Key)
	if err != nil || owner == nil || owner.ID == "" || owner.ID == c.router.GetLocalNodeID() {
		return c.CacheServiceClient.PutObject(ctx, req, opts...)
	}

	forwardedCtx, err := coordinator.IncrementHopCount(ctx, c.router.GetLocalNodeID())
	if err != nil {
		return gatewayPutError(err)
	}

	remote, err := c.router.RouteToNode(owner.ID)
	if err != nil {
		return gatewayPutError(err)
	}

	resp, err := remote.PutObject(forwardedCtx, req, opts...)
	if err != nil {
		return gatewayPutError(err)
	}
	return resp, nil
}

func gatewayPutError(err error) (*pb.PutResponse, error) {
	userErr := mapStorageErrorToGRPC(err)
	return &pb.PutResponse{Success: false, Error: userErr.Error()}, nil
}
