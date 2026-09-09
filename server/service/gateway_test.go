// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type gatewayTestClient struct {
	pb.CacheServiceClient
	putObject func(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error)
}

func (c *gatewayTestClient) PutObject(ctx context.Context, req *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
	return c.putObject(ctx, req, opts...)
}

type gatewayTestRouter struct {
	owner       *ring.NodeInfo
	resolveErr  error
	routeErr    error
	localID     string
	remote      pb.CacheServiceClient
	resolveCall int
	routeCall   int
}

func (r *gatewayTestRouter) Resolve(string) (*ring.NodeInfo, error) {
	r.resolveCall++
	return r.owner, r.resolveErr
}

func (r *gatewayTestRouter) RouteToNode(string) (pb.CacheServiceClient, error) {
	r.routeCall++
	return r.remote, r.routeErr
}

func (r *gatewayTestRouter) GetLocalNodeID() string {
	return r.localID
}

func TestGatewayCacheServiceClient_RoutesEligibleRemotePutObject(t *testing.T) {
	var localCalls, remoteCalls int
	var gotCtx context.Context
	var gotReq *pb.PutRequest
	var gotOpts int

	local := &gatewayTestClient{putObject: func(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error) {
		localCalls++
		return &pb.PutResponse{Success: true}, nil
	}}
	remote := &gatewayTestClient{putObject: func(ctx context.Context, req *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
		remoteCalls++
		gotCtx = ctx
		gotReq = req
		gotOpts = len(opts)
		return &pb.PutResponse{Success: true}, nil
	}}
	router := &gatewayTestRouter{
		owner:   &ring.NodeInfo{ID: "owner"},
		localID: "gateway",
		remote:  remote,
	}
	client := &gatewayCacheServiceClient{CacheServiceClient: local, router: router}

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		coordinator.MetadataKeyHop, "1",
		coordinator.MetadataKeyOrigin, "origin",
		"authorization", "bearer",
	))
	req := &pb.PutRequest{Key: "remote-key", Data: []byte("value"), TtlSeconds: 7}
	wantOption := grpc.StaticMethod()
	resp, err := client.PutObject(ctx, req, wantOption)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.Zero(t, localCalls)
	assert.Equal(t, 1, remoteCalls)
	assert.Equal(t, 1, router.resolveCall)
	assert.Equal(t, 1, router.routeCall)
	assert.Same(t, req, gotReq)
	assert.Equal(t, 1, gotOpts)

	outgoing, ok := metadata.FromOutgoingContext(gotCtx)
	require.True(t, ok)
	assert.Equal(t, []string{"2"}, outgoing.Get(coordinator.MetadataKeyHop))
	assert.Equal(t, []string{"true"}, outgoing.Get(coordinator.MetadataKeyForwarded))
	assert.Equal(t, []string{"origin"}, outgoing.Get(coordinator.MetadataKeyOrigin))
	assert.Equal(t, []string{"bearer"}, outgoing.Get("authorization"))
}

func TestGatewayCacheServiceClientKeepsOrdinaryPutObjectOnLocalClient(t *testing.T) {
	tests := []struct {
		name        string
		req         *pb.PutRequest
		router      *gatewayTestRouter
		wantResolve bool
	}{
		{
			name: "cluster disabled",
			req:  &pb.PutRequest{Key: "key", Data: []byte("value")},
		},
		{
			name: "invalid key",
			req:  &pb.PutRequest{Data: []byte("value")},
			router: &gatewayTestRouter{
				owner:   &ring.NodeInfo{ID: "owner"},
				localID: "gateway",
			},
		},
		{
			name:        "local owner",
			req:         &pb.PutRequest{Key: "key", Data: []byte("value")},
			wantResolve: true,
			router: &gatewayTestRouter{
				owner:   &ring.NodeInfo{ID: "gateway"},
				localID: "gateway",
			},
		},
		{
			name: "large body",
			req:  &pb.PutRequest{Key: "key", Data: bytes.Repeat([]byte("x"), gatewayUnaryForwardLimit+1)},
			router: &gatewayTestRouter{
				owner:   &ring.NodeInfo{ID: "owner"},
				localID: "gateway",
			},
		},
		{
			name:        "owner lookup failure",
			req:         &pb.PutRequest{Key: "key", Data: []byte("value")},
			wantResolve: true,
			router: &gatewayTestRouter{
				localID:    "gateway",
				resolveErr: errors.New("ring unavailable"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localCalls := 0
			local := &gatewayTestClient{putObject: func(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error) {
				localCalls++
				return &pb.PutResponse{Success: true}, nil
			}}
			var router gatewayRouter
			if tt.router != nil {
				router = tt.router
			}
			client := &gatewayCacheServiceClient{CacheServiceClient: local, router: router}

			resp, err := client.PutObject(context.Background(), tt.req)

			require.NoError(t, err)
			assert.True(t, resp.Success)
			assert.Equal(t, 1, localCalls)
			if tt.router != nil {
				assert.Zero(t, tt.router.routeCall)
				if tt.wantResolve {
					assert.Equal(t, 1, tt.router.resolveCall)
				} else {
					assert.Zero(t, tt.router.resolveCall)
				}
			}
		})
	}
}

func TestGatewayCacheServiceClientDelegatesOverHopLimit(t *testing.T) {
	localCalls := 0
	remoteCalls := 0
	local := &gatewayTestClient{putObject: func(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error) {
		localCalls++
		return &pb.PutResponse{Success: true}, nil
	}}
	remote := &gatewayTestClient{putObject: func(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error) {
		remoteCalls++
		return &pb.PutResponse{Success: true}, nil
	}}
	router := &gatewayTestRouter{
		owner:   &ring.NodeInfo{ID: "owner"},
		localID: "gateway",
		remote:  remote,
	}
	client := &gatewayCacheServiceClient{CacheServiceClient: local, router: router}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		coordinator.MetadataKeyHop, strconv.Itoa(coordinator.MaxHops+1),
	))

	resp, err := client.PutObject(ctx, &pb.PutRequest{Key: "key", Data: []byte("value")})

	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, 1, localCalls)
	assert.Zero(t, remoteCalls)
	assert.Zero(t, router.resolveCall)
}

func TestGatewayCacheServiceClientTranslatesForwardingFailures(t *testing.T) {
	tests := []struct {
		name      string
		routeErr  error
		remoteErr error
		wantError string
	}{
		{
			name:      "route failure",
			routeErr:  errors.New("owner unavailable"),
			wantError: "owner unavailable",
		},
		{
			name:      "remote failure",
			remoteErr: errors.New("rpc failed"),
			wantError: "rpc failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			local := &gatewayTestClient{putObject: func(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error) {
				return &pb.PutResponse{Success: true}, nil
			}}
			remote := &gatewayTestClient{putObject: func(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error) {
				return nil, tt.remoteErr
			}}
			router := &gatewayTestRouter{
				owner:    &ring.NodeInfo{ID: "owner"},
				localID:  "gateway",
				remote:   remote,
				routeErr: tt.routeErr,
			}
			client := &gatewayCacheServiceClient{CacheServiceClient: local, router: router}

			resp, err := client.PutObject(context.Background(), &pb.PutRequest{Key: "key", Data: []byte("value")})

			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.False(t, resp.Success)
			assert.Equal(t, tt.wantError, resp.Error)
		})
	}
}

func TestGatewayCacheServiceClientPreservesRemoteResponse(t *testing.T) {
	local := &gatewayTestClient{putObject: func(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error) {
		return &pb.PutResponse{Success: true}, nil
	}}
	remoteResponse := &pb.PutResponse{Success: false, Error: "owner rejected write"}
	remote := &gatewayTestClient{putObject: func(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error) {
		return remoteResponse, nil
	}}
	router := &gatewayTestRouter{
		owner:   &ring.NodeInfo{ID: "owner"},
		localID: "gateway",
		remote:  remote,
	}
	client := &gatewayCacheServiceClient{CacheServiceClient: local, router: router}

	resp, err := client.PutObject(context.Background(), &pb.PutRequest{Key: "key", Data: []byte("value")})

	require.NoError(t, err)
	assert.Same(t, remoteResponse, resp)
}
