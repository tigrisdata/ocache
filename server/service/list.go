// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	pb "github.com/tigrisdata/ocache/proto"
)

// List implements the public List RPC
// In cluster mode, coordinates K-way merge across all nodes via operations layer
// Returns a single response with sorted, paginated keys and continuation token
func (s *CacheService) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("List").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().
		Str("prefix", req.Prefix).
		Str("start_key", req.StartKey).
		Int32("limit", req.Limit).
		Str("continuation_token", req.ContinuationToken).
		Msg("gRPC List called")

	// Use operations layer for cluster-wide list (K-way merge inside)
	// Operations handles both single-node and cluster mode automatically
	continuationToken := req.ContinuationToken
	if continuationToken == "" && req.StartKey != "" {
		// For single-node compatibility, treat StartKey as continuation token
		continuationToken = req.StartKey
	}

	keys, token, hasMore, err := s.ops.ListPage(ctx, req.Prefix, int(req.Limit), continuationToken)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("List", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "List").Inc()
		return nil, mapStorageErrorToGRPC(err)
	}

	metrics.RPCRequests.WithLabelValues("List", "success").Inc()
	return &pb.ListResponse{
		Keys:              keys,
		ContinuationToken: token,
		HasMore:           hasMore,
	}, nil
}

// ListLocal implements the internal node-local List RPC
// This is used by the coordinator to query individual nodes in cluster mode
// Returns sorted, paginated keys from this node's local storage
func (s *CacheService) ListLocal(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("ListLocal").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().
		Str("prefix", req.Prefix).
		Str("start_key", req.StartKey).
		Int32("limit", req.Limit).
		Msg("gRPC ListLocal called")

	// Use operations layer for local-only list
	continuationToken := req.ContinuationToken
	if continuationToken == "" && req.StartKey != "" {
		continuationToken = req.StartKey
	}

	keys, token, hasMore, err := s.ops.ListLocal(ctx, req.Prefix, int(req.Limit), continuationToken)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("ListLocal", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "ListLocal").Inc()
		return nil, mapStorageErrorToGRPC(err)
	}

	metrics.RPCRequests.WithLabelValues("ListLocal", "success").Inc()
	return &pb.ListResponse{
		Keys:              keys,
		ContinuationToken: token,
		HasMore:           hasMore,
	}, nil
}
