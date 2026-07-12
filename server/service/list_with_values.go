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

// ListWithValues implements the public ListWithValues RPC.
// Returns sorted, paginated key-value pairs with continuation token.
func (s *CacheService) ListWithValues(ctx context.Context, req *pb.ListRequest) (*pb.ListWithValuesResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("ListWithValues").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().
		Str("prefix", req.Prefix).
		Str("start_key", req.StartKey).
		Int32("limit", req.Limit).
		Str("continuation_token", req.ContinuationToken).
		Msg("gRPC ListWithValues called")

	continuationToken := req.ContinuationToken
	if continuationToken == "" && req.StartKey != "" {
		continuationToken = req.StartKey
	}

	entries, token, hasMore, err := s.ops.ListPageWithValues(ctx, req.Prefix, int(req.Limit), continuationToken)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("ListWithValues", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "ListWithValues").Inc()
		return nil, mapStorageErrorToGRPC(err)
	}

	metrics.RPCRequests.WithLabelValues("ListWithValues", "success").Inc()
	return &pb.ListWithValuesResponse{
		Entries:           entries,
		ContinuationToken: token,
		HasMore:           hasMore,
	}, nil
}

// ListLocalWithValues implements the internal node-local ListWithValues RPC.
// Returns sorted, paginated key-value pairs from this node's local storage.
func (s *CacheService) ListLocalWithValues(ctx context.Context, req *pb.ListRequest) (*pb.ListWithValuesResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("ListLocalWithValues").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().
		Str("prefix", req.Prefix).
		Str("start_key", req.StartKey).
		Int32("limit", req.Limit).
		Msg("gRPC ListLocalWithValues called")

	continuationToken := req.ContinuationToken
	if continuationToken == "" && req.StartKey != "" {
		continuationToken = req.StartKey
	}

	entries, token, hasMore, err := s.ops.ListLocalWithValues(ctx, req.Prefix, int(req.Limit), continuationToken)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("ListLocalWithValues", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "ListLocalWithValues").Inc()
		return nil, mapStorageErrorToGRPC(err)
	}

	metrics.RPCRequests.WithLabelValues("ListLocalWithValues", "success").Inc()
	return &pb.ListWithValuesResponse{
		Entries:           entries,
		ContinuationToken: token,
		HasMore:           hasMore,
	}, nil
}
