// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PutObject implements the unary REST/HTTP endpoint for cache put
func (s *CacheService) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("PutObject").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Str("key", req.Key).Int64("ttl", req.TtlSeconds).Int("data_len", len(req.Data)).Msg("PutObject called (unary for REST)")

	if req.Key == "" {
		metrics.RPCRequests.WithLabelValues("PutObject", "invalid").Inc()
		userErr := status.Error(codes.InvalidArgument, "missing key")
		return &pb.PutResponse{Success: false, Error: userErr.Error()}, nil
	}

	// Use operations layer for routing and storage
	// Operations.PutBytes handles routing internally (local vs remote)
	err := s.ops.PutBytes(ctx, req.Key, req.Data, int(req.TtlSeconds))
	if err != nil {
		metrics.RPCRequests.WithLabelValues("PutObject", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "PutObject").Inc()
		// Convert storage error to user-friendly message
		userErr := mapStorageErrorToGRPC(err)
		return &pb.PutResponse{Success: false, Error: userErr.Error()}, nil
	}
	metrics.RPCRequests.WithLabelValues("PutObject", "success").Inc()
	return &pb.PutResponse{Success: true}, nil
}
