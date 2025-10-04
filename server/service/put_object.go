package service

import (
	"bytes"
	"context"
	"fmt"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/retry"
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

	// If clustering is enabled, handle routing
	if s.coordinator != nil && !s.coordinator.IsLocal(req.Key) {
		return s.handleClusteredPutObject(ctx, req)
	}

	// Use the same logic as the streaming Put, but for a single chunk
	// Wrap with retry logic for retryable errors
	err := retry.DoWithKey(ctx, retry.DefaultConfig(), "PutObject", req.Key, func() error {
		return s.storage.Put(req.Key, bytes.NewReader(req.Data), int(req.TtlSeconds))
	})
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

// handleClusteredPutObject handles unary PutObject requests in cluster mode
func (s *CacheService) handleClusteredPutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// Forward to the correct node
	client, err := s.coordinator.Route(req.Key)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("PutObject", "routing_error").Inc()
		return &pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("routing error: %v", err),
		}, nil
	}

	// Forward the request
	resp, err := client.PutObject(ctx, req)
	if err != nil {
		return &pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("error from remote node: %v", err),
		}, nil
	}

	metrics.RPCRequests.WithLabelValues("PutObject", "forwarded").Inc()
	return resp, nil
}
