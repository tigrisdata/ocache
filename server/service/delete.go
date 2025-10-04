package service

import (
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

// Delete implementation
func (s *CacheService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("Delete").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Str("key", req.Key).Msg("gRPC Delete called")

	if req.Key == "" {
		metrics.RPCRequests.WithLabelValues("Delete", "invalid").Inc()
		userErr := status.Error(codes.InvalidArgument, "missing key")
		return &pb.DeleteResponse{Success: false, Error: userErr.Error()}, nil
	}

	// If clustering is enabled, handle routing
	if s.coordinator != nil && !s.coordinator.IsLocal(req.Key) {
		return s.handleClusteredDelete(ctx, req)
	}

	err := retry.DoWithKey(ctx, retry.DefaultConfig(), "Delete", req.Key, func() error {
		return s.storage.DeleteKey(req.Key)
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Delete", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "Delete").Inc()
		userErr := mapStorageErrorToGRPC(err)
		return &pb.DeleteResponse{Success: false, Error: userErr.Error()}, nil
	}
	metrics.RPCRequests.WithLabelValues("Delete", "success").Inc()
	return &pb.DeleteResponse{Success: true}, nil
}

// handleClusteredDelete handles Delete requests in cluster mode
func (s *CacheService) handleClusteredDelete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	// Forward to the correct node
	client, err := s.coordinator.Route(req.Key)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Delete", "routing_error").Inc()
		return &pb.DeleteResponse{
			Success: false,
			Error:   fmt.Sprintf("routing error: %v", err),
		}, nil
	}

	// Forward the request
	resp, err := client.Delete(ctx, req)
	if err != nil {
		return &pb.DeleteResponse{
			Success: false,
			Error:   fmt.Sprintf("error from remote node: %v", err),
		}, nil
	}

	metrics.RPCRequests.WithLabelValues("Delete", "forwarded").Inc()
	return resp, nil
}
