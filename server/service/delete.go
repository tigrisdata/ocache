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

	// Use operations layer for routing and storage
	// Operations.Delete handles routing internally (local vs remote)
	err := s.ops.Delete(ctx, req.Key)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Delete", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "Delete").Inc()
		userErr := mapStorageErrorToGRPC(err)
		return &pb.DeleteResponse{Success: false, Error: userErr.Error()}, nil
	}
	metrics.RPCRequests.WithLabelValues("Delete", "success").Inc()
	return &pb.DeleteResponse{Success: true}, nil
}
