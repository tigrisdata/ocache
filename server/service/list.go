package service

import (
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/retry"
)

// Streaming List implementation
func (s *CacheService) List(req *pb.ListRequest, stream pb.CacheService_ListServer) error {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("List").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Str("prefix", req.Prefix).Msg("gRPC List called")
	// Wrap ListKeys with retry logic for retryable errors
	var keys []string
	err := retry.Do(stream.Context(), retry.DefaultConfig(), "ListKeys", func() error {
		var listErr error
		keys, listErr = s.storage.ListKeys(req.Prefix)
		return listErr
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("List", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "List").Inc()
		return mapStorageErrorToGRPC(err)
	}
	for _, key := range keys {
		if err := stream.Send(&pb.ListResponse{Keys: []string{key}}); err != nil {
			metrics.RPCRequests.WithLabelValues("List", "error").Inc()
			metrics.Errors.WithLabelValues("grpc", "List").Inc()
			return err
		}
	}
	metrics.RPCRequests.WithLabelValues("List", "success").Inc()
	return nil
}
