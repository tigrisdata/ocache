package service

import (
	"io"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/bufferpool"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Streaming Get for large values with byte-range support
func (s *CacheService) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	startTime := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("Get").Observe(float64(time.Since(startTime).Milliseconds()))
	}()

	zlog.Debug().Str("key", req.Key).Int64("start", req.Start).Int64("end", req.End).Msg("gRPC Get called")
	metrics.StreamsActive.Inc()
	defer metrics.StreamsActive.Dec()

	// If clustering is enabled, handle routing
	if s.coordinator != nil && !s.coordinator.IsLocal(req.Key) {
		return s.forwardStreamingGet(req, stream)
	}

	// Wrap Get with retry logic for retryable errors
	var r io.Reader
	var found bool
	err := retry.DoWithKey(stream.Context(), retry.DefaultConfig(), "Get", req.Key, func() error {
		var getErr error
		r, found, getErr = s.storage.Get(req.Key, req.Start, req.End)
		return getErr
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Get", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "Get").Inc()
		return mapStorageErrorToGRPC(err)
	}
	if !found {
		metrics.RPCRequests.WithLabelValues("Get", "not_found").Inc()
		return status.Error(codes.NotFound, "key not found")
	}

	// Ensure the reader is closed to release any file locks
	if closer, ok := r.(io.Closer); ok {
		defer closer.Close()
	}

	// Stream the data in chunks
	buf, release := bufferpool.AcquireBuffer(1 << 20) // 1 MiB
	defer release()
	for {
		readN, err := r.Read(buf)
		if readN > 0 {
			if err := stream.Send(&pb.GetResponse{Data: buf[:readN]}); err != nil {
				metrics.RPCRequests.WithLabelValues("Get", "error").Inc()
				metrics.Errors.WithLabelValues("grpc", "Get").Inc()
				return err
			}
			metrics.StreamBytesTransferred.WithLabelValues("download").Add(float64(readN))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			metrics.RPCRequests.WithLabelValues("Get", "error").Inc()
			metrics.Errors.WithLabelValues("grpc", "Get").Inc()
			return err
		}
	}
	metrics.RPCRequests.WithLabelValues("Get", "success").Inc()
	return nil
}

// forwardStreamingGet forwards a streaming Get request to a remote node
func (s *CacheService) forwardStreamingGet(req *pb.GetRequest, localStream pb.CacheService_GetServer) error {
	// Forward to the correct node
	client, err := s.coordinator.Route(req.Key)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Get", "routing_error").Inc()
		return status.Errorf(codes.Unavailable, "routing error: %v", err)
	}

	ctx := localStream.Context()

	// Increment hop count for forwarding loop detection
	ctx, err = coordinator.IncrementHopCount(ctx, s.coordinator.GetLocalNodeID())
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Get", "hop_limit_exceeded").Inc()
		return err // IncrementHopCount already returns a status error
	}

	// Create a streaming Get call to the remote node
	remoteStream, err := client.Get(ctx, req)
	if err != nil {
		return status.Errorf(codes.Unavailable, "failed to connect to remote node: %v", err)
	}

	// Forward chunks from remote to local stream
	for {
		chunk, err := remoteStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Unavailable, "error from remote node: %v", err)
		}

		if err := localStream.Send(chunk); err != nil {
			return err
		}
		metrics.StreamBytesTransferred.WithLabelValues("download").Add(float64(len(chunk.Data)))
	}

	metrics.RPCRequests.WithLabelValues("Get", "forwarded").Inc()
	return nil
}
