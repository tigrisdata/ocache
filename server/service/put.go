package service

import (
	"fmt"
	"io"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	pb "github.com/tigrisdata/ocache/proto"
)

// Streaming Put for large values
func (s *CacheService) Put(stream pb.CacheService_PutServer) error {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("Put").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Msg("gRPC Put called")
	metrics.StreamsActive.Inc()
	defer metrics.StreamsActive.Dec()

	// Read the first chunk to get key and ttl
	firstChunk, err := stream.Recv()
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "Put").Inc()
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
	}

	key := firstChunk.Key
	if key == "" {
		metrics.RPCRequests.WithLabelValues("Put", "invalid").Inc()
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: "missing key"})
	}

	// If clustering is enabled, handle routing
	if s.coordinator != nil && !s.coordinator.IsLocal(key) {
		return s.forwardStreamingPut(stream, firstChunk)
	}

	// Handle locally - reconstruct the stream for local processing
	return s.handleLocalPut(stream, firstChunk)
}

// handleLocalPut processes a Put request locally
func (s *CacheService) handleLocalPut(stream pb.CacheService_PutServer, firstChunk *pb.PutRequest) error {
	key := firstChunk.Key
	ttl := int(firstChunk.TtlSeconds)

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	// Start storage.Put in a goroutine so it can consume the pipe as we write to it
	go func() {
		// Note: We don't retry streaming Put operations at service layer since
		// the client would need to resend the entire stream
		errCh <- s.storage.Put(key, pr, ttl)
	}()

	// Write the first chunk's data if any
	if len(firstChunk.Data) > 0 {
		if _, err := pw.Write(firstChunk.Data); err != nil {
			pw.CloseWithError(err)
			<-errCh // wait for storage.Put to finish
			metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
			metrics.Errors.WithLabelValues("grpc", "Put").Inc()
			return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
		}
		metrics.StreamBytesTransferred.WithLabelValues("upload").Add(float64(len(firstChunk.Data)))
	}

	// Read remaining chunks from the stream and write to the pipe
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			pw.CloseWithError(err)
			<-errCh // wait for storage.Put to finish

			metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
			metrics.Errors.WithLabelValues("grpc", "Put").Inc()
			return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
		}
		if len(chunk.Data) > 0 {
			if _, err := pw.Write(chunk.Data); err != nil {
				pw.CloseWithError(err)
				<-errCh // wait for storage.Put to finish

				metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
				metrics.Errors.WithLabelValues("grpc", "Put").Inc()
				return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
			}
			metrics.StreamBytesTransferred.WithLabelValues("upload").Add(float64(len(chunk.Data)))
		}
	}
	pw.Close()

	err := <-errCh // wait for storage.Put to finish
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "Put").Inc()
		// Convert storage error to user-friendly message
		userErr := mapStorageErrorToGRPC(err)
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: userErr.Error()})
	}

	zlog.Debug().Str("key", key).Msg("Streaming put completed successfully")
	metrics.RPCRequests.WithLabelValues("Put", "success").Inc()
	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

// forwardStreamingPut forwards a streaming Put request to a remote node
func (s *CacheService) forwardStreamingPut(localStream pb.CacheService_PutServer, firstChunk *pb.PutRequest) error {
	key := firstChunk.Key

	// Forward to the correct node
	client, err := s.coordinator.Route(key)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Put", "routing_error").Inc()
		return localStream.SendAndClose(&pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("routing error: %v", err),
		})
	}

	ctx := localStream.Context()

	// Create a streaming Put call to the remote node
	remoteStream, err := client.Put(ctx)
	if err != nil {
		return localStream.SendAndClose(&pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to connect to remote node: %v", err),
		})
	}

	// Send the first chunk we already received
	if err := remoteStream.Send(firstChunk); err != nil {
		return localStream.SendAndClose(&pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to forward to remote node: %v", err),
		})
	}

	// Forward remaining chunks
	for {
		chunk, err := localStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			remoteStream.CloseSend()
			return localStream.SendAndClose(&pb.PutResponse{
				Success: false,
				Error:   fmt.Sprintf("error receiving chunk: %v", err),
			})
		}

		if err := remoteStream.Send(chunk); err != nil {
			return localStream.SendAndClose(&pb.PutResponse{
				Success: false,
				Error:   fmt.Sprintf("failed to forward chunk to remote node: %v", err),
			})
		}
	}

	// Close the remote stream and get the response
	resp, err := remoteStream.CloseAndRecv()
	if err != nil {
		return localStream.SendAndClose(&pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("error from remote node: %v", err),
		})
	}

	metrics.RPCRequests.WithLabelValues("Put", "forwarded").Inc()
	return localStream.SendAndClose(resp)
}
