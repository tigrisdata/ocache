package service

import (
	"bytes"
	"context"
	"fmt"
	"io"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// handleClusteredPut handles Put requests in cluster mode
func (s *CacheService) handleClusteredPut(stream pb.CacheService_PutServer) error {
	// Read the first chunk to get the key
	firstChunk, err := stream.Recv()
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
	}

	key := firstChunk.Key
	if key == "" {
		metrics.RPCRequests.WithLabelValues("Put", "invalid").Inc()
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: "missing key"})
	}

	// Check if this node owns the key
	if !s.coordinator.IsLocal(key) {
		// Forward to the correct node
		client, err := s.coordinator.Route(key)
		if err != nil {
			metrics.RPCRequests.WithLabelValues("Put", "routing_error").Inc()
			return stream.SendAndClose(&pb.PutResponse{
				Success: false,
				Error:   fmt.Sprintf("routing error: %v", err),
			})
		}

		// Forward the streaming Put to the remote node
		return s.forwardStreamingPut(stream, client, firstChunk)
	}

	// Handle locally - reconstruct the stream for local processing
	return s.handleLocalPut(stream, firstChunk)
}

// forwardStreamingPut forwards a streaming Put request to a remote node
func (s *CacheService) forwardStreamingPut(localStream pb.CacheService_PutServer, client pb.CacheServiceClient, firstChunk *pb.PutRequest) error {
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

// handleLocalPut processes a Put request locally
func (s *CacheService) handleLocalPut(stream pb.CacheService_PutServer, firstChunk *pb.PutRequest) error {
	key := firstChunk.Key
	ttl := int(firstChunk.TtlSeconds)

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	// Start storage.Put in a goroutine
	go func() {
		errCh <- s.storage.Put(key, pr, ttl)
	}()

	// Write the first chunk's data if any
	if len(firstChunk.Data) > 0 {
		if _, err := pw.Write(firstChunk.Data); err != nil {
			pw.CloseWithError(err)
			<-errCh
			metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
			return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
		}
		metrics.StreamBytesTransferred.WithLabelValues("upload").Add(float64(len(firstChunk.Data)))
	}

	// Read remaining chunks
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			pw.CloseWithError(err)
			<-errCh
			metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
			return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
		}
		if len(chunk.Data) > 0 {
			if _, err := pw.Write(chunk.Data); err != nil {
				pw.CloseWithError(err)
				<-errCh
				metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
				return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
			}
			metrics.StreamBytesTransferred.WithLabelValues("upload").Add(float64(len(chunk.Data)))
		}
	}
	pw.Close()

	err := <-errCh
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
		userErr := mapStorageErrorToGRPC(err)
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: userErr.Error()})
	}

	zlog.Debug().Str("key", key).Msg("Local put completed successfully")
	metrics.RPCRequests.WithLabelValues("Put", "success").Inc()
	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

// handleClusteredPutObject handles unary PutObject requests in cluster mode
func (s *CacheService) handleClusteredPutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Key == "" {
		metrics.RPCRequests.WithLabelValues("PutObject", "invalid").Inc()
		return &pb.PutResponse{Success: false, Error: "missing key"}, nil
	}

	// Check if this node owns the key
	if !s.coordinator.IsLocal(req.Key) {
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

	// Handle locally
	err := retry.DoWithKey(ctx, retry.DefaultConfig(), "PutObject", req.Key, func() error {
		return s.storage.Put(req.Key, bytes.NewReader(req.Data), int(req.TtlSeconds))
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("PutObject", "error").Inc()
		userErr := mapStorageErrorToGRPC(err)
		return &pb.PutResponse{Success: false, Error: userErr.Error()}, nil
	}

	metrics.RPCRequests.WithLabelValues("PutObject", "success").Inc()
	return &pb.PutResponse{Success: true}, nil
}

// handleClusteredGet handles Get requests in cluster mode
func (s *CacheService) handleClusteredGet(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	// Check if this node owns the key
	if !s.coordinator.IsLocal(req.Key) {
		// Forward to the correct node
		client, err := s.coordinator.Route(req.Key)
		if err != nil {
			metrics.RPCRequests.WithLabelValues("Get", "routing_error").Inc()
			return status.Errorf(codes.Unavailable, "routing error: %v", err)
		}

		// Forward the streaming Get to the remote node
		return s.forwardStreamingGet(req, stream, client)
	}

	// Handle locally
	return s.handleLocalGet(req, stream)
}

// forwardStreamingGet forwards a streaming Get request to a remote node
func (s *CacheService) forwardStreamingGet(req *pb.GetRequest, localStream pb.CacheService_GetServer, client pb.CacheServiceClient) error {
	ctx := localStream.Context()

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

// handleLocalGet processes a Get request locally
func (s *CacheService) handleLocalGet(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	var r io.Reader
	var found bool
	err := retry.DoWithKey(stream.Context(), retry.DefaultConfig(), "Get", req.Key, func() error {
		var getErr error
		r, found, getErr = s.storage.Get(req.Key, req.Start, req.End)
		return getErr
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Get", "error").Inc()
		return mapStorageErrorToGRPC(err)
	}
	if !found {
		metrics.RPCRequests.WithLabelValues("Get", "not_found").Inc()
		return status.Error(codes.NotFound, "key not found")
	}

	defer func() {
		if closer, ok := r.(io.Closer); ok {
			closer.Close()
		}
	}()

	// Stream the data in chunks
	buf := make([]byte, 64*1024) // 64KB chunks
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.GetResponse{Data: buf[:n]}); sendErr != nil {
				metrics.RPCRequests.WithLabelValues("Get", "error").Inc()
				return sendErr
			}
			metrics.StreamBytesTransferred.WithLabelValues("download").Add(float64(n))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			metrics.RPCRequests.WithLabelValues("Get", "error").Inc()
			return mapStorageErrorToGRPC(err)
		}
	}

	zlog.Debug().Str("key", req.Key).Msg("Local get completed successfully")
	metrics.RPCRequests.WithLabelValues("Get", "success").Inc()
	return nil
}

// handleClusteredDelete handles Delete requests in cluster mode
func (s *CacheService) handleClusteredDelete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Key == "" {
		metrics.RPCRequests.WithLabelValues("Delete", "invalid").Inc()
		return &pb.DeleteResponse{Success: false, Error: "missing key"}, nil
	}

	// Check if this node owns the key
	if !s.coordinator.IsLocal(req.Key) {
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

	// Handle locally
	err := retry.DoWithKey(ctx, retry.DefaultConfig(), "Delete", req.Key, func() error {
		return s.storage.DeleteKey(req.Key)
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Delete", "error").Inc()
		userErr := mapStorageErrorToGRPC(err)
		return &pb.DeleteResponse{Success: false, Error: userErr.Error()}, nil
	}

	metrics.RPCRequests.WithLabelValues("Delete", "success").Inc()
	return &pb.DeleteResponse{Success: true}, nil
}
