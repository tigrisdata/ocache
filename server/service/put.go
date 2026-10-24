// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"fmt"
	"io"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/operations"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	if s.coordinator != nil && !s.ops.IsLocal(key) {
		return s.forwardStreamingPut(stream, firstChunk)
	}

	// Handle locally - reconstruct the stream for local processing
	return s.handleLocalPut(stream, firstChunk)
}

// handleLocalPut processes a Put request locally
func (s *CacheService) handleLocalPut(stream pb.CacheService_PutServer, firstChunk *pb.PutRequest) error {
	key := firstChunk.Key
	ttl := int(firstChunk.TtlSeconds)

	// Decide a sheddable write ONCE, up front, before the body is read, so a
	// client that sends the key first and waits is answered instead of left
	// blocking. The local write below is then unconditional: a second check in
	// the writer could refuse after we are already waiting in Recv, with no way
	// to wake that wait. Crossing the ratio mid-write is tolerated; eviction
	// catches up.
	if firstChunk.ShedOverBudget && s.ops.OverBudget() {
		return stream.SendAndClose(putFailure("Put", storageErrors.NewOverBudgetError("Put")))
	}
	opts := operations.PutOptions{}

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	// Start the put operation in a goroutine so it can consume the pipe as we write to it
	go func() {
		// Note: PutLocal auto-detects that io.PipeReader is not seekable,
		// so it won't attempt retries. The client would need to resend the entire stream.
		err := s.ops.PutLocalWithOptions(stream.Context(), key, pr, ttl, opts)
		if err != nil {
			// A refused (shed) or failed put stops reading; unblock the writer
			// below with the real cause so it is what the client sees.
			pr.CloseWithError(err)
		}
		errCh <- err
	}()

	// Write the first chunk's data if any
	if len(firstChunk.Data) > 0 {
		if _, err := pw.Write(firstChunk.Data); err != nil {
			pw.CloseWithError(err)
			putErr := <-errCh // wait for storage.Put to finish
			return stream.SendAndClose(putFailure("Put", firstErr(putErr, err)))
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
			putErr := <-errCh // wait for storage.Put to finish
			return stream.SendAndClose(putFailure("Put", firstErr(putErr, err)))
		}
		if len(chunk.Data) > 0 {
			if _, err := pw.Write(chunk.Data); err != nil {
				pw.CloseWithError(err)
				putErr := <-errCh // wait for storage.Put to finish
				return stream.SendAndClose(putFailure("Put", firstErr(putErr, err)))
			}
			metrics.StreamBytesTransferred.WithLabelValues("upload").Add(float64(len(chunk.Data)))
		}
	}
	pw.Close()

	err := <-errCh // wait for storage.Put to finish
	if err != nil {
		return stream.SendAndClose(putFailure("Put", err))
	}

	zlog.Debug().Str("key", key).Msg("Streaming put completed successfully")
	metrics.RPCRequests.WithLabelValues("Put", "success").Inc()
	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

// forwardStreamingPut forwards a streaming Put request to a remote node
func (s *CacheService) forwardStreamingPut(localStream pb.CacheService_PutServer, firstChunk *pb.PutRequest) error {
	key := firstChunk.Key

	// Forward to the correct node
	client, err := s.coordinator.RouteContext(localStream.Context(), key)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Put", "routing_error").Inc()
		return localStream.SendAndClose(&pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("routing error: %v", err),
		})
	}

	ctx := localStream.Context()

	// Increment hop count for forwarding loop detection
	ctx, err = coordinator.IncrementHopCount(ctx, s.coordinator.GetLocalNodeID())
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Put", "hop_limit_exceeded").Inc()
		return localStream.SendAndClose(&pb.PutResponse{
			Success: false,
			Error:   err.Error(),
		})
	}

	// Create a streaming Put call to the remote node
	remoteStream, err := client.Put(ctx)
	if err != nil {
		return localStream.SendAndClose(&pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to connect to remote node: %v", err),
		})
	}

	return relayPut(localStream, remoteStream, firstChunk)
}

// relayPut pumps the client's chunks to the owner while listening for the
// owner's reply at the same time, so an owner that answers early (a sheddable
// write it refused on the first message) is relayed at once instead of after
// the client's next chunk — which a client waiting for that answer never sends.
func relayPut(localStream pb.CacheService_PutServer, remoteStream pb.CacheService_PutClient, firstChunk *pb.PutRequest) error {
	type reply struct {
		resp *pb.PutResponse
		err  error
	}
	replyCh := make(chan reply, 1)
	go func() {
		var resp pb.PutResponse
		err := remoteStream.RecvMsg(&resp)
		replyCh <- reply{resp: &resp, err: err}
	}()
	relay := func(r reply) error {
		if r.err != nil {
			return localStream.SendAndClose(&pb.PutResponse{
				Success: false,
				Error:   fmt.Sprintf("error from remote node: %v", r.err),
			})
		}
		metrics.RPCRequests.WithLabelValues("Put", "forwarded").Inc()
		return localStream.SendAndClose(r.resp)
	}

	// A Send returning io.EOF means the owner ended the RPC early; its reply
	// is on replyCh.
	if err := remoteStream.Send(firstChunk); err != nil {
		if err == io.EOF {
			return relay(<-replyCh)
		}
		return localStream.SendAndClose(&pb.PutResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to forward to remote node: %v", err),
		})
	}

	type chunk struct {
		req *pb.PutRequest
		err error
	}
	chunkCh := make(chan chunk)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			req, err := localStream.Recv()
			select {
			case chunkCh <- chunk{req: req, err: err}:
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case r := <-replyCh:
			return relay(r)
		case c := <-chunkCh:
			if c.err == io.EOF {
				_ = remoteStream.CloseSend()
				return relay(<-replyCh)
			}
			if c.err != nil {
				_ = remoteStream.CloseSend()
				return localStream.SendAndClose(&pb.PutResponse{
					Success: false,
					Error:   fmt.Sprintf("error receiving chunk: %v", c.err),
				})
			}
			if err := remoteStream.Send(c.req); err != nil {
				if err == io.EOF {
					return relay(<-replyCh)
				}
				return localStream.SendAndClose(&pb.PutResponse{
					Success: false,
					Error:   fmt.Sprintf("failed to forward chunk to remote node: %v", err),
				})
			}
		}
	}
}

// firstErr prefers the put's own error over the pipe/stream error it caused.
func firstErr(primary, fallback error) error {
	if primary != nil {
		return primary
	}
	return fallback
}

// putFailure builds the failure response for a put received as RPC method
// (Put or PutObject): the storage error mapped to its user-facing gRPC status,
// with the code carried alongside the message so a forwarding node and the
// caller can act on it. A refused sheddable write is marked shed and counted
// under its own status, never as an error (see operations.IsOverBudget).
func putFailure(method string, err error) *pb.PutResponse {
	userErr := mapStorageErrorToGRPC(err)
	code := codes.Unknown
	if st, ok := status.FromError(userErr); ok {
		code = st.Code()
	}
	// Local refusals wrap ErrOverBudget; refusals relayed from a remote owner
	// arrive as a PutError with Shed set. operations.IsOverBudget covers both.
	shed := operations.IsOverBudget(err)
	if shed {
		metrics.RPCRequests.WithLabelValues(method, "shed").Inc()
	} else {
		metrics.RPCRequests.WithLabelValues(method, "error").Inc()
		metrics.Errors.WithLabelValues("grpc", method).Inc()
	}
	return &pb.PutResponse{Success: false, Error: userErr.Error(), Code: int32(code), Shed: shed}
}
