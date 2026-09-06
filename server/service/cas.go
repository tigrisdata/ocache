// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"context"
	"fmt"
	"io"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/bufferpool"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Conditional (compare-and-swap) gRPC handlers — issue #254. A version mismatch
// is a normal outcome, not a transport error: it is returned as success=false
// with the current version, so a caller can re-read and retry. Only genuine
// failures surface in the response error field.

// GetObjectWithVersion returns a key's value together with its CAS version.
func (s *CacheService) GetObjectWithVersion(ctx context.Context, req *pb.GetRequest) (*pb.GetWithVersionResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("GetObjectWithVersion").Observe(float64(time.Since(start).Milliseconds()))
	}()

	if req.Key == "" {
		metrics.RPCRequests.WithLabelValues("GetObjectWithVersion", "invalid").Inc()
		return nil, status.Error(codes.InvalidArgument, "missing key")
	}

	data, version, found, err := s.ops.GetWithVersion(ctx, req.Key)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("GetObjectWithVersion", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "GetObjectWithVersion").Inc()
		return nil, mapStorageErrorToGRPC(err)
	}
	metrics.RPCRequests.WithLabelValues("GetObjectWithVersion", "success").Inc()
	return &pb.GetWithVersionResponse{Data: data, Version: version, Found: found}, nil
}

// PutObjectIfVersion writes a value only if the key's version matches.
func (s *CacheService) PutObjectIfVersion(ctx context.Context, req *pb.PutIfVersionRequest) (*pb.PutIfVersionResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("PutObjectIfVersion").Observe(float64(time.Since(start).Milliseconds()))
	}()

	// Info level while CAS is in initial production testing (new feature).
	zlog.Info().Str("key", req.Key).Uint64("expected", req.ExpectedVersion).Int("data_len", len(req.Data)).Msg("PutObjectIfVersion called")

	if req.Key == "" {
		metrics.RPCRequests.WithLabelValues("PutObjectIfVersion", "invalid").Inc()
		return &pb.PutIfVersionResponse{Success: false, Error: status.Error(codes.InvalidArgument, "missing key").Error()}, nil
	}

	newVersion, err := s.ops.PutIfVersion(ctx, req.Key, req.Data, int(req.TtlSeconds), req.ExpectedVersion)
	if err != nil {
		if vm, ok := storageErrors.IsVersionMismatch(err); ok {
			// A mismatch is a normal outcome, carried in-band so the caller can
			// read the current version and retry.
			metrics.RPCRequests.WithLabelValues("PutObjectIfVersion", "mismatch").Inc()
			return &pb.PutIfVersionResponse{Success: false, CurrentVersion: vm.CurrentVersion}, nil
		}
		// A real failure is returned as a gRPC status so its code survives (a
		// caller distinguishes retryable ResourceExhausted/Unavailable from
		// permanent Internal/Corruption). Consistent with GetObjectWithVersion.
		metrics.RPCRequests.WithLabelValues("PutObjectIfVersion", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "PutObjectIfVersion").Inc()
		return nil, mapStorageErrorToGRPC(err)
	}
	metrics.RPCRequests.WithLabelValues("PutObjectIfVersion", "success").Inc()
	return &pb.PutIfVersionResponse{Success: true, NewVersion: newVersion}, nil
}

// DeleteIfVersion deletes a key only if its version matches.
func (s *CacheService) DeleteIfVersion(ctx context.Context, req *pb.DeleteIfVersionRequest) (*pb.DeleteIfVersionResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("DeleteIfVersion").Observe(float64(time.Since(start).Milliseconds()))
	}()

	// Info level while CAS is in initial production testing (new feature).
	zlog.Info().Str("key", req.Key).Uint64("expected", req.ExpectedVersion).Msg("DeleteIfVersion called")

	if req.Key == "" {
		metrics.RPCRequests.WithLabelValues("DeleteIfVersion", "invalid").Inc()
		return &pb.DeleteIfVersionResponse{Success: false, Error: status.Error(codes.InvalidArgument, "missing key").Error()}, nil
	}

	err := s.ops.DeleteIfVersion(ctx, req.Key, req.ExpectedVersion)
	if err != nil {
		if vm, ok := storageErrors.IsVersionMismatch(err); ok {
			metrics.RPCRequests.WithLabelValues("DeleteIfVersion", "mismatch").Inc()
			return &pb.DeleteIfVersionResponse{Success: false, CurrentVersion: vm.CurrentVersion}, nil
		}
		// Real failure as a gRPC status (code preserved), consistent with the
		// other CAS RPCs; the response is used only for the mismatch outcome.
		metrics.RPCRequests.WithLabelValues("DeleteIfVersion", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "DeleteIfVersion").Inc()
		return nil, mapStorageErrorToGRPC(err)
	}
	metrics.RPCRequests.WithLabelValues("DeleteIfVersion", "success").Inc()
	return &pb.DeleteIfVersionResponse{Success: true}, nil
}

// casPutResult carries the streamed-put outcome back from the storage goroutine.
type casPutResult struct {
	version uint64
	err     error
}

// PutStreamIfVersion is the streaming form of PutObjectIfVersion (issue #258):
// the value is streamed rather than buffered, so it works for objects larger
// than the unary message cap. Same outcome contract as the unary op — a
// mismatch is success=false+current_version, a real failure is a gRPC status.
func (s *CacheService) PutStreamIfVersion(stream pb.CacheService_PutStreamIfVersionServer) error {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("PutStreamIfVersion").Observe(float64(time.Since(start).Milliseconds()))
	}()
	metrics.StreamsActive.Inc()
	defer metrics.StreamsActive.Dec()

	first, err := stream.Recv()
	if err != nil {
		metrics.RPCRequests.WithLabelValues("PutStreamIfVersion", "error").Inc()
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if first.Key == "" {
		metrics.RPCRequests.WithLabelValues("PutStreamIfVersion", "invalid").Inc()
		return status.Error(codes.InvalidArgument, "missing key")
	}
	if s.coordinator != nil && !s.ops.IsLocal(first.Key) {
		return s.forwardStreamingPutIfVersion(stream, first)
	}
	return s.handleLocalPutStreamIfVersion(stream, first)
}

func (s *CacheService) handleLocalPutStreamIfVersion(stream pb.CacheService_PutStreamIfVersionServer, first *pb.PutIfVersionRequest) error {
	key := first.Key
	ttl := int(first.TtlSeconds)
	expected := first.ExpectedVersion

	pr, pw := io.Pipe()
	resCh := make(chan casPutResult, 1)
	go func() {
		v, err := s.ops.PutStreamIfVersionLocal(stream.Context(), key, pr, ttl, expected)
		// Close the read end so the writer below unblocks even when storage
		// stops reading early — a CAS fast-fail (mismatch) returns before it
		// consumes the body, which would otherwise deadlock pw.Write.
		pr.CloseWithError(err)
		resCh <- casPutResult{version: v, err: err}
	}()

	// Pump the stream into the pipe. On a recv/write failure, close the pipe
	// WITH the error rather than cleanly: a plain Close is EOF to storage, which
	// would commit the partial body as a complete value and advance the version.
	// CloseWithError makes storage's copy fail so nothing is committed (this is
	// what the plain streaming Put does too). fromClient distinguishes a failed
	// client upload from the storage reader closing the pipe (fast-fail mismatch
	// or a storage error) so the two are not conflated below.
	pumpErr, fromClient := pumpStreamToPipe(stream, first, pw)
	if pumpErr != nil {
		pw.CloseWithError(pumpErr)
	} else {
		pw.Close()
	}
	res := <-resCh

	if res.err != nil {
		if vm, ok := storageErrors.IsVersionMismatch(res.err); ok {
			metrics.RPCRequests.WithLabelValues("PutStreamIfVersion", "mismatch").Inc()
			return stream.SendAndClose(&pb.PutIfVersionResponse{Success: false, CurrentVersion: vm.CurrentVersion})
		}
		metrics.RPCRequests.WithLabelValues("PutStreamIfVersion", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "PutStreamIfVersion").Inc()
		// Only a genuinely interrupted client upload is Aborted (nothing was
		// committed). If the pump stopped because storage closed the pipe, the
		// storage error is authoritative: map it so its real code survives
		// (ResourceExhausted for a full disk, Internal for corruption, ...)
		// rather than masking every storage failure as Aborted.
		if fromClient {
			return status.Error(codes.Aborted, pumpErr.Error())
		}
		return mapStorageErrorToGRPC(res.err)
	}
	metrics.RPCRequests.WithLabelValues("PutStreamIfVersion", "success").Inc()
	return stream.SendAndClose(&pb.PutIfVersionResponse{Success: true, NewVersion: res.version})
}

// pumpStreamToPipe writes the first message's data and then every subsequent
// chunk into pw, stopping at EOF or the first error. fromClient reports whether
// the error came from receiving on the gRPC stream (the client's upload failed,
// so the RPC must abort with nothing committed) rather than from writing to the
// pipe (the storage reader stopped consuming — a fast-fail mismatch or a storage
// error — in which case the storage goroutine's result is authoritative).
func pumpStreamToPipe(stream pb.CacheService_PutStreamIfVersionServer, first *pb.PutIfVersionRequest, pw *io.PipeWriter) (err error, fromClient bool) {
	if len(first.Data) > 0 {
		if _, werr := pw.Write(first.Data); werr != nil {
			return werr, false
		}
	}
	for {
		chunk, rerr := stream.Recv()
		if rerr == io.EOF {
			return nil, false
		}
		if rerr != nil {
			return rerr, true // client upload failed
		}
		if len(chunk.Data) > 0 {
			if _, werr := pw.Write(chunk.Data); werr != nil {
				return werr, false // storage stopped reading the body
			}
			metrics.StreamBytesTransferred.WithLabelValues("upload").Add(float64(len(chunk.Data)))
		}
	}
}

// GetStreamWithVersion is the streaming form of GetObjectWithVersion (issue
// #258): the first message carries version+found, the rest stream the value, so
// a large object's version and bytes arrive as one atomic snapshot without
// buffering the whole value server-side.
func (s *CacheService) GetStreamWithVersion(req *pb.GetRequest, stream pb.CacheService_GetStreamWithVersionServer) error {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("GetStreamWithVersion").Observe(float64(time.Since(start).Milliseconds()))
	}()
	metrics.StreamsActive.Inc()
	defer metrics.StreamsActive.Dec()

	if req.Key == "" {
		return status.Error(codes.InvalidArgument, "missing key")
	}
	if s.coordinator != nil && !s.ops.IsLocal(req.Key) {
		return s.forwardStreamingGetWithVersion(req, stream)
	}

	r, version, found, err := s.ops.GetReaderWithVersionLocal(stream.Context(), req.Key)
	if err != nil {
		metrics.RPCRequests.WithLabelValues("GetStreamWithVersion", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "GetStreamWithVersion").Inc()
		return mapStorageErrorToGRPC(err)
	}
	if !found {
		metrics.RPCRequests.WithLabelValues("GetStreamWithVersion", "not_found").Inc()
		return stream.Send(&pb.GetWithVersionResponse{Found: false, Version: 0})
	}
	if closer, ok := r.(io.Closer); ok {
		defer closer.Close()
	}
	// First message: version + found (no data).
	if err := stream.Send(&pb.GetWithVersionResponse{Version: version, Found: true}); err != nil {
		return err
	}
	buf, release := bufferpool.AcquireBuffer(1 << 20) // 1 MiB
	defer release()
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if err := stream.Send(&pb.GetWithVersionResponse{Data: buf[:n]}); err != nil {
				metrics.RPCRequests.WithLabelValues("GetStreamWithVersion", "error").Inc()
				return err
			}
			metrics.StreamBytesTransferred.WithLabelValues("download").Add(float64(n))
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			metrics.RPCRequests.WithLabelValues("GetStreamWithVersion", "error").Inc()
			metrics.Errors.WithLabelValues("grpc", "GetStreamWithVersion").Inc()
			return readErr
		}
	}
	metrics.RPCRequests.WithLabelValues("GetStreamWithVersion", "success").Inc()
	return nil
}

// forwardStreamingPutIfVersion forwards a streaming CAS put to the owner node.
func (s *CacheService) forwardStreamingPutIfVersion(localStream pb.CacheService_PutStreamIfVersionServer, first *pb.PutIfVersionRequest) error {
	client, err := s.coordinator.Route(first.Key)
	if err != nil {
		return status.Error(codes.Unavailable, fmt.Sprintf("routing error: %v", err))
	}
	ctx := localStream.Context()
	ctx, err = coordinator.IncrementHopCount(ctx, s.coordinator.GetLocalNodeID())
	if err != nil {
		return status.Error(codes.Aborted, err.Error())
	}
	remote, err := client.PutStreamIfVersion(ctx)
	if err != nil {
		return status.Error(codes.Unavailable, fmt.Sprintf("failed to connect to owner: %v", err))
	}
	// A Send to the owner returning io.EOF means it ended the RPC early —
	// typically a mismatch it resolved before consuming the body. Stop
	// forwarding and let CloseAndRecv retrieve the in-band outcome so it can be
	// relayed to the caller, rather than returning the send error and hiding the
	// mismatch.
	sendErr := remote.Send(first)
	if sendErr == nil {
		for {
			chunk, err := localStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err // caller's upload failed — nothing to relay
			}
			if err := remote.Send(chunk); err != nil {
				sendErr = err
				break
			}
		}
	}
	if sendErr != nil && sendErr != io.EOF {
		return sendErr
	}
	resp, err := remote.CloseAndRecv()
	if err != nil {
		return err
	}
	return localStream.SendAndClose(resp)
}

// forwardStreamingGetWithVersion forwards a streaming CAS read from the owner.
func (s *CacheService) forwardStreamingGetWithVersion(req *pb.GetRequest, localStream pb.CacheService_GetStreamWithVersionServer) error {
	client, err := s.coordinator.Route(req.Key)
	if err != nil {
		return status.Error(codes.Unavailable, fmt.Sprintf("routing error: %v", err))
	}
	ctx := localStream.Context()
	ctx, err = coordinator.IncrementHopCount(ctx, s.coordinator.GetLocalNodeID())
	if err != nil {
		return status.Error(codes.Aborted, err.Error())
	}
	remote, err := client.GetStreamWithVersion(ctx, req)
	if err != nil {
		return status.Error(codes.Unavailable, fmt.Sprintf("failed to connect to owner: %v", err))
	}
	for {
		msg, err := remote.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := localStream.Send(msg); err != nil {
			return err
		}
	}
}
