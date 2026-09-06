// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"context"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
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

	zlog.Debug().Str("key", req.Key).Uint64("expected", req.ExpectedVersion).Int("data_len", len(req.Data)).Msg("PutObjectIfVersion called")

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

	zlog.Debug().Str("key", req.Key).Uint64("expected", req.ExpectedVersion).Msg("DeleteIfVersion called")

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
