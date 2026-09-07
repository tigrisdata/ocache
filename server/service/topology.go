// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
)

// GetTopology returns the current cluster topology (for cluster-aware clients).
func (s *CacheService) GetTopology(ctx context.Context, req *pb.GetTopologyRequest) (*pb.GetTopologyResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("GetTopology").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Msg("gRPC GetTopology called")

	if s.coordinator == nil {
		metrics.RPCRequests.WithLabelValues("GetTopology", "not_clustered").Inc()
		return &pb.GetTopologyResponse{
			Error: "cluster mode not enabled",
		}, nil
	}

	topology, err := s.coordinator.GetClusterTopology(ctx, &clusterpb.Empty{})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("GetTopology", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "GetTopology").Inc()
		return &pb.GetTopologyResponse{
			Error: err.Error(),
		}, nil
	}

	metrics.RPCRequests.WithLabelValues("GetTopology", "success").Inc()
	return &pb.GetTopologyResponse{
		Topology: topology,
	}, nil
}
