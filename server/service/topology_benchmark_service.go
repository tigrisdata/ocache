// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_topology_benchmark

package service

import (
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
)

// CacheService is the topology-only service used by the benchmark build.
type CacheService struct {
	pb.UnimplementedCacheServiceServer
	coordinator *coordinator.Coordinator
}
