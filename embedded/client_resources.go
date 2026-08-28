// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_islocal_benchmark

package embedded

import (
	"net"

	"github.com/tigrisdata/ocache/server/operations"
	"github.com/tigrisdata/ocache/server/service"
	stor "github.com/tigrisdata/ocache/storage"
	"google.golang.org/grpc"
)

type clientResources struct {
	config     *Config
	storage    *stor.Storage
	ops        *operations.Operations
	service    *service.CacheService
	grpcServer *grpc.Server
	grpcLis    net.Listener
}
