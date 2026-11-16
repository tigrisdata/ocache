// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/internal/listinput"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestCacheService_ListLocalHeaders(t *testing.T) {
	s := setupTestStorage(t)
	client := startBufconnService(t, NewCacheService(nil, s),
		grpc.UnaryInterceptor(coordinator.UnaryServerEpochInterceptor(func() uint64 { return 42 })))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := client.PutObject(ctx, &pb.PutRequest{Key: "key", Data: []byte("value")})
	require.NoError(t, err)

	for _, withValues := range []bool{false, true} {
		name := "ListLocal"
		if withValues {
			name = "ListLocalWithValues"
		}
		t.Run(name, func(t *testing.T) {
			for _, prefix := range []string{"", "missing"} {
				var headers metadata.MD
				if withValues {
					_, err = client.ListLocalWithValues(ctx, &pb.ListRequest{Prefix: prefix, Limit: 1}, grpc.Header(&headers))
				} else {
					_, err = client.ListLocal(ctx, &pb.ListRequest{Prefix: prefix, Limit: 1}, grpc.Header(&headers))
				}
				require.NoError(t, err)
				require.True(t, listinput.HasEmptyStartKeyCursorCapability(headers), "prefix %q: missing cursor capability", prefix)
				require.Equal(t, []string{"42"}, headers.Get("x-ring-epoch"), "prefix %q: missing ring epoch", prefix)
			}
		})
	}
}
