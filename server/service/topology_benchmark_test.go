// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_topology_benchmark

package service

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tigrisdata/ocache/coordinator"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const benchmarkBufferSize = 1024 * 1024

type benchmarkClient struct {
	client pb.CacheServiceClient
	close  func()
}

func newBenchmarkClient(tb testing.TB, activeNodes, tokensPerNode int) *benchmarkClient {
	tb.Helper()

	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	tb.Cleanup(func() {
		zerolog.SetGlobalLevel(previousLogLevel)
	})

	ringManager, closeRing, err := ring.NewTopologyBenchmarkManager(activeNodes, tokensPerNode)
	if err != nil {
		tb.Fatalf("create topology ring: %v", err)
	}

	listener := bufconn.Listen(benchmarkBufferSize)
	server := grpc.NewServer()
	// Register CacheService itself so gRPC dispatch invokes its production
	// GetTopology method rather than a benchmark-specific RPC implementation.
	pb.RegisterCacheServiceServer(server, &CacheService{
		coordinator: coordinator.NewTopologyBenchmarkCoordinator(ringManager),
	})
	go func() {
		_ = server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		"topology-benchmark",
		grpc.WithBlock(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.Stop()
		_ = listener.Close()
		closeRing()
		tb.Fatalf("dial topology service: %v", err)
	}

	return &benchmarkClient{
		client: pb.NewCacheServiceClient(conn),
		close: func() {
			_ = conn.Close()
			server.Stop()
			_ = listener.Close()
			closeRing()
		},
	}
}

func assertTopologyResponse(tb testing.TB, response *pb.GetTopologyResponse, activeNodes, tokensPerNode int) {
	tb.Helper()

	if response.GetError() != "" {
		tb.Fatalf("GetTopology returned error: %s", response.GetError())
	}
	if response.GetTopology() == nil || response.GetTopology().GetRingConfig() == nil {
		tb.Fatal("GetTopology returned no ring configuration")
	}

	nodeTokens := response.GetTopology().GetRingConfig().GetNodeTokens()
	if len(nodeTokens) != activeNodes {
		tb.Fatalf("GetTopology returned %d active nodes, want %d", len(nodeTokens), activeNodes)
	}

	seen := make(map[string]struct{}, len(nodeTokens))
	for _, assignment := range nodeTokens {
		if _, exists := seen[assignment.GetNodeId()]; exists {
			tb.Fatalf("GetTopology returned duplicate assignment for %q", assignment.GetNodeId())
		}
		seen[assignment.GetNodeId()] = struct{}{}

		tokens := assignment.GetTokens()
		if len(tokens) != tokensPerNode {
			tb.Fatalf("GetTopology returned %d tokens for %q, want %d", len(tokens), assignment.GetNodeId(), tokensPerNode)
		}
		if !sort.SliceIsSorted(tokens, func(i, j int) bool { return tokens[i] < tokens[j] }) {
			tb.Fatalf("GetTopology returned unsorted tokens for %q", assignment.GetNodeId())
		}
	}
}

func TestCacheServiceGetTopologySortedTokens(t *testing.T) {
	for _, activeNodes := range []int{1, 8, 32} {
		for _, tokensPerNode := range []int{128, 512, 1024} {
			t.Run(fmt.Sprintf("nodes=%d/tokens=%d", activeNodes, tokensPerNode), func(t *testing.T) {
				client := newBenchmarkClient(t, activeNodes, tokensPerNode)
				defer client.close()

				response, err := client.client.GetTopology(context.Background(), &pb.GetTopologyRequest{})
				if err != nil {
					t.Fatalf("GetTopology: %v", err)
				}
				assertTopologyResponse(t, response, activeNodes, tokensPerNode)
			})
		}
	}
}

func BenchmarkCacheServiceGetTopology(b *testing.B) {
	for _, activeNodes := range []int{1, 8, 32} {
		for _, tokensPerNode := range []int{128, 512, 1024} {
			b.Run(fmt.Sprintf("nodes=%d/tokens=%d", activeNodes, tokensPerNode), func(b *testing.B) {
				client := newBenchmarkClient(b, activeNodes, tokensPerNode)
				defer client.close()

				ctx := context.Background()
				response, err := client.client.GetTopology(ctx, &pb.GetTopologyRequest{})
				if err != nil {
					b.Fatalf("warm GetTopology: %v", err)
				}
				assertTopologyResponse(b, response, activeNodes, tokensPerNode)

				b.ResetTimer()
				for b.Loop() {
					response, err = client.client.GetTopology(ctx, &pb.GetTopologyRequest{})
					if err != nil {
						b.Fatalf("GetTopology: %v", err)
					}
				}
				b.StopTimer()

				assertTopologyResponse(b, response, activeNodes, tokensPerNode)
			})
		}
	}
}
