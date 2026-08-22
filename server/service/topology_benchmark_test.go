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

const topologyBenchmarkBufferSize = 1024 * 1024

type topologyBenchmarkClient struct {
	client pb.CacheServiceClient
	close  func()
}

func newTopologyBenchmarkClient(tb testing.TB, members, tokensPerMember int) *topologyBenchmarkClient {
	tb.Helper()

	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	tb.Cleanup(func() {
		zerolog.SetGlobalLevel(previousLogLevel)
	})

	ringManager, closeRing, err := ring.NewTopologyBenchmarkManager(members, tokensPerMember)
	if err != nil {
		tb.Fatalf("create topology ring: %v", err)
	}

	listener := bufconn.Listen(topologyBenchmarkBufferSize)
	server := grpc.NewServer()
	pb.RegisterCacheServiceServer(server, &CacheService{
		coordinator: coordinator.NewTopologyBenchmarkCoordinator(ringManager),
	})

	serveDone := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serveDone)
	}()

	closeClient := func(conn *grpc.ClientConn) {
		if conn != nil {
			_ = conn.Close()
		}
		server.Stop()
		_ = listener.Close()
		<-serveDone
		closeRing()
	}

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
		closeClient(nil)
		tb.Fatalf("dial topology service: %v", err)
	}

	return &topologyBenchmarkClient{
		client: pb.NewCacheServiceClient(conn),
		close: func() {
			closeClient(conn)
		},
	}
}

func assertTopologyResponse(tb testing.TB, response *pb.GetTopologyResponse, members, tokensPerMember int) {
	tb.Helper()

	if response.GetError() != "" {
		tb.Fatalf("GetTopology returned error: %s", response.GetError())
	}
	if response.GetTopology() == nil || response.GetTopology().GetRingConfig() == nil {
		tb.Fatal("GetTopology returned no ring configuration")
	}

	assignments := response.GetTopology().GetRingConfig().GetNodeTokens()
	if len(assignments) != members {
		tb.Fatalf("GetTopology returned %d members, want %d", len(assignments), members)
	}

	memberIDs := make(map[string]struct{}, members)
	tokens := make(map[uint32]struct{}, members*tokensPerMember)
	for _, assignment := range assignments {
		if _, exists := memberIDs[assignment.GetNodeId()]; exists {
			tb.Fatalf("GetTopology returned duplicate assignment for %q", assignment.GetNodeId())
		}
		memberIDs[assignment.GetNodeId()] = struct{}{}

		memberTokens := assignment.GetTokens()
		if len(memberTokens) != tokensPerMember {
			tb.Fatalf("GetTopology returned %d tokens for %q, want %d", len(memberTokens), assignment.GetNodeId(), tokensPerMember)
		}
		if !sort.SliceIsSorted(memberTokens, func(i, j int) bool { return memberTokens[i] < memberTokens[j] }) {
			tb.Fatalf("GetTopology returned unsorted tokens for %q", assignment.GetNodeId())
		}
		for _, token := range memberTokens {
			if _, exists := tokens[token]; exists {
				tb.Fatalf("GetTopology returned duplicate token %d", token)
			}
			tokens[token] = struct{}{}
		}
	}
	if len(tokens) != members*tokensPerMember {
		tb.Fatalf("GetTopology returned %d distinct tokens, want %d", len(tokens), members*tokensPerMember)
	}
}

func BenchmarkCacheServiceGetTopology(b *testing.B) {
	for _, members := range []int{1, 3, 16} {
		for _, tokensPerMember := range []int{128, 512, 4096} {
			b.Run(fmt.Sprintf("members=%d/tokens=%d", members, tokensPerMember), func(b *testing.B) {
				client := newTopologyBenchmarkClient(b, members, tokensPerMember)
				defer client.close()

				ctx := context.Background()
				response, err := client.client.GetTopology(ctx, &pb.GetTopologyRequest{})
				if err != nil {
					b.Fatalf("warm GetTopology: %v", err)
				}
				assertTopologyResponse(b, response, members, tokensPerMember)

				b.ResetTimer()
				for b.Loop() {
					response, err = client.client.GetTopology(ctx, &pb.GetTopologyRequest{})
					if err != nil {
						b.Fatalf("GetTopology: %v", err)
					}
				}
				b.StopTimer()

				assertTopologyResponse(b, response, members, tokensPerMember)
			})
		}
	}
}
