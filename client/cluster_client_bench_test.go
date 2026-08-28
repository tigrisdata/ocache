// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BenchmarkClusterRouteDuringTopologyChanges measures keyed routing while the
// ordinary topology refresh loop repeatedly adds and removes a member pool. The
// key stays on node-0 in both ring states so the benchmark measures the route
// read itself rather than the expected transition window for a new address.
func BenchmarkClusterRouteDuringTopologyChanges(b *testing.B) {
	servers := make([]*testServer, 3)
	addresses := make([]string, len(servers))
	for i := range servers {
		server, err := newTestServerWithAddr()
		if err != nil {
			b.Fatal(err)
		}
		servers[i] = server
		addresses[i] = server.address
		b.Cleanup(server.Stop)
	}

	initialTopology := setupSimpleTopology(addresses[:2])
	addedTopology := setupSimpleTopology(addresses)
	addedTopology.Epoch = 2
	for _, server := range servers {
		server.cacheService.SetClusterTopology(initialTopology)
	}

	client, err := NewClusterClient(&ClientConfig{
		Addrs:              []string{addresses[0]},
		ConnectionPoolSize: 4,
		RefreshInterval:    2 * time.Millisecond,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = client.Close() })

	ring := NewTokenRing()
	nodeTokens := make(map[string][]uint32)
	nodeAddresses := make(map[string]string)
	for _, node := range addedTopology.Nodes {
		if node.Status == clusterpb.NodeStatus_NODE_STATUS_ACTIVE {
			nodeAddresses[node.Id] = node.ListenAddress
		}
	}
	for _, nodeTokensForNode := range addedTopology.RingConfig.NodeTokens {
		nodeTokens[nodeTokensForNode.NodeId] = nodeTokensForNode.Tokens
	}
	ring.Update(nodeTokens, nodeAddresses)

	var key string
	for i := 0; i < 100000; i++ {
		candidate := fmt.Sprintf("topology-churn-key-%d", i)
		initialAddress, initialErr := client.topology.GetNodeForKey(candidate)
		addedAddress, addedErr := ring.GetNodeForKey(candidate)
		if initialErr == nil && addedErr == nil && initialAddress == addresses[0] && addedAddress == addresses[0] {
			key = candidate
			break
		}
	}
	if key == "" {
		b.Fatal("could not find a key owned by node-0 in both ring states")
	}

	waitForEpoch := func(want uint64) {
		deadline := time.Now().Add(time.Second)
		for client.GetTopologyEpoch() != want && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if got := client.GetTopologyEpoch(); got != want {
			b.Fatalf("topology epoch = %d, want %d", got, want)
		}
	}

	// Exercise the ordinary refresh callback before timing the route workload.
	for _, server := range servers {
		server.cacheService.SetClusterTopology(addedTopology)
	}
	waitForEpoch(2)
	for _, server := range servers {
		server.cacheService.SetClusterTopology(initialTopology)
	}
	waitForEpoch(1)

	var stop atomic.Bool
	var routeErrors atomic.Int64
	var updates sync.WaitGroup
	updates.Add(1)

	b.ResetTimer()
	go func() {
		defer updates.Done()
		added := true
		for !stop.Load() {
			topology := initialTopology
			if added {
				topology = addedTopology
			}
			for _, server := range servers {
				server.cacheService.SetClusterTopology(topology)
			}
			added = !added
			time.Sleep(5 * time.Millisecond)
		}
	}()

	b.SetParallelism(2)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := client.Route(key); err != nil {
				routeErrors.Add(1)
			}
		}
	})
	b.StopTimer()
	stop.Store(true)
	updates.Wait()
	_ = client.Close()

	if got := routeErrors.Load(); got != 0 {
		b.Fatalf("route returned %d errors during topology changes", got)
	}
}
