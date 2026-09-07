// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_topology_benchmark

package ring

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	dskitring "github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
)

// NewTopologyBenchmarkManager creates an in-memory active ring for the
// CacheService.GetTopology benchmark.
func NewTopologyBenchmarkManager(members, tokensPerMember int) (*RingManager, func(), error) {
	if members <= 0 {
		return nil, nil, fmt.Errorf("members must be positive: %d", members)
	}
	if tokensPerMember <= 0 {
		return nil, nil, fmt.Errorf("tokens per member must be positive: %d", tokensPerMember)
	}
	if uint64(members)*uint64(tokensPerMember) > uint64(^uint32(0))+1 {
		return nil, nil, fmt.Errorf("%d members with %d tokens exceed the uint32 token space", members, tokensPerMember)
	}

	ctx := context.Background()
	reg := prometheus.NewRegistry()
	store, closer := consul.NewInMemoryClient(dskitring.GetCodec(), log.NewNopLogger(), reg)

	desc := dskitring.NewDesc()
	registeredAt := time.Now()
	for member := 0; member < members; member++ {
		tokens := make([]uint32, tokensPerMember)
		for token := range tokens {
			tokens[token] = uint32(member*tokensPerMember + token)
		}
		desc.AddIngester(
			fmt.Sprintf("member-%d", member),
			fmt.Sprintf("127.0.0.1:%d", 10000+member),
			"",
			tokens,
			dskitring.ACTIVE,
			registeredAt,
		)
	}

	if err := store.CAS(ctx, RingKey, func(interface{}) (interface{}, bool, error) {
		return desc, true, nil
	}); err != nil {
		_ = closer.Close()
		return nil, nil, fmt.Errorf("store topology fixture: %w", err)
	}

	manager, err := NewRingManager(LifecyclerConfig{
		RingConfig: Config{
			HeartbeatPeriod:   time.Hour,
			HeartbeatTimeout:  time.Hour,
			ReplicationFactor: 1,
		},
		InstanceID:   "topology-benchmark",
		InstanceAddr: "127.0.0.1",
		InstancePort: 10000,
		NumTokens:    tokensPerMember,
	}, store, log.NewNopLogger(), reg)
	if err != nil {
		_ = closer.Close()
		return nil, nil, fmt.Errorf("create topology ring manager: %w", err)
	}
	if err := services.StartAndAwaitRunning(ctx, manager.ring); err != nil {
		_ = closer.Close()
		return nil, nil, fmt.Errorf("start topology ring reader: %w", err)
	}

	var closeOnce sync.Once
	closeFixture := func() {
		closeOnce.Do(func() {
			_ = services.StopAndAwaitTerminated(context.Background(), manager.ring)
			_ = closer.Close()
		})
	}

	return manager, closeFixture, nil
}
