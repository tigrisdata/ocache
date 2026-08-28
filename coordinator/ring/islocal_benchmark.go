// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_islocal_benchmark

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

const isLocalBenchmarkLookupKey = "islocal-benchmark-key"

// NewIsLocalBenchmarkManager creates an in-memory ring fixture for the
// embedded.Client.IsLocal benchmark.
func NewIsLocalBenchmarkManager(replicationFactor int, states []dskitring.InstanceState, localNodeID string) (*RingManager, func(), string, error) {
	if replicationFactor <= 0 {
		return nil, nil, "", fmt.Errorf("replication factor must be positive: %d", replicationFactor)
	}
	if len(states) == 0 {
		return nil, nil, "", fmt.Errorf("ring fixture needs at least one instance")
	}

	ctx := context.Background()
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	store, closer := consul.NewInMemoryClient(dskitring.GetCodec(), logger, reg)

	keyToken := (&RingManager{}).tokenForKey(isLocalBenchmarkLookupKey)
	if keyToken > ^uint32(0)-uint32(len(states)) {
		_ = closer.Close()
		return nil, nil, "", fmt.Errorf("ring fixture token range overflows uint32")
	}
	firstToken := keyToken + 1
	desc := dskitring.NewDesc()
	registeredAt := time.Now().Add(-time.Minute)
	for instance, state := range states {
		id := fmt.Sprintf("islocal-%d", instance)
		desc.AddIngester(
			id,
			fmt.Sprintf("127.0.0.1:%d", 10000+instance),
			"",
			[]uint32{firstToken + uint32(instance)},
			state,
			registeredAt,
		)
	}

	if err := store.CAS(ctx, RingKey, func(interface{}) (interface{}, bool, error) {
		return desc, true, nil
	}); err != nil {
		_ = closer.Close()
		return nil, nil, "", fmt.Errorf("store ring fixture: %w", err)
	}

	dskitRing, err := dskitring.NewWithStoreClientAndStrategy(
		dskitring.Config{
			HeartbeatTimeout:  time.Hour,
			ReplicationFactor: replicationFactor,
		},
		RingName,
		RingKey,
		store,
		dskitring.NewIgnoreUnhealthyInstancesReplicationStrategy(),
		reg,
		logger,
	)
	if err != nil {
		_ = closer.Close()
		return nil, nil, "", fmt.Errorf("create ring fixture: %w", err)
	}
	if err := services.StartAndAwaitRunning(ctx, dskitRing); err != nil {
		_ = closer.Close()
		return nil, nil, "", fmt.Errorf("start ring fixture: %w", err)
	}

	manager := &RingManager{
		ring:        dskitRing,
		localNodeID: localNodeID,
	}
	var closeOnce sync.Once
	closeFixture := func() {
		closeOnce.Do(func() {
			_ = services.StopAndAwaitTerminated(context.Background(), dskitRing)
			_ = closer.Close()
		})
	}

	return manager, closeFixture, isLocalBenchmarkLookupKey, nil
}
