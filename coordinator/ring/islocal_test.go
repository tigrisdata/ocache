// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	dskitring "github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
)

const isLocalBenchmarkKey = "islocal-benchmark-key"

func newIsLocalTestManager(tb testing.TB, replicationFactor int, states []dskitring.InstanceState, localNodeID string) *RingManager {
	tb.Helper()
	if len(states) == 0 {
		tb.Fatal("ring fixture needs at least one instance")
	}

	ctx := context.Background()
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	store, closer := consul.NewInMemoryClient(dskitring.GetCodec(), logger, reg)

	keyToken := (&RingManager{}).tokenForKey(isLocalBenchmarkKey)
	if keyToken > ^uint32(0)-uint32(len(states)) {
		_ = closer.Close()
		tb.Fatalf("ring fixture token range overflows uint32")
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
		tb.Fatalf("store ring fixture: %v", err)
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
		tb.Fatalf("create ring fixture: %v", err)
	}
	if err := services.StartAndAwaitRunning(ctx, dskitRing); err != nil {
		_ = closer.Close()
		tb.Fatalf("start ring fixture: %v", err)
	}

	tb.Cleanup(func() {
		if err := services.StopAndAwaitTerminated(context.Background(), dskitRing); err != nil {
			tb.Errorf("stop ring fixture: %v", err)
		}
		if err := closer.Close(); err != nil {
			tb.Errorf("close ring fixture store: %v", err)
		}
	})

	return &RingManager{
		ring:        dskitRing,
		localNodeID: localNodeID,
	}
}

func TestRingManagerIsLocalPreservesOwnershipDuringTransitions(t *testing.T) {
	tests := []struct {
		name              string
		replicationFactor int
		states            []dskitring.InstanceState
		localNodeID       string
		want              bool
	}{
		{
			name:              "replication-factor-3",
			replicationFactor: 3,
			states:            []dskitring.InstanceState{dskitring.ACTIVE, dskitring.PENDING, dskitring.ACTIVE, dskitring.PENDING, dskitring.ACTIVE},
			localNodeID:       "islocal-0",
			want:              true,
		},
		{
			name:              "replication-factor-3-three-pending",
			replicationFactor: 3,
			states:            []dskitring.InstanceState{dskitring.PENDING, dskitring.PENDING, dskitring.PENDING, dskitring.ACTIVE, dskitring.ACTIVE, dskitring.ACTIVE},
			localNodeID:       "islocal-3",
			want:              true,
		},
		{
			name:              "replication-factor-1",
			replicationFactor: 1,
			states:            []dskitring.InstanceState{dskitring.PENDING, dskitring.ACTIVE, dskitring.ACTIVE},
			localNodeID:       "islocal-1",
			want:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newIsLocalTestManager(t, tt.replicationFactor, tt.states, tt.localNodeID)
			if got := manager.IsLocal(isLocalBenchmarkKey); got != tt.want {
				t.Fatalf("IsLocal() = %v, want %v", got, tt.want)
			}
		})
	}
}
