// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_islocal_benchmark

package embedded

import (
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/tigrisdata/ocache/coordinator"
	coordinatorring "github.com/tigrisdata/ocache/coordinator/ring"
)

func BenchmarkClientIsLocal(b *testing.B) {
	tests := []struct {
		name              string
		replicationFactor int
		states            []ring.InstanceState
		localNodeID       string
	}{
		{
			name:              "replication-factor=3/transition",
			replicationFactor: 3,
			states:            []ring.InstanceState{ring.ACTIVE, ring.PENDING, ring.ACTIVE, ring.PENDING, ring.ACTIVE},
			localNodeID:       "islocal-0",
		},
		{
			name:              "replication-factor=3/three-pending",
			replicationFactor: 3,
			states:            []ring.InstanceState{ring.PENDING, ring.PENDING, ring.PENDING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
			localNodeID:       "islocal-3",
		},
		{
			name:              "replication-factor=1/transition",
			replicationFactor: 1,
			states:            []ring.InstanceState{ring.PENDING, ring.ACTIVE, ring.ACTIVE},
			localNodeID:       "islocal-1",
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			manager, closeFixture, key, err := coordinatorring.NewIsLocalBenchmarkManager(tt.replicationFactor, tt.states, tt.localNodeID)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(closeFixture)

			client := &Client{
				coordinator: coordinator.NewIsLocalBenchmarkCoordinator(manager),
			}
			if !client.IsLocal(key) {
				b.Fatal("warm Client.IsLocal() did not find the expected owner")
			}

			var got bool
			b.ResetTimer()
			for b.Loop() {
				got = client.IsLocal(key)
			}
			b.StopTimer()
			if !got {
				b.Fatal("Client.IsLocal() did not find the expected owner")
			}
		})
	}
}
