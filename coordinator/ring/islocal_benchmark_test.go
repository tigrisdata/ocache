// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"testing"

	dskitring "github.com/grafana/dskit/ring"
)

func BenchmarkRingManagerIsLocal(b *testing.B) {
	tests := []struct {
		name              string
		replicationFactor int
		states            []dskitring.InstanceState
		localNodeID       string
	}{
		{
			name:              "replication-factor=3/transition",
			replicationFactor: 3,
			states:            []dskitring.InstanceState{dskitring.ACTIVE, dskitring.PENDING, dskitring.ACTIVE, dskitring.PENDING, dskitring.ACTIVE},
			localNodeID:       "islocal-0",
		},
		{
			name:              "replication-factor=1/transition",
			replicationFactor: 1,
			states:            []dskitring.InstanceState{dskitring.PENDING, dskitring.ACTIVE, dskitring.ACTIVE},
			localNodeID:       "islocal-1",
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			manager := newIsLocalTestManager(b, tt.replicationFactor, tt.states, tt.localNodeID)
			if !manager.IsLocal(isLocalBenchmarkKey) {
				b.Fatal("warm IsLocal() did not find the expected owner")
			}

			var got bool
			b.ResetTimer()
			for b.Loop() {
				got = manager.IsLocal(isLocalBenchmarkKey)
			}
			b.StopTimer()
			if !got {
				b.Fatal("IsLocal() did not find the expected owner")
			}
		})
	}
}
