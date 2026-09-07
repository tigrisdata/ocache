// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_topology_benchmark

package coordinator

import "github.com/tigrisdata/ocache/coordinator/ring"

// NewTopologyBenchmarkCoordinator wires an in-memory benchmark ring through
// the ordinary coordinator topology handler without starting a coordinator
// lifecycle.
func NewTopologyBenchmarkCoordinator(ringManager *ring.RingManager) *Coordinator {
	return &Coordinator{
		config: &Config{
			MyNodeID: "topology-benchmark",
			LifecyclerConfig: ring.LifecyclerConfig{
				RingConfig: ring.Config{ReplicationFactor: 1},
			},
		},
		ringManager: ringManager,
	}
}
