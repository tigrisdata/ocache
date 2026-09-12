// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_benchmark

package coordinator

import (
	"github.com/tigrisdata/ocache/coordinator/ring"
)

// NewBenchmarkCoordinator wires a running in-memory ring to the normal routing
// path without starting memberlist. It is used only by the cluster list
// cancellation benchmark.
func NewBenchmarkCoordinator(ringManager *ring.RingManager, localID string) *Coordinator {
	return &Coordinator{
		config: &Config{
			Enabled:  true,
			MyNodeID: localID,
			LifecyclerConfig: ring.LifecyclerConfig{
				RingConfig: ring.Config{ReplicationFactor: 1},
			},
		},
		ringManager: ringManager,
		router:      NewRouter(ringManager, localID),
		stopCh:      make(chan struct{}),
		errCh:       make(chan error, 1),
	}
}
