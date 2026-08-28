// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_islocal_benchmark

package coordinator

import "github.com/tigrisdata/ocache/coordinator/ring"

// NewIsLocalBenchmarkCoordinator wires an in-memory ring through the ordinary
// coordinator path without starting a coordinator lifecycle.
func NewIsLocalBenchmarkCoordinator(ringManager *ring.RingManager) *Coordinator {
	return &Coordinator{ringManager: ringManager}
}
