// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/grafana/dskit/ring"
)

// Epoch tracks the ring version using content-addressable hashing.
// Nodes with identical ring views will have identical epochs, enabling
// reliable cross-node comparisons and eliminating unnecessary topology refreshes.
//
// The epoch is computed as a deterministic hash of the ring state:
// - Sorted node IDs (for determinism)
// - Node states (JOINING, ACTIVE, LEAVING, etc.)
// - Token counts (not full tokens - too expensive, and tokens are immutable)
//
// This is an O(1) atomic load operation for reading - safe for hot paths.
// Computing the epoch is O(N) where N = number of nodes, but this only
// happens during heartbeat callbacks when ring state changes.
type Epoch struct {
	version atomic.Uint64
}

// NewEpoch creates a new Epoch tracker initialized to 0.
func NewEpoch() *Epoch {
	return &Epoch{}
}

// Get returns the current epoch value.
// This is O(1) - just an atomic load, safe to call from hot paths.
func (e *Epoch) Get() uint64 {
	return e.version.Load()
}

// Set computes epoch from ring membership state and stores it.
// Nodes with identical ring views will compute identical epochs.
//
// This is O(N) where N = number of nodes, but is only called during
// heartbeat callbacks when ring state may have changed.
//
// Returns the new epoch value.
func (e *Epoch) Set(ringDesc *ring.Desc) uint64 {
	if ringDesc == nil {
		return 0
	}

	newEpoch := ComputeRingEpoch(ringDesc)
	e.version.Store(newEpoch)
	return newEpoch
}

// ComputeRingEpoch creates a deterministic hash of ring state.
// This function is exported for testing purposes.
//
// The hash includes:
// - Node IDs (sorted for determinism)
// - Node states (to detect JOINING→ACTIVE transitions)
// - Token counts (to detect if tokens were modified)
//
// We intentionally do NOT include full tokens because:
// - Tokens are assigned once and persisted (dskit's token persistence)
// - Hashing 512 tokens × N nodes would be expensive
// - Token count is sufficient to detect "has tokens been modified"
func ComputeRingEpoch(ringDesc *ring.Desc) uint64 {
	if ringDesc == nil || len(ringDesc.Ingesters) == 0 {
		return 0
	}

	// Sort node IDs for determinism - map iteration order is not guaranteed
	ids := make([]string, 0, len(ringDesc.Ingesters))
	for id := range ringDesc.Ingesters {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	// Build canonical representation: "nodeID:state:tokenCount;..."
	var sb strings.Builder
	for _, id := range ids {
		inst := ringDesc.Ingesters[id]
		sb.WriteString(id)
		sb.WriteByte(':')
		sb.WriteString(strconv.Itoa(int(inst.State)))
		sb.WriteByte(':')
		sb.WriteString(strconv.Itoa(len(inst.Tokens)))
		sb.WriteByte(';')
	}

	// Hash using FNV-64a for fast, reasonable distribution
	h := fnv.New64a()
	_, _ = h.Write([]byte(sb.String())) // Write never returns an error for fnv
	return h.Sum64()
}

// GetEpochFromRing is a convenience function to safely get epoch from a potentially nil RingManager.
func GetEpochFromRing(rm *RingManager) uint64 {
	if rm == nil {
		return 0
	}
	return rm.GetEpoch()
}
