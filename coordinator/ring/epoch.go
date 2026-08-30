// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"hash/fnv"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/grafana/dskit/ring"
)

// Epoch tracks the ring version using content-addressable hashing.
// Nodes with identical ring views will have identical epochs, enabling
// reliable cross-node comparisons and eliminating unnecessary topology refreshes.
//
// The epoch is the XOR of deterministic per-node fingerprints. Each fingerprint
// includes the node ID, state, and token count (not full tokens). The commutative
// digest lets a liveness delta replace one node's contribution without scanning
// the other nodes. Full snapshots rebuild the node table and digest.
//
// This is an O(1) atomic load operation for reading - safe for hot paths.
type epochNode struct {
	state      ring.InstanceState
	tokenCount int
}

type Epoch struct {
	version atomic.Uint64
	mu      sync.Mutex
	nodes   map[string]epochNode
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
// This is O(N) where N = number of nodes and is reserved for full snapshots.
//
// Returns the new epoch value.
func (e *Epoch) Set(ringDesc *ring.Desc) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ringDesc == nil {
		e.nodes = nil
		e.version.Store(0)
		return 0
	}

	e.nodes = make(map[string]epochNode, len(ringDesc.Ingesters))
	for id, instance := range ringDesc.Ingesters {
		e.nodes[id] = epochNode{state: instance.State, tokenCount: len(instance.Tokens)}
	}
	newEpoch := ComputeRingEpoch(ringDesc)
	e.version.Store(newEpoch)
	return newEpoch
}

// ApplyLivenessDelta updates the epoch for state-only changes without scanning
// the complete ring. Timestamp-only updates leave the epoch unchanged.
func (e *Epoch) ApplyLivenessDelta(changes map[string]ring.InstanceDesc) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(changes) == 0 || e.nodes == nil {
		return e.version.Load()
	}

	newEpoch := e.version.Load()
	for id, instance := range changes {
		previous, ok := e.nodes[id]
		if !ok || previous.state == instance.State {
			continue
		}
		newEpoch ^= epochNodeFingerprint(id, previous.state, previous.tokenCount)
		newEpoch ^= epochNodeFingerprint(id, instance.State, previous.tokenCount)
		previous.state = instance.State
		e.nodes[id] = previous
	}
	e.version.Store(newEpoch)
	return newEpoch
}

func epochNodeFingerprint(id string, state ring.InstanceState, tokenCount int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(strconv.Itoa(int(state))))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(strconv.Itoa(tokenCount)))
	return h.Sum64()
}

// ComputeRingEpoch creates a deterministic hash of ring state.
// This function is exported for testing purposes.
//
// The hash includes:
// - Node IDs (to identify each per-node fingerprint)
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

	var epoch uint64
	for id, instance := range ringDesc.Ingesters {
		epoch ^= epochNodeFingerprint(id, instance.State, len(instance.Tokens))
	}
	return epoch
}

// GetEpochFromRing is a convenience function to safely get epoch from a potentially nil RingManager.
func GetEpochFromRing(rm *RingManager) uint64 {
	if rm == nil {
		return 0
	}
	return rm.GetEpoch()
}
