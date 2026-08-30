// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafana/dskit/ring"
)

// Epoch tracks the ring version using the content-addressable hash that is
// already sent by the coordinator. Keeping this representation stable lets old
// and new binaries agree on an epoch during a rolling upgrade.
//
// The node table is kept so timestamp-only deltas can avoid changing the epoch.
// A state delta rebuilds the compatible digest from that table; state changes are
// infrequent compared with heartbeat timestamp updates. Reads remain O(1).
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
	newEpoch := computeEpochNodes(e.nodes)
	e.version.Store(newEpoch)
	return newEpoch
}

// ApplyLivenessDelta updates the epoch for state-only changes. Timestamp-only
// updates leave the epoch unchanged. State changes rebuild the compatible
// sorted-string digest from the locally maintained node table.
func (e *Epoch) ApplyLivenessDelta(changes map[string]ring.InstanceDesc) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(changes) == 0 || e.nodes == nil {
		return e.version.Load()
	}

	stateChanged := false
	for id, instance := range changes {
		previous, ok := e.nodes[id]
		if !ok || previous.state == instance.State {
			continue
		}
		previous.state = instance.State
		e.nodes[id] = previous
		stateChanged = true
	}
	if stateChanged {
		e.version.Store(computeEpochNodes(e.nodes))
	}
	return e.version.Load()
}

// ComputeRingEpoch creates the deterministic, rolling-upgrade-compatible hash
// of ring state. This function is exported for testing purposes.
//
// The hash includes node IDs, states, and token counts. It intentionally omits
// token values because tokens are persisted and immutable between topology
// rebuilds. The sorted representation must remain stable because clients use
// this value to compare epochs across coordinator versions.
func ComputeRingEpoch(ringDesc *ring.Desc) uint64 {
	if ringDesc == nil || len(ringDesc.Ingesters) == 0 {
		return 0
	}

	nodes := make(map[string]epochNode, len(ringDesc.Ingesters))
	for id, instance := range ringDesc.Ingesters {
		nodes[id] = epochNode{state: instance.State, tokenCount: len(instance.Tokens)}
	}
	return computeEpochNodes(nodes)
}

func computeEpochNodes(nodes map[string]epochNode) uint64 {
	if len(nodes) == 0 {
		return 0
	}

	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var sb strings.Builder
	for _, id := range ids {
		node := nodes[id]
		sb.WriteString(id)
		sb.WriteByte(':')
		sb.WriteString(strconv.Itoa(int(node.state)))
		sb.WriteByte(':')
		sb.WriteString(strconv.Itoa(node.tokenCount))
		sb.WriteByte(';')
	}

	h := fnv.New64a()
	_, _ = h.Write([]byte(sb.String()))
	return h.Sum64()
}

// GetEpochFromRing is a convenience function to safely get epoch from a potentially nil RingManager.
func GetEpochFromRing(rm *RingManager) uint64 {
	if rm == nil {
		return 0
	}
	return rm.GetEpoch()
}
