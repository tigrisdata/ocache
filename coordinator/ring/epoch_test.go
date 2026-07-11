// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"sync"
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeRingEpoch_EmptyRing(t *testing.T) {
	// Nil ring
	assert.Equal(t, uint64(0), ComputeRingEpoch(nil))

	// Empty ingesters
	ringDesc := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{},
	}
	assert.Equal(t, uint64(0), ComputeRingEpoch(ringDesc))
}

func TestComputeRingEpoch_SingleNode(t *testing.T) {
	ringDesc := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {
				Addr:   "localhost:9001",
				State:  ring.ACTIVE,
				Tokens: []uint32{100, 200, 300},
			},
		},
	}

	epoch := ComputeRingEpoch(ringDesc)
	assert.NotEqual(t, uint64(0), epoch)

	// Same input should produce same epoch (deterministic)
	epoch2 := ComputeRingEpoch(ringDesc)
	assert.Equal(t, epoch, epoch2)
}

func TestComputeRingEpoch_Deterministic(t *testing.T) {
	// Create two identical ring descriptions
	ringDesc1 := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-a": {State: ring.ACTIVE, Tokens: []uint32{100, 200}},
			"node-b": {State: ring.ACTIVE, Tokens: []uint32{300, 400}},
			"node-c": {State: ring.JOINING, Tokens: []uint32{500}},
		},
	}

	ringDesc2 := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-c": {State: ring.JOINING, Tokens: []uint32{500}}, // Different order
			"node-a": {State: ring.ACTIVE, Tokens: []uint32{100, 200}},
			"node-b": {State: ring.ACTIVE, Tokens: []uint32{300, 400}},
		},
	}

	epoch1 := ComputeRingEpoch(ringDesc1)
	epoch2 := ComputeRingEpoch(ringDesc2)

	// Should produce same epoch despite different map iteration order
	assert.Equal(t, epoch1, epoch2, "Epochs should be equal for identical ring state")
}

func TestComputeRingEpoch_DifferentStates(t *testing.T) {
	// Ring with ACTIVE node
	ringActive := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100}},
		},
	}

	// Same ring but node is JOINING
	ringJoining := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.JOINING, Tokens: []uint32{100}},
		},
	}

	epochActive := ComputeRingEpoch(ringActive)
	epochJoining := ComputeRingEpoch(ringJoining)

	// Different states should produce different epochs
	assert.NotEqual(t, epochActive, epochJoining, "State changes should produce different epochs")
}

func TestComputeRingEpoch_DifferentTokenCounts(t *testing.T) {
	// Ring with 2 tokens
	ring2Tokens := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100, 200}},
		},
	}

	// Same ring but 3 tokens
	ring3Tokens := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100, 200, 300}},
		},
	}

	epoch2 := ComputeRingEpoch(ring2Tokens)
	epoch3 := ComputeRingEpoch(ring3Tokens)

	// Different token counts should produce different epochs
	assert.NotEqual(t, epoch2, epoch3, "Token count changes should produce different epochs")
}

func TestComputeRingEpoch_DifferentNodes(t *testing.T) {
	// Ring with node-a
	ringA := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-a": {State: ring.ACTIVE, Tokens: []uint32{100}},
		},
	}

	// Ring with node-b (different node ID)
	ringB := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-b": {State: ring.ACTIVE, Tokens: []uint32{100}},
		},
	}

	epochA := ComputeRingEpoch(ringA)
	epochB := ComputeRingEpoch(ringB)

	// Different node IDs should produce different epochs
	assert.NotEqual(t, epochA, epochB, "Different node IDs should produce different epochs")
}

func TestComputeRingEpoch_NodeAddition(t *testing.T) {
	// Ring with 2 nodes
	ring2Nodes := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100}},
			"node-2": {State: ring.ACTIVE, Tokens: []uint32{200}},
		},
	}

	// Ring with 3 nodes
	ring3Nodes := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100}},
			"node-2": {State: ring.ACTIVE, Tokens: []uint32{200}},
			"node-3": {State: ring.ACTIVE, Tokens: []uint32{300}},
		},
	}

	epoch2 := ComputeRingEpoch(ring2Nodes)
	epoch3 := ComputeRingEpoch(ring3Nodes)

	// Adding a node should produce different epoch
	assert.NotEqual(t, epoch2, epoch3, "Adding a node should produce different epoch")
}

func TestEpoch_SetFromRingState(t *testing.T) {
	e := NewEpoch()

	ringDesc := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100, 200}},
			"node-2": {State: ring.ACTIVE, Tokens: []uint32{300, 400}},
		},
	}

	// Set epoch from ring state
	epoch := e.Set(ringDesc)
	require.NotEqual(t, uint64(0), epoch)

	// Get should return the same value
	assert.Equal(t, epoch, e.Get())

	// Setting from same ring state should produce same epoch
	epoch2 := e.Set(ringDesc)
	assert.Equal(t, epoch, epoch2)
}

func TestEpoch_ConcurrentSetFromRingState(t *testing.T) {
	e := NewEpoch()
	const numGoroutines = 100
	const iterationsPerGoroutine = 100

	// Create different ring states
	ringStates := []*ring.Desc{
		{Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100}},
		}},
		{Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100}},
			"node-2": {State: ring.ACTIVE, Tokens: []uint32{200}},
		}},
		{Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100}},
			"node-2": {State: ring.ACTIVE, Tokens: []uint32{200}},
			"node-3": {State: ring.JOINING, Tokens: []uint32{300}},
		}},
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterationsPerGoroutine; j++ {
				// Alternate between ring states
				ringState := ringStates[(id+j)%len(ringStates)]
				e.Set(ringState)
				_ = e.Get() // Read should not panic
			}
		}(i)
	}

	wg.Wait()

	// Final epoch should be one of the valid epochs
	finalEpoch := e.Get()
	validEpochs := make(map[uint64]bool)
	for _, rs := range ringStates {
		validEpochs[ComputeRingEpoch(rs)] = true
	}
	assert.True(t, validEpochs[finalEpoch] || finalEpoch == 0, "Final epoch should be one of the valid epochs")
}

func TestGetEpochFromRing_Nil(t *testing.T) {
	// Should return 0 for nil RingManager
	assert.Equal(t, uint64(0), GetEpochFromRing(nil))
}

// TestComputeRingEpoch_TokenValuesDontMatter verifies that actual token values
// don't affect the epoch - only the count matters. This is by design since
// tokens are immutable once assigned.
func TestComputeRingEpoch_TokenValuesDontMatter(t *testing.T) {
	// Ring with tokens {100, 200}
	ringTokensA := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{100, 200}},
		},
	}

	// Ring with tokens {999, 888} - different values, same count
	ringTokensB := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"node-1": {State: ring.ACTIVE, Tokens: []uint32{999, 888}},
		},
	}

	epochA := ComputeRingEpoch(ringTokensA)
	epochB := ComputeRingEpoch(ringTokensB)

	// Same epoch because we only hash token COUNT, not values
	assert.Equal(t, epochA, epochB, "Token values should not affect epoch (only count matters)")
}
