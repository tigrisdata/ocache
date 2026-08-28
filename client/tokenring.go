// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync/atomic"
)

// tokenEntry represents a token and its owning node
type tokenEntry struct {
	token  uint32
	nodeID string
}

// ringState holds the immutable state of the token ring.
// This is atomically swapped on updates, enabling lock-free reads.
type ringState struct {
	tokens []tokenEntry      // Sorted by token value
	nodes  map[string]string // nodeID -> listenAddress
}

// TokenRing implements token-based consistent hashing matching the server's dskit ring.
// It uses FNV-1a 32-bit hash and binary search for O(log n) lookups.
// The ring is structured as a sorted array of tokens. Each token is owned by a specific node.
//
// Thread Safety:
// Updates use atomic pointer swapping, so reads are completely lock-free.
type TokenRing struct {
	state atomic.Pointer[ringState]
}

// NewTokenRing creates a new empty token ring
func NewTokenRing() *TokenRing {
	r := &TokenRing{}
	// Initialize with empty state
	r.state.Store(&ringState{
		tokens: nil,
		nodes:  make(map[string]string),
	})
	return r
}

// buildRingState constructs an immutable ring state without publishing it.
func buildRingState(nodeTokens map[string][]uint32, nodeAddresses map[string]string) *ringState {
	// Count total tokens for pre-allocation.
	totalTokens := 0
	for _, tokens := range nodeTokens {
		totalTokens += len(tokens)
	}

	// Build sorted token list.
	entries := make([]tokenEntry, 0, totalTokens)
	for nodeID, tokens := range nodeTokens {
		for _, token := range tokens {
			entries = append(entries, tokenEntry{token: token, nodeID: nodeID})
		}
	}

	// Sort by token value for binary search.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].token < entries[j].token
	})

	// Copy node addresses.
	nodes := make(map[string]string, len(nodeAddresses))
	for nodeID, addr := range nodeAddresses {
		nodes[nodeID] = addr
	}

	return &ringState{
		tokens: entries,
		nodes:  nodes,
	}
}

// snapshot returns the immutable state currently published by the ring.
func (r *TokenRing) snapshot() *ringState {
	return r.state.Load()
}

// Update rebuilds the ring with new token assignments.
// nodeTokens is a map of nodeID -> list of tokens owned by that node.
// nodeAddresses is a map of nodeID -> listen address for that node.
//
// This method uses atomic pointer swapping, so concurrent reads see either
// the old state or the new state atomically - never a partially updated state.
// This eliminates contention between topology updates and client requests.
func (r *TokenRing) Update(nodeTokens map[string][]uint32, nodeAddresses map[string]string) {
	r.state.Store(buildRingState(nodeTokens, nodeAddresses))
}

func getNodeForKeyFromState(state *ringState, key string) (string, error) {
	if state == nil || len(state.tokens) == 0 {
		return "", fmt.Errorf("ring is empty")
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(key)) // Write never returns an error for fnv
	token := h.Sum32()

	idx := sort.Search(len(state.tokens), func(i int) bool {
		return state.tokens[i].token >= token
	})
	if idx == len(state.tokens) {
		idx = 0
	}

	nodeID := state.tokens[idx].nodeID
	addr, exists := state.nodes[nodeID]
	if !exists {
		return "", fmt.Errorf("node %s not found in address map", nodeID)
	}
	return addr, nil
}

// GetNodeForKey returns the node address that owns the given key.
// Uses FNV-1a 32-bit hash (same as server) + binary search.
//
// This method is lock-free - it reads from an atomically swapped pointer.
func (r *TokenRing) GetNodeForKey(key string) (string, error) {
	return getNodeForKeyFromState(r.snapshot(), key)
}

// getNodeIDForKeyFromState returns the node ID from an immutable ring state.
func getNodeIDForKeyFromState(state *ringState, key string) (string, error) {
	if state == nil || len(state.tokens) == 0 {
		return "", fmt.Errorf("ring is empty")
	}

	// Hash key with FNV-1a 32-bit (same as server).
	h := fnv.New32a()
	_, _ = h.Write([]byte(key)) // Write never returns an error for fnv
	token := h.Sum32()

	// Binary search for the first token >= our hash.
	idx := sort.Search(len(state.tokens), func(i int) bool {
		return state.tokens[i].token >= token
	})
	if idx == len(state.tokens) {
		idx = 0
	}
	return state.tokens[idx].nodeID, nil
}

// getNodeInfoForKeyFromState returns the node ID and address from an immutable
// ring state.
func getNodeInfoForKeyFromState(state *ringState, key string) (nodeID, address string, err error) {
	nodeID, err = getNodeIDForKeyFromState(state, key)
	if err != nil {
		return "", "", err
	}
	address, exists := state.nodes[nodeID]
	if !exists {
		return "", "", fmt.Errorf("node %s not found in address map", nodeID)
	}
	return nodeID, address, nil
}

// GetNodeIDForKey returns the node ID that owns the given key.
// This is useful for debugging and testing.
// This method is lock-free.
func (r *TokenRing) GetNodeIDForKey(key string) (string, error) {
	return getNodeIDForKeyFromState(r.state.Load(), key)
}

// IsEmpty returns true if the ring has no tokens
func (r *TokenRing) IsEmpty() bool {
	state := r.state.Load()
	return len(state.tokens) == 0
}

// TokenCount returns the total number of tokens in the ring
func (r *TokenRing) TokenCount() int {
	state := r.state.Load()
	return len(state.tokens)
}

// NodeCount returns the number of unique nodes in the ring
func (r *TokenRing) NodeCount() int {
	state := r.state.Load()
	return len(state.nodes)
}

// GetNodeAddresses returns a copy of all node addresses
func (r *TokenRing) GetNodeAddresses() map[string]string {
	state := r.state.Load()

	result := make(map[string]string, len(state.nodes))
	for nodeID, addr := range state.nodes {
		result[nodeID] = addr
	}
	return result
}

// GetNodeInfoForKey returns both the node ID and address that owns the given key.
// This method is lock-free.
func (r *TokenRing) GetNodeInfoForKey(key string) (nodeID, address string, err error) {
	return getNodeInfoForKeyFromState(r.state.Load(), key)
}
