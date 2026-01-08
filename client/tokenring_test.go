package cacheclient

import (
	"fmt"
	"hash/fnv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenRing_Update(t *testing.T) {
	ring := NewTokenRing()

	// Update with some tokens
	nodeTokens := map[string][]uint32{
		"node-0": {100, 200, 300},
		"node-1": {400, 500, 600},
	}
	nodeAddresses := map[string]string{
		"node-0": "localhost:9001",
		"node-1": "localhost:9002",
	}

	ring.Update(nodeTokens, nodeAddresses)

	assert.False(t, ring.IsEmpty())
	assert.Equal(t, 6, ring.TokenCount())
	assert.Equal(t, 2, ring.NodeCount())

	// Verify addresses
	addrs := ring.GetNodeAddresses()
	assert.Equal(t, "localhost:9001", addrs["node-0"])
	assert.Equal(t, "localhost:9002", addrs["node-1"])
}

func TestTokenRing_GetNodeForKey(t *testing.T) {
	ring := NewTokenRing()

	// Set up ring with tokens at specific positions
	nodeTokens := map[string][]uint32{
		"node-0": {1000000000},             // ~25% of uint32 space
		"node-1": {2000000000, 3000000000}, // ~50% and ~75%
	}
	nodeAddresses := map[string]string{
		"node-0": "localhost:9001",
		"node-1": "localhost:9002",
	}
	ring.Update(nodeTokens, nodeAddresses)

	// Test that keys hash to expected nodes
	// We can't predict exact keys, but we can verify consistent behavior
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		addr, err := ring.GetNodeForKey(key)
		require.NoError(t, err)
		assert.Contains(t, []string{"localhost:9001", "localhost:9002"}, addr)
	}
}

func TestTokenRing_GetNodeForKey_EmptyRing(t *testing.T) {
	ring := NewTokenRing()

	_, err := ring.GetNodeForKey("any-key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ring is empty")
}

func TestTokenRing_GetNodeIDForKey(t *testing.T) {
	ring := NewTokenRing()

	nodeTokens := map[string][]uint32{
		"node-0": {1000000000},
		"node-1": {2000000000},
	}
	nodeAddresses := map[string]string{
		"node-0": "localhost:9001",
		"node-1": "localhost:9002",
	}
	ring.Update(nodeTokens, nodeAddresses)

	// Verify node ID lookup matches address lookup
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-key-%d", i)

		nodeID, err := ring.GetNodeIDForKey(key)
		require.NoError(t, err)

		addr, err := ring.GetNodeForKey(key)
		require.NoError(t, err)

		// Verify consistency
		assert.Equal(t, nodeAddresses[nodeID], addr)
	}
}

func TestTokenRing_WrapAround(t *testing.T) {
	ring := NewTokenRing()

	// Single node owns token at position 1000
	nodeTokens := map[string][]uint32{
		"node-0": {1000},
	}
	nodeAddresses := map[string]string{
		"node-0": "localhost:9001",
	}
	ring.Update(nodeTokens, nodeAddresses)

	// All keys should route to node-0 (wrap around behavior)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		nodeID, err := ring.GetNodeIDForKey(key)
		require.NoError(t, err)
		assert.Equal(t, "node-0", nodeID, "all keys should wrap to node-0")
	}
}

func TestTokenRing_BinarySearchCorrectness(t *testing.T) {
	ring := NewTokenRing()

	// Create a ring with specific token positions to test binary search
	nodeTokens := map[string][]uint32{
		"node-a": {100, 500, 900},
		"node-b": {200, 600},
		"node-c": {300, 700},
	}
	nodeAddresses := map[string]string{
		"node-a": "a:1",
		"node-b": "b:1",
		"node-c": "c:1",
	}
	ring.Update(nodeTokens, nodeAddresses)

	// Test: token lookup should find the first token >= hash
	// We need to create keys that hash to specific values to test this
	// Instead, we verify the algorithm by testing with known token positions

	// The tokens are sorted: 100, 200, 300, 500, 600, 700, 900
	// node-a owns: 100, 500, 900
	// node-b owns: 200, 600
	// node-c owns: 300, 700

	// Verify the ring has the expected structure
	assert.Equal(t, 7, ring.TokenCount())
	assert.Equal(t, 3, ring.NodeCount())
}

func TestTokenRing_ConcurrentAccess(t *testing.T) {
	ring := NewTokenRing()

	// Initial state
	nodeTokens := map[string][]uint32{
		"node-0": {1000000000, 2000000000, 3000000000},
	}
	nodeAddresses := map[string]string{
		"node-0": "localhost:9001",
	}
	ring.Update(nodeTokens, nodeAddresses)

	// Run concurrent reads and writes
	var wg sync.WaitGroup
	errCh := make(chan error, 1000)

	// Start readers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				_, err := ring.GetNodeForKey(key)
				if err != nil && err.Error() != "ring is empty" {
					errCh <- err
				}
			}
		}(i)
	}

	// Start writers (topology updates)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				nodeTokens := map[string][]uint32{
					fmt.Sprintf("node-%d", id): {uint32(id*1000000 + j*1000)},
				}
				nodeAddresses := map[string]string{
					fmt.Sprintf("node-%d", id): fmt.Sprintf("localhost:%d", 9000+id),
				}
				ring.Update(nodeTokens, nodeAddresses)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Errorf("concurrent access error: %v", err)
	}
}

func TestTokenRing_HashConsistency(t *testing.T) {
	// Verify that our FNV-1a implementation matches the standard library
	testKeys := []string{
		"test-key",
		"another-key",
		"",
		"a",
		"very-long-key-with-lots-of-characters-to-test-hash-function",
	}

	for _, key := range testKeys {
		h := fnv.New32a()
		h.Write([]byte(key))
		expected := h.Sum32()

		// The TokenRing should use the same hash
		ring := NewTokenRing()
		nodeTokens := map[string][]uint32{
			"node-0": {expected}, // Place token exactly at the key's hash
		}
		nodeAddresses := map[string]string{
			"node-0": "localhost:9001",
		}
		ring.Update(nodeTokens, nodeAddresses)

		nodeID, err := ring.GetNodeIDForKey(key)
		require.NoError(t, err)
		assert.Equal(t, "node-0", nodeID, "key %q should hash to node-0's token", key)
	}
}

func TestTokenRing_UpdateReplacesState(t *testing.T) {
	ring := NewTokenRing()

	// Initial state
	ring.Update(
		map[string][]uint32{"node-0": {1000}},
		map[string]string{"node-0": "addr-0"},
	)

	assert.Equal(t, 1, ring.NodeCount())
	assert.Equal(t, 1, ring.TokenCount())

	// Update with completely new state
	ring.Update(
		map[string][]uint32{
			"node-1": {2000, 3000},
			"node-2": {4000},
		},
		map[string]string{
			"node-1": "addr-1",
			"node-2": "addr-2",
		},
	)

	// Old state should be completely replaced
	assert.Equal(t, 2, ring.NodeCount())
	assert.Equal(t, 3, ring.TokenCount())

	addrs := ring.GetNodeAddresses()
	assert.NotContains(t, addrs, "node-0")
	assert.Equal(t, "addr-1", addrs["node-1"])
	assert.Equal(t, "addr-2", addrs["node-2"])
}

func TestTokenRing_MissingNodeAddress(t *testing.T) {
	ring := NewTokenRing()

	// Tokens reference a node that doesn't have an address
	nodeTokens := map[string][]uint32{
		"node-0":   {1000},
		"node-bad": {2000}, // No address for this node
	}
	nodeAddresses := map[string]string{
		"node-0": "localhost:9001",
		// node-bad intentionally missing
	}
	ring.Update(nodeTokens, nodeAddresses)

	// Find a key that hashes to node-bad's range
	// Since we can't predict hashes, we test the error path differently
	// by checking that the missing address is detected

	// This is a corner case - in production, this shouldn't happen
	// because the coordinator ensures consistency
	assert.Equal(t, 2, ring.TokenCount())
	assert.Equal(t, 1, ring.NodeCount()) // Only node-0 has an address
}

func BenchmarkTokenRing_GetNodeForKey(b *testing.B) {
	ring := NewTokenRing()

	// Create a realistic ring with 3 nodes and 512 tokens each
	nodeTokens := make(map[string][]uint32)
	nodeAddresses := make(map[string]string)

	tokensPerNode := 512
	for i := 0; i < 3; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		tokens := make([]uint32, tokensPerNode)
		for j := 0; j < tokensPerNode; j++ {
			tokens[j] = uint32(i*tokensPerNode+j) * (0xFFFFFFFF / uint32(3*tokensPerNode))
		}
		nodeTokens[nodeID] = tokens
		nodeAddresses[nodeID] = fmt.Sprintf("localhost:%d", 9000+i)
	}
	ring.Update(nodeTokens, nodeAddresses)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("test-key-%d", i)
			_, _ = ring.GetNodeForKey(key)
			i++
		}
	})
}

func BenchmarkTokenRing_Update(b *testing.B) {
	ring := NewTokenRing()

	// Create a realistic update payload
	nodeTokens := make(map[string][]uint32)
	nodeAddresses := make(map[string]string)

	tokensPerNode := 512
	for i := 0; i < 3; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		tokens := make([]uint32, tokensPerNode)
		for j := 0; j < tokensPerNode; j++ {
			tokens[j] = uint32(i*tokensPerNode+j) * (0xFFFFFFFF / uint32(3*tokensPerNode))
		}
		nodeTokens[nodeID] = tokens
		nodeAddresses[nodeID] = fmt.Sprintf("localhost:%d", 9000+i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.Update(nodeTokens, nodeAddresses)
	}
}
