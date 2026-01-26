package operations

import (
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/retry"
)

const (
	// MaxListLimit is the maximum number of keys to return in a single List call
	MaxListLimit = 1000
	// DefaultListLimit is the default limit if not specified
	DefaultListLimit = 1000
)

// ContinuationToken represents the state for resuming a paginated list operation
type ContinuationToken struct {
	NodeCursors map[string]string `json:"node_cursors"` // nodeID -> last_key_from_node
	Prefix      string            `json:"prefix"`       // Original prefix filter
}

// heapNode represents a key from a specific node for the K-way merge
type heapNode struct {
	key    string
	nodeID string
	done   bool // True if this node has no more keys
}

// keyHeap implements heap.Interface for K-way merge of sorted keys
type keyHeap []*heapNode

func (h keyHeap) Len() int           { return len(h) }
func (h keyHeap) Less(i, j int) bool { return h[i].key < h[j].key }
func (h keyHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *keyHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapNode))
}

func (h *keyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// NodeResponse holds the response from a single node
type NodeResponse struct {
	NodeID            string
	Keys              []string
	ContinuationToken string
	HasMore           bool
}

// List returns all keys matching prefix across the entire cluster.
// In cluster mode, performs K-way merge from all nodes.
// In single-node mode, queries local storage directly.
func (o *Operations) List(ctx context.Context, prefix string) ([]string, error) {
	done := recordOperationStart("Operations.List")

	var allKeys []string
	continuationToken := ""

	// Paginate through all results
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			done(err)
			return nil, err
		default:
		}

		keys, nextToken, hasMore, err := o.ListPage(ctx, prefix, MaxListLimit, continuationToken)
		if err != nil {
			done(err)
			return nil, err
		}

		allKeys = append(allKeys, keys...)

		if !hasMore || nextToken == "" {
			break
		}
		continuationToken = nextToken
	}

	done(nil)
	return allKeys, nil
}

// ListPage returns a page of keys with pagination support.
// In cluster mode, performs K-way merge from all nodes.
// In single-node mode, queries local storage directly.
// Returns (keys, continuationToken, hasMore, error).
func (o *Operations) ListPage(ctx context.Context, prefix string, limit int, continuationToken string) ([]string, string, bool, error) {
	done := recordOperationStart("Operations.ListPage")

	zlog.Debug().
		Str("prefix", prefix).
		Int("limit", limit).
		Str("continuation_token", continuationToken).
		Msg("Operations.ListPage called")

	// Apply limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	// If clustering is enabled, perform K-way merge
	if o.IsClusterMode() {
		keys, token, hasMore, err := o.listClusterWide(ctx, prefix, limit, continuationToken)
		done(err)
		return keys, token, hasMore, err
	}

	// Single-node mode: query local storage directly
	keys, token, hasMore, err := o.ListLocal(ctx, prefix, limit, continuationToken)
	done(err)
	return keys, token, hasMore, err
}

// ListLocal returns keys from local storage only.
func (o *Operations) ListLocal(ctx context.Context, prefix string, limit int, continuationToken string) ([]string, string, bool, error) {
	// Apply limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	// Determine start key from continuation token
	startKey := continuationToken

	// Query local storage with pagination
	var keys []string
	var lastKey string
	var hasMore bool

	err := retry.Do(ctx, retry.DefaultConfig(), "ListKeysWithPagination", func() error {
		var listErr error
		keys, lastKey, hasMore, listErr = o.storage.ListKeysWithPagination(prefix, startKey, limit)
		return listErr
	})
	if err != nil {
		return nil, "", false, err
	}

	// Build continuation token
	var nextToken string
	if hasMore && lastKey != "" {
		nextToken = lastKey
	}

	return keys, nextToken, hasMore, nil
}

// listClusterWide performs K-way merge of responses from all nodes
func (o *Operations) listClusterWide(ctx context.Context, prefix string, limit int, continuationToken string) ([]string, string, bool, error) {
	// Get all active nodes from the ring
	ringMgr := o.GetRing()
	nodes := ringMgr.GetActiveNodes()

	if len(nodes) == 0 {
		return nil, "", false, fmt.Errorf("no active nodes in cluster")
	}

	// Parse continuation token if provided
	nodeCursors, isPlainStartKey, err := o.parseContinuationToken(continuationToken, prefix)
	if err != nil {
		return nil, "", false, fmt.Errorf("invalid continuation token: %w", err)
	}

	// If the token is a plain start key (for backwards compatibility with single-node mode),
	// initialize all nodes to start from this key
	if isPlainStartKey && continuationToken != "" {
		nodeCursors = make(map[string]string)
		for _, node := range nodes {
			nodeCursors[node.ID] = continuationToken
		}
	}

	zlog.Debug().
		Str("prefix", prefix).
		Int("node_count", len(nodes)).
		Int("limit", limit).
		Interface("node_cursors", nodeCursors).
		Msg("Starting cluster-wide List with K-way merge")

	// Fetch keys from all nodes
	nodeResponses, err := o.fetchFromAllNodes(ctx, nodes, prefix, limit, nodeCursors)
	if err != nil {
		return nil, "", false, err
	}

	if len(nodeResponses) == 0 {
		return []string{}, "", false, nil
	}

	// Perform K-way merge
	result, newCursors, hasMore, err := o.kWayMerge(ctx, nodeResponses, limit)
	if err != nil {
		return nil, "", false, err
	}

	// Build continuation token if needed
	var newToken string
	if hasMore {
		newToken, err = o.encodeContinuationToken(newCursors, prefix)
		if err != nil {
			zlog.Warn().Err(err).Msg("Failed to encode continuation token")
			// Continue without token rather than failing the request
		}
	}

	return result, newToken, hasMore, nil
}

// fetchFromAllNodes fetches keys from all nodes in parallel
func (o *Operations) fetchFromAllNodes(ctx context.Context, nodes []*ring.NodeInfo, prefix string, limit int, nodeCursors map[string]string) (map[string]*NodeResponse, error) {
	localNodeID := o.GetLocalNodeID()
	router := o.GetRouter()

	responses := make(map[string]*NodeResponse)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(n *ring.NodeInfo) {
			defer wg.Done()

			// Determine start key for this node
			startKey := ""
			if cursor, exists := nodeCursors[n.ID]; exists && cursor != "" {
				startKey = cursor
			}

			var keys []string
			var nextToken string
			var hasMore bool
			var err error

			// If this is the local node, call directly
			if n.ID == localNodeID {
				keys, nextToken, hasMore, err = o.ListLocal(ctx, prefix, limit, startKey)
			} else {
				// Remote node: get client
				client, clientErr := router.GetClientForNode(n.ID)
				if clientErr != nil {
					zlog.Warn().Err(clientErr).Str("node_id", n.ID).Msg("Failed to get client for node, skipping")
					return
				}

				// Build request for remote node
				req := &pb.ListRequest{
					Prefix:   prefix,
					StartKey: startKey,
					Limit:    int32(limit),
				}

				resp, respErr := client.ListLocal(ctx, req)
				if respErr != nil {
					err = respErr
				} else {
					keys = resp.Keys
					nextToken = resp.ContinuationToken
					hasMore = resp.HasMore
				}
			}

			if err != nil {
				zlog.Warn().Err(err).Str("node_id", n.ID).Msg("Failed to list from node, skipping")
				return
			}

			// Store response
			mu.Lock()
			responses[n.ID] = &NodeResponse{
				NodeID:            n.ID,
				Keys:              keys,
				ContinuationToken: nextToken,
				HasMore:           hasMore,
			}
			mu.Unlock()
		}(node)
	}

	wg.Wait()

	zlog.Debug().Int("response_count", len(responses)).Int("node_count", len(nodes)).Msg("Fetched keys from nodes")
	return responses, nil
}

// kWayMerge performs K-way merge of sorted responses from nodes
func (o *Operations) kWayMerge(ctx context.Context, nodeResponses map[string]*NodeResponse, limit int) ([]string, map[string]string, bool, error) {
	// Initialize min-heap
	h := &keyHeap{}
	heap.Init(h)

	// Track node cursors (last key from each node)
	nodeCursors := make(map[string]string)

	// Track key indices for each node
	nodeIndices := make(map[string]int)

	// Prime the heap with first key from each node
	for nodeID, nodeResp := range nodeResponses {
		if len(nodeResp.Keys) > 0 {
			heap.Push(h, &heapNode{
				key:    nodeResp.Keys[0],
				nodeID: nodeID,
				done:   false,
			})
			nodeIndices[nodeID] = 0
		} else {
			// Initialize index even for empty responses to track HasMore state
			nodeIndices[nodeID] = -1
		}
	}

	// Collect merged results
	result := make([]string, 0, limit)
	seenKeys := make(map[string]bool) // Deduplication (defensive)

	// Merge until we have enough keys or heap is empty
	for h.Len() > 0 && len(result) < limit {
		select {
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		default:
		}

		// Pop minimum key
		minNode := heap.Pop(h).(*heapNode)

		// Always update cursor for this node (even if duplicate)
		nodeCursors[minNode.nodeID] = minNode.key

		// Deduplicate (shouldn't happen with proper partitioning, but handle it)
		addedToResult := false
		if !seenKeys[minNode.key] {
			result = append(result, minNode.key)
			seenKeys[minNode.key] = true
			addedToResult = true
		}

		// Try to get next key from this node's response
		nodeResp := nodeResponses[minNode.nodeID]
		currentIndex := nodeIndices[minNode.nodeID]
		nextIndex := currentIndex + 1

		if nextIndex < len(nodeResp.Keys) {
			// More keys available in this node's response
			heap.Push(h, &heapNode{
				key:    nodeResp.Keys[nextIndex],
				nodeID: minNode.nodeID,
				done:   false,
			})
			nodeIndices[minNode.nodeID] = nextIndex
		}

		// If we hit limit due to a duplicate key, continue to fill up to limit
		if !addedToResult && len(result) >= limit {
			break
		}
	}

	// Determine if there are more keys available
	hasMore := h.Len() > 0

	if !hasMore {
		// Check if any node has more data
		for nodeID, nodeResp := range nodeResponses {
			if nodeResp.HasMore {
				idx := nodeIndices[nodeID]
				if idx >= len(nodeResp.Keys)-1 {
					hasMore = true
					if nodeResp.ContinuationToken != "" {
						nodeCursors[nodeID] = nodeResp.ContinuationToken
					}
				}
			}
		}
	}

	return result, nodeCursors, hasMore, nil
}

// parseContinuationToken decodes the continuation token.
// Returns (nodeCursors, isPlainStartKey, error).
// If the token is not a valid cluster token (e.g., a plain start key for backwards compatibility),
// returns (nil, true, nil) to indicate the token should be treated as a plain start key.
func (o *Operations) parseContinuationToken(token string, prefix string) (map[string]string, bool, error) {
	if token == "" {
		return make(map[string]string), false, nil
	}

	// Decode base64
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		// Not a valid cluster token - treat as plain start key for backwards compatibility
		zlog.Debug().Str("token", token).Msg("Token is not base64, treating as plain start key")
		return nil, true, nil
	}

	// Parse JSON
	var ct ContinuationToken
	if err := json.Unmarshal(decoded, &ct); err != nil {
		// Valid base64 but not valid JSON - treat as plain start key
		zlog.Debug().Str("token", token).Msg("Token is not valid JSON, treating as plain start key")
		return nil, true, nil
	}

	// Validate prefix matches
	if ct.Prefix != prefix {
		return nil, false, fmt.Errorf("prefix mismatch: token has '%s', request has '%s'", ct.Prefix, prefix)
	}

	return ct.NodeCursors, false, nil
}

// encodeContinuationToken creates a continuation token from node cursors
func (o *Operations) encodeContinuationToken(nodeCursors map[string]string, prefix string) (string, error) {
	if len(nodeCursors) == 0 {
		return "", nil
	}

	ct := ContinuationToken{
		NodeCursors: nodeCursors,
		Prefix:      prefix,
	}

	// Encode as JSON
	jsonBytes, err := json.Marshal(ct)
	if err != nil {
		return "", err
	}

	// Encode as base64
	return base64.StdEncoding.EncodeToString(jsonBytes), nil
}
