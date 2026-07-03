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

// NodeResponse holds the response from a single node.
// In keys-only mode, Keys is populated. In withValues mode, Entries is populated.
type NodeResponse struct {
	NodeID            string
	Keys              []string       // populated in keys-only mode
	Entries           []*pb.KeyValue // populated in withValues mode
	ContinuationToken string
	HasMore           bool
}

// itemCount returns the number of items in this response.
func (nr *NodeResponse) itemCount() int {
	if len(nr.Entries) > 0 {
		return len(nr.Entries)
	}
	return len(nr.Keys)
}

// keyAt returns the key at the given index.
func (nr *NodeResponse) keyAt(i int) string {
	if len(nr.Entries) > 0 {
		return nr.Entries[i].Key
	}
	return nr.Keys[i]
}

// mergeResult holds the output of the unified K-way merge.
type mergeResult struct {
	Keys    []string       // always populated
	Entries []*pb.KeyValue // non-nil only in withValues mode
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
		merged, token, hasMore, err := o.listClusterWide(ctx, prefix, limit, continuationToken, false)
		done(err)
		if err != nil {
			return nil, "", false, err
		}
		return merged.Keys, token, hasMore, nil
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

// listClusterWide performs K-way merge of responses from all nodes.
// When withValues is true, responses include full key-value entries.
// Returns (mergeResult, continuationToken, hasMore, error).
func (o *Operations) listClusterWide(ctx context.Context, prefix string, limit int, continuationToken string, withValues bool) (*mergeResult, string, bool, error) {
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
		Bool("with_values", withValues).
		Interface("node_cursors", nodeCursors).
		Msg("Starting cluster-wide List with K-way merge")

	// Fetch from all nodes
	nodeResponses, err := o.fetchFromAllNodes(ctx, nodes, prefix, limit, nodeCursors, withValues)
	if err != nil {
		return nil, "", false, err
	}

	if len(nodeResponses) == 0 {
		return &mergeResult{Keys: []string{}}, "", false, nil
	}

	// Perform K-way merge
	merged, newCursors, hasMore, err := o.kWayMerge(ctx, nodeResponses, limit, withValues)
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

	return merged, newToken, hasMore, nil
}

// fetchFromAllNodes fetches keys (or key-value pairs) from all nodes in parallel.
// maxListNodeFanout bounds the number of concurrent per-node list RPCs issued
// by a single cluster-wide List. Clusters smaller than this fan out fully.
const maxListNodeFanout = 32

func (o *Operations) fetchFromAllNodes(ctx context.Context, nodes []*ring.NodeInfo, prefix string, limit int, nodeCursors map[string]string, withValues bool) (map[string]*NodeResponse, error) {
	localNodeID := o.GetLocalNodeID()
	router := o.GetRouter()

	responses := make(map[string]*NodeResponse)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Bound how many peer list RPCs run concurrently per List so a large cluster
	// (or many concurrent List requests) cannot fan out an unbounded number of
	// blocking calls — each of which would otherwise pin a goroutine/connection.
	sem := make(chan struct{}, maxListNodeFanout)

	for _, node := range nodes {
		wg.Add(1)
		go func(n *ring.NodeInfo) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Determine start key for this node
			startKey := ""
			if cursor, exists := nodeCursors[n.ID]; exists && cursor != "" {
				startKey = cursor
			}

			resp := &NodeResponse{NodeID: n.ID}
			var err error

			if n.ID == localNodeID {
				if withValues {
					resp.Entries, resp.ContinuationToken, resp.HasMore, err = o.ListLocalWithValues(ctx, prefix, limit, startKey)
				} else {
					resp.Keys, resp.ContinuationToken, resp.HasMore, err = o.ListLocal(ctx, prefix, limit, startKey)
				}
			} else {
				client, clientErr := router.GetClientForNode(n.ID)
				if clientErr != nil {
					zlog.Warn().Err(clientErr).Str("node_id", n.ID).Msg("Failed to get client for node, skipping")
					return
				}

				req := &pb.ListRequest{
					Prefix:   prefix,
					StartKey: startKey,
					Limit:    int32(limit),
				}

				if withValues {
					pbResp, respErr := client.ListLocalWithValues(ctx, req)
					if respErr != nil {
						err = respErr
					} else {
						resp.Entries = pbResp.Entries
						resp.ContinuationToken = pbResp.ContinuationToken
						resp.HasMore = pbResp.HasMore
					}
				} else {
					pbResp, respErr := client.ListLocal(ctx, req)
					if respErr != nil {
						err = respErr
					} else {
						resp.Keys = pbResp.Keys
						resp.ContinuationToken = pbResp.ContinuationToken
						resp.HasMore = pbResp.HasMore
					}
				}
			}

			if err != nil {
				zlog.Warn().Err(err).Str("node_id", n.ID).Msg("Failed to list from node, skipping")
				return
			}

			mu.Lock()
			responses[n.ID] = resp
			mu.Unlock()
		}(node)
	}

	wg.Wait()

	zlog.Debug().Int("response_count", len(responses)).Int("node_count", len(nodes)).Msg("Fetched from nodes")
	return responses, nil
}

// kWayMerge performs K-way merge of sorted responses from nodes using a min-heap.
// Works for both keys-only and withValues modes via NodeResponse.keyAt/itemCount.
func (o *Operations) kWayMerge(ctx context.Context, nodeResponses map[string]*NodeResponse, limit int, withValues bool) (*mergeResult, map[string]string, bool, error) {
	// Initialize min-heap
	h := &keyHeap{}
	heap.Init(h)

	// Track node cursors (last key from each node)
	nodeCursors := make(map[string]string)

	// Track key indices for each node
	nodeIndices := make(map[string]int)

	// Prime the heap with first key from each node
	for nodeID, nodeResp := range nodeResponses {
		if nodeResp.itemCount() > 0 {
			heap.Push(h, &heapNode{
				key:    nodeResp.keyAt(0),
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
	result := &mergeResult{
		Keys: make([]string, 0, limit),
	}
	if withValues {
		result.Entries = make([]*pb.KeyValue, 0, limit)
	}
	seenKeys := make(map[string]bool) // Deduplication (defensive)

	// Merge until we have enough keys or heap is empty
	for h.Len() > 0 && len(result.Keys) < limit {
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
			result.Keys = append(result.Keys, minNode.key)
			if withValues {
				nodeResp := nodeResponses[minNode.nodeID]
				idx := nodeIndices[minNode.nodeID]
				result.Entries = append(result.Entries, nodeResp.Entries[idx])
			}
			seenKeys[minNode.key] = true
			addedToResult = true
		}

		// Try to get next key from this node's response
		nodeResp := nodeResponses[minNode.nodeID]
		currentIndex := nodeIndices[minNode.nodeID]
		nextIndex := currentIndex + 1

		if nextIndex < nodeResp.itemCount() {
			// More keys available in this node's response
			heap.Push(h, &heapNode{
				key:    nodeResp.keyAt(nextIndex),
				nodeID: minNode.nodeID,
				done:   false,
			})
			nodeIndices[minNode.nodeID] = nextIndex
		}

		// If we hit limit due to a duplicate key, continue to fill up to limit
		if !addedToResult && len(result.Keys) >= limit {
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
				if idx >= nodeResp.itemCount()-1 {
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
