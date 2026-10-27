// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/logsample"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/internal/listinput"
	"github.com/tigrisdata/ocache/storage/retry"
)

const (
	// MaxListLimit is the maximum number of keys to return in a single List call
	MaxListLimit = 1000
	// DefaultListLimit is the default limit if not specified
	DefaultListLimit = 1000
)

// ContinuationToken represents the state for resuming a paginated list operation.
type ContinuationToken struct {
	NodeCursors map[string]string `json:"node_cursors"` // Per-node cursors; an empty ID carries the global key or peer-mode prefix.
	Prefix      string            `json:"prefix"`       // Original prefix filter
}

type peerContinuationToken struct {
	NodeCursors map[string]string `json:"node_cursors"`
}

type continuationTokenFields struct {
	NodeCursors json.RawMessage `json:"node_cursors"`
	Prefix      string          `json:"prefix"`
}

// The empty node ID is reserved: a one-entry map carries a global cursor; a
// multi-entry map uses its value for the prefix and the remaining entries for
// peer cursors. Coordinator node IDs are non-empty.
const clusterGlobalCursorNodeID = ""

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

	// Single-node mode: query local storage directly. ListLocal handles an
	// explicitly supplied StartKey in ctx as well as the continuation token.
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

	// Resolve cursor presence separately so an explicit empty key is exclusive.
	startKey, hasStartKey := listinput.StartKey(ctx)
	if !hasStartKey {
		startKey = continuationToken
		hasStartKey = startKey != ""
	}
	var cursor *string
	if hasStartKey {
		cursor = &startKey
	}

	// Query local storage with pagination
	var keys []string
	var lastKey string
	var hasMore bool

	err := retry.Do(ctx, retry.DefaultConfig(), "ListKeysWithPagination", func() error {
		var listErr error
		keys, lastKey, hasMore, listErr = o.storage.ListKeysWithPaginationCursor(prefix, cursor, limit)
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

	// An explicit StartKey is already a raw exclusive key. Otherwise parse the
	// opaque continuation token, including legacy per-node cursors.
	startKey, hasStartKey := listinput.StartKey(ctx)
	var peerCursors map[string]string
	var err error
	if !hasStartKey {
		startKey, hasStartKey, peerCursors, err = o.parseContinuationToken(continuationToken, prefix)
		if err != nil {
			return nil, "", false, fmt.Errorf("invalid continuation token: %w", err)
		}
	}

	// Complete fan-outs use one global cursor. Partial fan-outs retain the
	// successful peers' cursors so a recovered peer is not skipped.

	zlog.Debug().
		Str("prefix", prefix).
		Int("node_count", len(nodes)).
		Int("limit", limit).
		Bool("with_values", withValues).
		Str("start_key", startKey).
		Bool("has_start_key", hasStartKey).
		Bool("has_peer_cursors", peerCursors != nil).
		Msg("Starting cluster-wide List with K-way merge")

	// Fetch from all nodes
	nodeResponses, err := o.fetchFromAllNodesWithCursorPresence(ctx, nodes, prefix, limit, startKey, hasStartKey, peerCursors, withValues)
	if err != nil {
		return nil, "", false, err
	}

	if len(nodeResponses) == 0 {
		return &mergeResult{Keys: []string{}}, "", false, nil
	}

	// Partial fan-outs keep the responding peers' progress in peer-cursor tokens.
	partialFanout := len(nodeResponses) < len(nodes)
	trackPeerCursors := partialFanout || peerCursors != nil
	merged, pagePeerCursors, hasMore, err := o.kWayMerge(ctx, nodeResponses, limit, withValues, trackPeerCursors, peerCursors)
	if err != nil {
		return nil, "", false, err
	}

	var newToken string
	if hasMore {
		switch {
		case partialFanout:
			newToken, err = o.encodePeerContinuationToken(pagePeerCursors, prefix)
		case len(merged.Keys) > 0:
			newToken, err = o.encodeContinuationToken(merged.Keys[len(merged.Keys)-1], prefix)
		case peerCursors != nil:
			newToken, err = o.encodePeerContinuationToken(pagePeerCursors, prefix)
		}
		if err == nil && newToken == "" {
			err = fmt.Errorf("list has more results but no continuation token")
		}
		if err != nil {
			zlog.Warn().Err(err).Msg("Failed to encode continuation token")
			return nil, "", false, err
		}
	}

	return merged, newToken, hasMore, nil
}

// maxListNodeFanout bounds the number of concurrent per-node list RPCs issued
// by a single cluster-wide List. Clusters smaller than this fan out fully.
const maxListNodeFanout = 32

// fetchFromAllNodesWithCursorPresence fetches keys (or key-value pairs) from
// every node, preserving whether the resume cursor is present when it is empty.
func (o *Operations) fetchFromAllNodesWithCursorPresence(ctx context.Context, nodes []*ring.NodeInfo, prefix string, limit int, startKey string, hasStartKey bool, peerCursors map[string]string, withValues bool) (map[string]*NodeResponse, error) {
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

			// Bail if already cancelled before acquiring, and again after: a
			// plain acquire would keep queued goroutines waiting on a slow peer
			// RPC long past the deadline, while select picks randomly among ready
			// cases — so with a free slot and a done context it could take the
			// acquire branch and issue a doomed RPC (spurious error-log noise)
			// instead of returning cleanly.
			if ctx.Err() != nil {
				return
			}
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			if ctx.Err() != nil {
				return
			}

			// Determine this node's exclusive cursor. Partial-page tokens carry
			// only successful peers' cursors; absent peers start from the beginning.
			// Cursor presence stays separate so an empty key remains exclusive.
			nodeStartKey := startKey
			cursorPresent := hasStartKey
			if peerCursors != nil {
				nodeStartKey, cursorPresent = peerCursors[n.ID]
			}
			if !cursorPresent {
				nodeStartKey = ""
			}
			emptyCursorPresent := cursorPresent && nodeStartKey == ""

			resp := &NodeResponse{NodeID: n.ID}
			var err error

			if n.ID == localNodeID {
				localCtx := ctx
				if cursorPresent {
					localCtx = listinput.WithStartKey(ctx, nodeStartKey)
				}
				if withValues {
					resp.Entries, _, resp.HasMore, err = o.ListLocalWithValues(localCtx, prefix, limit, nodeStartKey)
				} else {
					resp.Keys, _, resp.HasMore, err = o.ListLocal(localCtx, prefix, limit, nodeStartKey)
				}
			} else {
				client, clientErr := router.GetClientForNode(ctx, n.ID)
				if clientErr != nil {
					logsample.DegradedRing().Err(clientErr).Str("node_id", n.ID).Msg("Failed to get client for node, skipping")
					return
				}

				req := &pb.ListRequest{
					Prefix:   prefix,
					StartKey: nodeStartKey,
					Limit:    int32(limit),
				}

				rpcCtx := listinput.WithEmptyStartKeyCursor(ctx, emptyCursorPresent)
				if withValues {
					pbResp, respErr := client.ListLocalWithValues(rpcCtx, req)
					if respErr != nil {
						err = respErr
					} else {
						resp.Entries = pbResp.Entries
						resp.HasMore = pbResp.HasMore
					}
				} else {
					pbResp, respErr := client.ListLocal(rpcCtx, req)
					if respErr != nil {
						err = respErr
					} else {
						resp.Keys = pbResp.Keys
						resp.HasMore = pbResp.HasMore
					}
				}
			}

			if err != nil {
				logsample.DegradedRing().Err(err).Str("node_id", n.ID).Msg("Failed to list from node, skipping")
				return
			}

			mu.Lock()
			responses[n.ID] = resp
			mu.Unlock()
		}(node)
	}

	wg.Wait()

	// If the context was cancelled or its deadline passed, some nodes may have
	// been skipped — queued behind the fan-out limit, or their RPC aborted — so
	// the response set is incomplete. Surface the cancellation rather than
	// returning a partial result that a downstream merge would treat as complete.
	// (Individual node failures with a live context remain best-effort skips.)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	zlog.Debug().Int("response_count", len(responses)).Int("node_count", len(nodes)).Msg("Fetched from nodes")
	return responses, nil
}

// kWayMerge performs K-way merge of sorted responses from nodes using a min-heap.
// Works for both keys-only and withValues modes via NodeResponse.keyAt/itemCount.
func (o *Operations) kWayMerge(ctx context.Context, nodeResponses map[string]*NodeResponse, limit int, withValues bool, trackPeerCursors bool, previousCursors map[string]string) (*mergeResult, map[string]string, bool, error) {
	// Initialize min-heap
	h := &keyHeap{}
	heap.Init(h)

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
	// Deduplicate keys returned by multiple shards on this page. A peer that
	// resumes after a partial page may legitimately return a key below another
	// peer's cursor, so do not seed this set from prior-page cursors.
	seenKeys := make(map[string]bool)
	var peerCursors map[string]string
	if trackPeerCursors {
		// Keep prior progress for peers that did not advance on this page.
		peerCursors = make(map[string]string, len(nodeResponses)+len(previousCursors))
		for nodeID, key := range previousCursors {
			peerCursors[nodeID] = key
		}
	}

	// Merge until we have enough keys or heap is empty
	for h.Len() > 0 && len(result.Keys) < limit {
		select {
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		default:
		}

		// Pop minimum key
		minNode := heap.Pop(h).(*heapNode)
		if trackPeerCursors {
			// Record consumption even when this key is a duplicate across shards.
			peerCursors[minNode.nodeID] = minNode.key
		}

		// Deduplicate (shouldn't happen with proper partitioning, but handle it)
		if !seenKeys[minNode.key] {
			result.Keys = append(result.Keys, minNode.key)
			if withValues {
				nodeResp := nodeResponses[minNode.nodeID]
				idx := nodeIndices[minNode.nodeID]
				result.Entries = append(result.Entries, nodeResp.Entries[idx])
			}
			seenKeys[minNode.key] = true
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
				}
			}
		}
	}

	return result, peerCursors, hasMore, nil
}

// parseContinuationToken returns the global exclusive resume key, whether it is
// present, and any peer cursors. Plain start keys remain accepted; unmarked
// legacy per-node maps resume globally at their greatest saved key.
func (o *Operations) parseContinuationToken(token string, prefix string) (string, bool, map[string]string, error) {
	if token == "" {
		return "", false, nil, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		zlog.Debug().Str("token", token).Msg("Token is not base64, treating as plain start key")
		return token, true, nil, nil
	}

	var fields continuationTokenFields
	if err := json.Unmarshal(decoded, &fields); err != nil {
		zlog.Debug().Str("token", token).Msg("Token is not valid JSON, treating as plain start key")
		return token, true, nil, nil
	}
	if len(fields.NodeCursors) == 0 {
		zlog.Debug().Str("token", token).Msg("Token has no recognized cluster cursor, treating as plain start key")
		return token, true, nil, nil
	}

	var nodeCursors map[string]string
	if err := json.Unmarshal(fields.NodeCursors, &nodeCursors); err != nil || len(nodeCursors) == 0 {
		zlog.Debug().Str("token", token).Msg("Token has no recognized cluster cursor, treating as plain start key")
		return token, true, nil, nil
	}

	if peerPrefix, hasReservedCursor := nodeCursors[clusterGlobalCursorNodeID]; hasReservedCursor && len(nodeCursors) > 1 {
		if peerPrefix != prefix {
			return "", false, nil, fmt.Errorf("prefix mismatch: token has '%s', request has '%s'", peerPrefix, prefix)
		}
		delete(nodeCursors, clusterGlobalCursorNodeID)
		return "", false, nodeCursors, nil
	}

	if fields.Prefix != prefix {
		return "", false, nil, fmt.Errorf("prefix mismatch: token has '%s', request has '%s'", fields.Prefix, prefix)
	}
	if lastKey, hasGlobalCursor := nodeCursors[clusterGlobalCursorNodeID]; hasGlobalCursor {
		return lastKey, true, nil, nil
	}

	// Legacy per-node tokens lack a fan-out marker and page history. Resume at
	// their greatest cursor to preserve complete-page ordering.
	var lastKey string
	for _, cursor := range nodeCursors {
		if cursor > lastKey {
			lastKey = cursor
		}
	}
	return lastKey, true, nil, nil
}

// encodeContinuationToken creates a compact continuation token from the last emitted key.
func (o *Operations) encodeContinuationToken(lastKey string, prefix string) (string, error) {
	// Reuse the legacy envelope with one global cursor. The empty node ID is
	// reserved, so this entry cannot be confused with a real shard cursor.
	ct := ContinuationToken{
		NodeCursors: map[string]string{clusterGlobalCursorNodeID: lastKey},
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

func (o *Operations) encodePeerContinuationToken(peerCursors map[string]string, prefix string) (string, error) {
	if len(peerCursors) == 0 {
		return "", nil
	}

	// The empty node ID is reserved. Store the prefix there so the partial
	// cursor envelope needs no additional top-level fields.
	cursors := make(map[string]string, len(peerCursors)+1)
	cursors[clusterGlobalCursorNodeID] = prefix
	for nodeID, key := range peerCursors {
		if nodeID == clusterGlobalCursorNodeID {
			return "", fmt.Errorf("node ID cannot be empty")
		}
		cursors[nodeID] = key
	}
	ct := peerContinuationToken{NodeCursors: cursors}
	jsonBytes, err := json.Marshal(ct)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(jsonBytes), nil
}
