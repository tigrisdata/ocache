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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// MaxListLimit is the maximum number of keys to return in a single List call
	MaxListLimit = 1000
	// DefaultListLimit is the default limit if not specified
	DefaultListLimit = 1000
)

// ContinuationToken represents the state for resuming a paginated list operation.
type ContinuationToken struct {
	NodeCursors  map[string]string `json:"node_cursors"`            // Per-node cursors; an empty ID marks a partial-page token.
	Prefix       string            `json:"prefix,omitempty"`        // Legacy prefix; empty is the zero value.
	CursorMode   string            `json:"cursor_mode,omitempty"`   // New cursor mode; legacy readers ignore it.
	CursorPrefix *string           `json:"cursor_prefix,omitempty"` // Actual prefix for empty-cursor formats.
}

type continuationTokenFields struct {
	NodeCursors  json.RawMessage `json:"node_cursors"`
	Prefix       string          `json:"prefix"`
	CursorMode   string          `json:"cursor_mode"`
	CursorPrefix *string         `json:"cursor_prefix"`
}

// The empty node ID is reserved for partial-page tokens. Coordinator node IDs
// are non-empty, so older readers ignore this marker and keep using the real
// per-node cursors in the same envelope.
const clusterGlobalCursorNodeID = ""

const (
	globalCursorMode      = "global-v1"
	globalEmptyCursorMode = "global-empty-v1"
	peerEmptyCursorMode   = "peers-empty-v1"
)

// legacyRejectingPrefix makes a base decoder fail its prefix check instead of
// silently treating an empty cursor as a fresh scan.
func legacyRejectingPrefix(prefix string) string {
	return prefix + "\x00"
}

type listCursorState struct {
	startKey    string
	hasStartKey bool
	nodeCursors map[string]string
	peerCursors bool
	// knownProgress marks a cursor map emitted by the current format; unmarked
	// raw and legacy cursors need active-node seeding before a new token is built.
	knownProgress bool
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
	NodeID                      string
	Keys                        []string       // populated in keys-only mode
	Entries                     []*pb.KeyValue // populated in withValues mode
	ContinuationToken           string
	HasMore                     bool
	supportsEmptyStartKeyCursor bool
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
	cursor := listCursorState{}
	if startKey, hasStartKey := listinput.StartKey(ctx); hasStartKey {
		cursor.startKey = startKey
		cursor.hasStartKey = true
	} else {
		var err error
		cursor, err = o.parseContinuationToken(continuationToken, prefix)
		if err != nil {
			return nil, "", false, fmt.Errorf("invalid continuation token: %w", err)
		}
	}

	// Unmarked legacy tokens and raw start keys do not carry enough provenance
	// to preserve per-node state. Seed each active peer at the global start so a
	// token emitted after this page remains readable by older coordinators.
	previousCursors := cursor.nodeCursors
	if !cursor.peerCursors && cursor.hasStartKey && !cursor.knownProgress {
		previousCursors = make(map[string]string, len(nodes))
		for _, node := range nodes {
			previousCursors[node.ID] = cursor.startKey
		}
	}

	// Complete pages resume from one global cursor. Partial pages retain the
	// successful peers' cursors so a recovered peer is not skipped.
	var peerCursors map[string]string
	if cursor.peerCursors {
		peerCursors = previousCursors
	}

	zlog.Debug().
		Str("prefix", prefix).
		Int("node_count", len(nodes)).
		Int("limit", limit).
		Bool("with_values", withValues).
		Str("start_key", cursor.startKey).
		Bool("has_start_key", cursor.hasStartKey).
		Bool("has_peer_cursors", cursor.peerCursors).
		Msg("Starting cluster-wide List with K-way merge")

	// Fetch from all nodes
	nodeResponses, err := o.fetchFromAllNodesWithCursorPresence(ctx, nodes, prefix, limit, cursor.startKey, cursor.hasStartKey, peerCursors, withValues)
	if err != nil {
		return nil, "", false, err
	}

	if len(nodeResponses) == 0 {
		return &mergeResult{Keys: []string{}}, "", false, nil
	}

	allNodesSupportEmptyCursor := allActiveNodesSupportEmptyCursor(nodes, nodeResponses)
	emptyCursorCapabilityChecked := false
	// A legacy local handler treats an empty StartKey as a fresh scan. Check
	// the currently healthy ring view, including LEAVING nodes. The separate
	// operator option below gates issuance because this view cannot inventory
	// every endpoint that may receive a public continuation token.
	needsEmptyCursor := (cursor.hasStartKey && cursor.startKey == "") ||
		(cursor.peerCursors && hasEmptyNodeCursor(previousCursors))
	if needsEmptyCursor {
		allNodesSupportEmptyCursor, err = o.allHealthyRingNodesSupportEmptyCursor(ctx, ringMgr.GetAllNodes(), nodes, nodeResponses, prefix)
		if err != nil {
			return nil, "", false, err
		}
		emptyCursorCapabilityChecked = true
		if !allNodesSupportEmptyCursor {
			return nil, "", false, status.Error(codes.FailedPrecondition, "cluster pagination after an empty key requires every currently healthy ring member to support explicit empty cursors")
		}
	}

	// Partial fan-outs keep the responding peers' progress in peer-cursor tokens.
	partialFanout := len(nodeResponses) < len(nodes)
	merged, pageCursors, hasMore, err := o.kWayMerge(ctx, nodeResponses, limit, withValues, previousCursors)
	if err != nil {
		return nil, "", false, err
	}

	var newToken string
	if hasMore {
		peerPage := partialFanout
		if !peerPage && cursor.peerCursors {
			peerPage = !canResumeGlobally(nodeResponses, previousCursors, merged.Keys)
		}
		if !peerPage && len(merged.Keys) > 0 {
			frontier := merged.Keys[len(merged.Keys)-1]
			if frontier != "" {
				advanceEmptyNodeCursors(pageCursors, frontier)
			}
		}
		if hasEmptyNodeCursor(pageCursors) {
			if !o.coordinator.AllowsEmptyKeyListPagination() {
				return nil, "", false, status.Error(codes.FailedPrecondition, "cluster pagination after an empty key is disabled; enable the cluster empty-key pagination option only after every endpoint that may receive continuation tokens supports explicit empty cursors")
			}
			if !emptyCursorCapabilityChecked {
				allNodesSupportEmptyCursor, err = o.allHealthyRingNodesSupportEmptyCursor(ctx, ringMgr.GetAllNodes(), nodes, nodeResponses, prefix)
				if err != nil {
					return nil, "", false, err
				}
				emptyCursorCapabilityChecked = true
			}
			if !allNodesSupportEmptyCursor {
				return nil, "", false, status.Error(codes.FailedPrecondition, "cluster pagination after an empty key requires every currently healthy ring member to support explicit empty cursors")
			}
		}
		if peerPage {
			newToken, err = o.encodePeerContinuationToken(pageCursors, prefix, allNodesSupportEmptyCursor)
		} else {
			newToken, err = o.encodeContinuationToken(pageCursors, prefix, allNodesSupportEmptyCursor)
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
				resp.supportsEmptyStartKeyCursor = true
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
				var responseHeaders metadata.MD
				if withValues {
					pbResp, respErr := client.ListLocalWithValues(rpcCtx, req, grpc.Header(&responseHeaders))
					if respErr != nil {
						err = respErr
					} else {
						resp.Entries = pbResp.Entries
						resp.HasMore = pbResp.HasMore
					}
				} else {
					pbResp, respErr := client.ListLocal(rpcCtx, req, grpc.Header(&responseHeaders))
					if respErr != nil {
						err = respErr
					} else {
						resp.Keys = pbResp.Keys
						resp.HasMore = pbResp.HasMore
					}
				}
				resp.supportsEmptyStartKeyCursor = listinput.HasEmptyStartKeyCursorCapability(responseHeaders)
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

// allActiveNodesSupportEmptyCursor requires a positive response from every
// active shard; a missing response or header is not evidence of support.
func allActiveNodesSupportEmptyCursor(nodes []*ring.NodeInfo, responses map[string]*NodeResponse) bool {
	if len(responses) != len(nodes) {
		return false
	}
	for _, node := range nodes {
		response, ok := responses[node.ID]
		if !ok || !response.supportsEmptyStartKeyCursor {
			return false
		}
	}
	return true
}

// allHealthyRingNodesSupportEmptyCursor checks non-active members in the current
// healthy ring view. This view is not an inventory of public endpoints; the
// explicit operator option gates issuance for endpoints outside that view.
func (o *Operations) allHealthyRingNodesSupportEmptyCursor(
	ctx context.Context,
	healthyNodes, activeNodes []*ring.NodeInfo,
	responses map[string]*NodeResponse,
	prefix string,
) (bool, error) {
	if !allActiveNodesSupportEmptyCursor(activeNodes, responses) || len(healthyNodes) == 0 {
		return false, nil
	}

	activeNodeIDs := make(map[string]struct{}, len(activeNodes))
	healthyNodeIDs := make(map[string]struct{}, len(healthyNodes))
	for _, node := range healthyNodes {
		if node == nil || node.ID == "" {
			return false, nil
		}
		healthyNodeIDs[node.ID] = struct{}{}
	}
	for _, node := range activeNodes {
		if node == nil {
			return false, nil
		}
		activeNodeIDs[node.ID] = struct{}{}
		if _, found := healthyNodeIDs[node.ID]; !found {
			return false, nil
		}
	}

	localNodeID := o.GetLocalNodeID()
	router := o.GetRouter()
	unsupported := make(chan struct{}, 1)
	markUnsupported := func() {
		select {
		case unsupported <- struct{}{}:
		default:
		}
	}
	sem := make(chan struct{}, maxListNodeFanout)
	var wg sync.WaitGroup
	for _, node := range healthyNodes {
		if _, active := activeNodeIDs[node.ID]; active || node.ID == localNodeID {
			continue
		}
		if router == nil {
			return false, nil
		}
		wg.Add(1)
		go func(n *ring.NodeInfo) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			if ctx.Err() != nil {
				return
			}

			client, err := router.GetClientForNode(ctx, n.ID)
			if err != nil {
				markUnsupported()
				return
			}
			var headers metadata.MD
			_, err = client.ListLocal(ctx, &pb.ListRequest{Prefix: prefix, Limit: 1}, grpc.Header(&headers))
			if err != nil || !listinput.HasEmptyStartKeyCursorCapability(headers) {
				markUnsupported()
			}
		}(node)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return len(unsupported) == 0, nil
}

func hasEmptyNodeCursor(cursors map[string]string) bool {
	for _, cursor := range cursors {
		if cursor == "" {
			return true
		}
	}
	return false
}

func advanceEmptyNodeCursors(cursors map[string]string, frontier string) {
	for nodeID, cursor := range cursors {
		if nodeID != clusterGlobalCursorNodeID && cursor == "" {
			cursors[nodeID] = frontier
		}
	}
}

// kWayMerge performs K-way merge of responses from nodes using a min-heap.
// Works for both keys-only and withValues modes via NodeResponse.keyAt/itemCount.
func (o *Operations) kWayMerge(ctx context.Context, nodeResponses map[string]*NodeResponse, limit int, withValues bool, previousCursors map[string]string) (*mergeResult, map[string]string, bool, error) {
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
	// Keep the per-node cursors cumulative. New readers resume from their
	// maximum as a global frontier; older readers still need the individual
	// shard positions to avoid restarting a peer omitted from the latest page.
	nodeCursors := make(map[string]string, len(nodeResponses)+len(previousCursors))
	for nodeID, key := range previousCursors {
		nodeCursors[nodeID] = key
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
		// Record consumption even when this key is a duplicate across shards.
		nodeCursors[minNode.nodeID] = minNode.key

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

	if len(result.Keys) > 0 {
		lastEmittedKey := result.Keys[len(result.Keys)-1]
		// The local pages are sorted. Advance a peer through every key at or
		// below the global cutoff, including duplicate copies the merge did not
		// need to pop before filling the requested page.
		for nodeID, nodeResp := range nodeResponses {
			for i := 0; i < nodeResp.itemCount(); i++ {
				key := nodeResp.keyAt(i)
				if key > lastEmittedKey {
					break
				}
				nodeCursors[nodeID] = key
			}
		}
	}

	return result, nodeCursors, hasMore, nil
}

// parseContinuationToken preserves the original raw-start and per-node token
// forms while recognizing the new explicit global and peer modes.
func (o *Operations) parseContinuationToken(token string, prefix string) (listCursorState, error) {
	if token == "" {
		return listCursorState{}, nil
	}

	plainStartKey := func() (listCursorState, error) {
		return listCursorState{startKey: token, hasStartKey: true}, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		zlog.Debug().Str("token", token).Msg("Token is not base64, treating as plain start key")
		return plainStartKey()
	}

	var fields continuationTokenFields
	if err := json.Unmarshal(decoded, &fields); err != nil {
		zlog.Debug().Str("token", token).Msg("Token is not valid JSON, treating as plain start key")
		return plainStartKey()
	}
	if len(fields.NodeCursors) == 0 {
		zlog.Debug().Str("token", token).Msg("Token has no recognized cluster cursor, treating as plain start key")
		return plainStartKey()
	}

	var nodeCursors map[string]string
	if err := json.Unmarshal(fields.NodeCursors, &nodeCursors); err != nil || len(nodeCursors) == 0 {
		zlog.Debug().Str("token", token).Msg("Token has no recognized cluster cursor, treating as plain start key")
		return plainStartKey()
	}

	if fields.CursorMode != "" {
		switch fields.CursorMode {
		case globalCursorMode:
			if fields.Prefix != prefix {
				return listCursorState{}, fmt.Errorf("prefix mismatch: token has '%s', request has '%s'", fields.Prefix, prefix)
			}
			if _, hasReservedCursor := nodeCursors[clusterGlobalCursorNodeID]; hasReservedCursor {
				return listCursorState{}, fmt.Errorf("global token contains reserved node cursor")
			}
			lastKey, hasCursor := maxNodeCursor(nodeCursors)
			return listCursorState{
				startKey:      lastKey,
				hasStartKey:   hasCursor,
				nodeCursors:   nodeCursors,
				knownProgress: true,
			}, nil
		case globalEmptyCursorMode:
			if fields.CursorPrefix == nil || *fields.CursorPrefix != prefix || fields.Prefix != legacyRejectingPrefix(prefix) {
				return listCursorState{}, fmt.Errorf("empty-cursor token prefix mismatch")
			}
			if _, hasReservedCursor := nodeCursors[clusterGlobalCursorNodeID]; hasReservedCursor {
				return listCursorState{}, fmt.Errorf("global token contains reserved node cursor")
			}
			lastKey, hasCursor := maxNodeCursor(nodeCursors)
			return listCursorState{
				startKey:      lastKey,
				hasStartKey:   hasCursor,
				nodeCursors:   nodeCursors,
				knownProgress: true,
			}, nil
		case peerEmptyCursorMode:
			if fields.CursorPrefix == nil || *fields.CursorPrefix != prefix || fields.Prefix != legacyRejectingPrefix(prefix) {
				return listCursorState{}, fmt.Errorf("empty-peer token prefix mismatch")
			}
			peerPrefix, hasReservedCursor := nodeCursors[clusterGlobalCursorNodeID]
			if !hasReservedCursor || len(nodeCursors) <= 1 || peerPrefix != prefix {
				return listCursorState{}, fmt.Errorf("empty-peer token is missing valid per-node cursors")
			}
			delete(nodeCursors, clusterGlobalCursorNodeID)
			return listCursorState{nodeCursors: nodeCursors, peerCursors: true, knownProgress: true}, nil
		default:
			return listCursorState{}, fmt.Errorf("unsupported continuation cursor mode %q", fields.CursorMode)
		}
	}

	if peerPrefix, hasReservedCursor := nodeCursors[clusterGlobalCursorNodeID]; hasReservedCursor && len(nodeCursors) > 1 {
		// Current-format partial tokens did not set top-level Prefix. Accept
		// them during migration, but require the new marker and any present
		// top-level prefix to agree with the request.
		if fields.Prefix != "" && fields.Prefix != prefix {
			return listCursorState{}, fmt.Errorf("prefix mismatch: token has '%s', request has '%s'", fields.Prefix, prefix)
		}
		if peerPrefix != prefix {
			return listCursorState{}, fmt.Errorf("prefix mismatch: token has '%s', request has '%s'", peerPrefix, prefix)
		}
		delete(nodeCursors, clusterGlobalCursorNodeID)
		return listCursorState{nodeCursors: nodeCursors, peerCursors: true}, nil
	}

	if fields.Prefix != prefix {
		return listCursorState{}, fmt.Errorf("prefix mismatch: token has '%s', request has '%s'", fields.Prefix, prefix)
	}
	if lastKey, hasGlobalCursor := nodeCursors[clusterGlobalCursorNodeID]; hasGlobalCursor {
		// Earlier global tokens used a singleton empty-ID cursor. Decode it, then
		// seed real node IDs before writing the next token.
		return listCursorState{startKey: lastKey, hasStartKey: true}, nil
	}

	// Base-revision per-node tokens have no mode marker. Their greatest cursor
	// remains the exclusive global resume point, while the per-node map is used
	// to seed an old-reader-compatible token after this page.
	lastKey, hasCursor := maxNodeCursor(nodeCursors)
	return listCursorState{startKey: lastKey, hasStartKey: hasCursor, nodeCursors: nodeCursors}, nil
}

func maxNodeCursor(nodeCursors map[string]string) (string, bool) {
	var lastKey string
	hasCursor := false
	for _, cursor := range nodeCursors {
		if !hasCursor || cursor > lastKey {
			lastKey = cursor
			hasCursor = true
		}
	}
	return lastKey, hasCursor
}

// canResumeGlobally reports whether a successful page after peer-mode recovery
// has re-established a single frontier. Every earlier peer cursor must be at
// or before the cutoff, and each response must cover through it or be exhausted.
func canResumeGlobally(nodeResponses map[string]*NodeResponse, previousCursors map[string]string, keys []string) bool {
	if len(keys) == 0 {
		return false
	}
	lastEmittedKey := keys[len(keys)-1]
	if previousKey, hasPrevious := maxNodeCursor(previousCursors); hasPrevious && previousKey > lastEmittedKey {
		return false
	}
	for _, nodeResp := range nodeResponses {
		if nodeResp.HasMore && (nodeResp.itemCount() == 0 || nodeResp.keyAt(nodeResp.itemCount()-1) < lastEmittedKey) {
			return false
		}
	}
	return true
}

// encodeContinuationToken creates a global-resume token while retaining
// per-node cursors for older coordinators that read the legacy envelope.
func (o *Operations) encodeContinuationToken(nodeCursors map[string]string, prefix string, allNodesSupportEmptyCursor bool) (string, error) {
	if len(nodeCursors) == 0 {
		return "", nil
	}
	if _, hasReservedCursor := nodeCursors[clusterGlobalCursorNodeID]; hasReservedCursor {
		return "", fmt.Errorf("node ID cannot be empty")
	}
	ct := ContinuationToken{NodeCursors: nodeCursors, Prefix: prefix}
	if hasEmptyNodeCursor(nodeCursors) {
		if !allNodesSupportEmptyCursor {
			return "", status.Error(codes.FailedPrecondition, "cluster pagination after an empty key requires every currently healthy ring member to support explicit empty cursors")
		}
		cursorPrefix := prefix
		ct.Prefix = legacyRejectingPrefix(prefix)
		ct.CursorMode = globalEmptyCursorMode
		ct.CursorPrefix = &cursorPrefix
	} else if allNodesSupportEmptyCursor {
		ct.CursorMode = globalCursorMode
	}
	jsonBytes, err := json.Marshal(ct)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(jsonBytes), nil
}

func (o *Operations) encodePeerContinuationToken(peerCursors map[string]string, prefix string, allNodesSupportEmptyCursor bool) (string, error) {
	if len(peerCursors) == 0 {
		return "", nil
	}

	// Older readers ignore the empty ID but keep the real node cursors. A
	// nonempty top-level Prefix is required by their legacy token decoder.
	cursors := make(map[string]string, len(peerCursors)+1)
	cursors[clusterGlobalCursorNodeID] = prefix
	for nodeID, key := range peerCursors {
		if nodeID == clusterGlobalCursorNodeID {
			return "", fmt.Errorf("node ID cannot be empty")
		}
		cursors[nodeID] = key
	}
	ct := ContinuationToken{NodeCursors: cursors, Prefix: prefix}
	if hasEmptyNodeCursor(peerCursors) {
		if !allNodesSupportEmptyCursor {
			return "", status.Error(codes.FailedPrecondition, "cluster pagination after an empty key requires every currently healthy ring member to support explicit empty cursors")
		}
		cursorPrefix := prefix
		ct.Prefix = legacyRejectingPrefix(prefix)
		ct.CursorMode = peerEmptyCursorMode
		ct.CursorPrefix = &cursorPrefix
	}
	jsonBytes, err := json.Marshal(ct)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(jsonBytes), nil
}
