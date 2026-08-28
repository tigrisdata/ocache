// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package embedded

import "github.com/tigrisdata/ocache/coordinator"

// Client provides embedded cache access with cluster routing.
// It implements the cacheclient.CacheClient interface.
type Client struct {
	coordinator *coordinator.Coordinator
	resources   *clientResources
}

// Coordinator returns the underlying coordinator.
// Returns nil if clustering is not enabled.
func (c *Client) Coordinator() *coordinator.Coordinator {
	return c.coordinator
}

// IsLocal reports whether key is owned by this node — i.e. a read for it is
// served from local storage rather than routed to a peer over gRPC. In
// single-node (non-cluster) mode every key is local. Callers use this to
// observe cross-node serve ratios without reaching into the coordinator.
func (c *Client) IsLocal(key string) bool {
	if c.coordinator == nil {
		return true
	}
	return c.coordinator.IsLocal(key)
}
