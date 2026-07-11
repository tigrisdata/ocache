// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package hash

// Ring configuration constants that must be consistent between client and server
const (
	// DefaultPartitionCount is the default number of partitions in the hash ring
	DefaultPartitionCount = 16384

	// DefaultReplicationFactor is the default number of virtual nodes per physical node
	DefaultReplicationFactor = 100

	// DefaultLoad is the default load factor for bounded loads
	DefaultLoad = 1.25
)
