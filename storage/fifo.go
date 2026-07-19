// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import "github.com/tigrisdata/ocache/storage/keys"

// fifoEvictionIndex describes the FIFO write-order index (!fifo/<write_nano>/<key>,
// which sorts oldest-written first) for evictByIndex. FIFO evicts strictly by
// write time, so a read never protects a key. The per-key back-reference
// (!fifo_ref/<key>) lets eviction skip superseded duplicate entries left by a
// concurrent overwrite without taking a per-key lock — see evictByIndex.
func fifoEvictionIndex() evictionIndex {
	return evictionIndex{
		policy:     EvictionPolicyFIFO,
		prefix:     keys.GetFifoIndexPrefix(),
		parseKey:   keys.ParseFifoIndexKey,
		backrefKey: keys.MakeFifoBackrefKey,
	}
}
