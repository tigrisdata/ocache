// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"hash/maphash"
	"sync/atomic"

	"github.com/tigrisdata/ocache/common/metrics"
)

// DefaultMissReasonEntries is the default slot count of the miss-reason table
// (8 bytes each: 32 MiB).
const DefaultMissReasonEntries = 1 << 22

// missReason is why a key that was once stored is no longer present. Codes fit
// in the low 3 bits of a table word; 0 is reserved for an empty slot.
type missReason uint64

const (
	missReasonCold missReason = iota // never stored, or forgotten by the table
	missReasonEvicted
	missReasonExpired
	missReasonDeleted
)

const missReasonMask = 7

// label returns the bounded metric label for r.
func (r missReason) label() string {
	switch r {
	case missReasonEvicted:
		return "evicted"
	case missReasonExpired:
		return "expired"
	case missReasonDeleted:
		return "deleted"
	default:
		return "cold"
	}
}

var missReasonSeed = maphash.MakeSeed()

// missReasonTable remembers, per key, why it last left the store. It is a
// fixed-size direct-mapped array of atomics: slot = hash & (len-1), word =
// (hash with the low 3 bits cleared) | reason. A colliding key overwrites the
// slot and a forgotten key reads as cold, so cold is an upper bound. Lock-free;
// a nil table records nothing and reads every key as cold.
type missReasonTable struct {
	slots []atomic.Uint64
	mask  uint64
}

// newMissReasonTable sizes the table to n rounded up to a power of two.
func newMissReasonTable(n int) *missReasonTable {
	size := 1
	for size < n {
		size <<= 1
	}
	return &missReasonTable{slots: make([]atomic.Uint64, size), mask: uint64(size - 1)}
}

func (t *missReasonTable) record(key string, r missReason) {
	if t == nil {
		return
	}
	h := maphash.String(missReasonSeed, key)
	t.slots[h&t.mask].Store(h&^missReasonMask | uint64(r))
}

func (t *missReasonTable) lookup(key string) missReason {
	if t == nil {
		return missReasonCold
	}
	h := maphash.String(missReasonSeed, key)
	w := t.slots[h&t.mask].Load()
	if w != 0 && w&^missReasonMask == h&^missReasonMask {
		return missReason(w & missReasonMask)
	}
	return missReasonCold
}

// clear empties key's slot if it still holds key, so a stored key is not later
// misattributed.
func (t *missReasonTable) clear(key string) {
	if t == nil {
		return
	}
	h := maphash.String(missReasonSeed, key)
	slot := &t.slots[h&t.mask]
	if w := slot.Load(); w != 0 && w&^missReasonMask == h&^missReasonMask {
		slot.CompareAndSwap(w, 0)
	}
}

// missKeyType returns the key's prefix before the first '|' when it is 1-8
// lowercase ASCII letters, else "other", keeping the label set bounded.
func missKeyType(key string) string {
	for i := 0; i < len(key) && i <= 8; i++ {
		c := key[i]
		if c == '|' {
			if i == 0 {
				return "other"
			}
			return key[:i]
		}
		if c < 'a' || c > 'z' {
			return "other"
		}
	}
	return "other"
}

// recordGetMiss counts a not-found Get by reason and key type.
func recordGetMiss(key string, r missReason) {
	metrics.StorageGetMisses.WithLabelValues(r.label(), missKeyType(key)).Inc()
}
