// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/ocache/storage/segment"
)

func TestCompactionCacheAdviceDropsMergedOutputRange(t *testing.T) {
	seg := segment.NewSegment("/segment", 0, 0, 0, 1024)
	var calls []cacheRange
	advice := &cacheAdvice{
		ranges: make(map[*segment.Segment]cacheRange),
		drop: func(_ string, offset, length int64) {
			calls = append(calls, cacheRange{offset: offset, length: length})
		},
	}

	advice.addOutput(seg, 128, 64)
	advice.addOutput(seg, 64, 96)
	advice.dropSyncedOutput(seg)
	advice.dropSyncedOutput(seg)

	assert.Equal(t, []cacheRange{{offset: 64, length: 128}}, calls)
}
