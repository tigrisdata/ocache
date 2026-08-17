// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"os"

	"github.com/tigrisdata/ocache/storage/segment"
)

type cacheRange struct {
	offset int64
	length int64
}

// cacheAdvice records the ranges written during one compaction batch. Callers
// must only drop a range after the segment containing it has been synced.
type cacheAdvice struct {
	ranges map[*segment.Segment]cacheRange
	drop   func(path string, offset, length int64)
}

func newCacheAdvice() *cacheAdvice {
	return &cacheAdvice{
		ranges: make(map[*segment.Segment]cacheRange),
		drop:   dropFileCacheByPath,
	}
}

func (a *cacheAdvice) addOutput(seg *segment.Segment, offset, length int64) {
	if a == nil || seg == nil || offset < 0 || length <= 0 {
		return
	}

	end := offset + length
	if end < offset {
		return
	}
	if a.ranges == nil {
		a.ranges = make(map[*segment.Segment]cacheRange)
	}

	existing, ok := a.ranges[seg]
	if !ok {
		a.ranges[seg] = cacheRange{offset: offset, length: length}
		return
	}

	existingEnd := existing.offset + existing.length
	if existingEnd < existing.offset {
		existingEnd = existing.offset
	}
	if offset < existing.offset {
		existing.offset = offset
	}
	if end > existingEnd {
		existingEnd = end
	}
	existing.length = existingEnd - existing.offset
	a.ranges[seg] = existing
}

func (a *cacheAdvice) dropSyncedOutput(seg *segment.Segment) {
	if a == nil || seg == nil {
		return
	}

	output, ok := a.ranges[seg]
	if !ok {
		return
	}
	delete(a.ranges, seg)

	if a.drop != nil {
		a.drop(seg.Path(), output.offset, output.length)
	}
}

func dropFileCacheByPath(path string, offset, length int64) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	dropFileCache(file, offset, length)
}
