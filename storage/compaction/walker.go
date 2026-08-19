// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"github.com/tigrisdata/ocache/storage/utils"

	"errors"
)

// Walk-gated recompaction (RFC-009): the per-segment delete index is an
// incremental counter, correct only if every mutation path credits it exactly
// once, forever — and history shows that invariant breaks (#218, #224, #225).
// Instead of trusting it, the recompaction loop derives each candidate
// segment's dead bytes from ground truth: walk the segment's own entry headers
// (a header-hop — no payload reads) and point-look-up each key's metadata row,
// the same liveness test recompactSegment applies before copying. The
// derivation gates recompaction directly; the delete index demotes to a
// prioritization hint that orders which segments get walked first. A wrong
// hint now costs at most one wasted walk (single-digit milliseconds: segments
// hold at most segmentSize/inlineThreshold entries) instead of leaked or
// pointlessly rewritten data.
//
// Safety inherits the recompaction age gate: only segments older than
// minSegmentAge are walked, which excludes segments whose metadata rows may
// still sit in an in-flight compaction run's uncommitted batch. Closed
// segments accept no new entries, so liveness only decreases during a walk;
// an entry deleted mid-walk still counts live for this pass, which errs
// conservative (recompaction postponed one rotation).

const (
	// DefaultSegmentWalkBudget is how many segments one recompaction pass may
	// walk. Rotation freshness = segments / budget × recompaction interval
	// (e.g. 3,000 segments at 8/min ≈ 6 h), while hint-priority ordering gets
	// hot-churn segments walked far sooner.
	DefaultSegmentWalkBudget = 8

	// DefaultSegmentWalkInterval is how long a walked segment's derivation is
	// trusted before it is walked again, bounding re-walks of segments whose
	// stale hint keeps ranking them first.
	DefaultSegmentWalkInterval = 1 * time.Hour
)

// walkSegmentLiveness derives a closed segment's dead entries and bytes from
// ground truth: footer totals minus the entries whose metadata rows still
// point at this segment. Any read or lookup failure aborts the derivation —
// deriving from partial knowledge would count unseen live entries dead.
func (sr *SegmentRecompactor) walkSegmentLiveness(ctx context.Context, seg *segment.Segment) (deadEntries, deadBytes int64, err error) {
	file, err := os.Open(seg.Path())
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	iter, err := seg.NewIterator(file)
	if err != nil {
		return 0, 0, err
	}

	var walked, liveEntries, liveBytes int64
	for {
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		entry, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			// A read error can hide any number of entries beyond it.
			return 0, 0, err
		}
		walked++

		meta, err := utils.GetMetadata(sr.meta, string(keys.MakeMetadataKey(entry.Key)))
		if err != nil {
			if errors.Is(err, utils.ErrMetadataNotFound) {
				continue // row absent: dead
			}
			// A transient lookup failure is NOT evidence of death; deriving
			// from it would over-count dead bytes and could trigger a
			// pointless rewrite of a healthy segment.
			return 0, 0, err
		}
		if meta.ValueType != pb.ValueType_SEGMENT ||
			meta.SegmentPath != seg.Path() ||
			meta.SegmentOffset != entry.Offset {
			continue // overwritten or moved: dead
		}
		liveEntries++
		liveBytes += entry.ValueLength
	}

	entries, dataBytes := int64(seg.GetNumEntries()), seg.GetDataBytes()
	if entries <= 0 || dataBytes <= 0 {
		return 0, 0, nil // no footer totals to derive from
	}
	// The iterator stops at the file's current size, so a truncated segment
	// reads as a clean early EOF — not an error. Cross-check against the
	// footer: fewer walked entries than were finalized means file damage, and
	// deriving from the partial walk would mark the unread entries dead.
	if walked != entries {
		return 0, 0, fmt.Errorf("segment %s walk saw %d entries, footer says %d — file truncated or damaged", seg.Path(), walked, entries)
	}

	deadEntries, deadBytes = entries-liveEntries, dataBytes-liveBytes
	if deadEntries < 0 {
		deadEntries = 0
	}
	if deadBytes < 0 {
		deadBytes = 0
	}
	return deadEntries, deadBytes, nil
}

// pruneWalkStates drops walk recency for segments that no longer exist, so the
// map stays O(live segments).
func (sr *SegmentRecompactor) pruneWalkStates() {
	known := make(map[string]struct{})
	for _, seg := range sr.sm.GetSegments() {
		if seg != nil {
			known[seg.Path()] = struct{}{}
		}
	}
	for path := range sr.walkStates {
		if _, ok := known[path]; !ok {
			delete(sr.walkStates, path)
		}
	}
}
