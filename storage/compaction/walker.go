// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
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

// DefaultSegmentWalkInterval is how long a walked segment's derivation is
// trusted before it is walked again (hint growth bypasses it). Walk volume
// needs no count cap: reads are paced by the shared compaction I/O limiter.
const DefaultSegmentWalkInterval = 1 * time.Hour

// walkSegmentLiveness chooses the durable offset index only after its marker
// proves complete for this exact segment footer. Legacy and damaged segments
// retain the source-header walk fallback.
func (sr *SegmentRecompactor) walkSegmentLiveness(ctx context.Context, seg *segment.Segment) (deadEntries, deadBytes int64, err error) {
	covered, err := segmentLiveIndexCovered(sr.meta, seg)
	if err != nil {
		return 0, 0, err
	}
	if covered {
		return sr.walkIndexedSegmentLiveness(ctx, seg)
	}
	return sr.walkSegmentLivenessByScan(ctx, seg)
}

// walkSegmentLivenessByScan derives a closed segment's dead entries and bytes
// from source headers. It remains the safe path for legacy or unproven indexes.
func (sr *SegmentRecompactor) walkSegmentLivenessByScan(ctx context.Context, seg *segment.Segment) (deadEntries, deadBytes int64, err error) {
	file, err := os.Open(seg.Path())
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	iter, err := seg.NewIterator(file)
	if err != nil {
		return 0, 0, err
	}

	// Each entry visit touches roughly one 4 KiB page of the segment file
	// (the header and key share it). Draw that from the shared compaction I/O
	// budget so walks and payload copies together stay under the configured
	// ceiling, and so a burst of small walk reads is paced instead of landing
	// as an IOPS spike. Metadata point lookups are not charged: they are
	// almost always block-cache hits and have no meaningful byte cost to
	// account.
	walkPageCost := int64(4096)
	if sr.rateLimiter != nil {
		if burst := int64(sr.rateLimiter.Burst()); burst > 0 && walkPageCost > burst {
			walkPageCost = burst
		}
	}

	var walked, liveEntries, liveBytes int64
	for {
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		if sr.rateLimiter != nil {
			if err := sr.rateLimiter.WaitN(ctx, int(walkPageCost)); err != nil {
				return 0, 0, err
			}
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

// walkIndexedSegmentLiveness derives liveness from covered rows. It performs
// metadata validation for every indexed candidate, but never opens the source
// file or parses a historical header/key. Stale speculative rows are pruned;
// malformed facts invalidate the marker and return the safe scan error path.
func (sr *SegmentRecompactor) walkIndexedSegmentLiveness(ctx context.Context, seg *segment.Segment) (deadEntries, deadBytes int64, err error) {
	entries, dataBytes := int64(seg.GetNumEntries()), seg.GetDataBytes()
	if entries <= 0 || dataBytes <= 0 {
		return 0, 0, nil
	}
	dataSize, _, err := segmentDataRegionSize(seg)
	if err != nil {
		return 0, 0, err
	}

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	var indexRows, liveEntries, liveBytes int64
	walkErr := indexedSegmentRows(ctx, sr.meta, seg.Path(), func(indexKey, indexValue []byte) error {
		indexRows++
		entry, err := indexedSegmentEntry(indexKey, indexValue, seg, dataSize)
		if err != nil {
			return err
		}
		_, live, err := validateIndexedMetadata(sr.meta, entry, indexValue, seg.Path())
		if err != nil {
			return err
		}
		if !live {
			batch.Delete(indexKey)
			return nil
		}
		liveEntries++
		liveBytes += entry.ValueLength
		return nil
	})

	commitRepairs := func(invalidate bool) error {
		if invalidate {
			batch.Delete(keys.MakeSegmentLiveCoverageKey(seg.Path()))
		}
		if batch.Count() == 0 {
			return nil
		}
		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		return sr.meta.Handle().Write(wo, batch)
	}
	if walkErr != nil {
		// Cancellation is not evidence that the index is bad. Preserve the
		// marker and all rows so a later pass can resume normally.
		if ctx.Err() != nil {
			return 0, 0, walkErr
		}
		if repairErr := commitRepairs(true); repairErr != nil {
			return 0, 0, repairErr
		}
		return 0, 0, walkErr
	}
	if indexRows > entries || liveEntries > entries || liveBytes > dataBytes {
		if repairErr := commitRepairs(true); repairErr != nil {
			return 0, 0, repairErr
		}
		return 0, 0, fmt.Errorf("segment %s live index exceeds footer totals", seg.Path())
	}
	if err := commitRepairs(false); err != nil {
		return 0, 0, err
	}
	return entries - liveEntries, dataBytes - liveBytes, nil
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
