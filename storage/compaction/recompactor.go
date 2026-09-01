// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/benchio"
	"github.com/tigrisdata/ocache/storage/deletion"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"github.com/tigrisdata/ocache/storage/utils"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	zlog "github.com/rs/zerolog/log"
)

const (
	// recompactorCallerIDPrefix is the prefix for the caller ID for the recompactor.
	recompactorCallerIDPrefix = "recompactor-"
)

// SegmentRecompactor handles recompaction of fragmented segments
type SegmentRecompactor struct {
	sm            *segment.Manager
	meta          *metadata.MetaDB
	deletionQueue *deletion.Queue
	fragThreshold float64
	minSegmentAge time.Duration
	minSegments   int
	rateLimiter   *rate.Limiter

	// Walk-gated recompaction (RFC-009): per-segment re-walk interval and walk
	// recency. See walker.go. Walk volume needs no count cap: the shared I/O
	// limiter paces the reads and walkInterval bounds re-derivation.
	walkInterval time.Duration
	walkStates   map[string]walkRecord
}

// walkRecord remembers when a segment was last derived and the delete-index
// hint observed at that moment. Hint GROWTH since the last walk invalidates
// the derivation immediately — new deaths were credited — so credited
// deletions trigger re-derivation on the very next pass, while the interval
// only bounds re-walks for segments whose hint is unchanged (covering drift
// the credits missed).
type walkRecord struct {
	at        time.Time
	hintBytes int64
}

// NewSegmentRecompactor creates a new segment recompactor without a rate limit.
// NewCompactorWithConfig uses the internal constructor to share its limiter
// with file compaction workers.
func NewSegmentRecompactor(meta *metadata.MetaDB, sm *segment.Manager, deletionQueue *deletion.Queue, fragThreshold float64, minSegmentAge time.Duration, minSegments int) *SegmentRecompactor {
	return newSegmentRecompactor(meta, sm, deletionQueue, fragThreshold, minSegmentAge, minSegments, nil)
}

func newSegmentRecompactor(meta *metadata.MetaDB, sm *segment.Manager, deletionQueue *deletion.Queue, fragThreshold float64, minSegmentAge time.Duration, minSegments int, rateLimiter *rate.Limiter) *SegmentRecompactor {
	return &SegmentRecompactor{
		sm:            sm,
		meta:          meta,
		deletionQueue: deletionQueue,
		fragThreshold: fragThreshold,
		minSegmentAge: minSegmentAge,
		minSegments:   minSegments,
		rateLimiter:   rateLimiter,
		walkInterval:  DefaultSegmentWalkInterval,
		walkStates:    make(map[string]walkRecord),
	}
}

// RecompactFragmentedSegments identifies and recompacts fragmented segments
func (sr *SegmentRecompactor) RecompactFragmentedSegments(ctx context.Context) error {
	zlog.Info().
		Float64("threshold", sr.fragThreshold).
		Msg("recompactor: starting segment recompaction scan")

	// Increment recompaction runs counter
	metrics.RecompactionRuns.Inc()
	startTime := time.Now()
	defer func() {
		// Record recompaction duration in milliseconds
		metrics.RecompactionDuration.Observe(float64(time.Since(startTime).Milliseconds()))
	}()

	// Candidate selection (RFC-009): every eligible segment not derived within
	// the walk interval (hint growth bypasses it), ordered by delete-index hint
	// (bytes credited, descending) then walk staleness, so reclaim-worthy
	// segments are walked and recompacted first. No count cap is needed: walk
	// reads draw from the shared compaction I/O limiter, so even a whole-fleet
	// pass (first pass after restart) is paced rather than bursty.
	segments := sr.sm.GetSegments()
	totalSegments := len(segments)

	if totalSegments == 0 {
		return nil
	}

	// Safety check: Need enough segments to safely recompact
	if totalSegments < sr.minSegments {
		zlog.Debug().Int("segmentCount", totalSegments).Int("minRequired", sr.minSegments).
			Msg("recompactor: too few segments to safely recompact")
		return nil
	}
	// Get the current open segment to ensure we never try to recompact it
	openSegments := sr.sm.GetOpenSegments()
	now := time.Now()

	type candidate struct {
		seg        *segment.Segment
		hintBytes  int64
		lastWalked time.Time
	}
	var candidates []candidate
	for i, seg := range segments {
		eligible, reason := sr.isSegmentEligibleForRecompaction(seg, openSegments, i, totalSegments)
		if !eligible {
			zlog.Debug().
				Str("segment", seg.Path()).
				Str("reason", reason).
				Msg("recompactor: skipping segment")
			continue
		}
		// The delete index is a prioritization HINT only — the walk below is
		// the decision. A hint read failure therefore just loses priority.
		_, hintBytes, err := sr.getDeleteIndexStats(seg.Path())
		if err != nil {
			zlog.Error().Err(err).Str("segment", seg.Path()).
				Msg("recompactor: failed to read delete-index hint")
			hintBytes = 0
		}
		if rec, ok := sr.walkStates[seg.Path()]; ok &&
			now.Sub(rec.at) < sr.walkInterval && hintBytes <= rec.hintBytes {
			// Recently derived and nothing new credited since: trust the
			// derivation until the interval elapses. Hint growth bypasses the
			// interval so credited deletions are re-derived next pass.
			continue
		}
		candidates = append(candidates, candidate{seg: seg, hintBytes: hintBytes, lastWalked: sr.walkStates[seg.Path()].at})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].hintBytes != candidates[j].hintBytes {
			return candidates[i].hintBytes > candidates[j].hintBytes
		}
		return candidates[i].lastWalked.Before(candidates[j].lastWalked)
	})
	recompactedCount := 0
	for _, c := range candidates {
		if ctx.Err() != nil {
			zlog.Info().Msg("recompactor: interrupted by cancellation")
			return ctx.Err()
		}

		deadEntries, deadBytes, err := sr.walkSegmentLiveness(ctx, c.seg)
		// Record the attempt even on failure so a damaged segment is retried
		// once per interval instead of hot-looping every pass. Stamp the walk's
		// COMPLETION time, not the pass start: without a count cap a pass can
		// run long (rate-limited whole-fleet walks), and pass-start stamps
		// would expire retained derivations prematurely, re-walking the fleet
		// every pass.
		sr.walkStates[c.seg.Path()] = walkRecord{at: time.Now(), hintBytes: c.hintBytes}
		metrics.SegmentWalks.Inc()
		if err != nil {
			zlog.Error().Err(err).Str("segment", c.seg.Path()).
				Msg("recompactor: segment walk aborted; leaving segment untouched")
			continue
		}
		if deadBytes <= 0 {
			continue
		}
		size := c.seg.GetSize()
		if size <= 0 || float64(deadBytes)/float64(size) < sr.fragThreshold {
			continue
		}

		zlog.Info().
			Str("segment", c.seg.Path()).
			Int64("derivedDeadEntries", deadEntries).
			Int64("derivedDeadBytes", deadBytes).
			Float64("fragmentation", float64(deadBytes)/float64(size)).
			Int64("hintBytes", c.hintBytes).
			Msg("recompactor: walk derived fragmented segment")

		if err := sr.recompactSegment(ctx, c.seg); err != nil {
			zlog.Error().Err(err).Str("segment", c.seg.Path()).
				Msg("recompactor: failed to recompact segment")
			// A derived-fragmented segment whose rewrite failed must be
			// retried promptly — reclaim matters most under the disk pressure
			// that makes rewrites fail — not parked for the walk interval.
			delete(sr.walkStates, c.seg.Path())
			continue
		}
		delete(sr.walkStates, c.seg.Path())

		// Increment segments counter
		metrics.RecompactionSegments.Inc()
		recompactedCount++
	}

	sr.pruneWalkStates()

	if recompactedCount > 0 {
		zlog.Info().Int("count", recompactedCount).
			Msg("recompactor: finished segment recompaction")
	}

	return nil
}

// recompactSegment copies live data from a fragmented segment to a new segment
func (sr *SegmentRecompactor) recompactSegment(ctx context.Context, oldSeg *segment.Segment) error {
	zlog.Info().Str("segment", oldSeg.Path()).Msg("recompactor: starting segment recompaction")

	covered, err := segmentLiveIndexCovered(sr.meta, oldSeg)
	if err != nil {
		return fmt.Errorf("failed to inspect segment live index coverage: %w", err)
	}

	// The source file is still needed for payload reads, but a covered index
	// avoids constructing a source iterator and avoids parsing dead headers and
	// keys. Legacy/uncovered segments use the complete historical walk.
	oldFile, err := os.Open(oldSeg.Path())
	if err != nil {
		return fmt.Errorf("failed to open segment %s: %w", oldSeg.Path(), err)
	}
	defer oldFile.Close()

	callerID := fmt.Sprintf("%s%s", recompactorCallerIDPrefix, oldSeg.Path())
	// Do not reserve or create a destination until the first validated live row
	// is found. A fully dead segment can then be removed without manufacturing
	// an empty destination segment.
	var newSeg *segment.Segment
	defer func() {
		if newSeg != nil {
			if err := newSeg.Release(callerID); err != nil {
				zlog.Error().Err(err).Str("callerID", callerID).Msg("failed to release segment")
			}
		}
	}()

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()
	advice := newCacheAdvice()
	var iter *segment.Iterator
	if !covered {
		iter, err = oldSeg.NewIterator(oldFile)
		if err != nil {
			return fmt.Errorf("failed to create segment iterator: %w", err)
		}
	}

	// A destination segment can be finalized by copyEntry when it fills before
	// the metadata batch is published. Mark those segments only after this batch
	// commits, so coverage never gets ahead of metadata.
	var finalizedSegments []*segment.Segment
	copiedEntries := uint32(0)
	copiedBytes := int64(0)
	failedEntries := 0

	copyLive := func(entry *segment.EntryInfo, indexKey, indexValue []byte, indexed bool) error {
		var meta *pb.ValueMessage
		if indexed {
			var live bool
			var err error
			meta, live, err = validateIndexedMetadata(sr.meta, entry, indexValue, oldSeg.Path())
			if err != nil {
				return err
			}
			if !live {
				// Rows emitted beside a conditional migration are speculative.
				// Once metadata no longer points at this source location, remove
				// the row without treating the source segment as incomplete.
				wb.Delete(indexKey)
				return nil
			}
		} else {
			var err error
			meta, err = utils.GetMetadata(sr.meta, string(keys.MakeMetadataKey(entry.Key)))
			if err != nil {
				if errors.Is(err, utils.ErrMetadataNotFound) {
					return nil
				}
				return err
			}
			if meta.ValueType != pb.ValueType_SEGMENT || meta.SegmentPath != oldSeg.Path() || meta.SegmentOffset != entry.Offset {
				return nil
			}
		}

		if newSeg == nil {
			var err error
			newSeg, err = sr.sm.AcquireOpenSegmentWithReservation(callerID, 0)
			if err != nil {
				return fmt.Errorf("failed to acquire new segment: %w", err)
			}
		}
		if err := sr.copyEntry(ctx, oldFile, &newSeg, callerID, entry, meta, wb, advice, &finalizedSegments); err != nil {
			failedEntries++
			zlog.Error().Err(err).Str("key", entry.Key).Msg("recompactor: failed to copy entry")
			return nil
		}
		copiedEntries++
		copiedBytes += entry.ValueLength
		metrics.RecompactionEntriesCopied.Inc()
		metrics.RecompactionBytesCopied.Add(float64(entry.ValueLength))
		return nil
	}

	if covered {
		dataSize, _, err := segmentDataRegionSize(oldSeg)
		if err != nil {
			return err
		}
		indexErr := indexedSegmentRows(ctx, sr.meta, oldSeg.Path(), func(indexKey, indexValue []byte) error {
			entry, err := indexedSegmentEntry(indexKey, indexValue, oldSeg, dataSize)
			if err != nil {
				return err
			}
			return copyLive(entry, indexKey, indexValue, true)
		})
		if indexErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// A malformed row or lookup failure means this pass cannot prove
			// that every live row was considered. Keep the source and force the
			// next pass back through the historical scan.
			failedEntries++
			wb.Delete(keys.MakeSegmentLiveCoverageKey(oldSeg.Path()))
			zlog.Error().Err(indexErr).Str("segment", oldSeg.Path()).Msg("recompactor: indexed walk incomplete")
		}
	} else {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			entry, err := iter.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				failedEntries++
				zlog.Error().Err(err).Int64("offset", iter.CurrentPosition()).Msg("recompactor: failed to read entry")
				break
			}
			if err := copyLive(entry, nil, nil, false); err != nil {
				failedEntries++
				zlog.Error().Err(err).Str("key", entry.Key).Msg("recompactor: metadata lookup failed; keeping old segment")
			}
		}
	}

	if copiedEntries == 0 {
		zlog.Info().Str("segment", oldSeg.Path()).Msg("recompactor: no live entries found")
	}

	// Persist the final open destination before publishing metadata. A batch
	// containing only stale-index repairs does not require syncing an empty
	// destination segment.
	if copiedEntries > 0 {
		if err := sr.commitRecompactionBatch(ctx, newSeg, wb, advice); err != nil {
			newPath := ""
			if newSeg != nil {
				newPath = newSeg.Path()
			}
			zlog.Error().Err(err).Str("oldSegment", oldSeg.Path()).Str("newSegment", newPath).Msg("recompactor: failed to commit metadata")
			return err
		}
	} else if wb.Count() > 0 {
		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		if err := sr.meta.Handle().Write(wo, wb); err != nil {
			zlog.Error().Err(err).Str("oldSegment", oldSeg.Path()).Msg("recompactor: failed to commit metadata")
			return fmt.Errorf("failed to commit metadata updates: %w", err)
		}
		wb.Clear()
	}
	for _, finalized := range finalizedSegments {
		if err := MarkSegmentLiveIndexComplete(sr.meta, finalized); err != nil {
			zlog.Warn().Err(err).Str("segment", finalized.Path()).Msg("recompactor: failed to mark destination live index coverage; retaining scan fallback")
		}
	}

	// An incomplete pass must not remove the old segment. Copied entries were
	// conditionally published and their source rows were removed atomically; the
	// remaining source rows keep the old segment reachable for a later retry.
	if failedEntries > 0 {
		zlog.Warn().Str("segment", oldSeg.Path()).Int("failed", failedEntries).Uint32("copied", copiedEntries).Msg("recompactor: incomplete pass; keeping old segment for a later retry")
		return nil
	}

	complete, verifyErr := sr.verifySegmentLiveIndexComplete(oldSeg.Path(), oldSeg)
	if verifyErr != nil {
		// A metadata read or physical-boundary error is not evidence that the
		// source is empty. Invalidate the marker so the next attempt uses the
		// complete source scan.
		wo := grocksdb.NewDefaultWriteOptions()
		deleteErr := sr.meta.Handle().Delete(wo, keys.MakeSegmentLiveCoverageKey(oldSeg.Path()))
		wo.Destroy()
		if deleteErr != nil {
			return deleteErr
		}
		return verifyErr
	}
	if !complete {
		zlog.Warn().Str("segment", oldSeg.Path()).Msg("recompactor: repaired incomplete live index; keeping source for a later retry")
		return nil
	}

	removedSeg := sr.sm.RemoveSegment(oldSeg.Path())
	if removedSeg == nil {
		zlog.Warn().Str("path", oldSeg.Path()).Msg("recompactor: segment already removed from manager")
	}
	if err := sr.removeSegmentLiveIndex(oldSeg.Path()); err != nil {
		zlog.Error().Err(err).Str("segment", oldSeg.Path()).Msg("recompactor: failed to remove segment live index")
	}

	if err := sr.deletionQueue.Add(oldSeg.Path()); err != nil {
		zlog.Error().Err(err).Str("path", oldSeg.Path()).Msg("recompactor: failed to queue old segment for deletion")
	}
	if err := sr.removeDeleteIndex(oldSeg.Path()); err != nil {
		zlog.Error().Err(err).Str("segment", oldSeg.Path()).Msg("recompactor: failed to remove delete index")
	}

	if oldSegInfo, err := os.Stat(oldSeg.Path()); err == nil {
		bytesFreed := oldSegInfo.Size() - copiedBytes
		if bytesFreed > 0 {
			metrics.RecompactionBytesFreed.Add(float64(bytesFreed))
		}
	}
	newPath := ""
	if newSeg != nil {
		newPath = newSeg.Path()
	}
	zlog.Info().Str("oldSegment", oldSeg.Path()).Str("newSegment", newPath).Uint32("copiedEntries", copiedEntries).Int64("copiedBytes", copiedBytes).Msg("recompactor: successfully recompacted segment")
	return nil
}

// commitRecompactionBatch syncs a destination before publishing the metadata
// and live-index rows that point at it. The batch is cleared only after the
// RocksDB write succeeds so a caller can retry a failed publication without
// losing staged migrations.
func (sr *SegmentRecompactor) commitRecompactionBatch(ctx context.Context, seg *segment.Segment, wb *grocksdb.WriteBatch, advice *cacheAdvice) error {
	if seg == nil || wb == nil {
		return fmt.Errorf("invalid recompaction batch arguments")
	}
	if wb.Count() == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := seg.Sync(); err != nil {
		return fmt.Errorf("failed to sync new segment: %w", err)
	}
	advice.dropSyncedOutput(seg)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	if err := sr.meta.Handle().Write(wo, wb); err != nil {
		return fmt.Errorf("failed to commit metadata updates: %w", err)
	}
	wb.Clear()
	return nil
}

// copyEntry copies a single entry from old segment to new segment
func (sr *SegmentRecompactor) copyEntry(ctx context.Context, oldFile *os.File, newSeg **segment.Segment, callerID string,
	entry *segment.EntryInfo, meta *pb.ValueMessage, wb *grocksdb.WriteBatch, advice *cacheAdvice, finalizedSegments *[]*segment.Segment,
) error {
	// Create a section reader for the value data (no checksum verification per review).
	// The wrapper charges the direct payload reads to the benchmark-only shared lane.
	valueOffset := entry.Offset + entry.HeaderSize
	payloadReader := benchio.WrapPayloadReaderAtForBenchmark(oldFile)
	dataReader := io.NewSectionReader(payloadReader, valueOffset, entry.ValueLength)

	// Check if we need a new segment
	// NOTE: FinalizeSegment and AcquireOpenSegmentWithReservation are thread-safe - the segment
	// manager uses internal locking and reservations to coordinate between compactor and recompactor
	totalNeeded := entry.HeaderSize + entry.ValueLength
	if (*newSeg).Remaining() < totalNeeded {
		// Publish the entries already written to this destination before
		// closing it. Once finalized, another recompaction pass may treat the
		// segment as eligible; exposing a closed segment whose metadata still
		// points at the source would let that pass reclaim the destination.
		if err := sr.commitRecompactionBatch(ctx, *newSeg, wb, advice); err != nil {
			return err
		}
		if err := sr.sm.FinalizeSegment(*newSeg); err != nil {
			return fmt.Errorf("failed to finalize segment: %w", err)
		}
		if finalizedSegments != nil {
			*finalizedSegments = append(*finalizedSegments, *newSeg)
		}

		// Now safe to release since it's finalized
		if err := (*newSeg).Release(callerID); err != nil {
			zlog.Error().Err(err).Str("callerID", callerID).Msg("failed to release segment after finalization")
		}
		var err error
		*newSeg, err = sr.sm.AcquireOpenSegmentWithReservation(callerID, 0)
		if err != nil {
			return fmt.Errorf("failed to acquire new segment: %w", err)
		}
	}

	// Create ValueMessage for WriteEntryFromReader
	vm := &pb.ValueMessage{
		ValueType:   pb.ValueType_SEGMENT,
		ValueLength: entry.ValueLength,
		Checksum:    entry.Checksum,
	}

	var reader io.Reader = dataReader
	if sr.rateLimiter != nil {
		reader = newRateLimitedReader(ctx, reader, entry.ValueLength, sr.rateLimiter)
	}

	// Use segment's WriteEntry function
	newOffset, err := (*newSeg).WriteEntry(entry.Key, reader, vm)
	if err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}
	advice.addOutput(*newSeg, newOffset, totalNeeded)

	// Old segment values are one-pass recompaction input.
	dropFileCache(oldFile, entry.Offset, totalNeeded)

	// Publish the new location via a compare-and-swap merge, never an
	// unconditional Put. The operand matches both the old segment path and
	// source offset, so two records from one source cannot cross-apply after a
	// concurrent overwrite. The transient source facts are cleared by the merge
	// operator before the destination metadata is persisted.
	oldPath := meta.SegmentPath
	meta.SegmentPath = (*newSeg).Path()
	meta.SegmentOffset = newOffset
	meta.RawFilePath = oldPath

	operand, err := merge.MakeSegmentCASOperand(meta, entry.Offset)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metaKey := keys.MakeMetadataKey(entry.Key)
	wb.Merge(metaKey, operand)
	if err := stageSegmentLiveIndexRow(wb, entry.Key, (*newSeg).Path(), newOffset, entry.ValueLength, entry.Checksum); err != nil {
		return err
	}
	if err := stageSegmentLiveIndexDelete(wb, oldPath, entry.Offset); err != nil {
		return err
	}

	return nil
}

// isSegmentEligibleForRecompaction performs all checks to determine if a segment
// can be safely recompacted. This includes checking if it's open, if it's too recent,
// and if it's old enough based on timestamp.
func (sr *SegmentRecompactor) isSegmentEligibleForRecompaction(seg *segment.Segment, openSegments []*segment.Segment, segmentIndex int, totalSegments int) (bool, string) {
	// Check 1: Skip any currently open segments
	for _, openSeg := range openSegments {
		if openSeg != nil && seg == openSeg {
			return false, "is an open segment"
		}
	}

	// Check 2: Skip if segment has an open file handle (defensive check)
	if seg.HasOpenFile() {
		return false, "has open file handle"
	}

	// Check 3: Verify segment age based on timestamp
	base := filepath.Base(seg.Path())
	var timestamp int64
	// Try parsing the segment name format
	if _, err := fmt.Sscanf(base, "segment_%d.seg", &timestamp); err != nil {
		// Can't parse timestamp - skip for safety
		zlog.Debug().Str("segment", seg.Path()).Msg("recompactor: cannot parse timestamp, skipping for safety")
		return false, "cannot parse timestamp"
	}

	segmentTime := time.Unix(0, timestamp)
	age := time.Since(segmentTime)
	if age <= sr.minSegmentAge {
		return false, fmt.Sprintf("too young (age: %v, required: %v)", age, sr.minSegmentAge)
	}

	return true, ""
}

// removeSegmentLiveIndex removes all durable live-location rows and the
// coverage marker for a source segment after it is no longer tracked.
func (sr *SegmentRecompactor) removeSegmentLiveIndex(segmentPath string) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	if err := deleteSegmentLiveIndexRows(sr.meta, batch, segmentPath); err != nil {
		return err
	}
	if batch.Count() == 0 {
		return nil
	}
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return sr.meta.Handle().Write(wo, batch)
}

// getDeleteIndexStats retrieves delete index statistics for a segment
func (sr *SegmentRecompactor) getDeleteIndexStats(segmentPath string) (int64, int64, error) {
	deleteIndexKey := keys.MakeDeleteIndexKey(segmentPath)

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	slice, err := sr.meta.Handle().Get(ro, deleteIndexKey)
	if err != nil {
		return 0, 0, err
	}
	defer slice.Free()

	// If no delete index exists, no deletions
	if !slice.Exists() || len(slice.Data()) == 0 {
		return 0, 0, nil
	}

	var entry pb.DeleteIndexEntry
	if err := proto.Unmarshal(slice.Data(), &entry); err != nil {
		return 0, 0, err
	}

	return entry.DeletedEntries, entry.DeletedBytes, nil
}

// removeDeleteIndex removes the delete index for a segment
func (sr *SegmentRecompactor) removeDeleteIndex(segmentPath string) error {
	deleteIndexKey := keys.MakeDeleteIndexKey(segmentPath)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	return sr.meta.Handle().Delete(wo, deleteIndexKey)
}
