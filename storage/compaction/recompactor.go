package compaction

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/deletion"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	"github.com/tigrisdata/ocache/storage/segment"
	"github.com/tigrisdata/ocache/storage/utils"
	"google.golang.org/protobuf/proto"

	zlog "github.com/rs/zerolog/log"
)

const (
	// recompactorCallerIDPrefix is the prefix for the caller ID for the recompactor.
	recompactorCallerIDPrefix = "recompactor-"

	// minSegments is the minimum number of segments required for recompaction.
	minSegmentsBeforeRecompaction = 3
)

// SegmentRecompactor handles recompaction of fragmented segments
type SegmentRecompactor struct {
	sm            *segment.Manager
	meta          *metadata.MetaDB
	deletionQueue *deletion.Queue
	fragThreshold float64
	minSegmentAge time.Duration
}

// NewSegmentRecompactor creates a new segment recompactor
func NewSegmentRecompactor(sm *segment.Manager, deletionQueue *deletion.Queue, fragThreshold float64, minSegmentAge time.Duration) *SegmentRecompactor {
	return &SegmentRecompactor{
		sm:            sm,
		meta:          metadata.GetMetaDB(),
		deletionQueue: deletionQueue,
		fragThreshold: fragThreshold,
		minSegmentAge: minSegmentAge,
	}
}

// RecompactFragmentedSegments identifies and recompacts fragmented segments
func (sr *SegmentRecompactor) RecompactFragmentedSegments(ctx context.Context) error {
	zlog.Info().
		Float64("threshold", sr.fragThreshold).
		Msg("recompactor: starting segment recompaction scan")

	// Get all segments
	segments := sr.sm.GetSegments()
	totalSegments := len(segments)

	if totalSegments == 0 {
		return nil
	}

	// Safety check: Need enough segments to safely recompact
	// In production, we need at least 3 segments (2 to skip + 1 to potentially recompact)
	// But allow overriding for testing
	minSegments := minSegmentsBeforeRecompaction
	if skipStr := os.Getenv("OCACHE_TEST_RECOMPACTION_SKIP_RECENT"); skipStr != "" {
		if skip, err := strconv.Atoi(skipStr); err == nil && skip == 0 {
			// If set in tests, allow recompaction with just 1 segment
			minSegments = 1
		}
	}
	if totalSegments < minSegments {
		zlog.Debug().Int("segmentCount", totalSegments).Int("minRequired", minSegments).
			Msg("recompactor: too few segments to safely recompact")
		return nil
	}

	// Get the current open segment to ensure we never try to recompact it
	openSegments := sr.sm.GetOpenSegments()

	recompactedCount := 0
	// Process segments, checking eligibility for each
	for i, seg := range segments {
		// Check if this segment is eligible for recompaction
		eligible, reason := sr.isSegmentEligibleForRecompaction(seg, openSegments, i, totalSegments)
		if !eligible {
			zlog.Debug().
				Str("segment", seg.Path()).
				Str("reason", reason).
				Msg("recompactor: skipping segment")
			continue
		}

		// Check context cancellation
		if ctx.Err() != nil {
			zlog.Info().Msg("recompactor: interrupted by cancellation")
			return ctx.Err()
		}

		// Check fragmentation level
		deletedEntries, deletedBytes, err := sr.getDeleteIndexStats(seg.Path())
		if err != nil {
			zlog.Error().Err(err).Str("segment", seg.Path()).
				Msg("recompactor: failed to get delete index stats")
			continue
		}

		// Skip if no deletions
		if deletedEntries == 0 {
			continue
		}

		// Check if segment is fragmented enough
		if !sr.sm.IsSegmentFragmented(seg.Path(), deletedBytes, sr.fragThreshold) {
			continue
		}

		zlog.Info().
			Str("segment", seg.Path()).
			Int64("deletedEntries", deletedEntries).
			Int64("deletedBytes", deletedBytes).
			Float64("fragmentation", sr.sm.GetFragmentationRatio(seg.Path(), deletedBytes)).
			Msg("recompactor: found fragmented segment")

		// Recompact this segment
		if err := sr.recompactSegment(ctx, seg); err != nil {
			zlog.Error().Err(err).Str("segment", seg.Path()).
				Msg("recompactor: failed to recompact segment")
			continue
		}

		recompactedCount++
	}

	if recompactedCount > 0 {
		zlog.Info().Int("count", recompactedCount).
			Msg("recompactor: finished segment recompaction")
	}

	return nil
}

// recompactSegment copies live data from a fragmented segment to a new segment
func (sr *SegmentRecompactor) recompactSegment(ctx context.Context, oldSeg *segment.Segment) error {
	zlog.Info().Str("segment", oldSeg.Path()).Msg("recompactor: starting segment recompaction")

	// Open the old segment for reading
	oldFile, err := os.Open(oldSeg.Path())
	if err != nil {
		return fmt.Errorf("failed to open segment %s: %w", oldSeg.Path(), err)
	}
	defer oldFile.Close()

	// Get file info for size
	fileInfo, err := oldFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat segment %s: %w", oldSeg.Path(), err)
	}

	// Create a new segment for the live data with reservation
	callerID := fmt.Sprintf("%s%s", recompactorCallerIDPrefix, oldSeg.Path()) // Unique ID per segment being recompacted
	newSeg, err := sr.sm.AcquireOpenSegmentWithReservation(callerID, 0)
	if err != nil {
		return fmt.Errorf("failed to acquire new segment: %w", err)
	}
	// Use a pointer to ensure we release the final segment, not the initial one
	defer func() {
		if newSeg != nil {
			if err := sr.sm.ReleaseSegment(newSeg, callerID); err != nil {
				zlog.Error().Err(err).Str("callerID", callerID).Msg("failed to release segment")
			}
		}
	}()

	// Track metadata updates
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	// Scan the old segment and copy live entries
	// TODO: Refactor to use an iterator on the Segment struct for cleaner entry iteration
	pos := int64(0)
	copiedEntries := uint32(0)
	copiedBytes := int64(0)

	for pos < fileInfo.Size()-int64(segment.SegmentFooterSize) {
		// Check context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Read the entry header
		valLen, headerSize, keyLen, version, checksum, err := segment.ReadValueHeaderAt(oldFile, pos)
		if err != nil {
			if err == io.EOF {
				break // End of valid data
			}
			zlog.Error().Err(err).Int64("offset", pos).
				Msg("recompactor: failed to read header")
			break
		}

		// Extract the key
		keyBuf := make([]byte, keyLen)
		if _, err := oldFile.ReadAt(keyBuf, pos+segment.ValueHeaderSize); err != nil {
			zlog.Error().Err(err).Int64("offset", pos).
				Msg("recompactor: failed to read key")
			break
		}
		userKey := string(keyBuf)

		// Check if this entry is still live (not deleted)
		metaKey := keys.MakeMetadataKey(userKey)
		meta, err := utils.GetMetadata(sr.meta, string(metaKey))
		if err != nil {
			// Entry has been deleted, skip it
			pos += headerSize + valLen
			continue
		}

		// Verify this entry still points to this segment
		if meta.ValueType != pb.ValueType_SEGMENT ||
			meta.SegmentPath != oldSeg.Path() ||
			meta.SegmentOffset != pos {
			// Entry has been overwritten or moved, skip it
			pos += headerSize + valLen
			continue
		}

		// This is a live entry, copy it to the new segment
		if err := sr.copyEntry(ctx, oldFile, &newSeg, callerID, userKey, pos, headerSize, valLen, version, checksum, meta, wb); err != nil {
			zlog.Error().Err(err).Str("key", userKey).
				Msg("recompactor: failed to copy entry")
			pos += headerSize + valLen
			continue
		}

		copiedEntries++
		copiedBytes += valLen
		pos += headerSize + valLen
	}

	// If no live entries were copied, abandon the new segment but still delete the old one
	if copiedEntries == 0 {
		zlog.Info().Str("segment", oldSeg.Path()).
			Msg("recompactor: no live entries found")
	}

	// Release the segment so it can be used by others
	if err := sr.sm.ReleaseSegment(newSeg, callerID); err != nil {
		zlog.Error().Err(err).Str("callerID", callerID).
			Msg("recompactor: failed to release segment")
	}

	// CRITICAL: Remove old segment from manager's tracking BEFORE committing metadata
	// This ensures no new reads can start on the old segment after metadata points to new segment
	removedSeg := sr.sm.RemoveSegment(oldSeg.Path())
	if removedSeg == nil {
		// Segment was already removed, possibly by another process
		zlog.Warn().Str("path", oldSeg.Path()).
			Msg("recompactor: segment already removed from manager")
	}

	// Now commit metadata updates - readers will only see the new segment
	if wb.Count() > 0 {
		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		if err := sr.meta.Handle().Write(wo, wb); err != nil {
			// If metadata commit fails, we need to restore the segment to the manager
			// This is a critical error as we've already removed it from tracking
			zlog.Error().Err(err).
				Str("oldSegment", oldSeg.Path()).
				Str("newSegment", newSeg.Path()).
				Msg("recompactor: CRITICAL - failed to commit metadata after removing segment from manager")
			return fmt.Errorf("failed to commit metadata updates: %w", err)
		}
	}

	// Queue old segment for deletion - it's safe now as no readers can access it
	if err := sr.deletionQueue.Add(oldSeg.Path()); err != nil {
		zlog.Error().Err(err).Str("path", oldSeg.Path()).
			Msg("recompactor: failed to queue old segment for deletion")
	}

	// Remove delete index for old segment
	if err := sr.removeDeleteIndex(oldSeg.Path()); err != nil {
		zlog.Error().Err(err).Str("segment", oldSeg.Path()).
			Msg("recompactor: failed to remove delete index")
	}

	zlog.Info().
		Str("oldSegment", oldSeg.Path()).
		Str("newSegment", newSeg.Path()).
		Uint32("copiedEntries", copiedEntries).
		Int64("copiedBytes", copiedBytes).
		Msg("recompactor: successfully recompacted segment")

	return nil
}

// copyEntry copies a single entry from old segment to new segment
func (sr *SegmentRecompactor) copyEntry(ctx context.Context, oldFile *os.File, newSeg **segment.Segment, callerID string,
	userKey string, oldOffset, headerSize, valLen int64, version uint16, checksum uint32,
	meta *pb.ValueMessage, wb *grocksdb.WriteBatch,
) error {
	// Create a section reader for the value data (no checksum verification per review)
	valueOffset := oldOffset + headerSize
	dataReader := io.NewSectionReader(oldFile, valueOffset, valLen)

	// Check if we need a new segment
	// NOTE: FinalizeSegment and AcquireOpenSegmentWithReservation are thread-safe - the segment
	// manager uses internal locking and reservations to coordinate between compactor and recompactor
	totalNeeded := headerSize + valLen
	if (*newSeg).Remaining() < totalNeeded {
		// Finalize the segment first, then release it
		// This prevents other threads from acquiring it while it's being finalized
		if err := sr.sm.FinalizeSegment(*newSeg); err != nil {
			return fmt.Errorf("failed to finalize segment: %w", err)
		}

		// Now safe to release since it's finalized
		if err := sr.sm.ReleaseSegment(*newSeg, callerID); err != nil {
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
		ValueLength: valLen,
		Checksum:    checksum,
	}

	// Use segment manager's WriteEntryFromReader function (avoids temp files)
	newOffset, err := sr.sm.WriteEntryFromReader(*newSeg, userKey, dataReader, vm)
	if err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	// Update metadata to point to new location
	meta.SegmentPath = (*newSeg).Path()
	meta.SegmentOffset = newOffset

	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metaKey := keys.MakeMetadataKey(userKey)
	wb.Put(metaKey, metaBytes)

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

	// Check 3: Skip the most recent segments (even if closed)
	// to avoid interfering with segments that might have just been finalized
	skipRecentCount := 2
	// Allow overriding for testing
	if skipStr := os.Getenv("OCACHE_TEST_RECOMPACTION_SKIP_RECENT"); skipStr != "" {
		if skip, err := strconv.Atoi(skipStr); err == nil && skip >= 0 {
			skipRecentCount = skip
		}
	}
	if skipRecentCount > 0 && segmentIndex >= totalSegments-skipRecentCount {
		return false, fmt.Sprintf("too recent (one of last %d segments)", skipRecentCount)
	}

	// Check 4: Verify segment age based on timestamp
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
