package compaction

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/bufferpool"
	"github.com/tigrisdata/ocache/storage/deletion"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	"github.com/tigrisdata/ocache/storage/segment"
	"github.com/tigrisdata/ocache/storage/utils"
	"google.golang.org/protobuf/proto"

	zlog "github.com/rs/zerolog/log"
)

const (
	// DefaultFragmentationThreshold is the default threshold for considering a segment fragmented
	// When dead space exceeds 50% of the segment, it's considered for recompaction
	DefaultFragmentationThreshold = 0.5

	// MinSegmentAgeForRecompaction is the minimum age a segment must have before recompaction
	// This prevents recompacting recently created segments that might still be receiving deletes
	MinSegmentAgeForRecompaction = 30 * time.Minute
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
func NewSegmentRecompactor(sm *segment.Manager, deletionQueue *deletion.Queue, fragThreshold float64) *SegmentRecompactor {
	if fragThreshold <= 0 || fragThreshold > 1 {
		fragThreshold = DefaultFragmentationThreshold
	}

	return &SegmentRecompactor{
		sm:            sm,
		meta:          metadata.GetMetaDB(),
		deletionQueue: deletionQueue,
		fragThreshold: fragThreshold,
		minSegmentAge: MinSegmentAgeForRecompaction,
	}
}

// RecompactFragmentedSegments identifies and recompacts fragmented segments
func (sr *SegmentRecompactor) RecompactFragmentedSegments(ctx context.Context) error {
	zlog.Info().
		Float64("threshold", sr.fragThreshold).
		Msg("recompactor: starting segment recompaction scan")

	// Get all segments
	segments := sr.sm.GetSegments()
	if len(segments) == 0 {
		return nil
	}

	recompactedCount := 0
	for _, seg := range segments {
		// Skip open segments (still being written to)
		if seg.HasOpenFile() {
			continue
		}

		// Check if segment is old enough for recompaction
		if !sr.isSegmentOldEnough(seg.Path()) {
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

	// Create a new segment for the live data
	newSeg, err := sr.sm.AcquireOpenSegment(0)
	if err != nil {
		return fmt.Errorf("failed to acquire new segment: %w", err)
	}

	// Track metadata updates
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	// Scan the old segment and copy live entries
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
		if err := sr.copyEntry(ctx, oldFile, &newSeg, userKey, pos, headerSize, valLen, version, checksum, meta, wb); err != nil {
			zlog.Error().Err(err).Str("key", userKey).
				Msg("recompactor: failed to copy entry")
			pos += headerSize + valLen
			continue
		}

		copiedEntries++
		copiedBytes += valLen
		pos += headerSize + valLen
	}

	// If no live entries were copied, abandon the new segment
	if copiedEntries == 0 {
		zlog.Info().Str("segment", oldSeg.Path()).
			Msg("recompactor: no live entries found, abandoning recompaction")
		// Clean up the new segment since we're not using it
		if err := sr.sm.FinalizeSegment(newSeg); err != nil {
			zlog.Error().Err(err).Msg("recompactor: failed to finalize abandoned segment")
		}
		// Queue the empty segment for deletion
		if err := sr.deletionQueue.Add(newSeg.Path()); err != nil {
			zlog.Error().Err(err).Str("path", newSeg.Path()).
				Msg("recompactor: failed to queue abandoned segment for deletion")
		}
		return nil
	}

	// Finalize the new segment
	if err := sr.sm.FinalizeSegment(newSeg); err != nil {
		return fmt.Errorf("failed to finalize new segment: %w", err)
	}

	// Commit metadata updates
	if wb.Count() > 0 {
		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		if err := sr.meta.Handle().Write(wo, wb); err != nil {
			return fmt.Errorf("failed to commit metadata updates: %w", err)
		}
	}

	// Queue old segment for deletion
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
// Note: We create a temporary file to use WriteEntry since it expects an *os.File
func (sr *SegmentRecompactor) copyEntry(ctx context.Context, oldFile *os.File, newSeg **segment.Segment,
	userKey string, oldOffset, headerSize, valLen int64, version uint16, checksum uint32,
	meta *pb.ValueMessage, wb *grocksdb.WriteBatch) error {

	// Create a temporary file for the value data
	tmpFile, err := os.CreateTemp("", "recompact-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy value data to temp file
	valueOffset := oldOffset + headerSize
	reader := io.NewSectionReader(oldFile, valueOffset, valLen)

	buf, release := bufferpool.AcquireBuffer(64 * 1024)
	defer release()

	// Verify checksum while copying if present
	var written int64
	if checksum != 0 {
		h := crc32.NewIEEE()
		teeReader := io.TeeReader(reader, h)
		written, err = io.CopyBuffer(tmpFile, teeReader, buf)
		if err != nil {
			return fmt.Errorf("failed to copy value to temp file: %w", err)
		}
		if h.Sum32() != checksum {
			return fmt.Errorf("checksum mismatch")
		}
	} else {
		written, err = io.CopyBuffer(tmpFile, reader, buf)
		if err != nil {
			return fmt.Errorf("failed to copy value to temp file: %w", err)
		}
	}

	if written != valLen {
		return fmt.Errorf("copied %d bytes, expected %d", written, valLen)
	}

	// Seek to start of temp file for WriteEntry
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek temp file: %w", err)
	}

	// Check if we need a new segment
	totalNeeded := headerSize + valLen
	if (*newSeg).Remaining() < totalNeeded {
		if err := sr.sm.FinalizeSegment(*newSeg); err != nil {
			return fmt.Errorf("failed to finalize segment: %w", err)
		}
		*newSeg, err = sr.sm.AcquireOpenSegment(0)
		if err != nil {
			return fmt.Errorf("failed to acquire new segment: %w", err)
		}
	}

	// Create ValueMessage for WriteEntry
	vm := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		ValueLength: valLen,
		Checksum:    checksum,
	}

	// Use segment manager's WriteEntry function
	newOffset, err := sr.sm.WriteEntry(*newSeg, userKey, tmpFile, vm)
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

// isSegmentOldEnough checks if a segment is old enough for recompaction
func (sr *SegmentRecompactor) isSegmentOldEnough(segmentPath string) bool {
	// Extract timestamp from segment filename (segment_<timestamp>.seg)
	base := filepath.Base(segmentPath)
	var timestamp int64
	if _, err := fmt.Sscanf(base, "segment_%d.seg", &timestamp); err != nil {
		// Can't parse timestamp, assume it's old enough
		return true
	}

	segmentTime := time.Unix(0, timestamp)
	return time.Since(segmentTime) > sr.minSegmentAge
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
