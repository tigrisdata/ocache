package segment

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/tigrisdata/ocache/common/bufferpool"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/utils"

	zlog "github.com/rs/zerolog/log"
)

// Manager manages the segments on disk.
type Manager struct {
	segmentsPath string
	segmentSize  int64
	segments     []*Segment          // ordered list (oldest→newest)
	segMap       map[string]*Segment // path → *Segment for O(1) lookup
	openSegments []*Segment          // list of currently open segments for writing
	mu           sync.RWMutex
	fdCache      *fd.FdCache // descriptor cache for closed segments

	// shutdown handling for background compaction goroutine
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewManager creates a new segment manager
func NewManager(basePath string, segmentSize int64) (*Manager, error) {
	segmentsPath := filepath.Join(basePath, "segments")

	zlog.Info().Str("segmentsPath", segmentsPath).Msg("creating segment manager")

	if err := os.MkdirAll(segmentsPath, 0o755); err != nil {
		zlog.Error().Err(err).Str("path", segmentsPath).Msg("failed to create segment directory")
		return nil, utils.WrapError("failed to create segment directory", segmentsPath, err)
	}

	sm := &Manager{
		segmentsPath: segmentsPath,
		segmentSize:  segmentSize,
		segMap:       make(map[string]*Segment),
		openSegments: []*Segment{}, // Initialize as empty slice
		closeCh:      make(chan struct{}),
	}

	// Instantiate descriptor cache for closed segments
	sm.fdCache = fd.GetFdCache()

	// Load existing segments
	if err := sm.loadSegments(); err != nil {
		zlog.Error().Err(err).Str("path", sm.segmentsPath).Msg("failed to load segments")
		return nil, err
	}

	zlog.Info().Msg("segment manager created")

	return sm, nil
}

// RegisterSegment allows helper code to add new segments without poking into
// internal maps externally.
func (sm *Manager) RegisterSegment(path string, entries uint32, bytes int64) {
	seg := NewSegment(path, entries, int64(bytes), int64(bytes), sm.segmentSize)
	sm.mu.Lock()
	sm.segments = append(sm.segments, seg)
	sm.segMap[path] = seg
	sm.mu.Unlock()
}

// ReadEntry returns an io.ReadCloser over a slice of a segment file.
func (sm *Manager) ReadEntry(userKey string, segPath string, offset, length int64) (io.ReadCloser, error) {
	if segPath == "" || offset < 0 || length <= 0 {
		return nil, fmt.Errorf("invalid segment path, offset or length: path=%s, offset=%d, length=%d", segPath, offset, length)
	}

	sm.mu.RLock()
	seg := sm.segMap[segPath]
	sm.mu.RUnlock()

	if seg == nil {
		// Segment has been removed (likely due to recompaction)
		// Return a specific error that can be handled by the caller
		return nil, fmt.Errorf("segment not found: %s", segPath)
	}

	return seg.ReadEntry(userKey, offset, length, sm.fdCache)
}

// AcquireOpenSegmentWithReservation returns an open segment reserved for the caller
// The callerID should be unique per goroutine/thread (e.g., "compactor", "recompactor-1")
// The segment will be reserved exclusively for this caller until released
func (sm *Manager) AcquireOpenSegmentWithReservation(callerID string, needed int64) (*Segment, error) {
	// Strict callerID validation
	if callerID == "" {
		return nil, fmt.Errorf("callerID cannot be empty")
	}

	// Always take write lock to simplify logic and prevent race conditions
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Look for an existing segment with enough space
	for _, seg := range sm.openSegments {
		seg.mu.RLock()
		if seg.file != nil && seg.Remaining() >= needed {
			// Check reservation status
			if seg.reservedBy == "" || seg.reservedBy == callerID {
				seg.mu.RUnlock()
				// Try to reserve it
				if seg.Reserve(callerID) {
					return seg, nil
				}
				// Reservation failed - continue searching
			} else {
				seg.mu.RUnlock()
			}
		} else {
			seg.mu.RUnlock()
		}
	}

	// No suitable segment found, create a new one with atomic reservation
	newSeg, err := sm.createNewSegmentWithReservationLocked(callerID)
	if err != nil {
		return nil, err
	}

	return newSeg, nil
}

// ReleaseAllSegments releases all segments reserved by the given caller
func (sm *Manager) ReleaseAllSegments(callerID string) error {
	if callerID == "" {
		return fmt.Errorf("callerID cannot be empty")
	}

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, seg := range sm.openSegments {
		seg.Release(callerID)
	}
	return nil
}

// FinalizeSegment writes a footer to the segment file and closes it so that no
// further writes are possible.
func (sm *Manager) FinalizeSegment(seg *Segment) error {
	start := time.Now()
	zlog.Info().Str("path", seg.path).Msg("finalizing segment")

	// First finalize the segment under its lock
	if err := seg.Finalize(); err != nil {
		metrics.StorageOperations.WithLabelValues("finalize", "segment", "error").Inc()
		return err
	}

	// Clear from openSegments if this was an open segment
	sm.mu.Lock()
	for i, openSeg := range sm.openSegments {
		if openSeg == seg {
			// Remove from slice by replacing with last element and shrinking
			sm.openSegments[i] = sm.openSegments[len(sm.openSegments)-1]
			sm.openSegments = sm.openSegments[:len(sm.openSegments)-1]
			break
		}
	}
	sm.mu.Unlock()

	zlog.Info().Str("path", seg.path).Msg("finished finalizing segment")

	// Track finalization metrics
	metrics.StorageOperations.WithLabelValues("finalize", "segment", "success").Inc()
	metrics.StorageOperationDuration.WithLabelValues("finalize", "segment").Observe(float64(time.Since(start).Milliseconds()))

	return nil
}

// createNewSegmentWithReservationLocked creates a new segment and atomically reserves it for the caller
// Must be called with write lock held
func (sm *Manager) createNewSegmentWithReservationLocked(callerID string) (*Segment, error) {
	// Generate unique filename using timestamp
	path := filepath.Join(sm.segmentsPath, fmt.Sprintf("segment_%d.seg", time.Now().UnixNano()))

	zlog.Info().Str("path", path).Str("caller", callerID).Msg("creating new segment with reservation")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, utils.WrapError("failed to create segment file", path, err)
	}

	// Pre-allocate file to configured segment size
	if err := file.Truncate(sm.segmentSize); err != nil {
		file.Close()
		return nil, utils.WrapError("truncate segment file", path, err)
	}

	// Create segment with atomic reservation
	segment := NewSegmentWithReservation(path, 0, 0, 0, sm.segmentSize, callerID)
	segment.SetOpenFile(file)

	sm.segments = append(sm.segments, segment)
	sm.segMap[path] = segment
	sm.openSegments = append(sm.openSegments, segment) // Add to open segments list

	return segment, nil
}

// loadSegments loads existing segments from disk
func (sm *Manager) loadSegments() error {
	zlog.Info().Str("path", sm.segmentsPath).Msg("loading segments")

	entries, err := os.ReadDir(sm.segmentsPath)
	if err != nil {
		return utils.WrapError("failed to read segment directory", sm.segmentsPath, err)
	}

	var openSegs []*Segment

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".seg" {
			continue
		}

		path := filepath.Join(sm.segmentsPath, entry.Name())
		file, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return utils.WrapError("failed to open segment", entry.Name(), err)
		}

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return utils.WrapError("failed to stat segment", entry.Name(), err)
		}

		// For open segments, we'll validate and determine actual size later
		// For now, just track that it exists. Don't use stat.Size() as the initial
		// size since it might be a sparse file with pre-allocated space.
		segment := NewSegment(path, 0, 0, 0, sm.segmentSize)
		segment.SetOpenFile(file)

		// Determine if file has footer
		if stat.Size() >= int64(SegmentFooterSize) {
			footer := make([]byte, SegmentFooterSize)
			if _, err := file.ReadAt(footer, stat.Size()-int64(SegmentFooterSize)); err == nil {
				if ver, ent, bytes, ok := ParseSegmentFooter(footer); ok {
					// Closed / finalized segment
					segment.version = ver
					segment.numEntries = ent
					segment.dataBytes = bytes

					// Closed segment – we don't keep a cached descriptor; rely on fdCache.
					file.Close()
					segment.file = nil
					sm.segments = append(sm.segments, segment)
					sm.segMap[path] = segment
					continue
				}
			}
		}

		// Open segment – needs validation/truncation
		if err := sm.validateOpenSegment(segment); err != nil {
			file.Close()
			return utils.WrapError("failed to validate open segment", entry.Name(), err)
		}
		openSegs = append(openSegs, segment)
		sm.segments = append(sm.segments, segment)
		sm.segMap[path] = segment
	}

	zlog.Info().Str("path", sm.segmentsPath).Msg("finished loading segments")

	// If more than one open segment, finalize all but the newest (by mod time)
	if len(openSegs) > 1 {
		zlog.Info().Str("path", sm.segmentsPath).Msg("finalizing open segments")

		// Sort openSegs by modification time ascending
		sort.Slice(openSegs, func(i, j int) bool {
			infoI, _ := os.Stat(openSegs[i].path)
			infoJ, _ := os.Stat(openSegs[j].path)
			return infoI.ModTime().Before(infoJ.ModTime())
		})

		for i := 0; i < len(openSegs)-1; i++ {
			if err := sm.FinalizeSegment(openSegs[i]); err != nil {
				return err
			}
		}
	}

	zlog.Info().Str("path", sm.segmentsPath).Msg("finished finalizing open segments")

	// Keep all remaining open segments in the openSegments list
	if len(openSegs) > 0 {
		// The remaining open segment(s) after finalization
		for _, seg := range openSegs[len(openSegs)-1:] {
			sm.openSegments = append(sm.openSegments, seg)
		}
	}

	// Initialize segment metrics
	segmentCount := len(sm.segments)
	var totalSize int64
	for _, s := range sm.segments {
		totalSize += s.size
	}
	metrics.SegmentCount.Set(float64(segmentCount))
	metrics.SegmentSize.Set(float64(totalSize))

	return nil
}

// validateOpenSegment scans the segment, counts entries, truncates invalid tail, and
// updates position/statistics. The segment file remains open for further writes.
func (sm *Manager) validateOpenSegment(seg *Segment) error {
	zlog.Info().Str("path", seg.path).Msg("validating open segment")

	// Seek to beginning to start validation
	if _, err := seg.file.Seek(0, io.SeekStart); err != nil {
		return utils.WrapError("seek to start for validation", seg.path, err)
	}

	// Get the actual file size (will be the full pre-allocated size for sparse files)
	fileInfo, err := seg.file.Stat()
	if err != nil {
		return utils.WrapError("stat segment file", seg.path, err)
	}
	actualFileSize := fileInfo.Size()

	// Create an iterator to scan through the segment
	iter, err := seg.NewIterator(seg.file)
	if err != nil {
		return utils.WrapError("create iterator for validation", seg.path, err)
	}

	entries := uint32(0)
	dataBytes := int64(0)
	lastValidPos := int64(0)

	for {
		currentPos := iter.CurrentPosition()

		// Get the next entry
		entry, err := iter.Next()
		if err != nil {
			// EOF or read error means we've reached the end of valid data
			if err := seg.file.Truncate(currentPos); err != nil {
				return utils.WrapError("truncate at read error", seg.path, err)
			}
			lastValidPos = currentPos
			break
		}

		// Check that header values are reasonable
		if entry.ValueLength <= 0 || len(entry.Key) <= 0 {
			if err := seg.file.Truncate(currentPos); err != nil {
				return utils.WrapError("truncate invalid header", seg.path, err)
			}
			lastValidPos = currentPos
			break
		}

		entryTotal := entry.HeaderSize + entry.ValueLength
		nextPos := currentPos + entryTotal

		// Ensure we have full entry in file
		if nextPos > actualFileSize {
			if err := seg.file.Truncate(currentPos); err != nil {
				return utils.WrapError("truncate partial entry", seg.path, err)
			}
			lastValidPos = currentPos
			break
		}

		// Checksum validation when header contains non-zero checksum.
		if entry.Checksum != 0 {
			valueOffset := currentPos + entry.HeaderSize // currentPos is entry start offset

			h := crc32.NewIEEE()
			section := io.NewSectionReader(seg.file, valueOffset, entry.ValueLength)

			buf, releaseBuf := bufferpool.AcquireBuffer(64 * 1024)
			if _, err := io.CopyBuffer(h, section, buf); err != nil {
				releaseBuf()
				return utils.WrapError("checksum read", seg.path, err)
			}
			releaseBuf()

			if h.Sum32() != entry.Checksum {
				zlog.Warn().Str("segment", seg.path).Int64("offset", currentPos).Msg("checksum mismatch – truncating segment")
				if err := seg.file.Truncate(currentPos); err != nil {
					return utils.WrapError("truncate after checksum mismatch", seg.path, err)
				}
				lastValidPos = currentPos
				break
			}
		}

		// Entry seems valid – advance
		entries++
		dataBytes += entry.ValueLength
		lastValidPos = iter.CurrentPosition()
	}

	zlog.Info().Str("path", seg.path).Int64("valid_data_size", lastValidPos).Int64("maxSupportedSize", seg.maxSupportedSize).Msg("finished validating open segment")

	// Update segment struct
	seg.numEntries = entries
	seg.dataBytes = dataBytes
	seg.size = lastValidPos // This tracks actual data written, not pre-allocated file size

	// Seek file to end for further writes
	if _, err := seg.file.Seek(lastValidPos, io.SeekStart); err != nil {
		return utils.WrapError("seek to end", seg.path, err)
	}

	return nil
}

// GetSegments returns a copy of the current segments slice for testing/inspection.
// The returned segments are safe to read but should not be modified.
func (sm *Manager) GetSegments() []*Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Return a copy of the slice to prevent external modification
	result := make([]*Segment, len(sm.segments))
	copy(result, sm.segments)
	return result
}

// GetOpenSegments returns all currently open segments (for testing/debugging)
func (sm *Manager) GetOpenSegments() []*Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make([]*Segment, len(sm.openSegments))
	copy(result, sm.openSegments)
	return result
}

// GetSegmentCount returns the number of segments currently managed.
func (sm *Manager) GetSegmentCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.segments)
}

// GetFragmentationRatio calculates the fragmentation ratio for a segment
// Returns the ratio of dead space to total segment size (0.0 to 1.0)
func (sm *Manager) GetFragmentationRatio(segmentPath string, deletedBytes int64) float64 {
	sm.mu.RLock()
	seg, exists := sm.segMap[segmentPath]
	sm.mu.RUnlock()

	if !exists || seg == nil {
		return 0.0
	}

	// Get the actual size of the segment
	segmentSize := seg.GetSize()
	if segmentSize == 0 {
		return 0.0
	}

	// Calculate fragmentation as ratio of deleted bytes to total size
	return float64(deletedBytes) / float64(segmentSize)
}

// IsSegmentFragmented checks if a segment exceeds the fragmentation threshold
func (sm *Manager) IsSegmentFragmented(segmentPath string, deletedBytes int64, threshold float64) bool {
	return sm.GetFragmentationRatio(segmentPath, deletedBytes) > threshold
}

// GetSegmentByPath returns a segment by its path
func (sm *Manager) GetSegmentByPath(path string) *Segment {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.segMap[path]
}

// Close closes all segment files
func (sm *Manager) Close() {
	zlog.Info().Msg("closing segment manager")

	// Signal background goroutine to exit and wait for it
	close(sm.closeCh)
	sm.wg.Wait()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, segment := range sm.segments {
		if segment.file != nil {
			if err := segment.file.Close(); err != nil {
				zlog.Error().Err(err).Str("path", segment.path).Msg("failed to close segment")
			}
		}
	}

	zlog.Info().Msg("segment manager closed")
}

// RemoveSegment removes a segment from the manager's tracking
// This should be called when a segment is being deleted permanently
// Returns the removed segment if found, nil otherwise
func (sm *Manager) RemoveSegment(segmentPath string) *Segment {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Get the segment before removing
	removedSeg := sm.segMap[segmentPath]

	// Remove from segMap
	delete(sm.segMap, segmentPath)

	// Remove from segments slice
	for i, seg := range sm.segments {
		if seg.Path() == segmentPath {
			// Remove by swapping with last element and truncating
			sm.segments[i] = sm.segments[len(sm.segments)-1]
			sm.segments = sm.segments[:len(sm.segments)-1]
			break
		}
	}

	// Remove from openSegments if present
	for i, seg := range sm.openSegments {
		if seg != nil && seg.Path() == segmentPath {
			sm.openSegments[i] = sm.openSegments[len(sm.openSegments)-1]
			sm.openSegments = sm.openSegments[:len(sm.openSegments)-1]
			break
		}
	}

	// Remove from fdCache - this removes from cache but doesn't close active file descriptors
	// Active readers can continue using their references
	sm.fdCache.Remove(segmentPath)

	if removedSeg != nil {
		zlog.Debug().Str("path", segmentPath).Msg("segment removed from manager")
	}

	return removedSeg
}
