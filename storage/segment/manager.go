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

	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/bufferpool"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/utils"

	zlog "github.com/rs/zerolog/log"
)

// Segment is a file on disk that contains key/value pairs.
type Segment struct {
	path string
	size int64
	file *os.File
	mu   sync.RWMutex

	// Statistics
	entries   uint32 // number of key/value pairs stored in this segment
	dataBytes int64  // total number of bytes occupied by value payloads (not counting headers)

	// Format version of this segment (derived from footer when closed or set when created).
	version int

	// Maximum size of the segment.
	maxSupportedSize int64
}

// Path returns the path of the segment.
func (s *Segment) Path() string {
	return s.path
}

// Remaining returns the remaining space in the segment.
func (s *Segment) Remaining() int64 {
	return s.maxSupportedSize - s.size
}

// SetOpenFile sets the open file for the segment.
func (s *Segment) SetOpenFile(file *os.File) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.file = file
}

// GetSize returns the current size of data written to the segment.
func (s *Segment) GetSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// GetEntries returns the number of entries in the segment.
func (s *Segment) GetEntries() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries
}

// HasOpenFile returns true if the segment has an open file.
func (s *Segment) HasOpenFile() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.file != nil
}

// Lock locks the segment for exclusive access
func (s *Segment) Lock() {
	s.mu.Lock()
}

// Unlock unlocks the segment
func (s *Segment) Unlock() {
	s.mu.Unlock()
}

// File returns the underlying file (must be called while holding lock)
func (s *Segment) File() *os.File {
	return s.file
}

// IncrementSize increments the segment size (must be called while holding lock)
func (s *Segment) IncrementSize(delta int64) {
	s.size += delta
}

// IncrementEntries increments the entry count (must be called while holding lock)
func (s *Segment) IncrementEntries() {
	s.entries++
}

// IncrementDataBytes increments the data bytes count (must be called while holding lock)
func (s *Segment) IncrementDataBytes(delta int64) {
	s.dataBytes += delta
}

// GetSizeUnsafe returns the size without locking (must be called while holding lock)
func (s *Segment) GetSizeUnsafe() int64 {
	return s.size
}

// NewSegment creates a new segment with the given path and size.
func NewSegment(path string, entries uint32, dataBytes int64, size int64, maxSupportedSize int64) *Segment {
	return &Segment{path: path, entries: entries, dataBytes: dataBytes, size: size, version: CurrentSegmentVersion, maxSupportedSize: maxSupportedSize}
}

// Manager manages the segments on disk.
type Manager struct {
	segmentsPath string
	segmentSize  int64
	segments     []*Segment          // ordered list (oldest→newest)
	segMap       map[string]*Segment // path → *Segment for O(1) lookup
	mu           sync.RWMutex
	fdCache      *fd.FdCache // descriptor cache for closed segments

	// shutdown handling for background compaction goroutine
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// Registry is implemented by segment managers that need to be informed when a
// new segment file is created (e.g. after promotion from a raw file).
type Registry interface {
	RegisterSegment(path string, entries uint32, bytes int64)
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

// ReadValue returns an io.ReadCloser over a slice of a segment file.
func (sm *Manager) ReadValue(userKey string, segPath string, offset, length int64) (io.ReadCloser, error) {
	if segPath == "" || offset < 0 || length <= 0 {
		return nil, fmt.Errorf("invalid segment path, offset or length: path=%s, offset=%d, length=%d", segPath, offset, length)
	}

	sm.mu.RLock()
	seg := sm.segMap[segPath]
	sm.mu.RUnlock()

	if seg == nil {
		return nil, fmt.Errorf("segment not found: %s", segPath)
	}

	// Acquire cached read-only descriptor via FdCache.
	entry, err := sm.fdCache.Acquire(segPath)
	if err != nil {
		return nil, err
	}

	// Take shared read lock to protect against concurrent writers.
	entry.RLock()

	// calculate the offset of the value in the segment
	offset += CalculateValueHeaderSize(userKey)

	reader := io.NewSectionReader(entry.File(), offset, length)
	return &readCloserWithOnClose{
		Reader: reader,
		onClose: func() {
			// Release lock & cached FD when caller is done.
			entry.RUnlock()
			sm.fdCache.Release(segPath, entry)
		},
	}, nil
}

// WriteEntryFromReader writes an entry to a segment from an io.Reader
func (sm *Manager) WriteEntryFromReader(seg *Segment, userKey string, r io.Reader, vm *pb.ValueMessage) (int64, error) {
	if vm.ValueType != pb.ValueType_RAW_FILE && vm.ValueType != pb.ValueType_SEGMENT {
		return 0, utils.WrapError("invalid value type", userKey, nil)
	}

	header := BuildValueHeader(userKey, vm.ValueLength, vm.Checksum, CurrentValueHeaderVersion)
	headerSize := CalculateValueHeaderSize(userKey)

	// Total bytes to add
	needed := headerSize + vm.ValueLength

	// Log the write operation
	zlog.Debug().
		Str("key", userKey).
		Int64("valueLength", vm.ValueLength).
		Str("segment", seg.path).
		Msg("writing entry to segment from reader")

	// Acquire lock to ensure only one writer to the segment at a time.
	seg.mu.Lock()
	defer seg.mu.Unlock()

	// Check if segment file is still open
	if seg.file == nil {
		return 0, utils.WrapError("segment file is closed", seg.path, nil)
	}

	// Ensure we have a writable segment with space
	// Offset where this value will be written inside the segment
	startOffset := seg.size

	// Sequential write: header then payload
	if _, err := seg.file.Write(header); err != nil {
		return 0, utils.WrapError("failed to write value header", seg.path, err)
	}

	// Copy with progress tracking for large files using pooled buffer
	buf, release := bufferpool.AcquireBuffer(64 * 1024) // 64KB buffer
	defer release()

	bytesWritten, err := io.CopyBuffer(seg.file, r, buf)
	if err != nil {
		return 0, utils.WrapError("copy value to segment", userKey, err)
	}

	// Verify we wrote the expected amount
	if bytesWritten != vm.ValueLength {
		return 0, utils.WrapError(fmt.Sprintf("wrote %d bytes, expected %d", bytesWritten, vm.ValueLength), userKey, nil)
	}

	seg.size += needed
	seg.entries++
	seg.dataBytes += vm.ValueLength

	zlog.Debug().
		Str("key", userKey).
		Int64("bytesWritten", bytesWritten).
		Int64("segmentSize", seg.size).
		Msg("successfully wrote entry to segment")

	return startOffset, nil
}

func (sm *Manager) WriteEntry(seg *Segment, userKey string, f *os.File, vm *pb.ValueMessage) (int64, error) {
	// Reset file cursor
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, utils.WrapError("seek to start", userKey, err)
	}

	// Use the reader-based implementation
	return sm.WriteEntryFromReader(seg, userKey, f, vm)
}

// Returns an open segment (may create a new one)
func (sm *Manager) AcquireOpenSegment(needed int64) (*Segment, error) {
	// If no open segments, create a new one
	if len(sm.segments) == 0 {
		return sm.createNewSegment()
	}

	// Get the newest (currently writable) segment
	lastSeg := sm.segments[len(sm.segments)-1]

	lastSeg.mu.RLock()
	defer lastSeg.mu.RUnlock()

	// If the segment is already finalized (file == nil) create a new one
	if lastSeg.file == nil {
		return sm.createNewSegment()
	}

	return lastSeg, nil
}

// SyncSegment syncs the segment.
func (sm *Manager) SyncSegment(seg *Segment) error {
	seg.mu.RLock()
	defer seg.mu.RUnlock()

	if seg.file == nil {
		return nil
	}

	// Flush file contents to disk
	err := seg.file.Sync()
	if err != nil {
		return utils.WrapError("failed to sync current segment", seg.path, err)
	}
	return nil
}

// FinalizeSegment writes a footer to the segment file and closes it so that no
// further writes are possible.
func (sm *Manager) FinalizeSegment(seg *Segment) error {
	zlog.Info().Str("path", seg.path).Msg("finalizing segment")

	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.file == nil {
		return nil // already closed
	}

	// Build footer [magic|entries|bytes]
	footer := BuildSegmentFooterWithVersion(seg.version, seg.entries, seg.dataBytes)

	if _, err := seg.file.Write(footer); err != nil {
		return utils.WrapError("failed to write segment footer", seg.path, err)
	}
	seg.size += int64(len(footer))
	// Shrink pre-allocated file to actual used size
	if err := seg.file.Truncate(seg.size); err != nil {
		return utils.WrapError("truncate segment", seg.path, err)
	}

	// Flush and close the R/W file descriptor
	if err := seg.file.Sync(); err != nil {
		return utils.WrapError("failed to sync segment", seg.path, err)
	}
	if err := seg.file.Close(); err != nil {
		return utils.WrapError("failed to close segment", seg.path, err)
	}

	// Clear pointer – closed segments will use fdCache for reads.
	seg.file = nil

	zlog.Info().Str("path", seg.path).Msg("finished finalizing segment")

	return nil
}

// createNewSegment creates a new segment file
func (sm *Manager) createNewSegment() (*Segment, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	path := filepath.Join(sm.segmentsPath, fmt.Sprintf("segment_%d.seg", time.Now().UnixNano()))

	zlog.Info().Str("path", path).Msg("creating new segment")

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, utils.WrapError("failed to create segment file", path, err)
	}

	// Pre-allocate file to configured segment size
	if err := file.Truncate(sm.segmentSize); err != nil {
		file.Close()
		return nil, utils.WrapError("truncate segment file", path, err)
	}

	segment := NewSegment(path, 0, 0, 0, sm.segmentSize)
	segment.SetOpenFile(file)

	sm.segments = append(sm.segments, segment)
	sm.segMap[path] = segment
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
					segment.entries = ent
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

	return nil
}

// validateOpenSegment scans the segment, counts entries, truncates invalid tail, and
// updates position/statistics. The segment file remains open for further writes.
func (sm *Manager) validateOpenSegment(seg *Segment) error {
	zlog.Info().Str("path", seg.path).Msg("validating open segment")

	pos := int64(0)
	entries := uint32(0)
	dataBytes := int64(0)

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

	for {
		// Read header at current position
		valLen, headerSize, keyLen, _, checksum, err := ReadValueHeaderAt(seg.file, pos)
		if err != nil {
			// EOF or read error means we've reached the end of valid data
			if err := seg.file.Truncate(pos); err != nil {
				return utils.WrapError("truncate at read error", seg.path, err)
			}
			break
		}

		// Check that header values are reasonable
		if valLen < 0 || keyLen < 0 {
			if err := seg.file.Truncate(pos); err != nil {
				return utils.WrapError("truncate invalid header", seg.path, err)
			}
			break
		}

		entryTotal := headerSize + valLen
		nextPos := pos + entryTotal

		// Ensure we have full entry in file
		if nextPos > actualFileSize {
			if err := seg.file.Truncate(pos); err != nil {
				return utils.WrapError("truncate partial entry", seg.path, err)
			}
			break
		}

		// Checksum validation when header contains non-zero checksum.
		if checksum != 0 {
			valueOffset := pos + headerSize // pos is entry start offset

			h := crc32.NewIEEE()
			section := io.NewSectionReader(seg.file, valueOffset, valLen)

			buf, releaseBuf := bufferpool.AcquireBuffer(64 * 1024)
			if _, err := io.CopyBuffer(h, section, buf); err != nil {
				releaseBuf()
				return utils.WrapError("checksum read", seg.path, err)
			}
			releaseBuf()

			if h.Sum32() != checksum {
				zlog.Warn().Str("segment", seg.path).Int64("offset", pos).Msg("checksum mismatch – truncating segment")
				if err := seg.file.Truncate(pos); err != nil {
					return utils.WrapError("truncate after checksum mismatch", seg.path, err)
				}
				break
			}
		}

		// Entry seems valid – advance
		entries++
		dataBytes += valLen
		pos = nextPos
	}

	zlog.Info().Str("path", seg.path).Int64("valid_data_size", pos).Int64("maxSupportedSize", seg.maxSupportedSize).Msg("finished validating open segment")

	// Update segment struct
	seg.entries = entries
	seg.dataBytes = dataBytes
	seg.size = pos // This tracks actual data written, not pre-allocated file size

	// Seek file to end for further writes
	if _, err := seg.file.Seek(pos, io.SeekStart); err != nil {
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

// readCloserWithOnClose wraps a reader and calls the provided function when closed.
type readCloserWithOnClose struct {
	io.Reader
	onClose func()
}

func (rc *readCloserWithOnClose) Close() error {
	if rc.onClose != nil {
		rc.onClose()
	}
	return nil
}
