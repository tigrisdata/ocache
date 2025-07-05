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

	pb "github.com/tigrisdata/cache_service/proto"
	"github.com/tigrisdata/cache_service/server/storage/bufferpool"
	"github.com/tigrisdata/cache_service/server/storage/fd"
	"github.com/tigrisdata/cache_service/server/utils"

	zlog "github.com/rs/zerolog/log"
)

const (
	// Segment sizes
	SmallSegmentSize   = 1 << 30 // 1GB
	LargeSegmentSize   = 1 << 32 // 4GB
	DefaultSegmentSize = SmallSegmentSize

	// Default compaction thresholds
	DefaultRawToSegmentPromotionThreshold   = 1 << 30  // 1GB
	DefaultCompactionMaxBytes               = 1 << 30  // 1GB
	DefaultCompactionIntermediateFlushBytes = 64 << 20 // 64 MiB
	DefaultRawCompactionInterval            = 5 * time.Minute
	DefaultSegmentCompactionInterval        = 1 * time.Hour
)

// Segment is a file on disk that contains key/value pairs.
type Segment struct {
	path     string
	size     int64
	file     *os.File
	mu       sync.RWMutex
	position int64

	// Statistics
	entries   uint32 // number of key/value pairs stored in this segment
	dataBytes int64  // total number of bytes occupied by value payloads (not counting headers)

	// Format version of this segment (derived from footer when closed or set when created).
	version int
}

// Manager manages the segments on disk.
type Manager struct {
	segmentsPath string
	segmentSize  int64
	segments     []*Segment          // ordered list (oldest→newest)
	segMap       map[string]*Segment // path → *Segment for O(1) lookup
	mu           sync.RWMutex
	rawManager   *RawFileManager
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

// RegisterSegment implements segmentfile.Registry allowing helper code to add
// new segments without poking into internal maps externally.
func (sm *Manager) RegisterSegment(path string, entries uint32, bytes int64) {
	seg := &Segment{path: path, entries: entries, dataBytes: int64(bytes), position: int64(bytes), version: CurrentSegmentVersion}
	sm.mu.Lock()
	sm.segments = append(sm.segments, seg)
	sm.segMap[path] = seg
	sm.mu.Unlock()
}

// NewManager creates a new segment manager
func NewManager(basePath string) (*Manager, error) {
	segmentsPath := filepath.Join(basePath, "segments")
	rawFilesPath := filepath.Join(basePath, "raw_files")

	zlog.Info().Str("segmentsPath", segmentsPath).Str("rawFilesPath", rawFilesPath).Msg("creating segment manager")

	if err := os.MkdirAll(segmentsPath, 0o755); err != nil {
		zlog.Error().Err(err).Str("path", segmentsPath).Msg("failed to create segment directory")
		return nil, utils.WrapError("failed to create segment directory", segmentsPath, err)
	}

	rawWriter, err := NewRawFileManager(rawFilesPath)
	if err != nil {
		zlog.Error().Err(err).Str("path", rawFilesPath).Msg("failed to create raw writer")
		return nil, utils.WrapError("failed to create raw writer", rawFilesPath, err)
	}

	sm := &Manager{
		segmentsPath: segmentsPath,
		segmentSize:  DefaultSegmentSize,
		rawManager:   rawWriter,
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

	// Start compaction goroutine
	sm.wg.Add(1)
	go sm.compactionLoop()

	zlog.Info().Msg("segment manager created")

	return sm, nil
}

// WriteValue writes a value to a raw file
func (sm *Manager) WriteValue(key string, reader io.Reader) (string, error) {
	return sm.rawManager.Write(key, reader)
}

// ReadValue returns a reader for a ValueMessage that references a raw file or a
// segment slice. Caller must ensure vm is non-nil. Returns (nil, nil) if no
// external data is referenced.
func (sm *Manager) ReadValue(vm *pb.ValueMessage) (io.ReadCloser, error) {
	if vm == nil {
		return nil, utils.WrapError("nil ValueMessage", "", nil)
	}

	// If the value is stored in a segment, return a reader for the segment slice
	if vm.SegmentPath != "" && vm.ValueLength > 0 {
		return sm.readSlice(vm.SegmentPath, vm.SegmentOffset, vm.ValueLength)
	}

	// If the value is stored in a raw file, return a reader for the raw file
	if vm.RawFilePath != "" {
		return sm.rawManager.Read(vm.RawFilePath)
	}

	// Return nil if no data is referenced in segment or raw file
	return nil, nil
}

// readSlice returns an io.ReadCloser over a slice of a segment file.
func (sm *Manager) readSlice(segPath string, offset, length int64) (io.ReadCloser, error) {
	sm.mu.RLock()
	seg := sm.segMap[segPath]
	sm.mu.RUnlock()

	if seg == nil {
		return nil, utils.WrapError("segment not found", segPath, nil)
	}

	// Acquire cached read-only descriptor via FdCache.
	entry, err := sm.fdCache.Acquire(segPath)
	if err != nil {
		return nil, err
	}

	// Take shared read lock to protect against concurrent writers.
	entry.RLock()

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

		segment := &Segment{
			path:     path,
			size:     stat.Size(),
			file:     file,
			position: stat.Size(),
			version:  CurrentSegmentVersion,
		}

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
			if err := sm.finalizeSegment(openSegs[i]); err != nil {
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

	for {
		// Read header
		valLen, headerSize, keyLen, _, checksum, err := ReadValueHeader(seg.file)
		if err != nil {
			return utils.WrapError("read header", seg.path, err)
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
		if nextPos > seg.size {
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

	zlog.Info().Str("path", seg.path).Msg("finished validating open segment")

	// Update segment struct
	seg.entries = entries
	seg.dataBytes = dataBytes
	seg.position = pos
	seg.size = pos

	// Seek file to end for further writes
	if _, err := seg.file.Seek(pos, io.SeekStart); err != nil {
		return utils.WrapError("seek to end", seg.path, err)
	}

	return nil
}

// WriteToSegment writes a value from a raw file into the current segment, creating a new one if needed
func (sm *Manager) WriteToSegment(key string, filePath string) (string, int64, int64, error) {
	src, err := os.Open(filePath)
	if err != nil {
		return "", 0, 0, utils.WrapError("open raw file", filePath, err)
	}
	defer src.Close()

	valueLen, headerSize, _, version, checksum, err := ReadValueHeader(src)
	if err != nil {
		return "", 0, 0, utils.WrapError("read value header", filePath, err)
	}

	// Build header
	header := BuildValueHeader(key, valueLen, checksum, version)

	// Total bytes to add
	needed := headerSize + valueLen

	// Acquire lock to ensure only one writer at a time.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Ensure we have a writable segment with space
	segment, err := sm.getWritableSegment(needed)
	if err != nil {
		return "", 0, 0, err
	}

	// Offset where this value will be written inside the segment
	startOffset := segment.position

	// Reset file cursor to start of value.
	if _, err := src.Seek(headerSize, io.SeekStart); err != nil {
		return "", 0, 0, utils.WrapError("seek to start", filePath, err)
	}

	// Sequential write: header then payload
	if _, err := segment.file.Write(header); err != nil {
		return "", 0, 0, utils.WrapError("failed to write header", filePath, err)
	}
	if _, err := io.Copy(segment.file, src); err != nil {
		return "", 0, 0, utils.WrapError("copy value to segment", filePath, err)
	}

	segment.position += needed
	segment.entries++
	segment.dataBytes += valueLen
	return segment.path, startOffset, valueLen, nil
}

// SyncCurrentSegment syncs the current segment.
func (sm *Manager) SyncCurrentSegment() error {
	// Fast path: no segments yet
	sm.mu.RLock()
	if len(sm.segments) == 0 {
		sm.mu.RUnlock()
		return nil
	}
	// Get the newest (currently writable) segment
	seg := sm.segments[len(sm.segments)-1]
	sm.mu.RUnlock()

	// If the segment is already finalized (file == nil) nothing to sync
	seg.mu.RLock()
	if seg.file == nil {
		seg.mu.RUnlock()
		return nil
	}

	// Flush file contents to disk
	err := seg.file.Sync()
	seg.mu.RUnlock()
	if err != nil {
		return utils.WrapError("failed to sync current segment", seg.path, err)
	}
	return nil
}

// getWritableSegment returns a segment that can be written to ensuring that
// `needed` additional bytes will still fit within the configured segment size.
// If the current open segment cannot accommodate the write, it is finalised
// and a new segment is created.
func (sm *Manager) getWritableSegment(needed int64) (*Segment, error) {
	if len(sm.segments) == 0 {
		return sm.createNewSegment()
	}

	lastSeg := sm.segments[len(sm.segments)-1]
	if lastSeg.position+needed > sm.segmentSize {
		// Not enough space – finalise current and roll over
		if err := sm.finalizeSegment(lastSeg); err != nil {
			return nil, err
		}
		return sm.createNewSegment()
	}

	return lastSeg, nil
}

// finalizeSegment writes a footer to the segment file and closes it so that no
// further writes are possible. Callers must hold sm.mu when invoking this.
func (sm *Manager) finalizeSegment(seg *Segment) error {
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
	seg.position += int64(len(footer))
	// Shrink pre-allocated file to actual used size
	if err := seg.file.Truncate(seg.position); err != nil {
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

	segment := &Segment{
		path:     path,
		file:     file,
		position: 0,
		version:  CurrentSegmentVersion,
	}

	sm.segments = append(sm.segments, segment)
	sm.segMap[path] = segment
	return segment, nil
}

// compactionLoop periodically compacts segments
func (sm *Manager) compactionLoop() {
	defer sm.wg.Done()

	zlog.Info().Msg("starting compaction loop")

	rawTicker := time.NewTicker(DefaultRawCompactionInterval)
	defer rawTicker.Stop()

	segmentTicker := time.NewTicker(DefaultSegmentCompactionInterval)
	defer segmentTicker.Stop()

	for {
		select {
		case <-rawTicker.C:
			sm.compactRawFiles()
		case <-segmentTicker.C:
			sm.compactSegments()
		case <-sm.closeCh:
			zlog.Info().Msg("segment manager: compaction loop stopping")
			return
		}
	}
}

// compactSegments is a placeholder for future segment-merging compaction logic.
func (sm *Manager) compactSegments() {
	// TODO: implement segment-level compaction (merge small closed segments, drop deleted keys, etc.)
}

// compactRawFiles moves data from the raw-files directory into segments using the
// RocksDB raw index (key prefix "!raw/"). After a successful copy the index row
// is removed. The raw file itself is **not** deleted yet – we keep it until the
// reader path understands segment offsets.
func (sm *Manager) compactRawFiles() {
	compactor := NewCompactor(sm.rawManager, sm)
	compactor.CompactRawFiles(DefaultCompactionMaxBytes, DefaultCompactionIntermediateFlushBytes)
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
