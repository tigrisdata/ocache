package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// Segment sizes
	SmallSegmentSize   = 1 << 30 // 1GB
	LargeSegmentSize   = 1 << 32 // 4GB
	DefaultSegmentSize = LargeSegmentSize

	// Header format:
	// - 4 bytes: value length (uint32)
	// - 8 bytes: timestamp (int64)
	// - 4 bytes: key length (uint32)
	// - N bytes: key
	HeaderSize = 16 // Minimum header size without key

	// Footer V1 layout (20 bytes total):
	//   0..7   : ASCII magic "SEGEOF01"
	//   8..11  : uint32 number of entries in segment
	//   12..19 : uint64 total value bytes
	SegmentFooterMagic = "SEGEOF01" // 8-byte ASCII marker + version
	SegmentFooterSize  = 8 + 4 + 8  // 20 bytes fixed size
)

type Segment struct {
	path     string
	size     int64
	file     *os.File
	mmap     []byte // writable mmap for the open segment (nil for closed)
	mu       sync.Mutex
	position int64

	// Statistics
	entries   uint32 // number of key/value pairs stored in this segment
	dataBytes int64  // total number of bytes occupied by value payloads (not counting headers)
}

type SegmentManager struct {
	segmentsPath string
	segmentSize  int64
	segments     []*Segment
	mu           sync.RWMutex
	compactionCh chan struct{}
	rawWriter    *RawWriter
}

// NewSegmentManager creates a new segment manager
func NewSegmentManager(basePath string, segmentSize int64) (*SegmentManager, error) {
	segmentsPath := filepath.Join(basePath, "segments")
	rawFilesPath := filepath.Join(basePath, "raw_files")

	if err := os.MkdirAll(segmentsPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory: %w", err)
	}

	rawWriter, err := NewRawWriter(rawFilesPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create raw writer: %w", err)
	}

	sm := &SegmentManager{
		segmentsPath: segmentsPath,
		segmentSize:  segmentSize,
		compactionCh: make(chan struct{}, 1),
		rawWriter:    rawWriter,
	}

	// Load existing segments
	if err := sm.loadSegments(); err != nil {
		return nil, err
	}

	// Start compaction goroutine
	go sm.compactionLoop()

	return sm, nil
}

func (sm *SegmentManager) Write(key string, reader io.Reader) (string, error) {
	return sm.rawWriter.Write(key, reader)
}

// loadSegments loads existing segments from disk
func (sm *SegmentManager) loadSegments() error {
	entries, err := os.ReadDir(sm.segmentsPath)
	if err != nil {
		return fmt.Errorf("failed to read segment directory: %w", err)
	}

	var openSegs []*Segment

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".seg" {
			continue
		}

		path := filepath.Join(sm.segmentsPath, entry.Name())
		file, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("failed to open segment %s: %w", entry.Name(), err)
		}

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to stat segment %s: %w", entry.Name(), err)
		}

		segment := &Segment{
			path:     path,
			size:     stat.Size(),
			file:     file,
			position: stat.Size(),
		}

		// Determine if file has footer
		if stat.Size() >= int64(SegmentFooterSize) {
			footer := make([]byte, SegmentFooterSize)
			if _, err := file.ReadAt(footer, stat.Size()-int64(SegmentFooterSize)); err == nil {
				if string(footer[0:8]) == SegmentFooterMagic {
					// Closed / finalized segment
					segment.entries = binary.BigEndian.Uint32(footer[8:12])
					segment.dataBytes = int64(binary.BigEndian.Uint64(footer[12:20]))

					// Close current R/W handle and reopen read-only to cache descriptor
					file.Close()
					ro, err := os.Open(segment.path)
					if err == nil {
						segment.file = ro // cached read-only handle
					} else {
						segment.file = nil // Fallback: no handle cached
					}
					sm.segments = append(sm.segments, segment)
					continue
				}
			}
		}

		// Open segment – needs validation/truncation
		if err := sm.validateOpenSegment(segment); err != nil {
			file.Close()
			return fmt.Errorf("failed to validate open segment %s: %w", entry.Name(), err)
		}
		openSegs = append(openSegs, segment)
		sm.segments = append(sm.segments, segment)
	}

	// If more than one open segment, finalize all but the newest (by mod time)
	if len(openSegs) > 1 {
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

	return nil
}

// validateOpenSegment scans the segment, counts entries, truncates invalid tail, and
// updates position/statistics. The segment file remains open for further writes.
func (sm *SegmentManager) validateOpenSegment(seg *Segment) error {
	const headerFixed = HeaderSize // 16 bytes

	pos := int64(0)
	entries := uint32(0)
	dataBytes := int64(0)
	buf := make([]byte, headerFixed)

	for {
		// Read fixed part of header
		n, err := seg.file.ReadAt(buf, pos)
		if err == io.EOF && n == 0 {
			// clean EOF
			break
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("read header: %w", err)
		}
		if n < headerFixed {
			// incomplete header – truncate
			if err := seg.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate incomplete header: %w", err)
			}
			break
		}

		valLen := int64(binary.BigEndian.Uint32(buf[0:4]))
		keyLen := int64(binary.BigEndian.Uint32(buf[12:16]))

		// Check that header values are reasonable
		if valLen < 0 || keyLen < 0 || keyLen > 1<<20 { // arbitrary 1MB key limit
			if err := seg.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate invalid header: %w", err)
			}
			break
		}

		entryTotal := headerFixed + keyLen + valLen
		nextPos := pos + entryTotal

		// Ensure we have full entry in file
		if nextPos > seg.size {
			if err := seg.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate partial entry: %w", err)
			}
			break
		}

		// Entry seems valid – advance
		entries++
		dataBytes += valLen
		pos = nextPos
	}

	// Update segment struct
	seg.entries = entries
	seg.dataBytes = dataBytes
	seg.position = pos
	seg.size = pos

	// Seek file to end for further writes
	if _, err := seg.file.Seek(pos, io.SeekStart); err != nil {
		return err
	}

	return nil
}

// WriteToSegment writes a value from a raw file into the current segment, creating a new one if needed
func (sm *SegmentManager) WriteToSegment(key string, filePath string) error {
	// Determine value length first (stat the raw file)
	info, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("stat raw file: %w", err)
	}
	valueLen := info.Size()

	// Total bytes we will add = header + value
	needed := int64(HeaderSize+len(key)) + valueLen

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Ensure we have a writable segment with space
	segment, err := sm.getWritableSegment(needed)
	if err != nil {
		return err
	}

	// Prepare header
	header := make([]byte, HeaderSize+len(key))
	binary.BigEndian.PutUint32(header[0:4], uint32(valueLen))
	binary.BigEndian.PutUint64(header[4:12], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(header[12:16], uint32(len(key)))
	copy(header[16:], []byte(key))

	segment.mu.Lock()
	defer segment.mu.Unlock()

	if segment.mmap != nil {
		// Fast path: copy into mmap
		copy(segment.mmap[segment.position:], header)

		dst := segment.mmap[segment.position+int64(len(header)) : segment.position+needed]
		src, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("open raw file: %w", err)
		}
		defer src.Close()

		if _, err := io.ReadFull(src, dst); err != nil {
			return fmt.Errorf("read raw into mmap: %w", err)
		}
	} else {
		// Fallback to normal write
		if _, err := segment.file.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
		src, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("open raw file: %w", err)
		}
		defer src.Close()
		if _, err := io.Copy(segment.file, src); err != nil {
			return fmt.Errorf("copy value to segment: %w", err)
		}
	}

	segment.position += needed
	segment.entries++
	segment.dataBytes += valueLen
	return nil
}

// initMmap ensures seg.mmap is initialized for writable use. It truncates the file
// to segmentSize (if needed) and maps the whole region as read-write shared. Safe
// to call multiple times; it is a no-op when seg.mmap already exists.
func (seg *Segment) initMmap(segmentSize int64) error {
	if seg.mmap != nil {
		return nil
	}
	if seg.file == nil {
		return fmt.Errorf("segment file is nil while attempting mmap")
	}
	// Enlarge file to full segment size so mapping length is fixed.
	if err := seg.file.Truncate(segmentSize); err != nil {
		return fmt.Errorf("truncate for mmap: %w", err)
	}
	buf, err := unix.Mmap(int(seg.file.Fd()), 0, int(segmentSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	seg.mmap = buf
	return nil
}

// getWritableSegment returns a segment that can be written to ensuring that
// `needed` additional bytes will still fit within the configured segment size.
// If the current open segment cannot accommodate the write, it is finalised
// and a new segment is created.
func (sm *SegmentManager) getWritableSegment(needed int64) (*Segment, error) {
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

	// Ensure mmap ready for fast writes
	if err := lastSeg.initMmap(sm.segmentSize); err != nil {
		return nil, err
	}

	return lastSeg, nil
}

// finalizeSegment writes a footer to the segment file and closes it so that no
// further writes are possible. Callers must hold sm.mu when invoking this.
func (sm *SegmentManager) finalizeSegment(seg *Segment) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.file == nil {
		return nil // already closed
	}

	// Build footer [magic|entries|bytes]
	footer := make([]byte, SegmentFooterSize)
	copy(footer[0:8], []byte(SegmentFooterMagic))
	binary.BigEndian.PutUint32(footer[8:12], seg.entries)
	binary.BigEndian.PutUint64(footer[12:20], uint64(seg.dataBytes))

	if seg.mmap != nil {
		copy(seg.mmap[seg.position:], footer)
		seg.position += int64(len(footer))
		// msync to flush footer & previous writes
		_ = unix.Msync(seg.mmap[:seg.position], unix.MS_SYNC)
		// Unmap now that the segment is closed
		_ = unix.Munmap(seg.mmap)
		seg.mmap = nil
		// Shrink file to actual size
		_ = seg.file.Truncate(seg.position)
	} else {
		if _, err := seg.file.Write(footer); err != nil {
			return fmt.Errorf("failed to write segment footer: %w", err)
		}
		seg.position += int64(len(footer))
	}

	// Flush and close the R/W file descriptor
	if err := seg.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment: %w", err)
	}
	if err := seg.file.Close(); err != nil {
		return fmt.Errorf("failed to close segment: %w", err)
	}

	// Reopen read-only cached descriptor
	if ro, err := os.Open(seg.path); err == nil {
		seg.file = ro
	} else {
		seg.file = nil
	}
	return nil
}

// createNewSegment creates a new segment file
func (sm *SegmentManager) createNewSegment() (*Segment, error) {
	path := filepath.Join(sm.segmentsPath, fmt.Sprintf("segment_%d.seg", time.Now().UnixNano()))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file: %w", err)
	}

	segment := &Segment{
		path:     path,
		file:     file,
		position: 0,
	}

	// Ensure mmap ready for fast writes
	if err := segment.initMmap(sm.segmentSize); err != nil {
		segment.file.Close()
		return nil, fmt.Errorf("mmap segment: %w", err)
	}

	sm.segments = append(sm.segments, segment)
	return segment, nil
}

// compactionLoop periodically compacts segments
func (sm *SegmentManager) compactionLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sm.compactRawFiles()
		case <-sm.compactionCh:
			sm.compactRawFiles()
		}
	}
}

// compactRawFiles merges raw files into segments
func (sm *SegmentManager) compactRawFiles() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// TODO: Implement raw files compaction logic
	// 1. Identify raw files that are not in any segment
	// 2. Merge raw files into segments
	// 3. Remove old raw files
}

// Close closes all segment files
func (sm *SegmentManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, segment := range sm.segments {
		if err := segment.file.Close(); err != nil {
			return fmt.Errorf("failed to close segment %s: %w", segment.path, err)
		}
	}
	return nil
}
