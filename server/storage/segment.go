package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
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
)

type Segment struct {
	path     string
	size     int64
	file     *os.File
	mu       sync.Mutex
	position int64
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

	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".seg" {
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
			sm.segments = append(sm.segments, segment)
		}
	}

	return nil
}

// WriteToSegment writes a value to the current segment, creating a new one if needed
func (sm *SegmentManager) WriteToSegment(key string, value []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Ensure we have a writable segment
	segment, err := sm.getWritableSegment()
	if err != nil {
		return err
	}

	// Write header
	header := make([]byte, HeaderSize+len(key))
	binary.BigEndian.PutUint32(header[0:4], uint32(len(value)))
	binary.BigEndian.PutUint64(header[4:12], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(header[12:16], uint32(len(key)))
	copy(header[16:], []byte(key))

	segment.mu.Lock()
	defer segment.mu.Unlock()

	// Write header
	if _, err := segment.file.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write value
	if _, err := segment.file.Write(value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	segment.position += int64(len(header) + len(value))
	return nil
}

// getWritableSegment returns a segment that can be written to
func (sm *SegmentManager) getWritableSegment() (*Segment, error) {
	if len(sm.segments) == 0 || sm.segments[len(sm.segments)-1].position >= sm.segmentSize {
		return sm.createNewSegment()
	}
	return sm.segments[len(sm.segments)-1], nil
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
			sm.compactSegments()
		case <-sm.compactionCh:
			sm.compactSegments()
		}
	}
}

// compactSegments merges small segments into larger ones
func (sm *SegmentManager) compactSegments() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// TODO: Implement segment compaction logic
	// 1. Identify segments that can be merged
	// 2. Create new segments with merged data
	// 3. Update segment list
	// 4. Remove old segments
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
