package segment

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/tigrisdata/ocache/storage/utils"
)

// Segment is a file on disk that contains key/value pairs.
type Segment struct {
	path string
	size int64
	file *os.File
	mu   sync.RWMutex

	// Statistics
	numEntries uint32 // number of key/value pairs stored in this segment
	dataBytes  int64  // total number of bytes occupied by value payloads (not counting headers)

	// Format version of this segment (derived from footer when closed or set when created).
	version int

	// Maximum size of the segment.
	maxSupportedSize int64

	// Reservation tracking - which caller has reserved this segment for exclusive use
	// Empty string means not reserved
	reservedBy string
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

// GetNumEntries returns the number of entries in the segment.
func (s *Segment) GetNumEntries() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.numEntries
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
	s.numEntries++
}

// IncrementDataBytes increments the data bytes count (must be called while holding lock)
func (s *Segment) IncrementDataBytes(delta int64) {
	s.dataBytes += delta
}

// GetSizeUnsafe returns the size without locking (must be called while holding lock)
func (s *Segment) GetSizeUnsafe() int64 {
	return s.size
}

// IsReservedBy checks if the segment is reserved by the given caller
func (s *Segment) IsReservedBy(callerID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reservedBy == callerID
}

// IsReserved checks if the segment is reserved by anyone
func (s *Segment) IsReserved() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reservedBy != ""
}

// Reserve attempts to reserve the segment for exclusive use by the caller
// Returns true if successful, false if already reserved or if segment is closed
func (s *Segment) Reserve(callerID string) bool {
	if callerID == "" {
		return false // Cannot reserve with empty callerID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Cannot reserve a closed/finalized segment
	if s.file == nil {
		return false
	}
	if s.reservedBy != "" && s.reservedBy != callerID {
		return false // Already reserved by someone else
	}
	s.reservedBy = callerID
	return true
}

// Release releases the reservation on the segment
func (s *Segment) Release(callerID string) error {
	if callerID == "" {
		return fmt.Errorf("callerID cannot be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservedBy == callerID {
		s.reservedBy = ""
	}

	return nil
}

// GetReservedBy returns who has reserved this segment (empty string if not reserved)
func (s *Segment) GetReservedBy() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reservedBy
}

// Finalize finalizes the segment by writing the footer and closing the file.
func (s *Segment) Finalize() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil // already closed
	}

	// Build footer [magic|entries|bytes]
	footer := BuildSegmentFooterWithVersion(s.version, s.numEntries, s.dataBytes)

	if _, err := s.file.Write(footer); err != nil {
		s.mu.Unlock()
		return utils.WrapError("failed to write segment footer", s.path, err)
	}
	s.size += int64(len(footer))
	// Shrink pre-allocated file to actual used size
	if err := s.file.Truncate(s.size); err != nil {
		s.mu.Unlock()
		return utils.WrapError("truncate segment", s.path, err)
	}

	// Flush and close the R/W file descriptor
	if err := s.file.Sync(); err != nil {
		s.mu.Unlock()
		return utils.WrapError("failed to sync segment", s.path, err)
	}
	if err := s.file.Close(); err != nil {
		s.mu.Unlock()
		return utils.WrapError("failed to close segment", s.path, err)
	}

	// Clear pointer – closed segments will use fdCache for reads.
	s.file = nil

	// Clear any reservation on this segment
	s.reservedBy = ""

	// Release the segment lock before acquiring manager lock to prevent deadlock
	s.mu.Unlock()

	return nil
}

// Sync syncs the segment to disk. This is a no-op if the segment is not open.
func (s *Segment) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.file == nil {
		return nil
	}

	// Flush file contents to disk
	err := s.file.Sync()
	if err != nil {
		return utils.WrapError("failed to sync current segment", s.path, err)
	}
	return nil
}

// NewSegment creates a new segment with the given path and size.
func NewSegment(path string, entries uint32, dataBytes int64, size int64, maxSupportedSize int64) *Segment {
	return &Segment{path: path, numEntries: entries, dataBytes: dataBytes, size: size, version: CurrentSegmentVersion, maxSupportedSize: maxSupportedSize}
}

// NewSegmentWithReservation creates a new segment with the given path and size, atomically reserved for the caller.
// This ensures the segment is created with a reservation in place, preventing race conditions.
func NewSegmentWithReservation(path string, entries uint32, dataBytes int64, size int64, maxSupportedSize int64, callerID string) *Segment {
	return &Segment{
		path:             path,
		numEntries:       entries,
		dataBytes:        dataBytes,
		size:             size,
		version:          CurrentSegmentVersion,
		maxSupportedSize: maxSupportedSize,
		reservedBy:       callerID, // Atomically set reservation during creation
	}
}

// EntryInfo contains information about a single entry in a segment
type EntryInfo struct {
	Key         string
	Offset      int64  // Offset in the segment file where this entry starts
	HeaderSize  int64  // Size of the header (including key)
	ValueLength int64  // Length of the value
	Checksum    uint32 // CRC32 checksum of the value
	Version     uint16 // Header format version
}

// Iterator provides sequential access to entries in a segment
type Iterator struct {
	segment    *Segment
	file       *os.File
	currentPos int64
	fileSize   int64
	mu         sync.Mutex
	lastError  error
}

// NewIterator creates a new iterator for the segment
// The caller is responsible for closing the iterator when done
func (s *Segment) NewIterator(file *os.File) (*Iterator, error) {
	if file == nil {
		return nil, io.ErrClosedPipe
	}

	// Get file size to know when to stop iterating
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &Iterator{
		segment:    s,
		file:       file,
		currentPos: 0,
		fileSize:   fileInfo.Size(),
	}, nil
}

// Next advances the iterator to the next entry and returns its information
// Returns io.EOF when there are no more entries
func (it *Iterator) Next() (*EntryInfo, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Check if we've reached the end of the segment (accounting for footer)
	if it.currentPos >= it.fileSize-int64(SegmentFooterSize) {
		return nil, io.EOF
	}

	// Read the entry header at current position
	valLen, headerSize, keyLen, version, checksum, err := ReadValueHeaderAt(it.file, it.currentPos)
	if err != nil {
		it.lastError = err
		return nil, err
	}

	// Read the key
	keyBuf := make([]byte, keyLen)
	if _, err := it.file.ReadAt(keyBuf, it.currentPos+ValueHeaderSize); err != nil {
		it.lastError = err
		return nil, err
	}

	entry := &EntryInfo{
		Key:         string(keyBuf),
		Offset:      it.currentPos,
		HeaderSize:  headerSize,
		ValueLength: valLen,
		Checksum:    checksum,
		Version:     version,
	}

	// Move to the next entry
	it.currentPos += headerSize + valLen

	return entry, nil
}

// Reset resets the iterator to the beginning of the segment
func (it *Iterator) Reset() {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.currentPos = 0
	it.lastError = nil
}

// CurrentPosition returns the current position in the segment file
func (it *Iterator) CurrentPosition() int64 {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.currentPos
}

// LastError returns the last error encountered during iteration
func (it *Iterator) LastError() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.lastError
}
