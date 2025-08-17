package segment

import (
	"os"
	"sync"
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
// Returns true if successful, false if already reserved
func (s *Segment) Reserve(callerID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservedBy != "" && s.reservedBy != callerID {
		return false // Already reserved by someone else
	}
	s.reservedBy = callerID
	return true
}

// Release releases the reservation on the segment
func (s *Segment) Release(callerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservedBy == callerID {
		s.reservedBy = ""
	}
}

// GetReservedBy returns who has reserved this segment (empty string if not reserved)
func (s *Segment) GetReservedBy() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reservedBy
}

// NewSegment creates a new segment with the given path and size.
func NewSegment(path string, entries uint32, dataBytes int64, size int64, maxSupportedSize int64) *Segment {
	return &Segment{path: path, entries: entries, dataBytes: dataBytes, size: size, version: CurrentSegmentVersion, maxSupportedSize: maxSupportedSize}
}
