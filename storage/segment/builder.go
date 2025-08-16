package segment

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// Constants defining the on-disk segment format.
const (
	// Header layout (fixed 18 bytes + key)
	//  0..7  : uint64 value length
	//  8..11 : uint32 key length
	// 12..15 : uint32 CRC32 checksum of value bytes
	// 16..17 : uint16 header format version
	// 18..N  : key bytes
	ValueHeaderSize = 18

	// Footer layout (20 bytes)
	//  0..5  : ASCII magic "SEGEOF"
	//  6..7  : ASCII version "01", "02", ...
	//  8..11 : uint32 entries in segment
	// 12..19 : uint64 total value bytes
	SegmentFooterMagicPrefix = "SEGEOF" // First 6 bytes – static prefix
	SegmentFooterSize        = 20

	// Segment versioning constants.
	CurrentSegmentVersion = 1 // Increment when format changes

	// Current header version – increment when ValueHeader layout changes.
	CurrentValueHeaderVersion = 1
)

// BuildValueHeader returns a header buffer for the given key and value length.  The
// caller owns the returned slice.
func BuildValueHeader(key string, valueLen int64, checksum uint32, version uint16) []byte {
	hdr := make([]byte, ValueHeaderSize+len(key))
	binary.BigEndian.PutUint64(hdr[0:8], uint64(valueLen))
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(key)))
	binary.BigEndian.PutUint32(hdr[12:16], checksum)
	binary.BigEndian.PutUint16(hdr[16:18], version)
	copy(hdr[18:], []byte(key))
	return hdr
}

// ReadValueHeader parses the header at the beginning of a segment/raw file and
// returns the value length, total header size and key length.
// It expects the file cursor to be at the beginning of the file or supports
// random access via Pread.
func ReadValueHeader(f *os.File) (valueLen int64, headerSize int64, keyLen int64, version uint16, checksum uint32, err error) {
	var fixed [ValueHeaderSize]byte
	n, err := unix.Pread(int(f.Fd()), fixed[:], 0)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}

	// Check if we read less than expected (EOF)
	if n < ValueHeaderSize {
		return 0, 0, 0, 0, 0, io.EOF
	}

	// Check if the header is all zeros (uninitialized sparse file region)
	isZero := true
	for _, b := range fixed {
		if b != 0 {
			isZero = false
			break
		}
	}
	if isZero {
		return 0, 0, 0, 0, 0, io.EOF
	}

	valueLen = int64(binary.BigEndian.Uint64(fixed[0:8]))
	keyLen = int64(binary.BigEndian.Uint32(fixed[8:12]))
	checksum = binary.BigEndian.Uint32(fixed[12:16])
	version = binary.BigEndian.Uint16(fixed[16:18])

	// Validate header fields for reasonable values
	// Key length should be positive
	if keyLen <= 0 {
		return 0, 0, 0, 0, 0, errors.New("invalid key length in header")
	}

	// Value length should be positive
	if valueLen <= 0 {
		return 0, 0, 0, 0, 0, errors.New("invalid value length in header")
	}

	// Version should be a known version
	if version <= 0 || version > CurrentValueHeaderVersion {
		return 0, 0, 0, 0, 0, errors.New("invalid header version")
	}

	headerSize = int64(ValueHeaderSize) + keyLen
	return
}

// ReadValueHeaderAt parses the header at a specific offset in a segment file and
// returns the value length, total header size and key length.
// This function uses pread to read from the specified offset without modifying
// the file's current position, making it safe for concurrent reads.
func ReadValueHeaderAt(f *os.File, offset int64) (valueLen int64, headerSize int64, keyLen int64, version uint16, checksum uint32, err error) {
	var fixed [ValueHeaderSize]byte
	n, err := unix.Pread(int(f.Fd()), fixed[:], offset)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}

	// Check if we read less than expected (EOF)
	if n < ValueHeaderSize {
		return 0, 0, 0, 0, 0, io.EOF
	}

	// Check if the header is all zeros (uninitialized sparse file region)
	isZero := true
	for _, b := range fixed {
		if b != 0 {
			isZero = false
			break
		}
	}
	if isZero {
		return 0, 0, 0, 0, 0, io.EOF
	}

	valueLen = int64(binary.BigEndian.Uint64(fixed[0:8]))
	keyLen = int64(binary.BigEndian.Uint32(fixed[8:12]))
	checksum = binary.BigEndian.Uint32(fixed[12:16])
	version = binary.BigEndian.Uint16(fixed[16:18])

	// Validate header fields for reasonable values
	// Key length should be positive
	if keyLen <= 0 {
		return 0, 0, 0, 0, 0, errors.New("invalid key length in header")
	}

	// Value length should be positive
	if valueLen <= 0 {
		return 0, 0, 0, 0, 0, errors.New("invalid value length in header")
	}

	// Version should be a known version
	if version <= 0 || version > CurrentValueHeaderVersion {
		return 0, 0, 0, 0, 0, errors.New("invalid header version")
	}

	headerSize = int64(ValueHeaderSize) + keyLen
	return
}

// CalculateValueHeaderSize calculates the size of the header for a given key.
func CalculateValueHeaderSize(key string) int64 {
	return int64(ValueHeaderSize + len(key))
}

// UpdateValueHeader updates both value length and checksum in the supplied
// header slice.
func UpdateValueHeader(header []byte, valueLen int64, checksum uint32) []byte {
	binary.BigEndian.PutUint64(header[0:8], uint64(valueLen))
	binary.BigEndian.PutUint32(header[12:16], checksum)
	return header
}

// BuildSegmentFooterWithVersion builds a segment footer that encodes the given
// version, entry count and total dataBytes. The returned slice is owned by the
// caller.
func BuildSegmentFooterWithVersion(version int, entries uint32, dataBytes int64) []byte {
	ftr := make([]byte, SegmentFooterSize)

	// 0..5 – static prefix "SEGEOF"
	copy(ftr[0:6], []byte(SegmentFooterMagicPrefix))

	// 6..7 – version encoded as zero-padded decimal ("01", "02", ... up to 99)
	if version < 0 || version > 99 {
		// fall back to 00 for unsupported values (should not happen)
		copy(ftr[6:8], []byte{'0', '0'})
	} else {
		tens := byte('0' + (version/10)%10)
		ones := byte('0' + version%10)
		ftr[6] = tens
		ftr[7] = ones
	}

	// 8..11 – entry count (uint32)
	binary.BigEndian.PutUint32(ftr[8:12], entries)

	// 12..19 – total value bytes (uint64)
	binary.BigEndian.PutUint64(ftr[12:20], uint64(dataBytes))

	return ftr
}

// ParseSegmentFooter parses a 20-byte segment footer and returns the encoded
// version, entry count and total dataBytes. ok will be false when the footer
// does not start with the expected magic prefix.
func ParseSegmentFooter(footer []byte) (version int, entries uint32, dataBytes int64, ok bool) {
	if len(footer) < SegmentFooterSize {
		return 0, 0, 0, false
	}

	// Verify static prefix first.
	if string(footer[0:6]) != SegmentFooterMagicPrefix {
		return 0, 0, 0, false
	}

	// Extract version – ascii digits.
	v1, v2 := footer[6], footer[7]
	if v1 < '0' || v1 > '9' || v2 < '0' || v2 > '9' {
		return 0, 0, 0, false
	}
	version = int(v1-'0')*10 + int(v2-'0')

	entries = binary.BigEndian.Uint32(footer[8:12])
	dataBytes = int64(binary.BigEndian.Uint64(footer[12:20]))
	ok = true
	return
}
