package segment

import (
	"encoding/binary"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Constants defining the on-disk segment format.
const (
	// Header layout (fixed 16 bytes + key)
	//  0..7  : uint64 value length
	//  8..15 : int64  nano timestamp
	// 16..19 : uint32 key length
	// 20..N  : key bytes
	ValueHeaderSize = 20

	// Footer layout (20 bytes)
	//  0..5  : ASCII magic "SEGEOF"
	//  6..7  : ASCII version "01", "02", ...
	//  8..11 : uint32 entries in segment
	// 12..19 : uint64 total value bytes
	SegmentFooterMagicPrefix = "SEGEOF" // First 6 bytes – static prefix
	SegmentFooterSize        = 20

	// Segment versioning constants.
	CurrentSegmentVersion = 1 // Increment when format changes
)

// BuildValueHeader returns a header buffer for the given key and value length.  The
// caller owns the returned slice.
func BuildValueHeader(key string, valueLen int64) []byte {
	hdr := make([]byte, ValueHeaderSize+len(key))
	binary.BigEndian.PutUint64(hdr[0:8], uint64(valueLen))
	binary.BigEndian.PutUint64(hdr[8:16], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(key)))
	copy(hdr[20:], []byte(key))
	return hdr
}

// ReadValueHeader parses the header at the beginning of a segment/raw file and
// returns the value length, total header size and key length.
// It expects the file cursor to be at the beginning of the file or supports
// random access via ReadAt.
func ReadValueHeader(f *os.File) (valueLen int64, headerSize int64, keyLen int64, err error) {
	var fixed [ValueHeaderSize]byte
	if _, err = unix.Pread(int(f.Fd()), fixed[:], 0); err != nil {
		return 0, 0, 0, err
	}
	valueLen = int64(binary.BigEndian.Uint64(fixed[0:8]))
	keyLen = int64(binary.BigEndian.Uint32(fixed[16:20]))
	headerSize = int64(ValueHeaderSize) + keyLen
	return
}

// CalculateValueHeaderSize calculates the size of the header for a given key.
func CalculateValueHeaderSize(key string) int64 {
	return int64(ValueHeaderSize + len(key))
}

// UpdateValueHeaderValueLen updates the value length in the header of a segment/raw
// file.
func UpdateValueHeaderValueLen(header []byte, valueLen int64) []byte {
	binary.BigEndian.PutUint64(header[0:8], uint64(valueLen))
	return header
}

// BuildSegmentFooterWithVersion builds a segment footer that encodes the given
// version, entry count and total dataBytes. The returned slice is owned by the
// caller.
func BuildSegmentFooterWithVersion(version int, entries uint32, dataBytes int64) []byte {
	ftr := make([]byte, SegmentFooterSize)

	// 0..5 – static prefix "SEGEOF"
	copy(ftr[0:6], []byte(SegmentFooterMagicPrefix))

	// 6..7 – version encoded as zero-padded decimal ("01", "02", ...)
	if version < 10 { // 1 digit
		ver := []byte{byte('0' + version)}
		copy(ftr[6:8], ver)
	} else {
		// 2 digits
		ver := []byte{
			byte('0' + (version/10)%10),
			byte('0' + version%10),
		}
		copy(ftr[6:8], ver)
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
