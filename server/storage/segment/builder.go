package segment

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Constants defining the on-disk segment format.
const (
	// Header layout (fixed 16 bytes + key)
	//  0..3  : uint32 value length
	//  4..11 : int64  nano timestamp
	// 12..15 : uint32 key length
	// 16..N  : key bytes
	HeaderSize = 16

	// Footer layout (20 bytes)
	//  0..7  : ASCII magic "SEGEOF01"
	//  8..11 : uint32 entries in segment
	// 12..19 : uint64 total value bytes
	SegmentFooterMagic = "SEGEOF01"
	SegmentFooterSize  = 8 + 4 + 8
)

// BuildHeader returns a header buffer for the given key and value length.  The
// caller owns the returned slice.
func BuildHeader(key string, valueLen int64) []byte {
	hdr := make([]byte, HeaderSize+len(key))
	binary.BigEndian.PutUint32(hdr[0:4], uint32(valueLen))
	binary.BigEndian.PutUint64(hdr[4:12], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(hdr[12:16], uint32(len(key)))
	copy(hdr[16:], []byte(key))
	return hdr
}

// BuildFooter returns a footer buffer for the given entry count and total
// payload bytes.
func BuildFooter(entries uint32, dataBytes int64) []byte {
	ftr := make([]byte, SegmentFooterSize)
	copy(ftr[0:8], []byte(SegmentFooterMagic))
	binary.BigEndian.PutUint32(ftr[8:12], entries)
	binary.BigEndian.PutUint64(ftr[12:20], uint64(dataBytes))
	return ftr
}

// Registry is implemented by segment managers that need to be informed when a
// new segment file is created (e.g. after promotion from a raw file).
type Registry interface {
	RegisterSegment(path string, entries uint32, bytes int64)
}

// ReadHeader parses the header at the beginning of a segment/raw file and
// returns the value length, total header size and key length.
// It expects the file cursor to be at the beginning of the file or supports
// random access via ReadAt.
func ReadHeader(f *os.File) (valueLen int64, headerSize int64, keyLen int64, err error) {
	var fixed [HeaderSize]byte
	if _, err = f.ReadAt(fixed[:], 0); err != nil {
		return 0, 0, 0, err
	}
	valueLen = int64(binary.BigEndian.Uint32(fixed[0:4]))
	keyLen = int64(binary.BigEndian.Uint32(fixed[12:16]))
	headerSize = int64(HeaderSize) + keyLen
	return
}

// PromoteRawFile converts an existing raw file that already contains
// [header|payload] into a fully-fledged single-entry segment by appending the
// footer and atomically renaming the file into destDir.  It returns the new
// segment path, header size (offset of payload) and payload length.
func PromoteRawFile(rawPath, destDir, userKey string, fileSize int64, reg Registry) (string, int64, int64, error) {
	headerSize := int64(HeaderSize + len(userKey))
	valueLen := fileSize - headerSize
	if valueLen <= 0 {
		return "", 0, 0, fmt.Errorf("computed negative value length")
	}

	// Append footer.
	f, err := os.OpenFile(rawPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return "", 0, 0, err
	}
	footer := BuildFooter(1, valueLen)
	if _, err := f.Write(footer); err != nil {
		f.Close()
		return "", 0, 0, err
	}
	_ = f.Sync()
	_ = f.Close()

	// Generate final path.
	newPath := filepath.Join(destDir, fmt.Sprintf("segment_%d.seg", time.Now().UnixNano()))
	if err := os.Rename(rawPath, newPath); err != nil {
		return "", 0, 0, err
	}

	if reg != nil {
		reg.RegisterSegment(newPath, 1, valueLen)
	}
	return newPath, headerSize, valueLen, nil
}
