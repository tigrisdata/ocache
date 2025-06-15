package segment

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/tigrisdata/cache_service/server/storage/bufferpool"
	"github.com/tigrisdata/cache_service/server/utils"

	zlog "github.com/rs/zerolog/log"
)

// RawFileManager manages all raw files in the raw directory
type RawFileManager struct {
	rawFilesPath string   // path to the raw files directory
	fileLocks    sync.Map // map of mutexes for individual files
	segmentSize  int64    // configured segment size used for promotion heuristics
}

// rawFileReadCloser wraps a Reader and closes the underlying file while
// releasing the per-file read lock when Close is invoked.
type rawFileReadCloser struct {
	io.Reader
	f  *os.File
	mu *sync.RWMutex
}

func (rc *rawFileReadCloser) Close() error {
	rc.mu.RUnlock()
	return rc.f.Close()
}

// NewRawFileManager creates a new RawFileManager for managing raw files. The
// segmentSize parameter (borrowed from SegmentManager) is used later to decide
// whether a raw file can be promoted to a standalone segment without copying.
func NewRawFileManager(rawFilesPath string, segmentSize int64) (*RawFileManager, error) {
	// Create the raw files directory if it doesn't exist
	if err := os.MkdirAll(rawFilesPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create raw files directory: %w", err)
	}

	return &RawFileManager{
		rawFilesPath: rawFilesPath,
		segmentSize:  segmentSize,
	}, nil
}

// getFileLock returns an RWMutex for the given key, creating it if it doesn't exist.
// An RWMutex allows multiple concurrent readers while still giving exclusive
// access to writers (Write/Delete), which is exactly the behaviour we need for
// raw files.
func (rw *RawFileManager) getFileLock(key string) *sync.RWMutex {
	lock, _ := rw.fileLocks.LoadOrStore(key, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

// Write writes a value to a raw file for the given key
func (rw *RawFileManager) Write(key string, reader io.Reader) (string, error) {
	// Create a new file for this key. We open it RDWR so we can patch the
	// header after the payload is streamed.
	filePath := filepath.Join(rw.rawFilesPath, key)

	// Get file-specific lock (exclusive for writers)
	fileLock := rw.getFileLock(filePath)
	fileLock.Lock()
	defer fileLock.Unlock()

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return "", utils.WrapError("failed to create raw file for key", key, err)
	}
	defer file.Close()

	// --------------------------------------------------------------------
	// 1. Write provisional header (valueLen = 0 for now)
	// --------------------------------------------------------------------
	header := BuildHeader(key, 0) // valueLen unknown yet
	if _, err := file.Write(header); err != nil {
		os.Remove(filePath)
		return "", utils.WrapError("write header", key, err)
	}

	// --------------------------------------------------------------------
	// 2. Stream payload directly from reader → file with pooled buffer
	// --------------------------------------------------------------------
	buf, release := bufferpool.AcquireBuffer(1 << 20) // 1 MiB
	defer release()

	bytesWritten, err := io.CopyBuffer(file, reader, buf)
	if err != nil {
		os.Remove(filePath)
		return "", utils.WrapError("copy payload", key, err)
	}

	// --------------------------------------------------------------------
	// 3. Patch header with actual value length
	// --------------------------------------------------------------------
	var lenBuf [4]byte
	if bytesWritten > (1<<32)-1 {
		zlog.Warn().Int64("bytes", bytesWritten).Msg("value exceeds 4GiB, truncating length for header")
	}
	binary.BigEndian.PutUint32(lenBuf[:], uint32(bytesWritten))
	if _, err := file.WriteAt(lenBuf[:], 0); err != nil {
		os.Remove(filePath)
		return "", utils.WrapError("patch header", key, err)
	}

	// Sync to ensure durability of header + data
	_ = file.Sync()

	// --------------------------------------------------------------------
	// 4. Record entry in Compactor for future compaction
	// --------------------------------------------------------------------
	RecordEntryForCompaction(key, filePath, bytesWritten)

	zlog.Debug().Str("key", key).Str("path", filePath).Int64("bytes", bytesWritten).Msg("rawWriter: completed write")
	return filePath, nil
}

// Read reads a value from a raw file for the given key
func (rw *RawFileManager) Read(filePath string) (io.ReadCloser, error) {
	// Get file-specific lock (shared for readers)
	fileLock := rw.getFileLock(filePath)
	fileLock.RLock()
	// We will release the lock in the returned reader's Close implementation

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
	if err != nil {
		fileLock.RUnlock()
		if os.IsNotExist(err) {
			zlog.Warn().Str("path", filePath).Msg("rawWriter: raw file not found")
			return nil, utils.WrapError("raw file not found", filePath, nil)
		}
		return nil, utils.WrapError("failed to open raw file", filePath, err)
	}

	// Read header to skip it so callers receive only the value bytes.
	valueLen, _, keyLen, err := ReadHeader(file)
	if err != nil {
		fileLock.RUnlock()
		zlog.Debug().Err(err).Str("path", filePath).Msg("rawWriter: returning full raw file (ReadHeader failed)")
		return file, nil
	}

	// If the value length is 0, the file is empty and we can return the whole file.
	if valueLen <= 0 {
		fileLock.RUnlock()
		return file, nil
	}

	headerSize := int64(HeaderSize) + keyLen
	section := io.NewSectionReader(file, headerSize, valueLen)

	// Wrap SectionReader so Close unlocks & closes.
	return &rawFileReadCloser{Reader: section, f: file, mu: fileLock}, nil
}

// Delete removes a raw file for the given key
func (rw *RawFileManager) Remove(filePath string) error {
	// Get file-specific lock
	fileLock := rw.getFileLock(filePath)
	fileLock.Lock()
	defer fileLock.Unlock()

	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			zlog.Debug().Str("path", filePath).Msg("rawWriter: file already deleted")
			return nil
		}
		zlog.Error().Err(err).Str("path", filePath).Msg("rawWriter: failed to delete raw file")
		return utils.WrapError("failed to delete raw file", filePath, err)
	}

	// Remove the lock from the map
	rw.fileLocks.Delete(filePath)
	zlog.Debug().Str("path", filePath).Msg("rawWriter: deleted raw file")
	return nil
}
