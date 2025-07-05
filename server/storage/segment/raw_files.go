package segment

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/tigrisdata/ocache/server/storage/bufferpool"
	"github.com/tigrisdata/ocache/server/storage/fd"
	"github.com/tigrisdata/ocache/server/utils"

	zlog "github.com/rs/zerolog/log"
)

// Default maximum number of open file descriptors kept in fdCache before new
// acquisitions stop being cached. Chosen conservatively; can be overridden by
// updating RawFileManager.maxFdCache after construction if needed.
const defaultFdCacheCapacity = 1024

// rawFileReadCloser wraps a Reader and closes the underlying file while
// releasing the per-file read lock when Close is invoked.
type rawFileReadCloser struct {
	io.Reader
	onClose func()
}

func (rc *rawFileReadCloser) Close() error {
	if rc.onClose != nil {
		rc.onClose()
	}
	return nil
}

// headerMeta caches parsed header information so we don't have to read the
// first 20 bytes on every call.
type headerMeta struct {
	valLen     int64
	checksum   uint32
	headerSize int64
	version    uint16
}

// RawFileManager manages all raw files in the raw directory
type RawFileManager struct {
	rawFilesPath string   // path to the raw files directory
	fileLocks    sync.Map // map of mutexes for individual files

	fdCache *fd.FdCache // descriptor cache shared across readers

	headerCache sync.Map // path -> headerMeta
}

// NewRawFileManager creates a new RawFileManager for managing raw files.
func NewRawFileManager(rawFilesPath string) (*RawFileManager, error) {
	// Create the raw files directory if it doesn't exist
	if err := os.MkdirAll(rawFilesPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create raw files directory: %w", err)
	}

	rwm := &RawFileManager{
		rawFilesPath: rawFilesPath,
	}

	// Instantiate a bounded descriptor cache that reuses RawFileManager's lock provider
	rwm.fdCache = fd.GetFdCache()

	return rwm, nil
}

// Write writes a value to a raw file for the given key
func (rw *RawFileManager) Write(key string, reader io.Reader) (string, error) {
	// Create a new file for this key. We open it RDWR so we can patch the
	// header after the payload is streamed.
	random := uuid.New().String()
	filePath := filepath.Join(rw.rawFilesPath, random)

	// Get file-specific lock (exclusive for writers). As the file does not exist yet, we need to
	// take the lock bypassing the fdCache.
	fileLock := rw.fdCache.GetFileLock(filePath)
	fileLock.Lock()
	defer fileLock.Unlock()

	// --------------------------------------------------------------------
	// 1. Record entry in Compactor for future compaction. We do this first
	//    so that there are no orphaned raw files.
	// --------------------------------------------------------------------
	RecordEntryForCompaction(key, filePath)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return "", utils.WrapError("failed to create raw file for key", key, err)
	}
	defer file.Close()

	// --------------------------------------------------------------------
	// 2. Write provisional header (valueLen = 0 for now)
	// --------------------------------------------------------------------
	header := BuildValueHeader(key, 0, 0, CurrentValueHeaderVersion) // valueLen unknown yet
	if _, err := file.Write(header); err != nil {
		os.Remove(filePath)
		return "", utils.WrapError("write header", key, err)
	}

	// --------------------------------------------------------------------
	// 3. Stream payload directly from reader → file with pooled buffer
	// --------------------------------------------------------------------
	buf, release := bufferpool.AcquireBuffer(1 << 20) // 1 MiB
	defer release()

	hash := crc32.NewIEEE()
	mw := io.MultiWriter(file, hash)

	bytesWritten, err := io.CopyBuffer(mw, reader, buf)
	if err != nil {
		os.Remove(filePath)
		return "", utils.WrapError("copy payload", key, err)
	}

	checksum := hash.Sum32()

	// --------------------------------------------------------------------
	// 4. Patch header with actual value length
	// --------------------------------------------------------------------
	header = UpdateValueHeader(header, bytesWritten, checksum)
	if _, err := file.WriteAt(header, 0); err != nil {
		os.Remove(filePath)
		return "", utils.WrapError("patch header", key, err)
	}

	// Sync to ensure durability of header + data
	_ = file.Sync()

	zlog.Debug().Str("key", key).Str("path", filePath).Int64("bytes", bytesWritten).Msg("rawWriter: completed write")
	return filePath, nil
}

// Read reads a value from a raw file for the given key
func (rw *RawFileManager) Read(filePath string) (io.ReadCloser, error) {
	e, err := rw.fdCache.Acquire(filePath)
	if err != nil {
		return nil, err
	}

	// Acquire shared read lock to protect against concurrent writers.
	e.RLock()

	// Attempt fast-path: header already cached.
	var (
		valLen     int64
		headerSize int64
		version    uint16
		checksum   uint32
	)

	if v, ok := rw.headerCache.Load(filePath); ok {
		hm := v.(headerMeta)
		valLen = hm.valLen
		headerSize = hm.headerSize
	} else {
		// Slow path: parse header and cache it.
		valLen, headerSize, _, version, checksum, err = ReadValueHeader(e.File())
		if err != nil {
			e.RUnlock()
			rw.fdCache.Release(filePath, e)
			return nil, err
		}
		rw.headerCache.Store(filePath, headerMeta{valLen: valLen, checksum: checksum, headerSize: headerSize, version: version})
	}

	reader := io.NewSectionReader(e.File(), headerSize, valLen)

	return &rawFileReadCloser{
		Reader: reader,
		onClose: func() {
			// Release lock & cached FD when caller is done.
			e.RUnlock()
			rw.fdCache.Release(filePath, e)
		},
	}, nil
}

// Delete removes a raw file for the given key
func (rw *RawFileManager) Remove(filePath string) error {
	// Get file-specific lock
	e, err := rw.fdCache.Acquire(filePath)
	if err != nil {
		return err
	}

	// Take exclusive lock to prevent concurrent writes.
	e.Lock()
	defer e.Unlock()

	if err := os.Remove(filePath); err != nil {
		if !os.IsNotExist(err) {
			zlog.Error().Err(err).Str("path", filePath).Msg("rawWriter: failed to delete raw file")
			return utils.WrapError("failed to delete raw file", filePath, err)
		}

		zlog.Debug().Str("path", filePath).Msg("rawWriter: file already deleted")
	}

	// Evict any cached file descriptor for this path.
	rw.fdCache.CleanUp(filePath)

	// Clean up any cached header metadata for this path.
	rw.CleanUp(filePath)

	zlog.Debug().Str("path", filePath).Msg("rawWriter: deleted raw file")
	return nil
}

// CleanUp removes any cached header metadata for the given path.
func (rw *RawFileManager) CleanUp(filePath string) {
	// Remove any cached header metadata for this path.
	rw.headerCache.Delete(filePath)
}
