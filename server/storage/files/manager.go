package files

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/tigrisdata/ocache/server/storage/bufferpool"
	"github.com/tigrisdata/ocache/server/storage/fd"
	"github.com/tigrisdata/ocache/server/utils"

	zlog "github.com/rs/zerolog/log"
)

// fileReadCloser wraps a Reader and closes the underlying file while
// releasing the per-file read lock when Close is invoked.
type fileReadCloser struct {
	io.Reader
	onClose func()
}

func (rc *fileReadCloser) Close() error {
	if rc.onClose != nil {
		rc.onClose()
	}
	return nil
}

// FileManager manages all files in the files directory
type FileManager struct {
	filesPath string // path to the files directory

	fdCache *fd.FdCache // descriptor cache shared across readers
}

// NewFileManager creates a new FileManager for managing files.
func NewFileManager(basePath string) (*FileManager, error) {
	filesPath := filepath.Join(basePath, "files")

	zlog.Info().Str("filesPath", filesPath).Msg("creating files manager")

	// Create the files directory if it doesn't exist
	if err := os.MkdirAll(filesPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create files directory: %w", err)
	}

	fm := &FileManager{
		filesPath: filesPath,
	}

	// Instantiate a bounded descriptor cache that reuses FileManager's lock provider
	fm.fdCache = fd.GetFdCache()

	return fm, nil
}

// Write writes a value to a file for the given key
// Returns the file path, checksum, and number of bytes written.
func (fm *FileManager) Write(key string, reader io.Reader) (string, uint32, int64, error) {
	// Create a new file for this key. We open it RDWR so we can patch the
	// header after the payload is streamed.
	random := uuid.New().String()
	filePath := filepath.Join(fm.filesPath, random)

	// Get file-specific lock (exclusive for writers). As the file does not exist yet, we need to
	// take the lock bypassing the fdCache.
	fileLock := fm.fdCache.GetFileLock(filePath)
	fileLock.Lock()
	defer fileLock.Unlock()

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return "", 0, 0, utils.WrapError("failed to create file for key", key, err)
	}
	defer file.Close()

	// Stream payload directly from reader → file with pooled buffer
	buf, release := bufferpool.AcquireBuffer(1 << 20) // 1 MiB
	defer release()

	hash := crc32.NewIEEE()
	mw := io.MultiWriter(file, hash)

	bytesWritten, err := io.CopyBuffer(mw, reader, buf)
	if err != nil {
		os.Remove(filePath)
		return "", 0, 0, utils.WrapError("copy payload", key, err)
	}

	checksum := hash.Sum32()

	// Sync to ensure durability of data
	_ = file.Sync()

	zlog.Debug().Str("key", key).Str("path", filePath).Int64("bytes", bytesWritten).Msg("fileManager: completed write")
	return filePath, checksum, bytesWritten, nil
}

// Read reads a value from a file for the given key
func (fm *FileManager) Read(filePath string, length int64) (io.ReadCloser, error) {
	if filePath == "" || length <= 0 {
		return nil, fmt.Errorf("invalid file path or length: path=%s, length=%d", filePath, length)
	}

	e, err := fm.fdCache.Acquire(filePath)
	if err != nil {
		return nil, err
	}

	// Acquire shared read lock to protect against concurrent writers.
	e.RLock()

	// Create a new SectionReader that reads from the file starting at offset 0
	// and reads up to length bytes.
	reader := io.NewSectionReader(e.File(), 0, length)

	return &fileReadCloser{
		Reader: reader,
		onClose: func() {
			// Release lock & cached FD when caller is done.
			e.RUnlock()
			fm.fdCache.Release(filePath, e)
		},
	}, nil
}

// Delete removes a file for the given key
func (fm *FileManager) Remove(filePath string) error {
	// Get file-specific lock (use GetFileLock, not Acquire, to avoid opening the file)
	fileLock := fm.fdCache.GetFileLock(filePath)

	// Try to acquire lock without blocking
	// If we can't get the lock immediately, the file is being read
	if !fileLock.TryLock() {
		zlog.Warn().Str("path", filePath).Msg("fileManager: file is currently being read, skipping deletion")
		// Return a specific error that the compactor can recognize
		return fmt.Errorf("file is locked for reading: %s", filePath)
	}

	defer fileLock.Unlock()

	if err := os.Remove(filePath); err != nil {
		if !os.IsNotExist(err) {
			zlog.Error().Err(err).Str("path", filePath).Msg("fileManager: failed to delete file")
			return utils.WrapError("failed to delete file", filePath, err)
		}

		zlog.Debug().Str("path", filePath).Msg("fileManager: file already deleted")
	}

	// Evict any cached file descriptor for this path.
	fm.fdCache.CleanUp(filePath)

	zlog.Debug().Str("path", filePath).Msg("fileManager: deleted file")
	return nil
}

// TryRemove attempts to remove a file but returns false if it's currently locked for reading.
// This is useful for the compactor which can defer deletion of locked files.
func (fm *FileManager) TryRemove(filePath string) (bool, error) {
	err := fm.Remove(filePath)
	if err != nil {
		// Check if it's a lock error
		if err.Error() == fmt.Sprintf("file is locked for reading: %s", filePath) {
			return false, nil // File is locked but no actual error
		}
		return false, err // Real error occurred
	}
	return true, nil // Successfully deleted
}
