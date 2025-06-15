package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	pb "github.com/tigrisdata/cache_service/proto"
	"github.com/tigrisdata/cache_service/server/storage/bufferpool"
	"github.com/tigrisdata/cache_service/server/storage/metadata"
	"github.com/tigrisdata/cache_service/server/utils"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

// RawFileManager manages all raw files in the raw directory
type RawFileManager struct {
	rawFilesPath string       // path to the raw files directory
	fileLocks    sync.Map     // map of mutexes for individual files
	db           *grocksdb.DB // RocksDB handle for raw-index entries
	segmentSize  int64        // configured segment size used for promotion heuristics
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
		db:           metadata.GetMetaDB(),
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
	// 4. Record entry in RocksDB raw index for future compaction
	// --------------------------------------------------------------------
	ts := time.Now().UnixNano()
	idxKey := fmt.Sprintf("!raw/%020d|%s", ts, key)
	var size int64
	if fi, err := file.Stat(); err == nil {
		size = fi.Size()
	}
	idxVal := fmt.Sprintf("%s|%d", filePath, size)
	wo := grocksdb.NewDefaultWriteOptions()
	if err := rw.db.Put(wo, []byte(idxKey), []byte(idxVal)); err != nil {
		// Failure to index should not make the write fail
		zlog.Error().Err(err).Str("key", key).Msg("rawWriter: failed to put raw index")
	} else {
		zlog.Debug().Str("key", key).Msg("rawWriter: indexed raw file in RocksDB")
	}

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

// CompactToSegments scans the RocksDB raw index and moves raw files into the
// provided SegmentManager. It deletes index rows after successful copy and
// removes the raw files. The compaction in a single run stops when
// maxBytes bytes have been migrated.
func (rw *RawFileManager) CompactToSegments(sm *Manager, maxBytes int64, flushBytes int64) {
	zlog.Info().Int64("maxBytes", maxBytes).Int64("flushBytes", flushBytes).Msg("rawWriter: starting compaction")

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetPrefixSameAsStart(true)
	it := rw.db.NewIterator(ro)
	defer it.Close()

	wo := grocksdb.NewDefaultWriteOptions()
	batch := grocksdb.NewWriteBatch()
	processed := 0
	var bytesMigrated, bytesToFlush int64

	rawPrefix := []byte("!raw/")
	for it.Seek(rawPrefix); it.ValidForPrefix(rawPrefix); it.Next() {
		k := it.Key().Data()
		v := it.Value().Data()

		userKey, filePath, fileSize, ok := parseRawIndexRow(k, v)
		if !ok {
			continue
		}

		// Load current metadata for the user key
		slice, err := rw.db.Get(ro, []byte(userKey))
		if err != nil {
			zlog.Error().Err(err).Str("key", userKey).Msg("rawWriter compaction: db.Get error")
			continue
		}

		metadataFound := slice.Exists()
		vm := &pb.ValueMessage{}
		if metadataFound {
			if err := proto.Unmarshal(slice.Data(), vm); err != nil {
				metadataFound = false
			}
		}

		var (
			bytesMoved int64
			promoted   bool
			promErr    error
		)
		if metadataFound {
			// Attempt zero-copy promotion first; fall back to copy otherwise.
			promoted, bytesMoved, promErr = rw.promoteLargeRaw(sm, userKey, filePath, fileSize, vm)
			if promErr != nil {
				zlog.Error().Err(promErr).Str("key", userKey).Msg("rawWriter compaction: promotion failed, falling back to copy")
				promoted = false
			}

			if !promoted {
				bytesMoved, promErr = rw.copyRawIntoSegment(sm, userKey, filePath, vm)
				if promErr != nil {
					zlog.Error().Err(promErr).Str("key", userKey).Msg("rawWriter compaction: copy failed")
					slice.Free()
					continue
				}
			}

			// Update metadata if present.
			if data, err := proto.Marshal(vm); err == nil {
				batch.Put([]byte(userKey), data)
			}

		}

		// Regardless of metadata presence, remove the raw file as it is no longer needed.
		rw.Remove(filePath)

		// Release slice as early as possible.
		slice.Free()

		// Remove the index row.
		batch.Delete(k)

		processed++
		bytesMigrated += bytesMoved
		bytesToFlush += bytesMoved

		// Flush intermediate batch when threshold reached
		if bytesToFlush >= flushBytes {
			_ = rw.db.Write(wo, batch)
			batch.Clear()
			bytesToFlush = 0

			zlog.Debug().Int("processed", processed).Int64("bytes", bytesMigrated).Msg("rawWriter compaction: batch flushed")
			if bytesMigrated >= maxBytes {
				break
			}
		}
	}

	// flush any remaining data
	if batch.Count() > 0 {
		_ = rw.db.Write(wo, batch)
	}

	zlog.Info().Int("migrated", processed).Int64("bytes", bytesMigrated).Dur("duration", time.Since(time.Now())).Msg("rawWriter: compaction finished")
}

// parseRawIndexRow extracts the userKey, filePath and size from RocksDB raw index
// key/value pairs. It returns ok=false when the row does not follow the expected
// format so that the caller can skip it quietly.
func parseRawIndexRow(k, v []byte) (userKey, filePath string, fileSize int64, ok bool) {
	// Key format: !raw/<ts>|<userKey>
	pipeIdx := bytes.IndexByte(k, '|')
	if pipeIdx <= 0 {
		return
	}
	userKey = string(k[pipeIdx+1:])

	// Value format: <filePath>|<size>
	parts := bytes.SplitN(v, []byte("|"), 2)
	if len(parts) < 1 {
		return
	}
	filePath = string(parts[0])
	if len(parts) == 2 {
		if sz, err := strconv.ParseInt(string(parts[1]), 10, 64); err == nil {
			fileSize = sz
		}
	}
	ok = true
	return
}

// promoteLargeRaw attempts to convert the raw file into a one-entry segment by
// appending the footer and renaming it. It returns promoted=true on success.
func (rw *RawFileManager) promoteLargeRaw(sm *Manager, userKey, filePath string, fileSize int64, vm *pb.ValueMessage) (promoted bool, valueBytes int64, err error) {
	// Require the file to be sufficiently big to justify promotion.
	if fileSize < rw.segmentSize*9/10 {
		return false, 0, nil // too small – fall back
	}

	// Use helper from segmentfile which also registers the segment.
	newPath, headerSize, valueLen, err := PromoteRawFile(filePath, sm.segmentsPath, userKey, fileSize, sm)
	if err != nil {
		return false, 0, err
	}

	// ValueMessage update.
	vm.RawFilePath = ""
	vm.SegmentPath = newPath
	vm.SegmentOffset = headerSize
	vm.ValueLength = valueLen

	return true, valueLen, nil
}

// copyRawIntoSegment copies the raw file into an open segment using the
// existing segment pipeline and updates the ValueMessage.
func (rw *RawFileManager) copyRawIntoSegment(sm *Manager, userKey, filePath string, vm *pb.ValueMessage) (copiedBytes int64, err error) {
	segPath, segOff, segLen, err := sm.WriteToSegment(userKey, filePath)
	if err != nil {
		return 0, err
	}

	vm.RawFilePath = ""
	vm.SegmentPath = segPath
	vm.SegmentOffset = segOff
	vm.ValueLength = segLen

	return segLen, nil
}
