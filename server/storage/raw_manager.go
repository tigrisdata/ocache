package storage

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/cache_service/proto"
	"google.golang.org/protobuf/proto"
)

// RawFileManager manages all raw files in the raw directory
type RawFileManager struct {
	rawFilesPath string       // path to the raw files directory
	fileLocks    sync.Map     // map of mutexes for individual files
	db           *grocksdb.DB // RocksDB handle for raw-index entries
}

// NewRawFileManager creates a new RawFileManager for managing raw files
func NewRawFileManager(rawFilesPath string) (*RawFileManager, error) {
	// Create the raw files directory if it doesn't exist
	if err := os.MkdirAll(rawFilesPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create raw files directory: %w", err)
	}

	return &RawFileManager{
		rawFilesPath: rawFilesPath,
		db:           getMetaDB(),
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
	// Create a new file for this key
	filePath := filepath.Join(rw.rawFilesPath, key)

	// Get file-specific lock (shared for readers)
	fileLock := rw.getFileLock(filePath)
	fileLock.Lock()
	defer fileLock.Unlock()

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return "", fmt.Errorf("failed to create raw file for key %s: %w", key, err)
	}
	defer file.Close()

	// Write the value using pooled buffer to reduce allocations
	buf := GetBuffer()
	defer PutBuffer(buf)

	var bytesWritten int64
	for {
		n, rerr := reader.Read(buf)
		if n > 0 {
			if _, werr := file.Write(buf[:n]); werr != nil {
				// Clean up the file if write fails
				os.Remove(filePath)
				return "", fmt.Errorf("failed to write value to raw file: %w", werr)
			}
			bytesWritten += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			os.Remove(filePath)
			return "", fmt.Errorf("failed to write value to raw file: %w", rerr)
		}
	}

	// Record entry in RocksDB raw index for future compaction
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
	defer fileLock.RUnlock()

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			zlog.Warn().Str("path", filePath).Msg("rawWriter: raw file not found")
			return nil, fmt.Errorf("raw file not found: %s", filePath)
		}
		return nil, fmt.Errorf("failed to open raw file for key %s: %w", filePath, err)
	}

	zlog.Debug().Str("path", filePath).Msg("rawWriter: opened raw file for reading")
	return file, nil
}

// Delete removes a raw file for the given key
func (rw *RawFileManager) Delete(filePath string) error {
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
		return fmt.Errorf("failed to delete raw file %s: %w", filePath, err)
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
func (rw *RawFileManager) CompactToSegments(sm *SegmentManager, maxBytes int64, flushBytes int64) {
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
		metadataFound := true

		// key format: !raw/<ts>|<userKey>
		pipeIdx := bytes.IndexByte(k, '|')
		if pipeIdx <= 0 {
			continue
		}
		userKey := string(k[pipeIdx+1:])

		// value format: <filePath>|<size>
		parts := bytes.SplitN(v, []byte("|"), 2)
		if len(parts) < 1 {
			continue
		}
		filePath := string(parts[0])
		var fileSize int64
		if len(parts) == 2 {
			fileSize, _ = strconv.ParseInt(string(parts[1]), 10, 64)
		}

		// Verify that the key exists in metadata (ValueMessage)
		slice, err := rw.db.Get(ro, []byte(userKey))
		if err != nil {
			zlog.Error().Err(err).Str("key", userKey).Msg("rawWriter compaction: error getting metadata")
			// skip this raw entry for now
			continue
		}

		// If the key is not found in metadata, we can't write it to the segment
		if !slice.Exists() {
			zlog.Debug().Str("key", userKey).Msg("rawWriter compaction: key not found in metadata")
			metadataFound = false
		}

		// Unmarshal existing message
		vm := &pb.ValueMessage{}
		if err := proto.Unmarshal(slice.Data(), vm); err != nil {
			// Unable to parse
			slice.Free()
			metadataFound = false
		}

		// Write value into segment if metadata is found
		if metadataFound {
			slice.Free()

			// Write value into segment
			segPath, segOff, segLen, err := sm.WriteToSegment(userKey, filePath)
			if err != nil {
				zlog.Error().Err(err).Str("key", userKey).Msg("rawWriter compaction: WriteToSegment failed")
				continue
			}

			// Update ValueMessage fields
			vm.RawFilePath = ""
			vm.SegmentPath = segPath
			vm.SegmentOffset = segOff
			vm.ValueLength = segLen

			if data, err := proto.Marshal(vm); err == nil {
				batch.Put([]byte(userKey), data)
			}
		}

		// delete raw file on disk
		_ = rw.Delete(filePath)

		batch.Delete(k)
		processed++
		bytesMigrated += fileSize
		bytesToFlush += fileSize

		// Flush intermediate batch when thresholds met
		if bytesToFlush >= flushBytes {
			_ = rw.db.Write(wo, batch)

			// clear batch and reset bytes to flush
			batch.Clear()
			bytesToFlush = 0

			zlog.Debug().Int("processed", processed).Int64("bytes", bytesMigrated).Msg("rawWriter compaction: batch flushed")

			// stop compaction this run if thresholds reached
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
