package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/protobuf/proto"

	"github.com/tigrisdata/ocache/server/compaction"
	"github.com/tigrisdata/ocache/server/storage/bufferpool"
	"github.com/tigrisdata/ocache/server/storage/fd"
	"github.com/tigrisdata/ocache/server/storage/files"
	"github.com/tigrisdata/ocache/server/storage/metadata"
	"github.com/tigrisdata/ocache/server/storage/segment"
)

const (
	// Default compaction thresholds
	DefaultCompactionMaxBytes     = 1 << 30 // 1GB
	DefaultFileCompactionInterval = 5 * time.Minute
)

// Storage wraps all RocksDB access and related logic
// It provides methods to store, retrieve, delete, and list keys
//
// Value encoding format:
// For small objects (in-memory):
//
//	"S|" + [8-byte big-endian expiry] + '|' + [data bytes]
//
// For large objects (spilled to disk):
//
//	"L|" + [8-byte big-endian expiry] + '|' + [file path as bytes]
//
// If no TTL is set, expiry and separator are omitted:
//
//	"S|" + [data bytes] or "L|" + [file path as bytes]
//
// The separator '|' after the expiry ensures robust parsing, even if the data or file path contains '|'.
//
// On read, expiry is checked (if present) and expired keys are deleted and not returned.
type Storage struct {
	meta             *metadata.MetaDB
	diskPath         string                // Path to the disk cache directory
	inlineThreshold  int                   // Threshold for small vs large objects
	compactThreshold int64                 // Objects less than this size are compacted to segments (bytes)
	segmentManager   *segment.Manager      // Segment manager for large objects on disk
	fileManager      *files.FileManager    // File manager for large objects on disk
	fdCache          *fd.FdCache           // File descriptor cache for open files
	compactor        *compaction.Compactor // Background compactor for raw → segment migration
}

var storage *Storage

// GetStorage returns the singleton Storage instance
func GetStorage() *Storage {
	return storage
}

// InitStorage initializes storage at dbPath
func InitStorage(diskPath string, ttl int, inlineThreshold int, compactThreshold int64, segmentSize int64, fdCacheSize int) {
	s, err := newStorage(diskPath, ttl, inlineThreshold, compactThreshold, segmentSize, fdCacheSize)
	if err != nil {
		zlog.Fatal().Err(err).Msg("failed to open RocksDB")
	}
	storage = s
}

// newStorage initializes RocksDB inside diskPath and returns a Storage instance
func newStorage(diskPath string, ttl int, inlineThreshold int, compactThreshold int64, segmentSize int64, fdCacheSize int) (*Storage, error) {
	// Create the data directory if it doesn't exist
	if err := os.MkdirAll(diskPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize the metadata DB
	meta, err := metadata.NewMetaDB(diskPath, ttl)
	if err != nil {
		return nil, err
	}

	// Initialize the fdCache
	fdCache := fd.NewFdCache(fdCacheSize)

	// Initialize the segment manager
	segmentManager, err := segment.NewManager(diskPath, segmentSize)
	if err != nil {
		return nil, err
	}

	// Initialize the file manager
	fileManager, err := files.NewFileManager(diskPath)
	if err != nil {
		return nil, err
	}

	// Initialize and start background compactor that migrates raw files into segments.
	compactor := compaction.NewCompactor(fileManager, segmentManager, DefaultCompactionMaxBytes, DefaultFileCompactionInterval)
	compactor.Start()

	return &Storage{meta: meta, diskPath: diskPath, inlineThreshold: inlineThreshold, segmentManager: segmentManager, fileManager: fileManager, fdCache: fdCache, compactor: compactor}, nil
}

// Close closes the storage
func CloseStorage() {
	if storage == nil {
		return
	}

	// Stop background compactor first so it does not race with segment manager shutdown
	if storage.compactor != nil {
		storage.compactor.Close()
	}

	// Close the segment manager
	storage.segmentManager.Close()

	// Close the metadata DB
	metadata.CloseMetaDB()
}

// ListKeys returns all keys in the RocksDB instance
func (s *Storage) ListKeys() ([]string, error) {
	ro := grocksdb.NewDefaultReadOptions()
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()

	var keys []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := string(it.Key().Data())
		v := it.Value().Data()
		// Check for expiry if value is in expected format
		if len(v) >= 11 && (v[0] == 'S' || v[0] == 'L') && v[1] == '|' && v[10] == '|' {
			expiry := int64(binary.BigEndian.Uint64(v[2:10]))
			if expiry > 0 && time.Now().Unix() > expiry {
				// Expired, skip and delete
				it.Key().Free()
				it.Value().Free()
				s.DeleteKey(k)
				continue
			}
		}
		keys = append(keys, k)
		it.Key().Free()
		it.Value().Free()
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

// DeleteKey removes metadata and spills for a key
func (s *Storage) DeleteKey(key string) {
	wo := grocksdb.NewDefaultWriteOptions()
	s.meta.Handle().Delete(wo, []byte(key))
}

// Get retrieves the value for the given key from the database and returns an io.Reader for streaming
func (s *Storage) Get(key string) (io.Reader, bool, error) {
	ro := grocksdb.NewDefaultReadOptions()

	slice, err := s.meta.Handle().Get(ro, []byte(key))
	if err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("storage.Get: db.Get error")
		return nil, false, err
	}
	defer slice.Free()
	if !slice.Exists() {
		zlog.Debug().Str("key", key).Msg("storage.Get: not found in DB")
		return nil, false, nil
	}
	v := slice.Data()

	// Try to decode as proto ValueMessage
	valueMsg := &pb.ValueMessage{}
	err = proto.Unmarshal(v, valueMsg)
	if err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("storage.Get: failed to unmarshal proto ValueMessage")
		s.DeleteKey(key)
		return nil, false, err
	}

	zlog.Debug().Str("key", key).Msg("storage.Get: decoded proto ValueMessage")
	if valueMsg.Expiry > 0 && time.Now().Unix() > valueMsg.Expiry {
		zlog.Debug().Str("key", key).Msg("storage.Get: expired, deleting")
		s.DeleteKey(key)
		return nil, false, nil
	}

	switch valueMsg.ValueType {
	case pb.ValueType_INLINE:
		return bytes.NewReader(valueMsg.Data), true, nil
	case pb.ValueType_SEGMENT:
		if r, err := s.segmentManager.ReadValue(key, valueMsg.SegmentPath, valueMsg.SegmentOffset, valueMsg.ValueLength); err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.Get: failed to read segment slice")
			s.DeleteKey(key)
			return nil, false, err
		} else if r != nil {
			return r, true, nil
		}
	case pb.ValueType_RAW_FILE:
		if r, err := s.fileManager.Read(valueMsg.RawFilePath, valueMsg.ValueLength); err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.Get: failed to read file")
			s.DeleteKey(key)
			return nil, false, err
		} else if r != nil {
			return r, true, nil
		}
	default:
		zlog.Error().Str("key", key).Int("value_type", int(valueMsg.ValueType)).Msg("storage.Get: unknown value type")
		s.DeleteKey(key)
		return nil, false, nil
	}

	return nil, false, nil
}

// Put streams the body into spillWriter, stores metadata, and handles TTL
func (s *Storage) Put(key string, body io.Reader, ttl int) error {
	// We need to read at most threshold+1 bytes to decide if the value is "large".
	// Allocate a buffer exactly that size to avoid the short-buffer error.
	firstReadSize := s.inlineThreshold + 1
	if firstReadSize <= 0 {
		firstReadSize = 1 // ensure at least 1
	}
	firstChunk, release := bufferpool.AcquireBuffer(firstReadSize)
	defer release()

	// Read up to firstReadSize bytes. io.ReadFull returns ErrUnexpectedEOF when the
	// value is smaller than firstReadSize – that is fine, we still get the bytes read.
	n, err := io.ReadFull(body, firstChunk)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		zlog.Error().Err(err).Str("key", key).Msg("storage.Put: failed to read value")
		return err
	}

	// Determine expiry timestamp if TTL is specified
	var expiry int64
	if ttl > 0 {
		expiry = time.Now().Add(time.Duration(ttl) * time.Second).Unix()
	}

	// Large value path: we managed to read more than threshold bytes, which means
	// the value length exceeds the small-value threshold.
	if n > s.inlineThreshold {
		// Combine the bytes we already read with the remaining reader and write via the segment manager
		multiReader := io.MultiReader(bytes.NewReader(firstChunk[:n]), body)
		filePath, checksum, bytesWritten, err := s.fileManager.Write(key, multiReader)
		if err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.Put: failed to write to segment")
			return err
		}

		valueMsg := &pb.ValueMessage{
			RawFilePath: filePath,
			Expiry:      expiry,
			ValueLength: bytesWritten,
			Checksum:    checksum,
			ValueType:   pb.ValueType_RAW_FILE,
		}
		val, err := proto.Marshal(valueMsg)
		if err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.Put: failed to marshal value message")
			return err
		}
		return s.putLow(key, val, filePath, bytesWritten)
	}

	// Small value: we have read the entire value into firstChunk[:n]
	smallValue := firstChunk[:n]

	// We don't need to store the checksum for small values because
	// we are relying on RocksDB to verify the integrity of the data.
	valueMsg := &pb.ValueMessage{
		Data:        smallValue,
		Expiry:      expiry,
		ValueLength: int64(n),
		ValueType:   pb.ValueType_INLINE,
	}
	val, err := proto.Marshal(valueMsg)
	if err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("storage.Put: failed to marshal value message")
		return err
	}
	return s.putLow(key, val, "", int64(n))
}

// putLow stores the key-value pair in the database
// If the value is larger than the compact threshold, record it for compaction.
func (s *Storage) putLow(key string, val []byte, filePath string, bytesWritten int64) error {
	zlog.Debug().Str("key", key).Msg("storage.putLow: storing in RocksDB")

	wo := grocksdb.NewDefaultWriteOptions()
	batch := grocksdb.NewWriteBatch()

	// If the value is larger than the inline threshold and smaller than the compact threshold,
	// record it for compaction.
	if bytesWritten > int64(s.inlineThreshold) && bytesWritten <= s.compactThreshold {
		cIdxKey, cIdxVal := compaction.PrepareEntryForCompaction(key, filePath)
		batch.Put(cIdxKey, cIdxVal)
	}

	// Store the metadata in the database
	batch.Put([]byte(key), val)

	return s.meta.Handle().Write(wo, batch)
}
