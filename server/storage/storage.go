package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	pb "github.com/tigrisdata/cache_service/proto"
	"github.com/tigrisdata/cache_service/server/storage/bufferpool"
	"github.com/tigrisdata/cache_service/server/storage/metadata"
	"github.com/tigrisdata/cache_service/server/storage/segment"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
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
	db             *grocksdb.DB
	diskPath       string           // Path to the disk cache directory
	threshold      int              // Threshold for small vs large objects
	segmentManager *segment.Manager // Segment manager for large objects on disk
}

var storage *Storage

// GetStorage returns the singleton Storage instance
func GetStorage() *Storage {
	return storage
}

// InitStorage initializes storage at dbPath
func InitStorage(diskPath string, ttl int, threshold int) {
	s, err := newStorage(diskPath, ttl, threshold)
	if err != nil {
		panic("failed to open RocksDB: " + err.Error())
	}
	storage = s
}

// newStorage initializes RocksDB inside diskPath and returns a Storage instance
func newStorage(diskPath string, ttl int, threshold int) (*Storage, error) {
	// Create the metadata DB directory if it doesn't exist
	if err := os.MkdirAll(diskPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize the metadata DB
	db, err := metadata.InitMetaDB(diskPath, ttl)
	if err != nil {
		return nil, err
	}

	// Initialize the segment manager
	segmentManager, err := segment.NewManager(diskPath, segment.DefaultSegmentSize)
	if err != nil {
		return nil, err
	}

	return &Storage{db: db, diskPath: diskPath, threshold: threshold, segmentManager: segmentManager}, nil
}

// ListKeys returns all keys in the RocksDB instance
func (s *Storage) ListKeys() ([]string, error) {
	ro := grocksdb.NewDefaultReadOptions()
	it := s.db.NewIterator(ro)
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
	s.db.Delete(wo, []byte(key))
}

// Get retrieves the value for the given key from the database and returns an io.Reader for streaming
func (s *Storage) Get(key string) (io.Reader, bool, error) {
	ro := grocksdb.NewDefaultReadOptions()

	slice, err := s.db.Get(ro, []byte(key))
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

	// Try to read from small value
	if len(valueMsg.Data) > 0 {
		return bytes.NewReader(valueMsg.Data), true, nil
	}

	// Try to read from segment or raw file (large value)
	if r, err := s.segmentManager.ReadValue(valueMsg); err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("storage.Get: failed to read large value")
		s.DeleteKey(key)
		return nil, false, err
	} else if r != nil {
		return r, true, nil
	}

	return nil, false, nil
}

// Put streams the body into spillWriter, stores metadata, and handles TTL
func (s *Storage) Put(key string, body io.Reader, ttl int) error {
	// We need to read at most threshold+1 bytes to decide if the value is "large".
	// Allocate a buffer exactly that size to avoid the short-buffer error.
	firstReadSize := s.threshold + 1
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
	if n > s.threshold {
		// Combine the bytes we already read with the remaining reader and write via the segment manager
		multiReader := io.MultiReader(bytes.NewReader(firstChunk[:n]), body)
		filePath, err := s.segmentManager.WriteValue(key, multiReader)
		if err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.Put: failed to write to segment")
			return err
		}

		valueMsg := &pb.ValueMessage{
			RawFilePath: filePath,
			Expiry:      expiry,
		}
		val, err := proto.Marshal(valueMsg)
		if err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.Put: failed to marshal value message")
			return err
		}
		return s.putLow(key, val)
	}

	// Small value: we have read the entire value into firstChunk[:n]
	smallValue := firstChunk[:n]

	valueMsg := &pb.ValueMessage{
		Data:   smallValue,
		Expiry: expiry,
	}
	val, err := proto.Marshal(valueMsg)
	if err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("storage.Put: failed to marshal value message")
		return err
	}
	return s.putLow(key, val)
}

// putLow stores the key-value pair in the database
func (s *Storage) putLow(key string, val []byte) error {
	zlog.Debug().Str("key", key).Msg("storage.putLow: storing in RocksDB")

	wo := grocksdb.NewDefaultWriteOptions()
	return s.db.Put(wo, []byte(key), val)
}
