package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"

	pb "github.com/tigrisdata/cache_service/proto"
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
	db        *grocksdb.DB
	diskPath  string // Path to the disk cache directory
	threshold int    // Threshold for small vs large objects
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
	dbPath := diskPath + "/rocksdb"
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDbWithTTL(opts, dbPath, ttl)
	if err != nil {
		return nil, err
	}
	return &Storage{db: db, diskPath: diskPath, threshold: threshold}, nil
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
	ro := grocksdb.NewDefaultReadOptions()
	slice, err := s.db.Get(ro, []byte(key))
	if err != nil || !slice.Exists() {
		return
	}
	v := slice.Data()
	slice.Free()
	if len(v) > 2 && v[1] == '|' && v[0] == 'L' {
		// large object on disk
		os.Remove(string(v[2:]))
	}
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
	if err := proto.Unmarshal(v, valueMsg); err == nil {
		zlog.Debug().Str("key", key).Msg("storage.Get: decoded proto ValueMessage")
		if valueMsg.Expiry > 0 && time.Now().Unix() > valueMsg.Expiry {
			zlog.Debug().Str("key", key).Msg("storage.Get: expired, deleting")
			s.DeleteKey(key)
			return nil, false, nil
		}
		if valueMsg.FilePath != "" {
			f, err := os.Open(valueMsg.FilePath)
			if err != nil {
				zlog.Error().Err(err).Str("key", key).Msg("storage.Get: failed to open file from proto")
				return nil, false, err
			}
			return f, true, nil
		}
		if len(valueMsg.Data) > 0 {
			return bytes.NewReader(valueMsg.Data), true, nil
		}
		return nil, false, nil
	}

	// Fallback: legacy encoding (for backward compatibility)
	// Log up to first 32 bytes of the raw value for debugging
	zlog.Debug().Str("key", key).Int("raw_len", len(v)).Msg("storage.Get: raw value bytes")
	if len(v) > 0 {
		max := len(v)
		if max > 32 {
			max = 32
		}
		zlog.Debug().Str("key", key).Msgf("storage.Get: first bytes: %v", string(v[:max]))
	}

	// Expect the separator after expiry bytes
	if len(v) >= 11 && (v[0] == 'S' || v[0] == 'L') && v[1] == '|' && v[10] == '|' {
		expiry := int64(binary.BigEndian.Uint64(v[2:10]))
		zlog.Debug().Str("key", key).Int64("expiry", expiry).Int64("now", time.Now().Unix()).Msg("storage.Get: parsed expiry")
		if expiry > 0 && time.Now().Unix() > expiry {
			zlog.Debug().Str("key", key).Msg("storage.Get: expired, deleting")
			s.DeleteKey(key)
			return nil, false, nil
		}
		valStart := 11

		// Check if the value is small or large
		if v[0] == 'S' {
			zlog.Debug().Str("key", key).Int("data_len", len(v[valStart:])).Msg("storage.Get: returning small object")
			// Defensive: copy the value to a new slice to avoid referencing mmap'd memory that may be freed
			data := make([]byte, len(v[valStart:]))
			copy(data, v[valStart:])
			return bytes.NewReader(data), true, nil
		}
		if v[0] == 'L' {
			zlog.Debug().Str("key", key).Str("path", string(v[valStart:])).Msg("storage.Get: returning large object")
			f, err := os.Open(string(v[valStart:]))
			if err != nil {
				zlog.Error().Err(err).Str("key", key).Msg("storage.Get: failed to open file")
				return nil, false, err
			}
			return f, true, nil
		}
	}
	zlog.Debug().Str("key", key).Msg("storage.Get: value format did not match expected encoding")
	return nil, false, nil
}

// Put streams the body into spillWriter, stores metadata, and handles TTL
func (s *Storage) Put(key string, body io.Reader, ttl int) error {
	sw := newSpillWriter(s.threshold, s.diskPath, key)
	buf := GetBuffer()
	if _, err := io.CopyBuffer(sw, body, buf); err != nil {
		return err
	}
	if sw.UsedFile() {
		sw.file.Close()
		bufferPool.Put(buf[:0]) // only return to pool if file was used
	}
	// For small objects, do NOT call sw.Close() yet

	// Store expiry as part of the value, always encode expiryBytes (0 if no TTL)
	var val []byte
	var expiry int64
	if ttl > 0 {
		expiry = time.Now().Add(time.Duration(ttl) * time.Second).Unix()
	}
	// Use proto ValueMessage for encoding
	valueMsg := &pb.ValueMessage{}
	if sw.UsedFile() {
		valueMsg.FilePath = sw.FilePath()
	} else {
		valueMsg.Data = sw.Buffer()
	}
	valueMsg.Expiry = expiry
	val, err := proto.Marshal(valueMsg)
	if err != nil {
		return err
	}

	ts := generateTimestamp()
	if err := s.putLow(key, val, ts); err != nil {
		return err
	}

	if !sw.UsedFile() {
		sw.Close() // only now return buffer to pool for small objects
	}

	return nil
}

// putLow stores the key-value pair in the database
func (s *Storage) putLow(key string, val []byte, ts []byte) error {
	wo := grocksdb.NewDefaultWriteOptions()
	return s.db.Put(wo, []byte(key), val) // Use standard Put, ignore ts
}

func generateTimestamp() []byte {
	// Use 8-byte big-endian encoding for optimal RocksDB timestamp compatibility
	ts := time.Now().UnixNano()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(ts))
	return buf
}
