package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
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
	db *grocksdb.DB
}

var storage *Storage

// bufferPool is used to reduce allocations
var (
	defaultBufferSize = 64 * 1024 // 64KB default buffer size
	bufferPool        = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, defaultBufferSize)
		},
	}
)

// getBuffer returns a buffer of defaultBufferSize length from the pool, or allocates a new one if needed
func getBuffer() []byte {
	buf := bufferPool.Get().([]byte)
	if cap(buf) < defaultBufferSize {
		return make([]byte, defaultBufferSize)
	}
	return buf[:defaultBufferSize]
}

// initStorage initializes storage at dbPath
func initStorage(diskPath string, ttl int) {
	s, err := NewStorage(diskPath, ttl)
	if err != nil {
		panic("failed to open RocksDB: " + err.Error())
	}
	storage = s
}

// NewStorage initializes RocksDB inside diskPath and returns a Storage instance
func NewStorage(diskPath string, ttl int) (*Storage, error) {
	dbPath := diskPath + "/rocksdb"
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDbWithTTL(opts, dbPath, ttl)
	if err != nil {
		return nil, err
	}
	return &Storage{db: db}, nil
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
			return &pooledFileReader{f: f}, true, nil
		}
	}
	zlog.Debug().Str("key", key).Msg("storage.Get: value format did not match expected encoding")
	return nil, false, nil
}

// Put streams the body into spillWriter, stores metadata, and handles TTL
func (s *Storage) Put(key string, body io.Reader, ttl int) error {
	threshold := GetThreshold()
	diskPath := GetDiskPath()

	sw := newSpillWriter(threshold, diskPath, key)
	buf := getBuffer()
	if _, err := io.CopyBuffer(sw, body, buf); err != nil {
		return err
	}
	if sw.UsedFile() {
		sw.file.Close()
		bufferPool.Put(buf[:0]) // only return to pool if file was used
	}
	// For small objects, do NOT call sw.Close() yet

	// Store expiry as part of the value if TTL is set
	var val []byte
	if ttl > 0 {
		expiry := time.Now().Add(time.Duration(ttl) * time.Second).Unix()
		expiryBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(expiryBytes, uint64(expiry))
		if sw.UsedFile() {
			val = append([]byte("L|"), expiryBytes...)
			val = append(val, '|')
			val = append(val, []byte(sw.FilePath())...)
		} else {
			val = append([]byte("S|"), expiryBytes...)
			val = append(val, '|')
			val = append(val, sw.Buffer()...)
		}
	} else {
		if sw.UsedFile() {
			val = []byte("L|" + sw.FilePath())
		} else {
			val = append([]byte("S|"), sw.Buffer()...)
		}
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
