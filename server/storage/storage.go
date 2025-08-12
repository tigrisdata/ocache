package storage

import (
	"bytes"
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
	"github.com/tigrisdata/ocache/server/storage/keys"
	"github.com/tigrisdata/ocache/server/storage/metadata"
	"github.com/tigrisdata/ocache/server/storage/segment"
)

const (
	// Default compaction thresholds
	DefaultCompactionMaxBytes     = 1 << 30 // 1GB
	DefaultFileCompactionInterval = 1 * time.Minute
	// Default TTL cleanup interval
	DefaultTTLCleanupInterval = 1 * time.Minute
	// Default access update buffer size and batch interval
	DefaultAccessUpdateBufferSize = 10000
	DefaultAccessUpdateInterval   = 100 * time.Millisecond
)

// getCleanupInterval returns the cleanup interval, allowing tests to override via env var
func getCleanupInterval() time.Duration {
	if testInterval := os.Getenv("OCACHE_TEST_CLEANUP_INTERVAL"); testInterval != "" {
		if d, err := time.ParseDuration(testInterval); err == nil {
			return d
		}
	}
	return DefaultTTLCleanupInterval
}

// getCompactionInterval returns the compaction interval, allowing tests to override via env var
func getCompactionInterval() time.Duration {
	if testInterval := os.Getenv("OCACHE_TEST_COMPACTION_INTERVAL"); testInterval != "" {
		if d, err := time.ParseDuration(testInterval); err == nil {
			return d
		}
	}
	return DefaultFileCompactionInterval
}

// accessUpdate represents a single access time update request
type accessUpdate struct {
	key  string
	time int64
}

// accessUpdater handles asynchronous batched updates of access times for LRU tracking
type accessUpdater struct {
	updates  chan accessUpdate
	done     chan struct{}
	flush    chan chan struct{} // Channel to request flush with completion notification
	storage  *Storage
	interval time.Duration
}

// newAccessUpdater creates a new access updater
func newAccessUpdater(s *Storage, bufferSize int, interval time.Duration) *accessUpdater {
	return &accessUpdater{
		updates:  make(chan accessUpdate, bufferSize),
		done:     make(chan struct{}),
		flush:    make(chan chan struct{}),
		storage:  s,
		interval: interval,
	}
}

// Start begins the background goroutine for processing access updates
func (a *accessUpdater) Start() {
	go a.run()
}

// Stop stops the access updater
func (a *accessUpdater) Stop() {
	close(a.done)
}

// Update queues an access time update (non-blocking)
func (a *accessUpdater) Update(key string, accessTime int64) {
	select {
	case a.updates <- accessUpdate{key: key, time: accessTime}:
		// Update queued successfully
	default:
		// Buffer full, drop the update (LRU tracking is best-effort)
	}
}

// UpdateNow queues an access time update with current time (non-blocking)
func (a *accessUpdater) UpdateNow(key string) {
	a.Update(key, time.Now().Unix())
}

// Flush forces all pending updates to be written to RocksDB immediately
// This is mainly useful for testing to ensure deterministic behavior
func (a *accessUpdater) Flush() {
	done := make(chan struct{})
	a.flush <- done
	<-done // Wait for flush to complete
}

// run is the main loop that processes batched updates
func (a *accessUpdater) run() {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	batch := make(map[string]int64)

	for {
		select {
		case <-a.done:
			// Flush remaining updates before exiting
			a.collectUpdates(batch)
			if len(batch) > 0 {
				a.flushBatch(batch)
			}
			return

		case update := <-a.updates:
			// Only keep the latest access time for each key
			batch[update.key] = update.time

		case done := <-a.flush:
			// Handle explicit flush request
			a.collectUpdates(batch)
			if len(batch) > 0 {
				a.flushBatch(batch)
				batch = make(map[string]int64)
			}
			close(done) // Signal completion

		case <-ticker.C:
			if len(batch) > 0 {
				a.flushBatch(batch)
				// Clear the batch
				batch = make(map[string]int64)
			}
		}
	}
}

// collectUpdates drains all pending updates from the channel
func (a *accessUpdater) collectUpdates(batch map[string]int64) {
	for {
		select {
		case update := <-a.updates:
			batch[update.key] = update.time
		default:
			return
		}
	}
}

// flushBatch writes a batch of access updates to RocksDB
func (a *accessUpdater) flushBatch(batch map[string]int64) {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	writeBatch := grocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	for key, accessTime := range batch {
		accessKey, accessVal := PrepareAccessEntry(key, accessTime)
		writeBatch.Put(accessKey, accessVal)
	}

	if err := a.storage.meta.Handle().Write(wo, writeBatch); err != nil {
		zlog.Error().Err(err).Msg("accessUpdater: failed to flush batch")
	}
}

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
	cleaner          *Cleaner              // Background TTL cleanup and eviction
	accessUpdater    *accessUpdater        // Async access time updater for LRU tracking
	syncMonitor      *files.SyncMonitor    // Passive monitor for file sync tracking
}

var storage *Storage

// GetStorage returns the singleton Storage instance
func GetStorage() *Storage {
	return storage
}

// InitStorage initializes storage at dbPath
func InitStorage(diskPath string, ttl int, inlineThreshold int, compactThreshold int64, segmentSize int64, fdCacheSize int, maxDiskUsage int64) {
	s, err := newStorage(diskPath, ttl, inlineThreshold, compactThreshold, segmentSize, fdCacheSize, maxDiskUsage)
	if err != nil {
		zlog.Fatal().Err(err).Msg("failed to open storage")
	}
	storage = s
}

// newStorage initializes RocksDB inside diskPath and returns a Storage instance
func newStorage(diskPath string, ttl int, inlineThreshold int, compactThreshold int64, segmentSize int64, fdCacheSize int, maxDiskUsage int64) (*Storage, error) {
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

	// Run recovery for raw files BEFORE starting any services
	recovery := files.NewRecoveryManager(meta, diskPath)
	if err := recovery.RecoverOnStartup(); err != nil {
		zlog.Error().Err(err).Msg("storage: file recovery failed")
		return nil, fmt.Errorf("file recovery failed: %w", err)
	}

	// Initialize and start background compactor that migrates raw files into segments.
	compactionInterval := getCompactionInterval()
	compactor := compaction.NewCompactor(fileManager, segmentManager, DefaultCompactionMaxBytes, compactionInterval)
	compactor.Start()

	s := &Storage{
		meta:             meta,
		diskPath:         diskPath,
		inlineThreshold:  inlineThreshold,
		compactThreshold: compactThreshold,
		segmentManager:   segmentManager,
		fileManager:      fileManager,
		fdCache:          fdCache,
		compactor:        compactor,
	}

	// Initialize and start the cleaner (always enabled for TTL cleanup)
	cleanupInterval := getCleanupInterval()
	s.cleaner = NewCleaner(s, cleanupInterval, maxDiskUsage)
	s.cleaner.Start()
	zlog.Info().
		Dur("ttl_cleanup_interval", cleanupInterval).
		Int64("max_disk_usage", maxDiskUsage).
		Msg("storage: started background cleaner with TTL cleanup and LRU eviction")

	// Initialize and start the access updater for async LRU tracking only if max disk usage is set
	if maxDiskUsage > 0 {
		s.accessUpdater = newAccessUpdater(s, DefaultAccessUpdateBufferSize, DefaultAccessUpdateInterval)
		s.accessUpdater.Start()
		zlog.Info().
			Int("buffer_size", DefaultAccessUpdateBufferSize).
			Dur("batch_interval", DefaultAccessUpdateInterval).
			Msg("storage: started async access updater for LRU tracking")
	}

	// Initialize and start the passive sync monitor
	s.syncMonitor = files.NewSyncMonitor(meta, 30*time.Second)
	s.syncMonitor.Start()
	zlog.Info().Msg("storage: started passive sync monitor")

	return s, nil
}

// Close closes the storage
func CloseStorage() {
	if storage == nil {
		return
	}

	// Stop background services
	if storage.syncMonitor != nil {
		storage.syncMonitor.Stop()
	}
	if storage.accessUpdater != nil {
		storage.accessUpdater.Stop()
	}
	storage.cleaner.Close()
	if storage.compactor != nil {
		storage.compactor.Close()
	}

	// Close the segment manager
	storage.segmentManager.Close()

	// Close the metadata DB
	metadata.CloseMetaDB()
}

// ListKeys returns all non-expired keys in the RocksDB instance
// Note: Expired keys are skipped but not deleted - deletion is handled by the background cleaner
func (s *Storage) ListKeys() ([]string, error) {
	ro := grocksdb.NewDefaultReadOptions()
	// Use prefix iteration to only scan metadata keys
	ro.SetPrefixSameAsStart(true)
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()

	var keyList []string
	prefix := []byte(keys.MetadataPrefix)

	// Seek to the metadata prefix to start iteration
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		k := it.Key().Data()
		v := it.Value().Data()

		// Try to decode as proto ValueMessage to check expiry
		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(v, valueMsg); err == nil {
			if valueMsg.Expiry > 0 && time.Now().Unix() >= valueMsg.Expiry {
				// Expired, skip but don't delete - let the cleaner handle it
				it.Key().Free()
				it.Value().Free()
				continue
			}
		}

		// Extract the original user key without the prefix
		userKey := keys.ExtractUserKey(k)
		keyList = append(keyList, userKey)
		it.Key().Free()
		it.Value().Free()
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return keyList, nil
}

// DeleteKey removes metadata and spills for a key
func (s *Storage) DeleteKey(key string) {
	// Get the value to track size changes and file cleanup
	ro := grocksdb.NewDefaultReadOptions()
	metaKey := keys.MakeMetadataKey(key)
	slice, err := s.meta.Handle().Get(ro, metaKey)
	if err != nil || !slice.Exists() {
		return // Key doesn't exist, nothing to delete
	}
	defer slice.Free()

	// Parse value to get size and file info
	valueMsg := &pb.ValueMessage{}
	if err := proto.Unmarshal(slice.Data(), valueMsg); err == nil {
		// Notify cleaner about size reduction
		s.notifyDelete(valueMsg.ValueLength)

		// Clean up files if necessary
		switch valueMsg.ValueType {
		case pb.ValueType_RAW_FILE:
			if err := s.fileManager.Remove(valueMsg.RawFilePath); err != nil {
				zlog.Error().Err(err).Str("key", key).Str("file", valueMsg.RawFilePath).
					Msg("storage.DeleteKey: failed to remove raw file")
			}
		case pb.ValueType_SEGMENT:
			// Segments are cleaned up by the compactor/segment manager
		}
	}

	// Delete key and its access index in a single batch
	wo := grocksdb.NewDefaultWriteOptions()
	batch := grocksdb.NewWriteBatch()
	batch.Delete(metaKey)
	batch.Delete(MakeAccessIndexKey(key))
	s.meta.Handle().Write(wo, batch)
}

// Get retrieves the value for the given key from the database and returns an io.Reader for streaming
func (s *Storage) Get(key string) (io.Reader, bool, error) {
	ro := grocksdb.NewDefaultReadOptions()
	metaKey := keys.MakeMetadataKey(key)

	slice, err := s.meta.Handle().Get(ro, metaKey)
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
	if valueMsg.Expiry > 0 && time.Now().Unix() >= valueMsg.Expiry {
		zlog.Debug().Str("key", key).Msg("storage.Get: key has expired, returning not found")
		// Don't delete the key here - let the background cleaner handle it
		// This avoids race conditions with the cleaner
		return nil, false, nil
	}

	// Update access time asynchronously for LRU tracking only if max disk usage is set
	if s.accessUpdater != nil {
		s.accessUpdater.UpdateNow(key)
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
		zlog.Debug().Str("key", key).Int("ttl", ttl).Int64("expiry", expiry).Msg("storage.Put: setting TTL")
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
		err = s.putLow(key, val, filePath, bytesWritten)
		if err == nil {
			s.notifyPut(bytesWritten)
		}
		return err
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
	err = s.putLow(key, val, "", int64(n))
	if err == nil {
		s.notifyPut(int64(n))
	}
	return err
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

	// Store the metadata in the database with the metadata prefix
	metaKey := keys.MakeMetadataKey(key)
	batch.Put(metaKey, val)

	// Add sync tracking for raw files
	if filePath != "" {
		syncKey := files.MakeSyncKey(filePath)
		syncEntry := &files.SyncEntry{
			MetadataKey: string(metaKey),
			Timestamp:   time.Now().Unix(),
		}
		syncVal, _ := files.EncodeSyncEntry(syncEntry)
		batch.Put(syncKey, syncVal)
	}

	// Add access time index entry for LRU tracking only if max disk usage is set
	if s.cleaner.maxDiskUsage > 0 {
		accessKey, accessVal := PrepareAccessEntry(key, time.Now().Unix())
		batch.Put(accessKey, accessVal)
	}

	return s.meta.Handle().Write(wo, batch)
}

// CleanerStats returns statistics from the background cleaner
func (s *Storage) CleanerStats() (cleaned, evicted int64) {
	return s.cleaner.Stats()
}

// FlushAccessUpdates forces all pending access time updates to be written immediately
// This is mainly useful for testing to ensure deterministic behavior
func (s *Storage) FlushAccessUpdates() {
	if s.accessUpdater != nil {
		s.accessUpdater.Flush()
	}
}

// SetAccessTime sets a specific access time for a key
// This is mainly useful for testing to create predictable LRU scenarios
func (s *Storage) SetAccessTime(key string, accessTime int64) {
	if s.accessUpdater != nil {
		s.accessUpdater.Update(key, accessTime)
	}
}

// notifyPut updates the cleaner's size tracking when a new key is added
func (s *Storage) notifyPut(size int64) {
	s.cleaner.UpdateSize(size)
}

// notifyDelete updates the cleaner's size tracking when a key is deleted
func (s *Storage) notifyDelete(size int64) {
	s.cleaner.UpdateSize(-size)
}
