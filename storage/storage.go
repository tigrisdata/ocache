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

	"github.com/tigrisdata/ocache/storage/bufferpool"
	"github.com/tigrisdata/ocache/storage/compaction"
	"github.com/tigrisdata/ocache/storage/deletion"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/files"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	"github.com/tigrisdata/ocache/storage/metadata"
	"github.com/tigrisdata/ocache/storage/segment"
)

const (
	// Default disk path
	DefaultDiskPath = "/var/cache"

	// Default TTL (seconds) when no key-level TTL is set
	DefaultTTL = 0

	// Default inline threshold (bytes) for small objects that are inlined in RocksDB
	DefaultInlineThreshold = 64 * 1024 // 64KB

	// Default compact threshold (bytes) for objects that are compacted to segments
	DefaultCompactThreshold = 16 * 1024 * 1024 // 16MB

	// Default segment size (bytes)
	DefaultSegmentSize = 256 * 1024 * 1024 // 256MB

	// Default file descriptor cache size (entries)
	DefaultFdCacheSize = 10000

	// Default max disk usage (bytes)
	DefaultMaxDiskUsage = 0

	// Default compaction thresholds
	DefaultMaxBytesPerCompactRound = 1 << 30 // 1GB
	DefaultCompactionInterval      = 30 * time.Second
	DefaultCompactionThreads       = 1 // Default to single thread for backwards compatibility

	// Default TTL cleanup interval
	DefaultTTLCleanupInterval = 1 * time.Minute

	// Default access update buffer size, batch interval and delay
	DefaultAccessUpdateBufferSize = 100000
	DefaultAccessUpdateInterval   = 1 * time.Second
	DefaultAccessUpdateDelay      = 5 * time.Minute

	// Default queue config
	DeleteBatchSize = 1000 // Number of deletions to process per batch

	// Default segment recompaction settings
	DefaultRecompactionDisabled          = false
	DefaultFragmentationThreshold        = 0.5           // Recompact when dead space exceeds 50%
	DefaultMinSegmentAgeForRecompaction  = 2 * time.Hour // Don't recompact segments younger than 2 hours (ensures they're cold)
	DefaultMinSegmentsBeforeRecompaction = 2             // Minimum number of segments required for recompaction

	// Default delete queue settings
	DeleteProcessInterval = time.Second    // Interval between batch processing
	DeletePruneAge        = 24 * time.Hour // Age after which entries are pruned
)

// StorageConfig holds all configuration parameters for initializing storage
type StorageConfig struct {
	DiskPath            string        // Directory for on-disk cache data
	TTL                 int           // Default TTL when no key-level TTL is set (seconds)
	InlineThreshold     int           // Threshold for small objects that are inlined in RocksDB (bytes)
	CompactThreshold    int64         // Objects less than this size are compacted to segments (bytes)
	SegmentSize         int64         // Segment size (bytes)
	FdCacheSize         int           // Size of the file descriptor cache
	MaxDiskUsage        int64         // Maximum disk usage in bytes (0 = unlimited)
	CompactionInterval  time.Duration // Compaction interval
	CompactionThreads   int           // Number of compaction threads
	FragThreshold       float64       // Fragmentation threshold for segment recompaction (0.0-1.0)
	MinSegmentAge       time.Duration // Minimum age for segment recompaction
	MinSegments         int           // Minimum number of segments for recompaction
	DisableRecompaction bool          // Disable automatic segment recompaction
	CleanupInterval     time.Duration // Cleanup interval
	AccessUpdateDelay   time.Duration // Access update delay
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
	deletionQueue    *deletion.Queue       // Centralized file deletion queue
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

// InitStorageWithConfig initializes storage with a config struct
func InitStorageWithConfig(config *StorageConfig) {
	s, err := newStorageWithConfig(config)
	if err != nil {
		zlog.Fatal().Err(err).Msg("failed to open storage")
	}
	storage = s
}

// newStorageWithConfig initializes RocksDB inside diskPath and returns a Storage instance
func newStorageWithConfig(config *StorageConfig) (*Storage, error) {
	// Create the data directory if it doesn't exist
	if err := os.MkdirAll(config.DiskPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize the metadata DB with multiplex merge operator
	mergeOp := merge.NewMultiplexOperator()
	meta, err := metadata.NewMetaDB(config.DiskPath, config.TTL, mergeOp)
	if err != nil {
		return nil, err
	}

	// Initialize the fdCache
	fdCache := fd.NewFdCache(config.FdCacheSize)

	// Initialize the segment manager
	segmentManager, err := segment.NewManager(config.DiskPath, config.SegmentSize)
	if err != nil {
		return nil, err
	}

	// Initialize the file manager
	fileManager, err := files.NewFileManager(config.DiskPath)
	if err != nil {
		return nil, err
	}

	// Run recovery for raw files BEFORE starting any services
	recovery := files.NewRecoveryManager(meta, config.DiskPath)
	if err := recovery.RecoverOnStartup(); err != nil {
		zlog.Error().Err(err).Msg("storage: file recovery failed")
		return nil, fmt.Errorf("file recovery failed: %w", err)
	}

	// Initialize and start the centralized deletion queue
	deletionQueue := deletion.NewQueue(meta, deletion.Config{
		BatchSize:       DeleteBatchSize,
		ProcessInterval: DeleteProcessInterval,
		PruneAge:        DeletePruneAge,
	})
	deletionQueue.Start()

	// Configure compactor with recompaction if enabled
	compactorConfig := &compaction.CompactorConfig{
		FileManager:             fileManager,
		SegmentManager:          segmentManager,
		DeletionQueue:           deletionQueue,
		MaxBytesPerCompactRound: DefaultMaxBytesPerCompactRound,
		Interval:                DefaultCompactionInterval,
		CompactionThreads:       DefaultCompactionThreads,
	}

	if config.CompactionInterval > 0 {
		compactorConfig.Interval = config.CompactionInterval
	}

	if config.CompactionThreads > 0 {
		compactorConfig.CompactionThreads = config.CompactionThreads
	}

	if !config.DisableRecompaction {
		compactorConfig.FragThreshold = config.FragThreshold
		if config.FragThreshold <= 0 || config.FragThreshold > 1 {
			compactorConfig.FragThreshold = DefaultFragmentationThreshold
		}

		compactorConfig.EnableRecompaction = true
		compactorConfig.MinSegments = config.MinSegments
		if config.MinSegments <= 0 {
			compactorConfig.MinSegments = DefaultMinSegmentsBeforeRecompaction
		}

		compactorConfig.MinSegmentAge = config.MinSegmentAge
		if config.MinSegmentAge <= 0 {
			compactorConfig.MinSegmentAge = DefaultMinSegmentAgeForRecompaction
		}

		zlog.Info().Float64("threshold", compactorConfig.FragThreshold).Msg("Segment recompaction enabled")
	}

	compactor := compaction.NewCompactorWithConfig(compactorConfig)
	compactor.Start()

	s := &Storage{
		meta:             meta,
		diskPath:         config.DiskPath,
		inlineThreshold:  config.InlineThreshold,
		compactThreshold: config.CompactThreshold,
		segmentManager:   segmentManager,
		fileManager:      fileManager,
		fdCache:          fdCache,
		deletionQueue:    deletionQueue,
		compactor:        compactor,
	}

	// Initialize and start the cleaner (always enabled for TTL cleanup)
	cleanupInterval := DefaultTTLCleanupInterval
	if config.CleanupInterval > 0 {
		cleanupInterval = config.CleanupInterval
	}
	s.cleaner = NewCleaner(s, cleanupInterval, config.MaxDiskUsage)
	s.cleaner.Start()
	zlog.Info().
		Dur("ttl_cleanup_interval", cleanupInterval).
		Int64("max_disk_usage", config.MaxDiskUsage).
		Msg("storage: started background cleaner with TTL cleanup and LRU eviction")

	// Initialize and start the access updater for async LRU tracking only if max disk usage is set
	if config.MaxDiskUsage > 0 {
		accessUpdateDelay := DefaultAccessUpdateDelay
		if config.AccessUpdateDelay > 0 {
			accessUpdateDelay = config.AccessUpdateDelay
		}
		s.accessUpdater = newAccessUpdater(s, DefaultAccessUpdateBufferSize, DefaultAccessUpdateInterval, accessUpdateDelay)
		s.accessUpdater.Start()
		zlog.Info().
			Int("buffer_size", DefaultAccessUpdateBufferSize).
			Dur("batch_interval", DefaultAccessUpdateInterval).
			Dur("access_time_update_delay", accessUpdateDelay).
			Msg("storage: started async access updater for LRU tracking")
	}

	// Initialize and start the passive sync monitor
	s.syncMonitor = files.NewSyncMonitor(meta, deletionQueue, 30*time.Second)
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
	if storage.deletionQueue != nil {
		storage.deletionQueue.Stop()
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
			// Update delete index to track this deletion for future garbage collection
			s.updateDeleteIndex(valueMsg.SegmentPath, valueMsg.ValueLength)
		}
	}

	// Delete key and its access index in a single batch
	wo := grocksdb.NewDefaultWriteOptions()
	batch := grocksdb.NewWriteBatch()
	batch.Delete(metaKey)

	// Use secondary index to find and delete the bucketed access entry
	bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
	if slice, err := s.meta.Handle().Get(ro, bucketIndexKey); err == nil && slice.Exists() {
		// Delete the bucketed entry
		bucketKey := slice.Data()
		batch.Delete(bucketKey)
		slice.Free()
	}
	// Delete the secondary index entry
	batch.Delete(bucketIndexKey)

	s.meta.Handle().Write(wo, batch)
}

// Get retrieves the value for the given key from the database and returns an io.Reader for streaming
// Supports byte-range requests via start and end parameters (0 means no limit)
func (s *Storage) Get(key string, start, end int64) (io.Reader, bool, error) {
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

	var reader io.Reader

	switch valueMsg.ValueType {
	case pb.ValueType_INLINE:
		reader = bytes.NewReader(valueMsg.Data)
	case pb.ValueType_SEGMENT:
		if r, err := s.segmentManager.ReadEntry(key, valueMsg.SegmentPath, valueMsg.SegmentOffset, valueMsg.ValueLength); err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.Get: failed to read segment slice")
			s.DeleteKey(key)
			return nil, false, err
		} else if r != nil {
			reader = r
		} else {
			return nil, false, nil
		}
	case pb.ValueType_RAW_FILE:
		if r, err := s.fileManager.Read(valueMsg.RawFilePath, valueMsg.ValueLength); err != nil {
			zlog.Error().Err(err).Str("key", key).Msg("storage.Get: failed to read file")
			s.DeleteKey(key)
			return nil, false, err
		} else if r != nil {
			reader = r
		} else {
			return nil, false, nil
		}
	default:
		zlog.Error().Str("key", key).Int("value_type", int(valueMsg.ValueType)).Msg("storage.Get: unknown value type")
		s.DeleteKey(key)
		return nil, false, nil
	}

	// Apply byte-range if specified
	if start > 0 || (end > 0 && end > start) {
		reader = s.applyByteRange(reader, start, end)
	}

	return reader, true, nil
}

// applyByteRange wraps the reader to support byte-range requests
func (s *Storage) applyByteRange(r io.Reader, start, end int64) io.Reader {
	// Create a wrapper that will handle seeking and limiting
	return &byteRangeReader{
		reader: r,
		start:  start,
		end:    end,
		pos:    0,
		seeked: false,
	}
}

// byteRangeReader wraps an io.Reader to provide byte-range support
type byteRangeReader struct {
	reader io.Reader
	start  int64
	end    int64
	pos    int64
	seeked bool
}

// Read implements io.Reader with byte-range support
func (br *byteRangeReader) Read(p []byte) (n int, err error) {
	// Seek to start position if not already done
	if !br.seeked && br.start > 0 {
		if seeker, ok := br.reader.(io.Seeker); ok {
			_, err = seeker.Seek(br.start, io.SeekStart)
			if err != nil {
				return 0, err
			}
			br.pos = br.start
		} else {
			// If not seekable, read and discard up to start
			buf, release := bufferpool.AcquireBuffer(1 << 20) // 1 MiB
			defer release()
			toSkip := br.start
			for toSkip > 0 {
				readLen := int64(len(buf))
				if readLen > toSkip {
					readLen = toSkip
				}
				readN, err := br.reader.Read(buf[:readLen])
				if readN > 0 {
					toSkip -= int64(readN)
					br.pos += int64(readN)
				}
				if err != nil {
					return 0, err
				}
			}
		}
		br.seeked = true
	}

	// Apply end limit if specified
	if br.end > 0 && br.pos >= br.end {
		return 0, io.EOF
	}

	// Limit read size if we have an end boundary
	if br.end > 0 && br.pos+int64(len(p)) > br.end {
		p = p[:br.end-br.pos]
	}

	// Read from underlying reader
	n, err = br.reader.Read(p)
	br.pos += int64(n)
	return n, err
}

// Close implements io.Closer
func (br *byteRangeReader) Close() error {
	if closer, ok := br.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
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
		syncKey := keys.MakeSyncKey(filePath)
		syncEntry := &pb.SyncEntry{
			MetadataKey: string(metaKey),
			Timestamp:   time.Now().Unix(),
		}
		syncVal, _ := files.EncodeSyncEntry(syncEntry)
		batch.Put(syncKey, syncVal)
	}

	// Add access time index entry for LRU tracking only if max disk usage is set
	if s.cleaner.maxDiskUsage > 0 {
		now := time.Now()
		accessKey := keys.MakeBucketedAccessKey(key, now)
		batch.Put(accessKey, []byte{})

		// Add secondary index entry
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		batch.Put(bucketIndexKey, accessKey)
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

// updateDeleteIndex updates the delete index for a segment when a key is deleted
// This uses RocksDB's merge operator for atomic updates, avoiding race conditions
func (s *Storage) updateDeleteIndex(segmentPath string, deletedBytes int64) {
	if segmentPath == "" {
		return
	}

	deleteIndexKey := keys.MakeDeleteIndexKey(segmentPath)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	// Create operand for merge: 1 entry deleted, N bytes deleted
	operand := merge.MakeDeleteIndexOperand(1, deletedBytes)

	// Use Merge for atomic update
	if err := s.meta.Handle().Merge(wo, deleteIndexKey, operand); err != nil {
		zlog.Error().Err(err).Str("segment", segmentPath).
			Msg("storage.updateDeleteIndex: failed to merge delete index entry")
	}
}

// GetDeleteIndexStats returns the delete index statistics for a segment
func (s *Storage) GetDeleteIndexStats(segmentPath string) (deletedEntries, deletedBytes int64, err error) {
	if segmentPath == "" {
		return 0, 0, nil
	}

	deleteIndexKey := keys.MakeDeleteIndexKey(segmentPath)
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	slice, err := s.meta.Handle().Get(ro, deleteIndexKey)
	if err != nil {
		return 0, 0, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return 0, 0, nil // No deletions tracked for this segment
	}

	var entry pb.DeleteIndexEntry
	if err := proto.Unmarshal(slice.Data(), &entry); err != nil {
		return 0, 0, err
	}

	return entry.DeletedEntries, entry.DeletedBytes, nil
}

// RemoveDeleteIndex removes the delete index entry for a segment (used when segment is removed)
func (s *Storage) RemoveDeleteIndex(segmentPath string) error {
	if segmentPath == "" {
		return nil
	}

	deleteIndexKey := keys.MakeDeleteIndexKey(segmentPath)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return s.meta.Handle().Delete(wo, deleteIndexKey)
}

// SegmentDeleteStats holds deletion statistics for a segment
type SegmentDeleteStats struct {
	SegmentPath    string
	DeletedEntries int64
	DeletedBytes   int64
}

// ListSegmentDeleteStats returns deletion statistics for all segments in the delete index
func (s *Storage) ListSegmentDeleteStats() ([]SegmentDeleteStats, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()

	var stats []SegmentDeleteStats
	prefix := []byte(keys.DeleteIndexPrefix)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		value := it.Value()

		// Extract segment path from key
		segmentPath := keys.ExtractSegmentPath(key.Data())
		key.Free()

		// Parse delete index entry
		var entry pb.DeleteIndexEntry
		if err := proto.Unmarshal(value.Data(), &entry); err != nil {
			value.Free()
			zlog.Error().Err(err).Str("segment", segmentPath).
				Msg("storage.ListSegmentDeleteStats: failed to unmarshal delete index entry")
			continue
		}
		value.Free()

		stats = append(stats, SegmentDeleteStats{
			SegmentPath:    segmentPath,
			DeletedEntries: entry.DeletedEntries,
			DeletedBytes:   entry.DeletedBytes,
		})
	}

	if err := it.Err(); err != nil {
		return nil, err
	}

	return stats, nil
}
