// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/tigrisdata/ocache/common/bufferpool"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/compaction"
	"github.com/tigrisdata/ocache/storage/deletion"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/files"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
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
	DefaultCompactThreshold = 64 * 1024 * 1024 // 64MB

	// Default segment size (bytes)
	DefaultSegmentSize = 256 * 1024 * 1024 // 256MB

	// Default file descriptor cache size (entries)
	DefaultFdCacheSize = 10000

	// Default max disk usage (bytes)
	DefaultMaxDiskUsage = 0

	// Default compaction threads
	DefaultCompactionThreads = 1 // Default to single thread for backwards compatibility

	// Default TTL cleanup interval
	DefaultTTLCleanupInterval = 1 * time.Minute

	// Default access update buffer size, batch interval and delay
	DefaultAccessUpdateBufferSize = 100000
	DefaultAccessUpdateInterval   = 1 * time.Second
	DefaultAccessUpdateDelay      = 5 * time.Minute

	// Default queue config
	DefaultDeleteBatchSize = 1000 // Number of deletions to process per batch

	// Default segment recompaction settings
	DefaultRecompactionDisabled          = false
	DefaultFragmentationThreshold        = 0.5             // Recompact when dead space exceeds 50%
	DefaultMinSegmentAgeForRecompaction  = 2 * time.Hour   // Don't recompact segments younger than 2 hours (ensures they're cold)
	DefaultMinSegmentsBeforeRecompaction = 2               // Minimum number of segments required for recompaction
	DefaultRecompactionInterval          = 1 * time.Minute // Interval between segment recompaction runs

	// Default delete queue settings
	DeleteProcessInterval = time.Second      // Interval between batch processing
	DeletePruneAge        = 24 * time.Hour   // Age after which entries are pruned
	DeleteRetryDelay      = 30 * time.Second // Backoff before retrying a failed deletion (bounds retry churn for stuck files)

	// Default number of parallel workers for startup file recovery
	DefaultRecoveryWorkers = files.MaxWorkers

	// Default RocksDB configuration
	DefaultMetadataCacheSize      = metadata.DefaultRocksDBBlockCacheSize
	DefaultMetadataBackgroundJobs = metadata.DefaultRocksDBMaxBackgroundJobs

	// Eviction policies for StorageConfig.EvictionPolicy (only relevant when
	// MaxDiskUsage > 0). Both walk the same time-bucketed access index oldest
	// first; they differ only in whether a read refreshes an entry's timestamp.
	//
	//   lru  - evict least-recently-accessed first; a read bumps the entry's
	//          timestamp, protecting recently-read data.
	//   fifo - evict oldest-written first; reads do not bump the timestamp, so
	//          a rare read of old data does not protect it from eviction.
	EvictionPolicyLRU  = "lru"
	EvictionPolicyFIFO = "fifo"

	// DefaultEvictionPolicy preserves the historical behavior (LRU).
	DefaultEvictionPolicy = EvictionPolicyLRU
)

// StorageConfig holds all configuration parameters for initializing storage
type StorageConfig struct {
	DiskPath             string        // Directory for on-disk cache data
	TTL                  int           // Default TTL when no key-level TTL is set (seconds)
	InlineThreshold      int           // Threshold for small objects that are inlined in RocksDB (bytes)
	CompactThreshold     int64         // Objects less than this size are compacted to segments (bytes)
	SegmentSize          int64         // Segment size (bytes)
	FdCacheSize          int           // Size of the file descriptor cache
	MaxDiskUsage         int64         // Maximum disk usage in bytes (0 = unlimited)
	CompactionThreads    int           // Number of compaction threads
	FragThreshold        float64       // Fragmentation threshold for segment recompaction (0.0-1.0)
	MinSegmentAge        time.Duration // Minimum age for segment recompaction
	MinSegments          int           // Minimum number of segments for recompaction
	DisableRecompaction  bool          // Disable automatic segment recompaction
	RecompactionInterval time.Duration // Interval between segment recompaction runs
	CleanupInterval      time.Duration // Cleanup interval
	AccessUpdateDelay    time.Duration // Access update delay
	RecoveryWorkers      int           // Number of parallel workers for startup file recovery (<= 0 = default)
	DeleteBatchSize      int           // Number of file deletions processed per deletion-queue batch (<= 0 = default)
	EvictionPolicy       string        // Eviction order when MaxDiskUsage > 0: "lru" (default) or "fifo"

	// RocksDB-specific configuration
	MetadataCacheSize      int64 // RocksDB Block cache size in bytes (0 = use default)
	MetadataBackgroundJobs int   // Max concurrent RocksDB background jobs, compactions+flushes (0 = use default)
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
	accessUpdater    *accessUpdater        // Async access time updater for LRU tracking (nil in FIFO mode)
	evictionPolicy   string                // "lru" or "fifo"; governs whether reads refresh access time
	closed           atomic.Bool           // True when storage has been closed
}

// NewStorageWithConfig creates a new isolated Storage instance with the given config.
func NewStorageWithConfig(config *StorageConfig) (*Storage, error) {
	// Apply defaults for unset fields
	if config.InlineThreshold <= 0 {
		config.InlineThreshold = DefaultInlineThreshold
	}
	if config.CompactThreshold <= 0 {
		config.CompactThreshold = DefaultCompactThreshold
	}
	if config.SegmentSize <= 0 {
		config.SegmentSize = DefaultSegmentSize
	}

	// Guardrail: an object larger than a segment cannot be compacted into one, so
	// CompactThreshold (the size at/below which objects are compaction-eligible)
	// must stay below SegmentSize. Clamp + warn on a misconfiguration rather than
	// failing to start; objects at/above SegmentSize then correctly remain
	// permanent raw files (#165).
	if config.CompactThreshold >= config.SegmentSize {
		clamped := config.SegmentSize - 1
		zlog.Warn().
			Int64("compact_threshold", config.CompactThreshold).
			Int64("segment_size", config.SegmentSize).
			Int64("clamped_to", clamped).
			Msg("storage: compact-threshold >= segment-size; clamping compact-threshold below segment-size so compaction-eligible objects always fit a segment")
		config.CompactThreshold = clamped
	}

	if config.FdCacheSize <= 0 {
		config.FdCacheSize = DefaultFdCacheSize
	}
	if config.DeleteBatchSize <= 0 {
		config.DeleteBatchSize = DefaultDeleteBatchSize
	}
	switch config.EvictionPolicy {
	case "":
		config.EvictionPolicy = DefaultEvictionPolicy
	case EvictionPolicyLRU, EvictionPolicyFIFO:
		// valid
	default:
		zlog.Warn().
			Str("eviction_policy", config.EvictionPolicy).
			Str("fallback", DefaultEvictionPolicy).
			Msg("storage: unknown eviction policy, falling back to default")
		config.EvictionPolicy = DefaultEvictionPolicy
	}

	// Create the data directory if it doesn't exist
	if err := os.MkdirAll(config.DiskPath, 0o755); err != nil {
		zlog.Error().Err(err).Str("path", config.DiskPath).Msg("storage: failed to create data directory")
		return nil, storageErrors.NewIOError("Init", "", err)
	}

	// Initialize the metadata DB with multiplex merge operator
	mergeOp := merge.NewMultiplexOperator()

	rocksConfig := metadata.DefaultRocksDBConfig()
	if config.MetadataCacheSize > 0 {
		rocksConfig.BlockCacheSize = config.MetadataCacheSize
	}
	if config.MetadataBackgroundJobs > 0 {
		rocksConfig.MaxBackgroundJobs = config.MetadataBackgroundJobs
	}

	// Use isolated instance constructor to avoid singleton sharing between multiple storage instances
	meta, err := metadata.NewMetaDB(config.DiskPath, config.TTL, mergeOp, rocksConfig)
	if err != nil {
		zlog.Error().Err(err).Msg("storage: failed to open metadata DB")
		return nil, storageErrors.NewInternalError("Init", err)
	}

	// Initialize the fdCache
	fdCache := fd.NewFdCache(config.FdCacheSize)

	// Initialize the segment manager
	segmentManager, err := segment.NewManager(config.DiskPath, config.SegmentSize)
	if err != nil {
		zlog.Error().Err(err).Msg("storage: failed to initialize segment manager")
		return nil, storageErrors.NewInternalError("Init", err)
	}

	// Initialize the file manager
	fileManager, err := files.NewFileManager(config.DiskPath, config.CompactThreshold)
	if err != nil {
		zlog.Error().Err(err).Msg("storage: failed to create file manager")
		return nil, storageErrors.NewInternalError("Init", err)
	}

	// Run recovery for raw files BEFORE starting any services
	recovery := files.NewRecoveryManager(meta, config.DiskPath, config.RecoveryWorkers)
	if err := recovery.RecoverOnStartup(); err != nil {
		zlog.Error().Err(err).Msg("storage: file recovery failed")
		return nil, storageErrors.NewInternalError("Init", err)
	}

	// Initialize and start the centralized deletion queue
	deletionQueue := deletion.NewQueue(meta, deletion.Config{
		BatchSize:       config.DeleteBatchSize,
		ProcessInterval: DeleteProcessInterval,
		PruneAge:        DeletePruneAge,
		RetryDelay:      DeleteRetryDelay,
	})
	deletionQueue.Start()

	// Configure compactor with recompaction if enabled
	compactorConfig := &compaction.CompactorConfig{
		MetaDB:            meta,
		FileManager:       fileManager,
		SegmentManager:    segmentManager,
		DeletionQueue:     deletionQueue,
		CompactionThreads: DefaultCompactionThreads,
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

		compactorConfig.RecompactionInterval = config.RecompactionInterval
		if config.RecompactionInterval <= 0 {
			compactorConfig.RecompactionInterval = DefaultRecompactionInterval
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
		evictionPolicy:   config.EvictionPolicy,
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

	// Initialize and start the access updater for async LRU tracking. Only
	// needed when eviction is active (MaxDiskUsage > 0) and the policy is LRU:
	// its sole job is to refresh an entry's access time on reads. In FIFO mode
	// reads must not refresh recency, so we leave it nil and the read path skips
	// the bump — the access index then stays frozen at each entry's write time,
	// which is exactly the FIFO eviction order.
	if config.MaxDiskUsage > 0 && config.EvictionPolicy == EvictionPolicyLRU {
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

	return s, nil
}

// Close closes this storage instance and releases all resources.
// This is safe to call on isolated instances created with NewStorageWithConfig.
func (s *Storage) Close() {
	// Mark storage as closed first to prevent new operations
	s.closed.Store(true)

	// Stop background services
	if s.accessUpdater != nil {
		s.accessUpdater.Stop()
	}
	if s.cleaner != nil {
		s.cleaner.Close()
	}
	if s.compactor != nil {
		s.compactor.Close()
	}
	if s.deletionQueue != nil {
		s.deletionQueue.Stop()
	}

	// Close the segment manager
	if s.segmentManager != nil {
		s.segmentManager.Close()
	}

	// Close the metadata DB
	if s.meta != nil {
		s.meta.Close()
	}
}

// TotalSize returns the current logical cache size in bytes: the sum of stored
// object lengths across all keys. It is maintained live on every write and
// eviction, so embedders can publish their own gauge without a full rescan.
// Returns 0 if the cleaner is not initialized.
func (s *Storage) TotalSize() int64 {
	if s.cleaner == nil {
		return 0
	}
	return s.cleaner.TotalSize()
}

// IsClosed returns true if this storage instance has been closed.
// This can be used to check if it's safe to call other Storage methods.
func (s *Storage) IsClosed() bool {
	return s.closed.Load()
}

// ListKeys returns all non-expired keys in the RocksDB instance that match the given prefix
// Note: Expired keys are skipped but not deleted - deletion is handled by the background cleaner
func (s *Storage) ListKeys(userPrefix string) ([]string, error) {
	keyList, _, _, err := s.ListKeysWithPagination(userPrefix, "", 0)
	if err != nil {
		return nil, err
	}
	return keyList, nil
}

// ListKeysWithPagination returns paginated, sorted keys from RocksDB
// Returns: (keys, lastKey, hasMore, error)
// - keys: Up to 'limit' keys starting after 'startKey'
// - lastKey: The last key in this page (for continuation)
// - hasMore: True if more keys exist beyond this page
func (s *Storage) ListKeysWithPagination(userPrefix string, startKey string, limit int) ([]string, string, bool, error) {
	storageType := "unknown"
	start := time.Now()
	defer func() {
		metrics.StorageOperationDuration.WithLabelValues("list_paginated", storageType).Observe(float64(time.Since(start).Milliseconds()))
	}()

	ro := metadata.CreateReadOptions(true, false)
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()

	var keyList []string
	var lastKey string

	// Determine where to start iteration
	var seekKey []byte
	if startKey != "" {
		// Start after the given key
		seekKey = keys.MakeMetadataKey(startKey)
	} else if userPrefix != "" {
		// Start at the prefix
		seekKey = keys.MakeMetadataKey(userPrefix)
	} else {
		// Start at the beginning of all keys
		seekKey = []byte(keys.MetadataPrefix)
	}

	// Construct the prefix boundary for iteration
	var prefixBoundary []byte
	if userPrefix != "" {
		prefixBoundary = keys.MakeMetadataKey(userPrefix)
	} else {
		prefixBoundary = []byte(keys.MetadataPrefix)
	}

	// Seek to start position
	it.Seek(seekKey)

	// If we have a startKey, skip it (pagination is exclusive)
	if startKey != "" && it.Valid() {
		k := it.Key().Data()
		currentUserKey := keys.ExtractUserKey(k)
		if currentUserKey == startKey {
			it.Key().Free()
			it.Value().Free()
			it.Next()
		}
	}

	// Collect up to limit keys
	for it.ValidForPrefix(prefixBoundary) {
		// Check if we've reached the limit
		if limit > 0 && len(keyList) >= limit {
			break
		}

		k := it.Key().Data()
		v := it.Value().Data()

		// Try to decode as proto ValueMessage to check expiry
		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(v, valueMsg); err == nil {
			if valueMsg.Expiry > 0 && time.Now().Unix() >= valueMsg.Expiry {
				// Expired, skip but don't delete - let the cleaner handle it
				it.Key().Free()
				it.Value().Free()
				it.Next()
				continue
			}
		}

		// Extract the original user key
		userKey := keys.ExtractUserKey(k)

		keyList = append(keyList, userKey)
		lastKey = userKey

		it.Key().Free()
		it.Value().Free()
		it.Next()
	}

	if err := it.Err(); err != nil {
		metrics.StorageOperations.WithLabelValues("list_paginated", storageType, "error").Inc()
		metrics.Errors.WithLabelValues("rocksdb", "list_paginated").Inc()
		return nil, "", false, mapRocksDBError("ListKeysWithPagination", "", err)
	}

	// Check if there are more keys
	hasMore := it.ValidForPrefix(prefixBoundary)
	if hasMore {
		// Peek at the next key to confirm it matches prefix
		k := it.Key().Data()
		nextUserKey := keys.ExtractUserKey(k)
		if userPrefix != "" && !bytes.HasPrefix([]byte(nextUserKey), []byte(userPrefix)) {
			hasMore = false
		}
		it.Key().Free()
	}

	// Clear lastKey if there are no more results
	if !hasMore {
		lastKey = ""
	}

	metrics.StorageOperations.WithLabelValues("list_paginated", storageType, "success").Inc()
	return keyList, lastKey, hasMore, nil
}

// MaxListValueSize caps the per-value bytes returned by List-with-values. A
// value larger than this is not read from disk; the entry is returned with the
// value omitted (and its size reported) so a List over large objects cannot
// buffer an object-sized allocation or exceed the gRPC message limit (#165).
const MaxListValueSize int64 = 1 << 20 // 1 MiB

// KeyValue holds a key and its associated value bytes.
type KeyValue struct {
	Key   string
	Value []byte
	// ValueLength is the size of the value in bytes, set even when the value was
	// omitted for exceeding MaxListValueSize.
	ValueLength int64
	// ValueOmitted is true when the value was not read because it exceeds
	// MaxListValueSize; Value is nil in that case.
	ValueOmitted bool
}

// ListKeyValuesWithPagination returns paginated, sorted key-value pairs from RocksDB.
// For inline values the data is returned directly; for file/segment values the data is
// read from disk. Returns: (entries, lastKey, hasMore, error).
func (s *Storage) ListKeyValuesWithPagination(userPrefix string, startKey string, limit int) ([]KeyValue, string, bool, error) {
	storageType := "unknown"
	start := time.Now()
	defer func() {
		metrics.StorageOperationDuration.WithLabelValues("list_kv_paginated", storageType).Observe(float64(time.Since(start).Milliseconds()))
	}()

	ro := metadata.CreateReadOptions(true, false)
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()

	var entries []KeyValue
	var lastKey string

	// Determine where to start iteration
	var seekKey []byte
	if startKey != "" {
		seekKey = keys.MakeMetadataKey(startKey)
	} else if userPrefix != "" {
		seekKey = keys.MakeMetadataKey(userPrefix)
	} else {
		seekKey = []byte(keys.MetadataPrefix)
	}

	// Construct the prefix boundary for iteration
	var prefixBoundary []byte
	if userPrefix != "" {
		prefixBoundary = keys.MakeMetadataKey(userPrefix)
	} else {
		prefixBoundary = []byte(keys.MetadataPrefix)
	}

	// Seek to start position
	it.Seek(seekKey)

	// If we have a startKey, skip it (pagination is exclusive)
	if startKey != "" && it.Valid() {
		k := it.Key().Data()
		currentUserKey := keys.ExtractUserKey(k)
		if currentUserKey == startKey {
			it.Key().Free()
			it.Value().Free()
			it.Next()
		}
	}

	// Collect up to limit key-value pairs
	for it.ValidForPrefix(prefixBoundary) {
		if limit > 0 && len(entries) >= limit {
			break
		}

		k := it.Key().Data()
		v := it.Value().Data()

		// Try to decode as proto ValueMessage to check expiry.
		// If unmarshal fails, include the key with nil value to keep
		// page boundaries consistent with ListKeysWithPagination.
		var data []byte
		var valueLength int64
		var valueOmitted bool
		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(v, valueMsg); err == nil {
			if valueMsg.Expiry > 0 && time.Now().Unix() >= valueMsg.Expiry {
				it.Key().Free()
				it.Value().Free()
				it.Next()
				continue
			}

			storageType = pb.ValueType_name[int32(valueMsg.ValueType)]
			valueLength = valueMsg.ValueLength

			if valueMsg.ValueLength > MaxListValueSize {
				// Value exceeds the List cap: omit it from the response (#165).
				// For SEGMENT/RAW_FILE this skips the object-sized disk read below.
				// With the default inline threshold (64 KB, well under the 1 MiB
				// cap) INLINE values never reach here; if the inline threshold is
				// configured above the cap, an INLINE value's bytes were already
				// materialized by proto.Unmarshal above, so the cap still bounds
				// the response but does not avoid that allocation.
				valueOmitted = true
				metrics.ListValuesOmitted.Inc()
			} else {
				switch valueMsg.ValueType {
				case pb.ValueType_INLINE:
					data = make([]byte, len(valueMsg.Data))
					copy(data, valueMsg.Data)
				case pb.ValueType_SEGMENT:
					r, readErr := s.segmentManager.ReadEntry(keys.ExtractUserKey(k), valueMsg.SegmentPath, valueMsg.SegmentOffset, valueMsg.ValueLength)
					if readErr == nil && r != nil {
						data, readErr = io.ReadAll(r)
						if closer, ok := r.(io.Closer); ok {
							closer.Close()
						}
						if readErr != nil {
							data = nil
						}
					}
				case pb.ValueType_RAW_FILE:
					r, readErr := s.fileManager.Read(valueMsg.RawFilePath, valueMsg.ValueLength)
					if readErr == nil && r != nil {
						data, readErr = io.ReadAll(r)
						if closer, ok := r.(io.Closer); ok {
							closer.Close()
						}
						if readErr != nil {
							data = nil
						}
					}
				}
			}
		}

		userKey := keys.ExtractUserKey(k)
		entries = append(entries, KeyValue{Key: userKey, Value: data, ValueLength: valueLength, ValueOmitted: valueOmitted})
		lastKey = userKey

		it.Key().Free()
		it.Value().Free()
		it.Next()
	}

	if err := it.Err(); err != nil {
		metrics.StorageOperations.WithLabelValues("list_kv_paginated", storageType, "error").Inc()
		metrics.Errors.WithLabelValues("rocksdb", "list_kv_paginated").Inc()
		return nil, "", false, mapRocksDBError("ListKeyValuesWithPagination", "", err)
	}

	// Check if there are more keys
	hasMore := it.ValidForPrefix(prefixBoundary)
	if hasMore {
		k := it.Key().Data()
		nextUserKey := keys.ExtractUserKey(k)
		if userPrefix != "" && !bytes.HasPrefix([]byte(nextUserKey), []byte(userPrefix)) {
			hasMore = false
		}
		it.Key().Free()
	}

	if !hasMore {
		lastKey = ""
	}

	metrics.StorageOperations.WithLabelValues("list_kv_paginated", storageType, "success").Inc()
	return entries, lastKey, hasMore, nil
}

// DeleteKey removes metadata and spills for a key
func (s *Storage) DeleteKey(key string) error {
	storageType := "unknown"
	start := time.Now()
	defer func() {
		metrics.StorageOperationDuration.WithLabelValues("delete", storageType).Observe(float64(time.Since(start).Milliseconds()))
	}()

	// Get the value to track size changes and file cleanup
	ro := metadata.CreateReadOptions(false, false)
	metaKey := keys.MakeMetadataKey(key)
	slice, err := s.meta.Handle().Get(ro, metaKey)
	if err != nil {
		metrics.StorageOperations.WithLabelValues("delete", storageType, "error").Inc()
		zlog.Error().Err(err).Str("key", key).Msg("storage.DeleteKey: db.Get error")
		// RocksDB errors are typically temporary
		return mapRocksDBError("DeleteKey", key, err)
	}
	if !slice.Exists() {
		metrics.StorageOperations.WithLabelValues("delete", storageType, "not_found").Inc()
		return nil // Key doesn't exist, nothing to delete
	}
	defer slice.Free()

	// Parse value to get size and file info
	dataSize := int64(0)
	valueMsg := &pb.ValueMessage{}
	if err := proto.Unmarshal(slice.Data(), valueMsg); err == nil {
		storageType = pb.ValueType_name[int32(valueMsg.ValueType)]
		dataSize = valueMsg.ValueLength
		// Notify cleaner about size reduction
		s.notifyDelete(valueMsg.ValueLength)

		// Clean up files if necessary
		switch valueMsg.ValueType {
		case pb.ValueType_RAW_FILE:
			if err := s.deletionQueue.Add(valueMsg.RawFilePath); err != nil {
				zlog.Error().Err(err).Str("key", key).Str("file", valueMsg.RawFilePath).
					Msg("storage.DeleteKey: failed to add raw file to deletion queue")
			}
		case pb.ValueType_SEGMENT:
			// Update delete index to track this deletion for future garbage collection
			s.updateDeleteIndex(valueMsg.SegmentPath, valueMsg.ValueLength)
		}
	}

	wo := grocksdb.NewDefaultWriteOptions()
	batch := grocksdb.NewWriteBatch()

	// Delete key and its access index in a single batch
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

	if err := s.meta.Handle().Write(wo, batch); err != nil {
		metrics.StorageOperations.WithLabelValues("delete", storageType, "error").Inc()
		metrics.Errors.WithLabelValues("rocksdb", "delete").Inc()
		zlog.Error().Err(err).Str("key", key).Msg("storage.DeleteKey: db.Write error")
		// RocksDB errors are typically temporary
		return mapRocksDBError("DeleteKey", key, err)
	}

	metrics.StorageOperations.WithLabelValues("delete", storageType, "success").Inc()
	metrics.ObjectSize.WithLabelValues("delete").Observe(float64(dataSize))
	return nil
}

// Get retrieves the value for the given key from the database and returns an io.Reader for streaming
// Supports byte-range requests via start and end parameters (0 means no limit)
func (s *Storage) Get(key string, start, end int64) (io.Reader, bool, error) {
	storageType := "unknown"
	startTime := time.Now()
	defer func() {
		metrics.StorageOperationDuration.WithLabelValues("get", storageType).Observe(float64(time.Since(startTime).Milliseconds()))
	}()
	ro := metadata.CreateReadOptions(false, true)
	metaKey := keys.MakeMetadataKey(key)

	slice, err := s.meta.Handle().Get(ro, metaKey)
	if err != nil {
		metrics.StorageOperations.WithLabelValues("get", storageType, "error").Inc()
		metrics.Errors.WithLabelValues("rocksdb", "get").Inc()
		zlog.Error().Err(err).Str("key", key).Msg("storage.Get: db.Get error")
		// RocksDB errors are typically temporary
		return nil, false, mapRocksDBError("Get", key, err)
	}
	defer slice.Free()
	if !slice.Exists() {
		metrics.StorageOperations.WithLabelValues("get", storageType, "not_found").Inc()
		zlog.Debug().Str("key", key).Msg("storage.Get: not found in DB")
		return nil, false, nil
	}
	v := slice.Data()

	// Try to decode as proto ValueMessage
	valueMsg := &pb.ValueMessage{}
	err = proto.Unmarshal(v, valueMsg)
	if err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("storage.Get: failed to unmarshal proto ValueMessage - corruption detected")
		// Return corruption error without deleting the key
		// This preserves the corrupted data for debugging/recovery
		return nil, false, storageErrors.NewCorruptionError("Get", key, err)
	}

	zlog.Debug().Str("key", key).Msg("storage.Get: decoded proto ValueMessage")
	if valueMsg.Expiry > 0 && time.Now().Unix() >= valueMsg.Expiry {
		zlog.Debug().Str("key", key).Msg("storage.Get: key has expired, returning not found")
		// Don't delete the key here - let the background cleaner handle it
		// This avoids race conditions with the cleaner
		return nil, false, nil
	}

	// Refresh access time for LRU tracking. The updater is only present when
	// eviction is active and the policy is LRU; in FIFO mode it is nil, so reads
	// deliberately do not bump recency and cannot protect old data from eviction.
	if s.accessUpdater != nil {
		s.accessUpdater.UpdateNow(key)
	}

	var reader io.Reader
	storageType = pb.ValueType_name[int32(valueMsg.ValueType)]

	switch valueMsg.ValueType {
	case pb.ValueType_INLINE:
		reader = bytes.NewReader(valueMsg.Data)
	case pb.ValueType_SEGMENT:
		if r, err := s.segmentManager.ReadEntry(key, valueMsg.SegmentPath, valueMsg.SegmentOffset, valueMsg.ValueLength); err != nil {
			metrics.StorageOperations.WithLabelValues("get", storageType, "error").Inc()
			metrics.Errors.WithLabelValues(storageType, "get").Inc()
			zlog.Error().Err(err).Str("key", key).Str("segment", valueMsg.SegmentPath).Msg("storage.Get: failed to read segment")
			// File read errors are usually I/O errors, retryable for reads
			return nil, false, storageErrors.NewIORetryableError("Get", key, err)
		} else if r != nil {
			reader = r
		} else {
			return nil, false, nil
		}
	case pb.ValueType_RAW_FILE:
		r, err := s.fileManager.Read(valueMsg.RawFilePath, valueMsg.ValueLength)
		if err != nil {
			// A missing backing file for a never-compacted (large) object is a
			// dangling reference: the write did not survive an unclean shutdown
			// and no compactor will ever recreate it (issue #150). Issue a
			// conditional purge and return a retryable error: the internal Get
			// retry (retry.DoWithKey in GetLocal) then re-drives Get and serves
			// the authoritative state — the purge tombstone yields a clean miss,
			// or a concurrent Put's fresh value yields a hit. Letting the re-read
			// produce the result (rather than inferring a miss here) avoids both
			// a spurious miss when a Put raced this read and crash-looping on the
			// dangling key. We do not count this as an "error" because it is an
			// expected, self-healing condition, not a failure.
			//
			// Restricted to large objects (> compactThreshold): medium objects
			// are migrated to segments by the compactor, which briefly unlinks
			// the raw file before its RAW_FILE→SEGMENT CAS lands, so their ENOENT
			// is transient and a retry recovers the value from the segment.
			if errors.Is(err, os.ErrNotExist) && valueMsg.ValueLength > s.compactThreshold {
				zlog.Warn().Str("key", key).Str("file", valueMsg.RawFilePath).
					Msg("storage.Get: raw file missing for large object, purging dangling key")
				s.purgeDanglingRawFile(key, valueMsg.RawFilePath)
				return nil, false, storageErrors.NewIORetryableError("Get", key, err)
			}

			metrics.StorageOperations.WithLabelValues("get", storageType, "error").Inc()
			metrics.Errors.WithLabelValues(storageType, "get").Inc()
			zlog.Error().Err(err).Str("key", key).Str("file", valueMsg.RawFilePath).Msg("storage.Get: failed to read file")
			// Check if it's a lock error from file manager
			if err == files.ErrFileLocked {
				return nil, false, storageErrors.NewLockError("Get", key, err)
			}
			// File read errors are usually I/O errors, retryable for reads
			return nil, false, storageErrors.NewIORetryableError("Get", key, err)
		} else if r != nil {
			reader = r
		} else {
			return nil, false, nil
		}
	default:
		zlog.Error().Str("key", key).Int("value_type", int(valueMsg.ValueType)).Msg("storage.Get: unknown value type - corruption detected")
		// Return error for unknown value types
		return nil, false, storageErrors.NewCorruptionError("Get", key, fmt.Errorf("unknown value type: %d", valueMsg.ValueType))
	}

	metrics.StorageOperations.WithLabelValues("get", storageType, "success").Inc()
	metrics.StorageBytes.WithLabelValues("get", storageType).Add(float64(valueMsg.ValueLength))
	metrics.ObjectSize.WithLabelValues("get").Observe(float64(valueMsg.ValueLength))

	// Apply byte-range if specified (end > 0 means inclusive end position; end <= 0 means read to EOF)
	if start > 0 || end > 0 {
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

	// Apply end limit if specified (inclusive: end byte is included; end <= 0 means no limit)
	if br.end > 0 && br.pos > br.end {
		return 0, io.EOF
	}

	// Limit read size if we have an end boundary (inclusive)
	if br.end > 0 && br.pos+int64(len(p)) > br.end+1 {
		p = p[:br.end-br.pos+1]
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
	storageType := "unknown"
	start := time.Now()
	defer func() {
		metrics.StorageOperationDuration.WithLabelValues("put", storageType).Observe(float64(time.Since(start).Milliseconds()))
	}()

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
		metrics.StorageOperations.WithLabelValues("put", storageType, "error").Inc()
		metrics.Errors.WithLabelValues("io", "put").Inc()
		zlog.Error().Err(err).Str("key", key).Msg("storage.Put: failed to read value")
		return storageErrors.NewIOError("Put", key, err)
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
		storageType = "raw_file"
		// Combine the bytes we already read with the remaining reader and write via the segment manager
		multiReader := io.MultiReader(bytes.NewReader(firstChunk[:n]), body)
		filePath, checksum, bytesWritten, err := s.fileManager.Write(key, multiReader)
		if err != nil {
			metrics.StorageOperations.WithLabelValues("put", storageType, "error").Inc()
			metrics.Errors.WithLabelValues("file", "put").Inc()
			zlog.Error().Err(err).Str("key", key).Msg("storage.Put: failed to write to segment")
			switch {
			case isNoSpaceError(err):
				return storageErrors.NewStorageFullError("Put", err)
			case os.IsNotExist(err) || os.IsPermission(err):
				return storageErrors.NewIORetryableError("Put", key, err)
			default:
				return storageErrors.NewIOError("Put", key, err)
			}
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
			return storageErrors.NewInternalError("Put", err)
		}
		err = s.putLow(key, val, filePath, bytesWritten)
		if err == nil {
			metrics.StorageOperations.WithLabelValues("put", storageType, "success").Inc()
			metrics.StorageBytes.WithLabelValues("put", storageType).Add(float64(bytesWritten))
			metrics.ObjectSize.WithLabelValues("put").Observe(float64(valueMsg.ValueLength))
			s.notifyPut(bytesWritten)
		} else {
			metrics.StorageOperations.WithLabelValues("put", storageType, "error").Inc()
			metrics.Errors.WithLabelValues("rocksdb", "put").Inc()
		}
		if err != nil {
			// Map RocksDB write errors appropriately
			return mapRocksDBError("Put", key, err)
		}
		return nil
	}

	// Small value: we have read the entire value into firstChunk[:n]
	smallValue := firstChunk[:n]
	storageType = "inline"

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
		return storageErrors.NewInternalError("Put", err)
	}
	err = s.putLow(key, val, "", int64(n))
	if err == nil {
		metrics.StorageOperations.WithLabelValues("put", storageType, "success").Inc()
		metrics.StorageBytes.WithLabelValues("put", storageType).Add(float64(n))
		metrics.ObjectSize.WithLabelValues("put").Observe(float64(valueMsg.ValueLength))
		s.notifyPut(int64(n))
	} else {
		metrics.StorageOperations.WithLabelValues("put", storageType, "error").Inc()
		metrics.Errors.WithLabelValues("rocksdb", "put").Inc()
	}
	if err != nil {
		// Map RocksDB write errors appropriately
		return mapRocksDBError("Put", key, err)
	}
	return nil
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

// purgeDanglingRawFile tombstones a metadata entry whose backing raw file no
// longer exists on disk (a dangling reference left by a write that did not
// survive an unclean shutdown).
//
// The purge is a conditional merge (see merge.MakeRawFilePurgeOperand /
// merge.mergeMetadataCAS): the key is tombstoned only when the metadata still
// references rawFilePath, so a concurrent Put — which always writes a fresh
// path — is never clobbered. The caller restricts this to large, never-compacted
// objects, so no compactor is racing to migrate the file into a segment.
//
// It is best-effort and idempotent: the caller returns a retryable error, so the
// internal Get retry re-reads the (now tombstoned, or concurrently rewritten)
// metadata and serves the authoritative result. Disk-usage accounting is
// intentionally not adjusted here — the dangling object's bytes stay counted
// until the cleaner recomputes the total at next startup. That is a bounded
// over-count in the safe direction (it can only make eviction more aggressive,
// never let usage exceed the quota) and avoids the double-decrement that
// side-channel accounting would cause when this races a Delete/eviction.
func (s *Storage) purgeDanglingRawFile(key, rawFilePath string) {
	operand, err := merge.MakeRawFilePurgeOperand(rawFilePath)
	if err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("storage.purgeDanglingRawFile: failed to build purge operand")
		return
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	if err := s.meta.Handle().Merge(wo, keys.MakeMetadataKey(key), operand); err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("storage.purgeDanglingRawFile: failed to purge dangling key")
	}
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

// Helper functions for error mapping

// isNoSpaceError checks if an error indicates disk space exhaustion
func isNoSpaceError(err error) bool {
	if err == nil {
		return false
	}
	// Check for ENOSPC (No space left on device)
	if pathErr, ok := err.(*os.PathError); ok {
		if errno, ok := pathErr.Err.(syscall.Errno); ok {
			return errno == syscall.ENOSPC
		}
	}
	return false
}

// mapRocksDBError maps RocksDB errors to appropriate storage errors
func mapRocksDBError(op, key string, err error) error {
	if err == nil {
		return nil
	}

	// Check for specific RocksDB error conditions
	errStr := err.Error()
	switch {
	case errStr == "rocksdb: not found":
		return storageErrors.NewNotFoundError(op, key)
	case errStr == "rocksdb: corruption":
		return storageErrors.NewCorruptionError(op, key, err)
	case errStr == "rocksdb: io error":
		// Write operations are not retryable for I/O errors
		return storageErrors.NewIOError(op, key, err)
	default:
		// Most RocksDB errors are temporary and can be retried
		return storageErrors.NewTemporaryError(op, key, err)
	}
}
