package storage

import (
	"sync"
	"sync/atomic"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/keys"
	"google.golang.org/protobuf/proto"
)

// keyEntry represents a key to be checked for expiration
type keyEntry struct {
	keyBytes []byte
	userKey  string
	value    []byte
}

// Cleaner is responsible for background TTL cleanup and LRU eviction
type Cleaner struct {
	storage      *Storage
	interval     time.Duration
	maxDiskUsage int64
	initialized  atomic.Bool
	concurrency  int

	// stats
	totalSize   atomic.Int64
	cleanedKeys atomic.Int64
	evictedKeys atomic.Int64

	// background loop coordination
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewCleaner creates a new Cleaner for background TTL cleanup and LRU eviction
func NewCleaner(storage *Storage, interval time.Duration, maxDiskUsage int64) *Cleaner {
	return &Cleaner{
		storage:      storage,
		interval:     interval,
		maxDiskUsage: maxDiskUsage,
		concurrency:  DefaultTTLConcurrency,
		closeCh:      make(chan struct{}),
	}
}

// NewCleanerWithConcurrency creates a new Cleaner with specified concurrency
func NewCleanerWithConcurrency(storage *Storage, interval time.Duration, maxDiskUsage int64, concurrency int) *Cleaner {
	if concurrency <= 0 {
		concurrency = DefaultTTLConcurrency
	}
	return &Cleaner{
		storage:      storage,
		interval:     interval,
		maxDiskUsage: maxDiskUsage,
		concurrency:  concurrency,
		closeCh:      make(chan struct{}),
	}
}

// Start launches the background cleanup goroutine
func (c *Cleaner) Start() {
	c.wg.Add(1)
	go c.cleanupLoop()
}

// Close stops the background cleanup loop and waits for it to exit
func (c *Cleaner) Close() {
	if c == nil {
		return
	}
	close(c.closeCh)

	// Wait with timeout to avoid hanging forever
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		zlog.Info().Msg("cleaner: shutdown completed")
	case <-time.After(5 * time.Second):
		zlog.Warn().Msg("cleaner: shutdown timed out after 5 seconds")
	}
}

// cleanupLoop runs periodic TTL cleanup and eviction checks
func (c *Cleaner) cleanupLoop() {
	defer c.wg.Done()

	zlog.Info().Msg("cleaner: starting background cleanup loop")

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Run initial size calculation
	c.calculateTotalSize()
	c.initialized.Store(true)

	for {
		select {
		case <-ticker.C:
			c.cleanupExpiredKeys()
			if c.maxDiskUsage > 0 {
				c.enforceDiskLimit()
			}
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: background loop stopping")
			return
		}
	}
}

// cleanupExpiredKeys scans for and removes expired keys using multiple workers
func (c *Cleaner) cleanupExpiredKeys() {
	start := time.Now()

	// Collect all keys to check
	var entries []keyEntry
	ro := grocksdb.NewDefaultReadOptions()
	it := c.storage.meta.Handle().NewIterator(ro)

	for it.SeekToFirst(); it.Valid(); it.Next() {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: cleanup interrupted by shutdown during collection")
			it.Close()
			return
		default:
		}

		keyBytes := it.Key().Data()

		// Only process user metadata keys
		if !keys.IsMetadataKey(keyBytes) {
			it.Key().Free()
			it.Value().Free()
			continue
		}

		// Extract the original user key
		userKey := keys.ExtractUserKey(keyBytes)
		value := it.Value().Data()

		// Store copies of the data
		entries = append(entries, keyEntry{
			keyBytes: append([]byte(nil), keyBytes...),
			userKey:  userKey,
			value:    append([]byte(nil), value...),
		})

		it.Key().Free()
		it.Value().Free()
	}
	it.Close()

	if len(entries) == 0 {
		zlog.Info().Msg("cleaner: no keys to check for expiry")
		return
	}

	// Create channels for work distribution
	workCh := make(chan keyEntry, len(entries))
	resultCh := make(chan *grocksdb.WriteBatch, c.concurrency)

	// Start worker goroutines
	var workerWg sync.WaitGroup
	for i := 0; i < c.concurrency; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			c.cleanupWorker(workerID, workCh, resultCh)
		}(i)
	}

	// Send work to workers
	go func() {
		for _, entry := range entries {
			select {
			case workCh <- entry:
			case <-c.closeCh:
				close(workCh)
				return
			}
		}
		close(workCh)
	}()

	// Start result aggregator
	var aggregatorWg sync.WaitGroup
	aggregatorWg.Add(1)
	totalCleaned := int64(0)
	go func() {
		defer aggregatorWg.Done()
		cleaned := c.aggregateResults(resultCh)
		atomic.AddInt64(&totalCleaned, cleaned)
	}()

	// Wait for workers to finish
	workerWg.Wait()
	close(resultCh)

	// Wait for aggregator to finish
	aggregatorWg.Wait()

	c.cleanedKeys.Add(totalCleaned)

	zlog.Info().
		Int64("cleaned", totalCleaned).
		Int("workers", c.concurrency).
		Dur("duration", time.Since(start)).
		Msg("cleaner: TTL cleanup completed")
}

// cleanupWorker processes key entries and checks for expiration
func (c *Cleaner) cleanupWorker(workerID int, workCh <-chan keyEntry, resultCh chan<- *grocksdb.WriteBatch) {
	batch := grocksdb.NewWriteBatch()
	now := time.Now().Unix()
	localCleaned := 0

	for entry := range workCh {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Int("worker", workerID).Msg("cleaner: worker interrupted by shutdown")
			if batch.Count() > 0 {
				resultCh <- batch
			} else {
				batch.Destroy()
			}
			return
		default:
		}

		// Try to decode as proto ValueMessage
		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(entry.value, valueMsg); err != nil {
			// Invalid entry, delete it
			batch.Delete(entry.keyBytes)
			// Also delete access index
			accessKey := MakeAccessIndexKey(entry.userKey)
			batch.Delete(accessKey)
			localCleaned++
		} else if valueMsg.Expiry > 0 && now >= valueMsg.Expiry {
			// Key is expired
			batch.Delete(entry.keyBytes)
			// Also delete access index
			accessKey := MakeAccessIndexKey(entry.userKey)
			batch.Delete(accessKey)
			localCleaned++

			zlog.Debug().
				Int("worker", workerID).
				Str("key", entry.userKey).
				Int64("expiry", valueMsg.Expiry).
				Int64("now", now).
				Msg("cleaner: deleting expired key")

			// Queue associated files for deletion
			switch valueMsg.ValueType {
			case pb.ValueType_RAW_FILE:
				if err := c.storage.deletionQueue.Add(valueMsg.RawFilePath); err != nil {
					zlog.Error().
						Int("worker", workerID).
						Err(err).
						Str("path", valueMsg.RawFilePath).
						Msg("cleaner: failed to queue raw file for deletion")
				}
			}
		}

		// Send batch when it reaches a threshold
		if batch.Count() >= 100 {
			resultCh <- batch
			batch = grocksdb.NewWriteBatch()
		}
	}

	// Send final batch if it has any operations
	if batch.Count() > 0 {
		resultCh <- batch
	} else {
		batch.Destroy()
	}

	zlog.Debug().
		Int("worker", workerID).
		Int("cleaned", localCleaned).
		Msg("cleaner: worker completed")
}

// aggregateResults collects batches from workers and writes them to RocksDB
func (c *Cleaner) aggregateResults(resultCh <-chan *grocksdb.WriteBatch) int64 {
	wo := grocksdb.NewDefaultWriteOptions()
	totalCleaned := int64(0)

	for batch := range resultCh {
		if batch.Count() > 0 {
			// Check if we're shutting down before writing
			select {
			case <-c.closeCh:
				batch.Destroy()
				zlog.Info().Msg("cleaner: result aggregation interrupted by shutdown")
				return totalCleaned
			default:
			}

			totalCleaned += int64(batch.Count() / 2) // Each key deletion has 2 operations (key + access index)

			if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
				zlog.Error().Err(err).Msg("cleaner: failed to write deletion batch")
			}
		}
		batch.Destroy()
	}

	return totalCleaned
}

// calculateTotalSize calculates the total size of stored data
func (c *Cleaner) calculateTotalSize() {
	var totalSize int64

	ro := grocksdb.NewDefaultReadOptions()
	it := c.storage.meta.Handle().NewIterator(ro)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: size calculation interrupted by shutdown")
			return
		default:
		}
		keyBytes := it.Key().Data()

		// Only process user metadata keys
		if !keys.IsMetadataKey(keyBytes) {
			// Skip all non-metadata keys (including other internal keys)
			it.Key().Free()
			it.Value().Free()
			continue
		}

		value := it.Value().Data()

		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(value, valueMsg); err == nil {
			totalSize += valueMsg.ValueLength
		}

		it.Key().Free()
		it.Value().Free()
	}

	c.totalSize.Store(totalSize)
	zlog.Info().Int64("total_size", totalSize).Msg("cleaner: calculated total storage size")
}

// enforceDiskLimit evicts keys if disk usage exceeds the limit
func (c *Cleaner) enforceDiskLimit() {
	currentSize := c.totalSize.Load()
	if currentSize <= c.maxDiskUsage {
		return
	}

	targetSize := int64(float64(c.maxDiskUsage) * 0.9) // Target 90% of max
	needToEvict := currentSize - targetSize

	zlog.Info().
		Int64("current", currentSize).
		Int64("max", c.maxDiskUsage).
		Int64("need_to_evict", needToEvict).
		Msg("cleaner: enforcing disk usage limit with LRU eviction")

	c.evictLRUKeys(needToEvict)
}

// UpdateSize updates the tracked total size when keys are added/removed
func (c *Cleaner) UpdateSize(delta int64) {
	c.totalSize.Add(delta)
}

// WaitForInitialization waits until the cleaner has completed its initial size calculation
func (c *Cleaner) WaitForInitialization() {
	for !c.initialized.Load() {
		time.Sleep(10 * time.Millisecond)
	}
}

// Stats returns cleaner statistics
func (c *Cleaner) Stats() (cleaned, evicted int64) {
	return c.cleanedKeys.Load(), c.evictedKeys.Load()
}
