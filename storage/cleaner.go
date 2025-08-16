package storage

import (
	"sync"
	"sync/atomic"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/keys"
	"google.golang.org/protobuf/proto"
)

// Cleaner is responsible for background TTL cleanup and LRU eviction
type Cleaner struct {
	storage      *Storage
	interval     time.Duration
	maxDiskUsage int64
	initialized  atomic.Bool

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

// cleanupExpiredKeys scans for and removes expired keys
func (c *Cleaner) cleanupExpiredKeys() {
	start := time.Now()
	cleaned := 0

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	it := c.storage.meta.Handle().NewIterator(ro)
	defer it.Close()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	now := time.Now().Unix()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: cleanup interrupted by shutdown")
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

		// Extract the original user key
		key := keys.ExtractUserKey(keyBytes)

		value := it.Value().Data()

		// Try to decode as proto ValueMessage
		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(value, valueMsg); err != nil {
			// Invalid entry, delete it
			batch.Delete(keyBytes)
			// Also delete access index
			accessKey := MakeAccessIndexKey(key)
			batch.Delete(accessKey)
			cleaned++
			it.Key().Free()
			it.Value().Free()
			continue
		}

		// Check if expired
		if valueMsg.Expiry > 0 {
			zlog.Debug().Str("key", key).Int64("expiry", valueMsg.Expiry).Int64("now", now).Bool("expired", now >= valueMsg.Expiry).Msg("cleaner: checking expiry")
		}
		if valueMsg.Expiry > 0 && now >= valueMsg.Expiry {
			batch.Delete(keyBytes)
			// Also delete access index
			accessKey := MakeAccessIndexKey(key)
			batch.Delete(accessKey)
			cleaned++
			zlog.Debug().Str("key", key).Int64("expiry", valueMsg.Expiry).Int64("now", now).Msg("cleaner: deleting expired key")

			// Queue associated files for deletion
			switch valueMsg.ValueType {
			case pb.ValueType_RAW_FILE:
				if err := c.storage.deletionQueue.Add(valueMsg.RawFilePath); err != nil {
					zlog.Error().Err(err).Str("path", valueMsg.RawFilePath).Msg("cleaner: failed to queue raw file for deletion")
				}
			}
		}

		it.Key().Free()
		it.Value().Free()

		// Write batch periodically to avoid large batches
		if batch.Count() >= 1000 {
			// Check if we're shutting down before writing
			select {
			case <-c.closeCh:
				zlog.Info().Msg("cleaner: cleanup interrupted by shutdown")
				return
			default:
			}

			if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
				zlog.Error().Err(err).Msg("cleaner: failed to write deletion batch")
			}
			batch.Clear()
		}
	}

	// Write final batch
	if batch.Count() > 0 {
		if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Msg("cleaner: failed to write final deletion batch")
		}
	}

	c.cleanedKeys.Add(int64(cleaned))

	zlog.Info().
		Int("cleaned", cleaned).
		Dur("duration", time.Since(start)).
		Msg("cleaner: TTL cleanup completed")
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
