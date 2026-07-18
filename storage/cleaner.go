// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"sync"
	"sync/atomic"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

const (
	// accessBucketCleanupInterval is the interval at which we clean up old access buckets
	accessBucketCleanupInterval = 24 * time.Hour

	// accessBucketCleanupThreshold is the threshold at which we clean up old access buckets
	accessBucketCleanupThreshold = 30 * 24 * time.Hour
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
// It performs an initial size calculation synchronously to establish accurate baseline
// before any concurrent operations can modify the size
func (c *Cleaner) Start() {
	// Calculate initial size synchronously to avoid race with concurrent puts
	c.calculateTotalSize()
	c.initialized.Store(true)

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

	// Track when we last cleaned up old buckets
	lastBucketCleanup := time.Now()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpiredKeys()
			if c.maxDiskUsage > 0 {
				c.enforceDiskLimit()
			}

			// Mirror the live totals (maintained on every write/evict) back to the
			// gauges, so ocache_disk_usage_bytes and the segment gauges track the
			// current contents instead of reflecting only the value at startup.
			c.refreshSizeMetrics()
			if c.storage != nil && c.storage.segmentManager != nil {
				c.storage.segmentManager.RefreshMetrics()
			}

			// Periodically clean up old access buckets regardless of disk limits
			// to prevent unbounded growth of the access index
			if time.Since(lastBucketCleanup) > accessBucketCleanupInterval {
				c.cleanupOldBuckets(accessBucketCleanupThreshold)
				lastBucketCleanup = time.Now()
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
	var bytesFreed int64

	// Track cleaner run
	metrics.CleanerRuns.WithLabelValues("ttl").Inc()

	ro := metadata.CreateReadOptions(false, false)
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
			// Use secondary index to delete bucketed access entry
			bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
			if slice, err := c.storage.meta.Handle().Get(ro, bucketIndexKey); err == nil && slice.Exists() {
				bucketKey := slice.Data()
				batch.Delete(bucketKey)
				slice.Free()
			}
			batch.Delete(bucketIndexKey)
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
			// Use secondary index to delete bucketed access entry
			bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
			if slice, err := c.storage.meta.Handle().Get(ro, bucketIndexKey); err == nil && slice.Exists() {
				bucketKey := slice.Data()
				batch.Delete(bucketKey)
				slice.Free()
			}
			batch.Delete(bucketIndexKey)
			cleaned++
			zlog.Debug().Str("key", key).Int64("expiry", valueMsg.Expiry).Int64("now", now).Msg("cleaner: deleting expired key")

			// Track bytes freed
			bytesFreed += valueMsg.ValueLength

			// Queue associated files for deletion
			switch valueMsg.ValueType {
			case pb.ValueType_RAW_FILE:
				if err := c.storage.deletionQueue.Add(valueMsg.RawFilePath); err != nil {
					zlog.Error().Err(err).Str("path", valueMsg.RawFilePath).Msg("cleaner: failed to queue raw file for deletion")
				}
			case pb.ValueType_SEGMENT:
				// Update delete index to track this deletion for future garbage collection
				c.storage.updateDeleteIndex(valueMsg.SegmentPath, valueMsg.ValueLength)
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

	// Record metrics
	duration := time.Since(start)
	metrics.CleanerDuration.WithLabelValues("ttl").Observe(float64(duration.Milliseconds()))
	metrics.CleanerKeysDeleted.WithLabelValues("ttl", "expired").Add(float64(cleaned))
	metrics.CleanerBytesFreed.WithLabelValues("ttl").Add(float64(bytesFreed))

	zlog.Info().
		Int("cleaned", cleaned).
		Int64("bytes_freed", bytesFreed).
		Dur("duration_ms", duration).
		Msg("cleaner: TTL cleanup completed")
}

// calculateTotalSize calculates the total size of stored data
func (c *Cleaner) calculateTotalSize() {
	start := time.Now()
	var totalSize int64

	ro := metadata.CreateReadOptions(false, false)
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

	// Publish the freshly computed size to the gauges.
	c.refreshSizeMetrics()

	zlog.Info().
		Int64("total_size", totalSize).
		Dur("duration_ms", time.Since(start)).
		Msg("cleaner: calculated total storage size")
}

// enforceDiskLimit evicts keys if disk usage exceeds the limit
func (c *Cleaner) enforceDiskLimit() {
	start := time.Now()
	currentSize := c.totalSize.Load()
	if currentSize <= c.maxDiskUsage {
		return
	}

	targetSize := int64(float64(c.maxDiskUsage) * 0.9) // Target 90% of max
	needToEvict := currentSize - targetSize

	// Track LRU eviction run
	metrics.CleanerRuns.WithLabelValues("lru").Inc()

	zlog.Info().
		Int64("current", currentSize).
		Int64("max", c.maxDiskUsage).
		Int64("need_to_evict", needToEvict).
		Msg("cleaner: enforcing disk usage limit with LRU eviction")

	evicted := c.evictLRUKeys(needToEvict)

	// Record metrics
	duration := time.Since(start)
	metrics.CleanerDuration.WithLabelValues("lru").Observe(float64(duration.Milliseconds()))
	metrics.LRUEvictions.Add(float64(evicted))
}

// UpdateSize updates the tracked total size when keys are added/removed
func (c *Cleaner) UpdateSize(delta int64) {
	c.totalSize.Add(delta)
}

// refreshSizeMetrics publishes the current tracked total size to the disk-usage
// gauges. Cheap (reads an atomic) and safe to call on every cleaner tick.
func (c *Cleaner) refreshSizeMetrics() {
	total := c.totalSize.Load()
	metrics.DiskUsageBytes.WithLabelValues("total").Set(float64(total))
	if c.maxDiskUsage > 0 {
		metrics.DiskUsageRatio.Set(float64(total) / float64(c.maxDiskUsage))
	}
}

// TotalSize returns the current tracked logical cache size in bytes (sum of
// stored object lengths), maintained live on every write and eviction.
func (c *Cleaner) TotalSize() int64 {
	return c.totalSize.Load()
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
