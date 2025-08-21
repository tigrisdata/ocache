package storage

import (
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/keys"
	"google.golang.org/protobuf/proto"
)

// evictLRUKeys evicts the least recently used keys using bucket iteration
// This implementation is scalable to millions of keys as it doesn't load all keys into memory
func (c *Cleaner) evictLRUKeys(targetBytes int64) {
	start := time.Now()

	var evicted int64
	var evictedCount int
	var processedKeys int

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Start iterating from the oldest bucket
	it := c.storage.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := GetOldestAccessBucketPrefix()
	zlog.Info().
		Int64("target_bytes", targetBytes).
		Str("prefix", string(prefix)).
		Msg("cleaner: starting scalable LRU eviction")

	// Iterate through all bucketed access entries from oldest to newest
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: LRU eviction interrupted by shutdown")
			return
		default:
		}

		// Check if we've evicted enough
		if evicted >= targetBytes {
			break
		}

		keyBytes := it.Key().Data()

		// Parse the bucketed key to get the original key and access time
		originalKey, accessTime, err := keys.ParseBucketedAccessKey(keyBytes)
		if err != nil {
			zlog.Debug().Err(err).Str("key", string(keyBytes)).Msg("cleaner: failed to parse bucketed key")

			it.Key().Free()
			it.Value().Free()
			continue
		}

		processedKeys++

		zlog.Debug().
			Str("key", originalKey).
			Time("last_access", accessTime).
			Int("processed", processedKeys).
			Msg("cleaner: processing key for LRU eviction")

		// Get the metadata for this key
		metaKey := keys.MakeMetadataKey(originalKey)
		slice, err := c.storage.meta.Handle().Get(ro, metaKey)
		if err != nil || !slice.Exists() {
			// Key doesn't exist in metadata, clean up the access entry
			batch.Delete(keyBytes)

			zlog.Debug().Str("key", originalKey).Msg("cleaner: removing orphaned access entry")

			it.Key().Free()
			it.Value().Free()

			continue
		}

		// Parse the metadata
		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(slice.Data(), valueMsg); err != nil {
			// Failed to parse metadata, clean up the metadata and access entry
			batch.Delete(metaKey)
			batch.Delete(keyBytes)

			zlog.Debug().Str("key", originalKey).Msg("cleaner: removing orphaned metadata and access entry during LRU eviction")

			slice.Free()
			it.Key().Free()
			it.Value().Free()

			continue
		}
		slice.Free()

		// If the key is expired, we don't need to delete it as it will be deleted by the background cleaner
		now := time.Now().Unix()
		if valueMsg.Expiry > 0 && now >= valueMsg.Expiry {
			zlog.Debug().Str("key", originalKey).Msg("cleaner: skipping expired key in LRU")

			it.Key().Free()
			it.Value().Free()

			continue
		}

		// Delete the key and its access entry
		batch.Delete(metaKey)
		batch.Delete(keyBytes)

		// Delete secondary index entry
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(originalKey)
		batch.Delete(bucketIndexKey)

		evicted += valueMsg.ValueLength
		evictedCount++

		zlog.Debug().
			Str("key", originalKey).
			Int64("size", valueMsg.ValueLength).
			Int64("evicted_so_far", evicted).
			Int("count", evictedCount).
			Msg("cleaner: evicting key")

		// Delete associated files
		switch valueMsg.ValueType {
		case pb.ValueType_RAW_FILE:
			if err := c.storage.fileManager.Remove(valueMsg.RawFilePath); err != nil {
				zlog.Error().Err(err).Str("path", valueMsg.RawFilePath).Msg("cleaner: failed to delete raw file during LRU eviction")
			}
		case pb.ValueType_SEGMENT:
			// Update delete index to track this deletion for future garbage collection
			c.storage.updateDeleteIndex(valueMsg.SegmentPath, valueMsg.ValueLength)
		}

		it.Key().Free()
		it.Value().Free()

		// Write batch periodically to avoid large batches
		if batch.Count() >= 1000 {
			// Check if we're shutting down before writing
			select {
			case <-c.closeCh:
				zlog.Info().Msg("cleaner: LRU eviction interrupted by shutdown")
				return
			default:
			}

			if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
				zlog.Error().Err(err).Msg("cleaner: failed to write LRU eviction batch")
			}
			batch.Clear()

			zlog.Info().
				Int("evicted_count", evictedCount).
				Int64("evicted_bytes", evicted).
				Int64("target_bytes", targetBytes).
				Float64("progress_pct", float64(evicted)*100/float64(targetBytes)).
				Msg("cleaner: LRU eviction progress")
		}
	}

	// Write final batch
	if batch.Count() > 0 {
		if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Msg("cleaner: failed to write final LRU eviction batch")
		}
	}

	// Update stats
	c.evictedKeys.Add(int64(evictedCount))
	c.totalSize.Add(-evicted)

	zlog.Info().
		Int("count", evictedCount).
		Int64("bytes", evicted).
		Int64("target", targetBytes).
		Int("keys_examined", processedKeys).
		Dur("duration", time.Since(start)).
		Msg("cleaner: scalable LRU eviction completed")
}

// cleanupOldBuckets removes access entries from buckets older than the specified duration
// This prevents unbounded growth of the access index
func (c *Cleaner) cleanupOldBuckets(olderThan time.Duration) {
	start := time.Now()
	deleted := 0

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Calculate cutoff time
	cutoff := time.Now().Add(-olderThan)
	cutoffBucket := keys.GetBucketedAccessKey(cutoff)

	it := c.storage.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := GetOldestAccessBucketPrefix()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		keyBytes := it.Key().Data()

		// Extract bucket from key
		bucket := keys.ExtractAccessBucketFromKey(keyBytes)
		if bucket == "" || bucket >= cutoffBucket {
			// We've reached buckets that are not old enough
			it.Key().Free()
			it.Value().Free()
			break
		}

		// Delete this old entry
		batch.Delete(keyBytes)
		deleted++

		it.Key().Free()
		it.Value().Free()

		// Write batch periodically
		if batch.Count() >= 1000 {
			if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
				zlog.Error().Err(err).Msg("cleaner: failed to write bucket cleanup batch")
			}
			batch.Clear()
		}
	}

	// Write final batch
	if batch.Count() > 0 {
		if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Msg("cleaner: failed to write final bucket cleanup batch")
		}
	}

	if deleted > 0 {
		zlog.Info().
			Int("deleted", deleted).
			Dur("older_than", olderThan).
			Dur("duration", time.Since(start)).
			Msg("cleaner: cleaned up old access buckets")
	}
}
