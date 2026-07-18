// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// evictFIFOKeys evicts oldest-written keys until at least targetBytes have been
// freed. It walks the FIFO index (!fifo/<write_nano>/<key>), which sorts
// oldest-first, and for each entry:
//
//   - metadata missing  -> the key was deleted, TTL-expired, or superseded by a
//     later overwrite entry; the index entry is an orphan and is reclaimed here.
//   - metadata present, expired -> left for the TTL cleaner.
//   - metadata present, live -> evicted (metadata + index entry + backing file).
//
// The existence check also reclaims any orphan entries left by a rare race
// (e.g. a concurrent overwrite). Under normal operation Put/Delete/TTL maintain
// exactly one entry per live key via the secondary index (see
// writeFifoIndexEntry / stageEvictionIndexDeletes). Returns the number of keys
// evicted.
func (c *Cleaner) evictFIFOKeys(targetBytes int64) int {
	start := time.Now()

	var evicted int64
	var evictedCount int
	var processedKeys int

	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	it := c.storage.meta.Handle().NewIterator(ro)
	defer it.Close()

	// commit writes any staged deletes. It must run before every return path,
	// including shutdown: evicted keys' raw files are queued for deletion
	// immediately (deletionQueue.Add), so dropping the batched metadata deletes
	// would leave live metadata pointing at deleted files (the dangling raw-file
	// class reconciled by #150/#152).
	commit := func() {
		if batch.Count() == 0 {
			return
		}
		if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Msg("cleaner: failed to write FIFO eviction batch")
		}
		batch.Clear()
	}

	prefix := keys.GetFifoIndexPrefix()
	zlog.Info().
		Int64("target_bytes", targetBytes).
		Msg("cleaner: starting FIFO eviction")

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: FIFO eviction interrupted by shutdown")
			commit()
			return evictedCount
		default:
		}

		if evicted >= targetBytes {
			break
		}

		keyBytes := it.Key().Data()

		originalKey, err := keys.ParseFifoIndexKey(keyBytes)
		if err != nil {
			zlog.Debug().Err(err).Str("key", string(keyBytes)).Msg("cleaner: failed to parse fifo index key")
			it.Key().Free()
			it.Value().Free()
			continue
		}
		processedKeys++

		// Look up the key's metadata.
		metaKey := keys.MakeMetadataKey(originalKey)
		slice, err := c.storage.meta.Handle().Get(ro, metaKey)
		if err != nil {
			// Transient read error: skip this entry. Do NOT treat it as an orphan
			// — deleting the entry would make a live key permanently invisible to
			// FIFO eviction (reads never re-index it). Retry on the next pass.
			zlog.Debug().Err(err).Str("key", originalKey).Msg("cleaner: fifo eviction metadata lookup failed; skipping")
			it.Key().Free()
			it.Value().Free()
			continue
		}
		if !slice.Exists() {
			// Orphan index entry (key deleted/expired, or superseded by a newer
			// overwrite entry) — reclaim it.
			slice.Free()
			batch.Delete(keyBytes)
			it.Key().Free()
			it.Value().Free()
			continue
		}

		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(slice.Data(), valueMsg); err != nil {
			// Corrupt metadata: drop the metadata, the index entry, and the
			// back-reference (which would otherwise dangle).
			batch.Delete(metaKey)
			batch.Delete(keyBytes)
			batch.Delete(keys.MakeFifoBackrefKey(originalKey))
			slice.Free()
			it.Key().Free()
			it.Value().Free()
			continue
		}
		slice.Free()

		// Leave expired keys for the TTL cleaner (it also frees backing files).
		now := time.Now().Unix()
		if valueMsg.Expiry > 0 && now >= valueMsg.Expiry {
			it.Key().Free()
			it.Value().Free()
			continue
		}

		// Evict the key, its FIFO index entry, and its back-reference.
		batch.Delete(metaKey)
		batch.Delete(keyBytes)
		batch.Delete(keys.MakeFifoBackrefKey(originalKey))

		evicted += valueMsg.ValueLength
		evictedCount++

		// Delete associated files (mirrors evictLRUKeys / cleanupExpiredKeys).
		switch valueMsg.ValueType {
		case pb.ValueType_RAW_FILE:
			if err := c.storage.deletionQueue.Add(valueMsg.RawFilePath); err != nil {
				zlog.Error().Err(err).Str("path", valueMsg.RawFilePath).Msg("cleaner: failed to queue raw file for deletion during FIFO eviction")
			}
		case pb.ValueType_SEGMENT:
			c.storage.updateDeleteIndex(valueMsg.SegmentPath, valueMsg.ValueLength)
		}

		it.Key().Free()
		it.Value().Free()

		// Write batch periodically to avoid large batches.
		if batch.Count() >= 1000 {
			select {
			case <-c.closeCh:
				zlog.Info().Msg("cleaner: FIFO eviction interrupted by shutdown")
				commit()
				return evictedCount
			default:
			}
			commit()
		}
	}

	// Write final batch.
	commit()

	c.evictedKeys.Add(int64(evictedCount))
	c.totalSize.Add(-evicted)

	metrics.CleanerKeysDeleted.WithLabelValues("fifo", "disk_limit").Add(float64(evictedCount))
	metrics.CleanerBytesFreed.WithLabelValues("fifo").Add(float64(evicted))

	zlog.Info().
		Int("count", evictedCount).
		Int64("bytes", evicted).
		Int64("target", targetBytes).
		Int("processed", processedKeys).
		Dur("duration_ms", time.Since(start)).
		Msg("cleaner: FIFO eviction complete")

	return evictedCount
}
