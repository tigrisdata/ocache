// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
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
//   - metadata missing -> the key was deleted or TTL-expired; the entry is an
//     orphan and is reclaimed here.
//   - metadata present, expired -> left for the TTL cleaner.
//   - metadata present but the back-reference points elsewhere -> this entry is
//     a superseded duplicate (from a concurrent overwrite, or a Put whose
//     back-reference lookup failed); reclaim just the entry, keep the key.
//   - metadata present and the back-reference points here -> the current, live
//     entry; evict (metadata + index entry + back-reference + backing file).
//
// The back-reference check is what makes eviction correct without a per-key
// lock: a stale duplicate never evicts the freshly-rewritten value. Under normal
// operation Put/Delete/TTL keep exactly one entry per live key (see
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

		// Bound the in-memory batch. Checked here (not only after an eviction) so
		// that runs of orphan/corrupt entries — which continue without evicting —
		// still flush periodically instead of accumulating one giant batch.
		if batch.Count() >= 1000 {
			commit()
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

		// Verify this entry against the key's back-reference before evicting. A
		// concurrent overwrite (Put takes no per-key lock), or a Put whose
		// back-reference lookup failed, can leave a stale duplicate entry while
		// the back-reference points at the newer value. Evicting via the stale
		// (older-timestamped) entry would discard the freshly-rewritten value at
		// its old position, out of FIFO order.
		//
		// Only treat this entry as superseded when the back-reference EXISTS and
		// points somewhere else — then reclaim just the entry and keep the key. If
		// the back-reference is absent, this entry is the key's authoritative (or
		// only) index entry: fall through and evict via it, rather than deleting
		// it and stranding a live key with no eviction entry.
		backref := keys.MakeFifoBackrefKey(originalKey)
		cur, err := c.storage.meta.Handle().Get(ro, backref)
		if err != nil {
			// Can't verify — skip rather than risk evicting a live key via a
			// possibly-stale entry. Retry on the next pass.
			it.Key().Free()
			it.Value().Free()
			continue
		}
		superseded := cur.Exists() && !bytes.Equal(cur.Data(), keyBytes)
		cur.Free()
		if superseded {
			batch.Delete(keyBytes)
			it.Key().Free()
			it.Value().Free()
			continue
		}

		// Evict the key, its FIFO index entry, and its back-reference.
		batch.Delete(metaKey)
		batch.Delete(keyBytes)
		batch.Delete(backref)

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
