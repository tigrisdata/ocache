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

// evictionIndex abstracts the per-policy ordered eviction index that
// evictByIndex walks oldest-first. LRU (access buckets) and FIFO (write order)
// differ only in these fields; the eviction algorithm itself is identical, so it
// lives once in evictByIndex rather than being duplicated per policy.
type evictionIndex struct {
	// policy is the metric label and log tag ("lru"/"fifo").
	policy string
	// prefix seeks the ordered index, which sorts oldest-eviction-candidate
	// first.
	prefix []byte
	// parseKey extracts the original user key from an index entry key.
	parseKey func(entryKey []byte) (string, error)
	// backrefKey returns the secondary-index key holding the user key's current
	// (authoritative) ordered entry, used to detect and skip superseded
	// duplicate entries left by a concurrent overwrite.
	backrefKey func(userKey string) []byte
}

// evictByIndex evicts entries from an ordered eviction index, oldest-first,
// until at least targetBytes have been freed or the index is exhausted. It is
// the single implementation behind both LRU and FIFO disk-limit eviction (they
// supply different evictionIndex descriptors); keeping it unified guarantees
// both policies share the same correctness invariants — notably the
// back-reference supersede check and the shutdown-safe batch commit. Returns the
// number of keys evicted.
//
// For each index entry:
//
//   - metadata read error -> transient; skip and retry next pass. Never treated
//     as an orphan: deleting the entry would make a live key permanently
//     invisible to eviction.
//   - metadata missing -> the key was deleted/expired, or this is a superseded
//     duplicate; the entry is an orphan and is reclaimed.
//   - metadata present, expired -> left for the TTL cleaner (which frees files).
//   - metadata present, back-reference points elsewhere -> superseded duplicate
//     (concurrent overwrite, or a Put whose back-reference lookup failed);
//     reclaim just the entry, keep the key.
//   - metadata present, back-reference points here or is absent -> the current,
//     live (or only) entry; evict metadata + entry + back-reference + backing
//     file.
func (c *Cleaner) evictByIndex(idx evictionIndex, targetBytes int64) (evictedCount int) {
	start := time.Now()

	var processedKeys int

	// committed*/pending* separate durable evictions from in-flight ones, so the
	// two side effects that assume a key was actually removed — reclaiming its
	// backing file and subtracting its bytes from the live total — happen only for
	// batches that committed. The batch write is atomic, so a failed Write removes
	// nothing; staging file deletions or decrementing the total for it would leave
	// live metadata pointing at deleted files and undercount the disk total (which
	// could wrongly stall further eviction). evictedCount (the named return) counts
	// only committed evictions.
	var committedBytes int64
	var pendingBytes int64
	var pendingCount int
	// pendingFiles holds the value metadata of keys deleted in the current batch,
	// so their backing files are reclaimed only after the batch's write succeeds.
	var pendingFiles []*pb.ValueMessage

	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	it := c.storage.meta.Handle().NewIterator(ro)
	defer it.Close()

	// commit writes the staged batch (periodic in-loop flush and the finalize
	// defer). Only on a successful write does it reclaim the batch's backing files
	// and promote the pending counts; on failure the keys were not removed, so it
	// discards the pending state and they are retried on the next pass. Reclaiming
	// files before the write would strand them (metadata still live) if the write
	// failed — the dangling raw-file class reconciled by #150/#152.
	commit := func() {
		if batch.Count() == 0 {
			return
		}
		if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Str("policy", idx.policy).Msg("cleaner: failed to write eviction batch; keys not evicted, will retry next pass")
		} else {
			for _, vm := range pendingFiles {
				c.storage.stageFileDeletion(vm)
			}
			committedBytes += pendingBytes
			evictedCount += pendingCount
		}
		pendingBytes = 0
		pendingCount = 0
		pendingFiles = pendingFiles[:0]
		batch.Clear()
	}

	// finalize flushes the last batch and then reconciles the size/stat accounting
	// for everything committed this run. It is deferred (registered after the
	// resource defers, so it runs before they release) so the commit and its
	// accounting stay together on EVERY return path — including the shutdown
	// early-return. Splitting them let a shutdown mid-eviction flush the deletes
	// while leaving totalSize inflated until the next startup recalculation.
	defer func() {
		commit()

		c.evictedKeys.Add(int64(evictedCount))
		c.totalSize.Add(-committedBytes)

		metrics.CleanerKeysDeleted.WithLabelValues(idx.policy, "disk_limit").Add(float64(evictedCount))
		metrics.CleanerBytesFreed.WithLabelValues(idx.policy).Add(float64(committedBytes))

		zlog.Info().
			Int("count", evictedCount).
			Int64("bytes", committedBytes).
			Int64("target", targetBytes).
			Int("processed", processedKeys).
			Str("policy", idx.policy).
			Dur("duration_ms", time.Since(start)).
			Msg("cleaner: eviction complete")
	}()

	zlog.Info().
		Int64("target_bytes", targetBytes).
		Str("policy", idx.policy).
		Msg("cleaner: starting eviction")

	for it.Seek(idx.prefix); it.ValidForPrefix(idx.prefix); it.Next() {
		// Check if we're shutting down.
		select {
		case <-c.closeCh:
			zlog.Info().Str("policy", idx.policy).Msg("cleaner: eviction interrupted by shutdown")
			// The finalize defer flushes the batch and reconciles accounting.
			return
		default:
		}

		// Bound the in-memory batch. Checked here (not only after an eviction) so
		// that runs of orphan/corrupt/superseded entries — which continue without
		// evicting — still flush periodically instead of accumulating one giant
		// batch.
		if batch.Count() >= 1000 {
			commit()
		}

		if committedBytes+pendingBytes >= targetBytes {
			break
		}

		keyBytes := it.Key().Data()

		originalKey, err := idx.parseKey(keyBytes)
		if err != nil {
			zlog.Debug().Err(err).Str("key", string(keyBytes)).Str("policy", idx.policy).Msg("cleaner: failed to parse eviction index key")
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
			// eviction. Retry on the next pass.
			zlog.Debug().Err(err).Str("key", originalKey).Str("policy", idx.policy).Msg("cleaner: eviction metadata lookup failed; skipping")
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
			batch.Delete(idx.backrefKey(originalKey))
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
		// (older-ordered) entry would discard the freshly-rewritten value at its
		// old position, out of eviction order.
		//
		// Only treat this entry as superseded when the back-reference EXISTS and
		// points somewhere else — then reclaim just the entry and keep the key. If
		// the back-reference is absent, this entry is the key's authoritative (or
		// only) index entry: fall through and evict via it, rather than deleting
		// it and stranding a live key with no eviction entry.
		backref := idx.backrefKey(originalKey)
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

		// Evict the key, its ordered index entry, and its back-reference. The
		// backing file is reclaimed by commit() only once this batch's write
		// succeeds (see pendingFiles), never before.
		batch.Delete(metaKey)
		batch.Delete(keyBytes)
		batch.Delete(backref)

		pendingBytes += valueMsg.ValueLength
		pendingCount++
		pendingFiles = append(pendingFiles, valueMsg)

		it.Key().Free()
		it.Value().Free()
	}

	// The finalize defer flushes the last batch and reconciles accounting;
	// evictedCount (named return) is promoted there for the committed batch.
	return
}
