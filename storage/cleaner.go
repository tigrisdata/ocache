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

	// totalSizeRecalcInterval is how often the tracked total is re-derived from a
	// full metadata scan. The running total is maintained incrementally on every
	// write and delete, which is exact in the common case but can still drift: a
	// crash between the metadata write and the accounting update, or two
	// concurrent Puts of the same key both reading the same previous row. Redoing
	// the startup scan keeps any such drift bounded instead of letting it
	// accumulate for the process lifetime; it is a metadata-only scan.
	totalSizeRecalcInterval = 1 * time.Hour

	// defaultDiskReserveBytes is the free space the cleaner keeps on the cache
	// volume as an ENOSPC backstop. The logical cap (MaxDiskUsage) bounds the sum
	// of stored object lengths, but physical usage can legitimately exceed it —
	// not-yet-recompacted segment dead space, orphaned raw files, RocksDB SST
	// amplification — so a logical-only cap cannot prevent the disk filling to
	// 100%, which is terminal (RocksDB can no longer open; #204). When actual free
	// space falls below this floor the cleaner evicts regardless of the logical
	// total. Sized to absorb a burst of in-flight large-object writes (segments
	// are 256 MiB) between cleaner ticks; eviction frees raw-file bytes promptly
	// for the large-object workload this protects.
	defaultDiskReserveBytes int64 = 2 * 1024 * 1024 * 1024 // 2 GiB

	// reserveVolumeFraction caps the effective reserve at 1/N of the volume's
	// total capacity, so the fixed 2 GiB floor cannot exceed a sane share of a
	// small volume (where a 2 GiB reserve might be unachievable and would evict
	// continuously). On production-sized volumes the 2 GiB floor is the binding
	// value.
	reserveVolumeFraction int64 = 10 // 10%

	// reserveEvictSliceFraction bounds a single reserve pass to at most 1/N of the
	// current cache, so one low statfs reading can't evict everything at once; the
	// next tick re-measures and continues if free is still low. Large caches evict
	// the whole (reserve-bounded) deficit in one pass; only a small cache relative
	// to the reserve is throttled across ticks.
	reserveEvictSliceFraction int64 = 4 // 25%
)

// Cleaner is responsible for background TTL cleanup and LRU eviction
type Cleaner struct {
	storage      *Storage
	interval     time.Duration
	maxDiskUsage int64
	// diskReserveBytes is the free-space ceiling for the ENOSPC backstop; the
	// effective reserve is min(diskReserveBytes, volume/reserveVolumeFraction).
	// A field (not a flag) defaulted from the const. diskUsageFn is the (injectable,
	// for tests) statfs source. The backstop is deliberately stateless — see
	// enforceFilesystemReserve.
	diskReserveBytes int64
	diskUsageFn      func(path string) (free, total int64, ok bool)
	initialized      atomic.Bool

	// backfillPending is set when a reconcile could not fully backfill eviction-
	// index coverage (a failed batch write or a truncated scan). While set, the
	// cleanup loop re-runs the reconcile every tick — instead of waiting for the
	// hourly recalc — so uncovered keys become evictable within a tick rather than
	// up to an hour, bounding the window in which the cap cannot reclaim them.
	backfillPending atomic.Bool

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
		storage:          storage,
		interval:         interval,
		maxDiskUsage:     maxDiskUsage,
		diskReserveBytes: defaultDiskReserveBytes,
		diskUsageFn:      diskUsage,
		closeCh:          make(chan struct{}),
	}
}

// Start launches the background cleanup goroutine
// It performs an initial size calculation synchronously to establish accurate baseline
// before any concurrent operations can modify the size
func (c *Cleaner) Start() {
	// Initial pass, synchronous (before any concurrent puts and before the loop):
	// recompute size and, when a cap is set, backfill eviction-index coverage for
	// keys written uncapped or under a prior policy (#189), so the first
	// enforcement tick sees a complete index.
	c.reconcileFromMetadata()
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
	// Track when we last re-derived the total size from the metadata
	lastSizeRecalc := time.Now()

	for {
		select {
		case <-ticker.C:
			// Correct any accumulated drift before enforcement acts on the total.
			// Also re-run promptly (every tick) while a prior backfill was left
			// incomplete, so uncovered keys become evictable within a tick rather
			// than waiting for the hourly recalc.
			if c.backfillPending.Load() || time.Since(lastSizeRecalc) > totalSizeRecalcInterval {
				c.calculateTotalSize()
				lastSizeRecalc = time.Now()
			}

			c.cleanupExpiredKeys()
			if c.maxDiskUsage > 0 {
				c.enforceDiskLimit()
				// ENOSPC backstop: evict on actual free space, independent of the
				// logical cap, so physical usage can't reach 100% (#204).
				c.enforceFilesystemReserve()
			}

			// Mirror the live totals (maintained on every write/evict) back to the
			// gauges, so ocache_disk_usage_bytes and the segment gauges track the
			// current contents instead of reflecting only the value at startup.
			c.refreshSizeMetrics()
			if c.storage != nil && c.storage.segmentManager != nil {
				c.storage.segmentManager.RefreshMetrics()
			}

			// Periodically clean up old access buckets to bound growth of the LRU
			// access index. Only under LRU: FIFO writes no access-bucket entries,
			// so the scan would be pure waste, and the FIFO index (a separate
			// keyspace) is reclaimed by its own eviction scan, not age-pruned.
			if c.storage != nil && c.storage.evictionPolicy != EvictionPolicyFIFO &&
				time.Since(lastBucketCleanup) > accessBucketCleanupInterval {
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

	// committed* accumulate only entries whose batch was written successfully.
	// pending* hold the current batch's deletes until it commits, so a failed
	// Write (which leaves those keys stored) does not drop their bytes from the
	// live total — otherwise the next run would re-collect and re-subtract them,
	// skewing TotalSize and disk-limit enforcement.
	committedCleaned := 0
	var committedBytes int64
	pendingCleaned := 0
	var pendingBytes int64
	// pendingFiles holds the value metadata of expired keys deleted in the current
	// batch, so their backing files are reclaimed only after the batch's write
	// succeeds. Staging file deletion earlier would strand the file (metadata still
	// live) if the write failed — the dangling raw-file class reconciled by
	// #150/#152.
	var pendingFiles []*pb.ValueMessage

	// Track cleaner run
	metrics.CleanerRuns.WithLabelValues("ttl").Inc()

	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	it := c.storage.meta.Handle().NewIterator(ro)
	defer it.Close()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// flush writes the current batch and, only on success, promotes the pending
	// counts to committed. On failure the deletes did not persist, so the
	// pending counts are discarded rather than committed.
	//
	// TTL cleanup deletes entries directly via this batch (bypassing DeleteKey,
	// where explicit deletes decrement the total), so it must subtract the freed
	// bytes itself. We do it here, per committed batch, rather than once at the
	// end: that keeps the live total correct even if the scan returns early on
	// shutdown after some batches have already persisted. Without it the total
	// stays inflated by expired-but-collected entries, inflating
	// ocache_disk_usage_bytes and risking unnecessary LRU eviction in
	// enforceDiskLimit.
	flush := func(final bool) {
		label := "deletion batch"
		if final {
			label = "final deletion batch"
		}
		if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Msgf("cleaner: failed to write %s", label)
		} else {
			// Metadata deletes are durable; only now is it safe to reclaim the
			// backing files.
			for _, vm := range pendingFiles {
				c.storage.stageFileDeletion(vm)
			}
			committedCleaned += pendingCleaned
			committedBytes += pendingBytes
			if pendingBytes > 0 {
				c.UpdateSize(-pendingBytes)
			}
		}
		pendingCleaned = 0
		pendingBytes = 0
		pendingFiles = pendingFiles[:0]
		batch.Clear()
	}

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
			c.storage.stageEvictionIndexDeletes(batch, ro, key)
			pendingCleaned++
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
			c.storage.stageEvictionIndexDeletes(batch, ro, key)
			pendingCleaned++
			zlog.Debug().Str("key", key).Int64("expiry", valueMsg.Expiry).Int64("now", now).Msg("cleaner: deleting expired key")

			// Track bytes freed
			pendingBytes += valueMsg.ValueLength

			// Defer file reclaim to flush(): the backing file is freed only once
			// this batch's write succeeds (see pendingFiles), never before.
			pendingFiles = append(pendingFiles, valueMsg)
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

			flush(false)
		}
	}

	// Write final batch
	if batch.Count() > 0 {
		flush(true)
	}

	c.cleanedKeys.Add(int64(committedCleaned))

	// Record metrics
	duration := time.Since(start)
	metrics.CleanerDuration.WithLabelValues("ttl").Observe(float64(duration.Milliseconds()))
	metrics.CleanerKeysDeleted.WithLabelValues("ttl", "expired").Add(float64(committedCleaned))
	metrics.CleanerBytesFreed.WithLabelValues("ttl").Add(float64(committedBytes))

	zlog.Info().
		Int("cleaned", committedCleaned).
		Int64("bytes_freed", committedBytes).
		Dur("duration_ms", duration).
		Msg("cleaner: TTL cleanup completed")
}

// calculateTotalSize re-derives the tracked total from the metadata. Thin wrapper
// retained for the hourly reconciliation caller and tests; see reconcileFromMetadata.
func (c *Cleaner) calculateTotalSize() {
	c.reconcileFromMetadata()
}

// reconcileFromMetadata scans every metadata row once, on a pinned RocksDB
// snapshot, to recompute the total size and apply it as a correction relative to
// the value the counter held when the snapshot was taken (never a plain store),
// so a write that commits during the scan keeps its incremental delta instead of
// being stomped by a scan that could not see it (#205).
//
// Whenever a cap is set it ALSO guarantees eviction-index coverage during the
// same scan. putLow indexes a key only when a cap is set at write time and only
// for the policy active then, so keys written uncapped — or under a prior policy
// before an lru<->fifo switch — have no index entry and are invisible to
// eviction: the cap can never reclaim them (#189, the state that fills the disk
// into the terminal ENOSPC of #204). Rather than a per-key lookup, this
// merge-joins the metadata rows against the active policy's back-reference rows
// — both stored sorted by the same key suffix — advancing a second iterator in
// lockstep, so coverage is checked in O(1) memory with no random reads. Any live
// key with no back-reference is given a fresh entry.
//
// Running on every reconcile (not just startup) makes coverage self-healing: a
// backfill left incomplete by a write or iterator error is finished on a later
// pass, and the merge is cheap when coverage is already complete. A concurrent
// put during an hourly pass can momentarily produce a duplicate/orphan index
// entry, which the eviction scan already validates against the back-reference
// and reclaims. Orphan back-references (no metadata row) are skipped. Backfilled
// keys are stamped at scan time, so the order among them is arbitrary but they
// become evictable.
func (c *Cleaner) reconcileFromMetadata() {
	start := time.Now()
	var totalSize int64
	backfill := c.maxDiskUsage > 0

	// Pin the scan before reading the counter: a write the scan cannot see then
	// also has its delta applied after tracked was read, so it survives.
	snapshot := c.storage.meta.Handle().NewSnapshot()
	defer c.storage.meta.Handle().ReleaseSnapshot(snapshot)
	tracked := c.totalSize.Load()

	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	ro.SetSnapshot(snapshot)
	it := c.storage.meta.Handle().NewIterator(ro)
	defer it.Close()

	// Backfill state: a second iterator over the active policy's back-reference
	// rows on the same snapshot, advanced in lockstep with the metadata rows.
	var (
		backrefIt     *grocksdb.Iterator
		backrefPrefix []byte
		wo            *grocksdb.WriteOptions
		batch         *grocksdb.WriteBatch
		now           time.Time
		backfilled    int
		writeErrs     int
		backrefBroken bool
	)
	if backfill {
		now = time.Now()
		if c.storage.evictionPolicy == EvictionPolicyFIFO {
			backrefPrefix = []byte(keys.FifoBackrefPrefix)
		} else {
			backrefPrefix = []byte(keys.AccessBucketIndexPrefix)
		}
		backrefIt = c.storage.meta.Handle().NewIterator(ro)
		defer backrefIt.Close()
		backrefIt.Seek(backrefPrefix)
		wo = grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		batch = grocksdb.NewWriteBatch()
		defer batch.Destroy()
	}
	flush := func() {
		if batch == nil || batch.Count() == 0 {
			return
		}
		if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
			writeErrs++
			zlog.Error().Err(err).Msg("cleaner: eviction-index backfill batch write failed")
		}
		batch.Clear()
	}

	for it.SeekToFirst(); it.Valid(); it.Next() {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: metadata reconciliation interrupted by shutdown")
			flush()
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

		// This scan sums only value_length across every metadata row, so read
		// it directly off the wire rather than fully decoding each message —
		// that skips a Data-payload copy per inline row (up to the 64 KiB inline
		// threshold) on both the startup scan and the hourly reconciliation.
		if length, ok := valueMessageValueLength(it.Value().Data()); ok {
			totalSize += length
		}

		if backfill && !backrefBroken {
			userKey := keys.ExtractUserKey(keyBytes)
			covered, ok := advanceBackrefTo(backrefIt, backrefPrefix, userKey)
			switch {
			case !ok:
				// The back-reference scan errored: coverage is now unknown, so stop
				// backfilling for the rest of this pass. Treating errored-out keys as
				// uncovered would re-stamp already-indexed keys (rewriting their
				// eviction order). backfillPending triggers a full retry next tick.
				backrefBroken = true
			case !covered:
				c.stageBackfillEntry(batch, userKey, now)
				backfilled++
				if batch.Count() >= 1000 {
					flush()
				}
			}
		}

		it.Key().Free()
		it.Value().Free()
	}
	flush()

	// A truncated scan yields a partial sum; applying it would corrupt the counter
	// (under-sizing the cap and weakening the very eviction this protects). Skip
	// the correction and let the next reconcile retry the full scan.
	if err := it.Err(); err != nil {
		// The scan was truncated, so coverage may be incomplete — flag a fast retry.
		if backfill {
			c.backfillPending.Store(true)
		}
		zlog.Error().Err(err).Int("backfilled", backfilled).
			Msg("cleaner: metadata scan truncated by iterator error; skipped size correction, will retry next tick")
		return
	}
	if backfill {
		incomplete := writeErrs > 0 || backrefBroken
		if err := backrefIt.Err(); err != nil {
			incomplete = true
			zlog.Warn().Err(err).
				Msg("cleaner: back-reference scan errored; stopped backfill to avoid re-stamping indexed keys, will retry next tick")
		}
		if writeErrs > 0 {
			zlog.Warn().Int("failed_batches", writeErrs).Int("backfilled", backfilled).
				Msg("cleaner: eviction-index backfill incomplete; will retry next tick")
		}
		// While set, the cleanup loop re-runs this reconcile every tick until a
		// pass completes cleanly, so uncovered keys are reclaimable within a tick.
		c.backfillPending.Store(incomplete)
	}

	c.totalSize.Add(totalSize - tracked)

	// Publish the freshly computed size to the gauges.
	c.refreshSizeMetrics()

	event := zlog.Info().
		Int64("total_size", totalSize).
		Int64("drift", tracked-totalSize).
		Dur("duration_ms", time.Since(start))
	if backfill {
		event = event.Int("backfilled", backfilled).Str("policy", c.storage.evictionPolicy)
	}
	event.Msg("cleaner: reconciled total storage size from metadata")
}

// advanceBackrefTo advances the sorted back-reference iterator to userKey and
// reports whether an entry for it exists (covered). ok is false only when the
// iterator is in an error state — coverage is then unknown, and the caller must
// NOT treat the key as uncovered (re-stamping an already-indexed key would
// rewrite its eviction order). Orphan back-references (a key that sorts before
// userKey, i.e. has no metadata row) are skipped. The iterator only moves
// forward, matching the ascending metadata scan, so the whole coverage check
// across a reconcile is a single linear merge — O(1) memory, no lookups.
func advanceBackrefTo(it *grocksdb.Iterator, prefix []byte, userKey string) (covered, ok bool) {
	for it.ValidForPrefix(prefix) {
		suffix := string(it.Key().Data()[len(prefix):])
		it.Key().Free()
		it.Value().Free()
		switch {
		case suffix < userKey:
			it.Next() // orphan back-reference with no metadata row; skip past it
		case suffix == userKey:
			it.Next() // consume it — each key matches at most one metadata row
			return true, true
		default: // suffix > userKey: no entry for this key; keep it for a later row
			return false, true
		}
	}
	// The prefix range ended: distinguish a genuine "no more entries" from an
	// iterator error, so a transient read failure does not masquerade as
	// "uncovered" and trigger re-stamping of the remaining keys.
	if it.Err() != nil {
		return false, false
	}
	return false, true
}

// stageBackfillEntry adds a fresh eviction-index entry for userKey to batch,
// stamped at now, for the active policy — mirroring putLow's index writes. FIFO
// goes through writeFifoIndexEntry so that if a concurrent put indexed this key
// after our snapshot (only possible on a live hourly pass), the stale entry is
// deleted via the back-reference rather than left as a duplicate. The LRU path
// leaves any superseded access-bucket entry as an orphan for later reclamation,
// exactly as putLow's LRU branch does.
func (c *Cleaner) stageBackfillEntry(batch *grocksdb.WriteBatch, userKey string, now time.Time) {
	if c.storage.evictionPolicy == EvictionPolicyFIFO {
		c.storage.writeFifoIndexEntry(batch, userKey, now)
		return
	}
	accessKey := keys.MakeBucketedAccessKey(userKey, now)
	batch.Put(accessKey, []byte{})
	batch.Put(keys.MakeBucketedAccessIndexKey(userKey), accessKey)
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

	fifo := c.storage != nil && c.storage.evictionPolicy == EvictionPolicyFIFO

	idx := lruEvictionIndex()
	if fifo {
		idx = fifoEvictionIndex()
	}
	policy := idx.policy

	// Track eviction run
	metrics.CleanerRuns.WithLabelValues(policy).Inc()

	zlog.Info().
		Int64("current", currentSize).
		Int64("max", c.maxDiskUsage).
		Int64("need_to_evict", needToEvict).
		Str("policy", policy).
		Msg("cleaner: enforcing disk usage limit")

	evicted := c.evictByIndex(idx, needToEvict)

	// Record metrics
	duration := time.Since(start)
	metrics.CleanerDuration.WithLabelValues(policy).Observe(float64(duration.Milliseconds()))
	// LRUEvictions is LRU-specific; FIFO eviction volume is tracked via the
	// policy-labeled CleanerKeysDeleted{fifo,disk_limit} / CleanerBytesFreed{fifo}.
	if !fifo {
		metrics.LRUEvictions.Add(float64(evicted))
	}
}

// enforceFilesystemReserve is the ENOSPC backstop: when the cache volume's
// *actual* free space (statfs) is below the reserve it evicts by the active
// policy's index, independent of the logical total. enforceDiskLimit only bounds
// the logical sum of object lengths, but physical usage can legitimately exceed
// the cap (segment dead space, orphaned raw files, SST amplification), so without
// this the disk can still fill to the terminal 100% state (#204).
//
// It evicts at most a bounded slice per tick and re-measures on the next tick,
// converging over several ticks rather than acting on the whole deficit at once.
// It deliberately keeps NO in-flight state. Freed space lags eviction (raw-file
// deletion queue, segment recompaction), but re-evicting while reclamation is
// pending is self-limiting — evicting segment entries raises their fragmentation,
// which triggers the recompaction that frees their space — and, crucially, the
// backstop can never stall on stale bookkeeping. Under-eviction risks the
// terminal ENOSPC this exists to prevent; over-eviction only costs recoverable
// cache warmth, so when in doubt it evicts.
func (c *Cleaner) enforceFilesystemReserve() {
	// The eviction indexes only exist when a cap is set, so there is nothing to
	// evict from otherwise.
	if c.maxDiskUsage <= 0 || c.storage == nil {
		return
	}

	free, total, ok := c.diskUsageFn(c.storage.diskPath)
	if !ok {
		return // statfs unavailable; treat the backstop as inactive this tick
	}
	metrics.FilesystemFreeBytes.Set(float64(free))

	// Cap the fixed reserve at a fraction of the volume so it stays achievable on
	// small volumes (otherwise free could be permanently below a 2 GiB reserve).
	reserve := c.diskReserveBytes
	if volCap := total / reserveVolumeFraction; total > 0 && volCap < reserve {
		reserve = volCap
	}
	if free >= reserve {
		return
	}

	// Bound the per-tick blast radius to a fraction of the cache so a single low
	// reading can't evict everything at once; the next tick re-measures and
	// continues if free is still low. A large cache's slice covers the whole
	// (reserve-bounded) deficit; only a small cache is throttled.
	slice := reserve - free
	if maxSlice := c.totalSize.Load() / reserveEvictSliceFraction; maxSlice > 0 && maxSlice < slice {
		slice = maxSlice
	}

	fifo := c.storage.evictionPolicy == EvictionPolicyFIFO
	idx := lruEvictionIndex()
	if fifo {
		idx = fifoEvictionIndex()
	}

	metrics.CleanerRuns.WithLabelValues(idx.policy).Inc()
	zlog.Warn().
		Int64("free", free).
		Int64("reserve", reserve).
		Int64("evict_target", slice).
		Str("policy", idx.policy).
		Msg("cleaner: filesystem free space below reserve; evicting to reclaim disk (ENOSPC backstop)")

	start := time.Now()
	evicted := c.evictByIndex(idx, slice)
	metrics.CleanerDuration.WithLabelValues(idx.policy).Observe(float64(time.Since(start).Milliseconds()))
	if !fifo {
		metrics.LRUEvictions.Add(float64(evicted))
	}
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
