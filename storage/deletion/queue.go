// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package deletion

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

// conditionalTag marks a deletion-queue entry whose file must be deleted only if
// no metadata row still references it. The tag byte is followed by the owning
// user key so the worker can do a cheap forward lookup before deleting. Plain
// (unconditional) entries use the legacy single-byte value {0x01} and are
// deleted on sight, exactly as before.
const conditionalTag = 0x02

// Config holds configuration for the deletion queue
type Config struct {
	BatchSize       int           // Number of deletions per batch
	ProcessInterval time.Duration // Interval between batch processing
	PruneAge        time.Duration // Age after which entries are pruned
	RetryDelay      time.Duration // Backoff before a failed deletion is retried (0 = retry next cycle)
}

// Queue manages centralized file deletion
type Queue struct {
	meta   *metadata.MetaDB
	config Config

	// Background processing
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	processed int64
	failed    int64
	pruned    int64
}

// NewQueue creates a new deletion queue
func NewQueue(meta *metadata.MetaDB, config Config) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{
		meta:   meta,
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins background processing
func (q *Queue) Start() {
	q.wg.Add(1)
	go q.processingLoop()
	zlog.Info().
		Int("batch_size", q.config.BatchSize).
		Dur("interval", q.config.ProcessInterval).
		Dur("prune_age", q.config.PruneAge).
		Msg("deletion queue: started")
}

// Stop gracefully stops the queue
func (q *Queue) Stop() {
	zlog.Info().Msg("deletion queue: stopping")
	q.cancel()
	q.wg.Wait()
	zlog.Info().
		Int64("processed", q.processed).
		Int64("failed", q.failed).
		Int64("pruned", q.pruned).
		Msg("deletion queue: stopped")
}

// Add adds a file to the deletion queue
func (q *Queue) Add(filepath string) error {
	if filepath == "" {
		return fmt.Errorf("empty filepath")
	}

	key := keys.MakeDeletionQueueKey(time.Now().UnixNano(), filepath)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	err := q.meta.Handle().Put(wo, key, []byte{0x01})
	if err != nil {
		zlog.Error().
			Str("filepath", filepath).
			Err(err).
			Msg("deletion queue: failed to add entry")
		return err
	}

	// Increment added counter
	metrics.DeletionQueueAdded.Inc()

	zlog.Debug().
		Str("filepath", filepath).
		Msg("deletion queue: added entry")
	return nil
}

// AddIfUnreferenced enqueues filepath for deletion but, unlike Add, defers the
// delete-or-keep decision to the worker: the file is removed only if userKey's
// metadata row does not (still) reference it. It is for callers that cannot yet
// tell whether the file is live — notably a CAS spill whose win/lose outcome
// could not be read back after the merge committed (issue #254). Staging it
// here makes the outcome durable and self-resolving (the worker retries until
// the DB reads cleanly): if the CAS won, the row references the file and it is
// kept; if it lost, the orphan is deleted — instead of leaking permanently
// (issue #156).
func (q *Queue) AddIfUnreferenced(filepath, userKey string) error {
	if filepath == "" || userKey == "" {
		return fmt.Errorf("empty filepath or key")
	}

	key := keys.MakeDeletionQueueKey(time.Now().UnixNano(), filepath)
	val := append([]byte{conditionalTag}, []byte(userKey)...)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	if err := q.meta.Handle().Put(wo, key, val); err != nil {
		zlog.Error().Str("filepath", filepath).Str("key", userKey).Err(err).
			Msg("deletion queue: failed to add conditional entry")
		return err
	}
	metrics.DeletionQueueAdded.Inc()
	return nil
}

// processingLoop runs the background processing
func (q *Queue) processingLoop() {
	defer q.wg.Done()

	ticker := time.NewTicker(q.config.ProcessInterval)
	defer ticker.Stop()

	// Prune old entries periodically (every hour)
	pruneTicker := time.NewTicker(time.Hour)
	defer pruneTicker.Stop()

	// Log queue depth periodically (every 5 minutes)
	depthTicker := time.NewTicker(5 * time.Minute)
	defer depthTicker.Stop()

	for {
		select {
		case <-ticker.C:
			q.ProcessBatch()
		case <-pruneTicker.C:
			q.pruneOldEntries()
		case <-depthTicker.C:
			q.logQueueDepth()
		case <-q.ctx.Done():
			return
		}
	}
}

// ProcessBatch processes a batch of deletion requests
func (q *Queue) ProcessBatch() {
	startTime := time.Now()
	defer func() {
		// Record batch duration in milliseconds
		metrics.DeletionQueueBatchDuration.Observe(float64(time.Since(startTime).Milliseconds()))
	}()
	seen := make(map[string]seenEntry) // filepath -> earliest queue entry

	// Scan and deduplicate
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(keys.DeletionQueuePrefix)
	count := 0
	nowNanos := time.Now().UnixNano()

	// Scan from the head (oldest first), collecting up to BatchSize distinct
	// filepaths that are due (timestamp <= now). Entries whose deletion fails are
	// re-enqueued at now+RetryDelay (see below), so the head always advances
	// (no head-of-line starvation) and a persistently-stuck file is only retried
	// once per RetryDelay rather than every cycle. Because keys are timestamp-
	// ordered, the first not-yet-due entry means every entry after it is also in
	// the future, so we can stop scanning.
	for it.Seek(prefix); it.ValidForPrefix(prefix) && count < q.config.BatchSize; it.Next() {
		// Check for shutdown
		select {
		case <-q.ctx.Done():
			return
		default:
		}

		key := it.Key()
		keyData := key.Data()

		// Extract timestamp and filepath from key: !del/<timestamp>/<filepath>
		ts, filepath, err := keys.ParseDeletionQueueKey(keyData)
		if err != nil {
			key.Free()
			it.Value().Free()
			continue
		}

		if ts > nowNanos {
			// Not yet due (a re-enqueued entry still in its backoff window); all
			// later entries are in the future too, so stop.
			key.Free()
			it.Value().Free()
			break
		}

		// Keep only the earliest entry for each filepath, capturing its value so
		// the worker can honor a conditional (reference-guarded) deletion and so a
		// re-enqueue preserves the entry's type.
		val := it.Value()
		if _, exists := seen[filepath]; !exists {
			seen[filepath] = seenEntry{queueKey: bytes.Clone(keyData), value: bytes.Clone(val.Data())}
			count++
		}
		val.Free()
		key.Free()
	}

	if len(seen) == 0 {
		return
	}

	// Attempt deletions
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	successful := 0
	failed := 0

	for filepath, se := range seen {
		if q.resolveDeletion(filepath, se.value) {
			batch.Delete(se.queueKey)
			successful++
			q.processed++
			// Increment processed counter
			metrics.DeletionQueueProcessed.Inc()
		} else {
			// Deletion failed and the file is still on disk (read-locked by an
			// active reader, read-only filesystem, ...). Re-enqueue the entry
			// under a future timestamp (now+RetryDelay): delete the current key
			// and re-add it. The head keeps advancing so a run of undeletable
			// files cannot starve newer, deletable entries, and the backoff
			// bounds how often a persistently-stuck file is rewritten — the scan
			// above skips not-yet-due entries, so it is retried roughly once per
			// RetryDelay instead of every cycle. The file is never dropped; it is
			// reclaimed once a later attempt succeeds. tryDelete treats a missing
			// file as success, so re-enqueued entries only reference files that
			// still exist.
			batch.Delete(se.queueKey)
			batch.Put(keys.MakeDeletionQueueKey(time.Now().Add(q.config.RetryDelay).UnixNano(), filepath), se.value)
			failed++
			q.failed++
			// Increment failed counter
			metrics.DeletionQueueFailed.Inc()
		}
	}

	// Commit successful deletions and tail re-enqueues
	if batch.Count() > 0 {
		if err := q.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().
				Err(err).
				Msg("deletion queue: failed to commit batch")
		}
	}

	if successful > 0 || failed > 0 {
		zlog.Info().
			Int("successful", successful).
			Int("failed", failed).
			Dur("duration_ms", time.Since(startTime)).
			Msg("deletion queue: processed batch")
	}
}

// seenEntry is the earliest queue entry observed for a given filepath in a
// batch scan: its queue key (to delete/re-enqueue) and its value (to honor a
// conditional deletion and to preserve the entry type across a re-enqueue).
type seenEntry struct {
	queueKey []byte
	value    []byte
}

// resolveDeletion decides a single entry's fate. It returns true when the entry
// is resolved and should be removed from the queue (the file was deleted, or a
// conditional entry's file is still referenced and must be KEPT), and false when
// it should be retried later. A conditional entry (see AddIfUnreferenced) is
// deleted only when its owning metadata row no longer references the file; if
// the reference check cannot be made (the DB read failed) the entry is retried
// rather than risk deleting a live file.
func (q *Queue) resolveDeletion(filepath string, value []byte) bool {
	if len(value) > 0 && value[0] == conditionalTag {
		userKey := string(value[1:])
		referenced, err := q.fileStillReferenced(userKey, filepath)
		if err != nil {
			return false // unknown — never delete a possibly-referenced file
		}
		if referenced {
			return true // the CAS won; keep the file, drop the entry
		}
		// Not referenced (the CAS lost, or the key is gone): delete the orphan.
	}
	return q.tryDelete(filepath)
}

// fileStillReferenced reports whether userKey's metadata row still points at
// filepath as its raw-file backing store.
func (q *Queue) fileStillReferenced(userKey, filepath string) (bool, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	slice, err := q.meta.Handle().Get(ro, keys.MakeMetadataKey(userKey))
	if err != nil {
		return false, err
	}
	defer slice.Free()
	if !slice.Exists() {
		return false, nil
	}
	var vm pb.ValueMessage
	if err := proto.Unmarshal(slice.Data(), &vm); err != nil {
		// A corrupt row does not usefully reference this file; deleting the orphan
		// is safe (the row itself is a separate concern).
		return false, nil
	}
	return vm.ValueType == pb.ValueType_RAW_FILE && vm.RawFilePath == filepath, nil
}

// tryDelete attempts to delete a file
func (q *Queue) tryDelete(filepath string) bool {
	lockManager := fd.GetFileLockManager()
	lock := lockManager.GetFileLock(filepath)

	// Try to acquire lock without blocking
	if !lock.TryLock() {
		zlog.Debug().
			Str("filepath", filepath).
			Msg("deletion queue: file locked, will retry")
		return false
	}
	defer lock.Unlock()

	err := os.Remove(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			// File already deleted, consider it successful
			zlog.Debug().
				Str("filepath", filepath).
				Msg("deletion queue: file already deleted")
			return true
		}
		zlog.Error().
			Str("filepath", filepath).
			Err(err).
			Msg("deletion queue: failed to delete file")
		return false
	}

	// Remove lock from manager after successful deletion
	lockManager.RemoveFileLock(filepath)

	zlog.Debug().
		Str("filepath", filepath).
		Msg("deletion queue: deleted file")
	return true
}

// pruneOldEntries removes queue entries older than PruneAge
func (q *Queue) pruneOldEntries() {
	startTime := time.Now()
	cutoff := time.Now().Add(-q.config.PruneAge).UnixNano()

	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	prefix := []byte(keys.DeletionQueuePrefix)
	pruned := 0
	stuck := 0

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		// Check for shutdown
		select {
		case <-q.ctx.Done():
			return
		default:
		}

		key := it.Key()
		keyData := key.Data()

		// Extract timestamp and filepath from key
		timestamp, filepath, err := keys.ParseDeletionQueueKey(keyData)
		if err == nil && timestamp > 0 && timestamp < cutoff {
			// The queue entry is the only durable record that this file must be
			// deleted. Dropping it while the file still exists orphans the file
			// permanently: it has no metadata, no compaction-index entry, and no
			// queue entry, so it is invisible to the disk-usage cap, LRU
			// eviction, and startup recovery (see issue #156). Only prune an
			// entry once the file is confirmed gone; otherwise keep it for
			// ProcessBatch to retry. Raw-file paths are UUIDs and never reused,
			// so ENOENT is a safe terminal signal that the deletion is done.
			if _, statErr := os.Stat(filepath); os.IsNotExist(statErr) {
				batch.Delete(bytes.Clone(keyData))
				pruned++
				q.pruned++
				// Increment pruned counter
				metrics.DeletionQueuePruned.Inc()
			} else {
				// File still present (read-locked by an active reader, read-only
				// filesystem, lost permissions, transient I/O error, ...). Keep
				// the entry so the file is reclaimed once deletion succeeds
				// rather than being abandoned on disk.
				stuck++
				zlog.Warn().
					Str("filepath", filepath).
					Dur("age", time.Since(time.Unix(0, timestamp))).
					Msg("deletion queue: entry past prune age but file still exists; keeping for retry")
			}
		}

		key.Free()
		it.Value().Free()

		// Commit batch periodically
		if batch.Count() >= 100 {
			if err := q.meta.Handle().Write(wo, batch); err != nil {
				zlog.Error().
					Err(err).
					Msg("deletion queue: failed to prune batch")
			}
			batch.Clear()
		}
	}

	// Commit final batch
	if batch.Count() > 0 {
		if err := q.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().
				Err(err).
				Msg("deletion queue: failed to prune final batch")
		}
	}

	if pruned > 0 {
		zlog.Info().
			Int("pruned", pruned).
			Dur("duration_ms", time.Since(startTime)).
			Msg("deletion queue: pruned entries whose files were already gone")
	}

	// With ProcessBatch re-enqueuing failed deletions to the tail under fresh
	// timestamps, an entry both past PruneAge and still backed by a file should
	// not normally occur; surface it as a warning rather than mislabeling it as a
	// prune.
	if stuck > 0 {
		zlog.Warn().
			Int("stuck", stuck).
			Dur("duration_ms", time.Since(startTime)).
			Msg("deletion queue: aged entries still backed by a file, kept for retry")
	}
}

// GetQueueDepth returns the current number of entries in the deletion queue.
func (q *Queue) GetQueueDepth() int64 {
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(keys.DeletionQueuePrefix)
	count := int64(0)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
		it.Key().Free()
		it.Value().Free()
	}

	return count
}

// logQueueDepth logs the current queue depth and stats. A backlog that fails to
// drain shows up as a sustained queue_depth together with a rising
// DeletionQueueFailed rate, which is how persistently-undeletable files surface.
func (q *Queue) logQueueDepth() {
	depth := q.GetQueueDepth()

	// Update queue depth gauge metric
	metrics.DeletionQueueDepth.Set(float64(depth))

	// Always log if there are items in the queue, or periodically log stats
	if depth > 0 {
		zlog.Info().
			Int64("queue_depth", depth).
			Int64("total_processed", q.processed).
			Int64("total_failed", q.failed).
			Int64("total_pruned", q.pruned).
			Msg("deletion queue: status")
	} else {
		// Log empty queue status less frequently
		zlog.Debug().
			Int64("queue_depth", depth).
			Int64("total_processed", q.processed).
			Int64("total_failed", q.failed).
			Int64("total_pruned", q.pruned).
			Msg("deletion queue: status (empty)")
	}
}
