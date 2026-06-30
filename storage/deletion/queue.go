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
)

// Config holds configuration for the deletion queue
type Config struct {
	BatchSize       int           // Number of deletions per batch
	ProcessInterval time.Duration // Interval between batch processing
	PruneAge        time.Duration // Age after which entries are pruned
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
	processed  int64
	failed     int64
	pruned     int64
	queueDepth int64

	// scanCursor remembers where the previous ProcessBatch scan stopped so a
	// run of undeletable entries at the head of the timestamp-ordered queue
	// cannot starve newer entries whose files can be removed. nil means start
	// from the head. Only touched by the single-threaded processing loop.
	scanCursor []byte
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
	seen := make(map[string][]byte) // filepath -> earliest queue key

	// Scan and deduplicate
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(keys.DeletionQueuePrefix)
	count := 0

	// Resume scanning from where the previous batch stopped. The queue is
	// ordered by timestamp, so always restarting at the head would let a
	// batch-sized run of entries whose deletion keeps failing (e.g. read-locked
	// files) be re-scanned every cycle and never reach newer, deletable entries
	// behind them. The cursor advances past such entries and wraps back to the
	// head at the end, so every entry is retried over successive cycles.
	startKey := prefix
	if q.scanCursor != nil {
		startKey = q.scanCursor
	}

	var lastKey []byte
	it.Seek(startKey)
	// Seek lands on the cursor key itself when it still exists (its deletion
	// failed last cycle); skip it to make forward progress. If it was deleted,
	// Seek lands on the next key and there is nothing to skip.
	if q.scanCursor != nil && it.ValidForPrefix(prefix) && bytes.Equal(it.Key().Data(), q.scanCursor) {
		it.Next()
	}

	for it.ValidForPrefix(prefix) && count < q.config.BatchSize {
		// Check for shutdown
		select {
		case <-q.ctx.Done():
			return
		default:
		}

		key := it.Key()
		keyData := key.Data()

		// Extract filepath from key: !del/<timestamp>/<filepath>
		if _, filepath, err := keys.ParseDeletionQueueKey(keyData); err == nil {
			// Keep only earliest entry for each filepath
			if _, exists := seen[filepath]; !exists {
				seen[filepath] = bytes.Clone(keyData)
				count++
			}
		}

		lastKey = bytes.Clone(keyData)
		key.Free()
		it.Value().Free()
		it.Next()
	}

	// Advance the cursor for the next cycle, wrapping to the head once the end
	// of the queue is reached.
	if it.ValidForPrefix(prefix) {
		q.scanCursor = lastKey
	} else {
		q.scanCursor = nil
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

	for filepath, queueKey := range seen {
		if q.tryDelete(filepath) {
			batch.Delete(queueKey)
			successful++
			q.processed++
			// Increment processed counter
			metrics.DeletionQueueProcessed.Inc()
		} else {
			failed++
			q.failed++
			// Increment failed counter
			metrics.DeletionQueueFailed.Inc()
		}
	}

	// Commit successful deletions
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

	// The DeletionQueueStuck gauge is published by logQueueDepth on a fixed
	// cadence rather than here, so it reflects entries that ProcessBatch drains
	// between prune runs instead of going stale for up to a full PruneAge.
	if pruned > 0 || stuck > 0 {
		zlog.Info().
			Int("pruned", pruned).
			Int("stuck", stuck).
			Dur("duration_ms", time.Since(startTime)).
			Msg("deletion queue: pruned old entries")
	}
}

// GetQueueDepth returns the current queue depth
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

// scanStats walks the queue once and returns the total depth and the number of
// "stuck" entries: entries older than PruneAge whose file is still on disk
// (i.e. deletions that keep failing). Computing this on the periodic logging
// cadence keeps the DeletionQueueStuck gauge fresh, so it drops back to zero
// once ProcessBatch reclaims the files rather than staying stale until the next
// prune run.
func (q *Queue) scanStats() (depth int64, stuck int64) {
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	cutoff := time.Now().Add(-q.config.PruneAge).UnixNano()
	prefix := []byte(keys.DeletionQueuePrefix)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		depth++

		keyData := it.Key().Data()
		if ts, filepath, err := keys.ParseDeletionQueueKey(keyData); err == nil && ts > 0 && ts < cutoff {
			if _, statErr := os.Stat(filepath); statErr == nil {
				stuck++
			}
		}

		it.Key().Free()
		it.Value().Free()
	}

	return depth, stuck
}

// logQueueDepth logs the current queue depth and stats
func (q *Queue) logQueueDepth() {
	depth, stuck := q.scanStats()

	// Update queue gauges. Stuck is recomputed here (not only at prune time) so
	// it reflects entries ProcessBatch has drained since the last prune.
	metrics.DeletionQueueDepth.Set(float64(depth))
	metrics.DeletionQueueStuck.Set(float64(stuck))

	// Always log if there are items in the queue, or periodically log stats
	if depth > 0 {
		zlog.Info().
			Int64("queue_depth", depth).
			Int64("stuck", stuck).
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
