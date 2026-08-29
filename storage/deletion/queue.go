// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package deletion

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
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
	RetryDelay      time.Duration // Backoff before a failed deletion is retried (0 = retry next cycle)
}

type retryState struct {
	cutoff       int64
	retryAt      int64
	watermarkKey string
}

type successWatermark struct {
	key    []byte
	cutoff int64
	paths  []string
}

// Queue manages centralized file deletion
type Queue struct {
	meta   *metadata.MetaDB
	config Config

	// retryStates records the timestamp watermark for a filepath after it has
	// been handled. Entries at or before the watermark belong to that same
	// lifecycle and can be retired without another filesystem attempt. A
	// retryAt of zero means the prior lifecycle is protected by the cutoff; it
	// is either successful or has been superseded by a later Add. A non-zero
	// value is a delayed retry. The map is populated before the next scan, so
	// it also covers duplicate rows left beyond a distinct-path batch boundary.
	retryStates map[string]retryState

	// Success watermarks are ordered by cutoff so cleanup does not scan every
	// active path on every worker tick. Each record contains all paths selected
	// by one bounded batch and is persisted as one RocksDB value.
	successWatermarks    []successWatermark
	successWatermarkHead int
	watermarkSequence    uint64

	// lifecycleMu serializes queue lifecycle changes with processing state. Add
	// uses it while replacing a failed retry, and ProcessBatch holds it across
	// the filesystem attempts and durable state commit so a late Add cannot
	// reintroduce a stale retry transition.
	lifecycleMu sync.Mutex

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
	q := &Queue{
		meta:        meta,
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
		retryStates: make(map[string]retryState),
	}
	q.loadRetryStates()
	q.loadSuccessWatermarks()
	return q
}

const retryStateValueSize = 16

func encodeRetryState(state retryState) []byte {
	value := make([]byte, retryStateValueSize)
	binary.BigEndian.PutUint64(value[0:8], uint64(state.cutoff))
	binary.BigEndian.PutUint64(value[8:16], uint64(state.retryAt))
	return value
}

func decodeRetryState(value []byte) (retryState, bool) {
	if len(value) != retryStateValueSize {
		return retryState{}, false
	}
	return retryState{
		cutoff:  int64(binary.BigEndian.Uint64(value[0:8])),
		retryAt: int64(binary.BigEndian.Uint64(value[8:16])),
	}, true
}

// loadRetryStates restores persisted path watermarks. It walks the separate
// retry-state namespace rather than the timestamp-ordered deletion backlog, so
// queue startup remains independent of the number of ordinary due entries.
func (q *Queue) loadRetryStates() {
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(keys.DeletionQueueRetryStatePrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		value := it.Value()
		filepath, err := keys.ParseDeletionQueueRetryStateKey(key.Data())
		state, valid := decodeRetryState(value.Data())
		key.Free()
		value.Free()
		if err != nil || !valid || state.cutoff <= 0 || state.retryAt < 0 || filepath == "" {
			continue
		}
		q.retryStates[filepath] = state
	}
}

const successWatermarkHeaderSize = 12

func encodeSuccessWatermark(cutoff int64, paths []string) []byte {
	size := successWatermarkHeaderSize
	for _, path := range paths {
		size += 4 + len(path)
	}
	value := make([]byte, size)
	binary.BigEndian.PutUint64(value[0:8], uint64(cutoff))
	binary.BigEndian.PutUint32(value[8:12], uint32(len(paths)))
	offset := successWatermarkHeaderSize
	for _, path := range paths {
		binary.BigEndian.PutUint32(value[offset:offset+4], uint32(len(path)))
		offset += 4
		copy(value[offset:offset+len(path)], path)
		offset += len(path)
	}
	return value
}

func decodeSuccessWatermark(value []byte) (int64, []string, bool) {
	if len(value) < successWatermarkHeaderSize {
		return 0, nil, false
	}
	cutoff := int64(binary.BigEndian.Uint64(value[0:8]))
	count := binary.BigEndian.Uint32(value[8:12])
	if cutoff <= 0 || uint64(count) > uint64(len(value)) {
		return 0, nil, false
	}

	paths := make([]string, 0, count)
	offset := successWatermarkHeaderSize
	for i := uint32(0); i < count; i++ {
		if offset+4 > len(value) {
			return 0, nil, false
		}
		length := int(binary.BigEndian.Uint32(value[offset : offset+4]))
		offset += 4
		if length > len(value)-offset {
			return 0, nil, false
		}
		paths = append(paths, string(value[offset:offset+length]))
		offset += length
	}
	if offset != len(value) || len(paths) == 0 {
		return 0, nil, false
	}
	return cutoff, paths, true
}

// loadSuccessWatermarks restores the per-batch generation records without
// scanning the timestamp-ordered deletion queue. The newest record wins if a
// path appears in more than one retained batch watermark.
func (q *Queue) loadSuccessWatermarks() {
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(keys.DeletionQueueWatermarkPrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		value := it.Value()
		cutoff, paths, valid := decodeSuccessWatermark(value.Data())
		if valid {
			watermarkKey := bytes.Clone(key.Data())
			q.successWatermarks = append(q.successWatermarks, successWatermark{
				key:    watermarkKey,
				cutoff: cutoff,
				paths:  paths,
			})
			for _, filepath := range paths {
				state, exists := q.retryStates[filepath]
				if !exists || state.cutoff <= cutoff {
					q.retryStates[filepath] = retryState{
						cutoff:       cutoff,
						watermarkKey: string(watermarkKey),
					}
				}
			}
		}
		key.Free()
		value.Free()
	}
	sort.Slice(q.successWatermarks, func(i, j int) bool {
		if q.successWatermarks[i].cutoff == q.successWatermarks[j].cutoff {
			return string(q.successWatermarks[i].key) < string(q.successWatermarks[j].key)
		}
		return q.successWatermarks[i].cutoff < q.successWatermarks[j].cutoff
	})
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

	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()

	timestamp := time.Now().UnixNano()
	previous, hadState := q.retryStates[filepath]
	supersedesDueRetry := hadState && previous.retryAt > 0 && timestamp >= previous.retryAt
	if supersedesDueRetry && timestamp == previous.retryAt {
		// Keep the new lifecycle strictly after the retry it supersedes. This is
		// only reachable on a same-nanosecond boundary, but avoids key collision
		// turning the new Add into another copy of the stale retry.
		timestamp++
	}
	key := keys.MakeDeletionQueueKey(timestamp, filepath)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.Put(key, []byte{0x01})

	var replacement retryState
	if supersedesDueRetry {
		// A retry that has reached its due time is ordered before this Add. Keep
		// a persisted protection cutoff at that retry key so all rows from the
		// failed lifecycle are retired without another filesystem attempt. The
		// new key is strictly later and therefore starts the new lifecycle.
		replacement = retryState{cutoff: previous.retryAt}
		batch.Delete(keys.MakeDeletionQueueKey(previous.retryAt, filepath))
		batch.Put(keys.MakeDeletionQueueRetryStateKey(filepath), encodeRetryState(replacement))
	}

	if err := q.meta.Handle().Write(wo, batch); err != nil {
		zlog.Error().
			Str("filepath", filepath).
			Err(err).
			Msg("deletion queue: failed to add entry")
		return err
	}
	if supersedesDueRetry {
		q.retryStates[filepath] = replacement
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

func queueKeysAreAtOrBefore(queueKeys [][]byte, cutoff int64) bool {
	for _, queueKey := range queueKeys {
		timestamp, _, err := keys.ParseDeletionQueueKey(queueKey)
		if err != nil || timestamp > cutoff {
			return false
		}
	}
	return true
}

// ProcessBatch processes a batch of deletion requests
func (q *Queue) ProcessBatch() {
	startTime := time.Now()
	defer func() {
		// Record batch duration in milliseconds
		metrics.DeletionQueueBatchDuration.Observe(float64(time.Since(startTime).Milliseconds()))
	}()
	// Keep every queue key observed for a filepath. A filepath is the unit of
	// deletion work, but each observed key must be retired explicitly so a
	// duplicate cannot remain due for the next processing tick.
	seen := make(map[string][][]byte) // filepath -> queue keys observed in this pass
	retired := make([][]byte, 0)

	// Scan and deduplicate
	q.lifecycleMu.Lock()
	retryStates := make(map[string]retryState, len(q.retryStates))
	for filepath, state := range q.retryStates {
		retryStates[filepath] = state
	}
	q.lifecycleMu.Unlock()

	nowNanos := time.Now().UnixNano()
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(keys.DeletionQueuePrefix)
	count := 0
	scannedThrough := int64(0)
	scanComplete := true

	// Scan from the head (oldest first), collecting up to BatchSize distinct
	// filepaths that are due (timestamp <= now). Entries whose deletion fails are
	// re-enqueued at now+RetryDelay (see below), so the head always advances
	// (no head-of-line starvation) and a persistently-stuck file is only retried
	// once per RetryDelay rather than every cycle. Because keys are timestamp-
	// ordered, the first not-yet-due entry means every entry after it is also in
	// the future, so we can stop scanning.
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
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
		scannedThrough = ts

		if ts > nowNanos {
			// Not yet due (a re-enqueued entry still in its backoff window); all
			// later entries are in the future too, so stop.
			scanComplete = false
			key.Free()
			it.Value().Free()
			break
		}

		// A watermark makes the exact queue-key timestamp the lifecycle
		// boundary. Old duplicates that were not selected in an earlier batch
		// can be retired without consuming a distinct-path slot or attempting
		// the filesystem again. A later Add has a newer timestamp and starts a
		// new lifecycle.
		if state, ok := retryStates[filepath]; ok &&
			ts <= state.cutoff && (state.retryAt == 0 || state.retryAt > nowNanos) {
			retired = append(retired, bytes.Clone(keyData))
			key.Free()
			it.Value().Free()
			continue
		}

		// Once the distinct-path limit is full, stop at the first new filepath.
		// Any duplicate for a path selected in an earlier pass is handled by its
		// watermark above, so the iterator never needs an unbounded suffix scan.
		if count >= q.config.BatchSize {
			scanComplete = false
			key.Free()
			it.Value().Free()
			break
		}

		// Count work by filepath, but retain every key observed for that
		// filepath. Only these exact keys belong to this pass: an Add that
		// arrives later owns a different key and must remain queued for its
		// own lifecycle.
		seen[filepath] = append(seen[filepath], bytes.Clone(keyData))
		if len(seen[filepath]) == 1 {
			count++
		}

		key.Free()
		it.Value().Free()
	}

	// A successful path may have duplicate rows beyond a batch boundary. Keep
	// its persisted watermark until the ordered scan has crossed that timestamp;
	// then no older duplicate can remain. Failed-path state is kept until its
	// delayed retry succeeds and is cleaned with its marker key.
	watermarkDeletes := make([]successWatermark, 0)
	watermarkHead := q.successWatermarkHead
	for watermarkHead < len(q.successWatermarks) {
		watermark := q.successWatermarks[watermarkHead]
		if !scanComplete && scannedThrough <= watermark.cutoff {
			break
		}
		watermarkHead++
		watermarkDeletes = append(watermarkDeletes, watermark)
	}

	if len(seen) == 0 && len(retired) == 0 && len(watermarkDeletes) == 0 {
		return
	}

	// Attempt deletions
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, queueKey := range retired {
		batch.Delete(queueKey)
	}
	for _, watermark := range watermarkDeletes {
		batch.Delete(watermark.key)
	}

	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()

	successful := 0
	failed := 0
	successfulPaths := make([]string, 0, len(seen))
	stateChanges := make(map[string]*retryState)
	stateNeeded := !scanComplete && scannedThrough <= nowNanos

	for filepath, queueKeys := range seen {
		previous, hadState := q.retryStates[filepath]
		if hadState && previous.retryAt == 0 && queueKeysAreAtOrBefore(queueKeys, previous.cutoff) {
			// Add can supersede a due retry after the iterator snapshot was
			// created. Retire only the old keys observed by this pass; the newer
			// Add remains in RocksDB for the next lifecycle attempt.
			for _, queueKey := range queueKeys {
				batch.Delete(queueKey)
			}
			continue
		}
		// One filesystem attempt represents the whole logical deletion. The
		// queue rows observed above are retired below in the same WriteBatch,
		// so a failed attempt can be replaced by one delayed retry instead of
		// leaving duplicate due rows behind.
		deleted := q.tryDelete(filepath)
		for _, queueKey := range queueKeys {
			batch.Delete(queueKey)
		}
		if hadState && previous.retryAt > 0 {
			// A later generation may be processed before the old delayed retry
			// becomes due. Remove that old queue row along with its marker so it
			// cannot trigger an extra attempt for the new lifecycle.
			batch.Delete(keys.MakeDeletionQueueKey(previous.retryAt, filepath))
		}

		if deleted {
			successful++
			q.processed++
			// Increment processed counter
			metrics.DeletionQueueProcessed.Inc()

			next := retryState{cutoff: nowNanos}
			if stateNeeded {
				if hadState {
					// Replace any prior lifecycle marker with the batch success
					// watermark. This also removes the persisted protection marker
					// that Add creates when it supersedes a due retry.
					batch.Delete(keys.MakeDeletionQueueRetryStateKey(filepath))
				}
				// Persist one batch watermark below for every successful path. The
				// single value keeps this restart-safe without adding one RocksDB
				// write key per distinct path.
				stateChanges[filepath] = &next
				successfulPaths = append(successfulPaths, filepath)
			} else {
				if hadState {
					batch.Delete(keys.MakeDeletionQueueRetryStateKey(filepath))
				}
				stateChanges[filepath] = nil
			}
		} else {
			// Deletion failed and the file is still on disk (read-locked by an
			// active reader, read-only filesystem, ...). Re-enqueue one entry
			// under a future timestamp (now+RetryDelay) after deleting every
			// observed duplicate. The head keeps advancing so a run of undeletable
			// files cannot starve newer, deletable entries, and the backoff
			// bounds how often a persistently-stuck file is rewritten — the scan
			// above skips not-yet-due entries, so it is retried roughly once per
			// RetryDelay instead of every cycle. The file is never dropped; it is
			// reclaimed once a later attempt succeeds. tryDelete treats a missing
			// file as success, so re-enqueued entries only reference files that
			// still exist.
			retryAt := time.Now().Add(q.config.RetryDelay).UnixNano()
			batch.Put(keys.MakeDeletionQueueKey(retryAt, filepath), []byte{0x01})
			batch.Put(keys.MakeDeletionQueueRetryStateKey(filepath), encodeRetryState(retryState{
				cutoff:  nowNanos,
				retryAt: retryAt,
			}))
			stateChanges[filepath] = &retryState{
				cutoff:  nowNanos,
				retryAt: retryAt,
			}
			failed++
			q.failed++
			// Increment failed counter
			metrics.DeletionQueueFailed.Inc()
		}
	}

	var pendingWatermark *successWatermark
	if len(successfulPaths) > 0 {
		sort.Strings(successfulPaths)
		watermarkKey := keys.MakeDeletionQueueWatermarkKey(nowNanos, q.watermarkSequence)
		batch.Put(watermarkKey, encodeSuccessWatermark(nowNanos, successfulPaths))
		pending := successWatermark{
			key:    watermarkKey,
			cutoff: nowNanos,
			paths:  successfulPaths,
		}
		pendingWatermark = &pending
		for _, filepath := range successfulPaths {
			state := stateChanges[filepath]
			state.watermarkKey = string(watermarkKey)
		}
	}

	// Commit successful deletions, retired duplicates, state markers, and tail
	// re-enqueues as one durable transition.
	if batch.Count() > 0 {
		if err := q.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().
				Err(err).
				Msg("deletion queue: failed to commit batch")
		} else {
			for filepath, state := range stateChanges {
				if state == nil {
					delete(q.retryStates, filepath)
				} else {
					q.retryStates[filepath] = *state
				}
			}
			if pendingWatermark != nil {
				q.successWatermarks = append(q.successWatermarks, *pendingWatermark)
				q.watermarkSequence++
			}

			// The watermark head and its per-path protections are part of the
			// same durable transition as the deletes above. Do not advance either
			// one until RocksDB has accepted the batch; otherwise a failed write
			// would make the next scan treat an old duplicate as a new lifecycle.
			q.successWatermarkHead = watermarkHead
			for _, watermark := range watermarkDeletes {
				for _, filepath := range watermark.paths {
					state, ok := q.retryStates[filepath]
					if !ok || state.retryAt != 0 || state.cutoff != watermark.cutoff ||
						state.watermarkKey != string(watermark.key) {
						continue
					}
					delete(q.retryStates, filepath)
				}
			}
			if q.successWatermarkHead > 64 && q.successWatermarkHead*2 >= len(q.successWatermarks) {
				q.successWatermarks = append([]successWatermark(nil), q.successWatermarks[q.successWatermarkHead:]...)
				q.successWatermarkHead = 0
			}
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
	type retryStateDeletion struct {
		filepath string
		state    retryState
	}
	pendingStateDeletes := make([]retryStateDeletion, 0)
	pendingPruned := 0

	commitBatch := func() {
		if batch.Count() == 0 {
			return
		}

		q.lifecycleMu.Lock()
		defer q.lifecycleMu.Unlock()

		// Add state-marker deletes only after taking the lifecycle lock. If Add
		// replaced a failed retry while this batch was being scanned, leaving its
		// marker out of this write preserves the newer lifecycle protection.
		for _, pending := range pendingStateDeletes {
			if state, ok := q.retryStates[pending.filepath]; ok && state == pending.state {
				batch.Delete(keys.MakeDeletionQueueRetryStateKey(pending.filepath))
			}
		}
		if err := q.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().
				Err(err).
				Msg("deletion queue: failed to prune batch")
		} else {
			// Keep the in-memory lifecycle state aligned with the durable
			// deletion. A failed write must leave the state protecting any
			// duplicate rows that remain in RocksDB.
			for _, pending := range pendingStateDeletes {
				if state, ok := q.retryStates[pending.filepath]; ok && state == pending.state {
					delete(q.retryStates, pending.filepath)
				}
			}
			q.pruned += int64(pendingPruned)
			for i := 0; i < pendingPruned; i++ {
				metrics.DeletionQueuePruned.Inc()
			}
			pruned += pendingPruned
		}
		batch.Clear()
		pendingStateDeletes = pendingStateDeletes[:0]
		pendingPruned = 0
	}

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
			// ProcessBatch to retry. Any success watermark is retained until the
			// ordered scan crosses its cutoff, so a pathname can be safely reused.
			if _, statErr := os.Stat(filepath); os.IsNotExist(statErr) {
				batch.Delete(bytes.Clone(keyData))
				q.lifecycleMu.Lock()
				state, ok := q.retryStates[filepath]
				q.lifecycleMu.Unlock()
				if ok && state.retryAt > 0 {
					// A failed lifecycle may have a delayed queue row after
					// this old duplicate. Remove that row too; otherwise a
					// later pathname reuse could process the stale retry.
					batch.Delete(keys.MakeDeletionQueueKey(state.retryAt, filepath))
					pendingStateDeletes = append(pendingStateDeletes, retryStateDeletion{
						filepath: filepath,
						state:    state,
					})
				}
				// A success watermark has retryAt == 0. Leave both its
				// marker and in-memory protection until ProcessBatch scans
				// past the cutoff; this old row may not be the last duplicate.
				pendingPruned++
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
			commitBatch()
		}
	}

	// Commit final batch
	commitBatch()

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
