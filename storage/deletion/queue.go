// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package deletion

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
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
	// cutoff is the ordered key boundary that was actually reached by the
	// bounded scan. duplicateCutoff is the timestamp snapshot horizon used to
	// retire same-generation rows that sit beyond that boundary.
	cutoff          int64
	duplicateCutoff int64
	retryAt         int64
	watermarkKey    string
	generation      string
}

type successWatermark struct {
	key             []byte
	cutoff          int64
	duplicateCutoff int64
	paths           []string
	generations     map[string]string
}

type deletionQueueEntry struct {
	key        []byte
	generation string
}

type deletionOutcome struct {
	filepath                    string
	entries                     []deletionQueueEntry
	generation                  string
	previous                    retryState
	hadState                    bool
	deleted                     bool
	allowMissingGenerationReuse bool
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
	// value is a delayed retry. A matching known generation may also use the
	// duplicate horizon for rows beyond the ordered boundary. The map is
	// populated before the next scan, so it covers duplicate rows left beyond a
	// distinct-path batch boundary.
	retryStates map[string]retryState

	// Success watermarks are ordered by cutoff so cleanup does not scan every
	// active path on every worker tick. Each record contains all paths selected
	// by one bounded batch and is persisted as one RocksDB value.
	successWatermarks    []successWatermark
	successWatermarkHead int
	watermarkSequence    uint64

	// lifecycleMu serializes queue lifecycle changes with durable processing
	// state. Add uses it while replacing a failed retry, and ProcessBatch holds it
	// around the state commit so a late Add cannot reintroduce a stale retry
	// transition. Filesystem attempts run without this queue-wide lock.
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

const (
	legacyRetryStateValueSize = 16
	retryStateValueHeaderSize = 28
	deletionQueueValueVersion = 2
	missingFileGeneration     = "missing"
)

func encodeRetryState(state retryState) []byte {
	value := make([]byte, retryStateValueHeaderSize+len(state.generation))
	binary.BigEndian.PutUint64(value[0:8], uint64(state.cutoff))
	binary.BigEndian.PutUint64(value[8:16], uint64(state.retryAt))
	duplicateCutoff := state.duplicateCutoff
	if duplicateCutoff == 0 {
		duplicateCutoff = state.cutoff
	}
	binary.BigEndian.PutUint64(value[16:24], uint64(duplicateCutoff))
	// The header grew from 16 to 28 bytes when the duplicate horizon and
	// pathname generation were made durable. Keep the length field last so old
	// 16-byte values remain decodable during an upgrade.
	binary.BigEndian.PutUint32(value[24:28], uint32(len(state.generation)))
	copy(value[28:], state.generation)
	return value
}

func decodeRetryState(value []byte) (retryState, bool) {
	if len(value) == legacyRetryStateValueSize {
		cutoff := int64(binary.BigEndian.Uint64(value[0:8]))
		return retryState{
			cutoff:          cutoff,
			duplicateCutoff: cutoff,
			retryAt:         int64(binary.BigEndian.Uint64(value[8:16])),
		}, true
	}
	if len(value) < retryStateValueHeaderSize {
		return retryState{}, false
	}
	generationLength := int(binary.BigEndian.Uint32(value[24:28]))
	if generationLength != len(value)-retryStateValueHeaderSize {
		return retryState{}, false
	}
	cutoff := int64(binary.BigEndian.Uint64(value[0:8]))
	duplicateCutoff := int64(binary.BigEndian.Uint64(value[16:24]))
	if duplicateCutoff == 0 {
		duplicateCutoff = cutoff
	}
	return retryState{
		cutoff:          cutoff,
		duplicateCutoff: duplicateCutoff,
		retryAt:         int64(binary.BigEndian.Uint64(value[8:16])),
		generation:      string(value[retryStateValueHeaderSize:]),
	}, true
}

func encodeDeletionQueueValue(generation string) []byte {
	if generation == "" {
		// Values written by older versions and synthetic queue rows use the
		// empty generation as a deliberately conservative wildcard.
		return []byte{0x01}
	}
	value := make([]byte, 5+len(generation))
	value[0] = deletionQueueValueVersion
	binary.BigEndian.PutUint32(value[1:5], uint32(len(generation)))
	copy(value[5:], generation)
	return value
}

func decodeDeletionQueueValue(value []byte) string {
	if len(value) < 5 || value[0] != deletionQueueValueVersion {
		return ""
	}
	generationLength := int(binary.BigEndian.Uint32(value[1:5]))
	if generationLength != len(value)-5 {
		return ""
	}
	return string(value[5:])
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

const (
	successWatermarkHeaderSize      = 12
	extendedWatermarkHeaderSize     = 20
	extendedWatermarkGenerationFlag = uint32(1 << 31)
)

func encodeSuccessWatermark(cutoff, duplicateCutoff int64, paths []string, generations map[string]string) []byte {
	extended := duplicateCutoff != cutoff
	for _, path := range paths {
		if generations[path] != "" {
			extended = true
			break
		}
	}

	if !extended {
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

	size := extendedWatermarkHeaderSize
	for _, path := range paths {
		size += 4 + len(generations[path]) + 4 + len(path)
	}
	value := make([]byte, size)
	binary.BigEndian.PutUint64(value[0:8], uint64(cutoff))
	binary.BigEndian.PutUint32(value[8:12], extendedWatermarkGenerationFlag|uint32(len(paths)))
	binary.BigEndian.PutUint64(value[12:20], uint64(duplicateCutoff))
	offset := extendedWatermarkHeaderSize
	for _, path := range paths {
		generation := generations[path]
		binary.BigEndian.PutUint32(value[offset:offset+4], uint32(len(generation)))
		offset += 4
		copy(value[offset:offset+len(generation)], generation)
		offset += len(generation)
		binary.BigEndian.PutUint32(value[offset:offset+4], uint32(len(path)))
		offset += 4
		copy(value[offset:offset+len(path)], path)
		offset += len(path)
	}
	return value
}

func decodeSuccessWatermark(value []byte) (int64, int64, []string, map[string]string, bool) {
	if len(value) < successWatermarkHeaderSize {
		return 0, 0, nil, nil, false
	}
	cutoff := int64(binary.BigEndian.Uint64(value[0:8]))
	rawCount := binary.BigEndian.Uint32(value[8:12])
	if cutoff <= 0 {
		return 0, 0, nil, nil, false
	}

	count := rawCount
	offset := successWatermarkHeaderSize
	duplicateCutoff := cutoff
	generations := make(map[string]string)
	if rawCount&extendedWatermarkGenerationFlag != 0 {
		count = rawCount &^ extendedWatermarkGenerationFlag
		if len(value) < extendedWatermarkHeaderSize {
			return 0, 0, nil, nil, false
		}
		duplicateCutoff = int64(binary.BigEndian.Uint64(value[12:20]))
		if duplicateCutoff <= 0 {
			return 0, 0, nil, nil, false
		}
		offset = extendedWatermarkHeaderSize
	}
	if count == 0 || uint64(count) > uint64(len(value)) {
		return 0, 0, nil, nil, false
	}

	paths := make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		if offset+4 > len(value) {
			return 0, 0, nil, nil, false
		}
		generation := ""
		if rawCount&extendedWatermarkGenerationFlag != 0 {
			generationLength := int(binary.BigEndian.Uint32(value[offset : offset+4]))
			offset += 4
			if generationLength > len(value)-offset {
				return 0, 0, nil, nil, false
			}
			generation = string(value[offset : offset+generationLength])
			offset += generationLength
			if offset+4 > len(value) {
				return 0, 0, nil, nil, false
			}
		}
		length := int(binary.BigEndian.Uint32(value[offset : offset+4]))
		offset += 4
		if length > len(value)-offset {
			return 0, 0, nil, nil, false
		}
		path := string(value[offset : offset+length])
		paths = append(paths, path)
		if generation != "" {
			generations[path] = generation
		}
		offset += length
	}
	if offset != len(value) || len(paths) == 0 {
		return 0, 0, nil, nil, false
	}
	return cutoff, duplicateCutoff, paths, generations, true
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
		_, sequence, sequenceErr := keys.ParseDeletionQueueWatermarkKey(key.Data())
		cutoff, duplicateCutoff, paths, generations, valid := decodeSuccessWatermark(value.Data())
		if valid {
			if sequenceErr == nil && sequence >= q.watermarkSequence {
				if sequence == ^uint64(0) {
					q.watermarkSequence = sequence
				} else {
					q.watermarkSequence = sequence + 1
				}
			}
			watermarkKey := bytes.Clone(key.Data())
			q.successWatermarks = append(q.successWatermarks, successWatermark{
				key:             watermarkKey,
				cutoff:          cutoff,
				duplicateCutoff: duplicateCutoff,
				paths:           paths,
				generations:     generations,
			})
			for _, filepath := range paths {
				state, exists := q.retryStates[filepath]
				if !exists || state.cutoff <= cutoff {
					q.retryStates[filepath] = retryState{
						cutoff:          cutoff,
						duplicateCutoff: duplicateCutoff,
						generation:      generations[filepath],
						watermarkKey:    string(watermarkKey),
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
	generation := fileGeneration(filepath)
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
	batch.Put(key, encodeDeletionQueueValue(generation))

	var replacement retryState
	if supersedesDueRetry {
		// A retry that has reached its due time is ordered before this Add. Keep
		// a persisted protection cutoff at that retry key so all rows from the
		// failed lifecycle are retired without another filesystem attempt. The
		// new key is strictly later and therefore starts the new lifecycle.
		replacement = retryState{
			cutoff:          previous.retryAt,
			duplicateCutoff: previous.duplicateCutoff,
			generation:      previous.generation,
		}
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

func deletionEntryProtected(entry deletionQueueEntry, state retryState, nowNanos int64) bool {
	timestamp, _, err := keys.ParseDeletionQueueKey(entry.key)
	if err != nil || state.retryAt != 0 && state.retryAt <= nowNanos {
		return false
	}
	if entry.generation != state.generation {
		return false
	}
	if state.generation == "" || state.generation == missingFileGeneration {
		// Legacy and missing-path rows cannot carry a durable file identity. Only
		// the ordered portion actually covered by the watermark is safe to
		// coalesce; a later due Add may be the request that belongs to a recreated
		// pathname.
		return timestamp <= state.cutoff
	}
	return timestamp <= state.cutoff || timestamp <= state.duplicateCutoff
}

func deletionEntriesProtected(entries []deletionQueueEntry, state retryState, nowNanos int64) bool {
	for _, entry := range entries {
		if !deletionEntryProtected(entry, state, nowNanos) {
			return false
		}
	}
	return true
}

// selectDeletionGeneration uses the newest observed queue row to identify the
// pathname lifecycle represented by the group. Retiring an older generation is
// safe because the newest row owns the result for this bounded pass; retaining
// every observed row also ensures a legacy row is never silently dropped when it
// follows a known-generation row. A legacy row is the newest row's wildcard and
// keeps the pre-generation behavior for that lifecycle.
func selectDeletionGeneration(entries []deletionQueueEntry) (selected, discarded []deletionQueueEntry, generation string) {
	if len(entries) == 0 {
		return nil, nil, ""
	}
	generation = entries[len(entries)-1].generation
	selected = append(selected, entries...)
	return selected, nil, generation
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
	seen := make(map[string][]deletionQueueEntry) // filepath -> entries observed in this pass
	retired := make([][]byte, 0)

	// Create the iterator snapshot before copying lifecycle state. An Add that
	// wins before this point is included in both views; an Add that wins after
	// it is excluded from the iterator and its newer state remains authoritative
	// at the commit check below.
	nowNanos := time.Now().UnixNano()
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	q.lifecycleMu.Lock()
	retryStates := make(map[string]retryState, len(q.retryStates))
	for filepath, state := range q.retryStates {
		retryStates[filepath] = state
	}
	q.lifecycleMu.Unlock()

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
		value := it.Value()
		keyData := key.Data()

		// Extract timestamp and filepath from key: !del/<timestamp>/<filepath>
		ts, filepath, err := keys.ParseDeletionQueueKey(keyData)
		if err != nil {
			key.Free()
			value.Free()
			continue
		}
		generation := decodeDeletionQueueValue(value.Data())
		scannedThrough = ts

		if ts > nowNanos {
			// Not yet due (a re-enqueued entry still in its backoff window); all
			// later entries are in the future too, so stop.
			scanComplete = false
			key.Free()
			value.Free()
			break
		}

		// A watermark makes the exact queue-key timestamp the lifecycle
		// boundary. Old duplicates that were not selected in an earlier batch
		// can be retired without consuming a distinct-path slot or attempting
		// the filesystem again. A later Add has a newer timestamp and starts a
		// new lifecycle.
		if state, ok := retryStates[filepath]; ok &&
			deletionEntryProtected(deletionQueueEntry{key: keyData, generation: generation}, state, nowNanos) {
			retired = append(retired, bytes.Clone(keyData))
			key.Free()
			value.Free()
			continue
		}

		// Once the distinct-path limit is full, stop at the first new filepath.
		// Any duplicate for a path selected in an earlier pass is handled by its
		// watermark above, so the iterator never needs an unbounded suffix scan.
		if count >= q.config.BatchSize {
			scanComplete = false
			key.Free()
			value.Free()
			break
		}

		// Count work by filepath, but retain every key observed for that
		// filepath. Only these exact keys belong to this pass: an Add that
		// arrives later owns a different key and must remain queued for its
		// own lifecycle.
		seen[filepath] = append(seen[filepath], deletionQueueEntry{
			key:        bytes.Clone(keyData),
			generation: generation,
		})
		if len(seen[filepath]) == 1 {
			count++
		}

		key.Free()
		value.Free()
	}

	// A successful path may have duplicate rows beyond a batch boundary. Keep
	// its persisted watermark until the ordered scan has crossed the duplicate
	// horizon; then no same-generation duplicate can remain. Failed-path state is
	// kept until its delayed retry succeeds and is cleaned with its marker key.
	watermarkDeletes := make([]successWatermark, 0)
	watermarkHead := q.successWatermarkHead
	for watermarkHead < len(q.successWatermarks) {
		watermark := q.successWatermarks[watermarkHead]
		if !scanComplete && scannedThrough <= watermark.duplicateCutoff {
			break
		}
		watermarkHead++
		watermarkDeletes = append(watermarkDeletes, watermark)
	}

	if len(seen) == 0 && len(retired) == 0 && len(watermarkDeletes) == 0 {
		return
	}

	// Attempt deletions without holding lifecycleMu. A queue tick can contain
	// many distinct paths, and Queue.Add must not wait for all filesystem work.
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

	stateNeeded := !scanComplete && scannedThrough <= nowNanos
	watermarkCutoff := nowNanos
	if stateNeeded {
		// This is the lifecycle boundary, not the timestamp at which this
		// function happened to start. A later Add beyond the ordered boundary
		// must remain eligible for its own deletion lifecycle.
		watermarkCutoff = scannedThrough
	}

	outcomes := make([]deletionOutcome, 0, len(seen))
	for filepath, entries := range seen {
		previous, hadState := retryStates[filepath]
		queueEntries, discardedEntries, generation := selectDeletionGeneration(entries)
		for _, entry := range discardedEntries {
			batch.Delete(entry.key)
		}
		if len(queueEntries) == 0 {
			continue
		}
		if hadState && previous.retryAt == 0 && deletionEntriesProtected(queueEntries, previous, nowNanos) {
			// Add can supersede a due retry after the iterator snapshot was
			// created. Retire only the old keys observed by this pass; a newer
			// generation remains in RocksDB for its own lifecycle attempt.
			for _, entry := range queueEntries {
				batch.Delete(entry.key)
			}
			continue
		}
		// One filesystem attempt represents the whole logical deletion. The
		// queue rows observed above are retired below in the same WriteBatch,
		// so a failed attempt can be replaced by one delayed retry instead of
		// leaving duplicate due rows behind.
		allowMissingGenerationReuse := false
		if generation == missingFileGeneration && hadState && previous.retryAt == 0 && previous.generation == generation {
			firstTimestamp, _, parseErr := keys.ParseDeletionQueueKey(queueEntries[0].key)
			allowMissingGenerationReuse = parseErr == nil && firstTimestamp > previous.cutoff
		}
		outcomes = append(outcomes, deletionOutcome{
			filepath:   filepath,
			entries:    queueEntries,
			generation: generation,
			previous:   previous,
			hadState:   hadState,
			deleted:    false,
			// Filesystem work is filled below outside lifecycleMu, once all
			// durable batch mutations have been assembled.
			allowMissingGenerationReuse: allowMissingGenerationReuse,
		})
	}

	// Distinct paths have independent file locks, so perform their stat/remove
	// attempts in parallel. This keeps pathname-generation protection without
	// adding one serialized filesystem round trip per selected path.
	workers := runtime.GOMAXPROCS(0) * 8
	const maxDeletionWorkers = 32
	if workers > maxDeletionWorkers {
		workers = maxDeletionWorkers
	}
	if workers > len(outcomes) {
		workers = len(outcomes)
	}
	if workers > 0 {
		jobs := make(chan int)
		var attempts sync.WaitGroup
		attempts.Add(workers)
		for worker := 0; worker < workers; worker++ {
			go func() {
				defer attempts.Done()
				for index := range jobs {
					outcome := &outcomes[index]
					outcome.deleted = q.tryDelete(outcome.filepath, outcome.generation, outcome.allowMissingGenerationReuse)
				}
			}()
		}
		for index := range outcomes {
			jobs <- index
		}
		close(jobs)
		attempts.Wait()
	}

	q.lifecycleMu.Lock()
	defer q.lifecycleMu.Unlock()

	successful := 0
	failed := 0
	successfulPaths := make([]string, 0, len(outcomes))
	successfulGenerations := make(map[string]string, len(outcomes))
	stateChanges := make(map[string]*retryState)

	for _, outcome := range outcomes {
		for _, entry := range outcome.entries {
			batch.Delete(entry.key)
		}
		if outcome.hadState && outcome.previous.retryAt > 0 {
			// A later generation may be processed before the old delayed retry
			// becomes due. Remove that old queue row along with its marker so it
			// cannot trigger an extra attempt for the new lifecycle.
			batch.Delete(keys.MakeDeletionQueueKey(outcome.previous.retryAt, outcome.filepath))
		}

		current, currentExists := q.retryStates[outcome.filepath]
		stateUnchanged := currentExists == outcome.hadState &&
			(!currentExists || current == outcome.previous)
		if !stateUnchanged {
			// Queue.Add changed the lifecycle while the filesystem attempt ran.
			// Retire only the rows observed by this pass; the newer state and its
			// queue row remain authoritative.
			continue
		}

		if outcome.deleted {
			successful++
			next := retryState{
				cutoff:          watermarkCutoff,
				duplicateCutoff: nowNanos,
				generation:      outcome.generation,
			}
			if stateNeeded {
				if outcome.hadState {
					// Replace any prior lifecycle marker with the batch success
					// watermark. This also removes the persisted protection marker
					// that Add creates when it supersedes a due retry.
					batch.Delete(keys.MakeDeletionQueueRetryStateKey(outcome.filepath))
				}
				// Persist one batch watermark below for every successful path. The
				// single value keeps this restart-safe without adding one RocksDB
				// write key per distinct path.
				stateChanges[outcome.filepath] = &next
				successfulPaths = append(successfulPaths, outcome.filepath)
				successfulGenerations[outcome.filepath] = outcome.generation
			} else {
				if outcome.hadState {
					batch.Delete(keys.MakeDeletionQueueRetryStateKey(outcome.filepath))
				}
				stateChanges[outcome.filepath] = nil
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
			batch.Put(keys.MakeDeletionQueueKey(retryAt, outcome.filepath), encodeDeletionQueueValue(outcome.generation))
			state := retryState{
				cutoff:          nowNanos,
				duplicateCutoff: nowNanos,
				retryAt:         retryAt,
				generation:      outcome.generation,
			}
			batch.Put(keys.MakeDeletionQueueRetryStateKey(outcome.filepath), encodeRetryState(state))
			stateChanges[outcome.filepath] = &state
			failed++
		}
	}

	var pendingWatermark *successWatermark
	if len(successfulPaths) > 0 {
		sort.Strings(successfulPaths)
		watermarkKey := keys.MakeDeletionQueueWatermarkKey(watermarkCutoff, q.watermarkSequence)
		batch.Put(watermarkKey, encodeSuccessWatermark(watermarkCutoff, nowNanos, successfulPaths, successfulGenerations))
		pending := successWatermark{
			key:             watermarkKey,
			cutoff:          watermarkCutoff,
			duplicateCutoff: nowNanos,
			paths:           successfulPaths,
			generations:     successfulGenerations,
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
						state.duplicateCutoff != watermark.duplicateCutoff ||
						state.generation != watermark.generations[filepath] ||
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
			q.processed += int64(successful)
			q.failed += int64(failed)
			metrics.DeletionQueueProcessed.Add(float64(successful))
			metrics.DeletionQueueFailed.Add(float64(failed))
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
func (q *Queue) tryDelete(filepath, expectedGeneration string, allowMissingGenerationReuse bool) bool {
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

	// A known queue generation must not unlink a newer file that reused the
	// pathname. An unknown legacy value skips this check for compatibility with
	// queue rows written before generations were recorded.
	if expectedGeneration != "" && !(expectedGeneration == missingFileGeneration && allowMissingGenerationReuse) {
		matches, comparable := sameFileGeneration(filepath, expectedGeneration)
		if !comparable {
			// A stat error is not evidence that the expected file is gone. Keep the
			// queue row so a later retry can make the safe decision.
			return false
		}
		if !matches {
			zlog.Debug().
				Str("filepath", filepath).
				Msg("deletion queue: stale generation, leaving current file")
			return true
		}
	}

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
