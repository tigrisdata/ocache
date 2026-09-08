// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/keys"
)

// accessUpdate represents a single access time update request. The time is
// kept at full precision: the access index orders entries by nanosecond and a
// put records its write time at that precision, so a read bump rounded down to
// the second would sort BEFORE keys written later in the same second and lose
// to them at eviction (issue #257).
type accessUpdate struct {
	key  string
	time time.Time
}

const (
	// accessIndexMultiGetBatchSize bounds each native access-index lookup group.
	// Keeping the group below the write-batch flush threshold limits the number
	// of native slices retained while preserving the existing write batching.
	accessIndexMultiGetBatchSize = 32
	// Small flushes use the existing individual-read path because a MultiGet's
	// argument setup is not worthwhile when there are only a few entries.
	accessIndexMultiGetMinBatchSize = 8
)

// accessUpdater handles asynchronous batched updates of access times for LRU tracking
type accessUpdater struct {
	updates       chan accessUpdate
	done          chan struct{}
	flush         chan chan int // Channel to request flush with the number of flushed entries
	storage       *Storage
	interval      time.Duration
	delay         time.Duration // The delay after which an access time update is considered stale and should be updated
	wg            sync.WaitGroup
	accessTimeLRU *lru.Cache[string, time.Time]
	batch         map[string]accessUpdate
	batchMutex    sync.Mutex
}

// newAccessUpdater creates a new access updater
func newAccessUpdater(s *Storage, bufferSize int, interval time.Duration, delay time.Duration) *accessUpdater {
	accessTimeLRU, err := lru.New[string, time.Time](bufferSize)
	if err != nil {
		zlog.Fatal().Err(err).Msg("accessUpdater: failed to create LRU cache")
	}

	return &accessUpdater{
		updates:       make(chan accessUpdate, bufferSize),
		done:          make(chan struct{}),
		flush:         make(chan chan int),
		storage:       s,
		interval:      interval,
		delay:         delay,
		accessTimeLRU: accessTimeLRU,
		batch:         make(map[string]accessUpdate),
	}
}

// Start begins the background goroutine for processing access updates
func (a *accessUpdater) Start() {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.run()
	}()

	zlog.Info().Msg("accessUpdater: started")
}

// Stop stops the access updater and waits for it to finish
func (a *accessUpdater) Stop() {
	zlog.Info().Msg("accessUpdater: stopping")

	close(a.done)
	a.wg.Wait()
}

// Update queues an access time update (non-blocking)
func (a *accessUpdater) Update(key string, accessTime time.Time) {
	select {
	case a.updates <- accessUpdate{key: key, time: accessTime}:
		// Update queued successfully
		metrics.LRUAccessUpdates.Inc()
	default:
		// Buffer full, drop the update (LRU tracking is best-effort)
		metrics.Errors.WithLabelValues("access_updater", "buffer_full").Inc()
	}
}

// UpdateNow queues an access time update with current time (non-blocking)
func (a *accessUpdater) UpdateNow(key string) {
	a.Update(key, time.Now())
}

// Flush forces all pending updates to be written to RocksDB immediately.
// It synchronizes with the background goroutine to ensure that all updates
// currently buffered in the `updates` channel are processed and flushed in a
// single, consistent batch. This avoids any races between the caller and the
// background `run` goroutine both trying to drain the `updates` channel.
//
// Flush is mainly useful for tests to ensure deterministic behaviour. It
// returns the number of distinct access updates flushed.
func (a *accessUpdater) Flush() int {
	zlog.Debug().Msg("accessUpdater: flushing (external request)")

	// Channel used to signal completion of the flush request.
	done := make(chan int)

	// Attempt to send the flush request. If the updater has already been
	// stopped (i.e. `done` is closed), return immediately.
	select {
	case a.flush <- done:
		// Wait until the background goroutine signals completion.
		return <-done
	case <-a.done:
		// The updater is shutting down; nothing to flush.
		return 0
	}
}

// run is the main loop that processes batched updates
func (a *accessUpdater) run() {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-a.done:
			// Flush remaining updates before exiting. We cannot use Flush() here
			// because that would attempt to send on the `flush` channel which is
			// serviced by this very goroutine. Instead, drain the channel and flush
			// directly.
			a.collectUpdates()
			a.flushBatch()
			return

		case update := <-a.updates:
			a.timeGateUpdate(update)

		case <-ticker.C:
			a.flushBatch()

		case doneCh := <-a.flush:
			// Synchronously handle external flush request.
			a.collectUpdates()
			doneCh <- a.flushBatch()
			close(doneCh)
		}
	}
}

// collectUpdates drains all pending updates from the channel
func (a *accessUpdater) collectUpdates() {
	zlog.Debug().Msg("accessUpdater: collecting updates")

	for {
		select {
		case update := <-a.updates:
			a.timeGateUpdate(update)
		default:
			return
		}
	}
}

// timeGateUpdate adds an update to the batch if it is stale and refreshes the
// key's recency in the access-time LRU.
func (a *accessUpdater) timeGateUpdate(update accessUpdate) {
	// Get marks a gated hit as recently used without changing its timestamp.
	accessTime, ok := a.accessTimeLRU.Get(update.key)
	// Only add to the batch when the timestamp is stale or the cache misses.
	if !ok || update.time.Sub(accessTime) > a.delay {
		a.addToBatch(update)

		// Also add the key to the LRU cache so that it is marked as most recently used
		a.accessTimeLRU.Add(update.key, update.time)
	}
}

// flushBatch writes a batch of access updates to RocksDB and returns its size.
func (a *accessUpdater) flushBatch() int {
	a.batchMutex.Lock()
	defer a.batchMutex.Unlock()

	if len(a.batch) == 0 {
		return 0
	}

	batchSize := len(a.batch)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	writeBatch := grocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	zlog.Debug().Msgf("accessUpdater: flushing batch of size %d", len(a.batch))

	flushWriteBatch := func() {
		cnt := writeBatch.Count()
		if err := a.storage.meta.Handle().Write(wo, writeBatch); err != nil {
			zlog.Error().Err(err).Msg("accessUpdater: failed to flush batch")
		} else {
			zlog.Info().Msgf("accessUpdater: flushed batch of size %d", cnt)
		}
		writeBatch.Clear()
	}

	addUpdate := func(key string, update accessUpdate, bucketIndexKey []byte, oldSlice *grocksdb.Slice) {
		if oldSlice != nil && oldSlice.Exists() {
			// Delete the old bucketed entry before adding its replacement.
			writeBatch.Delete(oldSlice.Data())
		}

		// Create the new bucketed entry.
		newKey := keys.MakeBucketedAccessKey(key, update.time)
		writeBatch.Put(newKey, []byte{})

		// Update the secondary index.
		writeBatch.Put(bucketIndexKey, newKey)

		// Flush the batch periodically to avoid writing a large batch at once.
		if writeBatch.Count() > 1000 {
			flushWriteBatch()
		}
	}

	flushIndividualEntry := func(key string, update accessUpdate, bucketIndexKey []byte) {
		slice, getErr := a.storage.meta.Handle().Get(ro, bucketIndexKey)
		if getErr == nil {
			addUpdate(key, update, bucketIndexKey, slice)
		} else {
			// The existing path still installs the replacement when the old index
			// lookup fails; it only skips the old-entry deletion.
			addUpdate(key, update, bucketIndexKey, nil)
		}
		if slice != nil {
			slice.Free()
		}
	}

	if batchSize < accessIndexMultiGetMinBatchSize {
		for key, update := range a.batch {
			flushIndividualEntry(key, update, keys.MakeBucketedAccessIndexKey(key))
		}
	} else {
		freeSlices := func(slices grocksdb.Slices) {
			for _, slice := range slices {
				if slice != nil {
					slice.Free()
				}
			}
		}

		flushIndividual := func(batchKeys []string, updates []accessUpdate, bucketIndexKeys [][]byte) {
			for i, bucketIndexKey := range bucketIndexKeys {
				flushIndividualEntry(batchKeys[i], updates[i], bucketIndexKey)
			}
		}

		flushMultiGetGroup := func(batchKeys []string, updates []accessUpdate, bucketIndexKeys [][]byte) {
			if len(bucketIndexKeys) < accessIndexMultiGetMinBatchSize {
				flushIndividual(batchKeys, updates, bucketIndexKeys)
				return
			}

			indexSlices, err := a.storage.meta.Handle().MultiGet(ro, bucketIndexKeys...)
			if err == nil && len(indexSlices) == len(bucketIndexKeys) {
				for i, slice := range indexSlices {
					addUpdate(batchKeys[i], updates[i], bucketIndexKeys[i], slice)
				}
				freeSlices(indexSlices)
				return
			}

			// MultiGet returns one aggregate error, so retry this group individually
			// to preserve the old per-key behavior when a single lookup fails.
			freeSlices(indexSlices)
			flushIndividual(batchKeys, updates, bucketIndexKeys)
		}

		batchKeys := make([]string, 0, accessIndexMultiGetBatchSize)
		updates := make([]accessUpdate, 0, accessIndexMultiGetBatchSize)
		bucketIndexKeys := make([][]byte, 0, accessIndexMultiGetBatchSize)
		for key, update := range a.batch {
			batchKeys = append(batchKeys, key)
			updates = append(updates, update)
			bucketIndexKeys = append(bucketIndexKeys, keys.MakeBucketedAccessIndexKey(key))
			if len(bucketIndexKeys) == accessIndexMultiGetBatchSize {
				flushMultiGetGroup(batchKeys, updates, bucketIndexKeys)
				batchKeys = batchKeys[:0]
				updates = updates[:0]
				bucketIndexKeys = bucketIndexKeys[:0]
			}
		}
		if len(bucketIndexKeys) > 0 {
			flushMultiGetGroup(batchKeys, updates, bucketIndexKeys)
		}
	}

	// Flush the remaining updates.
	if writeBatch.Count() > 0 {
		flushWriteBatch()
	}

	// Clear the batch.
	a.batch = make(map[string]accessUpdate)
	return batchSize
}

func (a *accessUpdater) addToBatch(update accessUpdate) {
	a.batchMutex.Lock()
	defer a.batchMutex.Unlock()

	a.batch[update.key] = update
}
