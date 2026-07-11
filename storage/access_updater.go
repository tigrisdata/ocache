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

// accessUpdate represents a single access time update request
type accessUpdate struct {
	key  string
	time int64
}

// accessUpdater handles asynchronous batched updates of access times for LRU tracking
type accessUpdater struct {
	updates       chan accessUpdate
	done          chan struct{}
	flush         chan chan struct{} // Channel to request flush with completion notification
	storage       *Storage
	interval      time.Duration
	delay         time.Duration // The delay after which an access time update is considered stale and should be updated
	wg            sync.WaitGroup
	accessTimeLRU *lru.Cache[string, int64]
	batch         map[string]accessUpdate
	batchMutex    sync.Mutex
}

// newAccessUpdater creates a new access updater
func newAccessUpdater(s *Storage, bufferSize int, interval time.Duration, delay time.Duration) *accessUpdater {
	accessTimeLRU, err := lru.New[string, int64](bufferSize)
	if err != nil {
		zlog.Fatal().Err(err).Msg("accessUpdater: failed to create LRU cache")
	}

	return &accessUpdater{
		updates:       make(chan accessUpdate, bufferSize),
		done:          make(chan struct{}),
		flush:         make(chan chan struct{}),
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
func (a *accessUpdater) Update(key string, accessTime int64) {
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
	a.Update(key, time.Now().Unix())
}

// Flush forces all pending updates to be written to RocksDB immediately.
// It synchronizes with the background goroutine to ensure that all updates
// currently buffered in the `updates` channel are processed and flushed in a
// single, consistent batch. This avoids any races between the caller and the
// background `run` goroutine both trying to drain the `updates` channel.
//
// Flush is mainly useful for tests to ensure deterministic behaviour.
func (a *accessUpdater) Flush() {
	zlog.Debug().Msg("accessUpdater: flushing (external request)")

	// Channel used to signal completion of the flush request.
	done := make(chan struct{})

	// Attempt to send the flush request. If the updater has already been
	// stopped (i.e. `done` is closed), return immediately.
	select {
	case a.flush <- done:
		// Wait until the background goroutine signals completion.
		<-done
	case <-a.done:
		// The updater is shutting down; nothing to flush.
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
			a.flushBatch()
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

// timeGateUpdate adds an update to the batch if it is stale
// and adds the key to the LRU cache so that it is marked as most recently used
func (a *accessUpdater) timeGateUpdate(update accessUpdate) {
	// Only add to the batch if the key in LRU cache is more than delay old
	// or if the key is not in the LRU cache
	accessTime, ok := a.accessTimeLRU.Peek(update.key)
	if (ok && time.Unix(update.time, 0).Sub(time.Unix(accessTime, 0)) > a.delay) || !ok {
		a.addToBatch(update)

		// Also add the key to the LRU cache so that it is marked as most recently used
		a.accessTimeLRU.Add(update.key, update.time)
	}
}

// flushBatch writes a batch of access updates to RocksDB
func (a *accessUpdater) flushBatch() {
	a.batchMutex.Lock()
	defer a.batchMutex.Unlock()

	if len(a.batch) == 0 {
		return
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	writeBatch := grocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	zlog.Debug().Msgf("accessUpdater: flushing batch of size %d", len(a.batch))

	for key, update := range a.batch {
		// Get the current bucket location from the secondary index
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := a.storage.meta.Handle().Get(ro, bucketIndexKey)
		if err == nil && slice.Exists() {
			// Delete the old bucketed entry
			oldBucketKey := slice.Data()
			writeBatch.Delete(oldBucketKey)
			slice.Free()
		}

		// Create the new bucketed entry
		accessTimeObj := time.Unix(update.time, 0)
		newKey := keys.MakeBucketedAccessKey(key, accessTimeObj)

		writeBatch.Put(newKey, []byte{})

		// Update the secondary index
		writeBatch.Put(bucketIndexKey, newKey)

		// Flush the batch periodically to avoid writing a large batch at once
		if writeBatch.Count() > 1000 {
			cnt := writeBatch.Count()
			if err := a.storage.meta.Handle().Write(wo, writeBatch); err != nil {
				zlog.Error().Err(err).Msg("accessUpdater: failed to flush batch")
			} else {
				zlog.Info().Msgf("accessUpdater: flushed batch of size %d", cnt)
			}

			writeBatch.Clear()
		}
	}

	// Flush the remaining updates
	if writeBatch.Count() > 0 {
		cnt := writeBatch.Count()
		if err := a.storage.meta.Handle().Write(wo, writeBatch); err != nil {
			zlog.Error().Err(err).Msg("accessUpdater: failed to flush batch")
		} else {
			zlog.Info().Msgf("accessUpdater: flushed batch of size %d", cnt)
		}
	}

	// Clear the batch
	a.batch = make(map[string]accessUpdate)
}

func (a *accessUpdater) addToBatch(update accessUpdate) {
	a.batchMutex.Lock()
	defer a.batchMutex.Unlock()

	a.batch[update.key] = update
}
