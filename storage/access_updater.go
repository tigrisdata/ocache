package storage

import (
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/keys"
	"google.golang.org/protobuf/proto"
)

// accessUpdate represents a single access time update request
type accessUpdate struct {
	key  string
	time int64
}

// accessUpdater handles asynchronous batched updates of access times for LRU tracking
type accessUpdater struct {
	updates  chan accessUpdate
	done     chan struct{}
	flush    chan chan struct{} // Channel to request flush with completion notification
	storage  *Storage
	interval time.Duration
	wg       sync.WaitGroup
}

// newAccessUpdater creates a new access updater
func newAccessUpdater(s *Storage, bufferSize int, interval time.Duration) *accessUpdater {
	return &accessUpdater{
		updates:  make(chan accessUpdate, bufferSize),
		done:     make(chan struct{}),
		flush:    make(chan chan struct{}),
		storage:  s,
		interval: interval,
	}
}

// Start begins the background goroutine for processing access updates
func (a *accessUpdater) Start() {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.run()
	}()
}

// Stop stops the access updater and waits for it to finish
func (a *accessUpdater) Stop() {
	close(a.done)
	a.wg.Wait()
}

// Update queues an access time update (non-blocking)
func (a *accessUpdater) Update(key string, accessTime int64) {
	select {
	case a.updates <- accessUpdate{key: key, time: accessTime}:
		// Update queued successfully
	default:
		// Buffer full, drop the update (LRU tracking is best-effort)
	}
}

// UpdateNow queues an access time update with current time (non-blocking)
func (a *accessUpdater) UpdateNow(key string) {
	a.Update(key, time.Now().Unix())
}

// Flush forces all pending updates to be written to RocksDB immediately
// This is mainly useful for testing to ensure deterministic behavior
func (a *accessUpdater) Flush() {
	done := make(chan struct{})
	a.flush <- done
	<-done // Wait for flush to complete
}

// run is the main loop that processes batched updates
func (a *accessUpdater) run() {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	batch := make(map[string]int64)

	for {
		select {
		case <-a.done:
			// Flush remaining updates before exiting
			a.collectUpdates(batch)
			if len(batch) > 0 {
				a.flushBatch(batch)
			}
			return

		case update := <-a.updates:
			// Only keep the latest access time for each key
			batch[update.key] = update.time

		case done := <-a.flush:
			// Handle explicit flush request
			a.collectUpdates(batch)
			if len(batch) > 0 {
				a.flushBatch(batch)
				batch = make(map[string]int64)
			}
			close(done) // Signal completion

		case <-ticker.C:
			if len(batch) > 0 {
				a.flushBatch(batch)
				// Clear the batch
				batch = make(map[string]int64)
			}
		}
	}
}

// collectUpdates drains all pending updates from the channel
func (a *accessUpdater) collectUpdates(batch map[string]int64) {
	for {
		select {
		case update := <-a.updates:
			batch[update.key] = update.time
		default:
			return
		}
	}
}

// flushBatch writes a batch of access updates to RocksDB
func (a *accessUpdater) flushBatch(batch map[string]int64) {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	writeBatch := grocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	for key, accessTime := range batch {
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
		accessTimeObj := time.Unix(accessTime, 0)
		newKey := keys.MakeBucketedAccessKey(key, accessTimeObj)

		// Get the size from metadata
		var size int64
		metaKey := keys.MakeMetadataKey(key)
		if slice, err := a.storage.meta.Handle().Get(ro, metaKey); err == nil && slice.Exists() {
			valueMsg := &pb.ValueMessage{}
			if err := proto.Unmarshal(slice.Data(), valueMsg); err == nil {
				size = valueMsg.ValueLength
			}
			slice.Free()
		}

		newVal := MakeBucketedAccessValue(size)
		writeBatch.Put(newKey, newVal)

		// Update the secondary index
		writeBatch.Put(bucketIndexKey, newKey)
	}

	if err := a.storage.meta.Handle().Write(wo, writeBatch); err != nil {
		zlog.Error().Err(err).Msg("accessUpdater: failed to flush batch")
	}
}
