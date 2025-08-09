package storage

import (
	"container/heap"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/keys"
	"google.golang.org/protobuf/proto"
)

// lruEntry represents a key with its last access time for LRU tracking
type lruEntry struct {
	key        string
	lastAccess int64
	size       int64
}

// lruHeap implements heap.Interface for min-heap based on lastAccess
type lruHeap []lruEntry

func (h lruHeap) Len() int           { return len(h) }
func (h lruHeap) Less(i, j int) bool { return h[i].lastAccess < h[j].lastAccess }
func (h lruHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *lruHeap) Push(x interface{}) {
	*h = append(*h, x.(lruEntry))
}

func (h *lruHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// evictLRUKeys evicts the least recently used keys until we free enough space
func (c *Cleaner) evictLRUKeys(targetBytes int64) {
	start := time.Now()

	// Build a min-heap of all keys sorted by last access time
	h := &lruHeap{}
	heap.Init(h)

	ro := grocksdb.NewDefaultReadOptions()

	// First, collect all keys and their metadata
	keyMap := make(map[string]int64) // key -> size
	it := c.storage.meta.Handle().NewIterator(ro)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: LRU eviction interrupted by shutdown")
			it.Close()
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
		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(value, valueMsg); err == nil {
			keyMap[key] = valueMsg.ValueLength
		}

		it.Key().Free()
		it.Value().Free()
	}
	it.Close()

	// Now collect access times from the access index
	for key, size := range keyMap {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: LRU eviction interrupted by shutdown")
			return
		default:
		}
		accessKey := MakeAccessIndexKey(key)
		slice, err := c.storage.meta.Handle().Get(ro, accessKey)
		if err != nil || !slice.Exists() {
			// No access time recorded, use a very old time
			entry := lruEntry{
				key:        key,
				lastAccess: time.Now().Add(-365 * 24 * time.Hour).Unix(),
				size:       size,
			}
			heap.Push(h, entry)
		} else {
			lastAccess := ParseAccessTime(slice.Data())
			entry := lruEntry{
				key:        key,
				lastAccess: lastAccess,
				size:       size,
			}
			heap.Push(h, entry)
			slice.Free()
		}
	}

	// Now evict entries starting from the least recently used
	var evicted int64
	var evictedCount int

	wo := grocksdb.NewDefaultWriteOptions()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for h.Len() > 0 && evicted < targetBytes {
		// Check if we're shutting down
		select {
		case <-c.closeCh:
			zlog.Info().Msg("cleaner: LRU eviction interrupted by shutdown")
			return
		default:
		}
		entry := heap.Pop(h).(lruEntry)
		zlog.Debug().Str("key", entry.key).Int64("lastAccess", entry.lastAccess).Int64("size", entry.size).Msg("LRU: considering for eviction")

		// Get the full value to access file paths
		metaKey := keys.MakeMetadataKey(entry.key)
		slice, err := c.storage.meta.Handle().Get(ro, metaKey)
		if err != nil || !slice.Exists() {
			continue
		}

		valueMsg := &pb.ValueMessage{}
		if err := proto.Unmarshal(slice.Data(), valueMsg); err != nil {
			slice.Free()
			continue
		}
		slice.Free()

		// Delete the key with its metadata prefix
		batch.Delete(metaKey)

		// Delete access index entry
		accessKey := MakeAccessIndexKey(entry.key)
		batch.Delete(accessKey)

		evicted += entry.size
		evictedCount++

		// Delete associated files
		switch valueMsg.ValueType {
		case pb.ValueType_RAW_FILE:
			if err := c.storage.fileManager.Remove(valueMsg.RawFilePath); err != nil {
				zlog.Error().Err(err).Str("path", valueMsg.RawFilePath).Msg("cleaner: failed to delete raw file during LRU eviction")
			}
		}

		// Write batch periodically
		if batch.Count() >= 1000 {
			// Check if we're shutting down before writing
			select {
			case <-c.closeCh:
				zlog.Info().Msg("cleaner: LRU eviction interrupted by shutdown")
				return
			default:
			}

			if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
				zlog.Error().Err(err).Msg("cleaner: failed to write LRU eviction batch")
			}
			batch.Clear()
		}
	}

	// Write final batch
	if batch.Count() > 0 {
		if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Msg("cleaner: failed to write final LRU eviction batch")
		}
	}

	c.evictedKeys.Add(int64(evictedCount))
	c.totalSize.Add(-evicted)

	zlog.Info().
		Int("count", evictedCount).
		Int64("bytes", evicted).
		Dur("duration", time.Since(start)).
		Msg("cleaner: LRU eviction completed")
}
