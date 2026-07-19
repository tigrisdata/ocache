// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
)

// lruEvictionIndex describes the LRU access-bucket index (bucketed by last-access
// time, oldest first) for evictByIndex. A read re-buckets the key (via the async
// access updater), so recently-read keys survive. The per-key secondary index
// (!access_bucket_index/<key>) lets eviction skip superseded duplicate bucket
// entries left by an overwrite without taking a per-key lock — see evictByIndex.
func lruEvictionIndex() evictionIndex {
	return evictionIndex{
		policy: EvictionPolicyLRU,
		prefix: GetOldestAccessBucketPrefix(),
		parseKey: func(entryKey []byte) (string, error) {
			key, _, err := keys.ParseBucketedAccessKey(entryKey)
			return key, err
		},
		backrefKey: keys.MakeBucketedAccessIndexKey,
	}
}

// cleanupOldBuckets removes access entries from buckets older than the specified duration
// This prevents unbounded growth of the access index
func (c *Cleaner) cleanupOldBuckets(olderThan time.Duration) {
	start := time.Now()
	deleted := 0

	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Calculate cutoff time
	cutoff := time.Now().Add(-olderThan)
	cutoffBucket := keys.GetBucketedAccessKey(cutoff)

	it := c.storage.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := GetOldestAccessBucketPrefix()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		keyBytes := it.Key().Data()

		// Extract bucket from key
		bucket := keys.ExtractAccessBucketFromKey(keyBytes)
		if bucket == "" || bucket >= cutoffBucket {
			// We've reached buckets that are not old enough
			it.Key().Free()
			it.Value().Free()
			break
		}

		// Delete this old entry
		batch.Delete(keyBytes)
		deleted++

		it.Key().Free()
		it.Value().Free()

		// Write batch periodically
		if batch.Count() >= 1000 {
			if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
				zlog.Error().Err(err).Msg("cleaner: failed to write bucket cleanup batch")
			}
			batch.Clear()
		}
	}

	// Write final batch
	if batch.Count() > 0 {
		if err := c.storage.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Msg("cleaner: failed to write final bucket cleanup batch")
		}
	}

	if deleted > 0 {
		zlog.Info().
			Int("deleted", deleted).
			Dur("older_than", olderThan).
			Dur("duration_ms", time.Since(start)).
			Msg("cleaner: cleaned up old access buckets")
	}
}
