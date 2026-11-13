// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"io"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
)

func TestAccessUpdater_BasicUpdate(t *testing.T) {
	// Create storage without MaxDiskUsage to avoid conflicting accessUpdater
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create a standalone access updater for testing
	updater := newAccessUpdater(storage, 100, 10*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// Queue an update
	updater.UpdateNow("test-key-1")

	// Wait for the update to be processed
	time.Sleep(500 * time.Millisecond)

	// Verify the update was written to RocksDB
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Check that the secondary index was created
	bucketIndexKey := keys.MakeBucketedAccessIndexKey("test-key-1")
	slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice.Exists(), "Secondary index for test-key-1 should exist")

	// Verify the bucketed key exists
	bucketKey := slice.Data()
	// Make a copy of the key before freeing the slice (important!)
	bucketKeyCopy := make([]byte, len(bucketKey))
	copy(bucketKeyCopy, bucketKey)
	slice.Free()

	slice2, err := storage.meta.Handle().Get(ro, bucketKeyCopy)
	require.NoError(t, err)
	require.True(t, slice2.Exists(), "Bucketed key for test-key-1 should exist")
	slice2.Free()
}

func TestAccessUpdater_InBatchDeduplication(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater with longer interval to batch updates
	updater := newAccessUpdater(storage, 1000, 200*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// Queue multiple updates for the same key within one batch window
	firstUpdate := time.Now()
	for i := 0; i < 5; i++ {
		updater.Update("test-key-dedup", firstUpdate.Add(time.Duration(i)*time.Second))
		time.Sleep(10 * time.Millisecond)
	}

	// Also add updates for different keys
	updater.UpdateNow("test-key-other-1")
	updater.UpdateNow("test-key-other-2")

	// Wait for batch to flush
	time.Sleep(500 * time.Millisecond)

	// Verify only one update per key was written
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Check test-key-dedup (should have only the latest update)
	bucketIndexKey := keys.MakeBucketedAccessIndexKey("test-key-dedup")
	slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice.Exists())

	bucketKey := slice.Data()
	_, accessTime, err := keys.ParseBucketedAccessKey(bucketKey)
	require.NoError(t, err)

	// The access time should be the time of the first update, at full precision
	assert.Equal(t, firstUpdate.UnixNano(), accessTime.UnixNano())
	slice.Free()

	// Verify other keys were also written
	for _, key := range []string{"test-key-other-1", "test-key-other-2"} {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists())
		slice.Free()
	}
}

func TestAccessUpdater_BufferOverflow(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater with very small buffer
	updater := newAccessUpdater(storage, 2, 100*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// Try to queue more updates than buffer can hold
	// These should not block (best-effort)
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			updater.UpdateNow("test-key-overflow")
		}
		done <- true
	}()

	// Should complete quickly without blocking
	select {
	case <-done:
		// no blocking
	case <-time.After(250 * time.Millisecond):
		t.Fatal("UpdateNow blocked when buffer was full")
	}
}

func TestAccessUpdater_ExplicitFlush(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater with long interval
	updater := newAccessUpdater(storage, 100, 10*time.Second, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// Queue updates
	updater.UpdateNow("test-key-flush-1")
	updater.UpdateNow("test-key-flush-2")

	time.Sleep(100 * time.Millisecond)

	// Explicitly flush without waiting for interval
	require.Equal(t, 2, updater.Flush())

	// Verify updates were written immediately
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	for _, key := range []string{"test-key-flush-1", "test-key-flush-2"} {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists(), "Key %s should exist after flush", key)
		slice.Free()
	}
}

func TestAccessUpdater_UpdatesOldBucketedEntry(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater
	updater := newAccessUpdater(storage, 100, 10*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	// First update with specific timestamp
	firstTime := time.Now()
	updater.Update("test-key-bucket-update", firstTime)
	updater.Flush()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Get the first bucket key
	bucketIndexKey := keys.MakeBucketedAccessIndexKey("test-key-bucket-update")
	slice1, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice1.Exists(), "First bucketed entry for test-key-bucket-update should exist")
	firstBucketKey := make([]byte, len(slice1.Data()))
	copy(firstBucketKey, slice1.Data())
	slice1.Free()

	// Verify the first bucketed entry exists
	slice, err := storage.meta.Handle().Get(ro, firstBucketKey)
	require.NoError(t, err)
	require.True(t, slice.Exists())
	slice.Free()

	// Second update with timestamp 6 minutes later (simulate time passing)
	// This ensures the bucket key timestamp is actually different
	secondTime := firstTime.Add(6 * time.Minute)
	updater.Update("test-key-bucket-update", secondTime)
	updater.Flush()

	// Get the new bucket key
	slice2, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice2.Exists())
	secondBucketKey := make([]byte, len(slice2.Data()))
	copy(secondBucketKey, slice2.Data())
	slice2.Free()

	// Keys should be different (different timestamps)
	t.Logf("First bucket key:  %s", string(firstBucketKey))
	t.Logf("Second bucket key: %s", string(secondBucketKey))
	assert.NotEqual(t, firstBucketKey, secondBucketKey, "Keys should be different after time-gated update")

	// Old bucketed entry should be deleted
	slice3, err := storage.meta.Handle().Get(ro, firstBucketKey)
	require.NoError(t, err)
	assert.False(t, slice3.Exists(), "Old bucketed entry should be deleted")
	slice3.Free()

	// New bucketed entry should exist
	slice4, err := storage.meta.Handle().Get(ro, secondBucketKey)
	require.NoError(t, err)
	assert.True(t, slice4.Exists(), "New bucketed entry should exist")
	slice4.Free()
}

func TestAccessUpdater_TimeGating(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater
	updater := newAccessUpdater(storage, 100, 10*time.Millisecond, 5*time.Minute)
	updater.Start()
	defer updater.Stop()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Update multiple keys
	testKeys := []string{"key1", "key2", "key3"}
	for _, key := range testKeys {
		updater.UpdateNow(key)
	}
	time.Sleep(500 * time.Millisecond)

	// Get the access times for the keys
	keyAccessTimes := make(map[string]time.Time)
	for _, key := range testKeys {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists(), "Key %s should exist (first pass)", key)
		bucketKey := slice.Data()

		_, accessTime, err := keys.ParseBucketedAccessKey(bucketKey)
		require.NoError(t, err)
		keyAccessTimes[key] = accessTime
		slice.Free()
	}

	// Try to update them again (should be gated)
	for _, key := range testKeys {
		updater.UpdateNow(key)
	}
	time.Sleep(500 * time.Millisecond)

	// Each key should have exactly one entry (second updates were gated)
	for _, key := range testKeys {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists(), "Key %s should exist (second pass)", key)
		bucketKey := slice.Data()

		_, accessTime, err := keys.ParseBucketedAccessKey(bucketKey)
		require.NoError(t, err)
		require.Equal(t, keyAccessTimes[key], accessTime, "Key %s should have the same access time (second pass)", key)
		slice.Free()
	}

	// Simulate time passing for one key
	updater.accessTimeLRU.Add("key2", time.Now().Add(-6*time.Minute))
	updater.UpdateNow("key2")
	time.Sleep(500 * time.Millisecond)

	// key2 should have a new timestamp, others should not
	bucketIndexKey := keys.MakeBucketedAccessIndexKey("key2")
	slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
	require.NoError(t, err)
	require.True(t, slice.Exists(), "Key key2 should exist (third pass)")

	bucketKey := slice.Data()
	_, accessTime, err := keys.ParseBucketedAccessKey(bucketKey)
	require.NoError(t, err)

	// Should be recent
	assert.True(t, accessTime.After(keyAccessTimes["key2"]), "key2 access time %v should be after %v", accessTime, keyAccessTimes["key2"])
	slice.Free()
}

func TestAccessUpdater_StopFlushesRemaining(t *testing.T) {
	storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 0)
	defer cleanup()

	// Create access updater with long interval
	updater := newAccessUpdater(storage, 100, 10*time.Second, 5*time.Minute)
	updater.Start()

	// Queue updates
	updater.UpdateNow("test-key-stop-flush-1")
	updater.UpdateNow("test-key-stop-flush-2")

	// Stop should flush remaining updates
	updater.Stop()

	// Verify updates were written
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	for _, key := range []string{"test-key-stop-flush-1", "test-key-stop-flush-2"} {
		bucketIndexKey := keys.MakeBucketedAccessIndexKey(key)
		slice, err := storage.meta.Handle().Get(ro, bucketIndexKey)
		require.NoError(t, err)
		require.True(t, slice.Exists(), "Key %s should exist after stop", key)
		slice.Free()
	}
}

// TestAccessUpdater_TimeGatingRefreshesLRURecency verifies that a gated read
// keeps a hot key resident when distinct keys create capacity pressure.
func TestAccessUpdater_TimeGatingRefreshesLRURecency(t *testing.T) {
	initialAccessTime := time.Unix(1_000_000, 0)

	updater := newAccessUpdater(nil, 2, time.Hour, time.Hour)
	updater.timeGateUpdate(accessUpdate{key: "hot", time: initialAccessTime})
	updater.timeGateUpdate(accessUpdate{key: "other", time: initialAccessTime.Add(1 * time.Second)})
	updater.timeGateUpdate(accessUpdate{key: "hot", time: initialAccessTime.Add(2 * time.Second)})
	updater.timeGateUpdate(accessUpdate{key: "new", time: initialAccessTime.Add(3 * time.Second)})

	_, ok := updater.accessTimeLRU.Peek("hot")
	assert.True(t, ok, "a gated hot-key read must refresh LRU recency")

	firstHotUpdate := updater.batch["hot"].time
	updater.timeGateUpdate(accessUpdate{key: "hot", time: initialAccessTime.Add(4 * time.Second)})
	hotUpdate, ok := updater.batch["hot"]
	require.True(t, ok)
	assert.True(t, firstHotUpdate.Equal(hotUpdate.time),
		"a hot key within the delay must not be readmitted to the batch")
}

// TestAccessUpdater_ReadBumpOutranksEarlierWrites pins the ordering contract
// behind LRU read protection: a read bump must sort AFTER every key written
// before the read, including keys written later in the same wall-clock second.
// The updater used to queue whole seconds and rebuild the bump at the start of
// that second, so a key read at hh:mm:ss.8 sorted before a key written at
// hh:mm:ss.1 and was evicted first — the eviction e2e flake in issue #257.
func TestAccessUpdater_ReadBumpOutranksEarlierWrites(t *testing.T) {
	s, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1<<30)
	defer cleanup()
	require.NotNil(t, s.accessUpdater, "LRU with a disk cap must run the access updater")

	require.NoError(t, s.Put("read-old", bytes.NewReader([]byte("old")), 0))
	require.NoError(t, s.Put("written-later", bytes.NewReader([]byte("new")), 0))

	// Bump read-old AFTER written-later was written, then force the flush.
	s.accessUpdater.UpdateNow("read-old")
	require.Equal(t, 1, s.accessUpdater.Flush())

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	entry := func(key string) []byte {
		slice, err := s.meta.Handle().Get(ro, keys.MakeBucketedAccessIndexKey(key))
		require.NoError(t, err)
		require.True(t, slice.Exists(), "%s should have an access index entry", key)
		defer slice.Free()
		return append([]byte(nil), slice.Data()...)
	}
	bumped, written := entry("read-old"), entry("written-later")
	assert.Equal(t, 1, bytes.Compare(bumped, written),
		"a read bump must sort after a key written before the read, so eviction reaches it later:\n  bumped:  %s\n  written: %s", bumped, written)
}

func TestAccessUpdater_DelayedReadDoesNotUndoNewerOverwrite(t *testing.T) {
	const (
		targetKey = "overwritten"
		middleKey = "middle"
		laterKey  = "later"
	)
	value := bytes.Repeat([]byte("x"), 128)
	middleValue := bytes.Repeat([]byte("m"), len(value))

	s, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:         t.TempDir(),
		InlineThreshold:  1024,
		CompactThreshold: 4096,
		SegmentSize:      16 * 1024 * 1024,
		FdCacheSize:      1000,
		MaxDiskUsage:     1 << 20,
		EvictionPolicy:   EvictionPolicyLRU,
		CleanupInterval:  time.Hour,
	})
	require.NoError(t, err)
	defer s.Close()
	require.NotNil(t, s.accessUpdater, "LRU with a disk cap must run the access updater")

	// Hold the automatic notification admitted by Get until after the overwrite.
	// The replacement worker's long interval prevents a timer flush while the
	// notification is held; Flush below still exercises the worker path.
	s.accessUpdater.Stop()
	updater := newAccessUpdater(s, 16, time.Hour, DefaultAccessUpdateDelay)
	s.accessUpdater = updater

	require.NoError(t, s.Put(targetKey, bytes.NewReader(value), 0))
	require.NoError(t, s.Put(middleKey, bytes.NewReader(middleValue), 0))
	require.NoError(t, s.Put(laterKey, bytes.NewReader([]byte("l")), 0))

	reader, found, err := s.Get(targetKey, 0, 0)
	require.NoError(t, err)
	require.True(t, found, "the initial read must find the inline value")
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, value, got)

	var readUpdate accessUpdate
	select {
	case readUpdate = <-updater.updates:
	case <-time.After(time.Second):
		t.Fatal("Get did not admit an automatic access update")
	}
	require.Equal(t, targetKey, readUpdate.key)

	// Put a live, equal-sized key between the read time and the overwrite time.
	middleTime := time.Unix(readUpdate.time.Unix()+1, 0)
	require.True(t, readUpdate.time.Before(middleTime))
	s.SetAccessTime(middleKey, middleTime.Unix())
	updater.Start()
	require.Equal(t, 1, updater.Flush(), "the explicit middle timestamp should be persisted")

	indexTime := func(key string) time.Time {
		entry, ok := readAccessIndex(t, s, key)
		require.True(t, ok, "%s should have a current LRU entry", key)
		indexedKey, accessTime, err := keys.ParseBucketedAccessKey([]byte(entry))
		require.NoError(t, err)
		require.Equal(t, key, indexedKey)
		return accessTime
	}
	keyExists := func(key string) bool {
		ro := grocksdb.NewDefaultReadOptions()
		defer ro.Destroy()
		slice, err := s.meta.Handle().Get(ro, keys.MakeMetadataKey(key))
		require.NoError(t, err)
		defer slice.Free()
		return slice.Exists()
	}
	indexEntryExists := func(entry string) bool {
		ro := grocksdb.NewDefaultReadOptions()
		defer ro.Destroy()
		slice, err := s.meta.Handle().Get(ro, []byte(entry))
		require.NoError(t, err)
		defer slice.Free()
		return slice.Exists()
	}

	require.True(t, indexTime(middleKey).Equal(middleTime))
	require.Eventually(t, func() bool {
		return time.Now().Unix() > middleTime.Unix()
	}, 5*time.Second, time.Millisecond, "the overwrite must use a timestamp in a later second")
	require.NoError(t, s.Put(targetKey, bytes.NewReader(value), 0))
	writeEntry, writeEntryExists := readAccessIndex(t, s, targetKey)
	require.True(t, writeEntryExists)
	writeTime := indexTime(targetKey)
	require.True(t, middleTime.Before(writeTime))
	require.Greater(t, writeTime.Unix(), readUpdate.time.Unix(), "read and overwrite timestamps must differ by seconds")

	// A later explicit index update shares the flush batch. Its persisted time
	// proves the batch write succeeded and puts it after both eviction candidates.
	laterTime := time.Unix(writeTime.Unix()+1, 0)
	require.True(t, writeTime.Before(laterTime))
	updater.updates <- readUpdate
	s.SetAccessTime(laterKey, laterTime.Unix())
	updater.Flush()
	require.True(t, indexTime(laterKey).Equal(laterTime),
		"the later index update must prove the flush batch committed")
	currentEntry, currentIndexExists := readAccessIndex(t, s, targetKey)
	assert.True(t, currentIndexExists && currentEntry == writeEntry && indexEntryExists(currentEntry),
		"automatic LRU update must not undo recency established by a completed overwrite")

	// Walk the ordinary LRU index for one value's bytes. The old read time sorts
	// before middleTime on the broken implementation, so it evicts targetKey.
	evicted := s.cleaner.evictByIndex(lruEvictionIndex(), int64(len(value)))
	evictionHolds := evicted == 1 && keyExists(targetKey) && !keyExists(middleKey)
	require.True(t, evictionHolds,
		"ordinary LRU eviction should remove the intermediate key and retain the overwritten key")

	// A later admitted automatic read still advances the persisted timestamp.
	updater.accessTimeLRU.Remove(targetKey)
	reader, found, err = s.Get(targetKey, 0, 0)
	require.NoError(t, err)
	require.True(t, found, "the overwritten value must survive the LRU eviction")
	got, err = io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, 1, updater.Flush())
	require.True(t, indexTime(targetKey).After(writeTime), "a newer automatic read should advance recency")

	// An explicit SetAccessTime is not an automatic read and may deliberately
	// move the index to an older timestamp. Remove only the throttle cache entry
	// so this control tests flush semantics rather than the read-delay gate.
	updater.accessTimeLRU.Remove(targetKey)
	explicitTime := time.Unix(readUpdate.time.Unix()-60, 0)
	s.SetAccessTime(targetKey, explicitTime.Unix())
	require.Equal(t, 1, updater.Flush())
	require.True(t, indexTime(targetKey).Equal(explicitTime), "explicit SetAccessTime may set an older timestamp")
}

func TestAccessUpdater_RewritesMalformedCurrentIndex(t *testing.T) {
	prefixLen := len(keys.AccessBucketPrefix)
	bucketEnd := prefixLen + len(keys.AccessBucketFormat)
	timestampStart := bucketEnd + 1
	cases := []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "prefix", mutate: func(entry []byte) { entry[0] = '?' }},
		{name: "bucket byte", mutate: func(entry []byte) { entry[prefixLen] = 'X' }},
		{name: "invalid bucket date", mutate: func(entry []byte) { copy(entry[prefixLen:bucketEnd], "2023023000") }},
		{name: "bucket timestamp mismatch", mutate: func(entry []byte) {
			lastBucketDigit := bucketEnd - 1
			if entry[lastBucketDigit] == '0' {
				entry[lastBucketDigit] = '1'
			} else {
				entry[lastBucketDigit] = '0'
			}
		}},
		{name: "timestamp separator", mutate: func(entry []byte) { entry[timestampStart+19] = '?' }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			storage, cleanup := createTestStorage(t, 3600, 1024, 4096, 16*1024*1024, 1000, 1<<20)
			defer cleanup()
			require.NotNil(t, storage.accessUpdater)

			storage.accessUpdater.Stop()
			updater := newAccessUpdater(storage, 16, time.Hour, DefaultAccessUpdateDelay)
			storage.accessUpdater = updater

			const key = "malformed"
			value := []byte("value")
			require.NoError(t, storage.Put(key, bytes.NewReader(value), 0))

			futureTime := time.Now().Add(time.Hour)
			malformedEntry := keys.MakeBucketedAccessKey(key, futureTime)
			tc.mutate(malformedEntry)
			wo := grocksdb.NewDefaultWriteOptions()
			defer wo.Destroy()
			require.NoError(t, storage.meta.Handle().Put(wo, malformedEntry, []byte{}))
			require.NoError(t, storage.meta.Handle().Put(wo, keys.MakeBucketedAccessIndexKey(key), malformedEntry))

			reader, found, err := storage.Get(key, 0, 0)
			require.NoError(t, err)
			require.True(t, found)
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, value, got)

			var update accessUpdate
			select {
			case update = <-updater.updates:
			case <-time.After(time.Second):
				t.Fatal("Get did not admit an automatic access update")
			}
			require.Equal(t, key, update.key)
			require.True(t, futureTime.After(update.time))
			updater.updates <- update
			updater.Start()
			require.Equal(t, 1, updater.Flush())
			currentEntry, ok := readAccessIndex(t, storage, key)
			require.True(t, ok)
			require.NotEqual(t, string(malformedEntry), currentEntry,
				"malformed current entries must follow the existing refresh path")
			indexedKey, accessTime, err := keys.ParseBucketedAccessKey([]byte(currentEntry))
			require.NoError(t, err)
			require.Equal(t, key, indexedKey)
			require.True(t, accessTime.Equal(update.time))

			ro := grocksdb.NewDefaultReadOptions()
			defer ro.Destroy()
			oldEntry, err := storage.meta.Handle().Get(ro, malformedEntry)
			require.NoError(t, err)
			defer oldEntry.Free()
			require.False(t, oldEntry.Exists(), "the malformed ordered row should be replaced")
		})
	}
}

func benchmarkStorageGet(b *testing.B, storage *Storage, key string) {
	b.Helper()

	reader, found, err := storage.Get(key, 0, 0)
	if err != nil {
		b.Fatal(err)
	}
	if !found {
		b.Fatalf("key %q was not found", key)
	}

	value, err := io.ReadAll(reader)
	if err != nil {
		b.Fatal(err)
	}
	if closer, ok := reader.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			b.Fatal(err)
		}
	}
	if string(value) != key {
		b.Fatalf("key %q returned %q", key, value)
	}
}

// BenchmarkAccessUpdater_CapacityPressure drives Storage.Get through the
// updater worker and flushes each access group to include access-index work.
func BenchmarkAccessUpdater_CapacityPressure(b *testing.B) {
	originalLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(originalLevel) })

	storage, cleanup := createTestStorage(b, 3600, 1024, 4096, 16*1024*1024, 1000, 1<<30)
	b.Cleanup(cleanup)

	storage.accessUpdater.Stop()
	updater := newAccessUpdater(storage, 2, time.Hour, time.Hour)
	// Keep the capacity-two LRU while allowing the full access group to queue.
	updater.updates = make(chan accessUpdate, 16)
	storage.accessUpdater = updater
	updater.Start()

	for _, key := range []string{"hot", "cold-a", "cold-b"} {
		if err := storage.Put(key, bytes.NewReader([]byte(key)), 0); err != nil {
			b.Fatal(err)
		}
	}

	runAccessGroup := func() int {
		for _, key := range []string{"hot", "cold-a", "hot", "cold-b", "hot"} {
			benchmarkStorageGet(b, storage, key)
		}
		return updater.Flush()
	}

	// Establish the same steady-state LRU contents before timing either side.
	if flushed := runAccessGroup(); flushed != 3 {
		b.Fatalf("warm-up flushed %d updates, want 3", flushed)
	}
	for b.Loop() {
		runAccessGroup()
	}

	// Each entry flushed here performs one secondary-index read. The three
	// preloaded keys already have index entries, so each entry also rewrites
	// its old bucket, new bucket, and secondary index.
	flushed := runAccessGroup()
	if flushed < 2 {
		b.Fatalf("access group flushed %d updates, want at least 2", flushed)
	}
	b.ReportMetric(float64(flushed), "access-index-reads/op")
	b.ReportMetric(float64(flushed*3), "access-index-key-writes/op")
	b.ReportMetric(float64(flushed-2), "hot-key-readmissions/op")
}
