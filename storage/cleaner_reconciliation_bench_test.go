// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/rs/zerolog"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

const cleanerReconciliationBenchmarkRows = 32

type cleanerReconciliationFixture struct {
	storage      *Storage
	keys         []string
	rawKeys      []string
	total        int64
	maxDiskUsage int64
}

// BenchmarkCleanerCleanupLoopReconciliation drives a scheduled Cleaner tick,
// rather than calling reconciliation directly. Capped cases begin with missing
// LRU back-references, which is the retry path after an incomplete backfill:
// reconcileFromMetadata scans the rows, repairs the indexes, then the rest of
// cleanupLoop runs normally. Uncapped cases make the normal hourly condition
// due without waiting an hour. The fixture uses the default inline limit and
// keeps the cap above the live total so eviction is not measured.
func BenchmarkCleanerCleanupLoopReconciliation(b *testing.B) {
	originalLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(originalLevel) })

	for _, tc := range []struct {
		name        string
		inlineCount int
		rawCount    int
		capped      bool
	}{
		{name: "uncapped-inline-64KiB", inlineCount: cleanerReconciliationBenchmarkRows},
		{name: "uncapped-default-threshold-mixed-75pct-inline", inlineCount: 24, rawCount: 8},
		{name: "inline-64KiB", inlineCount: cleanerReconciliationBenchmarkRows, capped: true},
		{name: "default-threshold-mixed-75pct-inline", inlineCount: 24, rawCount: 8, capped: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.StopTimer()
			fixture := newCleanerReconciliationFixture(b, tc.inlineCount, tc.rawCount, tc.capped)

			b.ReportAllocs()
			b.StartTimer()
			for b.Loop() {
				b.StopTimer()
				cleaner, tickDone, releaseTick, timeout := prepareCleanerReconciliationTick(b, fixture)

				b.StartTimer()
				startCleanerLoop(cleaner)
				waitForCleanerTick(b, cleaner, tickDone, releaseTick, timeout)
				b.StopTimer()

				close(releaseTick)
				timeout.Stop()
				cleaner.Close()
				assertReconciledCleanerState(b, cleaner, fixture)
				// B.Loop requires the timer to be running before its next call;
				// that iteration immediately stops it before fixture setup.
				b.StartTimer()
			}
		})
	}
}

// TestCleanerCleanupLoopReconciliationTick exercises the same timer-driven
// capped and uncapped reconciliation paths as the benchmark. It verifies that
// each complete cleaner tick restores the total and, when capped, the missing
// LRU back-references.
func TestCleanerCleanupLoopReconciliationTick(t *testing.T) {
	for _, tc := range []struct {
		name        string
		inlineCount int
		rawCount    int
		capped      bool
	}{
		{name: "uncapped-inline", inlineCount: 2},
		{name: "uncapped-default-threshold-mixed", inlineCount: 1, rawCount: 1},
		{name: "capped-inline", inlineCount: 2, capped: true},
		{name: "capped-default-threshold-mixed", inlineCount: 1, rawCount: 1, capped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newCleanerReconciliationFixture(t, tc.inlineCount, tc.rawCount, tc.capped)
			cleaner, tickDone, releaseTick, timeout := prepareCleanerReconciliationTick(t, fixture)
			defer timeout.Stop()

			startCleanerLoop(cleaner)
			waitForCleanerTick(t, cleaner, tickDone, releaseTick, timeout)
			close(releaseTick)
			cleaner.Close()

			assertReconciledCleanerState(t, cleaner, fixture)
			if tc.capped {
				assertBenchmarkLRUIndexes(t, fixture.storage, fixture.keys)
			}
		})
	}
}

// TestCleanerReconciliationSkipsInvalidUTF8Metadata verifies that accounting
// skips rows rejected by protobuf string validation.
func TestCleanerReconciliationSkipsInvalidUTF8Metadata(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field byte
	}{
		{name: "raw_file_path", field: 4},
		{name: "segment_path", field: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage, err := NewStorageWithConfig(&StorageConfig{
				DiskPath:        t.TempDir(),
				InlineThreshold: DefaultInlineThreshold,
				CleanupInterval: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(storage.Close)

			wo := grocksdb.NewDefaultWriteOptions()
			defer wo.Destroy()
			value := []byte{tc.field<<3 | 2, 1, 0xff, 7 << 3, 1}
			if err := storage.meta.Handle().Put(wo, keys.MakeMetadataKey("invalid-utf8"), value); err != nil {
				t.Fatal(err)
			}

			storage.cleaner.totalSize.Store(0)
			storage.cleaner.calculateTotalSize()
			if got := storage.cleaner.TotalSize(); got != 0 {
				t.Fatalf("reconciled total = %d, want 0 for malformed metadata", got)
			}
		})
	}
}

// TestCleanerCleanupExpiredInlineData checks that the TTL path deletes expired
// inline metadata and corrects the tracked total.
func TestCleanerCleanupExpiredInlineData(t *testing.T) {
	storage, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: DefaultInlineThreshold,
		CleanupInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(storage.Close)

	data := bytes.Repeat([]byte("x"), DefaultInlineThreshold)
	value, err := proto.Marshal(&pb.ValueMessage{
		ValueType:   pb.ValueType_INLINE,
		Data:        data,
		ValueLength: int64(len(data)),
		Expiry:      time.Now().Add(-time.Second).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	if err := storage.meta.Handle().Put(wo, keys.MakeMetadataKey("expired-inline"), value); err != nil {
		t.Fatal(err)
	}

	storage.cleaner.totalSize.Store(int64(len(data)))
	storage.cleaner.cleanupExpiredKeys()
	if got := storage.cleaner.TotalSize(); got != 0 {
		t.Fatalf("total after TTL cleanup = %d, want 0", got)
	}
	_, found, err := storage.Get("expired-inline", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expired inline metadata remains after TTL cleanup")
	}
}

// TestCleanerCleanupDeletesInvalidMetadata keeps the TTL scanner's malformed-row
// behavior aligned with the former full protobuf decode.
func TestCleanerCleanupDeletesInvalidMetadata(t *testing.T) {
	storage, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        t.TempDir(),
		InlineThreshold: DefaultInlineThreshold,
		CleanupInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(storage.Close)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := "invalid-cleanup-metadata"
	// raw_file_path is a protobuf string, so this invalid UTF-8 row must be
	// treated as malformed even though it has no expiry.
	value := []byte{4<<3 | 2, 1, 0xff, 7 << 3, 1}
	if err := storage.meta.Handle().Put(wo, keys.MakeMetadataKey(key), value); err != nil {
		t.Fatal(err)
	}

	storage.cleaner.cleanupExpiredKeys()

	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	slice, err := storage.meta.Handle().Get(ro, keys.MakeMetadataKey(key))
	if err != nil {
		t.Fatal(err)
	}
	defer slice.Free()
	if slice.Exists() {
		t.Fatal("invalid metadata remains after TTL cleanup")
	}
}

func newCleanerReconciliationFixture(tb testing.TB, inlineCount, rawCount int, capped bool) *cleanerReconciliationFixture {
	tb.Helper()

	inlineSize := DefaultInlineThreshold
	rawSize := DefaultInlineThreshold + 1
	total := int64(inlineCount*inlineSize + rawCount*rawSize)
	var maxDiskUsage int64
	if capped {
		maxDiskUsage = total * 2
	}

	storage, err := NewStorageWithConfig(&StorageConfig{
		DiskPath:        tb.TempDir(),
		InlineThreshold: inlineSize,
		// Keep the supported default 64 MiB compaction threshold. Raw values
		// just above the inline limit are migrated to segments during setup, so
		// the timed tick sees a stable default-threshold mixed store.
		MaxDiskUsage:        maxDiskUsage,
		EvictionPolicy:      EvictionPolicyLRU,
		CleanupInterval:     time.Hour,
		DisableRecompaction: true,
	})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(storage.Close)

	fixture := &cleanerReconciliationFixture{
		storage:      storage,
		total:        total,
		maxDiskUsage: maxDiskUsage,
		keys:         make([]string, 0, inlineCount+rawCount),
		rawKeys:      make([]string, 0, rawCount),
	}
	inlineValue := bytes.Repeat([]byte("i"), inlineSize)
	for i := 0; i < inlineCount; i++ {
		key := fmt.Sprintf("benchmark-inline-%02d", i)
		if err := storage.Put(key, bytes.NewReader(inlineValue), 0); err != nil {
			tb.Fatal(err)
		}
		fixture.keys = append(fixture.keys, key)
	}
	rawValue := bytes.Repeat([]byte("r"), rawSize)
	for i := 0; i < rawCount; i++ {
		key := fmt.Sprintf("benchmark-raw-%02d", i)
		if err := storage.Put(key, bytes.NewReader(rawValue), 0); err != nil {
			tb.Fatal(err)
		}
		fixture.keys = append(fixture.keys, key)
		fixture.rawKeys = append(fixture.rawKeys, key)
	}
	compactBenchmarkRawValues(tb, fixture)

	if got := storage.TotalSize(); got != total {
		tb.Fatalf("initial total = %d, want %d", got, total)
	}
	return fixture
}

// compactBenchmarkRawValues finishes the normal default-threshold raw-to-segment
// migration before timing the cleaner. This keeps compactor I/O out of the
// cleanup-loop measurement while retaining the metadata shape it produces.
func compactBenchmarkRawValues(tb testing.TB, fixture *cleanerReconciliationFixture) {
	tb.Helper()
	if len(fixture.rawKeys) == 0 {
		return
	}
	if fixture.storage.compactThreshold != DefaultCompactThreshold {
		tb.Fatalf("compact threshold = %d, want default %d", fixture.storage.compactThreshold, DefaultCompactThreshold)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		fixture.storage.compactor.CompactFiles(context.Background(), 0)

		allSegments := true
		for _, key := range fixture.rawKeys {
			value := fixture.storage.existingValue(key, keys.MakeMetadataKey(key))
			if value == nil {
				tb.Fatalf("missing compacted metadata for %q", key)
			}
			if value.ValueType != pb.ValueType_SEGMENT {
				allSegments = false
				break
			}
		}
		if allSegments {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatal("timed out waiting for default-threshold raw values to compact")
		}
		time.Sleep(time.Millisecond)
	}
}

// prepareCleanerReconciliationTick makes a capped fixture look like a cache
// whose prior backfill was interrupted. For the uncapped fixture, it makes the
// normal hourly reconciliation condition due without waiting an hour; no
// back-reference work is selected when MaxDiskUsage is 0. All setup is outside
// the benchmark timer.
func prepareCleanerReconciliationTick(tb testing.TB, fixture *cleanerReconciliationFixture) (*Cleaner, <-chan struct{}, chan struct{}, *time.Timer) {
	tb.Helper()
	if fixture.maxDiskUsage > 0 {
		removeBenchmarkLRUIndexes(tb, fixture.storage, fixture.keys)
	}

	tickDone := make(chan struct{}, 1)
	releaseTick := make(chan struct{})
	cleaner := NewCleaner(fixture.storage, time.Microsecond, fixture.maxDiskUsage)
	cleaner.totalSize.Store(0)
	if fixture.maxDiskUsage > 0 {
		cleaner.backfillPending.Store(true)
	} else {
		cleaner.lastSizeRecalc = time.Now().Add(-totalSizeRecalcInterval - time.Nanosecond)
	}
	// Keep the filesystem-reserve branch in the ordinary tick while avoiding a
	// host-capacity-dependent eviction from the benchmark fixture.
	cleaner.diskUsageFn = func(string) (free, total int64, ok bool) {
		return 1 << 40, 1 << 40, true
	}
	cleaner.onTickComplete = func() {
		// Hold the loop at the end of this tick until the caller stops timing,
		// so a very short test interval cannot start a second tick.
		select {
		case tickDone <- struct{}{}:
		default:
		}
		<-releaseTick
	}

	return cleaner, tickDone, releaseTick, time.NewTimer(5 * time.Second)
}

func startCleanerLoop(cleaner *Cleaner) {
	cleaner.wg.Add(1)
	go cleaner.cleanupLoop()
}

func waitForCleanerTick(tb testing.TB, cleaner *Cleaner, tickDone <-chan struct{}, releaseTick chan struct{}, timeout *time.Timer) {
	tb.Helper()
	select {
	case <-tickDone:
	case <-timeout.C:
		close(releaseTick)
		cleaner.Close()
		tb.Fatal("timed out waiting for cleaner cleanupLoop tick")
	}
}

func assertReconciledCleanerState(tb testing.TB, cleaner *Cleaner, fixture *cleanerReconciliationFixture) {
	tb.Helper()
	if got := cleaner.totalSize.Load(); got != fixture.total {
		tb.Fatalf("reconciled total = %d, want %d", got, fixture.total)
	}
	if cleaner.backfillPending.Load() {
		tb.Fatal("completed reconciliation left backfillPending set")
	}
}

func removeBenchmarkLRUIndexes(tb testing.TB, storage *Storage, userKeys []string) {
	tb.Helper()

	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, userKey := range userKeys {
		indexKey := keys.MakeBucketedAccessIndexKey(userKey)
		slice, err := storage.meta.Handle().Get(ro, indexKey)
		if err != nil {
			tb.Fatal(err)
		}
		if !slice.Exists() {
			slice.Free()
			tb.Fatalf("missing LRU back-reference for %q", userKey)
		}
		bucketKey := append([]byte(nil), slice.Data()...)
		slice.Free()

		batch.Delete(indexKey)
		batch.Delete(bucketKey)
	}
	if err := storage.meta.Handle().Write(wo, batch); err != nil {
		tb.Fatal(err)
	}
}

func assertBenchmarkLRUIndexes(tb testing.TB, storage *Storage, userKeys []string) {
	tb.Helper()

	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	for _, userKey := range userKeys {
		indexKey := keys.MakeBucketedAccessIndexKey(userKey)
		slice, err := storage.meta.Handle().Get(ro, indexKey)
		if err != nil {
			tb.Fatal(err)
		}
		if !slice.Exists() {
			slice.Free()
			tb.Fatalf("missing restored LRU back-reference for %q", userKey)
		}
		bucketKey := append([]byte(nil), slice.Data()...)
		slice.Free()

		bucketSlice, err := storage.meta.Handle().Get(ro, bucketKey)
		if err != nil {
			tb.Fatal(err)
		}
		if !bucketSlice.Exists() {
			bucketSlice.Free()
			tb.Fatalf("missing restored LRU entry for %q", userKey)
		}
		bucketSlice.Free()
	}
}
