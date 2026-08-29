// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package deletion

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/rs/zerolog"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
)

// BenchmarkQueueProcessingLoopDuplicateBatch measures one production worker
// tick through Queue.Start and its processingLoop for duplicate due entries for
// one path plus a mixed set of distinct paths. Queue setup is outside the timed
// operation. The benchmark waits one production interval plus a fixed scheduling
// margin, then stops the queue before reading its depth; it does not observe the
// queue through an extra RocksDB scan while the timer is running. The worker's
// processingLoop remains the ordinary entry point. The
// ProcessInterval and BatchSize match the service configuration so the
// queue_entries/op metric compares the backlog after one real worker tick.
// The missing-file mode exercises the ENOENT success path, the existing-file
// mode exercises successful os.Remove calls, and the locked mode reports the
// queue depth left by one failed attempt.
func BenchmarkQueueProcessingLoopDuplicateBatch(b *testing.B) {
	for _, duplicateCount := range []int{1, 8, 64, 256} {
		for _, mode := range []string{"existing", "missing", "locked"} {
			name := fmt.Sprintf("duplicates=%d/mode=%s", duplicateCount, mode)
			b.Run(name, func(b *testing.B) {
				benchmarkQueueProcessingLoopDuplicateBatch(b, duplicateCount, mode)
			})
		}
	}
}

func benchmarkQueueProcessingLoopDuplicateBatch(b *testing.B, duplicateCount int, mode string) {
	const (
		batchSize         = 1000
		distinctPathCount = 64
		processInterval   = time.Second
	)

	meta, err := metadata.NewMetaDB(b.TempDir(), 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { meta.Close() })

	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(previousLogLevel) })

	root := b.TempDir()
	paths := make([]string, distinctPathCount)
	for i := range paths {
		paths[i] = filepath.Join(root, fmt.Sprintf("file-%03d", i))
	}

	// Each iteration needs a fresh Queue context before the worker starts. The
	// explicit N loop lets setup stay outside the timed operation; testing.B.Loop
	// owns the timer and cannot be bracketed by StopTimer/StartTimer.
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clearBenchmarkQueue(b, meta)
		if mode != "missing" {
			for _, path := range paths {
				if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
					b.Fatal(err)
				}
			}
		}

		queue := NewQueue(meta, Config{
			BatchSize:       batchSize,
			ProcessInterval: processInterval,
			PruneAge:        time.Hour,
			RetryDelay:      time.Hour,
		})
		seedBenchmarkQueue(b, queue, paths, duplicateCount)

		if mode == "locked" {
			lock := fd.GetFileLockManager().GetFileLock(paths[0])
			lock.RLock()
			b.StartTimer()
			queue.Start()
			time.Sleep(processInterval + processInterval/10)
			b.StopTimer()
			queue.Stop()
			b.ReportMetric(float64(queue.GetQueueDepth()), "queue_entries/op")
			lock.RUnlock()
			continue
		}

		b.StartTimer()
		queue.Start()
		time.Sleep(processInterval + processInterval/10)
		b.StopTimer()
		queue.Stop()
		b.ReportMetric(float64(queue.GetQueueDepth()), "queue_entries/op")
	}
}

func seedBenchmarkQueue(b *testing.B, queue *Queue, paths []string, duplicateCount int) {
	b.Helper()

	baseTimestamp := time.Now().Add(-time.Second).UnixNano()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for i := 0; i < duplicateCount; i++ {
		batch.Put(
			keys.MakeDeletionQueueKey(baseTimestamp+int64(i), paths[0]),
			[]byte{0x01},
		)
	}
	for i := 1; i < len(paths); i++ {
		batch.Put(
			keys.MakeDeletionQueueKey(baseTimestamp+int64(duplicateCount+i), paths[i]),
			[]byte{0x01},
		)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	if err := queue.meta.Handle().Write(wo, batch); err != nil {
		b.Fatal(err)
	}
}

func clearBenchmarkQueue(b *testing.B, meta *metadata.MetaDB) {
	b.Helper()

	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	it := meta.Handle().NewIterator(ro)
	defer it.Close()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	prefix := []byte(keys.DeletionQueuePrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		batch.Delete(bytes.Clone(key.Data()))
		key.Free()
		it.Value().Free()
	}

	if batch.Count() == 0 {
		return
	}
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	if err := meta.Handle().Write(wo, batch); err != nil {
		b.Fatal(err)
	}
}
