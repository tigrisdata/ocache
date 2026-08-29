// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package deletion

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
)

const (
	deletionBenchmarkBatchSize       = 1000
	deletionBenchmarkProcessInterval = time.Second
	deletionBenchmarkRetryDelay      = 30 * time.Second

	deletionBenchmarkCompletionPollInterval = 10 * time.Millisecond
	deletionBenchmarkCompletionTimeout      = 5 * time.Second
)

func newLockedRetryQueue(b *testing.B) (*Queue, []string, func()) {
	b.Helper()

	meta, err := metadata.NewMetaDB(b.TempDir(), 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	queue := NewQueue(meta, Config{
		BatchSize:       deletionBenchmarkBatchSize,
		ProcessInterval: deletionBenchmarkProcessInterval,
		PruneAge:        time.Hour,
		RetryDelay:      deletionBenchmarkRetryDelay,
	})

	paths := make([]string, deletionBenchmarkBatchSize)
	locks := make([]*sync.RWMutex, deletionBenchmarkBatchSize)
	lockManager := fd.GetFileLockManager()
	tmpDir := b.TempDir()
	for i := range paths {
		path := filepath.Join(tmpDir, fmt.Sprintf("locked-%04d", i))
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			b.Fatal(err)
		}

		lock := lockManager.GetFileLock(path)
		lock.RLock()
		paths[i] = path
		locks[i] = lock
	}

	return queue, paths, func() {
		for _, lock := range locks {
			lock.RUnlock()
		}
		for _, path := range paths {
			lockManager.RemoveFileLock(path)
		}
		meta.Close()
	}
}

func newUnlockedDeletionQueue(b *testing.B) (*Queue, []string, func()) {
	b.Helper()

	meta, err := metadata.NewMetaDB(b.TempDir(), 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	queue := NewQueue(meta, Config{
		BatchSize:       deletionBenchmarkBatchSize,
		ProcessInterval: deletionBenchmarkProcessInterval,
		PruneAge:        time.Hour,
		RetryDelay:      deletionBenchmarkRetryDelay,
	})

	paths := make([]string, deletionBenchmarkBatchSize)
	tmpDir := b.TempDir()
	for i := range paths {
		path := filepath.Join(tmpDir, fmt.Sprintf("unlocked-%04d", i))
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			b.Fatal(err)
		}
		paths[i] = path
	}

	return queue, paths, func() {
		meta.Close()
	}
}

func enqueueDeletionBatch(b *testing.B, queue *Queue, paths []string) {
	b.Helper()

	for _, path := range paths {
		if err := queue.Add(path); err != nil {
			b.Fatal(err)
		}
	}
}

func queueEntryKey(b *testing.B, queue *Queue) []byte {
	b.Helper()

	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()

	it := queue.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(keys.DeletionQueuePrefix)
	it.Seek(prefix)
	if !it.ValidForPrefix(prefix) {
		b.Fatal("queued deletion entry not found")
	}

	key := it.Key()
	defer key.Free()
	value := it.Value()
	defer value.Free()
	return bytes.Clone(key.Data())
}

func queueEntryRemoved(queue *Queue, key []byte) (bool, error) {
	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()

	value, err := queue.meta.Handle().Get(ro, key)
	if err != nil {
		return false, err
	}
	defer value.Free()
	return !value.Exists(), nil
}

func waitForQueueEntryRemoval(b *testing.B, queue *Queue, key []byte) {
	b.Helper()

	deadline := time.NewTimer(deletionBenchmarkCompletionTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(deletionBenchmarkCompletionPollInterval)
	defer poll.Stop()

	for {
		removed, err := queueEntryRemoved(queue, key)
		if err != nil {
			b.Fatalf("check queued deletion entry: %v", err)
		}
		if removed {
			return
		}

		select {
		case <-deadline.C:
			b.Fatal("timed out waiting for queue processing loop")
		case <-poll.C:
		}
	}
}

func processQueueBatchThroughLoop(b *testing.B, queue *Queue, key []byte) {
	b.Helper()

	queue.Start()
	defer queue.Stop()
	waitForQueueEntryRemoval(b, queue, key)
}

func benchmarkQueueProcessingLoopLockedRetryBacklog(b *testing.B) {
	b.Helper()

	b.StopTimer()
	queue, paths, cleanup := newLockedRetryQueue(b)
	defer cleanup()
	enqueueDeletionBatch(b, queue, paths)
	key := queueEntryKey(b, queue)

	b.StartTimer()
	processQueueBatchThroughLoop(b, queue, key)
	b.StopTimer()

	if queue.processed != 0 || queue.failed != deletionBenchmarkBatchSize {
		b.Fatalf("processingLoop processed=%d failed=%d, want processed=0 failed=%d", queue.processed, queue.failed, deletionBenchmarkBatchSize)
	}
	if depth := queue.GetQueueDepth(); depth != deletionBenchmarkBatchSize {
		b.Fatalf("processingLoop queue depth=%d, want %d", depth, deletionBenchmarkBatchSize)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			b.Fatalf("locked file removed: %v", err)
		}
	}
}

func benchmarkQueueProcessingLoopUnlockedDeletionBacklog(b *testing.B) {
	b.Helper()

	b.StopTimer()
	queue, paths, cleanup := newUnlockedDeletionQueue(b)
	defer cleanup()
	enqueueDeletionBatch(b, queue, paths)
	key := queueEntryKey(b, queue)

	b.StartTimer()
	processQueueBatchThroughLoop(b, queue, key)
	b.StopTimer()

	if queue.processed != deletionBenchmarkBatchSize || queue.failed != 0 {
		b.Fatalf("processingLoop processed=%d failed=%d, want processed=%d failed=0", queue.processed, queue.failed, deletionBenchmarkBatchSize)
	}
	if depth := queue.GetQueueDepth(); depth != 0 {
		b.Fatalf("processingLoop queue depth=%d, want 0", depth)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			b.Fatalf("file remained after processingLoop: %v", err)
		}
	}
}

// BenchmarkQueueProcessingLoopLockedRetryBacklog measures a full timer-driven
// batch of 1,000 deletions that must be retried. Each path already has a read
// lock, so tryDelete reaches an existing GetFileLock entry before re-enqueuing
// it after 30 seconds. The benchmark waits for removal of an initial queue key,
// which happens in ProcessBatch's final atomic write after the entire batch has
// run, rather than sleeping for a fixed number of ticker intervals.
func BenchmarkQueueProcessingLoopLockedRetryBacklog(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkQueueProcessingLoopLockedRetryBacklog(b)
		b.StartTimer()
	}
}

// BenchmarkQueueProcessingLoopUnlockedDeletionBacklog measures one full
// timer-driven batch of 1,000 ordinary deletions. It uses the same one-second
// cadence, batch size, and retry delay as the locked-retry benchmark. Removal
// of the initial queue key synchronizes with ProcessBatch's final atomic write
// instead of adding a fixed-duration wait to the measured operation.
func BenchmarkQueueProcessingLoopUnlockedDeletionBacklog(b *testing.B) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	defer zerolog.SetGlobalLevel(level)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkQueueProcessingLoopUnlockedDeletionBacklog(b)
		b.StartTimer()
	}
}
