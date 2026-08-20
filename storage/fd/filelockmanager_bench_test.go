// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package fd

import (
	"sync/atomic"
	"testing"
)

// BenchmarkFileLockManager_GetFileLockHit measures a lookup for a path whose
// lock was registered by an earlier file operation.
func BenchmarkFileLockManager_GetFileLockHit(b *testing.B) {
	const path = "/test/file.txt"

	manager := &FileLockManager{}
	want := manager.GetFileLock(path)

	b.ReportAllocs()
	for b.Loop() {
		if got := manager.GetFileLock(path); got != want {
			b.Fatal("GetFileLock returned a different lock for an existing path")
		}
	}
}

// BenchmarkFileLockManager_GetFileLockMiss measures only the first lookup for
// a path. Resetting the manager while timing is stopped keeps every lookup on
// the miss path without charging RemoveFileLock to the benchmark.
func BenchmarkFileLockManager_GetFileLockMiss(b *testing.B) {
	const path = "/test/file.txt"

	manager := &FileLockManager{}

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		manager = &FileLockManager{}
		b.StartTimer()

		if lock := manager.GetFileLock(path); lock == nil {
			b.Fatal("GetFileLock returned nil")
		}
	}
}

func BenchmarkFileLockManager_GetFileLockHitParallel(b *testing.B) {
	const path = "/test/file.txt"

	manager := &FileLockManager{}
	want := manager.GetFileLock(path)
	var differentLock atomic.Bool

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if got := manager.GetFileLock(path); got != want {
				differentLock.Store(true)
				return
			}
		}
	})
	if differentLock.Load() {
		b.Fatal("GetFileLock returned a different lock for an existing path")
	}
}
