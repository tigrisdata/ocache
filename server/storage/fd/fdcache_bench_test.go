package fd

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func BenchmarkFdCacheConcurrentAcquireRelease(b *testing.B) {
	// Create temp directory and file
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0o644); err != nil {
		b.Fatal(err)
	}

	// Create fd cache
	cache := &FdCache{
		capacity:  100,
		fileLocks: sync.Map{},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			entry, err := cache.Acquire(testFile)
			if err != nil {
				b.Fatal(err)
			}
			cache.Release(testFile, entry)
		}
	})
}

func BenchmarkFdCacheMixedOperations(b *testing.B) {
	// Create temp directory and multiple files
	tmpDir := b.TempDir()
	numFiles := 10
	files := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		files[i] = filepath.Join(tmpDir, fmt.Sprintf("test%d.dat", i))
		if err := os.WriteFile(files[i], []byte("test data"), 0o644); err != nil {
			b.Fatal(err)
		}
	}

	// Create fd cache
	cache := &FdCache{
		capacity:  50,
		fileLocks: sync.Map{},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			file := files[i%numFiles]
			entry, err := cache.Acquire(file)
			if err != nil {
				b.Fatal(err)
			}
			cache.Release(file, entry)
			i++
		}
	})
}

func BenchmarkFdCacheHighContention(b *testing.B) {
	// Create temp directory and a single file
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0o644); err != nil {
		b.Fatal(err)
	}

	// Create fd cache
	cache := &FdCache{
		capacity:  10,
		fileLocks: sync.Map{},
	}

	// Pre-acquire to ensure cache hit path
	entry, _ := cache.Acquire(testFile)
	cache.Release(testFile, entry)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			entry, err := cache.Acquire(testFile)
			if err != nil {
				b.Fatal(err)
			}
			// Simulate some work
			_ = entry.File()
			cache.Release(testFile, entry)
		}
	})
}
