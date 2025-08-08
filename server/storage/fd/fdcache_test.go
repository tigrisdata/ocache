package fd

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFdCacheRaceCondition(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create fd cache
	cache := &FdCache{
		capacity:  10,
		fileLocks: sync.Map{},
	}

	// Test concurrent acquire and release operations
	const numGoroutines = 100
	const numOperations = 1000

	var wg sync.WaitGroup
	var acquireCount int32
	var releaseCount int32
	var errors int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				// Acquire
				entry, err := cache.Acquire(testFile)
				if err != nil {
					atomic.AddInt32(&errors, 1)
					continue
				}
				atomic.AddInt32(&acquireCount, 1)

				// Simulate some work
				time.Sleep(time.Microsecond)

				// Release
				cache.Release(testFile, entry)
				atomic.AddInt32(&releaseCount, 1)
			}
		}()
	}

	wg.Wait()

	// Verify no errors occurred
	if errors > 0 {
		t.Errorf("Got %d errors during concurrent operations", errors)
	}

	// Verify all acquires had matching releases
	if acquireCount != releaseCount {
		t.Errorf("Acquire count (%d) != Release count (%d)", acquireCount, releaseCount)
	}

	// Verify cache is empty after all releases
	empty := true
	cache.entries.Range(func(key, value interface{}) bool {
		entry := value.(*FileEntry)
		if atomic.LoadInt32(&entry.refs) > 0 {
			empty = false
			return false
		}
		return true
	})
	if !empty {
		t.Error("Cache still has entries with non-zero refs after all releases")
	}
}

func TestFdCacheConcurrentAcquireSamePath(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create fd cache
	cache := &FdCache{
		capacity:  10,
		fileLocks: sync.Map{},
	}

	// Test that multiple goroutines acquiring the same path
	// get the same FileEntry
	const numGoroutines = 50
	entries := make([]*FileEntry, numGoroutines)
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			entry, err := cache.Acquire(testFile)
			if err != nil {
				t.Errorf("Failed to acquire: %v", err)
				return
			}
			entries[idx] = entry
		}()
	}

	wg.Wait()

	// Verify all goroutines got the same entry
	if entries[0] == nil {
		t.Fatal("First entry is nil")
	}
	for i := 1; i < numGoroutines; i++ {
		if entries[i] != entries[0] {
			t.Errorf("Entry %d is different from entry 0", i)
		}
	}

	// Clean up - release all
	for i := 0; i < numGoroutines; i++ {
		if entries[i] != nil {
			cache.Release(testFile, entries[i])
		}
	}
}

func TestFdCacheReleaseWhileAcquiring(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create fd cache
	cache := &FdCache{
		capacity:  10,
		fileLocks: sync.Map{},
	}

	// First acquire
	entry1, err := cache.Acquire(testFile)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var entry2 *FileEntry
	var err2 error

	// Start goroutine to acquire while we're releasing
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Small delay to ensure Release starts first
		entry2, err2 = cache.Acquire(testFile)
	}()

	// Release the first entry
	time.Sleep(5 * time.Millisecond) // Ensure goroutine has started
	cache.Release(testFile, entry1)

	wg.Wait()

	// Verify second acquire succeeded
	if err2 != nil {
		t.Errorf("Second acquire failed: %v", err2)
	}
	if entry2 == nil {
		t.Error("Second entry is nil")
	}

	// Clean up
	if entry2 != nil {
		cache.Release(testFile, entry2)
	}
}

func TestFdCacheCapacityLimit(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()

	// Create multiple test files
	const numFiles = 5
	const cacheCapacity = 3
	files := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		files[i] = filepath.Join(tmpDir, fmt.Sprintf("test%d.dat", i))
		if err := os.WriteFile(files[i], []byte("test data"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Create fd cache with limited capacity
	cache := &FdCache{
		capacity:  cacheCapacity,
		fileLocks: sync.Map{},
	}

	// Acquire all files
	entries := make([]*FileEntry, numFiles)
	for i := 0; i < numFiles; i++ {
		entry, err := cache.Acquire(files[i])
		if err != nil {
			t.Fatalf("Failed to acquire file %d: %v", i, err)
		}
		entries[i] = entry
	}

	// Check that cache size doesn't exceed capacity
	if atomic.LoadInt32(&cache.size) > int32(cacheCapacity) {
		t.Errorf("Cache size (%d) exceeds capacity (%d)", cache.size, cacheCapacity)
	}

	// Release all entries
	for i := 0; i < numFiles; i++ {
		cache.Release(files[i], entries[i])
	}
}

func TestFdCacheRemove(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create fd cache
	cache := &FdCache{
		capacity:  10,
		fileLocks: sync.Map{},
	}

	// Acquire the file
	entry, err := cache.Acquire(testFile)
	if err != nil {
		t.Fatal(err)
	}

	// Remove from cache (simulating file deletion)
	cache.Remove(testFile)

	// Try to acquire again - should open a new file
	entry2, err2 := cache.Acquire(testFile)
	if err2 != nil {
		t.Fatalf("Failed to re-acquire after remove: %v", err2)
	}

	// Should be different entries
	if entry == entry2 {
		t.Error("Got same entry after Remove")
	}

	// Clean up
	cache.Release(testFile, entry2)
}