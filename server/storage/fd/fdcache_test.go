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

// Tests from main branch

func TestNewFdCache(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)
	if cache == nil {
		t.Fatal("NewFdCache returned nil")
	}
	if cache.capacity != 10 {
		t.Errorf("Expected capacity 10, got %d", cache.capacity)
	}

	cache2 := NewFdCache(20)
	if cache2 != cache {
		t.Error("NewFdCache should return singleton instance")
	}
	if cache2.capacity != 10 {
		t.Error("Singleton capacity should not change")
	}

	fdCache = nil
}

func TestGetFdCache(t *testing.T) {
	fdCache = nil
	_ = NewFdCache(5)
	cache := GetFdCache()
	if cache == nil {
		t.Fatal("GetFdCache returned nil")
	}
	if cache.capacity != 5 {
		t.Errorf("Expected capacity 5, got %d", cache.capacity)
	}
	fdCache = nil
}

func TestFileEntry(t *testing.T) {
	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	ready := make(chan struct{})
	close(ready) // Mark as ready
	entry := &FileEntry{
		refs:  0,
		f:     tmpFile,
		mu:    &sync.RWMutex{},
		ready: ready,
	}

	if entry.File() != tmpFile {
		t.Error("File() returned wrong file")
	}

	entry.Lock()
	if entry.refs != 0 {
		t.Errorf("expected refs 0, got %d", entry.refs)
	}
	entry.Unlock()

	entry.RLock()
	if entry.refs != 0 {
		t.Errorf("expected refs 0, got %d", entry.refs)
	}
	entry.RUnlock()
}

func TestFdCache_Acquire(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)

	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.WriteString("test content")
	tmpFile.Close()

	entry, err := cache.Acquire(tmpFile.Name())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if entry == nil {
		t.Fatal("Acquire returned nil entry")
	}
	if atomic.LoadInt32(&entry.refs) != 1 {
		t.Errorf("Expected refs=1, got %d", atomic.LoadInt32(&entry.refs))
	}

	entry2, err := cache.Acquire(tmpFile.Name())
	if err != nil {
		t.Fatalf("Second Acquire failed: %v", err)
	}
	if entry2 != entry {
		t.Error("Expected same entry from cache")
	}
	if atomic.LoadInt32(&entry.refs) != 2 {
		t.Errorf("Expected refs=2, got %d", atomic.LoadInt32(&entry.refs))
	}

	cache.Release(tmpFile.Name(), entry)
	cache.Release(tmpFile.Name(), entry2)

	fdCache = nil
}

func TestFdCache_AcquireNonExistent(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)

	_, err := cache.Acquire("/nonexistent/file.txt")
	if err != nil {
		// We expect an error for non-existent file
		if err.Error() == "" {
			t.Error("Expected non-empty error message for non-existent file")
		}
	} else {
		t.Error("Expected error for non-existent file, but got nil")
	}

	fdCache = nil
}

func TestFdCache_Release(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)

	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	entry, err := cache.Acquire(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&cache.size) != 1 {
		t.Errorf("Expected cache size 1, got %d", atomic.LoadInt32(&cache.size))
	}

	cache.Release(tmpFile.Name(), entry)

	if atomic.LoadInt32(&cache.size) != 0 {
		t.Errorf("Expected cache size 0 after release, got %d", atomic.LoadInt32(&cache.size))
	}

	fdCache = nil
}

func TestFdCache_Remove(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)

	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	entry, err := cache.Acquire(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	atomic.AddInt32(&entry.refs, 1)

	cache.Remove(tmpFile.Name())

	if _, ok := cache.entries.Load(tmpFile.Name()); ok {
		t.Error("Entry should be removed from cache")
	}

	fdCache = nil
}

func TestFdCache_GetFileLock(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)

	path := "/test/file.txt"
	lock1 := cache.GetFileLock(path)
	lock2 := cache.GetFileLock(path)

	if lock1 != lock2 {
		t.Error("GetFileLock should return same lock for same path")
	}

	fdCache = nil
}

func TestFdCache_CleanUp(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)

	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	entry, err := cache.Acquire(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	atomic.AddInt32(&entry.refs, 1)

	cache.CleanUp(tmpFile.Name())

	if _, ok := cache.entries.Load(tmpFile.Name()); ok {
		t.Error("Entry should be removed from cache")
	}
	if _, ok := cache.fileLocks.Load(tmpFile.Name()); ok {
		t.Error("File lock should be removed")
	}

	fdCache = nil
}

func TestFdCache_CapacityLimit(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(2)

	tmpDir := t.TempDir()
	files := make([]*os.File, 3)
	for i := 0; i < 3; i++ {
		f, err := os.CreateTemp(tmpDir, "test-*.txt")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		files[i] = f
	}

	entry1, _ := cache.Acquire(files[0].Name())
	entry2, _ := cache.Acquire(files[1].Name())

	if atomic.LoadInt32(&cache.size) != 2 {
		t.Errorf("Expected cache size 2, got %d", atomic.LoadInt32(&cache.size))
	}

	entry3, err := cache.Acquire(files[2].Name())
	if err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&cache.size) != 2 {
		t.Errorf("Cache size should remain at capacity, got %d", atomic.LoadInt32(&cache.size))
	}

	cache.Release(files[0].Name(), entry1)
	cache.Release(files[1].Name(), entry2)
	cache.Release(files[2].Name(), entry3)

	fdCache = nil
}

func TestFdCache_ConcurrentAccess(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(100)

	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				entry, err := cache.Acquire(tmpFile.Name())
				if err != nil {
					t.Errorf("Acquire failed: %v", err)
					return
				}
				cache.Release(tmpFile.Name(), entry)
			}
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&cache.size) != 0 {
		t.Errorf("Expected cache size 0, got %d", atomic.LoadInt32(&cache.size))
	}

	fdCache = nil
}

func TestFdCache_MultipleFiles(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)

	tmpDir := t.TempDir()
	numFiles := 5
	files := make([]string, numFiles)
	entries := make([]*FileEntry, numFiles)

	for i := 0; i < numFiles; i++ {
		f, err := os.CreateTemp(tmpDir, "test-*.txt")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		files[i] = f.Name()

		entries[i], err = cache.Acquire(files[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	if atomic.LoadInt32(&cache.size) != int32(numFiles) {
		t.Errorf("Expected cache size %d, got %d", numFiles, atomic.LoadInt32(&cache.size))
	}

	for i := 0; i < numFiles; i++ {
		cache.Release(files[i], entries[i])
	}

	if atomic.LoadInt32(&cache.size) != 0 {
		t.Errorf("Expected cache size 0, got %d", atomic.LoadInt32(&cache.size))
	}

	fdCache = nil
}

func BenchmarkFdCache_Acquire(b *testing.B) {
	fdCache = nil
	cache := NewFdCache(100)

	tmpFile, err := os.CreateTemp(b.TempDir(), "bench-*.txt")
	if err != nil {
		b.Fatal(err)
	}
	tmpFile.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, err := cache.Acquire(tmpFile.Name())
		if err != nil {
			b.Fatal(err)
		}
		cache.Release(tmpFile.Name(), entry)
	}

	fdCache = nil
}

func BenchmarkFdCache_ConcurrentAcquire(b *testing.B) {
	fdCache = nil
	cache := NewFdCache(100)

	tmpDir := b.TempDir()
	numFiles := 10
	files := make([]string, numFiles)

	for i := 0; i < numFiles; i++ {
		f, err := os.CreateTemp(tmpDir, "bench-*.txt")
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
		files[i] = f.Name()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			file := files[i%numFiles]
			i++
			entry, err := cache.Acquire(file)
			if err != nil {
				b.Fatal(err)
			}
			cache.Release(file, entry)
		}
	})

	fdCache = nil
}

func TestFdCache_LoadOrStore(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)

	tmpDir := t.TempDir()
	file1 := filepath.Join(tmpDir, "file1.txt")
	if err := os.WriteFile(file1, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	const goroutines = 10
	entries := make([]*FileEntry, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			e, err := cache.Acquire(file1)
			if err != nil {
				t.Errorf("Acquire failed: %v", err)
				return
			}
			entries[idx] = e
		}(i)
	}
	wg.Wait()

	first := entries[0]
	for i := 1; i < goroutines; i++ {
		if entries[i] != first {
			t.Error("All goroutines should get the same FileEntry")
		}
	}

	if atomic.LoadInt32(&first.refs) != int32(goroutines) {
		t.Errorf("Expected refs=%d, got %d", goroutines, atomic.LoadInt32(&first.refs))
	}

	for i := 0; i < goroutines; i++ {
		cache.Release(file1, entries[i])
	}

	fdCache = nil
}

// Additional race condition tests from our branch

func TestFdCacheRaceCondition(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create fd cache
	fdCache = nil
	cache := NewFdCache(10)

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

	fdCache = nil
}

func TestFdCacheConcurrentAcquireSamePath(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create fd cache
	fdCache = nil
	cache := NewFdCache(10)

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

	fdCache = nil
}

func TestFdCacheReleaseWhileAcquiring(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create fd cache
	fdCache = nil
	cache := NewFdCache(10)

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

	fdCache = nil
}

func TestFdCacheCapacityLimitExtended(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()

	// Create multiple test files
	const numFiles = 5
	const cacheCapacity = 3
	files := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		files[i] = filepath.Join(tmpDir, fmt.Sprintf("test%d.dat", i))
		if err := os.WriteFile(files[i], []byte("test data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Create fd cache with limited capacity
	fdCache = nil
	cache := NewFdCache(cacheCapacity)

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

	fdCache = nil
}

func TestFdCacheRemoveAndReacquire(t *testing.T) {
	// Create temp directory and file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("test data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create fd cache
	fdCache = nil
	cache := NewFdCache(10)

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

	fdCache = nil
}

func TestFdCache_RaceConditionExtended(t *testing.T) {
	fdCache = nil
	cache := NewFdCache(10)

	tmpFile, err := os.CreateTemp(t.TempDir(), "race-*.txt")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	path := tmpFile.Name()

	done := make(chan bool)

	go func() {
		for i := 0; i < 100; i++ {
			entry, _ := cache.Acquire(path)
			if entry != nil {
				cache.Release(path, entry)
			}
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			cache.Remove(path)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			cache.GetFileLock(path)
		}
		done <- true
	}()

	for i := 0; i < 3; i++ {
		<-done
	}

	fdCache = nil
}
