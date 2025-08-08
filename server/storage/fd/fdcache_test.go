package fd

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

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

	entry := &FileEntry{
		refs: 0,
		f:    tmpFile,
		mu:   &sync.RWMutex{},
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

func TestFdCache_RaceCondition(t *testing.T) {
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
