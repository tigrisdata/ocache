package fd

import (
	"sync"
	"testing"
)

func TestFileLockManager_Singleton(t *testing.T) {
	// Reset singleton for testing
	lockManagerOnce = sync.Once{}
	lockManager = nil

	// Get first instance
	flm1 := GetFileLockManager()
	if flm1 == nil {
		t.Fatal("GetFileLockManager() returned nil")
	}

	// Get second instance
	flm2 := GetFileLockManager()
	if flm2 == nil {
		t.Fatal("GetFileLockManager() returned nil")
	}

	// Verify they're the same instance
	if flm1 != flm2 {
		t.Error("GetFileLockManager() did not return singleton instance")
	}
}

func TestFileLockManager_GetFileLock(t *testing.T) {
	// Reset singleton for testing
	lockManagerOnce = sync.Once{}
	lockManager = nil

	flm := GetFileLockManager()

	path := "/test/file.txt"
	lock1 := flm.GetFileLock(path)
	lock2 := flm.GetFileLock(path)

	// Should return the same lock for the same path
	if lock1 != lock2 {
		t.Errorf("GetFileLock returned different locks for same path")
	}

	// Different paths should get different locks
	path2 := "/test/file2.txt"
	lock3 := flm.GetFileLock(path2)
	if lock1 == lock3 {
		t.Errorf("GetFileLock returned same lock for different paths")
	}
}

func TestFileLockManager_RemoveFileLock(t *testing.T) {
	// Reset singleton for testing
	lockManagerOnce = sync.Once{}
	lockManager = nil

	flm := GetFileLockManager()

	path := "/test/file.txt"
	lock1 := flm.GetFileLock(path)
	if lock1 == nil {
		t.Fatal("GetFileLock returned nil")
	}

	// Remove the lock
	flm.RemoveFileLock(path)

	// Getting the lock again should return a new lock
	lock2 := flm.GetFileLock(path)
	if lock2 == nil {
		t.Fatal("GetFileLock returned nil after RemoveFileLock")
	}

	// The locks should be different instances since we removed the first one
	if lock1 == lock2 {
		t.Error("GetFileLock returned same lock instance after RemoveFileLock")
	}
}

func TestFileLockManager_ConcurrentAccess(t *testing.T) {
	// Reset singleton for testing
	lockManagerOnce = sync.Once{}
	lockManager = nil

	flm := GetFileLockManager()

	var wg sync.WaitGroup
	const numGoroutines = 100
	const numPaths = 10

	// Test concurrent GetFileLock
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			path := "/test/file" + string(rune(id%numPaths)) + ".txt"
			lock := flm.GetFileLock(path)
			if lock == nil {
				t.Error("GetFileLock returned nil in concurrent access")
			}
		}(i)
	}
	wg.Wait()

	// Test concurrent RemoveFileLock
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			path := "/test/file" + string(rune(id%numPaths)) + ".txt"
			flm.RemoveFileLock(path)
		}(i)
	}
	wg.Wait()
}
