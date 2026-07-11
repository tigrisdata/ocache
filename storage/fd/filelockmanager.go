// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package fd

import (
	"sync"
)

// FileLockManager manages file-level locks for concurrent access control.
// It provides RWMutex locks for files, allowing multiple concurrent readers
// while giving exclusive access to writers (Write/Delete operations).
//
// Thread-safety: All methods are safe for concurrent use.
//
// Usage pattern:
//
//	lock := manager.GetFileLock(path)
//	lock.Lock() // or lock.RLock() for read operations
//	defer lock.Unlock() // or lock.RUnlock()
//	// ... perform file operations ...
//
// Note: RemoveFileLock should only be called after the file has been
// successfully deleted and the lock has been released.
type FileLockManager struct {
	fileLocks sync.Map // path -> *sync.RWMutex
}

var (
	lockManagerOnce sync.Once
	lockManager     *FileLockManager
)

// GetFileLockManager returns the singleton instance of FileLockManager.
func GetFileLockManager() *FileLockManager {
	lockManagerOnce.Do(func() {
		lockManager = &FileLockManager{
			fileLocks: sync.Map{},
		}
	})
	return lockManager
}

// GetFileLock returns an RWMutex for the given path, creating it if it doesn't exist.
// An RWMutex allows multiple concurrent readers while still giving exclusive
// access to writers (Write/Delete), which is exactly the behaviour we need for
// raw files.
func (flm *FileLockManager) GetFileLock(path string) *sync.RWMutex {
	lock, _ := flm.fileLocks.LoadOrStore(path, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

// RemoveFileLock removes the lock for the given path from the manager.
// This should be called only after a file has been successfully deleted
// and its lock has been released. Calling this while a lock is still held
// may lead to unexpected behavior.
func (flm *FileLockManager) RemoveFileLock(path string) {
	flm.fileLocks.Delete(path)
}

// HasLock returns true if a lock exists in the manager for the given path.
// Note: This only indicates whether a lock object exists in the manager,
// not whether the lock is currently held by any goroutine.
// This method is primarily intended for testing purposes.
func (flm *FileLockManager) HasLock(path string) bool {
	_, ok := flm.fileLocks.Load(path)
	return ok
}
