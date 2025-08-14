package fd

import (
	"sync"
)

// FileLockManager manages file-level locks for concurrent access control.
// It provides RWMutex locks for files, allowing multiple concurrent readers
// while giving exclusive access to writers (Write/Delete operations).
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

// RemoveFileLock removes the lock for the given path.
// This should be called when a file is permanently deleted.
func (flm *FileLockManager) RemoveFileLock(path string) {
	flm.fileLocks.Delete(path)
}

// IsFileLocked returns true if the file is locked.
func (flm *FileLockManager) IsFileLocked(path string) bool {
	_, ok := flm.fileLocks.Load(path)
	return ok
}
