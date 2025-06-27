package fd

import (
	"os"
	"sync"
	"sync/atomic"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/cache_service/server/utils"
)

// FileEntry wraps a cached os.File descriptor along with a per-file RWMutex and reference count.
// The lock protects concurrent readers/writers on the underlying file while refs ensures that the
// file descriptor stays open while in-use across goroutines.
//
// NOTE: refs is accessed atomically and therefore must be aligned on 64-bit platforms.
// It MUST be the first field in the struct according to the Go memory model to avoid false sharing.
// (See https://pkg.go.dev/sync/atomic#pkg-note-BUG)
//
// Callers interact with it exclusively through the FdCache APIs.

type FileEntry struct {
	refs int32 // accessed atomically – keep first
	f    *os.File
	mu   *sync.RWMutex // per-file lock shared with RawFileManager
}

// File returns the underlying os.File descriptor.
func (e *FileEntry) File() *os.File {
	return e.f
}

// Lock acquires an exclusive lock.
func (e *FileEntry) Lock() {
	e.mu.Lock()
}

// Unlock releases the exclusive lock.
func (e *FileEntry) Unlock() {
	e.mu.Unlock()
}

// RLock acquires a shared read lock.
func (e *FileEntry) RLock() {
	e.mu.RLock()
}

// RUnlock releases the read lock.
func (e *FileEntry) RUnlock() {
	e.mu.RUnlock()
}

// FdCache implements a capacity-bounded cache for open file descriptors. It allows multiple
// goroutines to share read-only *os.File handles while enforcing an upper bound on the total
// number of descriptors kept open concurrently.
//
// All public APIs (`Acquire`, `Release`, `Remove`) are safe for concurrent use.

type FdCache struct {
	capacity  int      // maximum number of cached entries (0 = unlimited)
	size      int32    // current size, accessed atomically
	entries   sync.Map // path -> *FileEntry
	fileLocks sync.Map // path -> *sync.RWMutex
}

// fdCache is a singleton instance of FdCache.
var fdCache *FdCache

// GetFdCache returns the singleton instance of FdCache.
func GetFdCache() *FdCache {
	return fdCache
}

// NewFdCache returns an initialised FdCache that will hold at most `capacity` file descriptors.
func NewFdCache(capacity int) *FdCache {
	if fdCache != nil {
		return fdCache
	}

	fdCache = &FdCache{
		capacity:  capacity,
		fileLocks: sync.Map{},
	}

	return fdCache
}

// Acquire returns a cached *fileEntry for the given path, opening the file in read-only mode if
// necessary. The lockProvider is used to obtain the RWMutex that guards access to a given file
// path. The entry's reference count is incremented; callers MUST invoke Release when they are done
// so the underlying descriptor can be closed (and the cache pruned) when no longer needed.
func (fc *FdCache) Acquire(path string) (*FileEntry, error) {
	// Fast-path: entry already in cache.
	if v, ok := fc.entries.Load(path); ok {
		e := v.(*FileEntry)
		atomic.AddInt32(&e.refs, 1)
		return e, nil
	}

	// Slow-path: open the file once.
	f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			zlog.Warn().Str("path", path).Msg("fdCache: file not found")
			return nil, utils.WrapError("raw file not found", path, nil)
		}
		return nil, utils.WrapError("failed to open raw file", path, err)
	}

	entry := &FileEntry{
		refs: 1,
		f:    f,
		mu:   fc.GetFileLock(path),
	}

	// If we are above capacity, skip caching this descriptor – it will be closed on Release.
	if fc.capacity > 0 && atomic.LoadInt32(&fc.size) >= int32(fc.capacity) {
		return entry, nil
	}

	// Attempt to insert into cache.
	actual, _ := fc.entries.LoadOrStore(path, entry)
	if actual != entry { // a concurrent goroutine won the race
		// We did not get our entry into the cache – close ours and use theirs.
		f.Close()
		entry = actual.(*FileEntry)
		atomic.AddInt32(&entry.refs, 1)
		return entry, nil
	}

	// Successfully cached new entry.
	atomic.AddInt32(&fc.size, 1)
	return entry, nil
}

// Release decrements the reference count for the given entry and, when it reaches zero, removes
// the entry from the cache and closes the underlying file.
func (fc *FdCache) Release(path string, e *FileEntry) {
	if atomic.AddInt32(&e.refs, -1) == 0 {
		_ = e.f.Close()
		if _, loaded := fc.entries.LoadAndDelete(path); loaded {
			atomic.AddInt32(&fc.size, -1)
		}
	}
}

// Remove forcibly evicts the entry for `path` from the cache (if present) and closes the file.
// This is primarily used when the file itself has been deleted from disk.
func (fc *FdCache) Remove(path string) {
	if v, ok := fc.entries.LoadAndDelete(path); ok {
		if e, ok2 := v.(*FileEntry); ok2 {
			_ = e.f.Close()
			atomic.AddInt32(&fc.size, -1)
		}
	}
}

// GetFileLock returns an RWMutex for the given path, creating it if it doesn't exist.
// An RWMutex allows multiple concurrent readers while still giving exclusive
// access to writers (Write/Delete), which is exactly the behaviour we need for
// raw files.
func (fc *FdCache) GetFileLock(path string) *sync.RWMutex {
	lock, _ := fc.fileLocks.LoadOrStore(path, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

// CleanUp removes the entry for `path` from the cache and closes the file.
// This is primarily used when the file itself has been deleted from disk.
func (fc *FdCache) CleanUp(path string) {
	fc.Remove(path)
	fc.fileLocks.Delete(path)
}
