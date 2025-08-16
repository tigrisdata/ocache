package fd

import (
	"os"
	"sync"
	"sync/atomic"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/storage/utils"
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
	refs   int32 // accessed atomically – keep first
	f      *os.File
	mu     *sync.RWMutex // per-file lock shared with RawFileManager
	ready  chan struct{} // closed when file is opened (for synchronization)
	cached bool          // whether this entry is in the cache (for capacity management)
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
	capacity int      // maximum number of cached entries (0 = unlimited)
	size     int32    // current size, accessed atomically
	entries  sync.Map // path -> *FileEntry
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
		capacity: capacity,
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
		// Increment refs BEFORE waiting to prevent removal during acquire
		atomic.AddInt32(&e.refs, 1)

		// Wait for file to be ready if another goroutine is opening it
		<-e.ready

		// Check if opening failed
		if e.f == nil {
			// Decrement refs since we're not using it
			atomic.AddInt32(&e.refs, -1)
			return nil, utils.WrapError("failed to open raw file", path, nil)
		}
		return e, nil
	}

	// Slow-path: need to open the file.
	// Check if we're at capacity before creating a new entry
	atCapacity := fc.capacity > 0 && atomic.LoadInt32(&fc.size) >= int32(fc.capacity)

	// Create entry with ready channel
	ready := make(chan struct{})
	entry := &FileEntry{
		refs:   1,
		f:      nil,
		mu:     GetFileLockManager().GetFileLock(path),
		ready:  ready,
		cached: !atCapacity, // Won't be cached if at capacity
	}

	// Try to store our entry atomically
	actual, loaded := fc.entries.LoadOrStore(path, entry)
	if loaded {
		// Another goroutine already started opening, wait for it
		existing := actual.(*FileEntry)
		// Increment refs BEFORE waiting to prevent removal during acquire
		atomic.AddInt32(&existing.refs, 1)

		<-existing.ready
		// Check if opening failed
		if existing.f == nil {
			// Decrement refs since we're not using it
			atomic.AddInt32(&existing.refs, -1)
			return nil, utils.WrapError("failed to open raw file", path, nil)
		}
		return existing, nil
	}

	// We won the race to insert the entry, now open the file
	f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		// Mark as failed (f remains nil) and signal waiters
		close(ready)
		// Remove our failed entry
		fc.entries.Delete(path)

		if os.IsNotExist(err) {
			zlog.Warn().Str("path", path).Msg("fdCache: file not found")
			return nil, utils.WrapError("raw file not found", path, err)
		}
		return nil, utils.WrapError("failed to open raw file", path, err)
	}

	// Set the file descriptor and signal success
	entry.f = f
	close(ready)

	// Track size only if we're actually caching this entry
	if entry.cached {
		atomic.AddInt32(&fc.size, 1)
	} else {
		// Not cached due to capacity limit, remove from entries map
		// but return the entry for this caller to use
		fc.entries.Delete(path)
	}

	return entry, nil
}

// Release decrements the reference count for the given entry and, when it reaches zero, removes
// the entry from the cache and closes the underlying file.
func (fc *FdCache) Release(path string, e *FileEntry) {
	newRefs := atomic.AddInt32(&e.refs, -1)
	if newRefs == 0 {
		// If this entry was cached, remove it
		if e.cached {
			// Try to remove from cache. We use CompareAndDelete to ensure we only
			// remove our specific entry and not one that another goroutine may have added
			if fc.entries.CompareAndDelete(path, e) {
				// Successfully removed our entry, clean up
				<-e.ready
				if e.f != nil {
					_ = e.f.Close()
				}
				atomic.AddInt32(&fc.size, -1)
			} else {
				// Either already removed or replaced by another goroutine
				// Just close the file if it's ready
				<-e.ready
				if e.f != nil {
					_ = e.f.Close()
				}
			}
		} else {
			// Not cached, just close the file
			<-e.ready
			if e.f != nil {
				_ = e.f.Close()
			}
		}
	} else if newRefs < 0 {
		// This shouldn't happen in normal operation, but let's handle it gracefully
		// Reset to 0 to prevent further decrements
		atomic.StoreInt32(&e.refs, 0)
	}
}

// Remove forcibly evicts the entry for `path` from the cache (if present) and closes the file.
// This is primarily used when the file itself has been deleted from disk.
func (fc *FdCache) Remove(path string) {
	if v, ok := fc.entries.LoadAndDelete(path); ok {
		if e, ok2 := v.(*FileEntry); ok2 {
			// Wait for file to be ready before closing (in case it's still opening)
			<-e.ready
			if e.f != nil {
				_ = e.f.Close()
			}
			atomic.AddInt32(&fc.size, -1)
		}
	}
}

// GetFileLock returns an RWMutex for the given path, creating it if it doesn't exist.
// An RWMutex allows multiple concurrent readers while still giving exclusive
// access to writers (Write/Delete), which is exactly the behaviour we need for
// raw files.
// Deprecated: Use GetFileLockManager().GetFileLock(path) instead.
func (fc *FdCache) GetFileLock(path string) *sync.RWMutex {
	return GetFileLockManager().GetFileLock(path)
}

// CleanUp removes the entry for `path` from the cache and closes the file.
// This is primarily used when the file itself has been deleted from disk.
func (fc *FdCache) CleanUp(path string) {
	fc.Remove(path)
	GetFileLockManager().RemoveFileLock(path)
}
