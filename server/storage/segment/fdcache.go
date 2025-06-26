package segment

import (
	"os"
	"sync"
	"sync/atomic"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/cache_service/server/utils"
)

// fileEntry wraps a cached os.File descriptor along with a per-file RWMutex and reference count.
// The lock protects concurrent readers/writers on the underlying file while refs ensures that the
// file descriptor stays open while in-use across goroutines.
//
// NOTE: refs is accessed atomically and therefore must be aligned on 64-bit platforms.
// It MUST be the first field in the struct according to the Go memory model to avoid false sharing.
// (See https://pkg.go.dev/sync/atomic#pkg-note-BUG)
//
// Callers interact with it exclusively through the FdCache APIs.

type fileEntry struct {
	refs int32 // accessed atomically – keep first
	f    *os.File
	mu   *sync.RWMutex // per-file lock shared with RawFileManager
}

// FdCache implements a capacity-bounded cache for open file descriptors. It allows multiple
// goroutines to share read-only *os.File handles while enforcing an upper bound on the total
// number of descriptors kept open concurrently.
//
// All public APIs (`Acquire`, `Release`, `Remove`) are safe for concurrent use.

type FdCache struct {
	capacity     int                        // maximum number of cached entries (0 = unlimited)
	size         int32                      // current size, accessed atomically
	entries      sync.Map                   // path -> *fileEntry
	lockProvider func(string) *sync.RWMutex // returns (and memoises) a per-file lock
}

// NewFdCache returns an initialised FdCache that will hold at most `capacity` file descriptors.
// The `lockProvider` callback is used to obtain the RWMutex that guards access to a given file
// path; typically this will be RawFileManager.GetFileLock.
func NewFdCache(capacity int, lockProvider func(string) *sync.RWMutex) *FdCache {
	return &FdCache{
		capacity:     capacity,
		lockProvider: lockProvider,
	}
}

// Acquire returns a cached *fileEntry for the given path, opening the file in read-only mode if
// necessary. The entry's reference count is incremented; callers MUST invoke Release when they are
// done so the underlying descriptor can be closed (and the cache pruned) when no longer needed.
func (fc *FdCache) Acquire(path string) (*fileEntry, error) {
	// Fast-path: entry already in cache.
	if v, ok := fc.entries.Load(path); ok {
		e := v.(*fileEntry)
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

	entry := &fileEntry{
		refs: 1,
		f:    f,
		mu:   fc.lockProvider(path),
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
		entry = actual.(*fileEntry)
		atomic.AddInt32(&entry.refs, 1)
		return entry, nil
	}

	// Successfully cached new entry.
	atomic.AddInt32(&fc.size, 1)
	return entry, nil
}

// Release decrements the reference count for the given entry and, when it reaches zero, removes
// the entry from the cache and closes the underlying file.
func (fc *FdCache) Release(path string, e *fileEntry) {
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
		if e, ok2 := v.(*fileEntry); ok2 {
			_ = e.f.Close()
			atomic.AddInt32(&fc.size, -1)
		}
	}
}
