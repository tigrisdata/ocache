// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package fd

import (
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/utils"
)

// refsClosed is the sentinel value for refs once a caller has claimed the
// close of a FileEntry. Any further Acquire attempts must observe this state
// and fall through to opening a fresh entry.
const refsClosed int32 = -1

// FileEntry wraps a cached os.File descriptor along with a per-file RWMutex and reference count.
// The lock protects concurrent readers/writers on the underlying file while refs ensures that the
// file descriptor stays open while in-use across goroutines.
//
// NOTE: refs is accessed atomically and therefore must be aligned on 64-bit platforms.
// It MUST be the first field in the struct according to the Go memory model to avoid false sharing.
// (See https://pkg.go.dev/sync/atomic#pkg-note-BUG)
//
// Callers interact with it exclusively through the FdCache APIs.
//
// State machine for refs:
//
//	> 0 : active holders (normal "live" state)
//	  0 : defensive/transient only (e.g. immediately after construction, or
//	      after all waiters have dropped their refs in the open-failure path).
//	      Steady-state code never leaves an in-map entry at refs == 0, because
//	      Release transitions refs 1 → -1 atomically to claim the close.
//	 -1 : "closed/closing" sentinel. No new Acquire may bump refs from here;
//	      it must fall through to opening a fresh entry.

type FileEntry struct {
	refs    int32 // accessed atomically – keep first
	f       *os.File
	openErr error         // set by the opener before closing ready; read by waiters when f == nil
	mu      *sync.RWMutex // per-file lock shared with RawFileManager
	ready   chan struct{} // closed when file is opened (for synchronization)
	cached  bool          // whether this entry is in the cache (for capacity management)
}

// openFailureErr returns the error a waiter should surface when it observes a
// failed entry (f == nil). It prefers the opener's recorded openErr (which
// carries the real cause, e.g. os.ErrNotExist, so callers can self-heal) and
// only falls back to a generic non-nil error if, defensively, none was set —
// never nil, since a nil error here is what caused the issue #150 panic.
func openFailureErr(path string, openErr error) error {
	if openErr != nil {
		return openErr
	}
	return utils.WrapError("failed to open raw file", path, errUnknownOpenFailure)
}

// errUnknownOpenFailure backstops openFailureErr so a failed entry can never
// produce a nil error.
var errUnknownOpenFailure = errors.New("unknown open failure")

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

// tryAcquireRef atomically increments refs if and only if refs > 0.
// Returns true on success. Returns false if the entry has been claimed for
// closing (refs <= 0), in which case the caller must fall through and create
// a fresh entry instead of using this one.
func (e *FileEntry) tryAcquireRef() bool {
	for {
		cur := atomic.LoadInt32(&e.refs)
		if cur <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(&e.refs, cur, cur+1) {
			return true
		}
	}
}

// dropRef atomically decrements refs. Returns true if this caller transitioned
// the count from 1 to the closing sentinel (-1), i.e. this caller now owns
// the close and must perform the final cleanup via closeEntry. Returns false
// if other holders remain or the entry has already been claimed.
func (e *FileEntry) dropRef() (ownsClose bool) {
	for {
		cur := atomic.LoadInt32(&e.refs)
		if cur <= 0 {
			// Already closed/closing or never held. Defensive no-op.
			return false
		}
		if cur > 1 {
			if atomic.CompareAndSwapInt32(&e.refs, cur, cur-1) {
				return false
			}
			continue
		}
		// cur == 1: try to claim the close atomically.
		if atomic.CompareAndSwapInt32(&e.refs, 1, refsClosed) {
			return true
		}
		// CAS failed: a concurrent tryAcquireRef revived the count. Retry.
	}
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

	// Initialize cache size metric
	metrics.FDCacheSize.Set(0)

	return fdCache
}

// Acquire returns a cached *fileEntry for the given path, opening the file in read-only mode if
// necessary. The entry's reference count is incremented; callers MUST invoke Release when they are
// done so the underlying descriptor can be closed (and the cache pruned) when no longer needed.
//
// Acquire is race-free against concurrent Release and Remove: if the cached entry has been
// claimed for closing (refs <= 0), Acquire transparently falls through and opens a fresh
// descriptor rather than returning a stale FileEntry whose file has been closed.
func (fc *FdCache) Acquire(path string) (*FileEntry, error) {
	for {
		// Fast-path: entry already in cache.
		if v, ok := fc.entries.Load(path); ok {
			e := v.(*FileEntry)
			if !e.tryAcquireRef() {
				// Entry is being closed by a concurrent Release/Remove. The
				// map entry will be removed shortly; yield briefly and retry
				// the top-level Acquire so we either find a fresh entry or
				// fall through to the slow path.
				runtime.Gosched()
				continue
			}

			// Wait for file to be ready if another goroutine is opening it.
			<-e.ready

			// Check if opening failed.
			if e.f == nil {
				if e.dropRef() {
					fc.closeEntry(path, e)
				}
				return nil, openFailureErr(path, e.openErr)
			}

			metrics.FDCacheHits.Inc()
			return e, nil
		}

		// Slow-path: need to open the file.
		metrics.FDCacheMisses.Inc()

		// Check if we're at capacity before creating a new entry.
		atCapacity := fc.capacity > 0 && atomic.LoadInt32(&fc.size) >= int32(fc.capacity)

		// Create entry with ready channel.
		ready := make(chan struct{})
		entry := &FileEntry{
			refs:   1,
			f:      nil,
			mu:     GetFileLockManager().GetFileLock(path),
			ready:  ready,
			cached: !atCapacity,
		}

		// Try to store our entry atomically.
		actual, loaded := fc.entries.LoadOrStore(path, entry)
		if loaded {
			// Another goroutine already started opening; wait for it.
			existing := actual.(*FileEntry)
			if !existing.tryAcquireRef() {
				// The existing entry is being closed. Retry the outer loop;
				// it will be evicted from the map shortly and we can either
				// find a replacement or win the LoadOrStore ourselves.
				runtime.Gosched()
				continue
			}

			<-existing.ready
			if existing.f == nil {
				if existing.dropRef() {
					fc.closeEntry(path, existing)
				}
				return nil, openFailureErr(path, existing.openErr)
			}

			metrics.FDCacheHits.Inc()
			return existing, nil
		}

		// We won the race to insert the entry; now open the file.
		f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
		if err != nil {
			// Record the failure on the entry BEFORE signalling waiters so they
			// observe the real cause (notably os.ErrNotExist) rather than a nil
			// error. Returning (nil, nil) to a waiter here is what produced the
			// nil-pointer panic in issue #150.
			if os.IsNotExist(err) {
				zlog.Warn().Str("path", path).Msg("fdCache: file not found")
				entry.openErr = utils.WrapError("raw file not found", path, err)
			} else {
				entry.openErr = utils.WrapError("failed to open raw file", path, err)
			}
			// Mark as failed (f remains nil) and signal waiters.
			close(ready)
			// Remove our failed entry so new callers don't reuse it.
			fc.entries.Delete(path)
			// Drop our construction ref. If a waiter joined before we Deleted,
			// the last dropper will run closeEntry (which is a no-op on a nil
			// file beyond the map cleanup that already happened).
			if entry.dropRef() {
				fc.closeEntry(path, entry)
			}

			return nil, entry.openErr
		}

		// Set the file descriptor and signal success.
		entry.f = f
		close(ready)

		// Track size only if we're actually caching this entry.
		if entry.cached {
			newSize := atomic.AddInt32(&fc.size, 1)
			metrics.FDCacheSize.Set(float64(newSize))
		} else {
			// At capacity: remove from entries map but return the entry for
			// this caller to use (its lifetime is bounded by Release).
			fc.entries.Delete(path)
			metrics.FDCacheNotCached.Inc()
		}

		return entry, nil
	}
}

// Release decrements the reference count for the given entry. When this is the
// last holder, Release atomically claims the close via dropRef and invokes
// closeEntry to finalize cleanup.
func (fc *FdCache) Release(path string, e *FileEntry) {
	if e.dropRef() {
		fc.closeEntry(path, e)
	}
}

// closeEntry finalizes cleanup after a caller has atomically claimed the close
// via dropRef (refs transitioned 1 → refsClosed) or Remove's CAS from 0 →
// refsClosed. It evicts the entry from the map (if still present and cached)
// and closes the underlying file descriptor.
//
// closeEntry may safely observe that the entry has already been removed from
// the map (CompareAndDelete returns false) when Remove raced to evict it; in
// that case Remove has already performed the size/eviction accounting and we
// need only close the FD.
func (fc *FdCache) closeEntry(path string, e *FileEntry) {
	<-e.ready
	if e.cached {
		if fc.entries.CompareAndDelete(path, e) {
			newSize := atomic.AddInt32(&fc.size, -1)
			metrics.FDCacheSize.Set(float64(newSize))
		}
	}
	if e.f != nil {
		_ = e.f.Close()
	}
}

// Remove forcibly evicts the entry for `path` from the cache (if present).
// This is primarily used when the file itself has been deleted from disk or
// when the segment containing it has been compacted away.
//
// Remove never closes a file descriptor that an active holder still owns.
// The close is always performed by the last Release (which transitions refs
// 1 → -1 and runs closeEntry). If Remove happens to observe that no holder
// exists (refs == 0, e.g. a defensive/manual state), it atomically claims
// the close via CAS(0, refsClosed) and closes the FD itself.
func (fc *FdCache) Remove(path string) {
	if v, ok := fc.entries.LoadAndDelete(path); ok {
		if e, ok2 := v.(*FileEntry); ok2 {
			// Wait for file to be ready (in case it's still opening).
			<-e.ready
			newSize := atomic.AddInt32(&fc.size, -1)
			metrics.FDCacheEvictions.Inc()
			metrics.FDCacheSize.Set(float64(newSize))

			// Claim the close only if nobody else is actively using the
			// entry. If refs > 0, the last Release will close via dropRef +
			// closeEntry (its CompareAndDelete will see the map is empty
			// and skip size accounting — which we've already done here).
			// If refs is already refsClosed, a Release already owns the
			// close and is about to run closeEntry.
			if atomic.CompareAndSwapInt32(&e.refs, 0, refsClosed) {
				if e.f != nil {
					_ = e.f.Close()
				}
			}
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
