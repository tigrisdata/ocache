package deletion

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
)

func setupTestQueue(t *testing.T) (*Queue, func()) {
	tmpDir := t.TempDir()
	meta, err := metadata.NewMetaDB(tmpDir, 0, nil, nil) // nil merge operator for tests
	require.NoError(t, err)

	config := Config{
		BatchSize:       10, // Small batch size for testing
		ProcessInterval: 100 * time.Millisecond,
		PruneAge:        1 * time.Hour,
	}

	queue := NewQueue(meta, config)

	cleanup := func() {
		queue.Stop()
		meta.Close()
	}

	return queue, cleanup
}

func TestQueue_AddAndProcess(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Create test files
	tmpDir := t.TempDir()
	testFiles := []string{
		filepath.Join(tmpDir, "file1.txt"),
		filepath.Join(tmpDir, "file2.txt"),
		filepath.Join(tmpDir, "file3.txt"),
	}

	for _, file := range testFiles {
		err := os.WriteFile(file, []byte("test"), 0o644)
		require.NoError(t, err)
	}

	// Add files to queue
	for _, file := range testFiles {
		err := queue.Add(file)
		require.NoError(t, err)
	}

	// Process batch
	queue.ProcessBatch()

	// Verify files are deleted
	for _, file := range testFiles {
		_, err := os.Stat(file)
		require.True(t, os.IsNotExist(err), "file should be deleted: %s", file)
	}
}

func TestQueue_Deduplication(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Create a test file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "duplicate.txt")
	err := os.WriteFile(testFile, []byte("test"), 0o644)
	require.NoError(t, err)

	// Add the same file multiple times
	for i := 0; i < 5; i++ {
		err := queue.Add(testFile)
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	// Process batch - should only delete once
	queue.ProcessBatch()

	// Verify file is deleted
	_, err = os.Stat(testFile)
	require.True(t, os.IsNotExist(err), "file should be deleted")

	// Check that processed count is 1, not 5
	require.Equal(t, int64(1), queue.processed)
}

func TestQueue_EmptyFilepath(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Try to add empty filepath
	err := queue.Add("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty filepath")
}

func TestQueue_NonExistentFile(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Add non-existent file
	err := queue.Add("/non/existent/file.txt")
	require.NoError(t, err)

	// Process batch - should handle gracefully
	queue.ProcessBatch()

	// Should count as processed (already deleted)
	require.Equal(t, int64(1), queue.processed)
}

func TestQueue_ConcurrentAdd(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	tmpDir := t.TempDir()
	numFiles := 100
	var wg sync.WaitGroup

	// Create files concurrently
	for i := 0; i < numFiles; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			file := filepath.Join(tmpDir, fmt.Sprintf("concurrent_%d.txt", idx))
			err := os.WriteFile(file, []byte("test"), 0o644)
			require.NoError(t, err)
			err = queue.Add(file)
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Process in batches
	for i := 0; i < (numFiles/queue.config.BatchSize)+1; i++ {
		queue.ProcessBatch()
	}

	// Verify all files are deleted
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Empty(t, entries, "all files should be deleted")
}

func TestQueue_BackgroundProcessing(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Start background processing
	queue.Start()

	// Create and add test files
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "background.txt")
	err := os.WriteFile(testFile, []byte("test"), 0o644)
	require.NoError(t, err)

	err = queue.Add(testFile)
	require.NoError(t, err)

	// Wait for background processing
	time.Sleep(200 * time.Millisecond)

	// Verify file is deleted
	_, err = os.Stat(testFile)
	require.True(t, os.IsNotExist(err), "file should be deleted by background processing")
}

func TestQueue_LockedFile(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Create a test file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "locked.txt")
	err := os.WriteFile(testFile, []byte("test"), 0o644)
	require.NoError(t, err)

	// Lock the file using the file lock manager
	lockManager := fd.GetFileLockManager()
	lock := lockManager.GetFileLock(testFile)
	lock.Lock()

	// Add to queue
	err = queue.Add(testFile)
	require.NoError(t, err)

	// Process batch - should fail to delete
	queue.ProcessBatch()

	// File should still exist (couldn't delete due to being locked)
	_, err = os.Stat(testFile)
	require.NoError(t, err, "file should still exist")

	// Should count as failed
	require.Equal(t, int64(1), queue.failed)

	// Unlock the file
	lock.Unlock()

	// Process again - should succeed now
	queue.ProcessBatch()

	// File should be deleted now
	_, err = os.Stat(testFile)
	require.True(t, os.IsNotExist(err), "file should be deleted after lock released")
}

func TestQueue_PruneOldEntries(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Set very short prune age for testing
	queue.config.PruneAge = 100 * time.Millisecond

	// Add a non-existent file (so it won't be deleted)
	err := queue.Add("/old/entry/that/wont/delete.txt")
	require.NoError(t, err)

	// Wait for entry to become old
	time.Sleep(150 * time.Millisecond)

	// Run pruning
	queue.pruneOldEntries()

	// Check that entry was pruned
	require.Equal(t, int64(1), queue.pruned)

	// Verify queue is empty
	depth := queue.GetQueueDepth()
	require.Equal(t, int64(0), depth)
}

// TestQueue_PruneOldEntries_KeepsExistingFile is the regression test for the
// secondary leak in issue #156: pruneOldEntries used to drop queue entries by
// age alone, abandoning a file that was still on disk (e.g. a deletion that kept
// failing because the file was read-locked). The queue entry is the only durable
// record that the file must be deleted, so dropping it orphans the file
// permanently. Prune must keep an aged entry whose file still exists; only the
// normal retry path (ProcessBatch) may reclaim it once deletion succeeds.
func TestQueue_PruneOldEntries_KeepsExistingFile(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Set very short prune age so the entry ages out quickly.
	queue.config.PruneAge = 100 * time.Millisecond

	// A real file on disk that the queue is responsible for reclaiming.
	tmpDir := t.TempDir()
	existing := filepath.Join(tmpDir, "still-here.bin")
	require.NoError(t, os.WriteFile(existing, []byte("data"), 0o644))

	require.NoError(t, queue.Add(existing))

	// Let the entry age past PruneAge.
	time.Sleep(150 * time.Millisecond)

	queue.pruneOldEntries()

	// The file still exists, so the entry MUST NOT be pruned — dropping it would
	// orphan the file permanently.
	require.Equal(t, int64(0), queue.pruned, "entry whose file still exists must not be pruned")
	require.Equal(t, int64(1), queue.GetQueueDepth(), "aged entry must be kept for retry while its file exists")
	require.FileExists(t, existing)

	// The normal retry path still reclaims the file (and removes the entry) once
	// the deletion can succeed.
	queue.ProcessBatch()
	require.NoFileExists(t, existing, "ProcessBatch should delete the file once it is no longer locked")
	require.Equal(t, int64(0), queue.GetQueueDepth(), "queue should drain after successful deletion")
}

// TestQueue_ProcessBatch_StuckEntriesRequeuedDoNotStarve verifies that
// undeletable entries (read-locked, so their deletion keeps failing) are
// re-enqueued to the tail rather than left blocking the head, so newer deletable
// entries are still reclaimed even when the stuck set EXCEEDS BatchSize. Without
// re-enqueue the oldest BatchSize stuck entries would fill every scan and the
// deletable file behind them would never be reached.
func TestQueue_ProcessBatch_StuckEntriesRequeuedDoNotStarve(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// BatchSize smaller than the stuck set: the stuck head alone would fill a
	// whole batch, so reaching the deletable file proves the head advances.
	queue.config.BatchSize = 2

	tmp := t.TempDir()

	// Three read-locked files at the head whose deletion can't succeed.
	stuck := []string{
		filepath.Join(tmp, "a-stuck"),
		filepath.Join(tmp, "b-stuck"),
		filepath.Join(tmp, "c-stuck"),
	}
	for _, f := range stuck {
		require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))
		require.NoError(t, queue.Add(f))
		lock := fd.GetFileLockManager().GetFileLock(f)
		lock.RLock()
		defer lock.RUnlock()
	}

	// A newer, deletable file enqueued behind the (oversized) stuck head.
	free := filepath.Join(tmp, "d-free")
	require.NoError(t, os.WriteFile(free, []byte("x"), 0o644))
	require.NoError(t, queue.Add(free))

	// A few cycles: failed entries rotate to the tail until the deletable file
	// reaches the head.
	for i := 0; i < 5; i++ {
		queue.ProcessBatch()
	}

	require.NoFileExists(t, free, "deletable file must be reclaimed even when the stuck set exceeds BatchSize")
	for _, f := range stuck {
		require.FileExists(t, f, "read-locked file must not be deleted")
	}
	// The stuck files are still tracked (re-enqueued), never dropped.
	require.Equal(t, int64(len(stuck)), queue.GetQueueDepth(), "stuck files remain queued for retry")
}

// TestQueue_ProcessBatch_RequeuedStuckFileReclaimedAfterUnlock confirms a stuck,
// re-enqueued file is reclaimed once it becomes deletable (its reader releases
// the lock), i.e. re-enqueue defers deletion rather than dropping it.
func TestQueue_ProcessBatch_RequeuedStuckFileReclaimedAfterUnlock(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	tmp := t.TempDir()
	f := filepath.Join(tmp, "temporarily-locked.bin")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))
	require.NoError(t, queue.Add(f))

	lock := fd.GetFileLockManager().GetFileLock(f)
	lock.RLock()

	// Deletion fails while locked; the entry is re-enqueued, not dropped.
	queue.ProcessBatch()
	require.FileExists(t, f, "locked file must not be deleted")
	require.Equal(t, int64(1), queue.GetQueueDepth(), "entry must stay queued for retry")

	// Once unlocked, the next cycle reclaims it.
	lock.RUnlock()
	queue.ProcessBatch()
	require.NoFileExists(t, f, "file must be reclaimed after the lock is released")
	require.Equal(t, int64(0), queue.GetQueueDepth(), "queue should drain after successful deletion")
}

func TestQueue_GetQueueDepth(t *testing.T) {
	queue, cleanup := setupTestQueue(t)
	defer cleanup()

	// Initially empty
	depth := queue.GetQueueDepth()
	require.Equal(t, int64(0), depth)

	// Add some entries
	for i := 0; i < 5; i++ {
		err := queue.Add(fmt.Sprintf("/test/file%d.txt", i))
		require.NoError(t, err)
	}

	// Check depth
	depth = queue.GetQueueDepth()
	require.Equal(t, int64(5), depth)

	// Process batch
	queue.ProcessBatch()

	// Should be empty after processing
	depth = queue.GetQueueDepth()
	require.Equal(t, int64(0), depth)
}

func TestQueue_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	meta, err := metadata.NewMetaDB(tmpDir, 0, nil, nil) // nil merge operator for tests
	require.NoError(t, err)
	defer meta.Close()

	config := Config{
		BatchSize:       10,
		ProcessInterval: 50 * time.Millisecond,
		PruneAge:        1 * time.Hour,
	}

	queue := NewQueue(meta, config)
	queue.Start()

	// Add some files
	for i := 0; i < 3; i++ {
		err := queue.Add(fmt.Sprintf("/test/ctx_%d.txt", i))
		require.NoError(t, err)
	}

	// Stop the queue (cancels context)
	queue.Stop()

	// Verify the background loop stopped
	// The WaitGroup should have completed
	done := make(chan struct{})
	go func() {
		queue.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Good, background loop stopped
	case <-time.After(1 * time.Second):
		t.Fatal("background loop did not stop after context cancellation")
	}
}

func TestQueue_KeyFunctions(t *testing.T) {
	// Test MakeDeletionQueueKey
	key := keys.MakeDeletionQueueKey(1234567890123456789, "/path/to/file.txt")
	expected := []byte("!del/01234567890123456789//path/to/file.txt")
	require.Equal(t, expected, key)

	// Test ParseDeletionQueueKey
	timestamp, filepath, err := keys.ParseDeletionQueueKey(key)
	require.NoError(t, err)
	require.Equal(t, int64(1234567890123456789), timestamp)
	require.Equal(t, "/path/to/file.txt", filepath)

	// Test with malformed key
	badKey := []byte("malformed")
	_, _, err = keys.ParseDeletionQueueKey(badKey)
	require.Error(t, err)

	// Test IsDeletionQueueKey
	require.True(t, keys.IsDeletionQueueKey(key))
	require.False(t, keys.IsDeletionQueueKey(badKey))
}
