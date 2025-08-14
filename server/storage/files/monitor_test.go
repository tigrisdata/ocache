package files

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/deletion"
	"github.com/tigrisdata/ocache/server/storage/keys"
	"google.golang.org/protobuf/proto"
)

func TestMonitorRemovesAgedEntries(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a file
	testFile := filepath.Join(filesDir, "aged.dat")
	data := []byte("test data")
	err := os.WriteFile(testFile, data, 0o644)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Add metadata
	metaKey := keys.MakeMetadataKey("aged-key")
	vm := &pb.ValueMessage{
		ValueLength: int64(len(data)),
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: testFile,
	}
	vmBytes, _ := proto.Marshal(vm)
	batch.Put(metaKey, vmBytes)

	// Add sync entry with old timestamp (>60s ago)
	oldTimestamp := time.Now().Add(-65 * time.Second).UnixNano()
	syncKey := []byte(fmt.Sprintf("%s%020d/%s", keys.SyncIndexPrefix, oldTimestamp, testFile))
	syncEntry := &pb.SyncEntry{
		MetadataKey: string(metaKey),
		Timestamp:   time.Now().Add(-65 * time.Second).Unix(),
	}
	syncVal, _ := EncodeSyncEntry(syncEntry)
	batch.Put(syncKey, syncVal)

	err = meta.Handle().Write(wo, batch)
	require.NoError(t, err)

	// Create deletion queue
	deletionQueue := deletion.NewQueue(meta, deletion.DefaultConfig())
	deletionQueue.Start()
	defer deletionQueue.Stop()

	// Create and run monitor once
	monitor := NewSyncMonitor(meta, deletionQueue, time.Hour) // Long interval so it doesn't repeat
	monitor.checkAndCleanup()

	// Verify aged sync entry was removed
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	syncSlice, _ := meta.Handle().Get(ro, syncKey)
	assert.False(t, syncSlice.Exists(), "Aged sync entry should be removed")
	syncSlice.Free()

	// Verify metadata still exists
	metaSlice, _ := meta.Handle().Get(ro, metaKey)
	assert.True(t, metaSlice.Exists(), "Metadata should still exist")
	metaSlice.Free()

	// Verify file still exists
	_, err = os.Stat(testFile)
	assert.NoError(t, err, "File should still exist")
}

func TestMonitorRemovesCorruptedFiles(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a file with initial data
	testFile := filepath.Join(filesDir, "corrupted.dat")
	initialData := []byte("initial data content")
	err := os.WriteFile(testFile, initialData, 0o644)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Add metadata with expected size (initial size)
	metaKey := keys.MakeMetadataKey("corrupted-key")
	vm := &pb.ValueMessage{
		ValueLength: int64(len(initialData)),
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: testFile,
	}
	vmBytes, _ := proto.Marshal(vm)
	batch.Put(metaKey, vmBytes)

	// Add sync entry (recent, so it won't be removed due to age)
	timestamp := time.Now().UnixNano()
	syncKey := []byte(fmt.Sprintf("%s%020d/%s", keys.SyncIndexPrefix, timestamp, testFile))
	syncEntry := &pb.SyncEntry{
		MetadataKey: string(metaKey),
		Timestamp:   time.Now().Unix(),
	}
	syncVal, _ := EncodeSyncEntry(syncEntry)
	batch.Put(syncKey, syncVal)

	err = meta.Handle().Write(wo, batch)
	require.NoError(t, err)

	// Now corrupt the file by changing its size
	corruptedData := []byte("corrupted")
	err = os.WriteFile(testFile, corruptedData, 0o644)
	require.NoError(t, err)

	// Create deletion queue
	deletionQueue := deletion.NewQueue(meta, deletion.DefaultConfig())
	deletionQueue.Start()
	defer deletionQueue.Stop()

	// Create and run monitor once
	monitor := NewSyncMonitor(meta, deletionQueue, time.Hour) // Long interval so it doesn't repeat
	monitor.checkAndCleanup()

	// Verify sync entry was removed
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	syncSlice, _ := meta.Handle().Get(ro, syncKey)
	assert.False(t, syncSlice.Exists(), "Sync entry for corrupted file should be removed")
	syncSlice.Free()

	// Verify metadata still exists (monitor doesn't remove metadata)
	metaSlice, _ := meta.Handle().Get(ro, metaKey)
	assert.True(t, metaSlice.Exists(), "Metadata should still exist")
	metaSlice.Free()

	// File should still exist (queued but not yet processed)
	_, err = os.Stat(testFile)
	assert.NoError(t, err, "Corrupted file should still exist (queued for deletion)")

	// Process the deletion queue
	deletionQueue.ProcessBatch()

	// Now verify corrupted file was deleted
	_, err = os.Stat(testFile)
	assert.True(t, os.IsNotExist(err), "Corrupted file should be deleted after queue processing")
}

func TestMonitorRemovesStaleEntries(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create two files
	oldFile := filepath.Join(filesDir, "old.dat")
	newFile := filepath.Join(filesDir, "new.dat")

	err := os.WriteFile(oldFile, []byte("old data"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(newFile, []byte("new data"), 0o644)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Add metadata pointing to NEW file
	metaKey := keys.MakeMetadataKey("test-key")
	vm := &pb.ValueMessage{
		ValueLength: 8,
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: newFile,
	}
	vmBytes, _ := proto.Marshal(vm)
	batch.Put(metaKey, vmBytes)

	// Add stale sync entry for OLD file (recent timestamp)
	syncKey := keys.MakeSyncKey(oldFile)
	syncEntry := &pb.SyncEntry{
		MetadataKey: string(metaKey),
		Timestamp:   time.Now().Unix(),
	}
	syncVal, _ := EncodeSyncEntry(syncEntry)
	batch.Put(syncKey, syncVal)

	err = meta.Handle().Write(wo, batch)
	require.NoError(t, err)

	// Verify old file exists before cleanup
	_, err = os.Stat(oldFile)
	require.NoError(t, err, "Old file should exist before cleanup")

	// Create deletion queue
	deletionQueue := deletion.NewQueue(meta, deletion.DefaultConfig())
	deletionQueue.Start()
	defer deletionQueue.Stop()

	// Create and run monitor once
	monitor := NewSyncMonitor(meta, deletionQueue, time.Hour)
	monitor.checkAndCleanup()

	// Verify stale sync entry was removed
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	syncSlice, _ := meta.Handle().Get(ro, syncKey)
	assert.False(t, syncSlice.Exists(), "Stale sync entry should be removed")
	syncSlice.Free()

	// File should still exist (queued but not yet processed)
	_, err = os.Stat(oldFile)
	assert.NoError(t, err, "Orphaned file should still exist (queued for deletion)")

	// Process the deletion queue
	deletionQueue.ProcessBatch()

	// Now verify orphaned file was deleted
	_, err = os.Stat(oldFile)
	assert.True(t, os.IsNotExist(err), "Orphaned file should be deleted after queue processing")

	// Verify new file still exists
	_, err = os.Stat(newFile)
	assert.NoError(t, err, "New file should still exist")

	// Verify metadata still exists
	metaSlice, _ := meta.Handle().Get(ro, metaKey)
	assert.True(t, metaSlice.Exists(), "Metadata should still exist")
	metaSlice.Free()
}

func TestMonitorDeletesFileWhenMetadataDeleted(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a file
	testFile := filepath.Join(filesDir, "deleted.dat")
	err := os.WriteFile(testFile, []byte("to be deleted"), 0o644)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	// Add sync entry WITHOUT metadata (simulating deleted metadata)
	syncKey := keys.MakeSyncKey(testFile)
	syncEntry := &pb.SyncEntry{
		MetadataKey: string(keys.MakeMetadataKey("deleted-key")),
		Timestamp:   time.Now().Unix(),
	}
	syncVal, _ := EncodeSyncEntry(syncEntry)
	err = meta.Handle().Put(wo, syncKey, syncVal)
	require.NoError(t, err)

	// Verify file exists before cleanup
	_, err = os.Stat(testFile)
	require.NoError(t, err, "File should exist before cleanup")

	// Create deletion queue
	deletionQueue := deletion.NewQueue(meta, deletion.DefaultConfig())
	deletionQueue.Start()
	defer deletionQueue.Stop()

	// Create and run monitor once
	monitor := NewSyncMonitor(meta, deletionQueue, time.Hour)
	monitor.checkAndCleanup()

	// Verify sync entry was removed
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	syncSlice, _ := meta.Handle().Get(ro, syncKey)
	assert.False(t, syncSlice.Exists(), "Sync entry should be removed")
	syncSlice.Free()

	// File should still exist (queued but not yet processed)
	_, err = os.Stat(testFile)
	assert.NoError(t, err, "Orphaned file should still exist (queued for deletion)")

	// Process the deletion queue
	deletionQueue.ProcessBatch()

	// Now verify orphaned file was deleted
	_, err = os.Stat(testFile)
	assert.True(t, os.IsNotExist(err), "Orphaned file should be deleted after queue processing")
}

func TestMonitorKeepsPendingEntries(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a file
	testFile := filepath.Join(filesDir, "pending.dat")
	data := []byte("test data")
	err := os.WriteFile(testFile, data, 0o644)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Add metadata
	metaKey := keys.MakeMetadataKey("pending-key")
	vm := &pb.ValueMessage{
		ValueLength: int64(len(data)),
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: testFile,
	}
	vmBytes, _ := proto.Marshal(vm)
	batch.Put(metaKey, vmBytes)

	// Add RECENT sync entry (<30s old)
	syncKey := keys.MakeSyncKey(testFile)
	syncEntry := &pb.SyncEntry{
		MetadataKey: string(metaKey),
		Timestamp:   time.Now().Unix(),
	}
	syncVal, _ := EncodeSyncEntry(syncEntry)
	batch.Put(syncKey, syncVal)

	err = meta.Handle().Write(wo, batch)
	require.NoError(t, err)

	// Create deletion queue
	deletionQueue := deletion.NewQueue(meta, deletion.DefaultConfig())
	deletionQueue.Start()
	defer deletionQueue.Stop()

	// Create and run monitor once
	monitor := NewSyncMonitor(meta, deletionQueue, time.Hour)
	monitor.checkAndCleanup()

	// Verify pending sync entry still exists
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	syncSlice, _ := meta.Handle().Get(ro, syncKey)
	assert.True(t, syncSlice.Exists(), "Pending sync entry should still exist")
	syncSlice.Free()
}

func TestMonitorHandlesCompactedFiles(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a file
	testFile := filepath.Join(filesDir, "compacted.dat")
	err := os.WriteFile(testFile, []byte("data"), 0o644)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Add metadata for SEGMENT (file was compacted)
	metaKey := keys.MakeMetadataKey("compacted-key")
	vm := &pb.ValueMessage{
		ValueLength:   4,
		ValueType:     pb.ValueType_SEGMENT, // Not RAW_FILE anymore
		SegmentPath:   "/path/to/segment",
		SegmentOffset: 100,
	}
	vmBytes, _ := proto.Marshal(vm)
	batch.Put(metaKey, vmBytes)

	// Add sync entry for the old raw file
	syncKey := keys.MakeSyncKey(testFile)
	syncEntry := &pb.SyncEntry{
		MetadataKey: string(metaKey),
		Timestamp:   time.Now().Unix(),
	}
	syncVal, _ := EncodeSyncEntry(syncEntry)
	batch.Put(syncKey, syncVal)

	err = meta.Handle().Write(wo, batch)
	require.NoError(t, err)

	// Create deletion queue
	deletionQueue := deletion.NewQueue(meta, deletion.DefaultConfig())
	deletionQueue.Start()
	defer deletionQueue.Stop()

	// Create and run monitor once
	monitor := NewSyncMonitor(meta, deletionQueue, time.Hour)
	monitor.checkAndCleanup()

	// Verify stale sync entry was removed (file was compacted)
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	syncSlice, _ := meta.Handle().Get(ro, syncKey)
	assert.False(t, syncSlice.Exists(), "Sync entry for compacted file should be removed")
	syncSlice.Free()
}

func TestMonitorConcurrentOperation(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create multiple sync entries
	numEntries := 50
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	for i := 0; i < numEntries; i++ {
		batch := grocksdb.NewWriteBatch()
		defer batch.Destroy()

		fileName := fmt.Sprintf("file%d.dat", i)
		filePath := filepath.Join(filesDir, fileName)
		data := []byte(fmt.Sprintf("data %d", i))
		err := os.WriteFile(filePath, data, 0o644)
		require.NoError(t, err)

		metaKey := keys.MakeMetadataKey(fmt.Sprintf("key%d", i))
		vm := &pb.ValueMessage{
			ValueLength: int64(len(data)),
			ValueType:   pb.ValueType_RAW_FILE,
			RawFilePath: filePath,
		}
		vmBytes, _ := proto.Marshal(vm)
		batch.Put(metaKey, vmBytes)

		// Mix of old and new entries
		var timestamp int64
		if i%2 == 0 {
			timestamp = time.Now().Add(-70 * time.Second).UnixNano() // Old
		} else {
			timestamp = time.Now().UnixNano() // Recent
		}

		syncKey := []byte(fmt.Sprintf("%s%020d/%s", keys.SyncIndexPrefix, timestamp, filePath))
		syncEntry := &pb.SyncEntry{
			MetadataKey: string(metaKey),
			Timestamp:   timestamp / 1e9,
		}
		syncVal, _ := EncodeSyncEntry(syncEntry)
		batch.Put(syncKey, syncVal)

		err = meta.Handle().Write(wo, batch)
		require.NoError(t, err)
	}

	// Run monitor
	monitor := NewSyncMonitor(meta, deletion.NewQueue(meta, deletion.DefaultConfig()), time.Hour)
	monitor.checkAndCleanup()

	// Count remaining sync entries
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := meta.Handle().NewIterator(ro)
	defer it.Close()

	remainingCount := 0
	prefix := []byte(keys.SyncIndexPrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		remainingCount++
	}

	// Should have only the recent entries (odd indices)
	assert.Equal(t, numEntries/2, remainingCount, "Only recent sync entries should remain")
}
