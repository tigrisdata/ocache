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
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"google.golang.org/protobuf/proto"
)

func setupTestEnvironment(t *testing.T) (string, *metadata.MetaDB, func()) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "recovery_test")
	require.NoError(t, err)

	// Create files subdirectory
	filesDir := filepath.Join(tmpDir, "files")
	err = os.MkdirAll(filesDir, 0o755)
	require.NoError(t, err)

	// Initialize metadata DB with nil merge operator
	meta, err := metadata.NewMetaDB(tmpDir, 0, nil, nil)
	require.NoError(t, err)

	cleanup := func() {
		meta.Close()
		os.RemoveAll(tmpDir)
	}

	return filesDir, meta, cleanup
}

func TestRecoveryDeletesCorruptedFiles(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a test file
	testFile := filepath.Join(filesDir, "corrupted.dat")
	actualData := []byte("actual data content")
	err := os.WriteFile(testFile, actualData, 0o644)
	require.NoError(t, err)

	// Add metadata with WRONG size (simulating corruption)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	metaKey := keys.MakeMetadataKey("corrupted-key")
	wrongSize := int64(len(actualData) + 100) // Metadata says file is larger
	vm := &pb.ValueMessage{
		ValueLength: wrongSize,
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: testFile,
		Checksum:    12345,
	}
	vmBytes, _ := proto.Marshal(vm)
	batch.Put(metaKey, vmBytes)

	// Add compaction entry
	compactionKey := keys.MakeCompactionKey(time.Now().UnixNano(), "corrupted-key")
	batch.Put(compactionKey, []byte(testFile))

	err = meta.Handle().Write(wo, batch)
	require.NoError(t, err)

	// Run recovery
	recovery := NewRecoveryManager(meta, filesDir)
	err = recovery.RecoverOnStartup()
	require.NoError(t, err)

	// Verify file was deleted
	_, err = os.Stat(testFile)
	assert.True(t, os.IsNotExist(err), "Corrupted file should be deleted")

	// Verify metadata was deleted
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	slice, _ := meta.Handle().Get(ro, metaKey)
	assert.False(t, slice.Exists(), "Metadata should be deleted for corrupted file")
	slice.Free()

	// Verify compaction entry was removed
	compactionSlice, _ := meta.Handle().Get(ro, compactionKey)
	assert.False(t, compactionSlice.Exists(), "Compaction entry should be removed")
	compactionSlice.Free()
}

func TestRecoveryHandlesStaleEntries(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create old and new files
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
		ValueLength: 8, // "new data"
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: newFile,
	}
	vmBytes, _ := proto.Marshal(vm)
	batch.Put(metaKey, vmBytes)

	// Add stale compaction entry for OLD file
	compactionKey := keys.MakeCompactionKey(time.Now().UnixNano(), "test-key")
	batch.Put(compactionKey, []byte(oldFile))

	err = meta.Handle().Write(wo, batch)
	require.NoError(t, err)

	// Run recovery
	recovery := NewRecoveryManager(meta, filesDir)
	err = recovery.RecoverOnStartup()
	require.NoError(t, err)

	// Verify stale compaction entry was removed
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	compactionSlice, _ := meta.Handle().Get(ro, compactionKey)
	assert.False(t, compactionSlice.Exists(), "Stale compaction entry should be removed")
	compactionSlice.Free()

	// Verify metadata still exists and points to new file
	metaSlice, _ := meta.Handle().Get(ro, metaKey)
	assert.True(t, metaSlice.Exists(), "Metadata should still exist")
	if metaSlice.Exists() {
		var checkVm pb.ValueMessage
		err = proto.Unmarshal(metaSlice.Data(), &checkVm)
		assert.NoError(t, err)
		assert.Equal(t, newFile, checkVm.RawFilePath, "Metadata should point to new file")
	}
	metaSlice.Free()

	// Both files should still exist (stale file not deleted)
	_, err = os.Stat(oldFile)
	assert.NoError(t, err, "Old file should still exist")
	_, err = os.Stat(newFile)
	assert.NoError(t, err, "New file should still exist")
}

func TestRecoveryHandlesOrphanedFiles(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create orphaned file (no metadata)
	orphanFile := filepath.Join(filesDir, "orphan.dat")
	err := os.WriteFile(orphanFile, []byte("orphan data"), 0o644)
	require.NoError(t, err)

	// Add compaction entry without corresponding metadata
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	compactionKey := keys.MakeCompactionKey(time.Now().UnixNano(), "nonexistent-key")
	batch.Put(compactionKey, []byte(orphanFile))

	err = meta.Handle().Write(wo, batch)
	require.NoError(t, err)

	// Run recovery
	recovery := NewRecoveryManager(meta, filesDir)
	err = recovery.RecoverOnStartup()
	require.NoError(t, err)

	// Verify compaction entry was removed
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	compactionSlice, _ := meta.Handle().Get(ro, compactionKey)
	assert.False(t, compactionSlice.Exists(), "Orphaned compaction entry should be removed")
	compactionSlice.Free()
}

func TestRecoveryValidatesAllEntriesRegardlessOfAge(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a file
	testFile := filepath.Join(filesDir, "old.dat")
	data := []byte("test data")
	err := os.WriteFile(testFile, data, 0o644)
	require.NoError(t, err)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Add metadata
	metaKey := keys.MakeMetadataKey("old-key")
	vm := &pb.ValueMessage{
		ValueLength: int64(len(data)),
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: testFile,
	}
	vmBytes, _ := proto.Marshal(vm)
	batch.Put(metaKey, vmBytes)

	// Add OLD compaction entry (simulate >1 hour old)
	oldTimestamp := time.Now().Add(-time.Hour).UnixNano()
	compactionKey := keys.MakeCompactionKey(oldTimestamp, "old-key")
	batch.Put(compactionKey, []byte(testFile))

	err = meta.Handle().Write(wo, batch)
	require.NoError(t, err)

	// Corrupt the file
	err = os.WriteFile(testFile, []byte("wrong"), 0o644)
	require.NoError(t, err)

	// Run recovery - should validate even though entry is old
	recovery := NewRecoveryManager(meta, filesDir)
	err = recovery.RecoverOnStartup()
	require.NoError(t, err)

	// Verify corrupted file was deleted
	_, err = os.Stat(testFile)
	assert.True(t, os.IsNotExist(err), "Corrupted file should be deleted even if compaction entry is old")

	// Verify compaction entry was removed
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	compactionSlice, _ := meta.Handle().Get(ro, compactionKey)
	assert.False(t, compactionSlice.Exists(), "Compaction entry should be removed")
	compactionSlice.Free()
}

func TestParallelRecovery(t *testing.T) {
	filesDir, meta, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create multiple files with different states
	numFiles := 100
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	for i := 0; i < numFiles; i++ {
		batch := grocksdb.NewWriteBatch()
		defer batch.Destroy()

		fileName := fmt.Sprintf("file%d.dat", i)
		filePath := filepath.Join(filesDir, fileName)

		if i%3 == 0 {
			// Valid files
			data := []byte(fmt.Sprintf("data for file %d", i))
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

			compactionKey := keys.MakeCompactionKey(time.Now().UnixNano(), fmt.Sprintf("key%d", i))
			batch.Put(compactionKey, []byte(filePath))
		} else if i%3 == 1 {
			// Corrupted files (size mismatch)
			data := []byte("short")
			err := os.WriteFile(filePath, data, 0o644)
			require.NoError(t, err)

			metaKey := keys.MakeMetadataKey(fmt.Sprintf("key%d", i))
			vm := &pb.ValueMessage{
				ValueLength: 1000, // Wrong size
				ValueType:   pb.ValueType_RAW_FILE,
				RawFilePath: filePath,
			}
			vmBytes, _ := proto.Marshal(vm)
			batch.Put(metaKey, vmBytes)

			compactionKey := keys.MakeCompactionKey(time.Now().UnixNano(), fmt.Sprintf("key%d", i))
			batch.Put(compactionKey, []byte(filePath))
		} else {
			// Stale entries (metadata points elsewhere)
			// Create the file but metadata points to different file
			data := []byte("stale file")
			err := os.WriteFile(filePath, data, 0o644)
			require.NoError(t, err)

			metaKey := keys.MakeMetadataKey(fmt.Sprintf("key%d", i))
			vm := &pb.ValueMessage{
				ValueLength: 10,
				ValueType:   pb.ValueType_RAW_FILE,
				RawFilePath: "/different/path.dat",
			}
			vmBytes, _ := proto.Marshal(vm)
			batch.Put(metaKey, vmBytes)

			compactionKey := keys.MakeCompactionKey(time.Now().UnixNano(), fmt.Sprintf("key%d", i))
			batch.Put(compactionKey, []byte(filePath))
		}

		err := meta.Handle().Write(wo, batch)
		require.NoError(t, err)
	}

	// Run recovery with parallel validation
	recovery := NewRecoveryManager(meta, filesDir)
	err := recovery.RecoverOnStartup()
	require.NoError(t, err)

	// Verify all compaction entries were removed after recovery
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := meta.Handle().NewIterator(ro)
	defer it.Close()

	compactionCount := 0
	prefix := []byte(keys.CompactionIndexPrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		compactionCount++
	}
	assert.Equal(t, 0, compactionCount, "All compaction entries should be removed after recovery")

	// Verify corrupted files were deleted
	for i := 1; i < numFiles; i += 3 {
		fileName := fmt.Sprintf("file%d.dat", i)
		filePath := filepath.Join(filesDir, fileName)
		_, err := os.Stat(filePath)
		assert.True(t, os.IsNotExist(err), "Corrupted file %s should be deleted", fileName)
	}
}
