// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package files_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage"
	"github.com/tigrisdata/ocache/storage/files"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/utils"
	"google.golang.org/protobuf/proto"
)

type recoveryValidationFault int

const (
	metadataPointReadFault recoveryValidationFault = iota
	fileStatFault
)

func TestRecoveryValidationFailureIsNonDestructive(t *testing.T) {
	t.Run("metadata point-read I/O error", func(t *testing.T) {
		testRecoveryValidationFailure(t, metadataPointReadFault, false)
	})
	t.Run("non-ENOENT stat error", func(t *testing.T) {
		testRecoveryValidationFailure(t, fileStatFault, false)
	})
	t.Run("errors in separate deletion batches", testRecoveryValidationFailuresAcrossBatches)
}

func TestRecoveryValidationFailureDoesNotRollbackOtherCleanup(t *testing.T) {
	testRecoveryValidationFailure(t, metadataPointReadFault, true)
}

func TestRecoveryConfirmedAbsenceCleanupControls(t *testing.T) {
	testRecoveryCleanupControls(t)
}

func TestNewStorageCanRetryAfterRecoveryError(t *testing.T) {
	root := t.TempDir()
	filesDir := filepath.Join(root, "files")
	require.NoError(t, os.MkdirAll(filesDir, 0o755))

	const userKey = "recovery-constructor-retry"
	filePath := filepath.Join(filesDir, "pending.raw")
	backupPath := filepath.Join(root, "pending.backup")
	data := []byte("original bytes after recovery retry")
	require.NoError(t, os.WriteFile(filePath, data, 0o644))

	var meta *metadata.MetaDB
	var store *storage.Storage
	t.Cleanup(func() {
		if store != nil {
			store.Close()
		}
		if meta != nil {
			meta.Close()
		}
	})

	meta, err := metadata.NewMetaDB(root, 0, nil, nil)
	require.NoError(t, err)
	message := &pb.ValueMessage{
		ValueLength: int64(len(data)),
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: filePath,
	}
	metadataKey, compactionKey, metadataBytes := writeRecoveryEntry(t, meta, userKey, filePath, message)
	meta.Close()
	meta = nil

	// Replace the previously valid file with a self-referential symlink so
	// os.Stat returns ELOOP while the original bytes remain available to restore.
	require.NoError(t, os.Rename(filePath, backupPath))
	require.NoError(t, os.Symlink(filepath.Base(filePath), filePath))

	config := &storage.StorageConfig{
		DiskPath:            root,
		RecoveryWorkers:     1,
		DisableRecompaction: true,
		CleanupInterval:     time.Hour,
	}
	first, firstErr := storage.NewStorageWithConfig(config)
	if first != nil {
		first.Close()
	}
	require.Nil(t, first)
	require.ErrorIs(t, firstErr, syscall.ELOOP)

	meta, err = metadata.NewMetaDB(root, 0, nil, nil)
	require.NoError(t, err, "the failed constructor must release its RocksDB lock")
	storedMetadata, hasMetadata := readDBValue(t, meta, metadataKey)
	require.True(t, hasMetadata)
	require.Equal(t, metadataBytes, storedMetadata)
	storedCompaction, hasCompaction := readDBValue(t, meta, compactionKey)
	require.True(t, hasCompaction)
	require.Equal(t, []byte(filePath), storedCompaction)
	meta.Close()
	meta = nil

	require.NoError(t, os.Remove(filePath))
	require.NoError(t, os.Rename(backupPath, filePath))
	store, err = storage.NewStorageWithConfig(config)
	require.NoError(t, err)
	checkStorageGet(t, store, userKey, data)
}

func testRecoveryValidationFailure(t *testing.T, fault recoveryValidationFault, checkUnrelatedCleanup bool) {
	t.Helper()

	root := t.TempDir()
	filesDir := filepath.Join(root, "files")
	require.NoError(t, os.MkdirAll(filesDir, 0o755))

	var meta *metadata.MetaDB
	var store *storage.Storage
	t.Cleanup(func() {
		if store != nil {
			store.Close()
		}
		if meta != nil {
			meta.Close()
		}
	})

	meta, err := metadata.NewMetaDB(root, 0, nil, nil)
	require.NoError(t, err)

	targetKey := "recovery-validation-target"
	if fault == fileStatFault {
		targetKey = "recovery-stat-target"
	}
	targetData := []byte("original pending raw-file bytes")
	targetFile := filepath.Join(filesDir, "pending.raw")
	require.NoError(t, os.WriteFile(targetFile, targetData, 0o644))
	targetMessage := &pb.ValueMessage{
		ValueLength: int64(len(targetData)),
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: targetFile,
	}
	targetMetadataKey, targetCompactionKey, targetMetadataBytes := writeRecoveryEntry(t, meta, targetKey, targetFile, targetMessage)

	var unrelatedFile string
	var unrelatedMetadataKey, unrelatedCompactionKey []byte
	if checkUnrelatedCleanup {
		// This independently corrupted entry must still be cleaned up even when
		// a different entry cannot be validated.
		unrelatedKey := "unrelated-size-mismatch"
		unrelatedData := []byte("bad")
		unrelatedFile = filepath.Join(filesDir, "unrelated-corrupt.raw")
		require.NoError(t, os.WriteFile(unrelatedFile, unrelatedData, 0o644))
		unrelatedMessage := &pb.ValueMessage{
			ValueLength: int64(len(unrelatedData) + 1),
			ValueType:   pb.ValueType_RAW_FILE,
			RawFilePath: unrelatedFile,
		}
		unrelatedMetadataKey, unrelatedCompactionKey, _ = writeRecoveryEntry(t, meta, unrelatedKey, unrelatedFile, unrelatedMessage)
	}

	getMetadata := utils.GetMetadata
	statFile := os.Stat
	var injectedErr error
	var metadataFaultReached bool
	var metadataLookupSucceeded bool
	var statFaultReached bool
	switch fault {
	case metadataPointReadFault:
		injectedErr = fmt.Errorf("injected metadata point-read error: %w", syscall.EIO)
		getMetadata = func(db *metadata.MetaDB, key string) (*pb.ValueMessage, error) {
			if key == string(targetMetadataKey) {
				metadataFaultReached = true
				return nil, injectedErr
			}
			return utils.GetMetadata(db, key)
		}
	case fileStatFault:
		injectedErr = &os.PathError{Op: "stat", Path: targetFile, Err: syscall.EIO}
		getMetadata = func(db *metadata.MetaDB, key string) (*pb.ValueMessage, error) {
			value, err := utils.GetMetadata(db, key)
			if key == string(targetMetadataKey) && err == nil {
				metadataLookupSucceeded = true
			}
			return value, err
		}
		statFile = func(path string) (os.FileInfo, error) {
			if path == targetFile {
				statFaultReached = true
				return nil, injectedErr
			}
			return os.Stat(path)
		}
	default:
		t.Fatalf("unknown recovery validation fault: %d", fault)
	}

	recovery := files.NewRecoveryManagerForTest(meta, root, 2, getMetadata, statFile)
	recoveryErr := recovery.RecoverOnStartup()
	if fault == metadataPointReadFault {
		require.True(t, metadataFaultReached, "metadata fault seam was not reached")
	}
	if fault == fileStatFault {
		require.True(t, metadataLookupSucceeded, "metadata lookup failed before the stat fault")
		require.True(t, statFaultReached, "stat fault seam was not reached")
	}
	checkRecoveryAssertion(t, errors.Is(recoveryErr, injectedErr), fmt.Sprintf("RecoverOnStartup returned %v, want an error wrapping %v", recoveryErr, injectedErr))

	storedMetadata, hasMetadata := readDBValue(t, meta, targetMetadataKey)
	checkRecoveryAssertion(t, hasMetadata && bytes.Equal(storedMetadata, targetMetadataBytes), "the affected metadata row changed or disappeared")
	storedCompaction, hasCompaction := readDBValue(t, meta, targetCompactionKey)
	checkRecoveryAssertion(t, hasCompaction && bytes.Equal(storedCompaction, []byte(targetFile)), "the affected compaction row changed or disappeared")
	storedFile, fileErr := os.ReadFile(targetFile)
	checkRecoveryAssertion(t, fileErr == nil && bytes.Equal(storedFile, targetData), fmt.Sprintf("the affected raw-file bytes changed or became unreadable: %v", fileErr))

	if checkUnrelatedCleanup {
		_, exists := readDBValue(t, meta, unrelatedMetadataKey)
		require.False(t, exists, "the unrelated size-mismatched metadata should be cleaned up")
		_, exists = readDBValue(t, meta, unrelatedCompactionKey)
		require.False(t, exists, "the unrelated size-mismatched compaction row should be cleaned up")
		_, err = os.Stat(unrelatedFile)
		require.True(t, os.IsNotExist(err), "the unrelated size-mismatched file should be cleaned up")
	}

	// Reopen through the public storage constructor after the injected fault has
	// gone away, then exercise the same Get operation used by ordinary callers.
	meta.Close()
	meta = nil
	store, err = storage.NewStorageWithConfig(&storage.StorageConfig{
		DiskPath:            root,
		RecoveryWorkers:     2,
		DisableRecompaction: true,
		CleanupInterval:     time.Hour,
	})
	require.NoError(t, err)

	checkStorageGet(t, store, targetKey, targetData)
}

func testRecoveryValidationFailuresAcrossBatches(t *testing.T) {
	root := t.TempDir()
	filesDir := filepath.Join(root, "files")
	require.NoError(t, os.MkdirAll(filesDir, 0o755))

	var meta *metadata.MetaDB
	var store *storage.Storage
	t.Cleanup(func() {
		if store != nil {
			store.Close()
		}
		if meta != nil {
			meta.Close()
		}
	})
	meta, err := metadata.NewMetaDB(root, 0, nil, nil)
	require.NoError(t, err)

	type pendingEntry struct {
		userKey       string
		filePath      string
		data          []byte
		metadataKey   []byte
		compactionKey []byte
		metadataBytes []byte
	}
	const entryCount = 101
	entries := make([]pendingEntry, entryCount)
	writeBatch := grocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	for i := range entries {
		userKey := fmt.Sprintf("batch-key-%03d", i)
		filePath := filepath.Join(filesDir, fmt.Sprintf("pending-%03d.raw", i))
		data := []byte(fmt.Sprintf("original batch bytes %03d", i))
		require.NoError(t, os.WriteFile(filePath, data, 0o644))
		message := &pb.ValueMessage{
			ValueLength: int64(len(data)),
			ValueType:   pb.ValueType_RAW_FILE,
			RawFilePath: filePath,
		}
		metadataBytes, err := proto.Marshal(message)
		require.NoError(t, err)
		metadataKey := keys.MakeMetadataKey(userKey)
		compactionKey := keys.MakeCompactionKey(int64(i+1), userKey)
		writeBatch.Put(metadataKey, metadataBytes)
		writeBatch.Put(compactionKey, []byte(filePath))
		entries[i] = pendingEntry{
			userKey:       userKey,
			filePath:      filePath,
			data:          data,
			metadataKey:   metadataKey,
			compactionKey: compactionKey,
			metadataBytes: metadataBytes,
		}
	}
	writeOptions := grocksdb.NewDefaultWriteOptions()
	defer writeOptions.Destroy()
	require.NoError(t, meta.Handle().Write(writeOptions, writeBatch))

	firstErr := errors.New("injected first-batch metadata point-read error")
	lastErr := &os.PathError{Op: "stat", Path: entries[entryCount-1].filePath, Err: syscall.EIO}
	var firstFaultReached bool
	var lastMetadataLookupSucceeded bool
	var statFaultReached bool
	getMetadata := func(db *metadata.MetaDB, key string) (*pb.ValueMessage, error) {
		if key == string(entries[0].metadataKey) {
			firstFaultReached = true
			return nil, firstErr
		}
		value, err := utils.GetMetadata(db, key)
		if key == string(entries[entryCount-1].metadataKey) && err == nil {
			lastMetadataLookupSucceeded = true
		}
		return value, err
	}
	statFile := func(path string) (os.FileInfo, error) {
		if path == lastErr.Path {
			statFaultReached = true
			return nil, lastErr
		}
		return os.Stat(path)
	}
	recovery := files.NewRecoveryManagerForTest(meta, root, 1, getMetadata, statFile)
	recoveryErr := recovery.RecoverOnStartup()
	require.True(t, firstFaultReached, "first-batch metadata fault seam was not reached")
	require.True(t, lastMetadataLookupSucceeded, "last-entry metadata lookup failed before the stat fault")
	require.True(t, statFaultReached, "last-entry stat fault seam was not reached")
	checkRecoveryAssertion(t, errors.Is(recoveryErr, firstErr) && errors.Is(recoveryErr, lastErr), fmt.Sprintf("RecoverOnStartup returned %v, want both validation failures", recoveryErr))

	for _, index := range []int{0, entryCount - 1} {
		entry := entries[index]
		storedMetadata, hasMetadata := readDBValue(t, meta, entry.metadataKey)
		checkRecoveryAssertion(t, hasMetadata && bytes.Equal(storedMetadata, entry.metadataBytes), fmt.Sprintf("metadata for %s changed or disappeared", entry.userKey))
		storedCompaction, hasCompaction := readDBValue(t, meta, entry.compactionKey)
		checkRecoveryAssertion(t, hasCompaction && bytes.Equal(storedCompaction, []byte(entry.filePath)), fmt.Sprintf("compaction row for %s changed or disappeared", entry.userKey))
		storedFile, fileErr := os.ReadFile(entry.filePath)
		checkRecoveryAssertion(t, fileErr == nil && bytes.Equal(storedFile, entry.data), fmt.Sprintf("raw-file bytes for %s changed or became unreadable: %v", entry.userKey, fileErr))
	}

	meta.Close()
	meta = nil
	store, err = storage.NewStorageWithConfig(&storage.StorageConfig{
		DiskPath:            root,
		RecoveryWorkers:     1,
		DisableRecompaction: true,
		CleanupInterval:     time.Hour,
	})
	require.NoError(t, err)
	for _, index := range []int{0, entryCount - 1} {
		entry := entries[index]
		checkStorageGet(t, store, entry.userKey, entry.data)
	}
}

func testRecoveryCleanupControls(t *testing.T) {
	root := t.TempDir()
	filesDir := filepath.Join(root, "files")
	require.NoError(t, os.MkdirAll(filesDir, 0o755))
	meta, err := metadata.NewMetaDB(root, 0, nil, nil)
	require.NoError(t, err)
	defer meta.Close()

	orphanKey := "recovery-control-orphan"
	orphanFile := filepath.Join(filesDir, "orphan.raw")
	require.NoError(t, os.WriteFile(orphanFile, []byte("orphan"), 0o644))
	orphanMetadataKey, orphanCompactionKey, _ := writeRecoveryEntry(t, meta, orphanKey, orphanFile, nil)

	missingKey := "recovery-control-enoent"
	missingFile := filepath.Join(filesDir, "genuinely-missing.raw")
	missingMessage := &pb.ValueMessage{
		ValueLength: 10,
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: missingFile,
	}
	missingMetadataKey, missingCompactionKey, _ := writeRecoveryEntry(t, meta, missingKey, missingFile, missingMessage)

	corruptKey := "recovery-control-size-mismatch"
	corruptData := []byte("wrong size")
	corruptFile := filepath.Join(filesDir, "wrong-size.raw")
	require.NoError(t, os.WriteFile(corruptFile, corruptData, 0o644))
	corruptMessage := &pb.ValueMessage{
		ValueLength: int64(len(corruptData) + 10),
		ValueType:   pb.ValueType_RAW_FILE,
		RawFilePath: corruptFile,
	}
	corruptMetadataKey, corruptCompactionKey, _ := writeRecoveryEntry(t, meta, corruptKey, corruptFile, corruptMessage)

	recovery := files.NewRecoveryManagerForTest(meta, root, 2, utils.GetMetadata, os.Stat)
	require.NoError(t, recovery.RecoverOnStartup())

	_, exists := readDBValue(t, meta, orphanMetadataKey)
	require.False(t, exists, "the missing-metadata control should remain absent")
	_, exists = readDBValue(t, meta, orphanCompactionKey)
	require.False(t, exists, "the missing-metadata compaction row should be removed")
	_, err = os.Stat(orphanFile)
	require.True(t, os.IsNotExist(err), "the orphaned file should be removed")

	_, exists = readDBValue(t, meta, missingMetadataKey)
	require.False(t, exists, "a file confirmed missing by ENOENT should have its metadata removed")
	_, exists = readDBValue(t, meta, missingCompactionKey)
	require.False(t, exists, "a file confirmed missing by ENOENT should have its compaction row removed")

	_, exists = readDBValue(t, meta, corruptMetadataKey)
	require.False(t, exists, "a size-mismatched file should have its metadata removed")
	_, exists = readDBValue(t, meta, corruptCompactionKey)
	require.False(t, exists, "a size-mismatched file should have its compaction row removed")
	_, err = os.Stat(corruptFile)
	require.True(t, os.IsNotExist(err), "a size-mismatched file should be removed")
}

func writeRecoveryEntry(
	t *testing.T,
	meta *metadata.MetaDB,
	userKey string,
	filePath string,
	message *pb.ValueMessage,
) (metadataKey, compactionKey, metadataBytes []byte) {
	return writeRecoveryEntryAt(t, meta, userKey, filePath, message, time.Now().UnixNano())
}

func writeRecoveryEntryAt(
	t *testing.T,
	meta *metadata.MetaDB,
	userKey string,
	filePath string,
	message *pb.ValueMessage,
	timestamp int64,
) (metadataKey, compactionKey, metadataBytes []byte) {
	t.Helper()

	metadataKey = keys.MakeMetadataKey(userKey)
	compactionKey = keys.MakeCompactionKey(timestamp, userKey)
	compactionValue := []byte(filePath)

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	if message != nil {
		var err error
		metadataBytes, err = proto.Marshal(message)
		require.NoError(t, err)
		batch.Put(metadataKey, metadataBytes)
	}
	batch.Put(compactionKey, compactionValue)

	writeOptions := grocksdb.NewDefaultWriteOptions()
	defer writeOptions.Destroy()
	require.NoError(t, meta.Handle().Write(writeOptions, batch))
	return metadataKey, compactionKey, metadataBytes
}

func readDBValue(t *testing.T, meta *metadata.MetaDB, key []byte) ([]byte, bool) {
	t.Helper()

	readOptions := grocksdb.NewDefaultReadOptions()
	defer readOptions.Destroy()
	slice, err := meta.Handle().Get(readOptions, key)
	require.NoError(t, err)
	require.NotNil(t, slice)
	exists := slice.Exists()
	var value []byte
	if exists {
		value = bytes.Clone(slice.Data())
	}
	slice.Free()
	return value, exists
}

func checkStorageGet(t *testing.T, store *storage.Storage, key string, data []byte) {
	t.Helper()

	reader, found, getErr := store.Get(key, 0, 0)
	var got []byte
	var readErr error
	if reader != nil {
		got, readErr = io.ReadAll(reader)
		if closer, ok := reader.(io.Closer); ok {
			if closeErr := closer.Close(); readErr == nil {
				readErr = closeErr
			}
		}
	}
	checkRecoveryAssertion(t, getErr == nil && found && readErr == nil && bytes.Equal(got, data), fmt.Sprintf("Get returned found=%t, error=%v, read error=%v, bytes=%q", found, getErr, readErr, got))
}

func checkRecoveryAssertion(t *testing.T, condition bool, detail string) {
	t.Helper()
	if !condition {
		t.Errorf("%s", detail)
	}
}
