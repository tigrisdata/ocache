// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/storage/keys"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/utils"
)

func TestRecoveryRestartPreservesPendingCompaction(t *testing.T) {
	const key = "recovery-restart-pending-compaction"

	diskPath := t.TempDir()
	config := func() *StorageConfig {
		return &StorageConfig{
			DiskPath:            diskPath,
			InlineThreshold:     DefaultInlineThreshold,
			CompactThreshold:    DefaultCompactThreshold,
			SegmentSize:         DefaultSegmentSize,
			MaxDiskUsage:        0,
			TTL:                 0,
			DisableRecompaction: true,
		}
	}

	first, err := NewStorageWithConfig(config())
	require.NoError(t, err)
	t.Cleanup(func() {
		if first != nil {
			first.Close()
		}
	})

	// Keep the pending row in RocksDB until startup recovery runs.
	first.compactor.Close()
	value := bytes.Repeat([]byte("m"), 2*DefaultInlineThreshold)
	require.NoError(t, first.Put(key, bytes.NewReader(value), 0))

	beforeRestart, err := utils.GetMetadata(first.meta, string(keys.MakeMetadataKey(key)))
	require.NoError(t, err)
	require.Equal(t, pb.ValueType_RAW_FILE, beforeRestart.ValueType)
	rawPath := beforeRestart.RawFilePath
	require.NotEmpty(t, rawPath)
	require.FileExists(t, rawPath)
	require.Equal(t, 1, recoveryRestartCompactionRows(t, first))

	first.Close()
	first = nil

	second, err := NewStorageWithConfig(config())
	require.NoError(t, err)
	t.Cleanup(func() {
		if second != nil {
			second.Close()
		}
	})

	recoveryRestartAssertValue(t, second, key, value)

	// The reopened storage's normal background compactor must consume the
	// durable row without another Put and publish the segment metadata.
	migrated := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		current, err := utils.GetMetadata(second.meta, string(keys.MakeMetadataKey(key)))
		if err == nil && current.ValueType == pb.ValueType_SEGMENT {
			migrated = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !migrated {
		t.Errorf("recovery-claim: reopened value did not migrate from RAW_FILE to SEGMENT")
		return
	}

	recoveryRestartAssertValue(t, second, key, value)

	// Compaction publishes the segment before the deletion queue reclaims the
	// old raw source.
	reclaimed := false
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, err := os.Stat(rawPath)
		if os.IsNotExist(err) {
			reclaimed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !reclaimed {
		t.Errorf("recovery-claim: raw source was not reclaimed after segment publication")
	}
}

func recoveryRestartAssertValue(t *testing.T, s *Storage, key string, want []byte) {
	t.Helper()

	reader, found, err := s.Get(key, 0, 0)
	if err != nil || !found {
		if closer, ok := reader.(io.Closer); ok {
			_ = closer.Close()
		}
		t.Errorf("recovery-claim: Get after restart did not return the value (found=%t, err=%v)", found, err)
		return
	}

	got, readErr := io.ReadAll(reader)
	if closer, ok := reader.(io.Closer); ok {
		if closeErr := closer.Close(); readErr == nil {
			readErr = closeErr
		}
	}
	if readErr != nil {
		t.Errorf("recovery-claim: Get after restart could not read the value: %v", readErr)
		return
	}
	if !bytes.Equal(got, want) {
		t.Errorf("recovery-claim: Get after restart returned bytes different from the Put payload")
	}
}

func recoveryRestartCompactionRows(t testing.TB, s *Storage) int {
	t.Helper()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	it := s.meta.Handle().NewIterator(ro)
	defer it.Close()

	count := 0
	prefix := []byte(keys.CompactionIndexPrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
	}
	require.NoError(t, it.Err())
	return count
}
