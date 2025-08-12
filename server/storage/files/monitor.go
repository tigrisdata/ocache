package files

import (
	"bytes"
	"context"
	"os"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	// KernelSyncAge is the age when we estimate kernel syncs
	KernelSyncAge = 60 * time.Second
)

// SyncMonitor passively monitors files and removes sync entries for files that have been synced
type SyncMonitor struct {
	meta          *metadata.MetaDB
	interval      time.Duration // How often to check
	kernelSyncAge time.Duration // Age when kernel syncs (30s)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSyncMonitor creates a new sync monitor
func NewSyncMonitor(meta *metadata.MetaDB, interval time.Duration) *SyncMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	return &SyncMonitor{
		meta:          meta,
		interval:      interval,
		kernelSyncAge: KernelSyncAge,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start begins the background monitoring
func (m *SyncMonitor) Start() {
	m.wg.Add(1)
	go m.run()
	zlog.Info().
		Dur("interval", m.interval).
		Dur("kernel_sync_age", m.kernelSyncAge).
		Msg("files.monitor: started sync monitor")
}

// Stop gracefully stops the monitor
func (m *SyncMonitor) Stop() {
	zlog.Info().Msg("files.monitor: stopping sync monitor")
	m.cancel()
	m.wg.Wait()
	zlog.Info().Msg("files.monitor: sync monitor stopped")
}

// run is the main monitoring loop
func (m *SyncMonitor) run() {
	defer m.wg.Done()

	// Initial check after startup
	m.checkAndCleanup()

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkAndCleanup()
		case <-m.ctx.Done():
			return
		}
	}
}

// monitorStats tracks statistics for a monitoring run
type monitorStats struct {
	checked       int
	synced        int
	stale         int
	pending       int
	errors        int
	filesDeleted  int
}

// checkAndCleanup checks for synced files and removes their tracking entries
func (m *SyncMonitor) checkAndCleanup() {
	startTime := time.Now()
	stats := &monitorStats{}

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := m.meta.Handle().NewIterator(ro)
	defer it.Close()

	var toDelete [][]byte
	var filesToDelete []string
	prefix := []byte(SyncIndexPrefix)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		stats.checked++

		key := it.Key()
		value := it.Value()

		// Parse sync entry
		timestamp, filepath, err := ParseSyncKey(key.Data())
		if err != nil {
			zlog.Warn().
				Str("key", string(key.Data())).
				Err(err).
				Msg("files.monitor: failed to parse sync key")
			stats.errors++
			key.Free()
			value.Free()
			continue
		}

		entry, err := DecodeSyncEntry(value.Data())
		if err != nil {
			zlog.Warn().
				Str("key", string(key.Data())).
				Err(err).
				Msg("files.monitor: failed to decode sync entry")
			stats.errors++
			key.Free()
			value.Free()
			continue
		}

		// Check if entry is stale (metadata changed)
		if m.isStaleEntry(entry, filepath) {
			toDelete = append(toDelete, bytes.Clone(key.Data()))
			// Mark orphaned file for deletion
			filesToDelete = append(filesToDelete, filepath)
			stats.stale++
			key.Free()
			value.Free()
			continue
		}

		// Check if kernel has synced (based on age)
		age := time.Since(time.Unix(0, timestamp))
		if age > m.kernelSyncAge {
			toDelete = append(toDelete, bytes.Clone(key.Data()))
			stats.synced++
			zlog.Debug().
				Str("filepath", filepath).
				Dur("age", age).
				Msg("files.monitor: file synced by kernel")
		} else {
			stats.pending++
		}

		key.Free()
		value.Free()
	}

	if err := it.Err(); err != nil {
		zlog.Error().
			Err(err).
			Msg("files.monitor: iterator error")
		return
	}

	// Batch delete synced/stale entries
	if len(toDelete) > 0 {
		if err := m.deleteEntries(toDelete); err != nil {
			zlog.Error().
				Err(err).
				Msg("files.monitor: failed to delete sync entries")
		}
	}

	// Delete orphaned files
	for _, filepath := range filesToDelete {
		if err := os.Remove(filepath); err != nil {
			if !os.IsNotExist(err) {
				zlog.Warn().
					Str("filepath", filepath).
					Err(err).
					Msg("files.monitor: failed to delete orphaned file")
				stats.errors++
			}
		} else {
			stats.filesDeleted++
			zlog.Debug().
				Str("filepath", filepath).
				Msg("files.monitor: deleted orphaned file")
		}
	}

	// Log statistics
	if stats.checked > 0 || len(toDelete) > 0 {
		zlog.Info().
			Int("checked", stats.checked).
			Int("synced", stats.synced).
			Int("stale", stats.stale).
			Int("pending", stats.pending).
			Int("files_deleted", stats.filesDeleted).
			Int("errors", stats.errors).
			Dur("duration", time.Since(startTime)).
			Msg("files.monitor: cleanup completed")
	}
}

// isStaleEntry checks if a sync entry is stale (metadata has changed)
func (m *SyncMonitor) isStaleEntry(entry *pb.SyncEntry, filepath string) bool {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Fetch current metadata
	metaSlice, err := m.meta.Handle().Get(ro, []byte(entry.MetadataKey))
	if err != nil || !metaSlice.Exists() {
		// Metadata doesn't exist - entry is stale
		if metaSlice != nil {
			metaSlice.Free()
		}
		return true
	}
	defer metaSlice.Free()

	var metadata pb.ValueMessage
	if err := proto.Unmarshal(metaSlice.Data(), &metadata); err != nil {
		zlog.Debug().
			Str("metadata_key", entry.MetadataKey).
			Err(err).
			Msg("files.monitor: failed to parse metadata")
		return true
	}

	// Check if metadata still points to this file
	if metadata.ValueType != pb.ValueType_RAW_FILE {
		// File was compacted or changed to inline
		zlog.Debug().
			Str("filepath", filepath).
			Str("value_type", metadata.ValueType.String()).
			Msg("files.monitor: stale entry (not raw file)")
		return true
	}

	if metadata.RawFilePath != filepath {
		// Key was updated with new file
		zlog.Debug().
			Str("old_file", filepath).
			Str("new_file", metadata.RawFilePath).
			Msg("files.monitor: stale entry (metadata updated)")
		return true
	}

	return false
}

// deleteEntries batch deletes sync entries
func (m *SyncMonitor) deleteEntries(keys [][]byte) error {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, key := range keys {
		batch.Delete(key)
	}

	return m.meta.Handle().Write(wo, batch)
}
