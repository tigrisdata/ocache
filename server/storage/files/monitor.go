package files

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/fd"
	"github.com/tigrisdata/ocache/server/storage/keys"
	"github.com/tigrisdata/ocache/server/storage/metadata"
	"github.com/tigrisdata/ocache/server/utils"
)

const (
	// KernelSyncAge is the age when we estimate kernel syncs
	KernelSyncAge = 60 * time.Second
)

// monitorStats tracks statistics for a monitoring run
type monitorStats struct {
	checked      int
	synced       int
	stale        int
	corrupted    int
	pending      int
	errors       int
	filesDeleted int
}

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

	// Skip initial check if context is already cancelled (quick shutdown in tests)
	select {
	case <-m.ctx.Done():
		return
	default:
	}

	// Initial check after startup
	m.checkAndCleanup()

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if we should stop before starting cleanup
			select {
			case <-m.ctx.Done():
				return
			default:
			}
			m.checkAndCleanup()
		case <-m.ctx.Done():
			return
		}
	}
}

// checkAndCleanup checks for synced files and removes their tracking entries
func (m *SyncMonitor) checkAndCleanup() {
	// Check if we should stop before accessing RocksDB
	select {
	case <-m.ctx.Done():
		return
	default:
	}

	startTime := time.Now()
	stats := &monitorStats{}

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := m.meta.Handle().NewIterator(ro)
	defer it.Close()

	var toDelete [][]byte
	var filesToDelete []string
	prefix := []byte(keys.SyncIndexPrefix)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		// Check context cancellation during iteration
		select {
		case <-m.ctx.Done():
			// Context cancelled, stop processing immediately
			return
		default:
		}

		stats.checked++

		key := it.Key()
		value := it.Value()

		// Parse sync entry
		timestamp, filepath, err := keys.ParseSyncKey(key.Data())
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

		// Check if entry is stale (metadata changed) or corrupted
		err = m.checkEntryStatus(entry, filepath)
		if err != nil {
			toDelete = append(toDelete, bytes.Clone(key.Data()))
			filesToDelete = append(filesToDelete, filepath)

			if errors.Is(err, utils.ErrFileCorrupted) {
				stats.corrupted++
				var sizeMismatch *utils.FileSizeMismatchError
				if errors.As(err, &sizeMismatch) {
					zlog.Warn().
						Str("filepath", sizeMismatch.FilePath).
						Str("key", sizeMismatch.Key).
						Int64("actualSize", sizeMismatch.ActualSize).
						Int64("expectedSize", sizeMismatch.ExpectedSize).
						Msg("files.monitor: file corrupted (size mismatch)")
				}
			} else {
				stats.stale++
			}
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
	m.deleteFiles(filesToDelete, stats)

	// Log statistics
	if stats.checked > 0 || len(toDelete) > 0 {
		zlog.Info().
			Int("checked", stats.checked).
			Int("synced", stats.synced).
			Int("stale", stats.stale).
			Int("corrupted", stats.corrupted).
			Int("pending", stats.pending).
			Int("files_deleted", stats.filesDeleted).
			Int("errors", stats.errors).
			Dur("duration", time.Since(startTime)).
			Msg("files.monitor: cleanup completed")
	}
}

// checkEntryStatus checks if a sync entry is stale (metadata changed) or corrupted (size mismatch)
// Returns nil if valid, utils.ErrEntryStale if stale, or utils.FileSizeMismatchError if corrupted
func (m *SyncMonitor) checkEntryStatus(entry *pb.SyncEntry, filepath string) error {
	// Check if context is cancelled before accessing database
	select {
	case <-m.ctx.Done():
		// Consider it stale if we're shutting down
		return utils.ErrEntryStale
	default:
	}

	// Fetch current metadata once
	metadata, err := utils.GetMetadata(m.meta, entry.MetadataKey)
	if err != nil {
		// Error fetching metadata - consider it stale
		return utils.ErrEntryStale
	}

	// Validate the file entry
	err = utils.ValidateFileEntry(metadata, filepath, "files.monitor", entry.MetadataKey)
	if err != nil {
		// Entry is stale if validation fails
		return utils.ErrEntryStale
	}

	// Check for file size mismatch (corruption)
	// Only check if metadata indicates it's a raw file
	if metadata.ValueType == pb.ValueType_RAW_FILE && metadata.RawFilePath == filepath {
		fileInfo, err := os.Stat(filepath)
		if err != nil {
			if os.IsNotExist(err) {
				// File doesn't exist - consider it stale
				return utils.ErrEntryStale
			}
			// Other stat error - log and consider stale
			zlog.Warn().
				Str("filepath", filepath).
				Err(err).
				Msg("files.monitor: failed to stat file")
			return utils.ErrEntryStale
		}

		// Check if file size matches metadata
		if fileInfo.Size() != metadata.ValueLength {
			// Return detailed corruption error
			return &utils.FileSizeMismatchError{
				Key:          entry.MetadataKey,
				FilePath:     filepath,
				ActualSize:   fileInfo.Size(),
				ExpectedSize: metadata.ValueLength,
			}
		}
	}

	// Entry is valid
	return nil
}

// deleteEntries batch deletes sync entries
func (m *SyncMonitor) deleteEntries(keys [][]byte) error {
	// Check if context is cancelled before writing to database
	select {
	case <-m.ctx.Done():
		// Don't attempt database operations during shutdown
		return nil
	default:
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, key := range keys {
		batch.Delete(key)
	}

	return m.meta.Handle().Write(wo, batch)
}

// deleteFiles deletes orphaned files
// These are files where the metadata no longer points to them (stale entries)
// It's safe to delete because the metadata has been updated or removed
func (m *SyncMonitor) deleteFiles(filesToDelete []string, stats *monitorStats) {
	lockManager := fd.GetFileLockManager()
	for _, filepath := range filesToDelete {
		// Track whether the file was successfully deleted
		deleted := false

		// Use a function to ensure proper lock handling with defer
		func() {
			// Acquire exclusive lock for the file before deletion
			fileLock := lockManager.GetFileLock(filepath)
			fileLock.Lock()
			defer fileLock.Unlock()

			if err := os.Remove(filepath); err != nil {
				if !os.IsNotExist(err) {
					zlog.Warn().
						Str("filepath", filepath).
						Err(err).
						Msg("files.monitor: failed to delete orphaned file")
					stats.errors++
				}
				// Don't mark as deleted if there was an error
				return
			}

			stats.filesDeleted++
			deleted = true
			zlog.Debug().
				Str("filepath", filepath).
				Msg("files.monitor: deleted orphaned file")
		}()

		// Remove the lock from the manager only if the file was successfully deleted
		// This happens outside the locked section to avoid any issues
		if deleted {
			lockManager.RemoveFileLock(filepath)
		}
	}
}
