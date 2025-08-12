package files

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/keys"
	"github.com/tigrisdata/ocache/server/storage/metadata"
	"google.golang.org/protobuf/proto"
)

// RecoveryManager handles startup recovery and validation of files with pending syncs
type RecoveryManager struct {
	meta       *metadata.MetaDB
	filesPath  string
	numWorkers int
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(meta *metadata.MetaDB, filesPath string) *RecoveryManager {
	return &RecoveryManager{
		meta:       meta,
		filesPath:  filesPath,
		numWorkers: runtime.NumCPU(),
	}
}

// RecoverOnStartup validates all files with pending syncs and cleans up corrupted files
func (r *RecoveryManager) RecoverOnStartup() error {
	zlog.Info().Msg("files.recovery: starting startup recovery")
	startTime := time.Now()

	// Collect all sync entries
	syncEntries, err := r.collectAllSyncEntries()
	if err != nil {
		return fmt.Errorf("failed to collect sync entries: %w", err)
	}

	if len(syncEntries) == 0 {
		zlog.Info().Msg("files.recovery: no pending syncs found")
		return nil
	}

	zlog.Info().
		Int("entries", len(syncEntries)).
		Msg("files.recovery: validating sync entries")

	// Validate entries in parallel
	results := r.validateEntriesParallel(syncEntries)

	// Perform cleanup based on validation results
	if err := r.performCleanup(results); err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}

	stats := r.calculateStats(results, time.Since(startTime))
	zlog.Info().
		Int("total", stats.Total).
		Int("valid", stats.Valid).
		Int("corrupted", stats.Corrupted).
		Int("stale", stats.Stale).
		Int("orphaned", stats.Orphaned).
		Int("missing", stats.Missing).
		Dur("duration", stats.Duration).
		Msg("files.recovery: completed")

	return nil
}

// syncEntryInfo holds a sync entry with its parsed components
type syncEntryInfo struct {
	Key       []byte
	Timestamp int64
	FilePath  string
	Value     *pb.SyncEntry
}

// collectAllSyncEntries gathers all sync index entries from RocksDB
func (r *RecoveryManager) collectAllSyncEntries() ([]*syncEntryInfo, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := r.meta.Handle().NewIterator(ro)
	defer it.Close()

	var entries []*syncEntryInfo
	prefix := []byte(SyncIndexPrefix)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		value := it.Value()

		// Parse key
		timestamp, filepath, err := ParseSyncKey(key.Data())
		if err != nil {
			zlog.Warn().
				Str("key", string(key.Data())).
				Err(err).
				Msg("files.recovery: failed to parse sync key")
			key.Free()
			value.Free()
			continue
		}

		// Decode value
		entry, err := DecodeSyncEntry(value.Data())
		if err != nil {
			zlog.Warn().
				Str("key", string(key.Data())).
				Err(err).
				Msg("files.recovery: failed to decode sync entry")
			key.Free()
			value.Free()
			continue
		}

		entries = append(entries, &syncEntryInfo{
			Key:       bytes.Clone(key.Data()),
			Timestamp: timestamp,
			FilePath:  filepath,
			Value:     entry,
		})

		key.Free()
		value.Free()
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return entries, nil
}

// validateEntriesParallel validates sync entries using multiple workers
func (r *RecoveryManager) validateEntriesParallel(entries []*syncEntryInfo) []*ValidationResult {
	var wg sync.WaitGroup
	jobs := make(chan *syncEntryInfo, len(entries))
	results := make(chan *ValidationResult, len(entries))

	// Start workers
	for i := 0; i < r.numWorkers; i++ {
		wg.Add(1)
		go r.validationWorker(&wg, jobs, results)
	}

	// Queue all jobs
	for _, entry := range entries {
		jobs <- entry
	}
	close(jobs)

	// Wait for workers to complete
	wg.Wait()
	close(results)

	// Collect results
	var allResults []*ValidationResult
	for result := range results {
		allResults = append(allResults, result)
	}

	return allResults
}

// validationWorker processes validation jobs
func (r *RecoveryManager) validationWorker(wg *sync.WaitGroup, jobs <-chan *syncEntryInfo, results chan<- *ValidationResult) {
	defer wg.Done()

	for entry := range jobs {
		result := r.validateEntry(entry)
		results <- result
	}
}

// validateEntry validates a single sync entry
func (r *RecoveryManager) validateEntry(entry *syncEntryInfo) *ValidationResult {
	result := &ValidationResult{
		SyncKey:  entry.Key,
		FilePath: entry.FilePath,
	}

	// Fetch metadata
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	metaSlice, err := r.meta.Handle().Get(ro, []byte(entry.Value.MetadataKey))
	if err != nil || !metaSlice.Exists() {
		// No metadata, sync entry is orphaned
		zlog.Debug().
			Str("sync_key", string(entry.Key)).
			Str("filepath", entry.FilePath).
			Msg("files.recovery: orphaned sync entry (no metadata)")

		if metaSlice != nil {
			metaSlice.Free()
		}
		result.Status = StatusOrphaned
		return result
	}
	defer metaSlice.Free()

	// Parse metadata
	var metadata pb.ValueMessage
	if err := proto.Unmarshal(metaSlice.Data(), &metadata); err != nil {
		zlog.Warn().
			Str("filepath", entry.FilePath).
			Err(err).
			Msg("files.recovery: failed to parse metadata")
		result.Status = StatusCorrupted
		result.Error = err
		return result
	}

	// Check if metadata still points to this file
	if metadata.ValueType != pb.ValueType_RAW_FILE {
		zlog.Debug().
			Str("filepath", entry.FilePath).
			Str("value_type", metadata.ValueType.String()).
			Msg("files.recovery: stale sync entry (not raw file)")
		result.Status = StatusStale
		return result
	}

	if metadata.RawFilePath != entry.FilePath {
		zlog.Debug().
			Str("old_file", entry.FilePath).
			Str("new_file", metadata.RawFilePath).
			Msg("files.recovery: stale sync entry (metadata updated)")
		result.Status = StatusStale
		return result
	}

	// Validate physical file
	stat, err := os.Stat(entry.FilePath)
	if err != nil {
		zlog.Warn().
			Str("filepath", entry.FilePath).
			Str("key", entry.Value.MetadataKey).
			Err(err).
			Msg("files.recovery: file missing")
		result.Status = StatusMissing
		result.MetadataKey = entry.Value.MetadataKey
		result.Error = err
		return result
	}

	// Validate size
	if stat.Size() != metadata.ValueLength {
		zlog.Error().
			Str("filepath", entry.FilePath).
			Str("key", entry.Value.MetadataKey).
			Int64("expected_size", metadata.ValueLength).
			Int64("actual_size", stat.Size()).
			Msg("files.recovery: file corrupted (size mismatch)")
		result.Status = StatusCorrupted
		result.MetadataKey = entry.Value.MetadataKey
		return result
	}

	// Optional: Validate checksum for small files
	if metadata.ValueLength < 1024*1024 && metadata.Checksum > 0 { // < 1MB
		actualChecksum, err := r.calculateChecksum(entry.FilePath)
		if err != nil || actualChecksum != metadata.Checksum {
			zlog.Error().
				Str("filepath", entry.FilePath).
				Str("key", entry.Value.MetadataKey).
				Uint32("expected_checksum", metadata.Checksum).
				Uint32("actual_checksum", actualChecksum).
				Err(err).
				Msg("files.recovery: file corrupted (checksum mismatch)")
			result.Status = StatusCorrupted
			result.MetadataKey = entry.Value.MetadataKey
			result.Error = err
			return result
		}
	}

	// File is valid
	zlog.Debug().
		Str("filepath", entry.FilePath).
		Msg("files.recovery: file validated successfully")
	result.Status = StatusValid
	return result
}

// calculateChecksum computes CRC32 checksum of a file
func (r *RecoveryManager) calculateChecksum(filepath string) (uint32, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	hash := crc32.NewIEEE()
	if _, err := io.Copy(hash, file); err != nil {
		return 0, err
	}

	return hash.Sum32(), nil
}

// performCleanup removes sync entries and deletes corrupted files
func (r *RecoveryManager) performCleanup(results []*ValidationResult) error {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for _, result := range results {
		// Always remove sync entry
		batch.Delete(result.SyncKey)

		switch result.Status {
		case StatusCorrupted, StatusMissing:
			// Delete corrupted/missing file
			if result.FilePath != "" && result.Status == StatusCorrupted {
				if err := os.Remove(result.FilePath); err != nil && !os.IsNotExist(err) {
					zlog.Error().
						Str("filepath", result.FilePath).
						Err(err).
						Msg("files.recovery: failed to delete corrupted file")
				} else {
					zlog.Info().
						Str("filepath", result.FilePath).
						Msg("files.recovery: deleted corrupted file")
				}
			}

			// Delete metadata
			if result.MetadataKey != "" {
				batch.Delete([]byte(result.MetadataKey))

				// Extract user key from metadata key for compaction index
				userKey := extractUserKey(result.MetadataKey)
				if userKey != "" {
					compactionKey := makeCompactionKey(userKey)
					batch.Delete(compactionKey)
				}
			}

		case StatusOrphaned:
			// Optionally delete orphaned file
			if result.FilePath != "" {
				if err := os.Remove(result.FilePath); err != nil && !os.IsNotExist(err) {
					zlog.Debug().
						Str("filepath", result.FilePath).
						Err(err).
						Msg("files.recovery: failed to delete orphaned file")
				}
			}
		}
	}

	if err := r.meta.Handle().Write(wo, batch); err != nil {
		return fmt.Errorf("failed to write cleanup batch: %w", err)
	}

	return nil
}

// calculateStats aggregates validation results into statistics
func (r *RecoveryManager) calculateStats(results []*ValidationResult, duration time.Duration) *RecoveryStats {
	stats := &RecoveryStats{
		Total:    len(results),
		Duration: duration,
	}

	for _, result := range results {
		switch result.Status {
		case StatusValid:
			stats.Valid++
		case StatusCorrupted:
			stats.Corrupted++
		case StatusStale:
			stats.Stale++
		case StatusOrphaned:
			stats.Orphaned++
		case StatusMissing:
			stats.Missing++
		}
	}

	return stats
}

// Helper functions

func extractUserKey(metadataKey string) string {
	// Remove metadata prefix to get user key
	prefix := keys.MetadataPrefix
	if len(metadataKey) > len(prefix) {
		return metadataKey[len(prefix):]
	}
	return ""
}

func makeCompactionKey(userKey string) []byte {
	// This should match the compaction key format used elsewhere
	return []byte(fmt.Sprintf("!compact/%s", userKey))
}
