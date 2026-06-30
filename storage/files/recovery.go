package files

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/utils"
)

const (
	// MaxWorkers is the default number of workers to use for validation when a
	// caller does not specify one (numWorkers <= 0).
	MaxWorkers = 16

	// entryBufferSize is the buffer size for the entries channel
	entryBufferSize = 10000

	// resultBufferSize is the buffer size for the results channel
	resultBufferSize = 10000
)

// compactionEntryInfo holds a compaction entry with its parsed components
type compactionEntryInfo struct {
	Key      []byte
	UserKey  string
	FilePath string
}

// RecoveryManager handles startup recovery and validation of files in the compaction index
type RecoveryManager struct {
	meta       *metadata.MetaDB
	filesPath  string
	numWorkers int
}

// NewRecoveryManager creates a new recovery manager. numWorkers sets the number
// of parallel validation workers; values <= 0 fall back to MaxWorkers.
func NewRecoveryManager(meta *metadata.MetaDB, filesPath string, numWorkers int) *RecoveryManager {
	if numWorkers <= 0 {
		numWorkers = MaxWorkers
	}
	return &RecoveryManager{
		meta:       meta,
		filesPath:  filesPath,
		numWorkers: numWorkers,
	}
}

// RecoverOnStartup validates all files in the compaction index and cleans up corrupted files
func (r *RecoveryManager) RecoverOnStartup() error {
	zlog.Info().Msg("files.recovery: starting startup recovery")
	startTime := time.Now()

	// Increment recovery runs counter
	metrics.RecoveryRuns.Inc()

	var stats *RecoveryStats
	var err error

	zlog.Info().Msg("files.recovery: using streaming approach")
	stats, err = r.processEntriesStreaming()
	if err != nil {
		return fmt.Errorf("streaming recovery failed: %w", err)
	}

	if stats == nil {
		stats = &RecoveryStats{}
	}

	stats.Duration = time.Since(startTime)

	// Record recovery duration in milliseconds
	metrics.RecoveryDuration.Observe(float64(stats.Duration.Milliseconds()))

	// Record recovered keys metric (valid entries that were recovered)
	metrics.RecoveryKeysRecovered.Add(float64(stats.Valid))

	zlog.Info().
		Int("total", stats.Total).
		Int("valid", stats.Valid).
		Int("corrupted", stats.Corrupted).
		Int("stale", stats.Stale).
		Int("orphaned", stats.Orphaned).
		Int("missing", stats.Missing).
		Dur("duration_ms", stats.Duration).
		Msg("files.recovery: completed")

	return nil
}

// processEntriesStreaming processes compaction entries in a streaming fashion without loading all into memory
func (r *RecoveryManager) processEntriesStreaming() (*RecoveryStats, error) {
	proc := &streamingProcessor{
		r:       r,
		entries: make(chan *compactionEntryInfo, entryBufferSize),
		results: make(chan *ValidationResult, resultBufferSize),
		stats:   &RecoveryStats{},
		errChan: make(chan error, 1),
	}

	// Start validation workers
	var workerWg sync.WaitGroup
	for i := 0; i < r.numWorkers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			proc.validationWorker()
		}()
	}

	// Start results collector in background
	collectorDone := make(chan struct{})
	go func() {
		proc.resultsCollector()
		close(collectorDone)
	}()

	// Stream entries from RocksDB
	if err := proc.streamEntries(); err != nil {
		close(proc.entries)
		workerWg.Wait()
		close(proc.results)
		<-collectorDone
		return nil, err
	}

	// Close entries channel to signal workers to stop
	close(proc.entries)

	// Wait for all validation workers to finish
	workerWg.Wait()

	// Close results channel after all workers are done
	close(proc.results)

	// Wait for results collector to finish
	<-collectorDone

	// Check for any errors from workers
	select {
	case err := <-proc.errChan:
		return proc.stats, err
	default:
		return proc.stats, nil
	}
}

// validateEntry validates a single compaction entry
func (r *RecoveryManager) validateEntry(entry *compactionEntryInfo) *ValidationResult {
	result := &ValidationResult{
		SyncKey:  entry.Key, // Actually compaction key, but reusing field name
		FilePath: entry.FilePath,
	}

	// Construct metadata key from user key
	metadataKey := string(keys.MakeMetadataKey(entry.UserKey))

	// Fetch metadata
	metadata, err := utils.GetMetadata(r.meta, metadataKey)
	if err != nil {
		// Check if it's specifically metadata not found
		if errors.Is(err, utils.ErrMetadataNotFound) {
			zlog.Warn().
				Str("filepath", entry.FilePath).
				Str("userKey", entry.UserKey).
				Str("metadata_key", metadataKey).
				Msg("files.recovery: orphaned compaction entry (metadata not found)")
			result.Status = StatusOrphaned
			return result
		}
		zlog.Warn().
			Str("filepath", entry.FilePath).
			Str("userKey", entry.UserKey).
			Err(err).
			Msg("files.recovery: failed to fetch metadata")
		result.Status = StatusCorrupted
		result.Error = err
		return result
	}

	// Check if metadata still points to this file
	if metadata.ValueType != pb.ValueType_RAW_FILE {
		zlog.Debug().
			Str("filepath", entry.FilePath).
			Str("userKey", entry.UserKey).
			Str("value_type", metadata.ValueType.String()).
			Msg("files.recovery: stale compaction entry (not raw file)")
		result.Status = StatusStale
		return result
	}

	if metadata.RawFilePath != entry.FilePath {
		zlog.Debug().
			Str("old_file", entry.FilePath).
			Str("new_file", metadata.RawFilePath).
			Str("userKey", entry.UserKey).
			Msg("files.recovery: stale compaction entry (metadata updated)")
		result.Status = StatusStale
		return result
	}

	// Validate physical file
	stat, err := os.Stat(entry.FilePath)
	if err != nil {
		zlog.Warn().
			Str("filepath", entry.FilePath).
			Str("userKey", entry.UserKey).
			Str("key", metadataKey).
			Err(err).
			Msg("files.recovery: file missing")
		result.Status = StatusMissing
		result.MetadataKey = metadataKey
		result.Error = err
		return result
	}

	// Validate size
	if stat.Size() != metadata.ValueLength {
		zlog.Error().
			Str("filepath", entry.FilePath).
			Str("userKey", entry.UserKey).
			Str("key", metadataKey).
			Int64("expected_size", metadata.ValueLength).
			Int64("actual_size", stat.Size()).
			Msg("files.recovery: file corrupted (size mismatch)")
		result.Status = StatusCorrupted
		result.MetadataKey = metadataKey
		return result
	}

	// File is valid
	zlog.Debug().
		Str("filepath", entry.FilePath).
		Str("userKey", entry.UserKey).
		Msg("files.recovery: file validated successfully")
	result.Status = StatusValid
	return result
}

// streamingProcessor handles streaming processing of compaction entries
type streamingProcessor struct {
	r       *RecoveryManager
	entries chan *compactionEntryInfo
	results chan *ValidationResult
	stats   *RecoveryStats
	errChan chan error
}

// streamEntries reads compaction entries from RocksDB and sends them to the processing channel
func (proc *streamingProcessor) streamEntries() error {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := proc.r.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(keys.CompactionIndexPrefix)
	entriesStreamed := 0

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		value := it.Value()

		// Parse key to get userKey and filepath
		userKey, filepath, ok := keys.ParseCompactionIndexRow(key.Data(), value.Data())
		if !ok {
			zlog.Warn().
				Str("key", string(key.Data())).
				Msg("files.recovery: failed to parse compaction key")
			key.Free()
			value.Free()
			continue
		}

		// Send to processing channel
		proc.entries <- &compactionEntryInfo{
			Key:      bytes.Clone(key.Data()),
			UserKey:  userKey,
			FilePath: filepath,
		}

		key.Free()
		value.Free()
		entriesStreamed++

		// Log progress periodically
		if entriesStreamed%entryBufferSize == 0 {
			zlog.Info().
				Int("streamed", entriesStreamed).
				Msg("files.recovery: streaming progress")
		}
	}

	if err := it.Err(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	zlog.Info().
		Int("entries", entriesStreamed).
		Msg("files.recovery: finished streaming entries")

	return nil
}

// validationWorker processes entries from the channel
func (proc *streamingProcessor) validationWorker() {
	for entry := range proc.entries {
		result := proc.r.validateEntry(entry)
		proc.results <- result
	}
}

// resultsCollector collects validation results and processes deletions
func (proc *streamingProcessor) resultsCollector() {
	batch := make([]*ValidationResult, 0, 100)

	for result := range proc.results {
		// Update stats
		proc.stats.Total++
		switch result.Status {
		case StatusValid:
			proc.stats.Valid++
		case StatusCorrupted:
			proc.stats.Corrupted++
		case StatusStale:
			proc.stats.Stale++
		case StatusOrphaned:
			proc.stats.Orphaned++
		case StatusMissing:
			proc.stats.Missing++
		}

		// Add ALL results to deletion batch
		// After recovery, we remove all compaction entries regardless of status
		// Valid entries mean files are good and can be safely tracked for compaction
		// Invalid entries need cleanup
		batch = append(batch, result)

		// Process batch when it reaches the size limit
		if len(batch) >= 100 {
			if err := proc.processDeletionBatch(batch); err != nil {
				select {
				case proc.errChan <- err:
				default:
				}
			}
			batch = batch[:0] // Reset batch
		}
	}

	// Process any remaining items in the batch
	if len(batch) > 0 {
		if err := proc.processDeletionBatch(batch); err != nil {
			select {
			case proc.errChan <- err:
			default:
			}
		}
	}
}

// processDeletionBatch processes a batch of deletions
func (proc *streamingProcessor) processDeletionBatch(batch []*ValidationResult) error {
	if len(batch) == 0 {
		return nil
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	writeBatch := grocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	for _, result := range batch {
		// Always remove compaction entry
		writeBatch.Delete(result.SyncKey) // SyncKey field repurposed to hold compaction key

		switch result.Status {
		case StatusCorrupted, StatusOrphaned, StatusMissing:
			// Delete corrupted/orphaned file
			if result.FilePath != "" && (result.Status == StatusCorrupted || result.Status == StatusOrphaned) {
				if err := os.Remove(result.FilePath); err != nil && !os.IsNotExist(err) {
					zlog.Error().
						Str("filepath", result.FilePath).
						Err(err).
						Msg("files.recovery: failed to delete corrupted/orphaned file")
				} else if err == nil {
					zlog.Info().
						Str("filepath", result.FilePath).
						Msg("files.recovery: deleted corrupted/orphaned file")
				}
			}

			// Delete metadata for corrupted, orphaned, and missing files
			if result.MetadataKey != "" {
				writeBatch.Delete([]byte(result.MetadataKey))
			}
		}
	}

	if err := proc.r.meta.Handle().Write(wo, writeBatch); err != nil {
		return fmt.Errorf("failed to write deletion batch: %w", err)
	}

	return nil
}
