package deletion

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/server/storage/fd"
	"github.com/tigrisdata/ocache/server/storage/metadata"
)

const (
	// DeletionQueuePrefix is the RocksDB key prefix for deletion queue entries
	DeletionQueuePrefix = "!del/"

	// DefaultBatchSize is the default number of deletions to process per batch
	DefaultBatchSize = 100

	// DefaultProcessInterval is the default interval between batch processing
	DefaultProcessInterval = time.Second

	// DefaultPruneAge is the default age after which queue entries are pruned
	DefaultPruneAge = 24 * time.Hour
)

// Config holds configuration for the deletion queue
type Config struct {
	BatchSize       int           // Number of deletions per batch
	ProcessInterval time.Duration // Interval between batch processing
	PruneAge        time.Duration // Age after which entries are pruned
}

// DefaultConfig returns default configuration
func DefaultConfig() Config {
	return Config{
		BatchSize:       DefaultBatchSize,
		ProcessInterval: DefaultProcessInterval,
		PruneAge:        DefaultPruneAge,
	}
}

// Queue manages centralized file deletion
type Queue struct {
	meta   *metadata.MetaDB
	config Config

	// Background processing
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	processed  int64
	failed     int64
	pruned     int64
	queueDepth int64
}

// NewQueue creates a new deletion queue
func NewQueue(meta *metadata.MetaDB, config Config) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{
		meta:   meta,
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins background processing
func (q *Queue) Start() {
	q.wg.Add(1)
	go q.processingLoop()
	zlog.Info().
		Int("batch_size", q.config.BatchSize).
		Dur("interval", q.config.ProcessInterval).
		Dur("prune_age", q.config.PruneAge).
		Msg("deletion queue: started")
}

// Stop gracefully stops the queue
func (q *Queue) Stop() {
	zlog.Info().Msg("deletion queue: stopping")
	q.cancel()
	q.wg.Wait()
	zlog.Info().
		Int64("processed", q.processed).
		Int64("failed", q.failed).
		Int64("pruned", q.pruned).
		Msg("deletion queue: stopped")
}

// Add adds a file to the deletion queue
func (q *Queue) Add(filepath string) error {
	if filepath == "" {
		return fmt.Errorf("empty filepath")
	}

	key := q.makeQueueKey(time.Now().UnixNano(), filepath)
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	err := q.meta.Handle().Put(wo, key, []byte{0x01})
	if err != nil {
		zlog.Error().
			Str("filepath", filepath).
			Err(err).
			Msg("deletion queue: failed to add entry")
		return err
	}

	zlog.Debug().
		Str("filepath", filepath).
		Msg("deletion queue: added entry")
	return nil
}

// processingLoop runs the background processing
func (q *Queue) processingLoop() {
	defer q.wg.Done()

	ticker := time.NewTicker(q.config.ProcessInterval)
	defer ticker.Stop()

	// Prune old entries periodically (every hour)
	pruneTicker := time.NewTicker(time.Hour)
	defer pruneTicker.Stop()

	for {
		select {
		case <-ticker.C:
			q.ProcessBatch()
		case <-pruneTicker.C:
			q.pruneOldEntries()
		case <-q.ctx.Done():
			return
		}
	}
}

// ProcessBatch processes a batch of deletion requests
func (q *Queue) ProcessBatch() {
	startTime := time.Now()
	seen := make(map[string][]byte) // filepath -> earliest queue key

	// Scan and deduplicate
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(DeletionQueuePrefix)
	count := 0

	for it.Seek(prefix); it.ValidForPrefix(prefix) && count < q.config.BatchSize; it.Next() {
		// Check for shutdown
		select {
		case <-q.ctx.Done():
			return
		default:
		}

		key := it.Key()
		keyData := key.Data()

		// Extract filepath from key: !del/<timestamp>/<filepath>
		filepath := q.extractFilepath(keyData)
		if filepath == "" {
			key.Free()
			it.Value().Free()
			continue
		}

		// Keep only earliest entry for each filepath
		if _, exists := seen[filepath]; !exists {
			seen[filepath] = bytes.Clone(keyData)
			count++
		}

		key.Free()
		it.Value().Free()
	}

	if len(seen) == 0 {
		return
	}

	// Attempt deletions
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	successful := 0
	failed := 0

	for filepath, queueKey := range seen {
		if q.tryDelete(filepath) {
			batch.Delete(queueKey)
			successful++
			q.processed++
		} else {
			failed++
			q.failed++
		}
	}

	// Commit successful deletions
	if batch.Count() > 0 {
		if err := q.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().
				Err(err).
				Msg("deletion queue: failed to commit batch")
		}
	}

	if successful > 0 || failed > 0 {
		zlog.Info().
			Int("successful", successful).
			Int("failed", failed).
			Dur("duration", time.Since(startTime)).
			Msg("deletion queue: processed batch")
	}
}

// tryDelete attempts to delete a file
func (q *Queue) tryDelete(filepath string) bool {
	lockManager := fd.GetFileLockManager()
	lock := lockManager.GetFileLock(filepath)

	// Try to acquire lock without blocking
	if !lock.TryLock() {
		zlog.Debug().
			Str("filepath", filepath).
			Msg("deletion queue: file locked, will retry")
		return false
	}
	defer lock.Unlock()

	err := os.Remove(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			// File already deleted, consider it successful
			zlog.Debug().
				Str("filepath", filepath).
				Msg("deletion queue: file already deleted")
			return true
		}
		zlog.Error().
			Str("filepath", filepath).
			Err(err).
			Msg("deletion queue: failed to delete file")
		return false
	}

	// Remove lock from manager after successful deletion
	lockManager.RemoveFileLock(filepath)

	zlog.Debug().
		Str("filepath", filepath).
		Msg("deletion queue: deleted file")
	return true
}

// pruneOldEntries removes queue entries older than PruneAge
func (q *Queue) pruneOldEntries() {
	startTime := time.Now()
	cutoff := time.Now().Add(-q.config.PruneAge).UnixNano()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	prefix := []byte(DeletionQueuePrefix)
	pruned := 0

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		// Check for shutdown
		select {
		case <-q.ctx.Done():
			return
		default:
		}

		key := it.Key()
		keyData := key.Data()

		// Extract timestamp from key
		timestamp := q.extractTimestamp(keyData)
		if timestamp > 0 && timestamp < cutoff {
			batch.Delete(bytes.Clone(keyData))
			pruned++
			q.pruned++

			filepath := q.extractFilepath(keyData)
			zlog.Warn().
				Str("filepath", filepath).
				Dur("age", time.Since(time.Unix(0, timestamp))).
				Msg("deletion queue: pruning old entry")
		}

		key.Free()
		it.Value().Free()

		// Commit batch periodically
		if batch.Count() >= 100 {
			if err := q.meta.Handle().Write(wo, batch); err != nil {
				zlog.Error().
					Err(err).
					Msg("deletion queue: failed to prune batch")
			}
			batch.Clear()
		}
	}

	// Commit final batch
	if batch.Count() > 0 {
		if err := q.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().
				Err(err).
				Msg("deletion queue: failed to prune final batch")
		}
	}

	if pruned > 0 {
		zlog.Info().
			Int("pruned", pruned).
			Dur("duration", time.Since(startTime)).
			Msg("deletion queue: pruned old entries")
	}
}

// makeQueueKey creates a RocksDB key for the queue
func (q *Queue) makeQueueKey(timestamp int64, filepath string) []byte {
	return []byte(fmt.Sprintf("%s%019d/%s", DeletionQueuePrefix, timestamp, filepath))
}

// extractFilepath extracts the filepath from a queue key
func (q *Queue) extractFilepath(key []byte) string {
	// Key format: !del/<timestamp>/<filepath>
	parts := bytes.SplitN(key, []byte("/"), 3)
	if len(parts) != 3 {
		return ""
	}
	return string(parts[2])
}

// extractTimestamp extracts the timestamp from a queue key
func (q *Queue) extractTimestamp(key []byte) int64 {
	// Key format: !del/<timestamp>/<filepath>
	parts := bytes.SplitN(key, []byte("/"), 3)
	if len(parts) < 2 {
		return 0
	}

	// Parse timestamp (skip prefix)
	var timestamp int64
	fmt.Sscanf(string(parts[1]), "%019d", &timestamp)
	return timestamp
}

// GetQueueDepth returns the current queue depth
func (q *Queue) GetQueueDepth() int64 {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := q.meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := []byte(DeletionQueuePrefix)
	count := int64(0)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
		it.Key().Free()
		it.Value().Free()
	}

	return count
}

