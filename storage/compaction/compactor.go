// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/deletion"
	"github.com/tigrisdata/ocache/storage/fd"
	"github.com/tigrisdata/ocache/storage/files"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/merge"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"github.com/tigrisdata/ocache/storage/utils"

	"google.golang.org/protobuf/proto"
)

// Compactor is responsible for migrating files referenced in RocksDB into
// proper segments managed by the Segment Manager. A Compactor operates on a
// FileManager + Segment Manager pair but is otherwise stateless, so callers
// are free to create a new instance for every compaction run.
//
// Callers should create a Compactor and invoke Compact(). The method is safe to
// call from multiple goroutines concurrently – each invocation creates its own
// RocksDB iterator and write-batch.
//
// The method is best-effort and does not propagate errors back to the caller to
// avoid interfering with the foreground workload.
//
//     comp := compaction.NewCompactor(rw, sm)
//     comp.Compact(maxBytes, flushBytes)
//
// NOTE: At present the implementation only migrates files; segment-level
// compaction (merging/deleting) lives in Manager.compactSegments().

const (
	// compactorCallerID is the caller ID for the compactor.
	compactorCallerID = "compactor"
)

// CompactorConfig contains configuration for the compactor
type CompactorConfig struct {
	MetaDB            *metadata.MetaDB
	FileManager       *files.FileManager
	SegmentManager    *segment.Manager
	DeletionQueue     *deletion.Queue
	CompactionThreads int // Number of compaction threads
	// Recompaction settings (optional)
	EnableRecompaction   bool
	FragThreshold        float64
	MinSegmentAge        time.Duration
	MinSegments          int
	RecompactionInterval time.Duration
}

// ErrFileSizeMismatch is returned when a file's actual size doesn't match its metadata
type ErrFileSizeMismatch struct {
	Key          string
	FilePath     string
	ActualSize   int64
	ExpectedSize int64
}

func (e *ErrFileSizeMismatch) Error() string {
	return fmt.Sprintf("file size mismatch for key %s: actual=%d expected=%d", e.Key, e.ActualSize, e.ExpectedSize)
}

type Compactor struct {
	fm                   *files.FileManager
	sm                   *segment.Manager
	meta                 *metadata.MetaDB
	fdCache              *fd.FdCache
	deletionQueue        *deletion.Queue
	compactionThreads    int
	recompactor          *SegmentRecompactor
	recompactionInterval time.Duration

	// background loop coordination
	cancel context.CancelFunc
	ctx    context.Context
	wg     sync.WaitGroup
}

// NewCompactorWithConfig creates a new compactor with configuration
func NewCompactorWithConfig(cfg *CompactorConfig) *Compactor {
	ctx, cancel := context.WithCancel(context.Background())

	// Default to 1 thread if not specified
	threads := cfg.CompactionThreads
	if threads <= 0 {
		threads = 1
	}

	c := &Compactor{
		meta:                 cfg.MetaDB,
		fm:                   cfg.FileManager,
		sm:                   cfg.SegmentManager,
		fdCache:              fd.GetFdCache(),
		deletionQueue:        cfg.DeletionQueue,
		compactionThreads:    threads,
		ctx:                  ctx,
		cancel:               cancel,
		recompactionInterval: cfg.RecompactionInterval,
	}

	// Set up recompactor if configured
	if cfg.EnableRecompaction && cfg.FragThreshold > 0 {
		c.recompactor = NewSegmentRecompactor(cfg.MetaDB, cfg.SegmentManager, cfg.DeletionQueue, cfg.FragThreshold, cfg.MinSegmentAge, cfg.MinSegments)
	}

	return c
}

// Start launches background goroutines for file and segment compaction
func (c *Compactor) Start() {
	// Start multiple file compaction workers
	for i := 0; i < c.compactionThreads; i++ {
		c.wg.Add(1)
		go c.fileCompactionLoop(i)
	}

	zlog.Info().Int("threads", c.compactionThreads).Msg("compactor: started file compaction workers")

	// Start segment recompaction loop if enabled
	if c.recompactor != nil && c.recompactor.fragThreshold > 0 {
		c.wg.Add(1)
		go c.segmentRecompactionLoop()
	}
}

// Close stops the background compaction loop and waits for it to exit.
func (c *Compactor) Close() {
	if c == nil {
		return
	}

	if c.cancel != nil {
		c.cancel()
		c.wg.Wait()
		c.cancel = nil
		zlog.Info().Msg("compactor: shutdown completed")
	}
}

// fileCompactionLoop continuously processes file compaction until Close is called.
// Workers continuously scan the compaction index and process entries as they appear.
// When no work is available, workers sleep briefly to avoid tight looping.
func (c *Compactor) fileCompactionLoop(workerID int) {
	defer c.wg.Done()

	zlog.Info().Int("worker", workerID).Msg("compactor: starting file compaction worker")

	lastProcessed := int64(0)
	lastBytesCopied := int64(0)

	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			zlog.Info().Int("worker", workerID).Msg("compactor: file compaction worker stopping")
			return
		case <-logTicker.C:
			if lastProcessed > 0 && lastBytesCopied > 0 {
				zlog.Info().Int("worker", workerID).Int64("processed", lastProcessed).Int64("bytesCopied", lastBytesCopied).Msg("compactor: file compaction worker progress")
				lastProcessed = 0
				lastBytesCopied = 0
			}
		default:
			// Check if context is already cancelled before starting compaction
			if c.ctx.Err() != nil {
				return
			}

			// Process available compaction entries (no byte limit - compact until segment is full or index is empty)
			processed, bytesCopied := c.CompactFiles(c.ctx, workerID)
			lastProcessed += int64(processed)
			lastBytesCopied += bytesCopied

			// If no work was done, sleep briefly to avoid tight loop
			if processed == 0 {
				select {
				case <-time.After(100 * time.Millisecond):
				case <-c.ctx.Done():
					return
				}
			}
		}
	}
}

// segmentRecompactionLoop triggers segment recompaction on a timer until Close is called.
func (c *Compactor) segmentRecompactionLoop() {
	defer c.wg.Done()

	zlog.Info().Msg("compactor: starting segment recompaction loop")

	ticker := time.NewTicker(c.recompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if context is already cancelled before starting recompaction
			if c.ctx.Err() != nil {
				return
			}
			if err := c.recompactor.RecompactFragmentedSegments(c.ctx); err != nil {
				if err != context.Canceled {
					zlog.Error().Err(err).Msg("compactor: segment recompaction failed")
				}
			}
		case <-c.ctx.Done():
			zlog.Info().Msg("compactor: segment recompaction loop stopping")
			return
		}
	}
}

// PrepareEntryForCompaction prepares the key and value to store in compaction index.
func PrepareEntryForCompaction(key, filePath string) ([]byte, []byte) {
	ts := time.Now().UnixNano()
	idxKey := keys.MakeCompactionKey(ts, key)
	idxVal := []byte(filePath)

	return idxKey, idxVal
}

// CompactFiles scans the RocksDB file-index and migrates files into segments.
// The context parameter allows for cancellation of the compaction process.
// The workerID is used to partition work across multiple workers.
// Compaction continues until the current segment is full or there are no more entries.
// Returns the number of files processed.
func (c *Compactor) CompactFiles(ctx context.Context, workerID int) (int, int64) {
	start := time.Now()

	// Track compaction run
	metrics.CompactionRuns.Inc()

	// RocksDB iterator setup
	ro := metadata.CreateReadOptions(true, false)
	it := c.meta.Handle().NewIterator(ro)
	defer it.Close()

	wb := grocksdb.NewWriteBatch()
	var (
		processed   int
		bytesCopied int64
	)

	// Create a unique caller ID for this worker
	workerCallerID := fmt.Sprintf("%s-%d", compactorCallerID, workerID)

	// Acquire the initial open segment with reservation
	seg, err := c.sm.AcquireOpenSegmentWithReservation(workerCallerID, 0)
	if err != nil {
		zlog.Error().Err(err).Int("worker", workerID).Msg("compactor: acquire open segment")
		return 0, 0
	}
	// Use a closure to ensure we release the final segment, not the initial one
	defer func() {
		if seg != nil {
			if err := seg.Release(workerCallerID); err != nil {
				zlog.Error().Err(err).Str("callerID", workerCallerID).Msg("failed to release segment")
			}
		}
	}()

	filePrefix := []byte(keys.CompactionIndexPrefix)
	iterationCount := 0
	lastLogTime := time.Now()

	for it.Seek(filePrefix); it.ValidForPrefix(filePrefix); it.Next() {
		iterationCount++

		// Log progress every 100 iterations or every 10 seconds
		if iterationCount%100 == 0 || time.Since(lastLogTime) > 10*time.Second {
			zlog.Debug().
				Int("worker", workerID).
				Int("iterations", iterationCount).
				Int("processed", processed).
				Int64("bytesCopied", bytesCopied).
				Msg("compactor: progress update")
			lastLogTime = time.Now()
		}

		// Check if context is cancelled
		if err := ctx.Err(); err != nil {
			zlog.Debug().Msg("compactor: interrupted by cancellation")
			// Don't attempt to commit when cancelled - just return
			return processed, bytesCopied
		}
		k := it.Key().Data()
		v := it.Value().Data()

		// Process the compaction entry
		userKey, filePath, ok := keys.ParseCompactionIndexRow(k, v)
		if !ok {
			zlog.Error().Str("row", string(k)).Msg("compactor: malformed index row")
			// Malformed index row - remove it
			wb.Delete(k)
			continue
		}

		// Skip entries not assigned to this worker (hash-based partitioning)
		if c.compactionThreads > 1 {
			// Use simple hash of user key to distribute work
			hash := utils.HashString(userKey)
			if int(hash%uint32(c.compactionThreads)) != workerID {
				continue
			}
		}

		// Log what we're about to process
		if iterationCount <= 10 || iterationCount%100 == 0 {
			zlog.Debug().
				Int("worker", workerID).
				Int("iteration", iterationCount).
				Str("userKey", userKey).
				Str("filePath", filePath).
				Msg("compactor: processing entry")
		}

		entry, err := c.processCompactionEntry(userKey, filePath)
		if err != nil {
			// Handle different error cases using error types
			switch {
			case errors.Is(err, utils.ErrMetadataNotFound):
				// Key already gone – queue file for deletion and remove index
				wb.Delete(k)
				if err := c.deletionQueue.Add(filePath); err != nil {
					zlog.Error().Err(err).Str("path", filePath).Msg("compactor: failed to queue file for deletion")
				}
				continue
			case errors.Is(err, utils.ErrAlreadyCompacted):
				// Remove compaction index entry
				// Note: don't delete the file since it might be referenced elsewhere
				wb.Delete(k)
				continue
			case errors.Is(err, utils.ErrNotRawFile), errors.Is(err, utils.ErrFilePathMismatch):
				// Stale entry - remove index and queue file for deletion
				wb.Delete(k)
				if err := c.deletionQueue.Add(filePath); err != nil {
					zlog.Error().Err(err).Str("path", filePath).Msg("compactor: failed to queue file for deletion")
				}
				continue
			case errors.Is(err, utils.ErrMalformedIndexRow):
				// Malformed index row - remove it
				wb.Delete(k)
				continue
			case errors.Is(err, utils.ErrFileNotExist):
				// File doesn't exist - remove the stale index row AND tombstone
				// the now-dangling metadata. Without the latter the metadata is
				// orphaned (still RAW_FILE -> a missing file) and, because
				// startup recovery only scans the compaction index, never
				// reconciled, so every Get of the key fails forever (#152).
				wb.Delete(k)
				c.purgeDanglingMeta(wb, userKey, filePath)
				continue
			default:
				// Other errors - just continue
				continue
			}
		}

		// Compact the entry
		if err := c.compactEntry(ctx, entry, &seg, workerCallerID, wb); err != nil {
			if err == context.Canceled {
				zlog.Debug().Msg("compactor: compaction cancelled")
				return processed, bytesCopied
			}
			// Check if it's a file size mismatch error
			var sizeMismatchErr *ErrFileSizeMismatch
			if errors.As(err, &sizeMismatchErr) {
				// Skip corrupted file - queue it for deletion AND tombstone the
				// metadata that references it. Like the ErrFileNotExist branch,
				// this would otherwise leave the metadata orphaned (still
				// RAW_FILE -> a file the compactor is about to delete) and
				// unreachable by startup recovery, so every Get of the key fails
				// indefinitely (#152). Mirrors recovery's StatusCorrupted, which
				// deletes both the file and the metadata.
				wb.Delete(k)
				c.purgeDanglingMeta(wb, entry.userKey, entry.filePath)
				if err := c.deletionQueue.Add(entry.filePath); err != nil {
					zlog.Error().Err(err).Str("path", entry.filePath).Msg("compactor: failed to queue corrupted file for deletion")
				}
				zlog.Warn().
					Str("key", sizeMismatchErr.Key).
					Str("file", sizeMismatchErr.FilePath).
					Int64("actualSize", sizeMismatchErr.ActualSize).
					Int64("expectedSize", sizeMismatchErr.ExpectedSize).
					Msg("compactor: skipping corrupted file")
				continue
			}
			// Log other errors and continue with next entry
			continue
		}

		// Housekeeping - queue successfully compacted file for deletion and cleanup indexes
		wb.Delete(k) // remove compaction index row

		if err := c.deletionQueue.Add(entry.filePath); err != nil {
			zlog.Error().Err(err).Str("path", entry.filePath).Msg("compactor: failed to queue compacted file for deletion")
		}

		processed++
		bytesCopied += entry.fileInfo.Size()
	}

	// Final commit.
	if err := c.commit(ctx, seg, wb); err != nil {
		if err != context.Canceled {
			zlog.Error().Err(err).Msg("compactor: commit failed")
		}
		return processed, bytesCopied
	}

	// Record compaction metrics
	duration := time.Since(start)
	metrics.CompactionDuration.Observe(float64(duration.Milliseconds()))
	metrics.CompactionFilesCompacted.Add(float64(processed))
	metrics.CompactionBytesCompacted.Add(float64(bytesCopied))

	return processed, bytesCopied
}

// compactionEntry represents a single entry to be compacted
type compactionEntry struct {
	userKey  string
	filePath string
	fileInfo os.FileInfo
	metadata *pb.ValueMessage
}

// processCompactionEntry processes a single compaction index entry
func (c *Compactor) processCompactionEntry(userKey, filePath string) (*compactionEntry, error) {
	// Stat first – cheap, and gives us size quickly.
	fInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, utils.ErrFileNotExist
		}
		zlog.Error().Err(err).Str("path", filePath).Msg("compactor: stat failed")
		return nil, err
	}

	// Load and validate metadata
	meta, err := c.loadAndValidateMetadata(userKey, filePath)
	if err != nil {
		return nil, err
	}

	return &compactionEntry{
		userKey:  userKey,
		filePath: filePath,
		fileInfo: fInfo,
		metadata: meta,
	}, nil
}

// loadAndValidateMetadata loads and validates metadata for a key
func (c *Compactor) loadAndValidateMetadata(userKey, filePath string) (*pb.ValueMessage, error) {
	metaKey := keys.MakeMetadataKey(userKey)

	// Load metadata first
	meta, err := utils.GetMetadata(c.meta, string(metaKey))
	if err != nil {
		// If metadata not found, return the specific error
		if errors.Is(err, utils.ErrMetadataNotFound) {
			return nil, utils.ErrMetadataNotFound
		}
		zlog.Error().Err(err).Str("key", userKey).Msg("compactor: bad metadata")
		return nil, err
	}

	// Validate the file entry
	if err := utils.ValidateFileEntry(meta, filePath, "compactor", userKey); err != nil {
		return nil, err
	}

	return meta, nil
}

// purgeDanglingMeta stages a conditional tombstone of userKey's metadata when it
// still references filePath. It is used when the compactor drops a
// compaction-index row for a file it found missing or corrupt: without it the
// metadata is left dangling (still RAW_FILE -> a file that no longer exists) and,
// because startup recovery only scans the compaction index, never reconciled, so
// every Get of the key fails for the life of the process (#152).
//
// The tombstone is a conditional merge (see merge.MakeRawFilePurgeOperand /
// merge.mergeMetadataCAS): it applies only when the metadata still references
// filePath, so a concurrent Put (which writes a fresh path) or a prior
// RAW_FILE->SEGMENT migration is never clobbered. ENOENT is treated as
// permanent; a transient absence (e.g. a flapping network mount) would evict an
// otherwise-healthy entry, but the cost is a cache miss + upstream re-fetch, not
// data corruption, and local-disk ENOENT is reliably permanent.
func (c *Compactor) purgeDanglingMeta(wb *grocksdb.WriteBatch, userKey, filePath string) {
	if filePath == "" {
		// No path to match against, so the conditional purge could not identify
		// the dangling entry anyway; skip rather than emit a no-op operand.
		return
	}

	metaKey := keys.MakeMetadataKey(userKey)

	// Only purge when a metadata row actually exists. The ErrFileNotExist caller
	// reaches here straight from os.Stat without loading metadata, so an index
	// row with no metadata (e.g. a key already Deleted whose compaction-index
	// row lingers) would otherwise stage a purge merge with no base — and
	// mergeMetadataCAS's no-base path emits the expired sentinel, materializing
	// a metadata key that never existed. Absence is already the desired state,
	// so skip. (A Delete that races in after this check still falls into the
	// benign sentinel path, which the cleaner sweeps.)
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	slice, err := c.meta.Handle().Get(ro, metaKey)
	if err != nil {
		zlog.Error().Err(err).Str("key", userKey).Msg("compactor: failed to read metadata for dangling purge")
		return
	}
	exists := slice.Exists()
	slice.Free()
	if !exists {
		return
	}

	operand, err := merge.MakeRawFilePurgeOperand(filePath)
	if err != nil {
		zlog.Error().Err(err).Str("key", userKey).Msg("compactor: failed to build dangling-metadata purge operand")
		return
	}
	wb.Merge(metaKey, operand)
}

// compactEntry performs the actual compaction of a single entry
func (c *Compactor) compactEntry(ctx context.Context, entry *compactionEntry, seg **segment.Segment, callerID string, wb *grocksdb.WriteBatch) error {
	// Validate file size matches metadata
	if entry.fileInfo.Size() != entry.metadata.ValueLength {
		zlog.Error().
			Str("key", entry.userKey).
			Str("filePath", entry.filePath).
			Int64("actualSize", entry.fileInfo.Size()).
			Int64("expectedSize", entry.metadata.ValueLength).
			Msg("compactor: file size mismatch - possible corruption")
		// Return error to skip this file
		return &ErrFileSizeMismatch{
			Key:          entry.userKey,
			FilePath:     entry.filePath,
			ActualSize:   entry.fileInfo.Size(),
			ExpectedSize: entry.metadata.ValueLength,
		}
	}

	// Calculate total space needed: header + value
	headerSize := segment.CalculateValueHeaderSize(entry.userKey)
	totalNeeded := headerSize + entry.metadata.ValueLength

	// Ensure we have space in the current segment
	if err := c.ensureCapacity(ctx, seg, callerID, totalNeeded); err != nil {
		return err
	}

	// Copy the raw file into the segment
	zlog.Debug().
		Str("key", entry.userKey).
		Str("filePath", entry.filePath).
		Int64("size", entry.fileInfo.Size()).
		Msg("compactor: opening file for compaction")

	f, err := os.Open(entry.filePath)
	if err != nil {
		zlog.Error().Err(err).Str("path", entry.filePath).Msg("compactor: open failed")
		return err
	}
	defer f.Close()

	zlog.Debug().
		Str("key", entry.userKey).
		Msg("compactor: copying file to segment")

	if err := c.copyFileIntoSegment(ctx, *seg, entry.userKey, f, entry.metadata); err != nil {
		if err == context.Canceled {
			return err
		}
		zlog.Error().Err(err).Str("key", entry.userKey).Msg("compactor: copy failed")
		return err
	}

	zlog.Debug().
		Str("key", entry.userKey).
		Msg("compactor: successfully copied file to segment")

	// Stage the metadata rewrite as a conditional merge rather than an
	// unconditional Put so we don't silently clobber a concurrent
	// storage.Put that replaced the raw file between our read of meta and
	// this commit. The operand is a SEGMENT-typed ValueMessage whose
	// RawFilePath field is overloaded to carry the CAS precondition (the
	// raw-file path we observed when we started migrating this entry).
	// See merge.mergeMetadataCAS for the precondition semantics; on a
	// mismatch the operand is dropped and the concurrent Put wins, with
	// the segment bytes we wrote becoming dead space reclaimable by the
	// segment recompactor.
	operandBytes, _ := proto.Marshal(entry.metadata)
	metaKey := keys.MakeMetadataKey(entry.userKey)
	wb.Merge(metaKey, operandBytes)

	return nil
}

// copyFileIntoSegment copies the file into an open segment using the
// existing segment pipeline and updates the ValueMessage.
func (c *Compactor) copyFileIntoSegment(ctx context.Context, seg *segment.Segment, userKey string, f *os.File, vm *pb.ValueMessage) error {
	// Check if context is cancelled before I/O operation
	if err := ctx.Err(); err != nil {
		return err
	}

	segOff, err := seg.WriteEntry(userKey, f, vm)
	if err != nil {
		return err
	}

	// NOTE: vm.RawFilePath is deliberately NOT cleared here. The caller
	// marshals vm as the operand bytes for a conditional-merge write
	// (see compactEntry and merge.mergeMetadataCAS), and that merge needs
	// the original raw-file path as its CAS precondition. The merge
	// operator clears RawFilePath before persisting the resolved SEGMENT
	// value, preserving the invariant that stored SEGMENT ValueMessages
	// carry no RawFilePath.
	vm.SegmentPath = seg.Path()
	vm.SegmentOffset = segOff
	vm.ValueType = pb.ValueType_SEGMENT

	return nil
}

// commit commits pending RocksDB mutations.
func (c *Compactor) commit(ctx context.Context, seg *segment.Segment, wb *grocksdb.WriteBatch) error {
	if wb.Count() == 0 {
		return nil // nothing to do
	}

	// Check if context is cancelled
	if err := ctx.Err(); err != nil {
		zlog.Info().Msg("compactor: commit skipped due to cancellation")
		// Return immediately without writing to avoid partial state
		return err
	}

	// Persist segment first so that metadata can safely reference it.
	if err := seg.Sync(); err != nil {
		return err
	}

	if err := c.meta.Handle().Write(grocksdb.NewDefaultWriteOptions(), wb); err != nil {
		return err
	}

	return nil
}

// ensureCapacity ensures that the segment has at least the needed bytes
// available, finalising and acquiring a fresh segment when necessary.
func (c *Compactor) ensureCapacity(ctx context.Context, seg **segment.Segment, callerID string, needed int64) error {
	if (*seg).Remaining() >= needed {
		return nil
	}

	// Check if context is cancelled before finalizing
	if err := ctx.Err(); err != nil {
		return err
	}

	// Finalize the segment first, then release it
	// This prevents other threads from acquiring it while it's being finalized
	if err := c.sm.FinalizeSegment(*seg); err != nil {
		return err
	}

	// Now safe to release since it's finalized
	if err := (*seg).Release(callerID); err != nil {
		zlog.Error().Err(err).Str("callerID", callerID).Msg("failed to release segment after finalization")
	}

	newSeg, err := c.sm.AcquireOpenSegmentWithReservation(callerID, 0)
	if err != nil {
		return err
	}
	*seg = newSeg
	return nil
}
