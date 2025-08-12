package compaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/fd"
	"github.com/tigrisdata/ocache/server/storage/files"
	"github.com/tigrisdata/ocache/server/storage/keys"
	"github.com/tigrisdata/ocache/server/storage/metadata"
	"github.com/tigrisdata/ocache/server/storage/segment"
	"github.com/tigrisdata/ocache/server/utils"

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
	fm       *files.FileManager
	sm       *segment.Manager
	meta     *metadata.MetaDB
	fdCache  *fd.FdCache
	maxBytes int64
	interval time.Duration

	// background loop coordination
	cancel context.CancelFunc
	ctx    context.Context
	wg     sync.WaitGroup
}

// NewCompactor creates a new Compactor bound to the provided FileManager and
// Segment Manager.
func NewCompactor(fm *files.FileManager, sm *segment.Manager, maxBytes int64, interval time.Duration) *Compactor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Compactor{
		fm:       fm,
		sm:       sm,
		meta:     metadata.GetMetaDB(),
		fdCache:  fd.GetFdCache(),
		maxBytes: maxBytes,
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start launches a background goroutine that periodically calls CompactFiles
// at the interval defined by DefaultFileCompactionInterval.
func (c *Compactor) Start() {
	c.wg.Add(1)
	go c.compactionLoop()
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

// compactionLoop triggers file compaction on a timer until Close is called.
func (c *Compactor) compactionLoop() {
	defer c.wg.Done()

	zlog.Info().Msg("compactor: starting background compaction loop")

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if context is already cancelled before starting compaction
			if c.ctx.Err() != nil {
				return
			}
			c.CompactFiles(c.ctx, c.maxBytes)
		case <-c.ctx.Done():
			zlog.Info().Msg("compactor: background loop stopping")
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
func (c *Compactor) CompactFiles(ctx context.Context, maxBytes int64) {
	zlog.Info().Int64("maxBytes", maxBytes).Msg("compactor: starting file compaction")

	// RocksDB iterator setup
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetPrefixSameAsStart(true)
	it := c.meta.Handle().NewIterator(ro)
	defer it.Close()

	wb := grocksdb.NewWriteBatch()
	var (
		processed   int
		bytesCopied int64
		filesToDel  []string
	)

	// Acquire the initial open segment.
	seg, err := c.sm.AcquireOpenSegment(0)
	if err != nil {
		zlog.Error().Err(err).Msg("compactor: acquire open segment")
		return
	}

	filePrefix := []byte(keys.CompactionIndexPrefix)
	iterationCount := 0
	lastLogTime := time.Now()

	for it.Seek(filePrefix); it.ValidForPrefix(filePrefix); it.Next() {
		iterationCount++

		// Log progress every 100 iterations or every 10 seconds
		if iterationCount%100 == 0 || time.Since(lastLogTime) > 10*time.Second {
			zlog.Debug().
				Int("iterations", iterationCount).
				Int("processed", processed).
				Int64("bytesCopied", bytesCopied).
				Msg("compactor: progress update")
			lastLogTime = time.Now()
		}

		// Check if context is cancelled
		if err := ctx.Err(); err != nil {
			zlog.Info().Msg("compactor: interrupted by cancellation")
			// Don't attempt to commit when cancelled - just return
			return
		}
		k := it.Key().Data()
		v := it.Value().Data()

		// Process the compaction entry
		userKey, filePath, _ := parseFileIndexRow(k, v)

		// Log what we're about to process
		if iterationCount <= 10 || iterationCount%100 == 0 {
			zlog.Debug().
				Int("iteration", iterationCount).
				Str("userKey", userKey).
				Str("filePath", filePath).
				Msg("compactor: processing entry")
		}

		entry, err := c.processCompactionEntry(ctx, k, v, ro)
		if err != nil {
			_, filePath, _ := parseFileIndexRow(k, v)

			// Handle different error cases using error types
			switch {
			case errors.Is(err, utils.ErrMetadataNotFound):
				// Key already gone – schedule raw file + index row for deletion
				wb.Delete(k)
				filesToDel = append(filesToDel, filePath)
				continue
			case errors.Is(err, utils.ErrAlreadyCompacted):
				// Remove compaction index entry
				// Note: don't delete the file since it might be referenced elsewhere
				wb.Delete(k)
				continue
			case errors.Is(err, utils.ErrNotRawFile), errors.Is(err, utils.ErrFilePathMismatch):
				// Stale entry - remove index and delete file
				wb.Delete(k)
				filesToDel = append(filesToDel, filePath)
				continue
			case errors.Is(err, utils.ErrMalformedIndexRow):
				// Malformed index row - remove it
				wb.Delete(k)
				continue
			case errors.Is(err, utils.ErrFileNotExist):
				// File doesn't exist - remove stale index
				wb.Delete(k)
				continue
			default:
				// Other errors - just continue
				continue
			}
		}

		// Advisory maxBytes check: if limit already reached and the next entry
		// (including header) would overflow the current segment, stop compaction.
		headerSize := segment.CalculateValueHeaderSize(entry.userKey)
		totalNeeded := headerSize + entry.fileInfo.Size()
		if bytesCopied >= maxBytes && seg.Remaining() < totalNeeded {
			break
		}

		// Compact the entry
		if err := c.compactEntry(ctx, entry, &seg, wb); err != nil {
			if err == context.Canceled {
				zlog.Info().Msg("compactor: compaction cancelled")
				return
			}
			// Check if it's a file size mismatch error
			var sizeMismatchErr *ErrFileSizeMismatch
			if errors.As(err, &sizeMismatchErr) {
				// Skip corrupted file
				wb.Delete(k)
				filesToDel = append(filesToDel, entry.filePath)
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

		// Housekeeping
		wb.Delete(k) // remove index row
		filesToDel = append(filesToDel, entry.filePath)

		processed++
		bytesCopied += entry.fileInfo.Size()
	}

	// Final commit.
	if err := c.commit(ctx, seg, wb, filesToDel); err != nil {
		if err != context.Canceled {
			zlog.Error().Err(err).Msg("compactor: commit failed")
		}
		return
	}

	zlog.Info().Int("migrated", processed).Int64("bytes", bytesCopied).Msg("compactor: finished file compaction")
}

// parseFileIndexRow extracts userKey, filePath and size from RocksDB file-index
// key/value pairs. Returns ok=false when the row does not follow the expected
// format.
func parseFileIndexRow(k, v []byte) (userKey, filePath string, ok bool) {
	// Key format: CompactionIndexKeyFormat (!compact/<ts>|<userKey>)
	pipeIdx := bytes.IndexByte(k, '|')
	if pipeIdx <= 0 {
		return
	}
	userKey = string(k[pipeIdx+1:])

	// Value format: <filePath>
	filePath = string(v)
	ok = true
	return
}

// compactionEntry represents a single entry to be compacted
type compactionEntry struct {
	userKey  string
	filePath string
	fileInfo os.FileInfo
	metadata *pb.ValueMessage
}

// processCompactionEntry processes a single compaction index entry
func (c *Compactor) processCompactionEntry(ctx context.Context, k, v []byte, ro *grocksdb.ReadOptions) (*compactionEntry, error) {
	userKey, filePath, ok := parseFileIndexRow(k, v)
	if !ok {
		zlog.Error().Str("row", string(k)).Msg("compactor: malformed index row")
		return nil, utils.ErrMalformedIndexRow
	}

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
	meta, err := c.loadAndValidateMetadata(ctx, userKey, filePath, ro)
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
func (c *Compactor) loadAndValidateMetadata(ctx context.Context, userKey, filePath string, ro *grocksdb.ReadOptions) (*pb.ValueMessage, error) {
	metaKey := keys.MakeMetadataKey(userKey)

	// Load metadata first
	meta, err := utils.GetMetadata(c.meta, string(metaKey))
	if err != nil {
		zlog.Error().Err(err).Str("key", userKey).Msg("compactor: bad metadata")
		return nil, err
	}

	// Validate the file entry
	if err := utils.ValidateFileEntry(meta, filePath, "compactor", userKey); err != nil {
		return nil, err
	}

	return meta, nil
}

// compactEntry performs the actual compaction of a single entry
func (c *Compactor) compactEntry(ctx context.Context, entry *compactionEntry, seg **segment.Segment, wb *grocksdb.WriteBatch) error {
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
	if err := c.ensureCapacity(ctx, seg, totalNeeded); err != nil {
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

	// Update metadata
	metaBytes, _ := proto.Marshal(entry.metadata)
	metaKey := keys.MakeMetadataKey(entry.userKey)
	wb.Put(metaKey, metaBytes)

	return nil
}

// copyFileIntoSegment copies the file into an open segment using the
// existing segment pipeline and updates the ValueMessage.
func (c *Compactor) copyFileIntoSegment(ctx context.Context, seg *segment.Segment, userKey string, f *os.File, vm *pb.ValueMessage) error {
	// Check if context is cancelled before I/O operation
	if err := ctx.Err(); err != nil {
		return err
	}

	segOff, err := c.sm.WriteEntry(seg, userKey, f, vm)
	if err != nil {
		return err
	}

	vm.RawFilePath = ""
	vm.SegmentPath = seg.Path()
	vm.SegmentOffset = segOff
	vm.ValueType = pb.ValueType_SEGMENT

	return nil
}

// commit commits pending RocksDB mutations and deletes migrated files.
func (c *Compactor) commit(ctx context.Context, seg *segment.Segment, wb *grocksdb.WriteBatch, toDelete []string) error {
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
	if err := c.sm.SyncSegment(seg); err != nil {
		return err
	}

	if err := c.meta.Handle().Write(grocksdb.NewDefaultWriteOptions(), wb); err != nil {
		return err
	}

	// Remove obsolete raw files on best-effort basis
	// Files that are currently being read will be skipped and left for next compaction run.
	zlog.Debug().Int("count", len(toDelete)).Msg("compactor: starting file deletion")

	filesDeleted := 0
	skippedFiles := 0
	for _, p := range toDelete {
		// Check if context is cancelled during cleanup
		if err := ctx.Err(); err != nil {
			zlog.Info().Msg("compactor: file cleanup interrupted by cancellation")
			return nil
		}

		err := c.fm.Remove(p)
		if err != nil {
			if errors.Is(err, files.ErrFileLocked) {
				skippedFiles++
				if skippedFiles <= 10 {
					zlog.Debug().Str("path", p).Msg("compactor: file locked for reading, will retry in next compaction")
				}
			} else {
				zlog.Error().Err(err).Str("path", p).Msg("compactor: failed to delete file")
			}

			continue
		}

		filesDeleted++
		if filesDeleted <= 10 || filesDeleted%100 == 0 {
			zlog.Debug().Str("path", p).Int("deleted_so_far", filesDeleted).Msg("compactor: deleted file")
		}
	}

	if skippedFiles > 0 {
		zlog.Info().Int("deleted", filesDeleted).Int("skipped", skippedFiles).Int("total", len(toDelete)).Msg("compactor: commit completed with some files skipped")
	} else if filesDeleted > 0 {
		zlog.Info().Int("deleted", filesDeleted).Msg("compactor: commit completed, all files deleted")
	}

	return nil
}

// ensureCapacity ensures that the segment has at least the needed bytes
// available, finalising and acquiring a fresh segment when necessary.
func (c *Compactor) ensureCapacity(ctx context.Context, seg **segment.Segment, needed int64) error {
	if (*seg).Remaining() >= needed {
		return nil
	}

	// Check if context is cancelled before finalizing
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := c.sm.FinalizeSegment(*seg); err != nil {
		return err
	}

	newSeg, err := c.sm.AcquireOpenSegment(0)
	if err != nil {
		return err
	}
	*seg = newSeg
	return nil
}
