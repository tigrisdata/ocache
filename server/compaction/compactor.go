package compaction

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/storage/fd"
	"github.com/tigrisdata/ocache/server/storage/files"
	"github.com/tigrisdata/ocache/server/storage/metadata"
	"github.com/tigrisdata/ocache/server/storage/segment"

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

type Compactor struct {
	fm       *files.FileManager
	sm       *segment.Manager
	meta     *metadata.MetaDB
	fdCache  *fd.FdCache
	maxBytes int64
	interval time.Duration

	// background loop coordination
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewCompactor creates a new Compactor bound to the provided FileManager and
// Segment Manager.
func NewCompactor(fm *files.FileManager, sm *segment.Manager, maxBytes int64, interval time.Duration) *Compactor {
	return &Compactor{
		fm:       fm,
		sm:       sm,
		meta:     metadata.GetMetaDB(),
		fdCache:  fd.GetFdCache(),
		maxBytes: maxBytes,
		interval: interval,
		closeCh:  make(chan struct{}),
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
	close(c.closeCh)
	c.wg.Wait()
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
			c.CompactFiles(c.maxBytes)
		case <-c.closeCh:
			zlog.Info().Msg("compactor: background loop stopping")
			return
		}
	}
}

// PrepareEntryForCompaction prepares the key and value to store in compaction index.
func PrepareEntryForCompaction(key, filePath string) ([]byte, []byte) {
	ts := time.Now().UnixNano()
	idxKey := fmt.Sprintf("!compact/%020d|%s", ts, key)
	idxVal := fmt.Sprintf("%s", filePath)

	return []byte(idxKey), []byte(idxVal)
}

// CompactFiles scans the RocksDB file-index and migrates files into segments.
func (c *Compactor) CompactFiles(maxBytes int64) {
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

	filePrefix := []byte("!compact/")
	for it.Seek(filePrefix); it.ValidForPrefix(filePrefix); it.Next() {
		k := it.Key().Data()
		v := it.Value().Data()

		userKey, filePath, ok := parseFileIndexRow(k, v)
		if !ok {
			zlog.Error().Str("row", string(k)).Msg("compactor: malformed index row")
			wb.Delete(k)
			continue
		}

		// Stat first – cheap, and gives us size quickly.
		fInfo, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				wb.Delete(k) // stale index row
				continue
			}
			zlog.Error().Err(err).Str("path", filePath).Msg("compactor: stat failed")
			continue
		}

		// Advisory maxBytes check: if limit already reached and the next file
		// would overflow the current segment, stop compaction.
		if bytesCopied >= maxBytes && seg.Remaining() < fInfo.Size() {
			break
		}

		// Fetch metadata for the user key.
		slice, err := c.meta.Handle().Get(ro, []byte(userKey))
		if err != nil {
			zlog.Error().Err(err).Str("key", userKey).Msg("compactor: db.Get")
			continue
		}

		if !slice.Exists() {
			// Key already gone – schedule raw file + index row for deletion.
			slice.Free()
			wb.Delete(k)
			filesToDel = append(filesToDel, filePath)
			continue
		}

		vm := &pb.ValueMessage{}
		if err := proto.Unmarshal(slice.Data(), vm); err != nil {
			zlog.Error().Err(err).Str("key", userKey).Msg("compactor: bad metadata")
			slice.Free()
			continue
		}

		// Check if this compaction entry refers to the current file for this key
		// If not, it's a stale entry from a previous Put operation that should be cleaned up
		if vm.ValueType == pb.ValueType_RAW_FILE && vm.RawFilePath != filePath {
			zlog.Debug().
				Str("key", userKey).
				Str("stale_file", filePath).
				Str("current_file", vm.RawFilePath).
				Msg("compactor: skipping stale compaction entry")
			slice.Free()
			wb.Delete(k)                              // Remove stale compaction index entry
			filesToDel = append(filesToDel, filePath) // Delete the stale file
			continue
		}

		// If the value is no longer a raw file (already compacted to segment or changed to inline),
		// clean up this compaction entry
		if vm.ValueType != pb.ValueType_RAW_FILE {
			zlog.Debug().
				Str("key", userKey).
				Str("value_type", vm.ValueType.String()).
				Msg("compactor: value no longer needs compaction")
			slice.Free()
			wb.Delete(k) // Remove compaction index entry
			// Note: don't delete the file since it might be referenced elsewhere
			continue
		}

		slice.Free()

		// Ensure we have space in the current segment.
		if err := c.ensureCapacity(&seg, vm.ValueLength); err != nil {
			zlog.Error().Err(err).Msg("compactor: segment rotation failed")
			return
		}

		// Copy the raw file into the segment.
		f, err := os.Open(filePath)
		if err != nil {
			zlog.Error().Err(err).Str("path", filePath).Msg("compactor: open failed")
			continue
		}
		if err := c.copyFileIntoSegment(seg, userKey, f, vm); err != nil {
			zlog.Error().Err(err).Str("key", userKey).Msg("compactor: copy failed")
			f.Close()
			continue
		}
		f.Close()

		// Update metadata & housekeeping.
		metaBytes, _ := proto.Marshal(vm)
		wb.Put([]byte(userKey), metaBytes)
		wb.Delete(k) // remove index row
		filesToDel = append(filesToDel, filePath)

		processed++
		bytesCopied += fInfo.Size()
	}

	// Final commit.
	if err := c.commit(seg, wb, filesToDel); err != nil {
		zlog.Error().Err(err).Msg("compactor: commit failed")
		return
	}

	zlog.Info().Int("migrated", processed).Int64("bytes", bytesCopied).Msg("compactor: finished file compaction")
}

// parseFileIndexRow extracts userKey, filePath and size from RocksDB file-index
// key/value pairs. Returns ok=false when the row does not follow the expected
// format.
func parseFileIndexRow(k, v []byte) (userKey, filePath string, ok bool) {
	// Key format: !compact/<ts>|<userKey>
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

// copyFileIntoSegment copies the file into an open segment using the
// existing segment pipeline and updates the ValueMessage.
func (c *Compactor) copyFileIntoSegment(seg *segment.Segment, userKey string, f *os.File, vm *pb.ValueMessage) (err error) {
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
func (c *Compactor) commit(seg *segment.Segment, wb *grocksdb.WriteBatch, toDelete []string) error {
	if wb.Count() == 0 {
		return nil // nothing to do
	}

	// Persist segment first so that metadata can safely reference it.
	if err := c.sm.SyncSegment(seg); err != nil {
		return err
	}

	if err := c.meta.Handle().Write(grocksdb.NewDefaultWriteOptions(), wb); err != nil {
		return err
	}

	// Remove obsolete raw files on best-effort basis
	for _, p := range toDelete {
		c.fm.Remove(p)
	}
	return nil
}

// ensureCapacity ensures that the segment has at least the needed bytes
// available, finalising and acquiring a fresh segment when necessary.
func (c *Compactor) ensureCapacity(seg **segment.Segment, needed int64) error {
	if (*seg).Remaining() >= needed {
		return nil
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
