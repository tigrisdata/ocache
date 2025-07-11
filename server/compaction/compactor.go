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

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetPrefixSameAsStart(true)
	it := c.meta.Handle().NewIterator(ro)
	defer it.Close()

	wo := grocksdb.NewDefaultWriteOptions()
	batch := grocksdb.NewWriteBatch()
	processed := 0
	var bytesMigrated int64
	var filesToRemove []string

	filePrefix := []byte("!compact/")
	for it.Seek(filePrefix); it.ValidForPrefix(filePrefix); it.Next() {
		k := it.Key().Data()
		v := it.Value().Data()

		userKey, filePath, ok := parseFileIndexRow(k, v)
		if !ok {
			zlog.Error().Str("key", string(k)).Msg("compactor: failed to parse file index row")
			continue
		}

		// If the file does not exist, remove the index row.
		_, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			zlog.Warn().Str("key", userKey).Str("path", filePath).Msg("compactor: file does not exist")
			batch.Delete(k)
			continue
		}

		// Load current metadata for the user key.
		slice, err := c.meta.Handle().Get(ro, []byte(userKey))
		if err != nil {
			zlog.Error().Err(err).Str("key", userKey).Msg("compactor: db.Get error")
			continue
		}

		metadataFound := slice.Exists()
		vm := &pb.ValueMessage{}
		if metadataFound {
			if err := proto.Unmarshal(slice.Data(), vm); err != nil {
				zlog.Error().Err(err).Str("key", userKey).Msg("compactor: failed to unmarshal metadata")
				metadataFound = false
			}
		}

		// Release slice as early as possible.
		slice.Free()

		var bytesMoved int64

		if metadataFound {

			if bytesMoved, err = c.copyFileIntoSegment(userKey, filePath, vm); err != nil {
				zlog.Error().Err(err).Str("key", userKey).Msg("compactor: copy failed")
				continue
			}
			zlog.Debug().Int64("bytesMoved", bytesMoved).Str("key", userKey).Msg("compactor: copied file into segment")

			// Update metadata if present.
			var data []byte
			if data, err = proto.Marshal(vm); err != nil {
				zlog.Error().Err(err).Str("key", userKey).Msg("compactor: failed to marshal metadata")
				continue
			}
			batch.Put([]byte(userKey), data)
		}

		// Regardless of metadata presence, remove the file as it is no longer needed.
		filesToRemove = append(filesToRemove, filePath)

		// Remove the index row.
		batch.Delete(k)

		processed++
		bytesMigrated += bytesMoved

		if bytesMigrated >= maxBytes {
			break
		}
	}

	// Flush any remaining data.
	if batch.Count() > 0 {
		// Ensure data written to the segment is durable on disk.
		if err := c.sm.SyncCurrentSegment(); err != nil {
			zlog.Error().Err(err).Msg("compactor: failed to sync current segment")
			// We can't write to RocksDB if the segment is not durable.
			// We need to retry the compaction.
			batch.Clear()
			return
		}

		// Only after segment is made durable, we can write to RocksDB.
		if err := c.meta.Handle().Write(wo, batch); err != nil {
			zlog.Error().Err(err).Msg("compactor: failed to write to RocksDB")
		}
	}

	// Remove files after the segment is made durable.
	for _, filePath := range filesToRemove {
		c.fm.Remove(filePath)
		zlog.Debug().Str("path", filePath).Msg("compactor: removed file")
	}

	zlog.Info().Int("migrated", processed).Int64("bytes", bytesMigrated).Dur("duration", time.Since(time.Now())).Msg("compactor: finished file compaction")
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
func (c *Compactor) copyFileIntoSegment(userKey, filePath string, vm *pb.ValueMessage) (copiedBytes int64, err error) {
	segPath, segOff, segLen, err := c.sm.WriteToSegment(userKey, filePath)
	if err != nil {
		return 0, err
	}

	vm.RawFilePath = ""
	vm.SegmentPath = segPath
	vm.SegmentOffset = segOff
	vm.ValueLength = segLen
	vm.ValueType = pb.ValueType_SEGMENT

	return segLen, nil
}
