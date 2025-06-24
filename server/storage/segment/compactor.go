package segment

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	zlog "github.com/rs/zerolog/log"
	pb "github.com/tigrisdata/cache_service/proto"
	"github.com/tigrisdata/cache_service/server/storage/metadata"

	"google.golang.org/protobuf/proto"
)

// Compactor is responsible for migrating raw files referenced in RocksDB into
// proper segments managed by the Segment Manager. A Compactor operates on a
// RawFileManager + Segment Manager pair but is otherwise stateless, so callers
// are free to create a new instance for every compaction run.
//
// Callers should create a Compactor and invoke Compact(). The method is safe to
// call from multiple goroutines concurrently – each invocation creates its own
// RocksDB iterator and write-batch.
//
// The method is best-effort and does not propagate errors back to the caller to
// avoid interfering with the foreground workload.
//
//     comp := segment.NewCompactor(rw, sm)
//     comp.Compact(maxBytes, flushBytes)
//
// NOTE: At present the implementation only migrates raw files; segment-level
// compaction (merging/deleting) lives in Manager.compactSegments().

type Compactor struct {
	rw   *RawFileManager
	sm   *Manager
	meta *metadata.MetaDB
}

// NewCompactor creates a new Compactor bound to the provided RawFileManager and
// Segment Manager.
func NewCompactor(rw *RawFileManager, sm *Manager) *Compactor {
	return &Compactor{rw: rw, sm: sm, meta: metadata.GetMetaDB()}
}

// RecordEntry records an entry in the RocksDB raw-index.
func RecordEntryForCompaction(key, filePath string) error {
	ts := time.Now().UnixNano()
	idxKey := fmt.Sprintf("!raw/%020d|%s", ts, key)
	idxVal := fmt.Sprintf("%s", filePath)

	wo := grocksdb.NewDefaultWriteOptions()
	if err := metadata.GetMetaDB().Handle().Put(wo, []byte(idxKey), []byte(idxVal)); err != nil {
		// Failure to index should not make the write fail
		zlog.Error().Err(err).Str("key", key).Msg("compactor: failed to put raw index")
	} else {
		zlog.Debug().Str("key", key).Msg("compactor: indexed raw file in RocksDB")
	}

	return nil
}

// CompactRawFiles scans the RocksDB raw-index and migrates raw files into segments.
func (c *Compactor) CompactRawFiles(maxBytes int64, flushBytes int64) {
	zlog.Info().Int64("maxBytes", maxBytes).Int64("flushBytes", flushBytes).Msg("compactor: starting raw-file compaction")

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetPrefixSameAsStart(true)
	it := c.meta.Handle().NewIterator(ro)
	defer it.Close()

	wo := grocksdb.NewDefaultWriteOptions()
	batch := grocksdb.NewWriteBatch()
	processed := 0
	var bytesMigrated, bytesToFlush int64

	rawPrefix := []byte("!raw/")
	for it.Seek(rawPrefix); it.ValidForPrefix(rawPrefix); it.Next() {
		k := it.Key().Data()
		v := it.Value().Data()

		userKey, filePath, ok := parseRawIndexRow(k, v)
		if !ok {
			continue
		}

		// If the raw file does not exist, remove the index row.
		fInfo, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			zlog.Warn().Str("key", userKey).Str("path", filePath).Msg("compactor: raw file does not exist")
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
				metadataFound = false
			}
		}

		var (
			bytesMoved int64
			promoted   bool
			promErr    error
		)

		if metadataFound {
			// Attempt zero-copy promotion first; fall back to copy otherwise.
			promoted, bytesMoved, promErr = c.promoteLargeRaw(userKey, filePath, fInfo.Size(), vm)
			if promErr != nil {
				zlog.Error().Err(promErr).Str("key", userKey).Msg("compactor: promotion failed, falling back to copy")
				promoted = false
			}

			if !promoted {
				bytesMoved, promErr = c.copyRawIntoSegment(userKey, filePath, vm)
				if promErr != nil {
					zlog.Error().Err(promErr).Str("key", userKey).Msg("compactor: copy failed")
					slice.Free()
					continue
				}
			}

			// Update metadata if present.
			if data, err := proto.Marshal(vm); err == nil {
				batch.Put([]byte(userKey), data)
			}
		}

		// Release slice as early as possible.
		slice.Free()

		// Regardless of metadata presence, remove the raw file as it is no longer needed.
		c.rw.Remove(filePath)

		// Remove the index row.
		batch.Delete(k)

		processed++
		bytesMigrated += bytesMoved
		bytesToFlush += bytesMoved

		// Flush intermediate batch when threshold reached.
		if bytesToFlush >= flushBytes {
			_ = c.meta.Handle().Write(wo, batch)
			batch.Clear()
			bytesToFlush = 0

			zlog.Debug().Int("processed", processed).Int64("bytes", bytesMigrated).Msg("compactor: batch flushed")
			if bytesMigrated >= maxBytes {
				break
			}
		}
	}

	// Flush any remaining data.
	if batch.Count() > 0 {
		_ = c.meta.Handle().Write(wo, batch)
	}

	zlog.Info().Int("migrated", processed).Int64("bytes", bytesMigrated).Dur("duration", time.Since(time.Now())).Msg("compactor: finished raw-file compaction")
}

// parseRawIndexRow extracts userKey, filePath and size from RocksDB raw-index
// key/value pairs. Returns ok=false when the row does not follow the expected
// format.
func parseRawIndexRow(k, v []byte) (userKey, filePath string, ok bool) {
	// Key format: !raw/<ts>|<userKey>
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

// promoteLargeRaw attempts to convert the raw file into a single-entry segment
// by appending a footer and renaming the file. Returns promoted=true on success.
func (c *Compactor) promoteLargeRaw(userKey, filePath string, fileSize int64, vm *pb.ValueMessage) (promoted bool, valueBytes int64, err error) {
	// Require the file to be sufficiently big to justify promotion.
	if fileSize < DefaultRawToSegmentPromotionThreshold {
		return false, 0, nil // Too small – fall back.
	}

	// Use helper from segmentfile which also registers the segment.
	newPath, headerSize, valueLen, err := c.promoteRawFileLow(filePath, c.sm.segmentsPath, userKey, fileSize)
	if err != nil {
		return false, 0, err
	}

	// Update ValueMessage.
	vm.RawFilePath = ""
	vm.SegmentPath = newPath
	vm.SegmentOffset = headerSize
	vm.ValueLength = valueLen

	return true, valueLen, nil
}

// copyRawIntoSegment copies the raw file into an open segment using the
// existing segment pipeline and updates the ValueMessage.
func (c *Compactor) copyRawIntoSegment(userKey, filePath string, vm *pb.ValueMessage) (copiedBytes int64, err error) {
	segPath, segOff, segLen, err := c.sm.WriteToSegment(userKey, filePath)
	if err != nil {
		return 0, err
	}

	vm.RawFilePath = ""
	vm.SegmentPath = segPath
	vm.SegmentOffset = segOff
	vm.ValueLength = segLen

	return segLen, nil
}

// promoteRawFileLow converts an existing raw file that already contains
// [header|payload] into a fully-fledged single-entry segment by appending the
// footer and atomically renaming the file into destDir. It returns the new
// segment path, header size (offset of payload) and payload length.
func (c *Compactor) promoteRawFileLow(rawPath, destDir, userKey string, fileSize int64) (string, int64, int64, error) {
	valueHeaderSize := CalculateValueHeaderSize(userKey)
	valueLen := fileSize - valueHeaderSize
	if valueLen <= 0 {
		return "", 0, 0, fmt.Errorf("computed negative value length")
	}

	// Get file lock.
	lock := c.rw.GetFileLock(rawPath)
	lock.Lock()
	defer lock.Unlock()

	// Append footer.
	f, err := os.OpenFile(rawPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return "", 0, 0, err
	}
	footer := BuildSegmentFooterWithVersion(CurrentSegmentVersion, 1, valueLen)
	if _, err := f.Write(footer); err != nil {
		f.Close()
		return "", 0, 0, err
	}
	_ = f.Sync()
	_ = f.Close()

	// Generate final path.
	newPath := filepath.Join(destDir, fmt.Sprintf("segment_%d.seg", time.Now().UnixNano()))
	if err := os.Rename(rawPath, newPath); err != nil {
		return "", 0, 0, err
	}

	c.sm.RegisterSegment(newPath, 1, valueLen)

	return newPath, valueHeaderSize, valueLen, nil
}
