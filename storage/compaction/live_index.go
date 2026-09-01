// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"github.com/tigrisdata/ocache/storage/utils"
	"google.golang.org/protobuf/proto"
)

// segmentDataRegionSize normalizes Segment.size's two in-process forms. A
// freshly finalized Segment currently includes the footer in size, while the
// manager subtracts that footer when loading a closed segment. The coverage
// fingerprint always stores the data-region size, so it remains stable across
// finalization and restart.
func segmentDataRegionSize(seg *segment.Segment) (int64, os.FileInfo, error) {
	if seg == nil {
		return 0, nil, fmt.Errorf("nil segment")
	}
	info, err := os.Stat(seg.Path())
	if err != nil {
		return 0, nil, err
	}
	size := seg.GetSize()
	switch {
	case info.Size() == size && size >= int64(segment.SegmentFooterSize):
		return size - int64(segment.SegmentFooterSize), info, nil
	case info.Size() == size+int64(segment.SegmentFooterSize):
		return size, info, nil
	default:
		return 0, info, fmt.Errorf("segment %s size %d does not match manager size %d", seg.Path(), info.Size(), size)
	}
}

// segmentLiveIndexCovered reports whether the marker for seg still matches its
// finalized footer fingerprint. Invalid or stale markers deliberately return
// false without an error: the caller then uses the historical source scan.
func segmentLiveIndexCovered(meta *metadata.MetaDB, seg *segment.Segment) (bool, error) {
	if meta == nil || seg == nil || seg.HasOpenFile() {
		return false, nil
	}

	dataSize, info, err := segmentDataRegionSize(seg)
	if err != nil {
		return false, nil
	}

	markerKey := keys.MakeSegmentLiveCoverageKey(seg.Path())
	ro := metadata.CreateReadOptions(false, false)
	defer ro.Destroy()
	slice, err := meta.Handle().Get(ro, markerKey)
	if err != nil {
		return false, err
	}
	defer slice.Free()
	if !slice.Exists() {
		return false, nil
	}

	coverage, err := keys.DecodeSegmentLiveIndexCoverage(slice.Data())
	if err != nil || coverage.Entries != seg.GetNumEntries() || coverage.DataBytes != seg.GetDataBytes() || coverage.Size != dataSize {
		return false, nil
	}
	if info.Size() != dataSize+int64(segment.SegmentFooterSize) {
		return false, nil
	}
	return true, nil
}

// MarkSegmentLiveIndexComplete publishes a coverage marker after a segment is
// finalized and all metadata/index rows for its records are durable. A marker
// write failure is safe: callers can leave it absent and the historical scan
// remains the fallback.
func MarkSegmentLiveIndexComplete(meta *metadata.MetaDB, seg *segment.Segment) error {
	if meta == nil || seg == nil {
		return fmt.Errorf("segment live index marker requires metadata and segment")
	}
	if seg.HasOpenFile() {
		return fmt.Errorf("cannot cover open segment %s", seg.Path())
	}
	dataSize, _, err := segmentDataRegionSize(seg)
	if err != nil {
		return err
	}
	value, err := keys.EncodeSegmentLiveIndexCoverage(keys.SegmentLiveIndexCoverage{
		Entries:   seg.GetNumEntries(),
		DataBytes: seg.GetDataBytes(),
		Size:      dataSize,
	})
	if err != nil {
		return err
	}
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return meta.Handle().Put(wo, keys.MakeSegmentLiveCoverageKey(seg.Path()), value)
}

// stageSegmentLiveIndexEntry adds the immutable source-location row that
// belongs beside a metadata SEGMENT publication. The row is intentionally a
// superset of current metadata: a conditional migration can lose its CAS after
// this batch is staged, and the later validation path prunes that speculative
// row.
func stageSegmentLiveIndexEntry(batch *grocksdb.WriteBatch, segmentPath string, entry *segment.EntryInfo) error {
	if batch == nil || entry == nil {
		return fmt.Errorf("segment live index batch or entry is nil")
	}
	value, err := keys.EncodeSegmentLiveIndexEntry(keys.SegmentLiveIndexEntry{
		Key:           entry.Key,
		ValueLength:   entry.ValueLength,
		HeaderSize:    entry.HeaderSize,
		Checksum:      entry.Checksum,
		HeaderVersion: entry.Version,
	})
	if err != nil {
		return err
	}
	indexKey := keys.MakeSegmentLiveIndexKey(segmentPath, entry.Offset)
	if indexKey == nil {
		return fmt.Errorf("invalid segment live index location: path=%q offset=%d", segmentPath, entry.Offset)
	}
	batch.Put(indexKey, value)
	return nil
}

func stageSegmentLiveIndexRow(batch *grocksdb.WriteBatch, userKey, segmentPath string, offset int64, valueLength int64, checksum uint32) error {
	return stageSegmentLiveIndexEntry(batch, segmentPath, &segment.EntryInfo{
		Key:         userKey,
		Offset:      offset,
		HeaderSize:  segment.CalculateValueHeaderSize(userKey),
		ValueLength: valueLength,
		Checksum:    checksum,
		Version:     segment.CurrentValueHeaderVersion,
	})
}

// stageSegmentLiveIndexDelete removes one source-location row in the same
// metadata batch that moves or removes its authoritative metadata.
func stageSegmentLiveIndexDelete(batch *grocksdb.WriteBatch, segmentPath string, offset int64) error {
	if batch == nil {
		return fmt.Errorf("segment live index batch is nil")
	}
	indexKey := keys.MakeSegmentLiveIndexKey(segmentPath, offset)
	if indexKey == nil {
		return fmt.Errorf("invalid segment live index location: path=%q offset=%d", segmentPath, offset)
	}
	batch.Delete(indexKey)
	return nil
}

// deleteSegmentLiveIndexRows stages removal of every row and its coverage
// marker for segmentPath. It is used only once the source segment's metadata
// has already been committed elsewhere, so it also cleans up partial indexes
// left by an old or failed migration.
func deleteSegmentLiveIndexRows(meta *metadata.MetaDB, batch *grocksdb.WriteBatch, segmentPath string) error {
	if meta == nil || batch == nil || segmentPath == "" {
		return fmt.Errorf("invalid segment live index cleanup arguments")
	}
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	it := meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := keys.MakeSegmentLiveIndexPrefix(segmentPath)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		batch.Delete(append([]byte(nil), it.Key().Data()...))
		it.Key().Free()
		it.Value().Free()
	}
	if err := it.Err(); err != nil {
		return err
	}
	batch.Delete(keys.MakeSegmentLiveCoverageKey(segmentPath))
	return nil
}

// backfillSegmentLiveIndex derives rows for closed legacy segments from actual
// source headers and metadata. It never publishes a marker until the complete
// source walk and every row batch has succeeded. A crash or read/write error
// therefore leaves the segment on the historical scan path instead of making
// an incomplete or physically incorrect index authoritative.
func backfillSegmentLiveIndex(meta *metadata.MetaDB, sm interface {
	GetSegments() []*segment.Segment
}) error {
	if meta == nil || sm == nil {
		return fmt.Errorf("segment live index backfill requires metadata and manager")
	}

	closed := make([]*segment.Segment, 0)
	for _, seg := range sm.GetSegments() {
		if seg != nil && !seg.HasOpenFile() {
			closed = append(closed, seg)
		}
	}
	if len(closed) == 0 {
		return nil
	}

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for _, seg := range closed {
		covered, err := segmentLiveIndexCovered(meta, seg)
		if err != nil {
			return err
		}
		if covered {
			// A durable marker makes this segment safe to use through the index;
			// do not turn every restart into another historical backfill scan.
			continue
		}
		// Remove the trust marker before doing any repair work. If startup is
		// interrupted midway, partial rows remain harmless because readers see
		// no marker and retain the historical scan fallback.
		if err := meta.Handle().Delete(wo, keys.MakeSegmentLiveCoverageKey(seg.Path())); err != nil {
			return err
		}
		batch := grocksdb.NewWriteBatch()
		if err := deleteSegmentLiveIndexRows(meta, batch, seg.Path()); err != nil {
			batch.Destroy()
			return err
		}
		file, err := os.Open(seg.Path())
		if err != nil {
			batch.Destroy()
			return fmt.Errorf("open segment %s during live index backfill: %w", seg.Path(), err)
		}
		iter, err := seg.NewIterator(file)
		if err != nil {
			file.Close()
			batch.Destroy()
			return fmt.Errorf("create iterator for segment %s during live index backfill: %w", seg.Path(), err)
		}

		var walked int64
		var dataBytes int64
		backfillErr := func() error {
			for {
				entry, err := iter.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				walked++
				dataBytes += entry.ValueLength
				metadataValue, err := utils.GetMetadata(meta, string(keys.MakeMetadataKey(entry.Key)))
				if err != nil {
					if errors.Is(err, utils.ErrMetadataNotFound) {
						continue
					}
					return err
				}
				if metadataValue.ValueType != pb.ValueType_SEGMENT || metadataValue.SegmentPath != seg.Path() || metadataValue.SegmentOffset != entry.Offset {
					continue
				}
				if metadataValue.ValueLength != entry.ValueLength || metadataValue.Checksum != entry.Checksum {
					return fmt.Errorf("metadata facts disagree with source entry %q in %s", entry.Key, seg.Path())
				}
				if err := stageSegmentLiveIndexEntry(batch, seg.Path(), entry); err != nil {
					return err
				}
				if batch.Count() >= 1000 {
					if err := meta.Handle().Write(wo, batch); err != nil {
						return err
					}
					batch.Clear()
				}
			}
			if walked != int64(seg.GetNumEntries()) || dataBytes != seg.GetDataBytes() {
				return fmt.Errorf("segment %s backfill saw %d entries/%d bytes, footer says %d/%d", seg.Path(), walked, dataBytes, seg.GetNumEntries(), seg.GetDataBytes())
			}
			return nil
		}()
		file.Close()
		if backfillErr != nil {
			// The marker was deleted before the scan. Any partial rows are
			// harmless because the recompactor will retain the scan fallback.
			batch.Destroy()
			return fmt.Errorf("backfill segment %s: %w", seg.Path(), backfillErr)
		}
		if batch.Count() > 0 {
			if err := meta.Handle().Write(wo, batch); err != nil {
				batch.Destroy()
				return err
			}
		}
		batch.Destroy()
		if err := MarkSegmentLiveIndexComplete(meta, seg); err != nil {
			return fmt.Errorf("mark segment live index coverage for %s: %w", seg.Path(), err)
		}
	}
	return nil
}

// BackfillSegmentLiveIndex is the startup hook used by Storage. It is exported
// so initialization can retain the safe fallback when backfill fails while
// still surfacing the failure in logs.
func BackfillSegmentLiveIndex(meta *metadata.MetaDB, sm interface {
	GetSegments() []*segment.Segment
}) error {
	return backfillSegmentLiveIndex(meta, sm)
}

// indexedSegmentRows walks the covered index in source-offset order and invokes
// visit for each raw row. It owns the RocksDB iterator and preserves the
// distinction between a clean end and an iterator failure.
func indexedSegmentRows(ctx context.Context, meta *metadata.MetaDB, segmentPath string, visit func(indexKey, indexValue []byte) error) error {
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	it := meta.Handle().NewIterator(ro)
	defer it.Close()

	prefix := keys.MakeSegmentLiveIndexPrefix(segmentPath)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := append([]byte(nil), it.Key().Data()...)
		value := append([]byte(nil), it.Value().Data()...)
		it.Key().Free()
		it.Value().Free()
		if err := visit(key, value); err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return err
	}
	return nil
}

// indexedSegmentEntry decodes an index row and proves that its location and
// record facts fit inside the finalized source data region. No source header or
// key read is performed.
func indexedSegmentEntry(indexKey, indexValue []byte, oldSeg *segment.Segment, dataSize int64) (*segment.EntryInfo, error) {
	path, offset, ok := keys.ParseSegmentLiveIndexKey(indexKey)
	if !ok || path != oldSeg.Path() {
		return nil, fmt.Errorf("malformed segment live index key")
	}
	row, err := keys.DecodeSegmentLiveIndexEntry(indexValue)
	if err != nil {
		return nil, err
	}
	if row.HeaderSize != segment.CalculateValueHeaderSize(row.Key) || row.HeaderVersion > segment.CurrentValueHeaderVersion {
		return nil, fmt.Errorf("invalid segment live index facts for %q", row.Key)
	}
	end := offset + row.HeaderSize
	if end < offset || end > dataSize || row.ValueLength > dataSize-end {
		return nil, fmt.Errorf("segment live index row for %q is outside %s", row.Key, oldSeg.Path())
	}
	return &segment.EntryInfo{
		Key:         row.Key,
		Offset:      offset,
		HeaderSize:  row.HeaderSize,
		ValueLength: row.ValueLength,
		Checksum:    row.Checksum,
		Version:     row.HeaderVersion,
	}, nil
}

// validateIndexedMetadata proves that a decoded row still represents the
// authoritative metadata location. It returns the metadata for a live row,
// false for a stale speculative row, and an error for a transient lookup or
// inconsistent live facts.
func validateIndexedMetadata(meta *metadata.MetaDB, entry *segment.EntryInfo, indexValue []byte, oldPath string) (metadataValue *pb.ValueMessage, live bool, err error) {
	row, err := keys.DecodeSegmentLiveIndexEntry(indexValue)
	if err != nil {
		return nil, false, err
	}
	metadataValue, err = utils.GetMetadata(meta, string(keys.MakeMetadataKey(row.Key)))
	if err != nil {
		if errors.Is(err, utils.ErrMetadataNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if metadataValue.ValueType != pb.ValueType_SEGMENT || metadataValue.SegmentPath != oldPath || metadataValue.SegmentOffset != entry.Offset {
		return nil, false, nil
	}
	if metadataValue.ValueLength != entry.ValueLength || metadataValue.Checksum != entry.Checksum {
		return nil, false, fmt.Errorf("segment live index facts disagree with metadata for %q", row.Key)
	}
	return metadataValue, true, nil
}

// verifySegmentLiveIndexComplete is a metadata-only guard before deleting a
// source segment. The coverage marker is a construction invariant, but this
// final check also protects against a lost row or an older mutation path: every
// metadata row still pointing at segmentPath must have a matching index row.
// Missing/mismatched rows are repaired only after a bounded source-header check
// proves the physical key and facts; otherwise the caller invalidates coverage
// and retries through the complete historical scan.
func (sr *SegmentRecompactor) verifySegmentLiveIndexComplete(segmentPath string, oldSeg *segment.Segment) (complete bool, err error) {
	dataSize, _, err := segmentDataRegionSize(oldSeg)
	if err != nil {
		return false, err
	}
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	it := sr.meta.Handle().NewIterator(ro)
	defer it.Close()
	pointRO := metadata.CreateReadOptions(false, false)
	defer pointRO.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	flush := func() error {
		if batch.Count() == 0 {
			return nil
		}
		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		if err := sr.meta.Handle().Write(wo, batch); err != nil {
			return err
		}
		batch.Clear()
		return nil
	}

	var sourceFile *os.File
	defer func() {
		if sourceFile != nil {
			_ = sourceFile.Close()
		}
	}()
	physicalEntry := func(offset int64, userKey string, expectedLength int64, expectedChecksum uint32) (*segment.EntryInfo, error) {
		if sourceFile == nil {
			var err error
			sourceFile, err = os.Open(segmentPath)
			if err != nil {
				return nil, err
			}
		}
		valueLength, headerSize, keyLength, version, checksum, err := segment.ReadValueHeaderAt(sourceFile, offset)
		if err != nil {
			return nil, err
		}
		if offset < 0 || offset > dataSize || headerSize <= 0 || headerSize > dataSize-offset || valueLength <= 0 || valueLength > dataSize-offset-headerSize {
			return nil, fmt.Errorf("source entry at offset %d in %s exceeds the data region", offset, segmentPath)
		}
		if keyLength <= 0 || keyLength > dataSize-offset-int64(segment.ValueHeaderSize) || keyLength > int64(^uint(0)>>1) {
			return nil, fmt.Errorf("invalid source key length at offset %d in %s", offset, segmentPath)
		}
		keyBytes := make([]byte, int(keyLength))
		if _, err := sourceFile.ReadAt(keyBytes, offset+int64(segment.ValueHeaderSize)); err != nil {
			return nil, err
		}
		if string(keyBytes) != userKey || valueLength != expectedLength || checksum != expectedChecksum {
			return nil, fmt.Errorf("source facts disagree with metadata for %q at offset %d in %s", userKey, offset, segmentPath)
		}
		return &segment.EntryInfo{
			Key:         userKey,
			Offset:      offset,
			HeaderSize:  headerSize,
			ValueLength: valueLength,
			Checksum:    checksum,
			Version:     version,
		}, nil
	}

	repaired := false
	prefix := []byte(keys.MetadataPrefix)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		keySlice := it.Key()
		valueSlice := it.Value()
		keyBytes := append([]byte(nil), keySlice.Data()...)
		valueBytes := append([]byte(nil), valueSlice.Data()...)
		keySlice.Free()
		valueSlice.Free()

		var value pb.ValueMessage
		if err := proto.Unmarshal(valueBytes, &value); err != nil {
			return false, err
		}
		if value.ValueType != pb.ValueType_SEGMENT || value.SegmentPath != segmentPath {
			continue
		}
		userKey := keys.ExtractUserKey(keyBytes)
		if value.SegmentOffset < 0 || value.ValueLength <= 0 {
			return false, fmt.Errorf("invalid metadata location for %q in %s", userKey, segmentPath)
		}
		indexKey := keys.MakeSegmentLiveIndexKey(segmentPath, value.SegmentOffset)
		indexSlice, err := sr.meta.Handle().Get(pointRO, indexKey)
		if err != nil {
			if indexSlice != nil {
				indexSlice.Free()
			}
			return false, err
		}
		if indexSlice == nil {
			return false, fmt.Errorf("nil live index lookup result for offset %d in %s", value.SegmentOffset, segmentPath)
		}
		needsRepair := !indexSlice.Exists()
		duplicate := false
		if !needsRepair {
			row, decodeErr := keys.DecodeSegmentLiveIndexEntry(indexSlice.Data())
			if decodeErr == nil && row.Key != userKey {
				duplicate = true
			} else {
				needsRepair = decodeErr != nil || row.ValueLength != value.ValueLength || row.HeaderSize != segment.CalculateValueHeaderSize(userKey) || row.Checksum != value.Checksum || row.HeaderVersion > segment.CurrentValueHeaderVersion
			}
		}
		indexSlice.Free()
		if duplicate {
			return false, fmt.Errorf("duplicate metadata offset %d in %s", value.SegmentOffset, segmentPath)
		}
		if needsRepair {
			entry, err := physicalEntry(value.SegmentOffset, userKey, value.ValueLength, value.Checksum)
			if err != nil {
				return false, err
			}
			if err := stageSegmentLiveIndexEntry(batch, segmentPath, entry); err != nil {
				return false, err
			}
			repaired = true
			if batch.Count() >= 1000 {
				if err := flush(); err != nil {
					return false, err
				}
			}
		}
	}
	if err := it.Err(); err != nil {
		return false, err
	}
	if err := flush(); err != nil {
		return false, err
	}
	if repaired {
		return false, nil
	}
	return true, nil
}
