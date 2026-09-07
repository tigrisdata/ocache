// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"bytes"
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
	if err != nil || !coverage.HasWitness || coverage.Entries != seg.GetNumEntries() || coverage.DataBytes != seg.GetDataBytes() || coverage.Size != dataSize {
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
	if err := verifySegmentLiveWitnessSet(meta, seg.Path()); err != nil {
		return err
	}
	value, err := keys.EncodeSegmentLiveIndexCoverage(keys.SegmentLiveIndexCoverage{
		Entries:    seg.GetNumEntries(),
		DataBytes:  seg.GetDataBytes(),
		Size:       dataSize,
		HasWitness: true,
	})
	if err != nil {
		return err
	}
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	return meta.Handle().Put(wo, keys.MakeSegmentLiveCoverageKey(seg.Path()), value)
}

// verifySegmentLiveWitnessSet checks the redundant per-segment membership rows
// before publishing a version-2 coverage marker. It touches only live-index
// rows, never the database-wide metadata namespace.
func verifySegmentLiveWitnessSet(meta *metadata.MetaDB, segmentPath string) error {
	if meta == nil || segmentPath == "" {
		return fmt.Errorf("invalid segment live witness arguments")
	}
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	pointRO := metadata.CreateReadOptions(false, false)
	defer pointRO.Destroy()

	primaryCount := 0
	primary := meta.Handle().NewIterator(ro)
	primaryPrefix := keys.MakeSegmentLiveIndexPrefix(segmentPath)
	for primary.Seek(primaryPrefix); primary.ValidForPrefix(primaryPrefix); primary.Next() {
		primaryKey := append([]byte(nil), primary.Key().Data()...)
		primaryValue := append([]byte(nil), primary.Value().Data()...)
		primary.Key().Free()
		primary.Value().Free()
		path, offset, ok := keys.ParseSegmentLiveIndexKey(primaryKey)
		if !ok || path != segmentPath {
			primary.Close()
			return fmt.Errorf("malformed live index row for %s", segmentPath)
		}
		if _, err := keys.DecodeSegmentLiveIndexEntry(primaryValue); err != nil {
			primary.Close()
			return fmt.Errorf("malformed live index row at offset %d in %s: %w", offset, segmentPath, err)
		}
		witnessKey := keys.MakeSegmentLiveIndexWitnessKey(segmentPath, offset)
		slice, err := meta.Handle().Get(pointRO, witnessKey)
		if err != nil {
			primary.Close()
			return err
		}
		if slice == nil {
			primary.Close()
			return fmt.Errorf("nil witness lookup at offset %d in %s", offset, segmentPath)
		}
		matches := slice.Exists() && bytes.Equal(slice.Data(), primaryValue)
		slice.Free()
		if !matches {
			primary.Close()
			return fmt.Errorf("live index witness missing or mismatched at offset %d in %s", offset, segmentPath)
		}
		primaryCount++
	}
	if err := primary.Err(); err != nil {
		primary.Close()
		return err
	}
	primary.Close()

	witnessCount := 0
	witness := meta.Handle().NewIterator(ro)
	prefix := keys.MakeSegmentLiveIndexWitnessPrefix(segmentPath)
	for witness.Seek(prefix); witness.ValidForPrefix(prefix); witness.Next() {
		key := append([]byte(nil), witness.Key().Data()...)
		value := append([]byte(nil), witness.Value().Data()...)
		witness.Key().Free()
		witness.Value().Free()
		path, _, ok := keys.ParseSegmentLiveIndexWitnessKey(key)
		if !ok || path != segmentPath {
			witness.Close()
			return fmt.Errorf("malformed live index witness for %s", segmentPath)
		}
		if _, err := keys.DecodeSegmentLiveIndexEntry(value); err != nil {
			witness.Close()
			return fmt.Errorf("malformed live index witness in %s: %w", segmentPath, err)
		}
		witnessCount++
	}
	if err := witness.Err(); err != nil {
		witness.Close()
		return err
	}
	witness.Close()
	if witnessCount != primaryCount {
		return fmt.Errorf("live index witness count %d does not match primary count %d for %s", witnessCount, primaryCount, segmentPath)
	}
	return nil
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
	witnessKey := keys.MakeSegmentLiveIndexWitnessKey(segmentPath, entry.Offset)
	if indexKey == nil || witnessKey == nil {
		return fmt.Errorf("invalid segment live index location: path=%q offset=%d", segmentPath, entry.Offset)
	}
	// The witness is deliberately written beside the primary row. A lost
	// primary row can then be repaired from this bounded per-segment set without
	// a database-wide metadata scan.
	batch.Put(indexKey, value)
	batch.Put(witnessKey, value)
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
	witnessKey := keys.MakeSegmentLiveIndexWitnessKey(segmentPath, offset)
	if indexKey == nil || witnessKey == nil {
		return fmt.Errorf("invalid segment live index location: path=%q offset=%d", segmentPath, offset)
	}
	batch.Delete(indexKey)
	batch.Delete(witnessKey)
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
	for _, prefix := range [][]byte{
		keys.MakeSegmentLiveIndexPrefix(segmentPath),
		keys.MakeSegmentLiveIndexWitnessPrefix(segmentPath),
	} {
		it := meta.Handle().NewIterator(ro)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			batch.Delete(append([]byte(nil), it.Key().Data()...))
			it.Key().Free()
			it.Value().Free()
		}
		if err := it.Err(); err != nil {
			it.Close()
			return err
		}
		it.Close()
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
	if !ok {
		path, offset, ok = keys.ParseSegmentLiveIndexWitnessKey(indexKey)
	}
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

// verifySegmentLiveIndexComplete is a bounded guard before deleting a source
// segment. Version-2 coverage maintains one witness row beside every primary
// live-location row. The check walks those two per-segment prefixes and only
// performs metadata point lookups for indexed candidates; it never scans the
// database-wide metadata namespace. A missing primary row is repaired from the
// witness after a bounded physical header/key check, and the source is retained
// for a later pass.
func (sr *SegmentRecompactor) verifySegmentLiveIndexComplete(segmentPath string, oldSeg *segment.Segment) (complete bool, err error) {
	dataSize, _, err := segmentDataRegionSize(oldSeg)
	if err != nil {
		return false, err
	}
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	pointRO := metadata.CreateReadOptions(false, false)
	defer pointRO.Destroy()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	retainSource := false

	var sourceFile *os.File
	defer func() {
		if sourceFile != nil {
			_ = sourceFile.Close()
		}
	}()
	physicalEntry := func(entry *segment.EntryInfo) error {
		if sourceFile == nil {
			var openErr error
			sourceFile, openErr = os.Open(segmentPath)
			if openErr != nil {
				return openErr
			}
		}
		valueLength, headerSize, keyLength, version, checksum, readErr := segment.ReadValueHeaderAt(sourceFile, entry.Offset)
		if readErr != nil {
			return readErr
		}
		if entry.Offset < 0 || entry.Offset > dataSize || headerSize <= 0 || headerSize > dataSize-entry.Offset ||
			valueLength <= 0 || valueLength > dataSize-entry.Offset-headerSize ||
			keyLength <= 0 || int64(segment.ValueHeaderSize)+keyLength != headerSize ||
			keyLength > int64(^uint(0)>>1) {
			return fmt.Errorf("source entry at offset %d in %s exceeds the data region", entry.Offset, segmentPath)
		}
		if headerSize != entry.HeaderSize || valueLength != entry.ValueLength || checksum != entry.Checksum || version != entry.Version {
			return fmt.Errorf("source facts disagree with live index for %q at offset %d in %s", entry.Key, entry.Offset, segmentPath)
		}
		keyBytes := make([]byte, int(keyLength))
		if _, readErr := sourceFile.ReadAt(keyBytes, entry.Offset+int64(segment.ValueHeaderSize)); readErr != nil {
			return readErr
		}
		if string(keyBytes) != entry.Key {
			return fmt.Errorf("source key disagrees with live index for %q at offset %d in %s", entry.Key, entry.Offset, segmentPath)
		}
		return nil
	}

	checkPrimary := func(entry *segment.EntryInfo, rowValue []byte) error {
		primaryKey := keys.MakeSegmentLiveIndexKey(segmentPath, entry.Offset)
		slice, getErr := sr.meta.Handle().Get(pointRO, primaryKey)
		if getErr != nil {
			if slice != nil {
				slice.Free()
			}
			return getErr
		}
		if slice == nil {
			return fmt.Errorf("nil live index lookup at offset %d in %s", entry.Offset, segmentPath)
		}
		matches := slice.Exists() && bytes.Equal(slice.Data(), rowValue)
		missing := !slice.Exists()
		slice.Free()
		if matches {
			return nil
		}
		if err := physicalEntry(entry); err != nil {
			return err
		}
		if err := stageSegmentLiveIndexEntry(batch, segmentPath, entry); err != nil {
			return err
		}
		if missing {
			// A missing primary row is the fault this witness protects against.
			// Keep the source until a later pass copies the repaired location.
			retainSource = true
		}
		return nil
	}

	// Witness rows are the audit side: a lost primary row leaves the witness
	// behind, so the missing location can be reconstructed without finding all
	// metadata rows in the database.
	witnessPrefix := keys.MakeSegmentLiveIndexWitnessPrefix(segmentPath)
	witnessIt := sr.meta.Handle().NewIterator(ro)
	for witnessIt.Seek(witnessPrefix); witnessIt.ValidForPrefix(witnessPrefix); witnessIt.Next() {
		indexKey := append([]byte(nil), witnessIt.Key().Data()...)
		indexValue := append([]byte(nil), witnessIt.Value().Data()...)
		witnessIt.Key().Free()
		witnessIt.Value().Free()
		entry, decodeErr := indexedSegmentEntry(indexKey, indexValue, oldSeg, dataSize)
		if decodeErr != nil {
			witnessIt.Close()
			return false, decodeErr
		}
		_, live, validateErr := validateIndexedMetadata(sr.meta, entry, indexValue, segmentPath)
		if validateErr != nil {
			witnessIt.Close()
			return false, validateErr
		}
		if !live {
			if err := stageSegmentLiveIndexDelete(batch, segmentPath, entry.Offset); err != nil {
				witnessIt.Close()
				return false, err
			}
			continue
		}
		if err := checkPrimary(entry, indexValue); err != nil {
			witnessIt.Close()
			return false, err
		}
	}
	if err := witnessIt.Err(); err != nil {
		witnessIt.Close()
		return false, err
	}
	witnessIt.Close()

	// Check the reverse direction too. This repairs a witness lost while its
	// primary row survived, keeping the pair atomic for the next cleanup pass.
	primaryPrefix := keys.MakeSegmentLiveIndexPrefix(segmentPath)
	primaryIt := sr.meta.Handle().NewIterator(ro)
	for primaryIt.Seek(primaryPrefix); primaryIt.ValidForPrefix(primaryPrefix); primaryIt.Next() {
		indexKey := append([]byte(nil), primaryIt.Key().Data()...)
		indexValue := append([]byte(nil), primaryIt.Value().Data()...)
		primaryIt.Key().Free()
		primaryIt.Value().Free()
		entry, decodeErr := indexedSegmentEntry(indexKey, indexValue, oldSeg, dataSize)
		if decodeErr != nil {
			primaryIt.Close()
			return false, decodeErr
		}
		_, live, validateErr := validateIndexedMetadata(sr.meta, entry, indexValue, segmentPath)
		if validateErr != nil {
			primaryIt.Close()
			return false, validateErr
		}
		if !live {
			if err := stageSegmentLiveIndexDelete(batch, segmentPath, entry.Offset); err != nil {
				primaryIt.Close()
				return false, err
			}
			continue
		}
		witnessKey := keys.MakeSegmentLiveIndexWitnessKey(segmentPath, entry.Offset)
		slice, getErr := sr.meta.Handle().Get(pointRO, witnessKey)
		if getErr != nil {
			if slice != nil {
				slice.Free()
			}
			primaryIt.Close()
			return false, getErr
		}
		if slice == nil {
			primaryIt.Close()
			return false, fmt.Errorf("nil witness lookup at offset %d in %s", entry.Offset, segmentPath)
		}
		matches := slice.Exists() && bytes.Equal(slice.Data(), indexValue)
		slice.Free()
		if !matches {
			if err := physicalEntry(entry); err != nil {
				primaryIt.Close()
				return false, err
			}
			if err := stageSegmentLiveIndexEntry(batch, segmentPath, entry); err != nil {
				primaryIt.Close()
				return false, err
			}
		}
	}
	if err := primaryIt.Err(); err != nil {
		primaryIt.Close()
		return false, err
	}
	primaryIt.Close()

	if batch.Count() > 0 {
		wo := grocksdb.NewDefaultWriteOptions()
		defer wo.Destroy()
		if err := sr.meta.Handle().Write(wo, batch); err != nil {
			return false, err
		}
	}
	if retainSource {
		return false, nil
	}
	return true, nil
}
