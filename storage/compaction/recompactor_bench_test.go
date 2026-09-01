//go:build linux

// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"testing"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
	"github.com/tigrisdata/ocache/storage/segment"
	"google.golang.org/protobuf/proto"
)

// These benchmark encodings are kept local so this measurement support commit
// also builds against the case base. The implementation commit consumes the
// same wire format from storage/keys.
const (
	benchmarkSegmentLivePrefix    = "!segment-live/"
	benchmarkSegmentCoveredPrefix = "!segment-live-covered/"
	benchmarkSegmentLiveVersion   = byte(1)
)

func benchmarkSegmentLiveKey(path string, offset int64) []byte {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(path))
	key := make([]byte, len(benchmarkSegmentLivePrefix)+len(encoded)+1+8)
	copy(key, benchmarkSegmentLivePrefix)
	copy(key[len(benchmarkSegmentLivePrefix):], encoded)
	key[len(benchmarkSegmentLivePrefix)+len(encoded)] = '/'
	binary.BigEndian.PutUint64(key[len(key)-8:], uint64(offset))
	return key
}

func benchmarkSegmentLiveRow(key string, valueLength, headerSize int64, checksum uint32, headerVersion uint16) []byte {
	row := make([]byte, 1+4+len(key)+8+8+4+2)
	row[0] = benchmarkSegmentLiveVersion
	binary.BigEndian.PutUint32(row[1:5], uint32(len(key)))
	pos := 5
	copy(row[pos:], key)
	pos += len(key)
	binary.BigEndian.PutUint64(row[pos:pos+8], uint64(valueLength))
	pos += 8
	binary.BigEndian.PutUint64(row[pos:pos+8], uint64(headerSize))
	pos += 8
	binary.BigEndian.PutUint32(row[pos:pos+4], checksum)
	pos += 4
	binary.BigEndian.PutUint16(row[pos:pos+2], headerVersion)
	return row
}

func benchmarkSegmentCoveredKey(path string) []byte {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(path))
	return []byte(benchmarkSegmentCoveredPrefix + encoded)
}

func benchmarkSegmentCoveredValue(entries uint32, dataBytes, size int64) []byte {
	value := make([]byte, 1+4+8+8)
	value[0] = benchmarkSegmentLiveVersion
	binary.BigEndian.PutUint32(value[1:5], entries)
	binary.BigEndian.PutUint64(value[5:13], uint64(dataBytes))
	binary.BigEndian.PutUint64(value[13:21], uint64(size))
	return value
}

// BenchmarkRecompactFragmentedSegment measures the ordinary full recompaction
// path for a closed segment with a fixed eight-entry live set and increasing
// dead history. The fixture is rebuilt outside the timed region on every
// iteration so the metric is the recompaction operation, not test setup.
//
// The coverage row makes the candidate arm eligible for the indexed path while
// the base arm ignores this unknown internal namespace and performs its
// historical scan. The benchmark therefore distinguishes the proposed source
// change without changing the ordinary workload or output contract.
func BenchmarkRecompactFragmentedSegment(b *testing.B) {
	for _, dead := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("dead-%d", dead), func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				recompactor, sm, meta, _, cleanup := setupTestRecompactor(b)
				seg, err := createBenchmarkRecompactionSegment(b, sm, meta, dead)
				if err != nil {
					cleanup()
					b.Fatal(err)
				}
				recompactor.minSegments = 1
				recompactor.fragThreshold = 0.01
				b.StartTimer()
				if err := recompactor.RecompactFragmentedSegments(b.Context()); err != nil {
					b.StopTimer()
					cleanup()
					b.Fatal(err)
				}
				b.StopTimer()
				if len(sm.GetSegments()) == 0 || seg == nil {
					cleanup()
					b.Fatal("recompaction removed every segment")
				}
				cleanup()
			}
		})
	}
}

func createBenchmarkRecompactionSegment(tb testing.TB, sm *segment.Manager, meta *metadata.MetaDB, dead int) (*segment.Segment, error) {
	tb.Helper()
	entries := make(map[string][]byte, 8+dead)
	for i := range 8 {
		entries[fmt.Sprintf("live-%03d", i)] = bytes.Repeat([]byte{'l'}, 256)
	}
	for i := range dead {
		entries[fmt.Sprintf("dead-%03d", i)] = bytes.Repeat([]byte{'d'}, 256)
	}

	seg, err := sm.AcquireOpenSegmentWithReservation("benchmark", 0)
	if err != nil {
		return nil, err
	}
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	for key, value := range entries {
		offset, err := seg.WriteEntry(key, bytes.NewReader(value), &pb.ValueMessage{
			ValueType:   pb.ValueType_SEGMENT,
			ValueLength: int64(len(value)),
		})
		if err != nil {
			return nil, err
		}
		if len(key) >= len("live-") && key[:len("live-")] == "live-" {
			vm := &pb.ValueMessage{
				ValueType:     pb.ValueType_SEGMENT,
				SegmentPath:   seg.Path(),
				SegmentOffset: offset,
				ValueLength:   int64(len(value)),
			}
			metaBytes, err := proto.Marshal(vm)
			if err != nil {
				return nil, err
			}
			batch.Put(keys.MakeMetadataKey(key), metaBytes)
			batch.Put(benchmarkSegmentLiveKey(seg.Path(), offset), benchmarkSegmentLiveRow(
				key, int64(len(value)), segment.CalculateValueHeaderSize(key), 0, segment.CurrentValueHeaderVersion,
			))
		}
	}
	writeOptions := grocksdb.NewDefaultWriteOptions()
	defer writeOptions.Destroy()
	if err := meta.Handle().Write(writeOptions, batch); err != nil {
		return nil, err
	}
	if err := sm.FinalizeSegment(seg); err != nil {
		return nil, err
	}

	// Remove dead metadata after finalization, then publish the coverage
	// fingerprint. The base recompactor ignores both rows; the candidate uses
	// them to avoid parsing dead source records.
	deleteBatch := grocksdb.NewWriteBatch()
	defer deleteBatch.Destroy()
	for i := range dead {
		deleteBatch.Delete(keys.MakeMetadataKey(fmt.Sprintf("dead-%03d", i)))
	}
	if err := meta.Handle().Write(writeOptions, deleteBatch); err != nil {
		return nil, err
	}
	coverage := benchmarkSegmentCoveredValue(seg.GetNumEntries(), seg.GetDataBytes(), seg.GetSize())
	if err := meta.Handle().Put(writeOptions, benchmarkSegmentCoveredKey(seg.Path()), coverage); err != nil {
		return nil, err
	}
	return seg, nil
}
