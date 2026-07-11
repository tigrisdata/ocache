// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package segment

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestBuildAndReadValueHeader(t *testing.T) {
	key := "foo"
	valueLen := int64(12345)
	checksum := uint32(0xDEADBEEF)
	version := uint16(CurrentValueHeaderVersion)

	header := BuildValueHeader(key, valueLen, checksum, version)

	tmp, err := os.CreateTemp(t.TempDir(), "header-*.seg")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmp.Close()

	// Write header bytes to file so that ReadValueHeader can pread.
	if _, err := tmp.Write(header); err != nil {
		t.Fatalf("write header: %v", err)
	}

	// Parse using implementation under test.
	gotValueLen, gotHdrSize, gotKeyLen, gotVersion, gotChecksum, err := ReadValueHeader(tmp)
	if err != nil {
		t.Fatalf("ReadValueHeader returned error: %v", err)
	}

	if gotValueLen != valueLen {
		t.Errorf("valueLen mismatch: got %d, want %d", gotValueLen, valueLen)
	}
	expectedHdrSize := int64(ValueHeaderSize + len(key))
	if gotHdrSize != expectedHdrSize {
		t.Errorf("headerSize mismatch: got %d, want %d", gotHdrSize, expectedHdrSize)
	}
	if gotKeyLen != int64(len(key)) {
		t.Errorf("keyLen mismatch: got %d, want %d", gotKeyLen, len(key))
	}
	if gotVersion != version {
		t.Errorf("version mismatch: got %d, want %d", gotVersion, version)
	}
	if gotChecksum != checksum {
		t.Errorf("checksum mismatch: got %x, want %x", gotChecksum, checksum)
	}
}

func TestCalculateValueHeaderSize(t *testing.T) {
	key := "someKey"
	if got, want := CalculateValueHeaderSize(key), int64(ValueHeaderSize+len(key)); got != want {
		t.Errorf("CalculateValueHeaderSize returned %d, want %d", got, want)
	}
}

func TestUpdateValueHeader(t *testing.T) {
	key := "bar"
	header := BuildValueHeader(key, 100, 0xAAAA, 1)

	newLen := int64(200)
	newChecksum := uint32(0x12345678)

	UpdateValueHeader(header, newLen, newChecksum)

	gotLen := int64(binary.BigEndian.Uint64(header[0:8]))
	if gotLen != newLen {
		t.Errorf("value length after update: got %d, want %d", gotLen, newLen)
	}
	gotChecksum := binary.BigEndian.Uint32(header[12:16])
	if gotChecksum != newChecksum {
		t.Errorf("checksum after update: got %x, want %x", gotChecksum, newChecksum)
	}
}

func TestBuildAndParseSegmentFooter(t *testing.T) {
	version := 2
	entries := uint32(10)
	dataBytes := int64(4096)

	footer := BuildSegmentFooterWithVersion(version, entries, dataBytes)

	gotVer, gotEntries, gotBytes, ok := ParseSegmentFooter(footer)
	if !ok {
		t.Fatalf("ParseSegmentFooter reported invalid footer")
	}
	if gotVer != version {
		t.Errorf("version mismatch: got %d, want %d", gotVer, version)
	}
	if gotEntries != entries {
		t.Errorf("entries mismatch: got %d, want %d", gotEntries, entries)
	}
	if gotBytes != dataBytes {
		t.Errorf("dataBytes mismatch: got %d, want %d", gotBytes, dataBytes)
	}
}

func TestParseSegmentFooterInvalidPrefix(t *testing.T) {
	bad := make([]byte, SegmentFooterSize)
	copy(bad, []byte("BADBAD"))
	if _, _, _, ok := ParseSegmentFooter(bad); ok {
		t.Fatalf("expected invalid footer to return ok=false")
	}
}
