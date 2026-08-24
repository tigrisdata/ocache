// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package files

import (
	"bytes"
	"hash/crc32"
	"io"
	"testing"
)

func TestCopyRawFilePayloadMultiReader(t *testing.T) {
	firstChunk := []byte("first chunk")
	remainder := bytes.Repeat([]byte("x"), 64*1024)
	reader := io.MultiReader(bytes.NewReader(firstChunk), bytes.NewReader(remainder))
	want := append(append([]byte(nil), firstChunk...), remainder...)

	var got bytes.Buffer
	checksum, bytesWritten, err := copyRawFilePayload(&got, reader)
	if err != nil {
		t.Fatalf("copy raw-file payload: %v", err)
	}
	if gotBytes, wantBytes := bytesWritten, int64(len(want)); gotBytes != wantBytes {
		t.Errorf("bytes written = %d, want %d", gotBytes, wantBytes)
	}
	if wantChecksum := crc32.ChecksumIEEE(want); checksum != wantChecksum {
		t.Errorf("checksum = %d, want %d", checksum, wantChecksum)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Error("copied payload did not preserve byte order")
	}
}
