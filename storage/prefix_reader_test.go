// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/tigrisdata/ocache/common/bufferpool"
)

type prefixReaderReadRecorder struct {
	io.Reader
	firstReadSize int
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) {
	return f(p)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) {
	return f(p)
}

type writerToReadRecorder struct {
	reader       *bytes.Reader
	writeToCalls int
}

type writeCallCounter struct {
	writer io.Writer
	calls  int
	sizes  []int
}

func (w *writeCallCounter) Write(p []byte) (int, error) {
	w.calls++
	w.sizes = append(w.sizes, len(p))
	return w.writer.Write(p)
}

func (r *writerToReadRecorder) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *writerToReadRecorder) WriteTo(writer io.Writer) (int64, error) {
	r.writeToCalls++
	return r.reader.WriteTo(writer)
}

func (r *prefixReaderReadRecorder) Read(p []byte) (int, error) {
	if r.firstReadSize == 0 {
		r.firstReadSize = len(p)
	}
	return r.Reader.Read(p)
}

func TestPrefixReaderDoesNotReadRemainderBeforePrefixDrained(t *testing.T) {
	prefix := []byte("first chunk")
	reader := &prefixReader{
		prefix: prefix,
		reader: readerFunc(func([]byte) (int, error) {
			t.Fatal("read remainder before draining prefix")
			return 0, nil
		}),
	}

	buffer := make([]byte, len(prefix))
	bytesRead, err := reader.Read(buffer)
	if err != nil {
		t.Fatalf("read prefix: %v", err)
	}
	if bytesRead != len(prefix) || !bytes.Equal(buffer, prefix) {
		t.Errorf("read = (%d, %q), want (%d, %q)", bytesRead, buffer, len(prefix), prefix)
	}
}

func TestJoinPrefixUsesCopyBufferForReaderOnlyRemainder(t *testing.T) {
	prefix := []byte("first chunk")
	remainder := bytes.Repeat([]byte("x"), 64*1024)
	trackedRemainder := &prefixReaderReadRecorder{Reader: bytes.NewReader(remainder)}
	reader := joinPrefix(prefix, trackedRemainder)
	buffer := make([]byte, 1<<20)
	var got bytes.Buffer
	var writeSizes []int

	if _, ok := reader.(io.WriterTo); ok {
		t.Fatal("reader-only remainder unexpectedly preserves io.WriterTo")
	}
	bytesWritten, err := io.CopyBuffer(writerFunc(func(p []byte) (int, error) {
		writeSizes = append(writeSizes, len(p))
		return got.Write(p)
	}), reader, buffer)
	if err != nil {
		t.Fatalf("copy prefix and remainder: %v", err)
	}
	want := append(append([]byte(nil), prefix...), remainder...)
	if bytesWritten != int64(len(want)) || !bytes.Equal(got.Bytes(), want) {
		t.Errorf("copy = (%d, %q), want (%d, %q)", bytesWritten, got.Bytes(), len(want), want)
	}
	if got, wantSize := trackedRemainder.firstReadSize, len(buffer); got != wantSize {
		t.Errorf("first remainder read buffer size = %d, want %d", got, wantSize)
	}
	if len(writeSizes) != 2 || writeSizes[0] != len(prefix) || writeSizes[1] != len(remainder) {
		t.Errorf("write sizes = %v, want [%d %d]", writeSizes, len(prefix), len(remainder))
	}
}

func TestJoinPrefixRegularFileUsesPooledBuffer(t *testing.T) {
	const total = 1 << 20
	prefixSize := DefaultInlineThreshold + 1
	want := bytes.Repeat([]byte("abcdefgh"), total/8)

	dir := t.TempDir()
	source, err := os.Create(filepath.Join(dir, "source"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Write(want); err != nil {
		source.Close()
		t.Fatal(err)
	}
	info, err := source.Stat()
	if err != nil {
		source.Close()
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		source.Close()
		t.Fatalf("source mode = %v, want regular file", info.Mode())
	}
	if _, err := source.Seek(int64(prefixSize), io.SeekStart); err != nil {
		source.Close()
		t.Fatal(err)
	}
	defer source.Close()

	output, err := os.Create(filepath.Join(dir, "output"))
	if err != nil {
		t.Fatal(err)
	}
	counter := &writeCallCounter{writer: output}
	reader := joinPrefix(want[:prefixSize], source)
	_, hasWriterTo := reader.(io.WriterTo)
	buffer, release := bufferpool.AcquireBuffer(1 << 20)
	defer release()
	hash := crc32.NewIEEE()

	bytesWritten, err := io.CopyBuffer(io.MultiWriter(counter, hash), reader, buffer)
	if err != nil {
		output.Close()
		t.Fatalf("copy regular-file remainder: %v", err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "output"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("regular-file copy changed payload order or identity")
	}
	if bytesWritten != int64(len(want)) {
		t.Errorf("bytes written = %d, want %d", bytesWritten, len(want))
	}
	if gotChecksum, wantChecksum := hash.Sum32(), crc32.ChecksumIEEE(want); gotChecksum != wantChecksum {
		t.Errorf("checksum = %08x, want %08x", gotChecksum, wantChecksum)
	}
	if hasWriterTo {
		t.Error("regular-file remainder unexpectedly preserves io.WriterTo")
	}
	if counter.calls != 2 || len(counter.sizes) != 2 || counter.sizes[0] != prefixSize || counter.sizes[1] != total-prefixSize {
		t.Errorf("file-boundary writes = %d calls with sizes %v, want 2 calls [%d %d]", counter.calls, counter.sizes, prefixSize, total-prefixSize)
	}
}

func TestJoinPrefixPreservesRemainderWriterTo(t *testing.T) {
	prefix := []byte("first chunk")
	remainder := []byte("remainder")
	trackedRemainder := &writerToReadRecorder{reader: bytes.NewReader(remainder)}
	reader := joinPrefix(prefix, trackedRemainder)
	buffer := make([]byte, 1<<20)
	var got bytes.Buffer

	if _, ok := reader.(io.WriterTo); !ok {
		t.Fatal("WriterTo remainder did not preserve io.WriterTo")
	}
	bytesWritten, err := io.CopyBuffer(writerFunc(got.Write), reader, buffer)
	if err != nil {
		t.Fatalf("copy prefix and WriterTo remainder: %v", err)
	}
	want := append(append([]byte(nil), prefix...), remainder...)
	if bytesWritten != int64(len(want)) || !bytes.Equal(got.Bytes(), want) {
		t.Errorf("copy = (%d, %q), want (%d, %q)", bytesWritten, got.Bytes(), len(want), want)
	}
	if trackedRemainder.writeToCalls != 1 {
		t.Errorf("remainder WriteTo calls = %d, want 1", trackedRemainder.writeToCalls)
	}
}

func TestPrefixWriterToReaderStopsBeforeRemainderOnShortPrefixWrite(t *testing.T) {
	trackedRemainder := &writerToReadRecorder{reader: bytes.NewReader([]byte("remainder"))}
	reader := joinPrefix([]byte("prefix"), trackedRemainder)
	writer, ok := reader.(io.WriterTo)
	if !ok {
		t.Fatal("WriterTo remainder did not preserve io.WriterTo")
	}

	bytesWritten, err := writer.WriteTo(writerFunc(func(p []byte) (int, error) {
		return len(p) - 1, nil
	}))
	if bytesWritten != int64(len("prefix")-1) {
		t.Errorf("bytes written = %d, want %d", bytesWritten, len("prefix")-1)
	}
	if err != io.ErrShortWrite {
		t.Errorf("write error = %v, want %v", err, io.ErrShortWrite)
	}
	if trackedRemainder.writeToCalls != 0 {
		t.Errorf("remainder WriteTo calls = %d, want 0", trackedRemainder.writeToCalls)
	}
}
