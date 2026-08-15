// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"bytes"
	"errors"
	"io"
	"testing"

	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
)

// mockGetClient is a mock pb.CacheService_GetClient that yields a scripted
// sequence of data chunks, then either an error (tailErr) or io.EOF.
type mockGetClient struct {
	grpc.ClientStream // embedded so the unused stream methods satisfy the interface
	chunks            [][]byte
	tailErr           error
	i                 int
}

func (m *mockGetClient) Recv() (*pb.GetResponse, error) {
	if m.i < len(m.chunks) {
		c := m.chunks[m.i]
		m.i++
		return &pb.GetResponse{Data: c}, nil
	}
	if m.tailErr != nil {
		err := m.tailErr
		m.tailErr = nil
		return nil, err
	}
	return nil, io.EOF
}

func TestGrpcStreamReader_ReassemblesChunks(t *testing.T) {
	r := &grpcStreamReader{stream: &mockGetClient{chunks: [][]byte{
		[]byte("hello "), []byte("world"), []byte("!"),
	}}}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello world!" {
		t.Fatalf("got %q, want %q", got, "hello world!")
	}
}

// TestGrpcStreamReader_SmallBufferDrains exercises the pending-buffer path: a
// chunk larger than the read buffer must be handed out across multiple Reads
// without re-calling Recv or dropping bytes.
func TestGrpcStreamReader_SmallBufferDrains(t *testing.T) {
	r := &grpcStreamReader{stream: &mockGetClient{chunks: [][]byte{[]byte("abcdef")}}}
	buf := make([]byte, 2)
	var out []byte
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if string(out) != "abcdef" {
		t.Fatalf("got %q, want %q", out, "abcdef")
	}
}

// TestGrpcStreamReader_PropagatesError verifies that data received before a
// mid-stream error is still delivered, and the error surfaces afterward.
func TestGrpcStreamReader_PropagatesError(t *testing.T) {
	wantErr := errors.New("boom")
	r := &grpcStreamReader{stream: &mockGetClient{
		chunks:  [][]byte{[]byte("part")},
		tailErr: wantErr,
	}}
	got, err := io.ReadAll(r)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if string(got) != "part" {
		t.Fatalf("got %q, want %q", got, "part")
	}
}

func TestGrpcStreamReader_EmptyStream(t *testing.T) {
	r := &grpcStreamReader{stream: &mockGetClient{}}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %q, want empty", got)
	}
}

// TestGrpcStreamReader_ReleasesOnEOF verifies the reader tears down its stream
// when drained to EOF, without the caller having to call Close.
func TestGrpcStreamReader_ReleasesOnEOF(t *testing.T) {
	released := false
	r := &grpcStreamReader{
		stream: &mockGetClient{chunks: [][]byte{[]byte("x")}},
		cancel: func() { released = true },
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !released {
		t.Fatal("stream not released on EOF")
	}
}

// TestGrpcStreamReader_ReleasesOnError verifies teardown also happens when the
// stream ends in an error rather than EOF.
func TestGrpcStreamReader_ReleasesOnError(t *testing.T) {
	released := false
	r := &grpcStreamReader{
		stream: &mockGetClient{chunks: [][]byte{[]byte("x")}, tailErr: errors.New("boom")},
		cancel: func() { released = true },
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("expected error")
	}
	if !released {
		t.Fatal("stream not released on error")
	}
}

func TestGrpcStreamReader_CloseCancels(t *testing.T) {
	cancelled := false
	r := &grpcStreamReader{stream: &mockGetClient{}, cancel: func() { cancelled = true }}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !cancelled {
		t.Fatal("Close did not invoke cancel")
	}
	// Close must be safe to call again (e.g. after full consumption).
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// peerChunkWriter retains the slices it receives so tests can verify that
// WriteTo passes peer response data directly to the destination.
type peerChunkWriter struct {
	chunks [][]byte
}

func (w *peerChunkWriter) Write(p []byte) (int, error) {
	w.chunks = append(w.chunks, p)
	return len(p), nil
}

func TestGrpcStreamReader_WriteToWritesPeerChunksDirectly(t *testing.T) {
	chunks := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	released := false
	r := &grpcStreamReader{
		stream:  &mockGetClient{chunks: chunks[1:]},
		pending: chunks[0],
		cancel:  func() { released = true },
	}
	w := &peerChunkWriter{}

	written, err := io.Copy(w, r)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if want := int64(len("firstsecondthird")); written != want {
		t.Fatalf("written = %d, want %d", written, want)
	}
	if len(w.chunks) != len(chunks) {
		t.Fatalf("writes = %d, want %d", len(w.chunks), len(chunks))
	}
	for i, chunk := range chunks {
		if !bytes.Equal(w.chunks[i], chunk) {
			t.Errorf("chunk %d = %q, want %q", i, w.chunks[i], chunk)
		}
		if &w.chunks[i][0] != &chunk[0] {
			t.Errorf("chunk %d was copied before Write", i)
		}
	}
	if !released {
		t.Fatal("stream not released on EOF")
	}
}

type maxChunkWriter struct {
	limit  int
	chunks [][]byte
}

func (w *maxChunkWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		return 0, errors.New("write exceeds limit")
	}
	w.chunks = append(w.chunks, p)
	return len(p), nil
}

func TestGrpcStreamReader_WriteToMatchesCopyWriteSize(t *testing.T) {
	chunk := bytes.Repeat([]byte("x"), 2*grpcStreamReaderWriteChunkSize)
	r := &grpcStreamReader{pending: chunk, stream: &mockGetClient{}}
	w := &maxChunkWriter{limit: grpcStreamReaderWriteChunkSize}

	written, err := io.Copy(w, r)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if written != int64(len(chunk)) {
		t.Fatalf("written = %d, want %d", written, len(chunk))
	}
	if len(w.chunks) != 2 {
		t.Fatalf("writes = %d, want 2", len(w.chunks))
	}
	for i, got := range w.chunks {
		if len(got) != grpcStreamReaderWriteChunkSize {
			t.Errorf("write %d length = %d, want %d", i, len(got), grpcStreamReaderWriteChunkSize)
		}
		if &got[0] != &chunk[i*grpcStreamReaderWriteChunkSize] {
			t.Errorf("write %d was copied before Write", i)
		}
	}
}

func TestGrpcStreamReader_WriteToPropagatesError(t *testing.T) {
	wantErr := errors.New("boom")
	r := &grpcStreamReader{
		stream:  &mockGetClient{chunks: [][]byte{[]byte("second")}, tailErr: wantErr},
		pending: []byte("first"),
	}
	var out bytes.Buffer

	written, err := io.Copy(&out, r)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Copy error = %v, want %v", err, wantErr)
	}
	if written != int64(len("firstsecond")) {
		t.Fatalf("written = %d, want %d", written, len("firstsecond"))
	}
	if got := out.String(); got != "firstsecond" {
		t.Fatalf("output = %q, want %q", got, "firstsecond")
	}
}

type shortWriter struct {
	limit int
}

func (w shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		return w.limit, nil
	}
	return len(p), nil
}

func TestGrpcStreamReader_WriteToPreservesShortWriteRemainder(t *testing.T) {
	r := &grpcStreamReader{
		stream:  &mockGetClient{chunks: [][]byte{[]byte("ef")}},
		pending: []byte("abcd"),
	}

	written, err := r.WriteTo(shortWriter{limit: 2})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteTo error = %v, want %v", err, io.ErrShortWrite)
	}
	if written != 2 {
		t.Fatalf("written = %d, want 2", written)
	}
	if got := string(r.pending); got != "cd" {
		t.Fatalf("pending = %q, want %q", got, "cd")
	}

	var out bytes.Buffer
	if _, err := io.Copy(&out, r); err != nil {
		t.Fatalf("Copy remainder: %v", err)
	}
	if got := out.String(); got != "cdef" {
		t.Fatalf("remainder = %q, want %q", got, "cdef")
	}
}
