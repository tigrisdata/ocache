package operations

import (
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
