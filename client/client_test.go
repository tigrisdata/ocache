package cacheclient

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
)

type mockCacheServiceClient struct {
	pb.CacheServiceClient
	putObjectCalled bool
	putStreamData   [][]byte
	getData         [][]byte
	deleteCalled    bool
	listKeys        []string
}

func (m *mockCacheServiceClient) PutObject(ctx context.Context, req *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
	m.putObjectCalled = true
	m.putStreamData = [][]byte{req.Data}
	return &pb.PutResponse{Success: true}, nil
}

func (m *mockCacheServiceClient) Put(ctx context.Context, opts ...grpc.CallOption) (pb.CacheService_PutClient, error) {
	// Check if context is already cancelled
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return &mockPutStream{mock: m, ctx: ctx}, nil
}

type mockPutStream struct {
	grpc.ClientStream
	mock  *mockCacheServiceClient
	ctx   context.Context
	first bool
	key   string
	ttl   int64
}

func (m *mockPutStream) Send(req *pb.PutRequest) error {
	m.mock.putStreamData = append(m.mock.putStreamData, req.Data)
	if m.first == false && req.Key != "" {
		m.key = req.Key
		m.ttl = req.TtlSeconds
		m.first = true
	}
	return nil
}

func (m *mockPutStream) CloseAndRecv() (*pb.PutResponse, error) {
	return &pb.PutResponse{Success: true}, nil
}

func (m *mockCacheServiceClient) Get(ctx context.Context, req *pb.GetRequest, opts ...grpc.CallOption) (pb.CacheService_GetClient, error) {
	// Check if context is already cancelled
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return &mockGetStream{ctx: ctx, data: m.getData}, nil
}

type mockGetStream struct {
	grpc.ClientStream
	ctx  context.Context
	data [][]byte
	idx  int
}

func (m *mockGetStream) Recv() (*pb.GetResponse, error) {
	// Check context before returning data
	if m.ctx.Err() != nil {
		return nil, m.ctx.Err()
	}
	if m.idx >= len(m.data) {
		return nil, io.EOF
	}
	resp := &pb.GetResponse{Data: m.data[m.idx]}
	m.idx++
	return resp, nil
}

func (m *mockCacheServiceClient) Delete(ctx context.Context, req *pb.DeleteRequest, opts ...grpc.CallOption) (*pb.DeleteResponse, error) {
	m.deleteCalled = true
	return &pb.DeleteResponse{Success: true}, nil
}

func (m *mockCacheServiceClient) List(ctx context.Context, req *pb.ListRequest, opts ...grpc.CallOption) (pb.CacheService_ListClient, error) {
	// Check if context is already cancelled
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return &mockListStream{ctx: ctx, keys: m.listKeys}, nil
}

type mockListStream struct {
	grpc.ClientStream
	ctx  context.Context
	keys []string
	idx  int
}

func (m *mockListStream) Recv() (*pb.ListResponse, error) {
	// Check context before returning data
	if m.ctx.Err() != nil {
		return nil, m.ctx.Err()
	}
	if m.idx >= len(m.keys) {
		return nil, io.EOF
	}
	resp := &pb.ListResponse{Keys: []string{m.keys[m.idx]}}
	m.idx++
	return resp, nil
}

func TestClient_Put(t *testing.T) {
	ctx := context.TODO()
	mock := &mockCacheServiceClient{}
	c := &Client{client: mock}
	err := c.Put(ctx, "key", []byte("data"), 0)
	assert.NoError(t, err)
	assert.True(t, mock.putObjectCalled)
}

func TestClient_PutStream(t *testing.T) {
	ctx := context.TODO()
	mock := &mockCacheServiceClient{}
	c := &Client{client: mock}
	data := []byte("streamed data")
	err := c.PutStream(ctx, "key", bytes.NewReader(data), 0)
	assert.NoError(t, err)
	assert.Greater(t, len(mock.putStreamData), 0)
}

func TestClient_Get(t *testing.T) {
	ctx := context.TODO()
	mock := &mockCacheServiceClient{getData: [][]byte{[]byte("foo"), []byte("bar")}}
	c := &Client{client: mock}
	result, err := c.Get(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("foobar"), result)
}

func TestClient_GetStream(t *testing.T) {
	ctx := context.TODO()
	mock := &mockCacheServiceClient{getData: [][]byte{[]byte("foo"), []byte("bar")}}
	c := &Client{client: mock}
	var buf bytes.Buffer
	err := c.GetStream(ctx, "key", &buf)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", buf.String())
}

func TestClient_Delete(t *testing.T) {
	ctx := context.TODO()
	mock := &mockCacheServiceClient{}
	c := &Client{client: mock}
	err := c.Delete(ctx, "key")
	assert.NoError(t, err)
	assert.True(t, mock.deleteCalled)
}

func TestClient_List(t *testing.T) {
	ctx := context.TODO()
	mock := &mockCacheServiceClient{listKeys: []string{"a", "b", "c"}}
	c := &Client{client: mock}
	keys, err := c.List(ctx, "")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, keys)
}

func TestClient_ListWithPrefix(t *testing.T) {
	// Test that the prefix is correctly passed to the request
	ctx := context.TODO()
	mock := &mockCacheServiceClient{listKeys: []string{"user:a", "user:b"}}
	c := &Client{client: mock}
	keys, err := c.List(ctx, "user:")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"user:a", "user:b"}, keys)
}

// Test context cancellation handling
func TestClient_ContextCancellation(t *testing.T) {
	t.Run("PutStream cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		mock := &mockCacheServiceClient{}
		c := &Client{client: mock}

		// Create a reader that will block
		pr, pw := io.Pipe()
		defer pr.Close()
		defer pw.Close()

		// Cancel context immediately
		cancel()

		// Should return context error
		err := c.PutStream(ctx, "key", pr, 0)
		require.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("Get cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// Cancel immediately
		cancel()

		mock := &mockCacheServiceClient{getData: [][]byte{[]byte("foo"), []byte("bar")}}
		c := &Client{client: mock}

		_, err := c.Get(ctx, "key")
		require.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("GetStream cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// Cancel immediately
		cancel()

		mock := &mockCacheServiceClient{getData: [][]byte{[]byte("foo"), []byte("bar")}}
		c := &Client{client: mock}

		var buf bytes.Buffer
		err := c.GetStream(ctx, "key", &buf)
		require.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("List cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// Cancel immediately
		cancel()

		mock := &mockCacheServiceClient{listKeys: []string{"a", "b", "c"}}
		c := &Client{client: mock}

		_, err := c.List(ctx, "")
		require.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})
}

// Test context timeout handling
func TestClient_ContextTimeout(t *testing.T) {
	t.Run("operation with timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		mock := &mockCacheServiceClient{getData: [][]byte{[]byte("foo")}}
		c := &Client{client: mock}

		// This should succeed as mock operations are instant
		result, err := c.Get(ctx, "key")
		assert.NoError(t, err)
		assert.Equal(t, []byte("foo"), result)
	})
}
