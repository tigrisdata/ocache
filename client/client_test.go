package cacheclient

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
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
	return &mockPutStream{mock: m}, nil
}

type mockPutStream struct {
	grpc.ClientStream
	mock  *mockCacheServiceClient
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
	return &mockGetStream{data: m.getData}, nil
}

type mockGetStream struct {
	grpc.ClientStream
	data [][]byte
	idx  int
}

func (m *mockGetStream) Recv() (*pb.GetResponse, error) {
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
	return &mockListStream{keys: m.listKeys}, nil
}

type mockListStream struct {
	grpc.ClientStream
	keys []string
	idx  int
}

func (m *mockListStream) Recv() (*pb.ListResponse, error) {
	if m.idx >= len(m.keys) {
		return nil, io.EOF
	}
	resp := &pb.ListResponse{Keys: []string{m.keys[m.idx]}}
	m.idx++
	return resp, nil
}

func TestClient_Put(t *testing.T) {
	mock := &mockCacheServiceClient{}
	c := &Client{client: mock}
	err := c.Put(context.Background(), "key", []byte("data"), 0)
	assert.NoError(t, err)
	assert.True(t, mock.putObjectCalled)
}

func TestClient_PutStream(t *testing.T) {
	mock := &mockCacheServiceClient{}
	c := &Client{client: mock}
	data := []byte("streamed data")
	err := c.PutStream(context.Background(), "key", bytes.NewReader(data), 0)
	assert.NoError(t, err)
	assert.Greater(t, len(mock.putStreamData), 0)
}

func TestClient_Get(t *testing.T) {
	mock := &mockCacheServiceClient{getData: [][]byte{[]byte("foo"), []byte("bar")}}
	c := &Client{client: mock}
	result, err := c.Get(context.Background(), "key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("foobar"), result)
}

func TestClient_GetStream(t *testing.T) {
	mock := &mockCacheServiceClient{getData: [][]byte{[]byte("foo"), []byte("bar")}}
	c := &Client{client: mock}
	var buf bytes.Buffer
	err := c.GetStream(context.Background(), "key", &buf)
	assert.NoError(t, err)
	assert.Equal(t, "foobar", buf.String())
}

func TestClient_Delete(t *testing.T) {
	mock := &mockCacheServiceClient{}
	c := &Client{client: mock}
	err := c.Delete(context.Background(), "key")
	assert.NoError(t, err)
	assert.True(t, mock.deleteCalled)
}

func TestClient_List(t *testing.T) {
	mock := &mockCacheServiceClient{listKeys: []string{"a", "b", "c"}}
	c := &Client{client: mock}
	keys, err := c.List(context.Background())
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, keys)
}
