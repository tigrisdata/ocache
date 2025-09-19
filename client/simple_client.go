package cacheclient

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/tigrisdata/ocache/common/bufferpool"
	pb "github.com/tigrisdata/ocache/proto"
)

// SimpleClient implements a simple round-robin cache client
type SimpleClient struct {
	conns      []*connection // Array of connections
	addresses  []string      // List of addresses for consistent ordering
	currentIdx atomic.Uint32 // Round-robin index
	config     *ClientConfig
	mu         sync.RWMutex
}

// NewSimpleClient creates a new SimpleClient with the given configuration
func NewSimpleClient(config *ClientConfig) (*SimpleClient, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if len(config.Addrs) == 0 {
		return nil, fmt.Errorf("at least one address is required")
	}

	config.SetDefaults()

	client := &SimpleClient{
		config:    config,
		addresses: config.Addrs,
		conns:     make([]*connection, 0, len(config.Addrs)),
	}

	// Create connections for each address
	var lastErr error
	for _, addr := range client.addresses {
		conn, err := newConnection(addr, config.DialOpts)
		if err != nil {
			lastErr = fmt.Errorf("failed to create connection for %s: %w", addr, err)
			// Continue trying other addresses
			continue
		}
		client.conns = append(client.conns, conn)
	}

	// Require at least one successful connection
	if len(client.conns) == 0 {
		if lastErr != nil {
			return nil, fmt.Errorf("failed to create any connections, last error: %w", lastErr)
		}
		return nil, fmt.Errorf("failed to create any connections")
	}

	return client, nil
}

// route selects a connection using hash-based routing for better key locality
func (c *SimpleClient) route(key string) (*connection, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.conns) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	// Use hash-based routing for better key locality
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	// Select connection based on hash
	idx := hash % uint32(len(c.conns))
	return c.conns[idx], nil
}

// roundRobinRoute selects a connection using round-robin (for operations without keys)
func (c *SimpleClient) roundRobinRoute() (*connection, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.conns) == 0 {
		return nil, fmt.Errorf("no available connections")
	}

	idx := c.currentIdx.Add(1) - 1
	return c.conns[idx%uint32(len(c.conns))], nil
}

// Put stores a value in the cache
func (c *SimpleClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	req := &pb.PutRequest{Key: key, Data: data, TtlSeconds: ttlSeconds}
	_, err = conn.getClient().PutObject(ctx, req)
	conn.recordError(err)
	return err
}

// PutStream streams data to the cache
func (c *SimpleClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	stream, err := conn.getClient().Put(ctx)
	if err != nil {
		return err
	}

	// Get buffer from pool
	buf, release := bufferpool.AcquireBuffer(DefaultBufferSize)
	defer release()

	first := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := r.Read(buf)
		if n > 0 {
			req := &pb.PutRequest{Data: buf[:n]}
			if first {
				req.Key = key
				req.TtlSeconds = ttlSeconds
				first = false
			}
			if sendErr := stream.Send(req); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return fmt.Errorf("put failed: %s", resp.Error)
	}
	return nil
}

// Get retrieves a value from the cache
func (c *SimpleClient) Get(ctx context.Context, key string) ([]byte, error) {
	conn, err := c.route(key)
	if err != nil {
		return nil, err
	}

	stream, err := conn.getClient().Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}

	// Pre-allocate result slice with initial capacity to reduce allocations
	result := make([]byte, 0, DefaultBufferSize)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, resp.Data...)
	}
	return result, nil
}

// GetStream streams a value from the cache
func (c *SimpleClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	stream, err := conn.getClient().Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := w.Write(resp.Data); err != nil {
			return err
		}
	}
	return nil
}

// GetRange retrieves a byte range from the cache
func (c *SimpleClient) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	conn, err := c.route(key)
	if err != nil {
		return nil, err
	}

	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}

	stream, err := conn.getClient().Get(ctx, req)
	if err != nil {
		return nil, err
	}

	result := make([]byte, 0, DefaultBufferSize)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, resp.Data...)
	}
	return result, nil
}

// GetRangeStream streams a byte range from the cache
func (c *SimpleClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}

	stream, err := conn.getClient().Get(ctx, req)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := w.Write(resp.Data); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes a key from the cache
func (c *SimpleClient) Delete(ctx context.Context, key string) error {
	conn, err := c.route(key)
	if err != nil {
		return err
	}

	_, err = conn.getClient().Delete(ctx, &pb.DeleteRequest{Key: key})
	return err
}

// List lists keys with optional prefix
func (c *SimpleClient) List(ctx context.Context, prefix string) ([]string, error) {
	conn, err := c.roundRobinRoute()
	if err != nil {
		return nil, err
	}

	stream, err := conn.getClient().List(ctx, &pb.ListRequest{Prefix: prefix})
	if err != nil {
		return nil, err
	}

	var keys []string
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		keys = append(keys, resp.Keys...)
	}
	return keys, nil
}

// Close closes all connections
func (c *SimpleClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, conn := range c.conns {
		if err := conn.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	c.conns = nil
	return firstErr
}

// GetMode returns the connection mode
func (c *SimpleClient) GetMode() ConnectionMode {
	return ModeSimple
}

// GetConnectedNodes returns the addresses of all connected nodes
func (c *SimpleClient) GetConnectedNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]string, 0, len(c.conns))
	for _, conn := range c.conns {
		nodes = append(nodes, conn.address)
	}
	sort.Strings(nodes)
	return nodes
}
