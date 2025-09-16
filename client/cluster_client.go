package cacheclient

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClusterClient is a cluster-aware cache client that connects to seed nodes
// and relies on server-side routing for request distribution
type ClusterClient struct {
	seedAddrs  []string
	clients    []*Client // Clients for each seed node
	currentIdx int32     // Current client index for round-robin
	mu         sync.RWMutex
	stopCh     chan struct{}
}

// NewClusterClient creates a new cluster-aware client that uses server-side routing
func NewClusterClient(seedAddrs []string, opts ...grpc.DialOption) (*ClusterClient, error) {
	if len(seedAddrs) == 0 {
		return nil, fmt.Errorf("at least one seed address is required")
	}

	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Set max message sizes for streaming
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(128*1024*1024), // 128MB
		grpc.MaxCallSendMsgSize(128*1024*1024), // 128MB
	))

	c := &ClusterClient{
		seedAddrs: seedAddrs,
		clients:   make([]*Client, 0, len(seedAddrs)),
		stopCh:    make(chan struct{}),
	}

	// Connect to all seed nodes
	for _, addr := range seedAddrs {
		client, err := New(addr, opts...)
		if err != nil {
			// Log error but continue - we want to connect to as many as possible
			continue
		}
		c.clients = append(c.clients, client)
	}

	if len(c.clients) == 0 {
		return nil, fmt.Errorf("failed to connect to any seed nodes")
	}

	// Start health check loop
	go c.healthCheckLoop()

	return c, nil
}

// healthCheckLoop periodically checks connection health and reconnects if needed
func (c *ClusterClient) healthCheckLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.checkAndReconnect()
		case <-c.stopCh:
			return
		}
	}
}

// checkAndReconnect checks connection health and reconnects failed connections
func (c *ClusterClient) checkAndReconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check each client's connection state
	for i, client := range c.clients {
		if client == nil || !client.isHealthy() {
			// Try to reconnect
			addr := c.seedAddrs[i]
			newClient, err := New(addr)
			if err == nil {
				// Close old client if it exists
				if client != nil {
					client.Close()
				}
				c.clients[i] = newClient
			}
		}
	}
}

// getClient returns an available client using round-robin with failover
func (c *ClusterClient) getClient() (*Client, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.clients) == 0 {
		return nil, fmt.Errorf("no available clients")
	}

	// Try each client starting from current index
	startIdx := atomic.LoadInt32(&c.currentIdx)
	for i := 0; i < len(c.clients); i++ {
		idx := (int(startIdx) + i) % len(c.clients)
		client := c.clients[idx]

		if client != nil && client.isHealthy() {
			// Update current index for next request (round-robin)
			atomic.StoreInt32(&c.currentIdx, int32((idx+1)%len(c.clients)))
			return client, nil
		}
	}

	return nil, fmt.Errorf("no healthy clients available")
}

// Close closes all connections
func (c *ClusterClient) Close() error {
	close(c.stopCh)

	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for _, client := range c.clients {
		if client != nil {
			if err := client.Close(); err != nil {
				lastErr = err
			}
		}
	}

	return lastErr
}

// Put stores a value in the cache (server-side routing)
func (c *ClusterClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	client, err := c.getClient()
	if err != nil {
		return err
	}
	return client.Put(ctx, key, data, ttlSeconds)
}

// PutStream streams data to the cache (server-side routing)
func (c *ClusterClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	client, err := c.getClient()
	if err != nil {
		return err
	}
	return client.PutStream(ctx, key, r, ttlSeconds)
}

// Get retrieves a value from the cache (server-side routing)
func (c *ClusterClient) Get(ctx context.Context, key string) ([]byte, error) {
	client, err := c.getClient()
	if err != nil {
		return nil, err
	}
	return client.Get(ctx, key)
}

// GetStream streams a value from the cache (server-side routing)
func (c *ClusterClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	client, err := c.getClient()
	if err != nil {
		return err
	}
	return client.GetStream(ctx, key, w)
}

// GetRange retrieves a byte range from the cache (server-side routing)
func (c *ClusterClient) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	client, err := c.getClient()
	if err != nil {
		return nil, err
	}
	return client.GetRange(ctx, key, start, end)
}

// GetRangeStream streams a byte range from the cache (server-side routing)
func (c *ClusterClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	client, err := c.getClient()
	if err != nil {
		return err
	}
	return client.GetRangeStream(ctx, key, start, end, w)
}

// Delete removes a key from the cache (server-side routing)
func (c *ClusterClient) Delete(ctx context.Context, key string) error {
	client, err := c.getClient()
	if err != nil {
		return err
	}
	return client.Delete(ctx, key)
}

// List lists all keys with optional prefix (server-side routing)
func (c *ClusterClient) List(ctx context.Context, prefix string) ([]string, error) {
	client, err := c.getClient()
	if err != nil {
		return nil, err
	}
	return client.List(ctx, prefix)
}

// GetConnectedNodes returns the addresses of connected seed nodes
func (c *ClusterClient) GetConnectedNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	connected := make([]string, 0, len(c.clients))
	for i, client := range c.clients {
		if client != nil && client.isHealthy() {
			connected = append(connected, c.seedAddrs[i])
		}
	}
	return connected
}
