// Package embedded provides an embedded ocache client for use in other services.
// This allows services like TAG to embed ocache and get full cluster-aware
// caching with metrics, routing, and cluster-wide list operations.
package embedded

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	zlog "github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/ocache/coordinator"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/operations"
	"github.com/tigrisdata/ocache/server/service"
	stor "github.com/tigrisdata/ocache/storage"
	"google.golang.org/grpc"
)

// Config contains configuration for the embedded cache client.
type Config struct {
	// DiskPath is the path to the cache data directory (required)
	DiskPath string

	// TTL is the default time-to-live for cache entries (required)
	TTL time.Duration

	// MaxDiskUsage is the maximum disk usage in bytes (0 = unlimited)
	MaxDiskUsage int64

	// InlineThreshold is the size threshold for inline vs file storage (default: 64KB)
	// Objects smaller than this are stored in RocksDB, larger ones as files.
	InlineThreshold int

	// NodeID is the unique identifier for this node in cluster mode
	NodeID string

	// ClusterAddr is the address for cluster membership (gossip) protocol
	// Example: ":7000"
	ClusterAddr string

	// GRPCAddr is the address for the gRPC server to listen on
	// Example: ":9000"
	GRPCAddr string

	// AdvertiseAddr is the address advertised to other nodes for gRPC connections
	// Example: "node1.cluster:9000"
	AdvertiseAddr string

	// SeedNodes is a list of seed nodes for cluster discovery
	// Example: []string{"node1:7000", "node2:7000"}
	SeedNodes []string

	// RequestLogging enables logging of gRPC requests (default: false)
	RequestLogging bool
}

// SetDefaults sets default values for unspecified config fields.
func (c *Config) SetDefaults() {
	if c.InlineThreshold <= 0 {
		c.InlineThreshold = 64 * 1024 // 64KB default
	}
}

// Validate checks that required configuration is provided.
func (c *Config) Validate() error {
	if c.DiskPath == "" {
		return errors.New("DiskPath is required")
	}
	if c.TTL <= 0 {
		return errors.New("TTL must be positive")
	}
	return nil
}

// IsClusterMode returns true if cluster configuration is provided.
func (c *Config) IsClusterMode() bool {
	return c.NodeID != "" && c.ClusterAddr != ""
}

// Client provides embedded cache access with cluster routing.
// It implements the cacheclient.CacheClient interface.
type Client struct {
	config      *Config
	storage     *stor.Storage
	coordinator *coordinator.Coordinator
	ops         *operations.Operations
	service     *service.CacheService
	grpcServer  *grpc.Server
	grpcLis     net.Listener
}

// New creates a new embedded cache client.
// The client provides direct access to local storage with automatic routing
// to remote nodes when running in cluster mode.
func New(cfg *Config) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("config is required")
	}

	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	zlog.Info().
		Str("disk_path", cfg.DiskPath).
		Dur("ttl", cfg.TTL).
		Int64("max_disk_usage", cfg.MaxDiskUsage).
		Bool("cluster_mode", cfg.IsClusterMode()).
		Str("node_id", cfg.NodeID).
		Msg("Creating embedded cache client")

	// Create storage
	storageConfig := &stor.StorageConfig{
		DiskPath:        cfg.DiskPath,
		TTL:             int(cfg.TTL.Seconds()),
		MaxDiskUsage:    cfg.MaxDiskUsage,
		InlineThreshold: cfg.InlineThreshold,
	}
	storage, err := stor.NewStorageWithConfig(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	// Create coordinator if cluster mode is enabled
	var coord *coordinator.Coordinator
	if cfg.IsClusterMode() {
		coordCfg := &coordinator.Config{
			Enabled:       true,
			MyNodeID:      cfg.NodeID,
			ClusterAddr:   cfg.ClusterAddr,
			ListenAddr:    cfg.GRPCAddr,
			AdvertiseAddr: cfg.AdvertiseAddr,
			Seeds:         cfg.SeedNodes,
			DiskPath:      cfg.DiskPath,
		}
		coord, err = coordinator.New(coordCfg)
		if err != nil {
			storage.Close()
			return nil, fmt.Errorf("failed to create coordinator: %w", err)
		}
	}

	// Create operations layer
	ops := operations.New(storage, coord)

	// Create cache service (for gRPC server)
	svc := service.NewCacheService(coord, storage)

	return &Client{
		config:      cfg,
		storage:     storage,
		coordinator: coord,
		ops:         ops,
		service:     svc,
	}, nil
}

// StartGRPCServer starts the gRPC server for handling remote requests.
// This must be called in cluster mode to allow other nodes to route requests here.
func (c *Client) StartGRPCServer() error {
	if c.config.GRPCAddr == "" {
		return errors.New("GRPCAddr not configured")
	}

	var opts []grpc.ServerOption
	opts = append(opts,
		grpc.MaxRecvMsgSize(128*1024*1024), // 128MB
		grpc.MaxSendMsgSize(128*1024*1024), // 128MB
	)

	// Add epoch interceptors if cluster mode is enabled
	if c.coordinator != nil {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(coordinator.UnaryServerEpochInterceptor(c.coordinator.GetEpoch)),
			grpc.ChainStreamInterceptor(coordinator.StreamServerEpochInterceptor(c.coordinator.GetEpoch)),
		)
	}

	c.grpcServer = grpc.NewServer(opts...)
	pb.RegisterCacheServiceServer(c.grpcServer, c.service)

	// Register ClusterService if clustering is enabled
	if c.coordinator != nil {
		clusterpb.RegisterClusterServiceServer(c.grpcServer, c.coordinator)
	}

	var err error
	c.grpcLis, err = net.Listen("tcp", c.config.GRPCAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", c.config.GRPCAddr, err)
	}

	zlog.Info().Str("addr", c.config.GRPCAddr).Msg("Starting embedded gRPC server")

	// Start serving in a goroutine
	go func() {
		if err := c.grpcServer.Serve(c.grpcLis); err != nil {
			zlog.Error().Err(err).Msg("gRPC server error")
		}
	}()

	return nil
}

// WaitReady waits for the client to be ready to serve requests.
// In cluster mode, this waits for the coordinator to reach ACTIVE state.
func (c *Client) WaitReady(ctx context.Context) error {
	if c.coordinator == nil {
		return nil
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if c.coordinator.IsReady() {
				zlog.Info().Msg("Embedded cache client is ready")
				return nil
			}
		}
	}
}

// IsReady returns true if the client is ready to serve requests.
func (c *Client) IsReady() bool {
	return c.ops.IsReady()
}

// --- CacheClient interface implementation ---

// Put stores data for the given key.
func (c *Client) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	return c.ops.PutBytes(ctx, key, data, int(ttlSeconds))
}

// Get retrieves data for the given key.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	data, found, err := c.ops.GetBytes(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil // Return nil, nil for not found (consistent with gRPC client)
	}
	return data, nil
}

// Delete removes a key from the cache.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.ops.Delete(ctx, key)
}

// List returns all keys matching the given prefix across the entire cluster.
func (c *Client) List(ctx context.Context, prefix string) ([]string, error) {
	return c.ops.List(ctx, prefix)
}

// ListPage returns a page of keys with pagination support.
func (c *Client) ListPage(ctx context.Context, prefix string, limit int, continuationToken string) (keys []string, nextToken string, hasMore bool, err error) {
	return c.ops.ListPage(ctx, prefix, limit, continuationToken)
}

// PutStream stores data from a reader for the given key.
func (c *Client) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	return c.ops.Put(ctx, key, r, int(ttlSeconds))
}

// GetStream retrieves data and writes it to the provided writer.
func (c *Client) GetStream(ctx context.Context, key string, w io.Writer) error {
	found, err := c.ops.GetStream(ctx, key, w)
	if err != nil {
		return err
	}
	if !found {
		return nil // Return nil for not found (caller can check bytes written)
	}
	return nil
}

// GetRange retrieves a byte range for the given key.
func (c *Client) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	data, found, err := c.ops.GetRange(ctx, key, start, end)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return data, nil
}

// GetRangeStream retrieves a byte range and writes it to the provided writer.
func (c *Client) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	found, err := c.ops.GetRangeStream(ctx, key, start, end, w)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	return nil
}

// Close shuts down the embedded client and releases all resources.
func (c *Client) Close() error {
	zlog.Info().Msg("Closing embedded cache client")

	var errs []error

	// Stop gRPC server
	if c.grpcServer != nil {
		c.grpcServer.GracefulStop()
	}

	// Close gRPC listener
	if c.grpcLis != nil {
		if err := c.grpcLis.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close gRPC listener: %w", err))
		}
	}

	// Stop coordinator
	if c.coordinator != nil {
		if err := c.coordinator.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop coordinator: %w", err))
		}
	}

	// Close storage (doesn't return an error)
	if c.storage != nil {
		c.storage.Close()
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// GetMode returns the connection mode.
func (c *Client) GetMode() cacheclient.ConnectionMode {
	if c.coordinator != nil {
		return cacheclient.ModeCluster
	}
	return cacheclient.ModeSimple
}

// GetConnectedNodes returns the list of connected nodes.
func (c *Client) GetConnectedNodes() []string {
	if c.coordinator == nil {
		return []string{c.config.NodeID}
	}

	ring := c.coordinator.GetRing()
	if ring == nil {
		return []string{c.config.NodeID}
	}

	nodes := ring.GetActiveNodes()
	result := make([]string, 0, len(nodes))
	for _, n := range nodes {
		result = append(result, n.ID)
	}
	return result
}

// --- Additional embedded-specific methods ---

// Operations returns the underlying operations layer.
// This provides direct access to the routing logic for advanced use cases.
func (c *Client) Operations() *operations.Operations {
	return c.ops
}

// Storage returns the underlying storage layer.
// This provides direct access to local storage for advanced use cases.
func (c *Client) Storage() *stor.Storage {
	return c.storage
}

// Coordinator returns the underlying coordinator.
// Returns nil if clustering is not enabled.
func (c *Client) Coordinator() *coordinator.Coordinator {
	return c.coordinator
}

// Service returns the gRPC service.
// This is useful for registering additional handlers or middleware.
func (c *Client) Service() *service.CacheService {
	return c.service
}

// GetGRPCServer returns the gRPC server.
// Returns nil if StartGRPCServer has not been called.
func (c *Client) GetGRPCServer() *grpc.Server {
	return c.grpcServer
}

// Compile-time check that Client implements CacheClient
var _ cacheclient.CacheClient = (*Client)(nil)
