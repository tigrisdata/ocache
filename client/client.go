package cacheclient

import (
	"context"
	"fmt"

	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
)

// Client is a wrapper that maintains backward compatibility
// It delegates to either SimpleClient or ClusterClient based on mode detection
type Client struct {
	CacheClient
	mode ConnectionMode
}

// New creates a new client with default configuration
func New(addrs ...string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("at least one address is required")
	}
	return NewWithConfig(&ClientConfig{
		Addrs: addrs,
		Mode:  ModeAuto,
	})
}

// NewWithConfig creates a new client with custom configuration
func NewWithConfig(config *ClientConfig) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if len(config.Addrs) == 0 {
		return nil, fmt.Errorf("at least one address is required")
	}

	config.SetDefaults()

	// Resolve auto mode
	mode := config.Mode
	if mode == ModeAuto {
		mode = detectMode(config.Addrs, config.DialOpts)
	}

	// Create appropriate client based on mode
	var cacheClient CacheClient
	var err error

	switch mode {
	case ModeCluster:
		cacheClient, err = NewClusterClient(config)
	case ModeSimple:
		cacheClient, err = NewSimpleClient(config)
	default:
		return nil, fmt.Errorf("unknown mode: %s", mode)
	}

	if err != nil {
		return nil, err
	}

	return &Client{
		CacheClient: cacheClient,
		mode:        mode,
	}, nil
}

// detectMode attempts to detect if cluster topology is available
func detectMode(addrs []string, dialOpts []grpc.DialOption) ConnectionMode {
	ctx, cancel := context.WithTimeout(context.Background(), TopologyDetectTimeout)
	defer cancel()

	// Try to fetch topology from any seed address
	for _, addr := range addrs {
		conn, err := grpc.DialContext(ctx, addr, dialOpts...)
		if err != nil {
			continue
		}

		cacheClient := pb.NewCacheServiceClient(conn)
		resp, err := cacheClient.GetTopology(ctx, &pb.GetTopologyRequest{})
		conn.Close()

		if err == nil && resp != nil && resp.Error == "" && resp.Topology != nil {
			// Successfully fetched topology
			return ModeCluster
		}
	}

	// No topology service available, use simple mode
	return ModeSimple
}

// GetMode returns the actual connection mode being used
func (c *Client) GetMode() ConnectionMode {
	return c.mode
}

// Additional methods for backward compatibility with tests

// GetTopologyEpoch returns the current topology epoch (cluster mode only)
func (c *Client) GetTopologyEpoch() uint64 {
	if cc, ok := c.CacheClient.(*ClusterClient); ok {
		return cc.GetTopologyEpoch()
	}
	return 0
}

// HasRing returns true if the consistent hash ring is initialized (cluster mode only)
func (c *Client) HasRing() bool {
	if cc, ok := c.CacheClient.(*ClusterClient); ok {
		return cc.HasRing()
	}
	return false
}

// GetPartitionOwner returns the node ID that owns the given partition (cluster mode only)
func (c *Client) GetPartitionOwner(partitionID int32) string {
	if cc, ok := c.CacheClient.(*ClusterClient); ok {
		return cc.GetPartitionOwner(partitionID)
	}
	return ""
}

// GetPartitionOwnerCount returns the number of partition owners (cluster mode only)
func (c *Client) GetPartitionOwnerCount() int {
	if cc, ok := c.CacheClient.(*ClusterClient); ok {
		return cc.GetPartitionOwnerCount()
	}
	return 0
}
