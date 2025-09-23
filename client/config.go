package cacheclient

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ConnectionMode defines how the client connects to servers
type ConnectionMode string

const (
	// ModeAuto automatically detects cluster vs simple mode (default)
	ModeAuto ConnectionMode = "auto"
	// ModeSimple uses direct connections without topology service
	ModeSimple ConnectionMode = "simple"
	// ModeCluster uses topology service for smart routing
	ModeCluster ConnectionMode = "cluster"
)

const (
	// DefaultRefreshInterval is the default topology refresh interval
	DefaultRefreshInterval = 30 * time.Second
	// MaxMessageSize is the maximum message size for gRPC
	MaxMessageSize = 128 * 1024 * 1024 // 128MB
	// TopologyDetectTimeout is the timeout for detecting cluster topology
	TopologyDetectTimeout = 2 * time.Second
	// DefaultBufferSize is the default buffer size for streaming operations
	DefaultBufferSize = 64 * 1024 // 64KB
	// ConnectionHealthCheckInterval is the interval for checking connection health
	ConnectionHealthCheckInterval = 30 * time.Second
	// ConnectionErrorWindow is the time window for tracking connection errors
	ConnectionErrorWindow = 30 * time.Second
	// DefaultConnectionPoolSize is the default number of connections per address
	DefaultConnectionPoolSize = 4
)

// ClientConfig contains configuration for the unified Client
type ClientConfig struct {
	Addrs              []string          // One or more server addresses
	Mode               ConnectionMode    // Connection mode (default: "auto")
	RefreshInterval    time.Duration     // Topology refresh for cluster mode (default: 30s)
	ConnectionPoolSize int               // Number of connections per address (default: 4)
	DialOpts           []grpc.DialOption // Optional gRPC dial options
}

// SetDefaults sets default values for unspecified config fields
func (c *ClientConfig) SetDefaults() {
	if c.Mode == "" {
		c.Mode = ModeAuto
	}
	if c.RefreshInterval == 0 {
		c.RefreshInterval = DefaultRefreshInterval
	}
	if c.ConnectionPoolSize <= 0 {
		c.ConnectionPoolSize = DefaultConnectionPoolSize
	}
	if len(c.DialOpts) == 0 {
		c.DialOpts = DefaultDialOptions()
	}
}

// DefaultDialOptions returns the default gRPC dial options
func DefaultDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageSize),
			grpc.MaxCallSendMsgSize(MaxMessageSize),
		),
	}
}
