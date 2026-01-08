package ring

import (
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
)

const (
	// DefaultHeartbeatPeriod is the default interval for heartbeat updates
	DefaultHeartbeatPeriod = 5 * time.Second

	// DefaultHeartbeatTimeout is the default timeout before marking a node unhealthy.
	// Set long enough to handle rolling updates (60s allows for typical pod restarts).
	DefaultHeartbeatTimeout = 60 * time.Second

	// DefaultNumTokens is the default number of tokens per instance.
	// 512 provides good distribution across the ring.
	DefaultNumTokens = 512

	// DefaultReplicationFactor is the default replication factor (1 = no replication)
	DefaultReplicationFactor = 1

	// RingName is the name used for the ring in metrics and KV store
	RingName = "ocache"

	// RingKey is the key used to store the ring in the KV store
	RingKey = "ring"
)

// Config holds configuration for the dskit ring integration
type Config struct {
	// KVStore configures the key-value store backend (memberlist)
	KVStore kv.Config `yaml:"kvstore"`

	// HeartbeatPeriod is the interval at which this instance sends heartbeats
	HeartbeatPeriod time.Duration `yaml:"heartbeat_period"`

	// HeartbeatTimeout is the time after which an instance is considered unhealthy
	// if no heartbeat is received. Should be long enough for rolling updates.
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`

	// ReplicationFactor is the number of replicas for each key (1 = no replication)
	ReplicationFactor int `yaml:"replication_factor"`
}

// RegisterFlags registers the ring configuration flags
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	c.KVStore.RegisterFlagsWithPrefix("ring.", "Ring key-value store configuration.", f)

	f.DurationVar(&c.HeartbeatPeriod, "ring.heartbeat-period", DefaultHeartbeatPeriod,
		"Interval at which this instance sends heartbeats to the ring")
	f.DurationVar(&c.HeartbeatTimeout, "ring.heartbeat-timeout", DefaultHeartbeatTimeout,
		"Time after which an instance is considered unhealthy. Should be >= 2x heartbeat period")
	f.IntVar(&c.ReplicationFactor, "ring.replication-factor", DefaultReplicationFactor,
		"Number of replicas for each key (1 = no replication)")
}

// ApplyDefaults applies default values for any unset or invalid configuration fields.
// This should be called before using the configuration.
func (c *Config) ApplyDefaults() {
	if c.HeartbeatPeriod <= 0 {
		c.HeartbeatPeriod = DefaultHeartbeatPeriod
	}
	if c.HeartbeatTimeout < 2*c.HeartbeatPeriod {
		c.HeartbeatTimeout = 2 * c.HeartbeatPeriod
	}
	if c.ReplicationFactor < 1 {
		c.ReplicationFactor = 1
	}
}

// ToRingConfig converts to dskit ring.Config
func (c *Config) ToRingConfig() ring.Config {
	return ring.Config{
		KVStore:           c.KVStore,
		HeartbeatTimeout:  c.HeartbeatTimeout,
		ReplicationFactor: c.ReplicationFactor,
	}
}

// LifecyclerConfig holds configuration for an individual instance's lifecycle
type LifecyclerConfig struct {
	// RingConfig is the shared ring configuration
	RingConfig Config `yaml:"ring"`

	// InstanceID is the unique identifier for this instance
	InstanceID string `yaml:"instance_id"`

	// InstanceAddr is the address other instances use to reach this one (for client requests)
	InstanceAddr string `yaml:"instance_addr"`

	// InstancePort is the port this instance listens on for client requests
	InstancePort int `yaml:"instance_port"`

	// NumTokens is the number of tokens this instance claims on the ring
	NumTokens int `yaml:"num_tokens"`

	// DiskPath is the base directory for persistent storage (e.g., the -disk flag value).
	// Used to derive TokensFilePath if not explicitly set.
	DiskPath string `yaml:"disk_path"`

	// TokensFilePath is the path to persist tokens for stable ownership across restarts.
	// If empty, defaults to <DiskPath>/coordinator/ring-tokens.
	// Token persistence is essential for stable ownership across restarts.
	TokensFilePath string `yaml:"tokens_file_path"`

	// ObservePeriod is the time to wait after joining before marking as ACTIVE.
	// Used to observe the ring state before fully joining.
	ObservePeriod time.Duration `yaml:"observe_period"`

	// MinReadyDuration is the minimum time this instance must be in ACTIVE state
	// before the /ready endpoint returns ready.
	MinReadyDuration time.Duration `yaml:"min_ready_duration"`

	// UnregisterOnShutdown controls whether this instance is removed from the ring on shutdown.
	// If true (default), the instance transitions to LEAVING then leaves the ring.
	// If false, the instance stays in the ring and will be detected as unhealthy via heartbeat timeout.
	UnregisterOnShutdown bool `yaml:"unregister_on_shutdown"`
}

// SetupLifecyclerConfig creates a LifecyclerConfig from coordinator parameters.
// This is the preferred way to create a LifecyclerConfig as it ensures all required fields are set.
// Parameters:
//   - nodeID: unique identifier for this instance
//   - listenAddr: the address this instance listens on for client requests (e.g., ":9001" or "0.0.0.0:9001")
//   - baseConfig: the base configuration to use
func SetupLifecyclerConfig(nodeID, listenAddr string, baseConfig *LifecyclerConfig) error {
	if baseConfig == nil {
		return fmt.Errorf("base config is required")
	}

	baseConfig.InstanceID = nodeID
	baseConfig.InstanceAddr = listenAddr
	baseConfig.UnregisterOnShutdown = true // Default to graceful departure
	baseConfig.ApplyDefaults()

	return nil
}

// RegisterFlags registers the lifecycler configuration flags
func (c *LifecyclerConfig) RegisterFlags(f *flag.FlagSet) {
	c.RingConfig.RegisterFlags(f)

	f.StringVar(&c.InstanceID, "ring.instance-id", "",
		"Unique identifier for this instance. Defaults to hostname if not set.")
	f.StringVar(&c.InstanceAddr, "ring.instance-addr", "",
		"Address advertised to other instances for client requests")
	f.IntVar(&c.InstancePort, "ring.instance-port", 0,
		"Port advertised to other instances for client requests")
	f.IntVar(&c.NumTokens, "ring.num-tokens", DefaultNumTokens,
		"Number of tokens this instance claims on the ring")
	f.StringVar(&c.DiskPath, "ring.disk-path", "",
		"Base directory for persistent storage (typically same as -disk flag)")
	f.StringVar(&c.TokensFilePath, "ring.tokens-file-path", "",
		"Path to persist tokens. Defaults to <disk-path>/coordinator/ring-tokens")
	f.DurationVar(&c.ObservePeriod, "ring.observe-period", 0,
		"Time to observe the ring before marking as ACTIVE")
	f.DurationVar(&c.MinReadyDuration, "ring.min-ready-duration", 15*time.Second,
		"Minimum time in ACTIVE state before /ready returns ready")
	f.BoolVar(&c.UnregisterOnShutdown, "ring.unregister-on-shutdown", true,
		"Whether to unregister from the ring on shutdown")
}

// ApplyDefaults applies default values for any unset or invalid configuration fields.
// This should be called before using the configuration.
// Note: This method mutates the config to apply defaults.
func (c *LifecyclerConfig) ApplyDefaults() {
	c.RingConfig.ApplyDefaults()
	if c.NumTokens <= 0 {
		c.NumTokens = DefaultNumTokens
	}
	// Derive TokensFilePath from DiskPath if not explicitly set
	if c.TokensFilePath == "" && c.DiskPath != "" {
		c.TokensFilePath = filepath.Join(c.DiskPath, "coordinator", "ring-tokens")
	}
}

// ToBasicLifecyclerConfig converts to dskit ring.BasicLifecyclerConfig
func (c *LifecyclerConfig) ToBasicLifecyclerConfig() ring.BasicLifecyclerConfig {
	addr := c.InstanceAddr
	if c.InstancePort > 0 {
		addr = fmt.Sprintf("%s:%d", c.InstanceAddr, c.InstancePort)
	}

	return ring.BasicLifecyclerConfig{
		ID:                              c.InstanceID,
		Addr:                            addr,
		HeartbeatPeriod:                 c.RingConfig.HeartbeatPeriod,
		HeartbeatTimeout:                c.RingConfig.HeartbeatTimeout,
		TokensObservePeriod:             c.ObservePeriod,
		NumTokens:                       c.NumTokens,
		KeepInstanceInTheRingOnShutdown: !c.UnregisterOnShutdown,
	}
}
