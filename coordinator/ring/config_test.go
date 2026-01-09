package ring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ApplyDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    Config
		expected Config
	}{
		{
			name:  "empty config gets all defaults",
			input: Config{},
			expected: Config{
				HeartbeatPeriod:   DefaultHeartbeatPeriod,
				HeartbeatTimeout:  2 * DefaultHeartbeatPeriod, // 2x heartbeat period
				ReplicationFactor: 1,
			},
		},
		{
			name: "custom heartbeat period updates timeout minimum",
			input: Config{
				HeartbeatPeriod:  10 * time.Second,
				HeartbeatTimeout: 5 * time.Second, // less than 2x period
			},
			expected: Config{
				HeartbeatPeriod:   10 * time.Second,
				HeartbeatTimeout:  20 * time.Second, // adjusted to 2x period
				ReplicationFactor: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.input
			cfg.ApplyDefaults()

			assert.Equal(t, tt.expected.HeartbeatPeriod, cfg.HeartbeatPeriod)
			assert.Equal(t, tt.expected.HeartbeatTimeout, cfg.HeartbeatTimeout)
			assert.Equal(t, tt.expected.ReplicationFactor, cfg.ReplicationFactor)
		})
	}
}

func TestLifecyclerConfig_ApplyDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    LifecyclerConfig
		expected LifecyclerConfig
	}{
		{
			name:  "empty config gets defaults",
			input: LifecyclerConfig{},
			expected: LifecyclerConfig{
				RingConfig: Config{
					HeartbeatPeriod:   DefaultHeartbeatPeriod,
					HeartbeatTimeout:  2 * DefaultHeartbeatPeriod,
					ReplicationFactor: 1,
				},
				NumTokens: DefaultNumTokens,
			},
		},
		{
			name: "disk path derives tokens file path",
			input: LifecyclerConfig{
				DiskPath: "/data/ocache",
			},
			expected: LifecyclerConfig{
				RingConfig: Config{
					HeartbeatPeriod:   DefaultHeartbeatPeriod,
					HeartbeatTimeout:  2 * DefaultHeartbeatPeriod,
					ReplicationFactor: 1,
				},
				DiskPath:       "/data/ocache",
				TokensFilePath: "/data/ocache/coordinator/ring-tokens",
				NumTokens:      DefaultNumTokens,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.input
			cfg.ApplyDefaults()

			assert.Equal(t, tt.expected.RingConfig.HeartbeatPeriod, cfg.RingConfig.HeartbeatPeriod)
			assert.Equal(t, tt.expected.RingConfig.HeartbeatTimeout, cfg.RingConfig.HeartbeatTimeout)
			assert.Equal(t, tt.expected.RingConfig.ReplicationFactor, cfg.RingConfig.ReplicationFactor)
			assert.Equal(t, tt.expected.NumTokens, cfg.NumTokens)
			assert.Equal(t, tt.expected.TokensFilePath, cfg.TokensFilePath)
		})
	}
}

func TestSetupLifecyclerConfig(t *testing.T) {
	tests := []struct {
		name       string
		nodeID     string
		listenAddr string
		diskPath   string
	}{
		{
			name:       "basic config",
			nodeID:     "node-1",
			listenAddr: "localhost:9001",
			diskPath:   "/data/ocache1",
		},
		{
			name:       "with IP address",
			nodeID:     "node-2",
			listenAddr: "192.168.1.1:8080",
			diskPath:   "/data/ocache2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := LifecyclerConfig{}
			err := SetupLifecyclerConfig(tt.nodeID, tt.listenAddr, tt.diskPath, &cfg)
			assert.NoError(t, err)

			assert.Equal(t, tt.nodeID, cfg.InstanceID)
			assert.Equal(t, tt.listenAddr, cfg.InstanceAddr)
			assert.True(t, cfg.UnregisterOnShutdown, "should default to graceful departure")
			assert.Equal(t, DefaultNumTokens, cfg.NumTokens)
			assert.Equal(t, DefaultHeartbeatPeriod, cfg.RingConfig.HeartbeatPeriod)
			assert.Contains(t, cfg.TokensFilePath, tt.diskPath)
		})
	}
}

func TestLifecyclerConfig_ToBasicLifecyclerConfig(t *testing.T) {
	tests := []struct {
		name         string
		cfg          LifecyclerConfig
		expectedAddr string
	}{
		{
			name: "address without port",
			cfg: LifecyclerConfig{
				InstanceID:   "node1",
				InstanceAddr: "192.168.1.1",
				InstancePort: 0,
				NumTokens:    512,
				RingConfig: Config{
					HeartbeatPeriod:  5 * time.Second,
					HeartbeatTimeout: 30 * time.Second,
				},
			},
			expectedAddr: "192.168.1.1",
		},
		{
			name: "address with port",
			cfg: LifecyclerConfig{
				InstanceID:   "node1",
				InstanceAddr: "192.168.1.1",
				InstancePort: 9000,
				NumTokens:    512,
				RingConfig: Config{
					HeartbeatPeriod:  5 * time.Second,
					HeartbeatTimeout: 30 * time.Second,
				},
			},
			expectedAddr: "192.168.1.1:9000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			basicCfg := tt.cfg.ToBasicLifecyclerConfig()

			assert.Equal(t, tt.cfg.InstanceID, basicCfg.ID)
			assert.Equal(t, tt.expectedAddr, basicCfg.Addr)
			assert.Equal(t, tt.cfg.NumTokens, basicCfg.NumTokens)
			assert.Equal(t, tt.cfg.RingConfig.HeartbeatPeriod, basicCfg.HeartbeatPeriod)
			assert.Equal(t, tt.cfg.RingConfig.HeartbeatTimeout, basicCfg.HeartbeatTimeout)
		})
	}
}
