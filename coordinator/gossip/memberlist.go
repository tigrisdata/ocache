package gossip

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	zlog "github.com/rs/zerolog/log"
)

// Memberlist wraps dskit's memberlist gossip service with lifecycle management
type Memberlist struct {
	cfg         memberlistConfig
	kv          *memberlist.KV
	client      *memberlist.Client
	dnsProvider *simpleDNSProvider
	logger      log.Logger
	reg         prometheus.Registerer
}

// NewMemberlist creates a new memberlist gossip service.
func NewMemberlist(nodeID, clusterAddr string, seeds []string, logger log.Logger, reg prometheus.Registerer) (*Memberlist, error) {
	cfg := newMemberlistConfig(nodeID, clusterAddr, seeds)

	mlCfg := cfg.ToKVConfig()

	// Create DNS provider for resolving seed addresses
	// This supports both static IPs and DNS names (e.g., Kubernetes headless services)
	dnsProvider := newSimpleDNSProvider(cfg.JoinMembers, logger)

	// Create the memberlist KV
	mlKV := memberlist.NewKV(mlCfg, logger, dnsProvider, reg)

	// Create a client wrapper with the ring codec
	client, err := memberlist.NewClient(mlKV, ring.GetCodec())
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist client: %w", err)
	}

	zlog.Info().
		Str("node_name", cfg.NodeName).
		Any("config", mlCfg).
		Msg("Memberlist gossip service initialized")

	return &Memberlist{
		cfg:         cfg,
		kv:          mlKV,
		client:      client,
		dnsProvider: dnsProvider,
		logger:      logger,
		reg:         reg,
	}, nil
}

// Start starts the memberlist KV service
func (m *Memberlist) Start(ctx context.Context) error {
	if err := services.StartAndAwaitRunning(ctx, m.kv); err != nil {
		return fmt.Errorf("failed to start memberlist: %w", err)
	}

	zlog.Info().
		Strs("join_members", m.cfg.JoinMembers).
		Str("bind_addr", m.cfg.BindAddr).
		Int("bind_port", m.cfg.BindPort).
		Msg("Memberlist gossip service started")

	return nil
}

// Stop stops the memberlist KV service.
// Uses dskit's built-in service state to handle cleanup correctly:
// - New state: call Stop() - dskit handles never-started services correctly
// - Failed state: skip Stop() - dskit would try to create new transport for leave messages
// - Running state: call Stop() normally for graceful shutdown
func (m *Memberlist) Stop(ctx context.Context) error {
	// If the service failed to start, don't call dskit Stop because it will
	// try to create a new transport for leave messages, which fails since
	// the port is already bound from the failed start attempt.
	if m.kv.State() == services.Failed {
		zlog.Debug().Msg("Memberlist in Failed state, skipping Stop to avoid transport recreation")
		return nil
	}

	// For New, Running, and other states, call dskit's Stop - it handles them correctly
	if err := services.StopAndAwaitTerminated(ctx, m.kv); err != nil {
		return fmt.Errorf("failed to stop memberlist: %w", err)
	}

	zlog.Info().Msg("Memberlist KV stopped")
	return nil
}

// Client returns the KV client interface for use with dskit ring
func (m *Memberlist) Client() kv.Client {
	return m.client
}

// GetKV returns the underlying memberlist KV for direct access
func (m *Memberlist) GetKV() *memberlist.KV {
	return m.kv
}
