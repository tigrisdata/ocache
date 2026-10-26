// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"fmt"
	"net"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
)

const (
	// DefaultGossipInterval is the default interval for gossip messages
	DefaultGossipInterval = 200 * time.Millisecond

	// DefaultGossipNodes is the default number of nodes to gossip to
	DefaultGossipNodes = 3

	// DefaultPushPullInterval is the default interval for push/pull sync
	DefaultPushPullInterval = 30 * time.Second

	// DefaultLeaveTimeout is the default timeout for leave announcements
	DefaultLeaveTimeout = 5 * time.Second

	// DefaultPacketDialTimeout bounds the TCP connect for each gossip packet.
	// Zero (dskit's zero value) falls through to the kernel SYN timeout, which
	// is minutes against an address that black-holes (a pod IP that no longer
	// exists), and the probe loop is serialized behind it.
	DefaultPacketDialTimeout = 2 * time.Second

	// DefaultPacketWriteTimeout bounds the write of a gossip packet (dskit's
	// flag default); zero means no deadline.
	DefaultPacketWriteTimeout = 5 * time.Second

	// DefaultGossipToTheDeadTime is how long a dead node is still gossiped to so
	// it can refute its death (memberlist's LAN default).
	DefaultGossipToTheDeadTime = 30 * time.Second

	// DefaultDeadNodeReclaimTime is how soon a dead node's name may be reused by
	// an alive announcement from a different address. Node names are stable and
	// addresses change on restart, so a restarted node must not be refused with
	// "Conflicting address" until its old entry is reaped.
	DefaultDeadNodeReclaimTime = 5 * time.Second

	// DefaultRejoinInterval re-resolves the seeds and rejoins periodically so a
	// node that has lost every peer finds the cluster again.
	DefaultRejoinInterval = time.Minute
)

// memberlistConfig holds configuration for the memberlist KV store
type memberlistConfig struct {
	// JoinMembers is a list of seed addresses to join
	// Can be static IPs or DNS names (for Kubernetes headless services)
	JoinMembers flagext.StringSlice `yaml:"join_members"`

	// AbortIfJoinFails controls whether to abort if cluster join fails
	// Set to false to allow bootstrap of first node
	AbortIfJoinFails bool `yaml:"abort_if_cluster_join_fails"`

	// BindAddr is the address to bind the gossip listener
	BindAddr string `yaml:"bind_addr"`

	// BindPort is the port to bind the gossip listener
	BindPort int `yaml:"bind_port"`

	// GossipInterval is the interval between gossip messages
	GossipInterval time.Duration `yaml:"gossip_interval"`

	// GossipNodes is the number of nodes to gossip to per interval
	GossipNodes int `yaml:"gossip_nodes"`

	// PushPullInterval is the interval for full state sync
	PushPullInterval time.Duration `yaml:"push_pull_interval"`

	// LeaveTimeout is the timeout for leave announcements
	LeaveTimeout time.Duration `yaml:"leave_timeout"`

	// PacketDialTimeout bounds the TCP connect for each gossip packet
	PacketDialTimeout time.Duration `yaml:"packet_dial_timeout"`

	// PacketWriteTimeout bounds the write of each gossip packet
	PacketWriteTimeout time.Duration `yaml:"packet_write_timeout"`

	// GossipToTheDeadTime is how long dead nodes are still gossiped to
	GossipToTheDeadTime time.Duration `yaml:"gossip_to_dead_nodes_time"`

	// DeadNodeReclaimTime is how soon a dead node's name can be reused by a new address
	DeadNodeReclaimTime time.Duration `yaml:"dead_node_reclaim_time"`

	// RejoinInterval is how often to re-resolve the seeds and rejoin the cluster
	RejoinInterval time.Duration `yaml:"rejoin_interval"`

	// NodeName is the unique name for this node (defaults to hostname)
	NodeName string `yaml:"node_name"`

	// RandomizeNodeName appends a random suffix to node name
	RandomizeNodeName bool `yaml:"randomize_node_name"`
}

// newMemberlistConfig creates a MemberlistConfig.
// Parameters:
//   - nodeID: unique identifier for this node (used as memberlist node name)
//   - clusterAddr: address for memberlist gossip in "host:port" format (e.g., "0.0.0.0:7946")
//   - seeds: list of seed addresses to join (can be DNS names for Kubernetes headless services)
func newMemberlistConfig(nodeID, clusterAddr string, seeds []string) memberlistConfig {
	cfg := memberlistConfig{
		JoinMembers: seeds,
		// Use the coordinator's node ID as memberlist node name to ensure uniqueness.
		// Without this, all nodes on the same host would use the hostname and conflict.
		NodeName: nodeID,
	}

	// Parse bind address and port from clusterAddr (format: "host:port")
	if host, portStr, err := net.SplitHostPort(clusterAddr); err == nil {
		if host != "" {
			cfg.BindAddr = host
		} else {
			cfg.BindAddr = "0.0.0.0"
		}
		if port, err := parsePort(portStr); err == nil {
			cfg.BindPort = port
		}
	}

	cfg.ApplyDefaults()

	return cfg
}

// ApplyDefaults applies default values for any unset or invalid configuration fields.
// This should be called before using the configuration.
// Note: BindAddr and BindPort must be set by the caller (e.g., via NewMemberlistConfigFromCoordinator).
func (c *memberlistConfig) ApplyDefaults() {
	if c.GossipInterval <= 0 {
		c.GossipInterval = DefaultGossipInterval
	}
	if c.GossipNodes <= 0 {
		c.GossipNodes = DefaultGossipNodes
	}
	if c.PushPullInterval <= 0 {
		c.PushPullInterval = DefaultPushPullInterval
	}
	if c.LeaveTimeout <= 0 {
		c.LeaveTimeout = DefaultLeaveTimeout
	}
	if c.PacketDialTimeout <= 0 {
		c.PacketDialTimeout = DefaultPacketDialTimeout
	}
	if c.PacketWriteTimeout <= 0 {
		c.PacketWriteTimeout = DefaultPacketWriteTimeout
	}
	if c.GossipToTheDeadTime <= 0 {
		c.GossipToTheDeadTime = DefaultGossipToTheDeadTime
	}
	if c.DeadNodeReclaimTime <= 0 {
		c.DeadNodeReclaimTime = DefaultDeadNodeReclaimTime
	}
	if c.RejoinInterval <= 0 {
		c.RejoinInterval = DefaultRejoinInterval
	}
}

// ToKVConfig converts to dskit memberlist.KVConfig
func (c *memberlistConfig) ToKVConfig() memberlist.KVConfig {
	cfg := memberlist.KVConfig{
		JoinMembers:       c.JoinMembers,
		AbortIfJoinFails:  c.AbortIfJoinFails,
		LeaveTimeout:      c.LeaveTimeout,
		GossipInterval:    c.GossipInterval,
		GossipNodes:       c.GossipNodes,
		PushPullInterval:  c.PushPullInterval,
		NodeName:          c.NodeName,
		RandomizeNodeName: c.RandomizeNodeName,
		// Failure detection and re-convergence after restarts; see the
		// Default* constants for why each is set.
		GossipToTheDeadTime: c.GossipToTheDeadTime,
		DeadNodeReclaimTime: c.DeadNodeReclaimTime,
		RejoinInterval:      c.RejoinInterval,
		// RetransmitMult is the multiplier for number of retransmissions.
		// Default hashicorp memberlist uses 4, dskit uses 2. We use 4 for faster convergence.
		RetransmitMult: 4,
		// StreamTimeout is the timeout for establishing connections and read/write operations
		StreamTimeout: 10 * time.Second,
		// Register ring codec before joining other members
		Codecs: []codec.Codec{ring.GetCodec()},
	}

	// Configure TCP transport
	cfg.TCPTransport = memberlist.TCPTransportConfig{
		BindAddrs:          flagext.StringSlice{c.BindAddr},
		BindPort:           c.BindPort,
		PacketDialTimeout:  c.PacketDialTimeout,
		PacketWriteTimeout: c.PacketWriteTimeout,
	}

	return cfg
}

// parsePort parses a port string to an integer
func parsePort(portStr string) (int, error) {
	var port int
	_, err := fmt.Sscanf(portStr, "%d", &port)
	return port, err
}
