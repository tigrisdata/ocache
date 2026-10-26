// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-kit/log"
	dskitmemberlist "github.com/grafana/dskit/kv/memberlist"
	hashicorpmemberlist "github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// freePort reserves and releases a loopback port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// membersCount reads memberlist_client_cluster_members_count from a node's registry.
func membersCount(t *testing.T, reg *prometheus.Registry) int {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "memberlist_client_cluster_members_count" {
			return int(f.GetMetric()[0].GetGauge().GetValue())
		}
	}
	return -1
}

func waitMembers(t *testing.T, want int, timeout time.Duration, nodes map[string]*prometheus.Registry) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		all := true
		state := ""
		for name, reg := range nodes {
			n := membersCount(t, reg)
			state += fmt.Sprintf(" %s=%d", name, n)
			if n != want {
				all = false
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("members did not reach %d within %s:%s", want, timeout, state)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func startNode(t *testing.T, ctx context.Context, name string, port int, seeds []string) (*Memberlist, *prometheus.Registry) {
	t.Helper()
	cfg := memberlistConfig{
		JoinMembers:      seeds,
		BindAddr:         "127.0.0.1",
		BindPort:         port,
		NodeName:         name,
		PushPullInterval: time.Second,
		LeaveTimeout:     time.Second,
		// Reclaim quickly but keep the dead entry around for the full
		// gossip-to-dead window, so acceptance must come from reclaim,
		// not from the entry having been reaped.
		DeadNodeReclaimTime: time.Second,
		GossipToTheDeadTime: 30 * time.Second,
		RejoinInterval:      2 * time.Second,
	}
	reg := prometheus.NewRegistry()
	ml, err := newMemberlistFromConfig(cfg, log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, ml.Start(ctx))
	t.Cleanup(func() { _ = ml.Stop(context.Background()) })
	return ml, reg
}

// A node that dies without leaving and comes back under the same name on a
// new address (a restarted pod) must be accepted by its peers well inside the
// gossip-to-dead window. Without dead-node reclaim the peers refuse the new
// address as a conflict until the stale entry is reaped.
func TestMemberlist_RestartedNodeOnNewAddressRejoins(t *testing.T) {
	ctx := context.Background()
	portA, portB := freePort(t), freePort(t)
	seedA := []string{fmt.Sprintf("127.0.0.1:%d", portA)}

	_, regA := startNode(t, ctx, "node-a", portA, seedA)
	_, regB := startNode(t, ctx, "node-b", portB, seedA)
	peers := map[string]*prometheus.Registry{"a": regA, "b": regB}
	waitMembers(t, 2, 30*time.Second, peers)

	// node-c joins through dskit's TCP transport, then dies without leaving.
	portC1 := freePort(t)
	transport, err := dskitmemberlist.NewTCPTransport(dskitmemberlist.TCPTransportConfig{
		BindAddrs:          []string{"127.0.0.1"},
		BindPort:           portC1,
		PacketDialTimeout:  DefaultPacketDialTimeout,
		PacketWriteTimeout: DefaultPacketWriteTimeout,
	}, log.NewNopLogger(), nil)
	require.NoError(t, err)
	zombieCfg := hashicorpmemberlist.DefaultLANConfig()
	zombieCfg.Name = "node-c"
	zombieCfg.Transport = transport
	zombieCfg.DisableTcpPings = true
	zombieCfg.LogOutput = io.Discard
	zombie, err := hashicorpmemberlist.Create(zombieCfg)
	require.NoError(t, err)
	_, err = zombie.Join(seedA)
	require.NoError(t, err)
	waitMembers(t, 3, 30*time.Second, peers)

	require.NoError(t, zombie.Shutdown()) // no Leave: peers must detect the death
	waitMembers(t, 2, 90*time.Second, peers)

	// Same name, new address, inside the gossip-to-dead window.
	portC2 := freePort(t)
	require.NotEqual(t, portC1, portC2)
	_, regC := startNode(t, ctx, "node-c", portC2, seedA)
	peers["c"] = regC
	waitMembers(t, 3, 20*time.Second, peers)
}
