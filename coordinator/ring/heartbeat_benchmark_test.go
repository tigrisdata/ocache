// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	dskitring "github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tigrisdata/ocache/coordinator/gossip"
)

const (
	heartbeatBenchmarkMembers       = 8
	heartbeatBenchmarkTokensPerNode = 1
	heartbeatBenchmarkStep          = 100 * time.Millisecond
)

type heartbeatBenchmarkDelegate struct {
	base       dskitring.InstanceRegisterDelegate
	callbacks  atomic.Uint64
	production ringDelegate
}

func (d *heartbeatBenchmarkDelegate) OnRingInstanceRegister(l *dskitring.BasicLifecycler, desc dskitring.Desc, exists bool, id string, instance dskitring.InstanceDesc) (dskitring.InstanceState, dskitring.Tokens) {
	return d.base.OnRingInstanceRegister(l, desc, exists, id, instance)
}

func (d *heartbeatBenchmarkDelegate) OnRingInstanceTokens(*dskitring.BasicLifecycler, dskitring.Tokens) {
}

func (d *heartbeatBenchmarkDelegate) OnRingInstanceStopping(*dskitring.BasicLifecycler) {}

func (d *heartbeatBenchmarkDelegate) OnRingInstanceHeartbeat(l *dskitring.BasicLifecycler, desc *dskitring.Desc, instance *dskitring.InstanceDesc) {
	d.callbacks.Add(1)
	d.production.OnRingInstanceHeartbeat(l, desc, instance)
}

type heartbeatBenchmarkFixture struct {
	delegate     *heartbeatBenchmarkDelegate
	reg          *prometheus.Registry
	lifecycler   *dskitring.BasicLifecycler
	memberlistKV *gossip.Memberlist
	cancel       context.CancelFunc
	stopOnce     sync.Once
}

func newHeartbeatBenchmarkFixture(b *testing.B) *heartbeatBenchmarkFixture {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	nodeID := "heartbeat-benchmark-member-0"
	memberlistKV, err := gossip.NewMemberlist(nodeID, "127.0.0.1:0", nil, logger, reg)
	if err != nil {
		cancel()
		b.Fatalf("create memberlist: %v", err)
	}
	if err := memberlistKV.Start(ctx); err != nil {
		cancel()
		b.Fatalf("start memberlist: %v", err)
	}

	desc := dskitring.NewDesc()
	registeredAt := time.Now()
	for member := 0; member < heartbeatBenchmarkMembers; member++ {
		desc.AddIngester(
			fmt.Sprintf("heartbeat-benchmark-member-%d", member),
			fmt.Sprintf("127.0.0.1:%d", 10000+member),
			"",
			[]uint32{uint32(member + 1)},
			dskitring.ACTIVE,
			registeredAt,
		)
	}
	if err := memberlistKV.Client().CAS(ctx, RingKey, func(interface{}) (interface{}, bool, error) {
		return desc, true, nil
	}); err != nil {
		_ = memberlistKV.Stop(context.Background())
		cancel()
		b.Fatalf("seed ring: %v", err)
	}

	cfg := LifecyclerConfig{
		InstanceID:   nodeID,
		InstanceAddr: "127.0.0.1:10000",
		NumTokens:    heartbeatBenchmarkTokensPerNode,
	}
	cfg.ApplyDefaults()

	delegate := &heartbeatBenchmarkDelegate{
		base:       dskitring.NewInstanceRegisterDelegate(dskitring.ACTIVE, heartbeatBenchmarkTokensPerNode),
		production: ringDelegate{rm: &RingManager{logger: logger}},
	}
	basicCfg := cfg.ToBasicLifecyclerConfig()
	// Keep teardown from unregistering the seeded instance with a non-heartbeat CAS.
	basicCfg.KeepInstanceInTheRingOnShutdown = true
	lifecycler, err := dskitring.NewBasicLifecycler(
		basicCfg,
		RingName,
		RingKey,
		memberlistKV.Client(),
		delegate,
		logger,
		reg,
	)
	if err != nil {
		_ = memberlistKV.Stop(context.Background())
		cancel()
		b.Fatalf("create lifecycler: %v", err)
	}
	if err := services.StartAndAwaitRunning(ctx, lifecycler); err != nil {
		_ = memberlistKV.Stop(context.Background())
		cancel()
		b.Fatalf("start lifecycler: %v", err)
	}

	fixture := &heartbeatBenchmarkFixture{
		delegate:     delegate,
		reg:          reg,
		lifecycler:   lifecycler,
		memberlistKV: memberlistKV,
		cancel:       cancel,
	}
	b.Cleanup(fixture.stop)
	return fixture
}

func (f *heartbeatBenchmarkFixture) stop() {
	f.stopOnce.Do(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), f.lifecycler)
		_ = f.memberlistKV.Stop(context.Background())
		f.cancel()
	})
}

type heartbeatBenchmarkCounts struct {
	casAttempts  float64
	casSuccesses float64
	heartbeats   float64
	callbacks    uint64
}

func heartbeatBenchmarkSnapshot(b *testing.B, fixture *heartbeatBenchmarkFixture) heartbeatBenchmarkCounts {
	b.Helper()

	families, err := fixture.reg.Gather()
	if err != nil {
		b.Fatalf("gather heartbeat metrics: %v", err)
	}

	var counts heartbeatBenchmarkCounts
	found := make(map[string]bool, 3)
	for _, family := range families {
		var target *float64
		switch family.GetName() {
		case "memberlist_client_cas_attempt_total":
			target = &counts.casAttempts
		case "memberlist_client_cas_success_total":
			target = &counts.casSuccesses
		case "ring_member_heartbeats_total":
			target = &counts.heartbeats
		default:
			continue
		}
		found[family.GetName()] = true
		for _, metric := range family.GetMetric() {
			*target += metric.GetCounter().GetValue()
		}
	}

	for _, name := range []string{
		"memberlist_client_cas_attempt_total",
		"memberlist_client_cas_success_total",
		"ring_member_heartbeats_total",
	} {
		if !found[name] {
			b.Fatalf("metric %s not found", name)
		}
	}
	counts.callbacks = fixture.delegate.callbacks.Load()
	return counts
}

func BenchmarkRingHeartbeat(b *testing.B) {
	fixture := newHeartbeatBenchmarkFixture(b)
	before := heartbeatBenchmarkSnapshot(b, fixture)

	b.ResetTimer()
	for b.Loop() {
		time.Sleep(heartbeatBenchmarkStep)
	}
	b.StopTimer()

	// Stop-time lifecycle work is outside the heartbeat workload. Capture the
	// endpoint before stopping so failed teardown CAS attempts cannot be folded
	// into the heartbeat-attempt calculation.
	after := heartbeatBenchmarkSnapshot(b, fixture)
	fixture.stop()

	heartbeats := after.heartbeats - before.heartbeats
	callbacks := float64(after.callbacks - before.callbacks)
	casAttempts := after.casAttempts - before.casAttempts
	casSuccesses := after.casSuccesses - before.casSuccesses
	nonHeartbeatCAS := casSuccesses - heartbeats
	heartbeatAttempts := casAttempts - nonHeartbeatCAS
	if heartbeats <= 0 {
		b.Fatalf("heartbeat benchmark produced no successful heartbeats (attempts=%.0f callbacks=%.0f)", casAttempts, callbacks)
	}
	if heartbeatAttempts < heartbeats || callbacks < heartbeats {
		b.Fatalf("heartbeat counters are inconsistent (attempts=%.0f successes=%.0f callbacks=%.0f heartbeats=%.0f)", heartbeatAttempts, casSuccesses, callbacks, heartbeats)
	}

	b.ReportMetric(0, "ns/op")
	b.ReportMetric(heartbeatAttempts/heartbeats, "cas-attempts/heartbeat")
	b.ReportMetric(callbacks/heartbeats, "heartbeat-callbacks/heartbeat")
}
