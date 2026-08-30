// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/memberlist"
	dskitring "github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
)

type ringManagerBenchmarkUpdate struct {
	memberID  string
	timestamp int64
}

type ringManagerBenchmarkHeartbeat struct {
	proceed chan struct{}
	done    chan struct{}
}

type ringManagerBenchmarkKV struct {
	mu      sync.Mutex
	current *dskitring.Desc
	version uint64
	ready   chan struct{}
	full    []func(interface{}) bool
	delta   []func(memberlist.WatchKeyChange) bool

	measuring        bool
	heartbeatStarted chan *ringManagerBenchmarkHeartbeat
}

func newRingManagerBenchmarkKV(members, tokensPerMember int) *ringManagerBenchmarkKV {
	desc := dskitring.NewDesc()
	now := time.Now().Unix()
	for member := 0; member < members; member++ {
		tokens := make([]uint32, tokensPerMember)
		for token := range tokens {
			tokens[token] = uint32(member*tokensPerMember + token + 1)
		}
		id := fmt.Sprintf("member-%d", member)
		desc.AddIngester(id, id, "", tokens, dskitring.ACTIVE, time.Unix(now, 0))
	}
	return &ringManagerBenchmarkKV{
		current:          desc,
		version:          1,
		ready:            make(chan struct{}, 2),
		heartbeatStarted: make(chan *ringManagerBenchmarkHeartbeat),
	}
}

func (c *ringManagerBenchmarkKV) List(context.Context, string) ([]string, error) { return nil, nil }

func (c *ringManagerBenchmarkKV) Get(context.Context, string) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return proto.Clone(c.current), nil
}

func (c *ringManagerBenchmarkKV) GetWithVersion(context.Context, string) (interface{}, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return proto.Clone(c.current), c.version, nil
}

func (c *ringManagerBenchmarkKV) Delete(context.Context, string) error {
	return errors.New("benchmark KV does not support Delete")
}

func (c *ringManagerBenchmarkKV) CAS(_ context.Context, _ string, f func(interface{}) (interface{}, bool, error)) error {
	c.mu.Lock()
	var heartbeat *ringManagerBenchmarkHeartbeat
	if c.measuring {
		heartbeat = &ringManagerBenchmarkHeartbeat{
			proceed: make(chan struct{}),
			done:    make(chan struct{}),
		}
	}
	c.mu.Unlock()
	if heartbeat != nil {
		// Keep the idle interval between ticker events outside the timed operation,
		// while the handshake starts timing before the CAS clones the descriptor.
		// The per-CAS token prevents a completion from a CAS that began before
		// measurement from satisfying the current iteration.
		c.heartbeatStarted <- heartbeat
		<-heartbeat.proceed
		defer c.recordHeartbeat(heartbeat)
	}

	c.mu.Lock()
	in := proto.Clone(c.current)
	c.mu.Unlock()

	out, retry, err := f(in)
	if err != nil || !retry || out == nil {
		return err
	}
	desc, ok := out.(*dskitring.Desc)
	if !ok {
		return fmt.Errorf("benchmark CAS returned %T, want *ring.Desc", out)
	}

	c.mu.Lock()
	c.current = desc
	c.version++
	version := c.version
	fullWatchers := append([]func(interface{}) bool(nil), c.full...)
	deltaWatchers := append([]func(memberlist.WatchKeyChange) bool(nil), c.delta...)
	instance := desc.Ingesters["member-0"]
	fullValue := proto.Clone(desc)
	delta := dskitring.NewDesc()
	delta.Ingesters["member-0"] = instance
	c.mu.Unlock()

	for _, watcher := range fullWatchers {
		if !watcher(proto.Clone(fullValue)) {
			continue
		}
	}
	change := memberlist.WatchKeyChange{
		Value:       delta,
		ChangedKeys: []string{"member-0"},
		Sequence:    version,
	}
	for _, watcher := range deltaWatchers {
		if !watcher(change) {
			continue
		}
	}
	return nil
}

func (c *ringManagerBenchmarkKV) waitForHeartbeatStart() *ringManagerBenchmarkHeartbeat {
	return <-c.heartbeatStarted
}

func (c *ringManagerBenchmarkKV) proceedHeartbeat(heartbeat *ringManagerBenchmarkHeartbeat) {
	heartbeat.proceed <- struct{}{}
}

func (c *ringManagerBenchmarkKV) recordHeartbeat(heartbeat *ringManagerBenchmarkHeartbeat) {
	close(heartbeat.done)
}

func (c *ringManagerBenchmarkKV) beginMeasurement() {
	c.mu.Lock()
	c.measuring = true
	c.mu.Unlock()
}

func (c *ringManagerBenchmarkKV) endMeasurement() {
	c.mu.Lock()
	c.measuring = false
	c.mu.Unlock()
}

func (c *ringManagerBenchmarkKV) waitForHeartbeat(heartbeat *ringManagerBenchmarkHeartbeat) {
	<-heartbeat.done
}

func (c *ringManagerBenchmarkKV) WatchKey(ctx context.Context, _ string, f func(interface{}) bool) {
	c.mu.Lock()
	c.full = append(c.full, f)
	c.mu.Unlock()
	c.ready <- struct{}{}
	<-ctx.Done()
}

func (c *ringManagerBenchmarkKV) WatchKeyWithChanges(ctx context.Context, _ string, f func(memberlist.WatchKeyChange) bool) {
	c.mu.Lock()
	c.delta = append(c.delta, f)
	initial := proto.Clone(c.current).(memberlist.Mergeable)
	version := c.version
	c.mu.Unlock()
	if !f(memberlist.WatchKeyChange{Value: initial, Sequence: version, FullSnapshot: true}) {
		return
	}
	c.ready <- struct{}{}
	<-ctx.Done()
}

func (c *ringManagerBenchmarkKV) WatchPrefix(context.Context, string, func(string, interface{}) bool) {
}

func (c *ringManagerBenchmarkKV) publish(update ringManagerBenchmarkUpdate) {
	c.mu.Lock()
	c.version++
	version := c.version
	instance := c.current.Ingesters[update.memberID]
	instance.Timestamp = update.timestamp
	c.current.Ingesters[update.memberID] = instance
	fullWatchers := append([]func(interface{}) bool(nil), c.full...)
	deltaWatchers := append([]func(memberlist.WatchKeyChange) bool(nil), c.delta...)
	delta := dskitring.NewDesc()
	delta.Ingesters[update.memberID] = instance
	c.mu.Unlock()

	for _, watcher := range fullWatchers {
		if !watcher(proto.Clone(c.current)) {
			continue
		}
	}
	change := memberlist.WatchKeyChange{
		Value:       delta,
		ChangedKeys: []string{update.memberID},
		Sequence:    version,
	}
	for _, watcher := range deltaWatchers {
		if !watcher(change) {
			continue
		}
	}
}

func BenchmarkRingManagerHeartbeatUpdate(b *testing.B) {
	for _, members := range []int{16, 64} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			client := newRingManagerBenchmarkKV(members, 512)
			manager, err := NewRingManager(LifecyclerConfig{
				RingConfig: Config{
					HeartbeatPeriod:   DefaultHeartbeatPeriod,
					HeartbeatTimeout:  time.Hour,
					ReplicationFactor: 1,
				},
				InstanceID:   "member-0",
				InstanceAddr: "member-0",
				NumTokens:    512,
			}, client, log.NewNopLogger(), prometheus.NewRegistry())
			if err != nil {
				b.Fatal(err)
			}

			ctx := context.Background()
			if err := manager.Start(ctx); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := manager.Stop(context.Background()); err != nil {
					b.Error(err)
				}
			})
			for watcher := 0; watcher < 2; watcher++ {
				select {
				case <-client.ready:
				case <-time.After(10 * time.Second):
					b.Fatal("ring watchers did not register")
				}
			}

			client.beginMeasurement()
			b.ReportAllocs()
			b.ResetTimer()
			for heartbeat := 0; heartbeat < b.N; heartbeat++ {
				b.StopTimer()
				run := client.waitForHeartbeatStart()
				b.StartTimer()
				client.proceedHeartbeat(run)
				client.waitForHeartbeat(run)
			}
			b.StopTimer()
			client.endMeasurement()
		})
	}
}

var _ kv.Client = (*ringManagerBenchmarkKV)(nil)
