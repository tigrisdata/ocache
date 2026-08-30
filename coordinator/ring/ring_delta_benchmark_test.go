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
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
)

type ringManagerBenchmarkUpdate struct {
	memberID  string
	timestamp int64
}

type ringManagerBenchmarkKV struct {
	mu      sync.Mutex
	current *dskitring.Desc
	version uint64
	ready   chan struct{}
	full    []func(interface{}) bool
	delta   []func(memberlist.WatchKeyChange) bool
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
		current: desc,
		version: 1,
		ready:   make(chan struct{}, 2),
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

func (c *ringManagerBenchmarkKV) CAS(context.Context, string, func(interface{}) (interface{}, bool, error)) error {
	return errors.New("benchmark KV does not support CAS")
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
					HeartbeatTimeout:  time.Hour,
					ReplicationFactor: 1,
				},
				InstanceID:   "benchmark-node",
				InstanceAddr: "127.0.0.1",
			}, client, log.NewNopLogger(), prometheus.NewRegistry())
			if err != nil {
				b.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			if err := services.StartAndAwaitRunning(ctx, manager.ring); err != nil {
				cancel()
				b.Fatal(err)
			}
			manager.lastKnownNodes = make(map[string]dskitring.InstanceState, members)
			manager.startRingWatcher(ctx)
			for range 2 {
				select {
				case <-client.ready:
				case <-time.After(10 * time.Second):
					cancel()
					b.Fatal("ring watchers did not register")
				}
			}
			b.Cleanup(func() {
				cancel()
				if err := services.StopAndAwaitTerminated(context.Background(), manager.ring); err != nil {
					b.Error(err)
				}
			})

			updates := make([]ringManagerBenchmarkUpdate, 0, members)
			for member := 0; member < members; member++ {
				updates = append(updates, ringManagerBenchmarkUpdate{
					memberID:  fmt.Sprintf("member-%d", member),
					timestamp: time.Now().Unix() + int64(member) + 1,
				})
			}
			delegate := &ringDelegate{rm: manager}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				update := updates[i%len(updates)]
				update.timestamp += int64(i / len(updates))
				client.publish(update)
				instance := client.current.Ingesters[update.memberID]
				delegate.OnRingInstanceHeartbeat(nil, client.current, &instance)
			}
		})
	}
}

var _ kv.Client = (*ringManagerBenchmarkKV)(nil)
