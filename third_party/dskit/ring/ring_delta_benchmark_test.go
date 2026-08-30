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
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
)

type benchmarkRingUpdate struct {
	memberID  string
	timestamp int64
}

type benchmarkRingKV struct {
	mu       sync.Mutex
	initial  *Desc
	version  uint64
	ready    chan struct{}
	readyOne sync.Once
	full     func(interface{}) bool
	delta    func(memberlist.WatchKeyChange) bool
}

func (c *benchmarkRingKV) List(context.Context, string) ([]string, error) { return nil, nil }
func (c *benchmarkRingKV) Get(context.Context, string) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return proto.Clone(c.initial), nil
}
func (c *benchmarkRingKV) GetWithVersion(context.Context, string) (interface{}, uint64, error) {
	value, err := c.Get(context.Background(), "")
	return value, c.version, err
}
func (c *benchmarkRingKV) Delete(context.Context, string) error { return nil }
func (c *benchmarkRingKV) CAS(context.Context, string, func(interface{}) (interface{}, bool, error)) error {
	return errors.New("not implemented")
}
func (c *benchmarkRingKV) WatchKey(ctx context.Context, _ string, f func(interface{}) bool) {
	c.mu.Lock()
	c.full = f
	c.mu.Unlock()
	c.readyOne.Do(func() { close(c.ready) })
	<-ctx.Done()
}
func (c *benchmarkRingKV) WatchPrefix(context.Context, string, func(string, interface{}) bool) {}

func (c *benchmarkRingKV) WatchKeyWithChanges(ctx context.Context, _ string, f func(memberlist.WatchKeyChange) bool) {
	c.mu.Lock()
	c.delta = f
	initial := proto.Clone(c.initial).(memberlist.Mergeable)
	version := c.version
	c.mu.Unlock()
	if !f(memberlist.WatchKeyChange{Value: initial, Sequence: version, FullSnapshot: true}) {
		return
	}
	c.readyOne.Do(func() { close(c.ready) })
	<-ctx.Done()
}

func (c *benchmarkRingKV) publish(update benchmarkRingUpdate) {
	c.mu.Lock()
	c.version++
	version := c.version
	full := c.full
	deltaWatcher := c.delta
	base := c.initial.Ingesters[update.memberID]
	c.mu.Unlock()

	if deltaWatcher != nil {
		base.Timestamp = update.timestamp
		base.Tokens = append([]uint32(nil), base.Tokens...)
		delta := NewDesc()
		delta.Ingesters[update.memberID] = base
		deltaWatcher(memberlist.WatchKeyChange{
			Value:       delta,
			ChangedKeys: []string{update.memberID},
			Sequence:    version,
		})
		return
	}

	fullValue := proto.Clone(c.initial).(*Desc)
	instance := fullValue.Ingesters[update.memberID]
	instance.Timestamp = update.timestamp
	fullValue.Ingesters[update.memberID] = instance
	full(fullValue)
}

func benchmarkRingDesc(members, tokensPerMember int) *Desc {
	desc := NewDesc()
	now := time.Now().Unix()
	for member := 0; member < members; member++ {
		tokens := make([]uint32, tokensPerMember)
		for token := range tokens {
			tokens[token] = uint32(member*tokensPerMember + token + 1)
		}
		id := fmt.Sprintf("member-%d", member)
		desc.AddIngester(id, id, "", tokens, ACTIVE, time.Unix(now, 0))
	}
	return desc
}

func BenchmarkRingHeartbeatUpdate(b *testing.B) {
	for _, members := range []int{16, 64} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			initial := benchmarkRingDesc(members, 512)
			client := &benchmarkRingKV{initial: initial, version: 1, ready: make(chan struct{})}
			r, err := NewWithStoreClientAndStrategy(
				Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 3},
				"benchmark", "ring", client, NewDefaultReplicationStrategy(),
				prometheus.NewRegistry(), log.NewNopLogger(),
			)
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			if err := services.StartAndAwaitRunning(ctx, r); err != nil {
				cancel()
				b.Fatal(err)
			}
			<-client.ready
			defer func() {
				cancel()
				if err := services.StopAndAwaitTerminated(context.Background(), r); err != nil {
					b.Error(err)
				}
			}()

			updates := make([]benchmarkRingUpdate, 0, members)
			for member := 0; member < members; member++ {
				id := fmt.Sprintf("member-%d", member)
				updates = append(updates, benchmarkRingUpdate{memberID: id, timestamp: time.Now().Unix() + int64(member) + 1})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				update := updates[i%len(updates)]
				update.timestamp += int64(i / len(updates))
				client.publish(update)
			}
		})
	}
}

var _ kv.Client = (*benchmarkRingKV)(nil)
