package ring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/prometheus/client_golang/prometheus"
)

type deltaTestClient struct {
	snapshot *Desc
	version  uint64
}

func (c *deltaTestClient) List(context.Context, string) ([]string, error) { return nil, nil }
func (c *deltaTestClient) Get(context.Context, string) (interface{}, error) {
	if c.snapshot == nil {
		return nil, nil
	}
	return c.snapshot.Clone(), nil
}
func (c *deltaTestClient) GetWithVersion(context.Context, string) (interface{}, uint64, error) {
	value, err := c.Get(context.Background(), "")
	return value, c.version, err
}
func (c *deltaTestClient) Delete(context.Context, string) error { return nil }
func (c *deltaTestClient) CAS(context.Context, string, func(interface{}) (interface{}, bool, error)) error {
	return errors.New("not implemented")
}
func (c *deltaTestClient) WatchKey(context.Context, string, func(interface{}) bool)            {}
func (c *deltaTestClient) WatchPrefix(context.Context, string, func(string, interface{}) bool) {}

func newDeltaTestRing(t *testing.T, client kv.Client) *Ring {
	t.Helper()
	r, err := NewWithStoreClientAndStrategy(
		Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 1},
		"delta-test", "ring", client, NewDefaultReplicationStrategy(),
		prometheus.NewRegistry(), log.NewNopLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func deltaTestDesc(addr string, timestamp int64, tokens []uint32, state InstanceState) *Desc {
	desc := NewDesc()
	desc.Ingesters["a"] = InstanceDesc{
		Addr:                addr,
		Timestamp:           timestamp,
		State:               state,
		Tokens:              append([]uint32(nil), tokens...),
		RegisteredTimestamp: 1,
		Id:                  "a",
	}
	return desc
}

func TestDescMergeMarksTopologyChangesWithoutChangingDescriptorValues(t *testing.T) {
	now := time.Now().Unix()
	current := deltaTestDesc("a:1", now, []uint32{10}, ACTIVE)

	liveness, err := current.Merge(deltaTestDesc("a:1", now+1, []uint32{10}, LEAVING), false)
	if err != nil {
		t.Fatal(err)
	}
	if liveness.(*Desc).TopologyChanged() {
		t.Fatal("liveness-only merge was marked as topology change")
	}

	topology, err := current.Merge(deltaTestDesc("a:2", now+2, []uint32{20}, ACTIVE), false)
	if err != nil {
		t.Fatal(err)
	}
	topologyDesc := topology.(*Desc)
	defer topologyDesc.ClearTopologyChanged()
	if !topologyDesc.TopologyChanged() {
		t.Fatal("topology merge was not marked")
	}
	clone := topology.Clone().(*Desc)
	if !clone.TopologyChanged() {
		t.Fatal("topology marker was lost while cloning merge result")
	}
	clone.ClearTopologyChanged()
	if clone.TopologyChanged() {
		t.Fatal("topology marker was not cleared")
	}
}

func TestRingAppliesLivenessDeltaWithoutReplacingTopology(t *testing.T) {
	now := time.Now().Unix()
	initial := deltaTestDesc("a:1", now, []uint32{10, 20}, ACTIVE)
	client := &deltaTestClient{snapshot: initial, version: 1}
	r := newDeltaTestRing(t, client)
	r.updateRingState(initial)
	r.deltaSequence = 1

	delta := deltaTestDesc("a:1", now+1, []uint32{10, 20}, LEAVING)
	instance := delta.Ingesters["a"]
	instance.Id = "" // older lifecyclers did not populate InstanceDesc.Id
	delta.Ingesters["a"] = instance
	if !r.applyRingDelta(delta, []string{"a"}, 2, false) {
		t.Fatal("liveness delta was rejected")
	}

	if got := r.ringTokens; len(got) != 2 || got[0] != 10 || got[1] != 20 {
		t.Fatalf("topology changed while applying liveness delta: %v", got)
	}
	state, err := r.GetInstanceState("a")
	if err != nil {
		t.Fatal(err)
	}
	if state != LEAVING {
		t.Fatalf("got state %s, want %s", state, LEAVING)
	}

	readDesc, err := r.getRing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := readDesc.Ingesters["a"].Timestamp; got != now+1 {
		t.Fatalf("got timestamp %d, want %d", got, now+1)
	}
	if got := r.metricCounts[LEAVING.String()]; got != 1 {
		t.Fatalf("got %d leaving members, want 1", got)
	}
	if got := r.metricCounts[ACTIVE.String()]; got != 0 {
		t.Fatalf("got %d active members, want 0", got)
	}
}

func TestRingLivenessDeltaChangesRoutingAndHealthyReaders(t *testing.T) {
	now := time.Now().Unix()
	initial := NewDesc()
	initial.AddIngester("a", "a:1", "", []uint32{10}, ACTIVE, time.Unix(1, 0))
	initial.AddIngester("b", "b:1", "", []uint32{20}, ACTIVE, time.Unix(1, 0))
	r := newDeltaTestRing(t, &deltaTestClient{snapshot: initial, version: 1})
	r.strategy = NewIgnoreUnhealthyInstancesReplicationStrategy()
	r.updateRingStateWithSequence(initial, 1)

	delta := NewDesc()
	delta.Ingesters["a"] = InstanceDesc{
		Addr:                "a:1",
		Timestamp:           now + 1,
		State:               LEAVING,
		Tokens:              []uint32{10},
		RegisteredTimestamp: 1,
		Id:                  "a",
	}
	if !r.applyRingDelta(delta, []string{"a"}, 2, false) {
		t.Fatal("routing liveness delta was rejected")
	}

	set, err := r.Get(5, Write, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Instances) != 1 || set.Instances[0].Addr != "b:1" {
		t.Fatalf("got write routes %#v, want only b:1", set.Instances)
	}
	healthy, err := r.GetAllHealthy(Write)
	if err != nil {
		t.Fatal(err)
	}
	if len(healthy.Instances) != 1 || healthy.Instances[0].Addr != "b:1" {
		t.Fatalf("got healthy routes %#v, want only b:1", healthy.Instances)
	}
}

func TestRingDeltaFallsBackForTopologyAndSequenceGap(t *testing.T) {
	now := time.Now().Unix()
	initial := deltaTestDesc("a:1", now, []uint32{10}, ACTIVE)
	client := &deltaTestClient{snapshot: initial, version: 1}
	r := newDeltaTestRing(t, client)
	r.updateRingStateWithSequence(initial, 1)

	topology := deltaTestDesc("a:2", now+1, []uint32{20}, ACTIVE)
	client.snapshot = topology
	client.version = 2
	if !r.updateRingChange(context.Background(), memberlist.WatchKeyChange{Value: topology, ChangedKeys: []string{"a"}, Sequence: 2, TopologyChanged: true}) {
		t.Fatal("topology fallback stopped the watcher")
	}
	if got := r.ringTokens; len(got) != 1 || got[0] != 20 {
		t.Fatalf("fallback did not rebuild topology: %v", got)
	}

	tokenMismatch := deltaTestDesc("a:2", now+2, []uint32{21}, ACTIVE)
	client.snapshot = tokenMismatch
	client.version = 3
	if !r.updateRingChange(context.Background(), memberlist.WatchKeyChange{Value: tokenMismatch, ChangedKeys: []string{"a"}, Sequence: 3}) {
		t.Fatal("token-change fallback stopped the watcher")
	}
	if got := r.ringTokens; len(got) != 1 || got[0] != 21 {
		t.Fatalf("token-change fallback did not rebuild topology: %v", got)
	}

	gap := deltaTestDesc("a:2", now+3, []uint32{21}, LEAVING)
	client.snapshot = gap
	client.version = 5
	if !r.updateRingChange(context.Background(), memberlist.WatchKeyChange{Value: gap, ChangedKeys: []string{"a"}, Sequence: 5}) {
		t.Fatal("sequence-gap recovery stopped the watcher")
	}
	state, err := r.GetInstanceState("a")
	if err != nil {
		t.Fatal(err)
	}
	if state != LEAVING || r.deltaSequence != 5 {
		t.Fatalf("recovery got state %s and sequence %d, want %s and 5", state, r.deltaSequence, LEAVING)
	}
}
