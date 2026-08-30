package ring

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

func TestDescMergeMarksTokenlessLeftTombstoneAsTopologyChange(t *testing.T) {
	now := time.Now().Unix()
	current := NewDesc()
	current.Ingesters["a"] = InstanceDesc{Addr: "a:1", Timestamp: now, State: ACTIVE, Tokens: []uint32{10}, RegisteredTimestamp: 1, Id: "a"}
	current.Ingesters["b"] = InstanceDesc{Addr: "b:1", Timestamp: now, State: ACTIVE, Tokens: []uint32{20}, RegisteredTimestamp: 1, Id: "b"}

	// A token conflict can leave an otherwise ACTIVE member tokenless.
	conflict := NewDesc()
	conflict.Ingesters["b"] = InstanceDesc{Addr: "b:1", Timestamp: now + 1, State: ACTIVE, Tokens: []uint32{10}, RegisteredTimestamp: 1, Id: "b"}
	if _, err := current.Merge(conflict, false); err != nil {
		t.Fatal(err)
	}
	if got := current.Ingesters["b"].Tokens; len(got) != 0 {
		t.Fatalf("conflict winner left b with tokens %v, want no tokens", got)
	}

	left := NewDesc()
	left.Ingesters["b"] = InstanceDesc{Addr: "b:1", Timestamp: now + 2, State: LEFT, RegisteredTimestamp: 1, Id: "b"}
	change, err := current.Merge(left, false)
	if err != nil {
		t.Fatal(err)
	}
	if !change.(*Desc).TopologyChanged() {
		t.Fatal("tokenless LEFT tombstone was not marked as a topology change")
	}
}

func TestRingRecoversSnapshotForTokenlessLeftTombstone(t *testing.T) {
	now := time.Now().Unix()
	initial := NewDesc()
	initial.Ingesters["a"] = InstanceDesc{Addr: "a:1", Timestamp: now, State: ACTIVE, Tokens: []uint32{10}, RegisteredTimestamp: 1, Id: "a"}
	initial.Ingesters["b"] = InstanceDesc{Addr: "b:1", Timestamp: now, State: ACTIVE, RegisteredTimestamp: 1, Id: "b"}
	client := &deltaTestClient{snapshot: initial, version: 1}
	r := newDeltaTestRing(t, client)
	r.updateRingStateWithSequence(initial, 1)

	merged := initial.Clone().(*Desc)
	left := NewDesc()
	left.Ingesters["b"] = InstanceDesc{Addr: "b:1", Timestamp: now + 1, State: LEFT, RegisteredTimestamp: 1, Id: "b"}
	change, err := merged.Merge(left, false)
	if err != nil {
		t.Fatal(err)
	}
	if !change.(*Desc).TopologyChanged() {
		t.Fatal("tokenless LEFT tombstone was not marked as a topology change")
	}

	recovered := NewDesc()
	recovered.Ingesters["a"] = initial.Ingesters["a"]
	client.snapshot = recovered
	client.version = 2
	if !r.updateRingChange(context.Background(), memberlist.WatchKeyChange{
		Value:           change,
		ChangedKeys:     []string{"b"},
		Sequence:        2,
		TopologyChanged: true,
	}) {
		t.Fatal("snapshot recovery stopped the watcher")
	}
	if _, ok := r.ringDesc.Ingesters["b"]; ok {
		t.Fatal("recovered ring retained tokenless LEFT tombstone")
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

func TestRingLivenessDeltaEmitsLeftMetrics(t *testing.T) {
	now := time.Now().Unix()
	initial := deltaTestDesc("a:1", now, []uint32{10}, ACTIVE)
	registry := prometheus.NewRegistry()
	r, err := NewWithStoreClientAndStrategy(
		Config{HeartbeatTimeout: 0, ReplicationFactor: 1},
		"left-metrics", "ring", nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	r.updateRingStateWithSequence(initial, 1)

	delta := deltaTestDesc("a:1", now+1, []uint32{10}, LEFT)
	if !r.applyRingDelta(delta, []string{"a"}, 2, false) {
		t.Fatal("left liveness delta was rejected")
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, family := range families {
		if family.GetName() != "ring_members" && family.GetName() != "ring_oldest_member_timestamp" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "state" && label.GetValue() == LEFT.String() {
					found[family.GetName()] = true
				}
			}
		}
	}
	if !found["ring_members"] || !found["ring_oldest_member_timestamp"] {
		t.Fatalf("LEFT metrics were not emitted: %v", found)
	}
}

func TestRingRejectsMixedDeltaWithoutPartialApply(t *testing.T) {
	now := time.Now().Unix()
	initial := NewDesc()
	initial.Ingesters["a"] = InstanceDesc{
		Addr:                "a:1",
		Timestamp:           now,
		State:               ACTIVE,
		Tokens:              []uint32{10},
		RegisteredTimestamp: 1,
		Id:                  "a",
	}
	initial.Ingesters["b"] = InstanceDesc{
		Addr:                "b:1",
		Timestamp:           now + 10,
		State:               ACTIVE,
		Tokens:              []uint32{20},
		RegisteredTimestamp: 1,
		Id:                  "b",
	}
	r := newDeltaTestRing(t, &deltaTestClient{snapshot: initial, version: 1})
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
	delta.Ingesters["b"] = InstanceDesc{
		Addr:                "b:1",
		Timestamp:           now + 1,
		State:               LEAVING,
		Tokens:              []uint32{20},
		RegisteredTimestamp: 1,
		Id:                  "b",
	}
	if r.applyRingDelta(delta, []string{"a", "b"}, 2, false) {
		t.Fatal("delta with an older member timestamp was accepted")
	}

	if r.deltaSequence != 1 {
		t.Fatalf("delta sequence advanced to %d, want 1", r.deltaSequence)
	}
	for id, wantTimestamp := range map[string]int64{"a": now, "b": now + 10} {
		got, err := r.getRing(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		instance := got.Ingesters[id]
		if instance.State != ACTIVE || instance.Timestamp != wantTimestamp {
			t.Fatalf("member %s became %s at %d, want ACTIVE at %d", id, instance.State, instance.Timestamp, wantTimestamp)
		}
	}
	if r.metricCounts[ACTIVE.String()] != 2 || r.metricCounts[LEAVING.String()] != 0 {
		t.Fatalf("metrics changed after rejected delta: active=%d leaving=%d", r.metricCounts[ACTIVE.String()], r.metricCounts[LEAVING.String()])
	}
}

func TestRingLivenessDeltaKeepsMetricTimestampHeapBounded(t *testing.T) {
	now := time.Now().Unix()
	initial := NewDesc()
	initial.Ingesters["a"] = InstanceDesc{Addr: "a:1", Timestamp: now, State: ACTIVE, Tokens: []uint32{10}, RegisteredTimestamp: 1, Id: "a"}
	initial.Ingesters["b"] = InstanceDesc{Addr: "b:1", Timestamp: now, State: ACTIVE, Tokens: []uint32{20}, RegisteredTimestamp: 1, Id: "b"}
	registry := prometheus.NewRegistry()
	r, err := NewWithStoreClientAndStrategy(
		Config{HeartbeatTimeout: 0, ReplicationFactor: 1},
		"bounded-metrics", "ring", nil, NewDefaultReplicationStrategy(), registry, log.NewNopLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	r.updateRingStateWithSequence(initial, 1)

	for i := int64(1); i <= 100; i++ {
		delta := NewDesc()
		instance := initial.Ingesters["b"]
		instance.Timestamp = now + i
		delta.Ingesters["b"] = instance
		if !r.applyRingDelta(delta, []string{"b"}, uint64(i+1), false) {
			t.Fatalf("heartbeat delta %d was rejected", i)
		}
	}

	if got := r.metricTimestampHeap[ACTIVE.String()].Len(); got != 2 {
		t.Fatalf("got %d metric timestamp entries, want one per member", got)
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

func TestSharedLivenessSnapshotIsConsistent(t *testing.T) {
	baseTimestamp := time.Now().Unix()
	if baseTimestamp%2 != 0 {
		baseTimestamp++
	}
	initial := NewDesc()
	initial.AddIngester("a", "a:1", "", []uint32{10}, ACTIVE, time.Unix(baseTimestamp, 0))
	initial.AddIngester("b", "b:1", "", []uint32{20}, ACTIVE, time.Unix(baseTimestamp, 0))
	r := newDeltaTestRing(t, &deltaTestClient{snapshot: initial, version: 1})
	r.updateRingStateWithSequence(initial, 1)

	// A cached subring has its own read lock but shares the parent's liveness
	// slots. This is the concurrent path that must see a single pair.
	subring := &Ring{
		cfg:                r.cfg,
		strategy:           r.strategy,
		ringDesc:           &Desc{Ingesters: map[string]InstanceDesc{"a": initial.Ingesters["a"]}},
		liveness:           r.liveness,
		livenessReadAtomic: true,
	}
	op := NewOp([]InstanceState{ACTIVE, LEAVING}, nil)
	const (
		readerCount = 8
		updateCount = 100000
	)
	stop := make(chan struct{})
	failures := make(chan error, 1)
	var (
		stopOnce sync.Once
		failOnce sync.Once
	)
	stopAll := func() { stopOnce.Do(func() { close(stop) }) }
	reportFailure := func(err error) {
		failOnce.Do(func() {
			failures <- err
			stopAll()
		})
	}

	var readers sync.WaitGroup
	readers.Add(readerCount)
	for reader := 0; reader < readerCount; reader++ {
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				set, err := subring.GetAllHealthy(op)
				if err != nil {
					reportFailure(err)
					return
				}
				if len(set.Instances) != 1 {
					reportFailure(fmt.Errorf("got %d cached instances, want 1", len(set.Instances)))
					return
				}
				instance := set.Instances[0]
				if instance.Timestamp%2 == 0 && instance.State != ACTIVE {
					reportFailure(fmt.Errorf("got %s with even timestamp %d", instance.State, instance.Timestamp))
					return
				}
				if instance.Timestamp%2 != 0 && instance.State != LEAVING {
					reportFailure(fmt.Errorf("got %s with odd timestamp %d", instance.State, instance.Timestamp))
					return
				}
			}
		}()
	}

	for update := 0; update < updateCount; update++ {
		timestamp := baseTimestamp + int64(update+1)
		state := ACTIVE
		if timestamp%2 != 0 {
			state = LEAVING
		}
		delta := NewDesc()
		delta.Ingesters["a"] = InstanceDesc{
			Addr:                "a:1",
			Timestamp:           timestamp,
			State:               state,
			Tokens:              []uint32{10},
			RegisteredTimestamp: initial.Ingesters["a"].RegisteredTimestamp,
			Id:                  "a",
		}
		if !r.applyRingDelta(delta, []string{"a"}, uint64(update+2), false) {
			reportFailure(fmt.Errorf("update %d was rejected", update))
			break
		}
	}
	stopAll()
	readers.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
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
