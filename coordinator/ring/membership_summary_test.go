// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	dskitring "github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/common/metrics"
)

type membershipSnapshotTestClient struct {
	kv.Client
	desc *dskitring.Desc
}

func (c *membershipSnapshotTestClient) CAS(_ context.Context, _ string, f func(interface{}) (interface{}, bool, error)) error {
	out, _, err := f(c.desc.Clone())
	if err != nil {
		return err
	}
	if out != nil {
		c.desc = out.(*dskitring.Desc)
	}
	return nil
}

var _ kv.Client = (*membershipSnapshotTestClient)(nil)

func membershipTestDesc(states ...dskitring.InstanceState) *dskitring.Desc {
	ingesters := make(map[string]dskitring.InstanceDesc, len(states))
	for i, state := range states {
		id := "node-" + string(rune('a'+i))
		ingesters[id] = dskitring.InstanceDesc{Id: id, State: state}
	}
	return &dskitring.Desc{Ingesters: ingesters}
}

func TestHeartbeatInitializesMembershipSnapshot(t *testing.T) {
	rm := &RingManager{}
	delegate := &ringDelegate{rm: rm}
	desc := membershipTestDesc(dskitring.ACTIVE, dskitring.LEAVING, dskitring.JOINING)

	delegate.OnRingInstanceHeartbeat(nil, desc, nil)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.False(t, counts.synchronized, "a first heartbeat is not watcher-synchronized yet")
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 3, counts.total)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(3), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestHeartbeatNormalCASUsesSnapshotWithoutChangingRingDescriptor(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lifecycler: &dskitring.BasicLifecycler{}}
	desc := membershipTestDesc(dskitring.ACTIVE, dskitring.ACTIVE)
	rm.logMembershipChange(desc, 1)
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)
	expected := rm.membershipCounts.Load()
	store := &membershipSnapshotTestClient{desc: desc}
	client := &membershipClient{delegate: store, manager: rm}
	delegate := &ringDelegate{rm: rm}
	local := desc.Ingesters["node-a"]

	require.NoError(t, client.CAS(context.Background(), RingKey, func(in interface{}) (interface{}, bool, error) {
		delegate.OnRingInstanceHeartbeat(rm.lifecycler, in.(*dskitring.Desc), &local)
		return in, true, nil
	}))

	assert.Same(t, expected, rm.membershipCounts.Load(), "a stable CAS must not rebuild the snapshot")
	assert.Equal(t, 2, len(store.desc.Ingesters), "the shared descriptor must contain only real members")
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestHeartbeatNormalCASRefreshesLocalStateWithObserver(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lifecycler: &dskitring.BasicLifecycler{}}
	delegate := &ringDelegate{rm: rm}
	initial := membershipTestDesc(dskitring.ACTIVE)
	rm.logMembershipChange(initial, 1)
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)
	rm.heartbeatCASActive.Store(true)
	store := &membershipSnapshotTestClient{desc: initial}
	client := &membershipClient{delegate: store, manager: rm}

	local := initial.Ingesters["node-a"]
	require.NoError(t, client.CAS(context.Background(), RingKey, func(in interface{}) (interface{}, bool, error) {
		delegate.OnRingInstanceHeartbeat(rm.lifecycler, in.(*dskitring.Desc), &local)
		return in, true, nil
	}))

	changed := initial.Clone().(*dskitring.Desc)
	changed.Ingesters["node-a"] = dskitring.InstanceDesc{Id: "node-a", State: dskitring.LEAVING, Timestamp: 2}
	store.desc = changed
	local = changed.Ingesters["node-a"]
	require.NoError(t, client.CAS(context.Background(), RingKey, func(in interface{}) (interface{}, bool, error) {
		delegate.OnRingInstanceHeartbeat(rm.lifecycler, in.(*dskitring.Desc), &local)
		return in, true, nil
	}))

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 0, counts.active)
	assert.Equal(t, 1, counts.total)
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestMembershipSnapshotIgnoresOlderDecodedDeltas(t *testing.T) {
	rm := &RingManager{}
	counts := rm.membershipSnapshot()
	initial := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"active":  {Id: "active", State: dskitring.ACTIVE, Timestamp: 10},
		"joining": {Id: "joining", State: dskitring.JOINING, Timestamp: 10},
	}}
	counts.replace(initial, true)
	counts.applyDelta(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"active":  {Id: "active", State: dskitring.LEAVING, Timestamp: 12},
		"joining": {Id: "joining", State: dskitring.ACTIVE, Timestamp: 12},
	}})
	counts.applyDelta(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"active":  {Id: "active", State: dskitring.ACTIVE, Timestamp: 11},
		"joining": {Id: "joining", State: dskitring.JOINING, Timestamp: 11},
	}})

	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 2, counts.total)
}

func TestMembershipSnapshotApplyDeltaInvalidatesSynchronization(t *testing.T) {
	rm := &RingManager{}
	counts := rm.membershipSnapshot()
	initial := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"node-a": {Id: "node-a", State: dskitring.ACTIVE, Timestamp: 1},
	}}
	counts.replace(initial, true)
	assert.True(t, counts.read().synchronized)

	counts.applyDelta(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"node-a": {Id: "node-a", State: dskitring.LEAVING, Timestamp: 2},
	}})

	values := counts.read()
	assert.False(t, values.synchronized)
	assert.True(t, values.pending)
	assert.Equal(t, 0, values.active)
	assert.Equal(t, 1, values.total)
}

func TestMembershipSnapshotIgnoresTimestampOnlyDelta(t *testing.T) {
	rm := &RingManager{}
	counts := rm.membershipSnapshot()
	initial := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"active":  {Id: "active", State: dskitring.ACTIVE, Timestamp: 1},
		"joining": {Id: "joining", State: dskitring.JOINING, Timestamp: 1},
	}}
	require.True(t, counts.replaceValuesIfCurrent(
		nil,
		1,
		2,
		map[string]dskitring.InstanceState{
			"active":  dskitring.ACTIVE,
			"joining": dskitring.JOINING,
		},
		map[string]int64{
			"active":  1,
			"joining": 1,
		},
		initial,
		true,
		false,
	))

	before := counts.read()
	counts.applyDelta(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"active": {Id: "active", State: dskitring.ACTIVE, Timestamp: 2},
	}})

	after := counts.read()
	require.Same(t, before.values, after.values, "a timestamp-only heartbeat must not replace the fast-path snapshot")
	assert.True(t, after.synchronized)
	assert.False(t, after.pending)
	assert.True(t, after.values.descriptorValidated)
	assert.Equal(t, 1, after.active)
	assert.Equal(t, 2, after.total)
	updated, ok := counts.cache.states.Load("active")
	require.True(t, ok)
	assert.Equal(t, int64(2), updated.(membershipEntryState).timestamp)

	// A normal CAS heartbeat can keep using the same immutable snapshot after
	// the remote timestamp advances; it must not fall back to countMembership.
	lifecycler := &dskitring.BasicLifecycler{}
	rm.lifecycler = lifecycler
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)
	rm.heartbeatCASActive.Store(true)
	rm.heartbeatCASIdentity.Store(true)
	rm.heartbeatCASSnapshot.Store(after.values)
	local := initial.Ingesters["active"]
	(&ringDelegate{rm: rm}).OnRingInstanceHeartbeat(lifecycler, initial, &local)
	assert.Same(t, after.values, counts.read().values, "timestamp-only updates must preserve the callback snapshot")

	// Once a real state transition is pending, later heartbeats in that same
	// state must advance its timestamp without dropping the pending marker.
	counts.applyDelta(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"active": {Id: "active", State: dskitring.LEAVING, Timestamp: 3},
	}})
	counts.applyDelta(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"active": {Id: "active", State: dskitring.LEAVING, Timestamp: 4},
	}})

	pending := counts.read()
	assert.False(t, pending.synchronized)
	assert.True(t, pending.pending)
	assert.Equal(t, 0, pending.active)
	updated, ok = counts.cache.states.Load("active")
	require.True(t, ok)
	assert.Equal(t, int64(4), updated.(membershipEntryState).timestamp)
	assert.True(t, updated.(membershipEntryState).pending)
}

func TestHeartbeatSameTimestampAuthoritativeStateWins(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lastKnownNodes: map[string]dskitring.InstanceState{}}
	delegate := &ringDelegate{rm: rm}
	lifecycler := &dskitring.BasicLifecycler{}
	rm.lifecycler = lifecycler
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)
	rm.heartbeatCASActive.Store(true)

	initial := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local": {Id: "local", State: dskitring.ACTIVE, Timestamp: 9},
	}}
	rm.logMembershipChange(initial, 1)
	rm.applyMembershipChange(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local": {Id: "local", State: dskitring.JOINING, Timestamp: 10},
	}})

	authoritative := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local": {Id: "local", State: dskitring.ACTIVE, Timestamp: 10},
	}}
	// dskit accepts only LEFT as a same-timestamp override. The full ACTIVE
	// descriptor must therefore replace the equal-timestamp pending JOINING
	// delta before the heartbeat publishes the gauges.
	rm.logMembershipChange(authoritative, 2)
	store := &membershipSnapshotTestClient{desc: authoritative}
	client := &membershipClient{delegate: store, manager: rm}
	require.NoError(t, client.CAS(context.Background(), RingKey, func(in interface{}) (interface{}, bool, error) {
		desc := in.(*dskitring.Desc)
		local := desc.Ingesters["local"]
		delegate.OnRingInstanceHeartbeat(lifecycler, desc, &local)
		return in, true, nil
	}))

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 1, counts.total)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestHeartbeatDropsDecodedMemberAbsentFromAuthoritativeDescriptor(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lastKnownNodes: map[string]dskitring.InstanceState{}}
	delegate := &ringDelegate{rm: rm}
	lifecycler := &dskitring.BasicLifecycler{}
	rm.lifecycler = lifecycler
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)

	authoritative := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local":  {Id: "local", State: dskitring.ACTIVE, Timestamp: 2},
		"remote": {Id: "remote", State: dskitring.ACTIVE, Timestamp: 2},
	}}
	rm.logMembershipChange(authoritative, 1)
	rm.applyMembershipChange(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		// The memberlist store may reject this stale registration because a
		// newer LEFT tombstone is already present, even though the tombstone
		// is absent from the descriptor passed to the heartbeat callback.
		"stale": {Id: "stale", State: dskitring.JOINING, Timestamp: 1},
	}})

	store := &membershipSnapshotTestClient{desc: authoritative}
	client := &membershipClient{delegate: store, manager: rm}
	require.NoError(t, client.CAS(context.Background(), RingKey, func(in interface{}) (interface{}, bool, error) {
		desc := in.(*dskitring.Desc)
		local := desc.Ingesters["local"]
		delegate.OnRingInstanceHeartbeat(lifecycler, desc, &local)
		return in, true, nil
	}))

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	values := counts.read()
	assert.Equal(t, 2, values.active)
	assert.Equal(t, 2, values.total)
	assert.False(t, values.pending)
	assert.True(t, values.synchronized)
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestExistingReservedNameRemainsAClusterMember(t *testing.T) {
	rm := &RingManager{}
	delegate := &ringDelegate{rm: rm}
	reservedID := "__ocache_membership_summary__"
	desc := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		reservedID:      {Id: reservedID, State: dskitring.ACTIVE},
		"ordinary-node": {Id: "ordinary-node", State: dskitring.JOINING},
	}}

	// Reserved-looking IDs are ordinary descriptor members and remain visible
	// to readers, including after an upgrade.
	delegate.OnRingInstanceHeartbeat(nil, desc, nil)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 2, counts.total)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestReservedSummaryNamesRemainMembers(t *testing.T) {
	rm := &RingManager{}
	delegate := &ringDelegate{rm: rm}
	summaryID := "__ocache_membership_summary__"
	versionID := "__ocache_membership_summary_version__"
	desc := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		summaryID:       {Id: summaryID, Addr: "1,1", State: dskitring.PENDING, Timestamp: 7},
		versionID:       {Id: versionID, Addr: "version", State: dskitring.PENDING, Timestamp: 7},
		"ordinary-node": {Id: "ordinary-node", State: dskitring.ACTIVE},
	}}

	// The descriptor membership map is authoritative. Even a pair of entries
	// that resembles an older bookkeeping format remains visible to readers.
	delegate.OnRingInstanceHeartbeat(nil, desc, nil)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 3, counts.total)
}

func TestWatcherEqualCardinalityChangeRequiresHeartbeatValidation(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lastKnownNodes: map[string]dskitring.InstanceState{}}
	delegate := &ringDelegate{rm: rm}
	lifecycler := &dskitring.BasicLifecycler{}
	rm.lifecycler = lifecycler
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)
	rm.heartbeatCASActive.Store(true)

	initial := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local":  {Id: "local", State: dskitring.ACTIVE, Timestamp: 1},
		"remote": {Id: "remote", State: dskitring.ACTIVE, Timestamp: 1},
	}}
	rm.logMembershipChange(initial, 1)
	local := initial.Ingesters["local"]
	delegate.OnRingInstanceHeartbeat(lifecycler, initial, &local)

	// The current descriptor replaces the old remote member with a JOINING
	// member. Its cardinality is unchanged, so a count-only validity check
	// would accept an older watcher value after this transition.
	current := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local":   {Id: "local", State: dskitring.ACTIVE, Timestamp: 2},
		"joining": {Id: "joining", State: dskitring.JOINING, Timestamp: 2},
	}}
	rm.logMembershipChange(current, 2)

	// Deliver the value cloned before the replacement. It must invalidate the
	// fast path rather than make the old, equal-sized set appear synchronized.
	rm.logMembershipChange(initial, 1)
	rm.logMembershipChange(initial, 1)
	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	values := counts.read().values
	require.NotNil(t, values)
	assert.False(t, values.synchronized)

	delegate.OnRingInstanceHeartbeat(lifecycler, current, &local)
	counts = rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 2, counts.total)
	assert.True(t, counts.synchronized)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestWatcherDoesNotRollbackDecodedRemoteState(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lastKnownNodes: map[string]dskitring.InstanceState{}}
	delegate := &ringDelegate{rm: rm}
	initial := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local":  {Id: "local", State: dskitring.ACTIVE, Timestamp: 1},
		"remote": {Id: "remote", State: dskitring.ACTIVE, Timestamp: 1},
	}}
	rm.logMembershipChange(initial, 1)
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)
	rm.applyMembershipChange(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"remote": {Id: "remote", State: dskitring.LEAVING, Timestamp: 2},
	}})

	// This callback represents an older watcher value arriving after the
	// decoded delta. It must not restore remote to ACTIVE in the cache.
	rm.logMembershipChange(initial, 1)
	lifecycler := &dskitring.BasicLifecycler{}
	rm.lifecycler = lifecycler
	rm.heartbeatCASActive.Store(true)
	local := initial.Ingesters["local"]
	changed := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local":  local,
		"remote": {Id: "remote", State: dskitring.LEAVING, Timestamp: 2},
	}}
	delegate.OnRingInstanceHeartbeat(lifecycler, changed, &local)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 2, counts.total)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestHeartbeatRefreshesUnknownLocalStateBeforeWatcher(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lastKnownNodes: map[string]dskitring.InstanceState{}}
	delegate := &ringDelegate{rm: rm}
	lifecycler := &dskitring.BasicLifecycler{}
	rm.lifecycler = lifecycler
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)
	rm.heartbeatCASActive.Store(true)

	initial := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local": {Id: "local", State: dskitring.JOINING, Timestamp: 1},
	}}
	rm.logMembershipChange(initial, 1)
	changed := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local": {Id: "local", State: dskitring.ACTIVE, Timestamp: 2},
	}}
	local := changed.Ingesters["local"]

	// The first normal heartbeat must compare the local callback state even
	// though no prior callback has initialized the local-state marker.
	delegate.OnRingInstanceHeartbeat(lifecycler, changed, &local)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 1, counts.total)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestPendingDeltaSurvivesOldHeartbeatAndUnrelatedWatcher(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lastKnownNodes: map[string]dskitring.InstanceState{}}
	delegate := &ringDelegate{rm: rm}
	lifecycler := &dskitring.BasicLifecycler{}
	rm.lifecycler = lifecycler
	rm.heartbeatCASActive.Store(true)
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)
	initial := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local":  {Id: "local", State: dskitring.ACTIVE, Timestamp: 1},
		"remote": {Id: "remote", State: dskitring.ACTIVE, Timestamp: 1},
	}}
	rm.logMembershipChange(initial, 1)
	rm.applyMembershipChange(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"remote": {Id: "remote", State: dskitring.LEAVING, Timestamp: 2},
	}})

	// The old descriptor forces an exact fallback, but the decoded delta must
	// remain unresolved until a full descriptor confirms or supersedes it.
	local := initial.Ingesters["local"]
	delegate.OnRingInstanceHeartbeat(lifecycler, initial, &local)

	// The authoritative descriptor is older than the decoded delta. Retaining
	// that delta must update the state index and counters together; otherwise a
	// later confirmation can clear pending without ever applying the transition.
	retained := rm.membershipCounts.Load()
	require.NotNil(t, retained)
	assert.Equal(t, 1, retained.active)
	assert.Equal(t, 2, retained.total)
	remote, known := retained.cache.states.Load("remote")
	require.True(t, known)
	assert.Equal(t, dskitring.LEAVING, remote.(membershipEntryState).state)
	assert.True(t, remote.(membershipEntryState).pending)

	watcher := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local":   {Id: "local", State: dskitring.ACTIVE, Timestamp: 1},
		"remote":  {Id: "remote", State: dskitring.ACTIVE, Timestamp: 1},
		"joining": {Id: "joining", State: dskitring.JOINING, Timestamp: 3},
	}}
	rm.logMembershipChange(watcher, 2)

	current := &dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"local":   local,
		"remote":  {Id: "remote", State: dskitring.LEAVING, Timestamp: 2},
		"joining": {Id: "joining", State: dskitring.JOINING, Timestamp: 3},
	}}
	delegate.OnRingInstanceHeartbeat(lifecycler, current, &local)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 3, counts.total)
	assert.True(t, counts.read().synchronized, "confirmed deltas must publish a synchronized snapshot")
	confirmed, known := counts.cache.states.Load("remote")
	require.True(t, known)
	assert.Equal(t, dskitring.LEAVING, confirmed.(membershipEntryState).state)
	assert.False(t, confirmed.(membershipEntryState).pending)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(3), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestHeartbeatUsesDecodedRemoteStateBeforeWatcher(t *testing.T) {
	rm := &RingManager{
		logger:         log.NewNopLogger(),
		lastKnownNodes: map[string]dskitring.InstanceState{},
	}
	delegate := &ringDelegate{rm: rm}
	initial := &dskitring.Desc{
		Ingesters: map[string]dskitring.InstanceDesc{
			"local":  {Id: "local", State: dskitring.ACTIVE, Timestamp: 1},
			"remote": {Id: "remote", State: dskitring.ACTIVE, Timestamp: 1},
		},
	}
	rm.logMembershipChange(initial, 1)
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)

	// The codec observer sees the remote delta before the memberlist store
	// merges it. The normal heartbeat can therefore trust the incremental state
	// even though the watcher callback has not run yet.
	rm.applyMembershipChange(&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{
		"remote": {Id: "remote", State: dskitring.LEAVING, Timestamp: 2},
	}})

	lifecycler := &dskitring.BasicLifecycler{}
	rm.lifecycler = lifecycler
	local := initial.Ingesters["local"]
	changed := &dskitring.Desc{
		Ingesters: map[string]dskitring.InstanceDesc{
			"local":  local,
			"remote": {Id: "remote", State: dskitring.LEAVING, Timestamp: 2},
		},
	}
	rm.heartbeatCASActive.Store(true)
	delegate.OnRingInstanceHeartbeat(lifecycler, changed, &local)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 2, counts.total)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestHeartbeatDirectCallerRefreshesChangedDescriptor(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lifecycler: &dskitring.BasicLifecycler{}}
	delegate := &ringDelegate{rm: rm}
	initial := membershipTestDesc(dskitring.ACTIVE, dskitring.ACTIVE)
	rm.logMembershipChange(initial, 1)
	local := initial.Ingesters["node-a"]
	changed := initial.Clone().(*dskitring.Desc)
	changed.Ingesters["node-b"] = dskitring.InstanceDesc{Id: "node-b", State: dskitring.LEAVING}

	// This call is not inside the lifecycler's CAS wrapper, so a changed
	// descriptor remains authoritative even though the cardinality is unchanged.
	delegate.OnRingInstanceHeartbeat(rm.lifecycler, changed, &local)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 2, counts.total)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestHeartbeatCASValidatesInitialSnapshotBeforeEqualCardinalityFastPath(t *testing.T) {
	rm := &RingManager{
		logger:         log.NewNopLogger(),
		lifecycler:     &dskitring.BasicLifecycler{},
		lastKnownNodes: map[string]dskitring.InstanceState{},
	}
	delegate := &ringDelegate{rm: rm}
	cached := membershipTestDesc(dskitring.ACTIVE, dskitring.JOINING)
	rm.logMembershipChange(cached, 1)
	rm.membershipWatcherObserved.Store(true)
	rm.membershipChangeObserverPresent.Store(true)
	rm.membershipLocalState.Store(int32(dskitring.ACTIVE))
	rm.membershipLocalStateKnown.Store(true)

	// First confirm the cached descriptor. The generic test client cannot
	// establish descriptor identity, so this callback must use the authoritative
	// path even though its member count is already cached.
	store := &membershipSnapshotTestClient{desc: cached}
	client := &membershipClient{delegate: store, manager: rm}
	local := cached.Ingesters["node-a"]
	require.NoError(t, client.CAS(context.Background(), RingKey, func(in interface{}) (interface{}, bool, error) {
		desc := in.(*dskitring.Desc)
		delegate.OnRingInstanceHeartbeat(rm.lifecycler, desc, &local)
		return in, true, nil
	}))

	// A later CAS sees a same-sized descriptor with a different member state.
	// It must still use the authoritative input instead of the cached one-active
	// count.
	current := membershipTestDesc(dskitring.ACTIVE, dskitring.ACTIVE)
	store.desc = current
	local = current.Ingesters["node-a"]
	require.NoError(t, client.CAS(context.Background(), RingKey, func(in interface{}) (interface{}, bool, error) {
		desc := in.(*dskitring.Desc)
		delegate.OnRingInstanceHeartbeat(rm.lifecycler, desc, &local)
		return in, true, nil
	}))

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 2, counts.active)
	assert.Equal(t, 2, counts.total)
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestHeartbeatRefreshesLocalStateBeforeWatcher(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lifecycler: &dskitring.BasicLifecycler{}}
	delegate := &ringDelegate{rm: rm}
	initial := membershipTestDesc(dskitring.ACTIVE)
	rm.logMembershipChange(initial, 1)
	changed := initial.Clone().(*dskitring.Desc)
	changed.Ingesters["node-a"] = dskitring.InstanceDesc{Id: "node-a", State: dskitring.LEAVING}
	instance := changed.Ingesters["node-a"]
	rm.heartbeatCASActive.Store(true)

	delegate.OnRingInstanceHeartbeat(rm.lifecycler, changed, &instance)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 0, counts.active)
	assert.Equal(t, 1, counts.total)
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("active")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ClusterNodes.WithLabelValues("total")))
}

func TestHeartbeatRefreshesRegistrationBeforeWatcher(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lifecycler: &dskitring.BasicLifecycler{}}
	delegate := &ringDelegate{rm: rm}
	initial := membershipTestDesc(dskitring.ACTIVE)
	rm.logMembershipChange(initial, 1)
	changed := initial.Clone().(*dskitring.Desc)
	changed.Ingesters["node-b"] = dskitring.InstanceDesc{Id: "node-b", State: dskitring.JOINING}
	instance := changed.Ingesters["node-a"]
	rm.heartbeatCASActive.Store(true)

	delegate.OnRingInstanceHeartbeat(rm.lifecycler, changed, &instance)

	counts := rm.membershipCounts.Load()
	require.NotNil(t, counts)
	assert.Equal(t, 1, counts.active)
	assert.Equal(t, 2, counts.total)
}

func TestMembershipSnapshotTracksWatcherStateChanges(t *testing.T) {
	rm := &RingManager{logger: log.NewNopLogger(), lastKnownNodes: map[string]dskitring.InstanceState{}}
	delegate := &ringDelegate{rm: rm}

	states := []*dskitring.Desc{
		membershipTestDesc(dskitring.JOINING),
		membershipTestDesc(dskitring.ACTIVE),
		membershipTestDesc(dskitring.LEAVING),
		&dskitring.Desc{Ingesters: map[string]dskitring.InstanceDesc{}},
	}
	wantActive := []int{0, 1, 0, 0}
	wantTotal := []int{1, 1, 1, 0}
	for i, desc := range states {
		rm.logMembershipChange(desc, uint64(i+1))
		delegate.OnRingInstanceHeartbeat(nil, desc, nil)
		counts := rm.membershipCounts.Load()
		require.NotNil(t, counts)
		assert.Equal(t, wantActive[i], counts.active)
		assert.Equal(t, wantTotal[i], counts.total)
	}
}

func TestMembershipClientDoesNotAddDescriptorEntries(t *testing.T) {
	desc := membershipTestDesc(dskitring.ACTIVE)
	store := &membershipSnapshotTestClient{desc: desc}
	client := &membershipClient{delegate: store}

	require.NoError(t, client.CAS(context.Background(), RingKey, func(in interface{}) (interface{}, bool, error) {
		return in, true, nil
	}))

	assert.Equal(t, []string{"node-a"}, sortedMembershipIDs(store.desc))
}

func sortedMembershipIDs(desc *dskitring.Desc) []string {
	ids := make([]string, 0, len(desc.Ingesters))
	for id := range desc.Ingesters {
		ids = append(ids, id)
	}
	// The helper is used with one member in this test; keep it small and avoid
	// introducing a sorting dependency into production code.
	return ids
}
