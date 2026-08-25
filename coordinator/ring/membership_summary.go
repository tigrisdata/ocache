// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/grafana/dskit/kv"
	dskitring "github.com/grafana/dskit/ring"
)

// membershipEntryCount is deliberately O(1) on the normal ring format. The
// optimized heartbeat path never writes bookkeeping entries into Ingesters, so
// len is the real membership cardinality and can be compared without another
// membership traversal.
func membershipEntryCount(desc *dskitring.Desc) int {
	if desc == nil {
		return 0
	}
	return len(desc.Ingesters)
}

type membershipSnapshotRead struct {
	active       int
	total        int
	synchronized bool
	descriptor   *dskitring.Desc
	pending      bool
	values       *membershipSnapshotValues
}

type membershipSnapshotValues struct {
	active       int
	total        int
	descriptor   *dskitring.Desc
	synchronized bool
	pending      bool
	// descriptorValidated is false until a full descriptor has been checked on
	// the heartbeat path. A watcher snapshot with a new member or state is
	// therefore never trusted solely because its cardinality is unchanged.
	descriptorValidated bool
	// callbackValidated means the counts came from the descriptor passed to a
	// heartbeat callback. It remains useful while a decoded delta is waiting for
	// the memberlist merge: repeated callbacks can reuse those counts without
	// comparing cloned descriptor pointers or scanning the ring again.
	callbackValidated bool
}

type membershipEntryState struct {
	state     dskitring.InstanceState
	timestamp int64
	pending   bool
	// counted is false when a pending decoded member is absent from the
	// descriptor used for the current counters.
	counted bool
}

// membershipSnapshotCache keeps the counters in one atomic value for the
// heartbeat path. Delta updates serialize only with one another; stable reads
// do not take their update lock.
type membershipSnapshotCache struct {
	updateMu sync.Mutex
	current  atomic.Pointer[membershipSnapshotValues]
	states   sync.Map // map[string]membershipEntryState
	pending  sync.Map // set of IDs whose decoded delta is not in a full descriptor yet
}

func (cache *membershipSnapshotCache) store(values *membershipSnapshotValues) {
	cache.current.Store(values)
}

func newMembershipSnapshotCache() *membershipSnapshotCache {
	cache := &membershipSnapshotCache{}
	cache.current.Store(&membershipSnapshotValues{})
	return cache
}

// read returns a consistent view of the counters. The state index is kept
// separate because only transition paths need to inspect it.
func (s *membershipCountSnapshot) read() membershipSnapshotRead {
	cache := s.ensureCache()
	values := cache.current.Load()
	if values == nil {
		values = &membershipSnapshotValues{}
	}

	return membershipSnapshotRead{
		active:       values.active,
		total:        values.total,
		synchronized: values.synchronized,
		descriptor:   values.descriptor,
		pending:      values.pending,
		values:       values,
	}
}

func (s *membershipCountSnapshot) pendingMatches(desc *dskitring.Desc) bool {
	cache := s.ensureCache()
	values := cache.current.Load()
	if values == nil || !values.pending {
		return true
	}
	if desc == nil {
		return false
	}

	// Probe one pending entry. A mismatch is enough to prove that this callback
	// still has the pre-merge descriptor. If the probe matches, confirmPending
	// checks the remaining entries once and the fallback reconciles the complete
	// descriptor. This keeps repeated callbacks on an unmerged descriptor O(1)
	// even when many decoded changes are waiting for the memberlist merge.
	matches := true
	checked := false
	cache.pending.Range(func(key, value interface{}) bool {
		id := key.(string)
		change := value.(membershipEntryState)
		instance, ok := desc.Ingesters[id]
		matches = ok && instance.State == change.state && instance.Timestamp == change.timestamp
		checked = true
		return false
	})
	return !checked || matches
}

func (s *membershipCountSnapshot) confirmPending(desc *dskitring.Desc) {
	if desc == nil {
		return
	}
	cache := s.ensureCache()
	cache.updateMu.Lock()
	defer cache.updateMu.Unlock()

	cache.pending.Range(func(key, value interface{}) bool {
		id := key.(string)
		change := value.(membershipEntryState)
		instance, ok := desc.Ingesters[id]
		if !ok || instance.State != change.state || instance.Timestamp != change.timestamp {
			return true
		}
		cache.pending.Delete(id)
		if current, known := cache.states.Load(id); known {
			entry := current.(membershipEntryState)
			entry.pending = false
			entry.counted = true
			cache.states.Store(id, entry)
		}
		return true
	})

	values := cache.current.Load()
	if values != nil && values.pending && !hasPendingMembershipChange(cache) {
		next := *values
		next.pending = false
		cache.store(&next)
	}
}

func hasPendingMembershipChange(cache *membershipSnapshotCache) bool {
	pending := false
	cache.pending.Range(func(_, _ interface{}) bool {
		pending = true
		return false
	})
	return pending
}

// membershipStatesMatch reports whether a complete watcher value has the same
// membership IDs and states as the snapshot that was already cached. A watcher
// can deliver a descriptor it cloned before a later update was installed, so a
// changed full value must not be considered synchronized merely because its
// counts match. The watcher already pays the O(N) cost to build states; keeping
// this comparison off the heartbeat path makes that ordering check free for
// stable heartbeats.
func membershipStatesMatch(cache *membershipSnapshotCache, states map[string]dskitring.InstanceState) bool {
	cachedCount := 0
	matches := true
	cache.states.Range(func(key, value interface{}) bool {
		cachedCount++
		id := key.(string)
		cached := value.(membershipEntryState)
		state, exists := states[id]
		if !exists || state != cached.state {
			matches = false
			return false
		}
		return true
	})
	if !matches || cachedCount != len(states) {
		return false
	}
	return true
}

func (s *membershipCountSnapshot) stateChanged(instanceID string, instanceDesc *dskitring.InstanceDesc) bool {
	if instanceDesc == nil || instanceID == "" {
		return false
	}
	cached, known := s.ensureCache().states.Load(instanceID)
	return !known || cached.(membershipEntryState).state != instanceDesc.State
}

// updateLocalStateIfCurrent applies the local lifecycler's state transition to
// a synchronized snapshot. The CAS descriptor already contains the complete
// membership view, while only this instance changed between the cached view
// and the callback. If another watcher or decoder update wins first, the
// caller falls back to an authoritative descriptor scan.
func (s *membershipCountSnapshot) updateLocalStateIfCurrent(expected *membershipSnapshotValues, descriptor *dskitring.Desc, instanceID string, instanceDesc *dskitring.InstanceDesc) bool {
	if expected == nil || descriptor == nil || instanceID == "" || instanceDesc == nil {
		return false
	}

	cache := s.ensureCache()
	cache.updateMu.Lock()
	defer cache.updateMu.Unlock()

	values := cache.current.Load()
	if values == nil || values != expected || !values.synchronized || values.pending || values.total != len(descriptor.Ingesters) {
		return false
	}
	if descriptorInstance, ok := descriptor.Ingesters[instanceID]; !ok || descriptorInstance.State != instanceDesc.State {
		return false
	}

	currentValue, known := cache.states.Load(instanceID)
	if !known {
		return false
	}
	current := currentValue.(membershipEntryState)
	if current.pending {
		return false
	}
	if current.state == instanceDesc.State {
		if instanceDesc.Timestamp <= current.timestamp {
			return true
		}
		next := *values
		next.descriptor = descriptor
		cache.states.Store(instanceID, membershipEntryState{state: current.state, timestamp: instanceDesc.Timestamp, counted: true})
		cache.store(&next)
		s.descriptor = descriptor
		s.synchronized = next.synchronized
		return true
	}
	if instanceDesc.Timestamp < current.timestamp {
		return false
	}

	next := *values
	if current.state == dskitring.ACTIVE {
		next.active--
	}
	if instanceDesc.State == dskitring.ACTIVE {
		next.active++
	}
	next.descriptor = descriptor
	cache.states.Store(instanceID, membershipEntryState{state: instanceDesc.State, timestamp: instanceDesc.Timestamp, counted: true})
	cache.store(&next)
	s.active = next.active
	s.total = next.total
	s.descriptor = descriptor
	s.synchronized = next.synchronized
	return true
}

func membershipEntryIsNewer(candidate, current membershipEntryState) bool {
	// Match dskit ring.Merge: a state change wins by timestamp, with LEFT as
	// the only same-timestamp override. Other equal-timestamp deltas are not
	// newer than the authoritative descriptor that already has that timestamp.
	return candidate.timestamp > current.timestamp ||
		(candidate.timestamp == current.timestamp && candidate.state == dskitring.LEFT && current.state != dskitring.LEFT)
}

func replaceMergedMembershipEntry(merged map[string]membershipEntryState, id string, candidate membershipEntryState, active, total *int) {
	current, exists := merged[id]
	if !exists {
		*total = *total + 1
		if candidate.state == dskitring.ACTIVE {
			*active = *active + 1
		}
	} else if current.state == dskitring.ACTIVE && candidate.state != dskitring.ACTIVE {
		*active = *active - 1
	} else if current.state != dskitring.ACTIVE && candidate.state == dskitring.ACTIVE {
		*active = *active + 1
	}
	candidate.counted = true
	merged[id] = candidate
}

// replaceValues publishes an authoritative watcher result. The cache merges
// it with newer decoded deltas so an older watcher callback cannot roll back a
// state that was observed synchronously by the memberlist codec.
func (s *membershipCountSnapshot) replaceValues(active, total int, states map[string]dskitring.InstanceState, timestamps map[string]int64, descriptor *dskitring.Desc, synchronized bool) {
	s.replaceValuesMerged(nil, active, total, states, timestamps, descriptor, synchronized)
}

// replaceValuesIfCurrent publishes an exact descriptor only when the cache has
// not changed since the caller read it. It is used by heartbeat transition
// fallbacks, where the callback's descriptor is the authoritative input.
func (s *membershipCountSnapshot) replaceValuesIfCurrent(expected *membershipSnapshotValues, active, total int, states map[string]dskitring.InstanceState, timestamps map[string]int64, descriptor *dskitring.Desc, synchronized, retainPending bool) bool {
	return s.replaceValuesInternal(expected, false, retainPending, false, active, total, states, timestamps, descriptor, synchronized)
}

func (s *membershipCountSnapshot) replaceValuesIfCurrentRetainingAbsent(expected *membershipSnapshotValues, active, total int, states map[string]dskitring.InstanceState, timestamps map[string]int64, descriptor *dskitring.Desc, synchronized, retainPending bool) bool {
	return s.replaceValuesInternal(expected, false, retainPending, true, active, total, states, timestamps, descriptor, synchronized)
}

func (s *membershipCountSnapshot) replaceValuesMerged(expected *membershipSnapshotValues, active, total int, states map[string]dskitring.InstanceState, timestamps map[string]int64, descriptor *dskitring.Desc, synchronized bool) bool {
	return s.replaceValuesInternal(expected, true, false, false, active, total, states, timestamps, descriptor, synchronized)
}

func (s *membershipCountSnapshot) replaceValuesInternal(expected *membershipSnapshotValues, mergeNewer, retainPending, retainAbsentPending bool, active, total int, states map[string]dskitring.InstanceState, timestamps map[string]int64, descriptor *dskitring.Desc, synchronized bool) bool {
	cache := s.ensureCache()
	cache.updateMu.Lock()
	defer cache.updateMu.Unlock()

	if expected != nil && cache.current.Load() != expected {
		return false
	}

	// A full watcher value is authoritative only after the next heartbeat has
	// validated it against the descriptor used by the lifecycler. In particular,
	// an equal-cardinality value may have replaced one member with another. If
	// the value differs from a previously cached complete snapshot, leave the
	// cache unsynchronized so a heartbeat cannot publish it without that check.
	// The initial watcher value also waits for one authoritative heartbeat; this
	// prevents a CAS that started before the watcher value was installed from
	// trusting counts for a same-sized descriptor.
	values := cache.current.Load()
	descriptorValidated := !mergeNewer
	if mergeNewer {
		hasExistingSnapshot := values != nil && (values.descriptor != nil || values.active != 0 || values.total != 0)
		if hasExistingSnapshot {
			statesMatch := membershipStatesMatch(cache, states)
			if !values.synchronized || !statesMatch {
				synchronized = false
			} else {
				descriptorValidated = values.descriptorValidated
			}
		}
	}

	pendingChanges := make(map[string]membershipEntryState)
	cache.pending.Range(func(key, value interface{}) bool {
		pendingChanges[key.(string)] = value.(membershipEntryState)
		return true
	})

	merged := make(map[string]membershipEntryState, len(states))
	for id, state := range states {
		merged[id] = membershipEntryState{state: state, timestamp: timestamps[id], counted: true}
	}
	activeCount := active
	totalCount := total
	pendingToRetain := make(map[string]membershipEntryState)
	if retainPending {
		// A heartbeat descriptor can be the value that a local CAS read before
		// the decoded change was merged. Keep an absent pending entry in the
		// side index so a later same-cardinality descriptor can prove the new
		// member is present. A watcher value is authoritative and does not use
		// this path, so it may discard an absent stale registration.
		for id, cached := range pendingChanges {
			authoritative, exists := merged[id]
			if (retainAbsentPending && !exists) || (exists && membershipEntryIsNewer(cached, authoritative)) {
				pendingToRetain[id] = cached
			}
		}
		for id, cached := range pendingToRetain {
			if authoritative, exists := merged[id]; exists && membershipEntryIsNewer(cached, authoritative) {
				replaceMergedMembershipEntry(merged, id, cached, &activeCount, &totalCount)
			}
		}
	}
	if mergeNewer {
		cache.states.Range(func(key, value interface{}) bool {
			id := key.(string)
			cached := value.(membershipEntryState)
			if cached.pending {
				// Pending deltas are merged below from their separate index.
				return true
			}
			authoritative, exists := merged[id]
			if !exists {
				return true
			}
			if membershipEntryIsNewer(cached, authoritative) {
				replaceMergedMembershipEntry(merged, id, cached, &activeCount, &totalCount)
			}
			return true
		})
		// The full watcher descriptor is authoritative for IDs it does not
		// contain; do not resurrect a registration that the store rejected.
		for id, cached := range pendingChanges {
			authoritative, exists := merged[id]
			if exists && membershipEntryIsNewer(cached, authoritative) {
				replaceMergedMembershipEntry(merged, id, cached, &activeCount, &totalCount)
			}
		}
	}

	cache.states.Range(func(key, _ interface{}) bool {
		cache.states.Delete(key)
		return true
	})
	cache.pending.Range(func(key, _ interface{}) bool {
		cache.pending.Delete(key)
		return true
	})

	stateCopy := make(map[string]dskitring.InstanceState, len(merged))
	timestampCopy := make(map[string]int64, len(merged))
	for id, entry := range merged {
		cache.states.Store(id, entry)
		if entry.pending {
			cache.pending.Store(id, entry)
		}
		stateCopy[id] = entry.state
		timestampCopy[id] = entry.timestamp
	}
	for id, entry := range pendingToRetain {
		if _, exists := merged[id]; !exists {
			entry.counted = false
			cache.states.Store(id, entry)
		}
		cache.pending.Store(id, entry)
	}
	pending := hasPendingMembershipChange(cache)
	synchronized = synchronized && !pending
	descriptorValidated = descriptorValidated && synchronized
	cache.store(&membershipSnapshotValues{
		active:              activeCount,
		total:               totalCount,
		descriptor:          descriptor,
		synchronized:        synchronized,
		pending:             pending,
		descriptorValidated: descriptorValidated,
		callbackValidated:   !mergeNewer,
	})

	// Keep these fields useful to package tests without putting them on the
	// heartbeat read path.
	s.active = activeCount
	s.total = totalCount
	s.states = stateCopy
	s.timestamps = timestampCopy
	s.descriptor = descriptor
	s.synchronized = synchronized
	return true
}

func (s *membershipCountSnapshot) markUnsynchronized() {
	cache := s.ensureCache()
	cache.updateMu.Lock()
	defer cache.updateMu.Unlock()

	values := cache.current.Load()
	if values == nil || !values.synchronized {
		return
	}
	next := *values
	next.synchronized = false
	next.callbackValidated = false
	cache.store(&next)
	s.synchronized = false
}

// replace refreshes the complete snapshot from an authoritative descriptor.
func (s *membershipCountSnapshot) replace(desc *dskitring.Desc, synchronized bool) {
	counts := countMembership(desc)
	s.replaceValues(counts.active, counts.total, counts.states, counts.timestamps, counts.descriptor, synchronized)
}

// applyDelta updates the process-local snapshot from a decoded ring change.
// Memberlist decodes an incoming change before merging it into its store, so a
// local heartbeat cannot observe a remote state change before this update.
// Only changed entries are visited; full descriptor replacement remains the
// watcher and transition fallback's responsibility.
func (s *membershipCountSnapshot) applyDelta(desc *dskitring.Desc) {
	if desc == nil {
		return
	}

	cache := s.ensureCache()
	cache.updateMu.Lock()
	defer cache.updateMu.Unlock()

	values := cache.current.Load()
	if values == nil {
		values = &membershipSnapshotValues{}
	}
	next := *values
	changed := false

	for id, instance := range desc.Ingesters {
		oldValue, known := cache.states.Load(id)
		var old membershipEntryState
		if known {
			old = oldValue.(membershipEntryState)
		}

		// Heartbeats advance timestamps without changing membership counts. Keep
		// the timestamp index current for a later state transition, but do not
		// invalidate the synchronized count snapshot or force the next heartbeat
		// back through the full descriptor scan.
		if known && instance.State == old.state {
			if instance.Timestamp > old.timestamp {
				old.timestamp = instance.Timestamp
				cache.states.Store(id, old)
				if old.pending {
					cache.pending.Store(id, old)
				}
			}
			continue
		}

		newer := !known || instance.Timestamp > old.timestamp
		leftOverride := known && instance.Timestamp == old.timestamp && old.state != dskitring.LEFT && instance.State == dskitring.LEFT
		if !newer && !leftOverride {
			continue
		}
		changed = true

		counted := !known || old.counted
		if counted {
			if !known {
				next.total++
			} else if old.state == dskitring.ACTIVE && instance.State != dskitring.ACTIVE {
				next.active--
			} else if old.state != dskitring.ACTIVE && instance.State == dskitring.ACTIVE {
				next.active++
			}
			if !known && instance.State == dskitring.ACTIVE {
				next.active++
			}
		}

		entry := membershipEntryState{state: instance.State, timestamp: instance.Timestamp, pending: true, counted: counted}
		cache.states.Store(id, entry)
		cache.pending.Store(id, entry)
	}

	if !changed {
		// Timestamp-only heartbeats update the side index above, but leave the
		// atomic count value and its identity intact for an in-flight CAS.
		return
	}

	// A decoded delta is only a partial view. Membership changes must be
	// validated by a full watcher value before the fast heartbeat path can use
	// the snapshot again.
	next.synchronized = false
	next.descriptorValidated = false
	next.callbackValidated = false
	next.pending = hasPendingMembershipChange(cache)
	cache.store(&next)
	s.active = next.active
	s.total = next.total
	s.synchronized = next.synchronized
}

func (s *membershipCountSnapshot) ensureCache() *membershipSnapshotCache {
	if s.cache == nil {
		cache := newMembershipSnapshotCache()
		cache.current.Store(&membershipSnapshotValues{
			active:       s.active,
			total:        s.total,
			descriptor:   s.descriptor,
			synchronized: s.synchronized,
		})
		s.cache = cache
	}
	return s.cache
}

// membershipSnapshot returns the manager's lazily initialized process-local
// cache. A partial delta before the first watcher observation remains marked
// unsynchronized, so heartbeats use an authoritative descriptor scan until the
// watcher establishes a complete view.
func (rm *RingManager) membershipSnapshot() *membershipCountSnapshot {
	counts := rm.membershipCounts.Load()
	if counts != nil {
		counts.ensureCache()
		return counts
	}

	initial := &membershipCountSnapshot{cache: newMembershipSnapshotCache()}
	if rm.membershipCounts.CompareAndSwap(nil, initial) {
		return initial
	}
	counts = rm.membershipCounts.Load()
	counts.ensureCache()
	return counts
}

// applyMembershipChange is registered with the memberlist ring codec. It runs
// synchronously while an incoming ring delta is decoded, before dskit merges
// that delta into the local descriptor.
func (rm *RingManager) applyMembershipChange(desc *dskitring.Desc) {
	rm.membershipSnapshot().applyDelta(desc)
}

// membershipCASIdentityProvider is implemented by the memberlist client whose
// decoder updates the snapshot before a remote change can enter the local CAS.
// Other KV clients cannot establish that the descriptor passed to CAS belongs
// to the cached membership set, so their callbacks use the authoritative path.
type membershipCASIdentityProvider interface {
	RegisterRingChangeObserver(func(*dskitring.Desc))
}

// membershipClient supplies the lifecycler's CAS callback context and arms the
// current validated snapshot for that callback. It does not change the shared
// ring descriptor. Older binaries count every Ingesters entry, so putting
// bookkeeping entries in that map would make rolling upgrades report false
// members.
type membershipClient struct {
	delegate kv.Client
	manager  *RingManager
}

func (c *membershipClient) List(ctx context.Context, prefix string) ([]string, error) {
	return c.delegate.List(ctx, prefix)
}

func (c *membershipClient) Get(ctx context.Context, key string) (interface{}, error) {
	return c.delegate.Get(ctx, key)
}

func (c *membershipClient) Delete(ctx context.Context, key string) error {
	return c.delegate.Delete(ctx, key)
}

func (c *membershipClient) CAS(ctx context.Context, key string, f func(interface{}) (interface{}, bool, error)) error {
	if c.manager == nil || key != RingKey {
		return c.delegate.CAS(ctx, key, f)
	}
	if _, ok := c.delegate.(membershipCASIdentityProvider); !ok {
		return c.delegate.CAS(ctx, key, f)
	}

	c.manager.heartbeatCASSnapshot.Store(c.manager.membershipSnapshot().read().values)
	c.manager.heartbeatCASIdentity.Store(true)
	c.manager.heartbeatCASActive.Store(true)
	defer func() {
		c.manager.heartbeatCASActive.Store(false)
		c.manager.heartbeatCASIdentity.Store(false)
		c.manager.heartbeatCASSnapshot.Store(nil)
	}()
	return c.delegate.CAS(ctx, key, f)
}

func (c *membershipClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	c.delegate.WatchKey(ctx, key, f)
}

func (c *membershipClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	c.delegate.WatchPrefix(ctx, prefix, f)
}

var _ kv.Client = (*membershipClient)(nil)
