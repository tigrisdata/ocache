// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/go-kit/log"
	dskitring "github.com/grafana/dskit/ring"
)

func BenchmarkOnRingInstanceHeartbeat(b *testing.B) {
	for _, members := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			ringDesc := heartbeatBenchmarkDesc(members)
			manager := &RingManager{
				logger:         log.NewNopLogger(),
				lastKnownNodes: make(map[string]dskitring.InstanceState, members),
			}
			lifecycler := &dskitring.BasicLifecycler{}
			manager.lifecycler = lifecycler
			delegate := &ringDelegate{rm: manager}
			localInstance := ringDesc.Ingesters["member-0"]

			// Put the ring into the stable state used by the benchmark. The helper
			// enables the normal callback context when those fields exist in the
			// measured revision; the comparison revision simply keeps scanning.
			prepareHeartbeatBenchmark(manager, ringDesc)

			b.ResetTimer()
			for b.Loop() {
				delegate.OnRingInstanceHeartbeat(lifecycler, ringDesc, &localInstance)
			}
		})
	}
}

// BenchmarkOnRingInstanceHeartbeatLocalStateChange measures a local state
// transition separately from the stable callback. The callback must preserve
// the gauge values without rebuilding the full membership maps for this path.
func BenchmarkOnRingInstanceHeartbeatLocalStateChange(b *testing.B) {
	for _, members := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			baseDesc := heartbeatBenchmarkDesc(members)
			joiningDesc := baseDesc.Clone().(*dskitring.Desc)
			joiningDesc.Ingesters["member-0"] = dskitring.InstanceDesc{
				Id:        "member-0",
				State:     dskitring.JOINING,
				Timestamp: 1,
			}
			activeDesc := baseDesc.Clone().(*dskitring.Desc)
			activeDesc.Ingesters["member-0"] = dskitring.InstanceDesc{
				Id:        "member-0",
				State:     dskitring.ACTIVE,
				Timestamp: 1,
			}

			manager := &RingManager{
				logger:         log.NewNopLogger(),
				lastKnownNodes: make(map[string]dskitring.InstanceState, members),
			}
			lifecycler := &dskitring.BasicLifecycler{}
			manager.lifecycler = lifecycler
			delegate := &ringDelegate{rm: manager}
			prepareHeartbeatBenchmark(manager, baseDesc)
			joiningInstance := joiningDesc.Ingesters["member-0"]
			activeInstance := activeDesc.Ingesters["member-0"]

			useJoining := true
			b.ResetTimer()
			for b.Loop() {
				if useJoining {
					delegate.OnRingInstanceHeartbeat(lifecycler, joiningDesc, &joiningInstance)
				} else {
					delegate.OnRingInstanceHeartbeat(lifecycler, activeDesc, &activeInstance)
				}
				useJoining = !useJoining
			}
		})
	}
}

// prepareHeartbeatBenchmark makes the same benchmark exercise the normal
// callback context on revisions that have the membership snapshot fields. The
// comparison revision predates those fields, so its setup is a no-op there and
// the original callback remains the measured path.
func prepareHeartbeatBenchmark(manager *RingManager, ringDesc *dskitring.Desc) {
	manager.logMembershipChange(ringDesc, 1)

	value := reflect.ValueOf(manager).Elem()
	for _, name := range []string{
		"membershipWatcherObserved",
		"membershipChangeObserverPresent",
		"heartbeatCASActive",
	} {
		field := value.FieldByName(name)
		if !field.IsValid() || !field.CanAddr() {
			continue
		}
		// These are atomic.Bool fields in the optimized revision. Reaching them
		// by name keeps this benchmark source compilable on the scan baseline,
		// where the fields do not exist.
		field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
		store := field.Addr().MethodByName("Store")
		if store.IsValid() {
			store.Call([]reflect.Value{reflect.ValueOf(true)})
		}
	}
}

func heartbeatBenchmarkDesc(members int) *dskitring.Desc {
	ringDesc := &dskitring.Desc{
		Ingesters: make(map[string]dskitring.InstanceDesc, members),
	}
	for member := 0; member < members; member++ {
		id := fmt.Sprintf("member-%d", member)
		ringDesc.Ingesters[id] = dskitring.InstanceDesc{
			Id:    id,
			State: dskitring.ACTIVE,
		}
	}
	return ringDesc
}
