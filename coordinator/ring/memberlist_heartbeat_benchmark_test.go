// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	dskitmemberlist "github.com/grafana/dskit/kv/memberlist"
	dskitring "github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator/gossip"
)

// BenchmarkMemberlistHeartbeat measures the descriptor clone performed by the
// memberlist KV before the lifecycler invokes its heartbeat callback. The setup
// creates a stable descriptor through the same memberlist store used in
// production without adding bookkeeping entries to the shared ring.
func BenchmarkMemberlistHeartbeat(b *testing.B) {
	b.Run("members=1000", func(b *testing.B) {
		rawDesc := memberlistHeartbeatDescriptor(b, 1000)

		manager := &RingManager{
			logger:         log.NewNopLogger(),
			lastKnownNodes: make(map[string]dskitring.InstanceState, len(rawDesc.Ingesters)),
		}
		lifecycler := &dskitring.BasicLifecycler{}
		manager.lifecycler = lifecycler
		prepareHeartbeatBenchmark(manager, rawDesc)
		delegate := &ringDelegate{rm: manager}

		b.ResetTimer()
		for b.Loop() {
			desc := rawDesc.Clone().(*dskitring.Desc)
			_, _ = desc.RemoveTombstones(time.Time{})
			localInstance := desc.Ingesters["member-0"]
			delegate.OnRingInstanceHeartbeat(lifecycler, desc, &localInstance)
		}
		b.ReportMetric(float64(len(rawDesc.Ingesters)), "raw-entries")
	})
}

func memberlistHeartbeatDescriptor(b *testing.B, members int) *dskitring.Desc {
	ctx := context.Background()
	memberlistKV, err := gossip.NewMemberlist(
		"heartbeat-benchmark",
		"127.0.0.1:0",
		nil,
		log.NewNopLogger(),
		prometheus.NewRegistry(),
	)
	require.NoError(b, err)
	require.NoError(b, memberlistKV.Start(ctx))

	client := memberlistKV.Client()

	for member := 0; member < members; member++ {
		instanceID := fmt.Sprintf("member-%d", member)
		require.NoError(b, client.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
			desc, _ := in.(*dskitring.Desc)
			if desc == nil {
				desc = dskitring.NewDesc()
			}
			desc.Ingesters[instanceID] = dskitring.InstanceDesc{
				Id:        instanceID,
				State:     dskitring.ACTIVE,
				Timestamp: time.Now().Unix(),
				Tokens:    make([]uint32, 128),
			}
			return desc, true, nil
		}))
	}

	rawState := memberlistKV.GetKV().LocalState(false)
	require.NoError(b, memberlistKV.Stop(ctx))

	reader := bytes.NewReader(rawState)
	var desc *dskitring.Desc
	for reader.Len() > 0 {
		var size uint32
		require.NoError(b, binary.Read(reader, binary.BigEndian, &size))
		data := make([]byte, size)
		_, err := io.ReadFull(reader, data)
		require.NoError(b, err)

		pair := dskitmemberlist.KeyValuePair{}
		require.NoError(b, pair.Unmarshal(data))
		value, err := dskitring.GetCodec().Decode(pair.Value)
		require.NoError(b, err)
		desc, _ = value.(*dskitring.Desc)
	}
	require.NotNil(b, desc)
	return desc
}
