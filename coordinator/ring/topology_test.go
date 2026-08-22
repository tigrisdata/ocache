// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	dskitring "github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestGetNodeTokensPreservesIngressOrderingAndOwnership(t *testing.T) {
	const tokensPerNode = 512

	generatedDelegate := &ringDelegate{rm: &RingManager{
		cfg:    LifecyclerConfig{NumTokens: tokensPerNode},
		epoch:  NewEpoch(),
		logger: log.NewNopLogger(),
	}}
	_, generatedTokens := generatedDelegate.OnRingInstanceRegister(
		nil,
		*dskitring.NewDesc(),
		false,
		"generated",
		dskitring.InstanceDesc{},
	)
	require.Len(t, generatedTokens, tokensPerNode)
	require.True(t, tokensAreSorted(generatedTokens), "generated tokens must be sorted before registration")

	legacyTokensPath := filepath.Join(t.TempDir(), "ring-tokens")
	require.NoError(t, os.WriteFile(legacyTokensPath, []byte(`{"tokens":[300,100,200]}`), 0o600))
	persistedDelegate := &ringDelegate{rm: &RingManager{
		cfg:    LifecyclerConfig{TokensFilePath: legacyTokensPath},
		epoch:  NewEpoch(),
		logger: log.NewNopLogger(),
	}}
	_, persistedTokens := persistedDelegate.OnRingInstanceRegister(
		nil,
		*dskitring.NewDesc(),
		false,
		"persisted",
		dskitring.InstanceDesc{},
	)
	require.Equal(t, dskitring.Tokens{100, 200, 300}, persistedTokens)

	desc := dskitring.NewDesc()
	registeredAt := time.Now()
	desc.AddIngester("generated", "127.0.0.1:10001", "", generatedTokens, dskitring.ACTIVE, registeredAt)
	desc.AddIngester("persisted", "127.0.0.1:10002", "", persistedTokens, dskitring.ACTIVE, registeredAt)
	desc.AddIngester("legacy", "127.0.0.1:10003", "", []uint32{900, 700, 800}, dskitring.ACTIVE, registeredAt)

	encoded, err := desc.Marshal()
	require.NoError(t, err)
	deserialized := dskitring.NewDesc()
	require.NoError(t, deserialized.Unmarshal(encoded))
	legacy := deserialized.Ingesters["legacy"]
	require.False(t, tokensAreSorted(legacy.Tokens), "the wire format can contain legacy unsorted tokens")

	rm := newTopologyTestRingManager(t, deserialized)
	instances, err := rm.ring.GetAllHealthy(dskitring.Reporting)
	require.NoError(t, err)
	require.Len(t, instances.Instances, 3)
	for _, instance := range instances.Instances {
		require.Truef(t, tokensAreSorted(instance.Tokens), "%s tokens must be sorted when the ring materializes a descriptor", instance.Id)
	}

	assignments := rm.GetNodeTokens()
	require.Len(t, assignments, 3)
	for nodeID, tokens := range assignments {
		require.Truef(t, tokensAreSorted(tokens), "%s tokens must be sorted in topology output", nodeID)
	}

	originalGenerated := append([]uint32(nil), assignments["generated"]...)
	assignments["generated"][0] = ^uint32(0)
	require.Equal(t, originalGenerated, rm.GetNodeTokens()["generated"], "topology output must not share ring token storage")
}

func newTopologyTestRingManager(t *testing.T, desc *dskitring.Desc) *RingManager {
	t.Helper()

	ctx := context.Background()
	reg := prometheus.NewRegistry()
	store, closer := consul.NewInMemoryClient(dskitring.GetCodec(), log.NewNopLogger(), reg)
	t.Cleanup(func() {
		require.NoError(t, closer.Close())
	})

	require.NoError(t, store.CAS(ctx, RingKey, func(interface{}) (interface{}, bool, error) {
		return desc, true, nil
	}))

	reader, err := dskitring.New(dskitring.Config{
		KVStore:           kv.Config{Mock: store},
		HeartbeatTimeout:  time.Hour,
		ReplicationFactor: 1,
	}, "topology-test", RingKey, log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), reader))
	})

	return &RingManager{ring: reader}
}

func tokensAreSorted(tokens []uint32) bool {
	return sort.SliceIsSorted(tokens, func(i, j int) bool { return tokens[i] < tokens[j] })
}
