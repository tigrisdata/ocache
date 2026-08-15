// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/tigrisdata/ocache/coordinator/ring"
	"github.com/tigrisdata/ocache/embedded"
)

const remoteRangeStreamNodeID = "embedded-range-stream-benchmark"

type remoteRangeStreamFixture struct {
	client  *embedded.Client
	nextKey int
}

func newRemoteRangeStreamFixture(tb testing.TB) *remoteRangeStreamFixture {
	tb.Helper()

	config := DefaultIntegrationTestConfig()
	// Keep test-only maintenance loops out of steady-state stream samples.
	config.CleanupInterval = time.Hour
	config.RecompactionInterval = time.Hour
	harness := NewClusterTestHarness(tb, 2, config)
	if err := harness.StartAllNodes(); err != nil {
		harness.Cleanup()
		tb.Fatalf("start peer cluster: %v", err)
	}

	ports, err := getFreePorts(2)
	if err != nil {
		harness.Cleanup()
		tb.Fatalf("allocate embedded client ports: %v", err)
	}

	client, err := embedded.New(&embedded.Config{
		DiskPath:        tb.TempDir(),
		TTL:             time.Hour,
		InlineThreshold: int(config.InlineThreshold),
		NodeID:          remoteRangeStreamNodeID,
		ClusterAddr:     fmt.Sprintf("127.0.0.1:%d", ports[0]),
		GRPCAddr:        fmt.Sprintf("127.0.0.1:%d", ports[1]),
		AdvertiseAddr:   fmt.Sprintf("127.0.0.1:%d", ports[1]),
		SeedNodes:       []string{fmt.Sprintf("127.0.0.1:%d", harness.memberlistPorts[0])},
		Registerer:      prometheus.NewRegistry(),
		Lifecycler: &ring.LifecyclerConfig{
			NumTokens:            128,
			ObservePeriod:        100 * time.Millisecond,
			MinReadyDuration:     0,
			UnregisterOnShutdown: true,
			RingConfig: ring.Config{
				HeartbeatPeriod:  100 * time.Millisecond,
				HeartbeatTimeout: 10 * time.Second,
			},
		},
	})
	if err != nil {
		harness.Cleanup()
		tb.Fatalf("create embedded client: %v", err)
	}
	if err := client.StartGRPCServer(); err != nil {
		_ = client.Close()
		harness.Cleanup()
		tb.Fatalf("start embedded client gRPC server: %v", err)
	}

	tb.Cleanup(func() {
		if err := client.Close(); err != nil {
			tb.Errorf("close embedded client: %v", err)
		}
		harness.Cleanup()
	})

	waitForRemoteRangeStreamCluster(tb, client, harness)
	return &remoteRangeStreamFixture{client: client}
}

func waitForRemoteRangeStreamCluster(tb testing.TB, client *embedded.Client, harness *ClusterTestHarness) {
	tb.Helper()

	const nodeCount = 3
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(client.GetConnectedNodes()) == nodeCount && peerNodesActive(harness, nodeCount) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	tb.Fatalf("cluster did not converge with embedded client: embedded sees %d active nodes", len(client.GetConnectedNodes()))
}

func peerNodesActive(harness *ClusterTestHarness, want int) bool {
	for _, node := range harness.Nodes {
		ring := node.Coordinator.GetRing()
		if ring == nil || len(ring.GetActiveNodes()) != want {
			return false
		}
	}
	return true
}

func (f *remoteRangeStreamFixture) remoteKey(tb testing.TB, prefix string) string {
	tb.Helper()

	for i := 0; i < 10_000; i++ {
		f.nextKey++
		key := fmt.Sprintf("%s-%d", prefix, f.nextKey)
		if !f.client.IsLocal(key) {
			return key
		}
	}
	tb.Fatalf("could not find a remote key for %s", prefix)
	return ""
}

func (f *remoteRangeStreamFixture) putRemote(tb testing.TB, key string, data []byte) {
	tb.Helper()
	if f.client.IsLocal(key) {
		tb.Fatalf("key %q is local", key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.client.Put(ctx, key, data, 0); err != nil {
		tb.Fatalf("put %q: %v", key, err)
	}
}

func TestEmbeddedClient_GetRangeStreamRemote(t *testing.T) {
	fixture := newRemoteRangeStreamFixture(t)
	data := remoteRangeStreamData(1 << 20)
	key := fixture.remoteKey(t, "remote-range-stream")
	fixture.putRemote(t, key, data)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Operations.Get receives the first frame before returning so it resolves a
	// found result before the caller starts draining the peer stream.
	reader, found, err := fixture.client.Operations().Get(ctx, key, 0, int64(len(data)-1))
	if err != nil {
		t.Fatalf("eager remote Get: %v", err)
	}
	if !found || reader == nil {
		t.Fatalf("eager remote Get = (reader %v, found %t), want a found reader", reader, found)
	}
	closer, ok := reader.(io.Closer)
	if !ok {
		t.Fatalf("remote reader %T does not support Close", reader)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("close eager remote reader: %v", err)
	}

	var output bytes.Buffer
	if err := fixture.client.GetRangeStream(ctx, key, 0, int64(len(data)-1), &output); err != nil {
		t.Fatalf("GetRangeStream: %v", err)
	}
	if !bytes.Equal(output.Bytes(), data) {
		t.Fatal("GetRangeStream output did not preserve peer byte order")
	}

	missingKey := fixture.remoteKey(t, "missing-remote-range-stream")
	reader, found, err = fixture.client.Operations().Get(ctx, missingKey, 0, 0)
	if err != nil {
		t.Fatalf("eager missing remote Get: %v", err)
	}
	if found || reader != nil {
		t.Fatalf("eager missing remote Get = (reader %v, found %t), want a miss", reader, found)
	}

	output.Reset()
	if err := fixture.client.GetRangeStream(ctx, missingKey, 0, 0, &output); err != nil {
		t.Fatalf("missing GetRangeStream: %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("missing GetRangeStream wrote %d bytes", output.Len())
	}
}

type remoteRangeStreamDiscardWriter struct{}

func (remoteRangeStreamDiscardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

// BenchmarkEmbeddedClient_GetRangeStreamRemote measures the steady-state
// embedded GetRangeStream route through Operations.Get and a remote peer.
func BenchmarkEmbeddedClient_GetRangeStreamRemote(b *testing.B) {
	// Keep integration logging out of the benchmark output and samples.
	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(previousLogLevel) })

	fixture := newRemoteRangeStreamFixture(b)
	ctx := context.Background()

	for _, size := range []int{64 << 10, 256 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			b.StopTimer()
			data := remoteRangeStreamData(size)
			key := fixture.remoteKey(b, fmt.Sprintf("remote-range-stream-%d", size))
			fixture.putRemote(b, key, data)

			var output bytes.Buffer
			if err := fixture.client.GetRangeStream(ctx, key, 0, int64(len(data)-1), &output); err != nil {
				b.Fatalf("preflight GetRangeStream: %v", err)
			}
			if !bytes.Equal(output.Bytes(), data) {
				b.Fatal("preflight GetRangeStream output did not preserve peer byte order")
			}

			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := fixture.client.GetRangeStream(ctx, key, 0, int64(len(data)-1), remoteRangeStreamDiscardWriter{}); err != nil {
					b.Fatalf("GetRangeStream: %v", err)
				}
			}
		})
	}
}

func remoteRangeStreamData(size int) []byte {
	data := make([]byte, size)
	var state uint32 = 1
	for i := range data {
		state = state*1664525 + 1013904223
		data[i] = byte(state >> 24)
	}
	return data
}
