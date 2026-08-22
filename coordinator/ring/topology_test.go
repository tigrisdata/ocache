// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ring

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	dskitring "github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/coordinator/gossip"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	"google.golang.org/protobuf/proto"
)

const (
	topologySnapshotMembers        = 3
	topologySnapshotTokensPerNode  = 128
	topologySnapshotTokenStride    = 10000
	topologySnapshotUpdateCount    = 8
	topologySnapshotReaderCount    = 4
	topologySnapshotPropagationMax = 5 * time.Second
)

func TestGetNodeTokens_ResponseSlicesRemainStableDuringRingUpdates(t *testing.T) {
	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	t.Cleanup(func() {
		zerolog.SetGlobalLevel(previousLogLevel)
	})

	ctx := context.Background()
	reg := prometheus.NewRegistry()
	memberlistKV, err := gossip.NewMemberlist("topology-response-snapshot", "127.0.0.1:0", nil, log.NewNopLogger(), reg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, memberlistKV.Stop(context.Background()))
	})
	require.NoError(t, memberlistKV.Start(ctx))

	client := memberlistKV.Client()
	timestamp := time.Now().Unix() + topologySnapshotUpdateCount + 1
	require.NoError(t, publishTopologySnapshot(ctx, client, 0, timestamp))

	manager, err := NewRingManager(LifecyclerConfig{
		RingConfig: Config{
			HeartbeatTimeout:  time.Hour,
			ReplicationFactor: 1,
		},
		InstanceID:   "topology-response-reader",
		InstanceAddr: "127.0.0.1",
	}, client, log.NewNopLogger(), reg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), manager.ring))
	})
	require.NoError(t, services.StartAndAwaitRunning(ctx, manager.ring))
	require.NoError(t, waitForTopologySnapshot(manager, 0, topologySnapshotPropagationMax))

	stopReaders := make(chan struct{})
	readerReady := make(chan struct{}, topologySnapshotReaderCount)
	readerErrs := make(chan error, topologySnapshotReaderCount)
	var marshaled atomic.Uint64
	var readers sync.WaitGroup
	for range topologySnapshotReaderCount {
		readers.Add(1)
		go func() {
			defer readers.Done()
			ready := false
			for {
				select {
				case <-stopReaders:
					return
				default:
				}

				if err := marshalAndVerifyTopologySnapshot(manager.GetNodeTokens()); err != nil {
					select {
					case readerErrs <- err:
					case <-stopReaders:
					}
					return
				}
				marshaled.Add(1)
				if !ready {
					readerReady <- struct{}{}
					ready = true
				}
			}
		}()
	}

	for range topologySnapshotReaderCount {
		select {
		case <-readerReady:
		case err := <-readerErrs:
			close(stopReaders)
			readers.Wait()
			require.NoError(t, err)
			return
		case <-time.After(topologySnapshotPropagationMax):
			close(stopReaders)
			readers.Wait()
			t.Fatal("topology readers did not marshal their initial snapshots")
		}
	}

	var updateErr error
	for generation := 1; generation <= topologySnapshotUpdateCount; generation++ {
		if err := publishTopologySnapshot(ctx, client, generation, timestamp+int64(generation)); err != nil {
			updateErr = fmt.Errorf("publish snapshot %d: %w", generation, err)
			break
		}
		if err := waitForTopologySnapshot(manager, generation, topologySnapshotPropagationMax); err != nil {
			updateErr = err
			break
		}
		readCount := marshaled.Load()
		if err := waitForTopologyMarshal(&marshaled, readCount, topologySnapshotPropagationMax); err != nil {
			updateErr = err
			break
		}
	}

	close(stopReaders)
	readers.Wait()
	require.NoError(t, updateErr)

	select {
	case err := <-readerErrs:
		require.NoError(t, err)
	default:
	}
}

func publishTopologySnapshot(ctx context.Context, client kv.Client, generation int, timestamp int64) error {
	return client.CAS(ctx, RingKey, func(interface{}) (interface{}, bool, error) {
		desc := dskitring.NewDesc()
		registeredAt := time.Unix(timestamp, 0)
		for member := 0; member < topologySnapshotMembers; member++ {
			nodeID := topologySnapshotNodeID(member)
			desc.AddIngester(
				nodeID,
				fmt.Sprintf("127.0.0.1:%d", 10000+member),
				"",
				topologySnapshotTokens(generation, member),
				dskitring.ACTIVE,
				registeredAt,
			)
			instance := desc.Ingesters[nodeID]
			instance.Timestamp = timestamp
			desc.Ingesters[nodeID] = instance
		}
		return desc, true, nil
	})
}

func waitForTopologySnapshot(manager *RingManager, generation int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		assignments := manager.GetNodeTokens()
		if topologySnapshotMatches(assignments, generation) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ring did not publish snapshot %d", generation)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTopologyMarshal(marshaled *atomic.Uint64, after uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for marshaled.Load() <= after {
		if time.Now().After(deadline) {
			return fmt.Errorf("topology readers did not marshal after a ring update")
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

func marshalAndVerifyTopologySnapshot(assignments map[string][]uint32) error {
	if len(assignments) != topologySnapshotMembers {
		return fmt.Errorf("GetNodeTokens returned %d members, want %d", len(assignments), topologySnapshotMembers)
	}

	config := &clusterpb.RingConfig{
		NodeTokens: make([]*clusterpb.NodeTokens, 0, topologySnapshotMembers),
	}
	for member := 0; member < topologySnapshotMembers; member++ {
		nodeID := topologySnapshotNodeID(member)
		tokens, ok := assignments[nodeID]
		if !ok {
			return fmt.Errorf("GetNodeTokens omitted %q", nodeID)
		}
		config.NodeTokens = append(config.NodeTokens, &clusterpb.NodeTokens{
			NodeId: nodeID,
			Tokens: tokens,
		})
	}

	encoded, err := proto.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal topology: %w", err)
	}
	var decoded clusterpb.RingConfig
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		return fmt.Errorf("unmarshal topology: %w", err)
	}
	if len(decoded.GetNodeTokens()) != topologySnapshotMembers {
		return fmt.Errorf("decoded %d members, want %d", len(decoded.GetNodeTokens()), topologySnapshotMembers)
	}
	for member, assignment := range decoded.GetNodeTokens() {
		if assignment.GetNodeId() != topologySnapshotNodeID(member) {
			return fmt.Errorf("decoded member %d as %q", member, assignment.GetNodeId())
		}
		if err := validateTopologySnapshotTokens(assignment.GetTokens(), member); err != nil {
			return fmt.Errorf("decoded tokens for %q: %w", assignment.GetNodeId(), err)
		}
	}
	return nil
}

func topologySnapshotMatches(assignments map[string][]uint32, generation int) bool {
	if len(assignments) != topologySnapshotMembers {
		return false
	}
	for member := 0; member < topologySnapshotMembers; member++ {
		if !topologySnapshotTokensMatch(assignments[topologySnapshotNodeID(member)], generation, member) {
			return false
		}
	}
	return true
}

func topologySnapshotTokensMatch(tokens []uint32, generation, member int) bool {
	if len(tokens) != topologySnapshotTokensPerNode {
		return false
	}
	for index, token := range tokens {
		if token != uint32(generation*topologySnapshotTokenStride+member*topologySnapshotTokensPerNode+index) {
			return false
		}
	}
	return true
}

func validateTopologySnapshotTokens(tokens []uint32, member int) error {
	if len(tokens) != topologySnapshotTokensPerNode {
		return fmt.Errorf("got %d tokens, want %d", len(tokens), topologySnapshotTokensPerNode)
	}
	memberOffset := uint32(member * topologySnapshotTokensPerNode)
	if tokens[0] < memberOffset || (tokens[0]-memberOffset)%topologySnapshotTokenStride != 0 {
		return fmt.Errorf("unexpected first token %d", tokens[0])
	}
	for index, token := range tokens {
		if token != tokens[0]+uint32(index) {
			return fmt.Errorf("token %d is %d, want %d", index, token, tokens[0]+uint32(index))
		}
	}
	return nil
}

func topologySnapshotTokens(generation, member int) []uint32 {
	tokens := make([]uint32, topologySnapshotTokensPerNode)
	for index := range tokens {
		tokens[index] = uint32(generation*topologySnapshotTokenStride + member*topologySnapshotTokensPerNode + index)
	}
	return tokens
}

func topologySnapshotNodeID(member int) string {
	return fmt.Sprintf("topology-member-%d", member)
}
