package ring

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	dskitring "github.com/grafana/dskit/ring"
)

func (c *ringManagerBenchmarkKV) skipNotification() {
	c.mu.Lock()
	c.version++
	c.mu.Unlock()
}

func TestRingManagerDeltaWatcherRecoversSequenceGap(t *testing.T) {
	client := newRingManagerBenchmarkKV(1, 1)
	manager := &RingManager{
		kvClient:       client,
		logger:         log.NewNopLogger(),
		epoch:          NewEpoch(),
		lastKnownNodes: make(map[string]dskitring.InstanceState),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	manager.startRingWatcher(ctx)
	select {
	case <-client.ready:
	case <-time.After(time.Second):
		t.Fatal("delta watcher did not register")
	}

	update := ringManagerBenchmarkUpdate{memberID: "member-0", timestamp: time.Now().Unix() + 2}
	client.publish(update)
	client.skipNotification()
	update.timestamp++
	client.publish(update)

	client.mu.Lock()
	version := client.version
	client.mu.Unlock()
	manager.stateMu.Lock()
	defer manager.stateMu.Unlock()
	if manager.lastDeltaSequence != version {
		t.Fatalf("last delta sequence = %d, want recovered version %d", manager.lastDeltaSequence, version)
	}
	if len(manager.lastKnownNodes) != 1 || manager.lastKnownNodes["member-0"] != dskitring.ACTIVE {
		t.Fatalf("recovered membership state = %#v", manager.lastKnownNodes)
	}
}
