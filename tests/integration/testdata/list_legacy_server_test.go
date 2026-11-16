// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Compiled in the legacy source tree by the rolling-upgrade integration test.
func TestLegacyPaginationServer(t *testing.T) {
	infoPath := os.Getenv("OCACHE_LEGACY_SERVER_INFO")
	if infoPath == "" {
		t.Skip("requires a parent compatibility test")
	}
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = time.Hour
	config.RecompactionInterval = time.Hour
	h := NewClusterTestHarness(t, 2, config)
	t.Cleanup(h.Cleanup)
	if err := h.StartAllNodes(); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for id := range h.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) != 2 {
		t.Fatalf("got %d nodes", len(ids))
	}
	for i, keys := range [][]string{{"p/a", "p/z", "outside"}, {"p/b", "p/c"}} {
		for _, key := range keys {
			if err := h.Nodes[ids[i]].Storage.Put(key, bytes.NewReader([]byte("value-"+key)), 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	data, err := json.Marshal(h.Nodes[ids[0]].ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(infoPath+".tmp", data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(infoPath+".tmp", infoPath); err != nil {
		t.Fatal(err)
	}
	// Parent closes stdin after completing the RPC observations.
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatal(err)
	}
}
