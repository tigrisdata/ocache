// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func testClusterListLegacyServerRPC(t *testing.T) {
	binary := buildLegacyListServer(t)
	dir := t.TempDir()
	info := filepath.Join(dir, "address.json")
	logPath := filepath.Join(dir, "legacy.log")
	output, err := os.Create(logPath)
	require.NoError(t, err)
	t.Cleanup(func() { output.Close() })
	cmd := exec.Command(binary, "-test.run=^TestLegacyPaginationServer$", "-test.v", "-test.timeout=300s")
	cmd.Env = append(os.Environ(), "OCACHE_LEGACY_SERVER_INFO="+info)
	cmd.Stdout, cmd.Stderr = output, output
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdin.Close()
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("old server exit: %v", err)
			}
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("old server did not stop")
		}
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("old server output:\n%s", data)
		}
	})
	var addr string
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(info)
		return err == nil && json.Unmarshal(data, &addr) == nil && addr != ""
	}, 90*time.Second, 100*time.Millisecond)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	oldClient := pb.NewCacheServiceClient(conn)
	var failList, failValues atomic.Bool
	var failures atomic.Int32
	factory := listFailureInterceptorFactory("cluster-node-2", &failList, &failValues, &failures)
	f := newClusterListPaginationFixtureWithInterceptor(t, factory)
	for _, key := range []string{"p/a", "p/z", "outside"} {
		putClusterListFixtureValue(t, f.nodeA, key)
	}
	for _, key := range []string{"p/b", "p/c"} {
		putClusterListFixtureValue(t, f.nodeB, key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, partial := range []bool{false, true} {
		for _, withValues := range []bool{false, true} {
			failuresBefore := failures.Load()
			failList.Store(partial && !withValues)
			failValues.Store(partial && withValues)
			var token string
			pages := 2
			if partial {
				pages = 1
			}
			for i := 0; i < pages; i++ {
				expected := []string{"p/a", "p/b"}[i]
				if withValues {
					page, err := f.client.ListWithValues(ctx, &pb.ListRequest{Prefix: "p/", Limit: 1, ContinuationToken: token})
					require.NoError(t, err)
					require.Len(t, page.Entries, 1)
					require.Equal(t, expected, page.Entries[0].Key)
					require.True(t, page.HasMore)
					token = page.ContinuationToken
				} else {
					page, err := f.client.List(ctx, &pb.ListRequest{Prefix: "p/", Limit: 1, ContinuationToken: token})
					require.NoError(t, err)
					require.Equal(t, []string{expected}, page.Keys)
					require.True(t, page.HasMore)
					token = page.ContinuationToken
				}
			}
			require.NotEmpty(t, token)
			expectedFailures := int32(0)
			if partial {
				expectedFailures = 1
			}
			require.Equal(t, failuresBefore+expectedFailures, failures.Load())
			req := &pb.ListRequest{Prefix: "p/", Limit: 1000, ContinuationToken: token}
			var keys []string
			if withValues {
				page, err := oldClient.ListWithValues(ctx, req)
				require.NoError(t, err)
				require.False(t, page.HasMore)
				require.Empty(t, page.ContinuationToken)
				for _, e := range page.Entries {
					keys = append(keys, e.Key)
					require.Equal(t, []byte("value-"+e.Key), e.Value)
				}
			} else {
				page, err := oldClient.List(ctx, req)
				require.NoError(t, err)
				require.False(t, page.HasMore)
				require.Empty(t, page.ContinuationToken)
				keys = page.Keys
			}
			expected := []string{"p/c", "p/z"}
			if partial {
				expected = []string{"p/b", "p/c", "p/z"}
			}
			require.Equal(t, expected, keys, "partial=%v values=%v", partial, withValues)
			t.Logf("old-server RPC partial=%v values=%v: %q, terminal", partial, withValues, keys)
		}
	}
}

// Build the real pre-frontier handlers, not a model of their token decoder.
func buildLegacyListServer(t *testing.T) string {
	t.Helper()
	const revision = "1779e6cd94baa082a15f74df610a676268fa1e10"
	// CI checkouts may be shallow. Fetch only the pinned historical tree;
	// never move the current checkout or resolve a moving branch.
	if err := exec.Command("git", "cat-file", "-e", revision+"^{tree}").Run(); err != nil {
		out, err := exec.Command("git", "fetch", "--no-tags", "https://github.com/tigrisdata/ocache.git", revision).CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	require.NoError(t, err)
	archiveCommand := exec.Command("git", "archive", revision)
	archiveCommand.Dir = strings.TrimSpace(string(root))
	archive, err := archiveCommand.Output()
	require.NoError(t, err)
	source := t.TempDir()
	unpack := exec.Command("tar", "-xf", "-", "-C", source)
	unpack.Stdin = bytes.NewReader(archive)
	out, err := unpack.CombinedOutput()
	require.NoError(t, err, "%s", out)
	worker, err := os.ReadFile("testdata/list_legacy_server_test.go")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source, "tests/integration/list_legacy_server_test.go"), worker, 0600))
	binary := filepath.Join(t.TempDir(), "legacy-list.test")
	build := exec.Command("make", "bench-integration", "INTEGRATION_BENCH_BINARY="+binary)
	build.Dir = source
	// The workflow exports an absolute GOWORK; do not accidentally build the
	// historical worker against this checkout's current modules.
	build.Env = append(os.Environ(), "GOWORK="+filepath.Join(source, "go.work"))
	out, err = build.CombinedOutput()
	require.NoError(t, err, "%s", out)
	return binary
}
