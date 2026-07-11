// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package logsample

import (
	"bytes"
	"testing"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

// resetSampler gives a test a fresh burst budget so its assertions don't depend
// on how many lines earlier tests consumed from the process-global sampler.
func resetSampler(t *testing.T) {
	t.Helper()
	orig := degradedRingSampler
	degradedRingSampler = newDegradedRingSampler()
	t.Cleanup(func() { degradedRingSampler = orig })
}

// TestDegradedRing_Samples verifies that a flood of DegradedRing() calls emits
// far fewer lines than calls made -- the whole point of the sampler is to keep a
// single-node outage from producing millions of identical WARN lines (#164).
func TestDegradedRing_Samples(t *testing.T) {
	resetSampler(t)

	var buf bytes.Buffer
	orig := zlog.Logger
	zlog.Logger = zerolog.New(&buf)
	t.Cleanup(func() { zlog.Logger = orig })

	const calls = 100000
	for range calls {
		DegradedRing().Str("key", "k").Msg("Failed to route key")
	}

	lines := bytes.Count(buf.Bytes(), []byte("\n"))
	if lines == 0 {
		t.Fatal("expected some sampled lines, got none")
	}
	if lines >= calls {
		t.Fatalf("expected sampling to drop most lines, got %d of %d", lines, calls)
	}
	// Well under 1% should survive (burst + 1-in-1000).
	if lines > calls/100 {
		t.Fatalf("sampler let through too many lines: %d of %d", lines, calls)
	}
}

// TestDegradedRing_EmitsFields confirms the returned event still carries fields
// and level, so switching a call site from zlog.Warn() is a drop-in change. With
// a fresh sampler the first burst lines pass unconditionally, so a single call
// is guaranteed to emit.
func TestDegradedRing_EmitsFields(t *testing.T) {
	resetSampler(t)

	var buf bytes.Buffer
	orig := zlog.Logger
	zlog.Logger = zerolog.New(&buf)
	t.Cleanup(func() { zlog.Logger = orig })

	DegradedRing().Str("node_id", "n1").Msg("Circuit breaker open for node")

	if buf.Len() == 0 {
		t.Fatal("expected at least one sampled line, got none")
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"node_id":"n1"`)) {
		t.Fatalf("expected node_id field in output, got %q", buf.String())
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"level":"warn"`)) {
		t.Fatalf("expected warn level in output, got %q", buf.String())
	}
}
