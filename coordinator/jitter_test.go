// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"testing"
	"time"
)

func TestJittered_WithinEqualJitterBounds(t *testing.T) {
	d := 100 * time.Millisecond
	for i := 0; i < 10000; i++ {
		got := jittered(d)
		if got < d/2 || got > d {
			t.Fatalf("jittered(%v) = %v, want within [%v, %v]", d, got, d/2, d)
		}
	}
}

func TestJittered_NonPositiveReturnedUnchanged(t *testing.T) {
	for _, d := range []time.Duration{0, -1, -5 * time.Second} {
		if got := jittered(d); got != d {
			t.Fatalf("jittered(%v) = %v, want %v", d, got, d)
		}
	}
}

func TestJittered_Varies(t *testing.T) {
	// Equal jitter should not collapse to a single value; over many samples we
	// expect more than one distinct result (this is what breaks lockstep retry).
	d := time.Second
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 100; i++ {
		seen[jittered(d)] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("jittered produced %d distinct values over 100 samples, want >= 2", len(seen))
	}
}
