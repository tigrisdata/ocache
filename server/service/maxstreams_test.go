package service

import "testing"

// TestEffectiveMaxConcurrentStreams verifies the shared default-application used
// by both the standalone server and the embedded client: 0 (unset) maps to the
// default, any explicit value is preserved.
func TestEffectiveMaxConcurrentStreams(t *testing.T) {
	if got := EffectiveMaxConcurrentStreams(0); got != DefaultMaxConcurrentStreams {
		t.Fatalf("unset (0) should map to DefaultMaxConcurrentStreams (%d), got %d", DefaultMaxConcurrentStreams, got)
	}
	// In-range values (including exactly the ceiling) are preserved.
	for _, v := range []uint32{1, 128, 512, 4096, MaxAllowedConcurrentStreams} {
		if got := EffectiveMaxConcurrentStreams(v); got != v {
			t.Fatalf("in-range %d should be preserved, got %d", v, got)
		}
	}
	// Above the ceiling clamps down to MaxAllowedConcurrentStreams.
	for _, v := range []uint32{MaxAllowedConcurrentStreams + 1, 1 << 20} {
		if got := EffectiveMaxConcurrentStreams(v); got != MaxAllowedConcurrentStreams {
			t.Fatalf("%d should clamp to MaxAllowedConcurrentStreams (%d), got %d", v, MaxAllowedConcurrentStreams, got)
		}
	}
}
