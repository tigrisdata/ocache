package service

import "testing"

// TestEffectiveMaxConcurrentStreams verifies the shared default-application used
// by both the standalone server and the embedded client: 0 (unset) maps to the
// default, any explicit value is preserved.
func TestEffectiveMaxConcurrentStreams(t *testing.T) {
	if got := EffectiveMaxConcurrentStreams(0); got != DefaultMaxConcurrentStreams {
		t.Fatalf("unset (0) should map to DefaultMaxConcurrentStreams (%d), got %d", DefaultMaxConcurrentStreams, got)
	}
	for _, v := range []uint32{1, 128, 512, 4096} {
		if got := EffectiveMaxConcurrentStreams(v); got != v {
			t.Fatalf("explicit %d should be preserved, got %d", v, got)
		}
	}
}
