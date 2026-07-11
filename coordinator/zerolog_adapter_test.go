// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stringer is a test helper that implements fmt.Stringer (like go-kit level.Value).
type stringer string

func (s stringer) String() string { return string(s) }

func TestZerologAdapter_LevelMapping(t *testing.T) {
	tests := []struct {
		name          string
		levelValue    interface{}
		expectedLevel string
	}{
		// fmt.Stringer path (go-kit level.Value)
		{"stringer_error", stringer("error"), "error"},
		{"stringer_warn", stringer("warn"), "warn"},
		{"stringer_info", stringer("info"), "info"},
		{"stringer_debug", stringer("debug"), "trace"},
		{"stringer_unknown", stringer("unknown"), "debug"},

		// Plain string path (dskit memberlist logger fallback)
		{"string_error", "error", "error"},
		{"string_warn", "warn", "warn"},
		{"string_info", "info", "info"},
		{"string_debug", "debug", "trace"},
		{"string_unknown", "unknown", "debug"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			origLogger := zlog.Logger
			origLevel := zerolog.GlobalLevel()
			zlog.Logger = zerolog.New(&buf)
			zerolog.SetGlobalLevel(zerolog.TraceLevel)
			defer func() {
				zlog.Logger = origLogger
				zerolog.SetGlobalLevel(origLevel)
			}()

			adapter := &zerologAdapter{}
			err := adapter.Log("level", tt.levelValue, "msg", "test message")
			require.NoError(t, err)

			output := buf.String()
			assert.Contains(t, output, fmt.Sprintf(`"level":"%s"`, tt.expectedLevel))
			assert.Contains(t, output, `"message":"[dskit]"`)
			assert.Contains(t, output, `"msg":"test message"`)
		})
	}
}

func TestZerologAdapter_NoLevel(t *testing.T) {
	var buf bytes.Buffer
	origLogger := zlog.Logger
	origLevel := zerolog.GlobalLevel()
	zlog.Logger = zerolog.New(&buf)
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	defer func() {
		zlog.Logger = origLogger
		zerolog.SetGlobalLevel(origLevel)
	}()

	adapter := &zerologAdapter{}
	err := adapter.Log("msg", "no level message")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, `"level":"info"`)
}

func TestZerologAdapter_DebugSuppressedAtInfoLevel(t *testing.T) {
	var buf bytes.Buffer
	origLogger := zlog.Logger
	origLevel := zerolog.GlobalLevel()
	zlog.Logger = zerolog.New(&buf)
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	defer func() {
		zlog.Logger = origLogger
		zerolog.SetGlobalLevel(origLevel)
	}()

	adapter := &zerologAdapter{}

	// fmt.Stringer debug should be suppressed
	_ = adapter.Log("level", stringer("debug"), "msg", "should be suppressed")
	assert.Empty(t, buf.String())

	// Plain string debug should also be suppressed
	_ = adapter.Log("level", "debug", "msg", "should also be suppressed")
	assert.Empty(t, buf.String())
}

func TestZerologAdapter_UnrecognizedLevelType(t *testing.T) {
	// When level value is neither fmt.Stringer nor string (e.g., int),
	// the adapter should preserve the default INFO level.
	var buf bytes.Buffer
	origLogger := zlog.Logger
	origLevel := zerolog.GlobalLevel()
	zlog.Logger = zerolog.New(&buf)
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	defer func() {
		zlog.Logger = origLogger
		zerolog.SetGlobalLevel(origLevel)
	}()

	adapter := &zerologAdapter{}
	err := adapter.Log("level", 42, "msg", "unrecognized level type")
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, `"level":"info"`)
	assert.Contains(t, output, `"msg":"unrecognized level type"`)
}

func TestZerologAdapter_KeyValueTypes(t *testing.T) {
	var buf bytes.Buffer
	origLogger := zlog.Logger
	origLevel := zerolog.GlobalLevel()
	zlog.Logger = zerolog.New(&buf)
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	defer func() {
		zlog.Logger = origLogger
		zerolog.SetGlobalLevel(origLevel)
	}()

	adapter := &zerologAdapter{}
	err := adapter.Log(
		"level", stringer("info"),
		"msg", "typed values",
		"count", 42,
		"ratio", 3.14,
		"flag", true,
	)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, `"msg":"typed values"`)
	assert.Contains(t, output, `"count":42`)
	assert.Contains(t, output, `"flag":true`)
}
