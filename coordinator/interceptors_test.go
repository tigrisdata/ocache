package coordinator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestExtractRequestMetadata(t *testing.T) {
	tests := []struct {
		name     string
		md       metadata.MD
		expected RequestMetadata
	}{
		{
			name:     "empty context",
			md:       nil,
			expected: RequestMetadata{},
		},
		{
			name: "full metadata",
			md: metadata.Pairs(
				MetadataKeyRingEpoch, "42",
				MetadataKeyHop, "2",
				MetadataKeyForwarded, "true",
				MetadataKeyOrigin, "node1",
			),
			expected: RequestMetadata{
				RingEpoch:  42,
				HopCount:   2,
				Forwarded:  true,
				OriginNode: "node1",
			},
		},
		{
			name: "partial metadata - only epoch",
			md: metadata.Pairs(
				MetadataKeyRingEpoch, "100",
			),
			expected: RequestMetadata{
				RingEpoch: 100,
			},
		},
		{
			name: "invalid epoch is ignored",
			md: metadata.Pairs(
				MetadataKeyRingEpoch, "not-a-number",
			),
			expected: RequestMetadata{},
		},
		{
			name: "invalid hop count is ignored",
			md: metadata.Pairs(
				MetadataKeyHop, "not-a-number",
			),
			expected: RequestMetadata{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ctx context.Context
			if tt.md != nil {
				ctx = metadata.NewIncomingContext(context.Background(), tt.md)
			} else {
				ctx = context.Background()
			}

			rm := ExtractRequestMetadata(ctx)

			assert.Equal(t, tt.expected.RingEpoch, rm.RingEpoch)
			assert.Equal(t, tt.expected.HopCount, rm.HopCount)
			assert.Equal(t, tt.expected.Forwarded, rm.Forwarded)
			assert.Equal(t, tt.expected.OriginNode, rm.OriginNode)
		})
	}
}

func TestAttachForwardingMetadata(t *testing.T) {
	rm := RequestMetadata{
		RingEpoch:  42,
		HopCount:   1,
		Forwarded:  true,
		OriginNode: "node1",
	}

	ctx := AttachForwardingMetadata(context.Background(), rm)

	// Extract outgoing metadata
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	assert.Equal(t, []string{"42"}, md.Get(MetadataKeyRingEpoch))
	assert.Equal(t, []string{"1"}, md.Get(MetadataKeyHop))
	assert.Equal(t, []string{"true"}, md.Get(MetadataKeyForwarded))
	assert.Equal(t, []string{"node1"}, md.Get(MetadataKeyOrigin))
}

func TestCheckHopCount(t *testing.T) {
	tests := []struct {
		name      string
		hopCount  int
		expectErr bool
	}{
		{
			name:      "max hops (3) is allowed",
			hopCount:  3,
			expectErr: false,
		},
		{
			name:      "more than max hops is exceeded",
			hopCount:  5,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckHopCount(tt.hopCount)

			if tt.expectErr {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, codes.ResourceExhausted, st.Code())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIncrementHopCount(t *testing.T) {
	tests := []struct {
		name            string
		initialHop      int
		initialOrigin   string
		localNodeID     string
		expectErr       bool
		expectedHop     int
		expectedOrigin  string
		expectedForward bool
	}{
		{
			name:            "first hop sets origin",
			initialHop:      0,
			initialOrigin:   "",
			localNodeID:     "node1",
			expectErr:       false,
			expectedHop:     1,
			expectedOrigin:  "node1",
			expectedForward: true,
		},
		{
			name:            "subsequent hop preserves origin",
			initialHop:      1,
			initialOrigin:   "node1",
			localNodeID:     "node2",
			expectErr:       false,
			expectedHop:     2,
			expectedOrigin:  "node1",
			expectedForward: true,
		},
		{
			name:          "max hops exceeded",
			initialHop:    4,
			initialOrigin: "node1",
			localNodeID:   "node4",
			expectErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up incoming context with initial metadata
			md := metadata.Pairs(
				MetadataKeyHop, intToString(tt.initialHop),
				MetadataKeyOrigin, tt.initialOrigin,
			)
			ctx := metadata.NewIncomingContext(context.Background(), md)

			// Call IncrementHopCount
			newCtx, err := IncrementHopCount(ctx, tt.localNodeID)

			if tt.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Extract outgoing metadata
			outMD, ok := metadata.FromOutgoingContext(newCtx)
			require.True(t, ok)

			assert.Equal(t, intToString(tt.expectedHop), outMD.Get(MetadataKeyHop)[0])
			assert.Equal(t, tt.expectedOrigin, outMD.Get(MetadataKeyOrigin)[0])
			assert.Equal(t, "true", outMD.Get(MetadataKeyForwarded)[0])
		})
	}
}

func TestUnaryServerEpochInterceptor(t *testing.T) {
	// Create interceptor with a mock epoch getter
	var currentEpoch uint64 = 42
	epochGetter := func() uint64 { return currentEpoch }

	interceptor := UnaryServerEpochInterceptor(epochGetter)
	require.NotNil(t, interceptor)

	// Test with hop count above the limit (should reject)
	md := metadata.Pairs(MetadataKeyHop, "4")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := interceptor(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		t.Fatal("handler should not be called when hop limit exceeded")
		return nil, nil
	})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
}

func intToString(i int) string {
	return string(rune('0' + i))
}
