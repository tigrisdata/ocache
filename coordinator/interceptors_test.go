package coordinator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

// mockServerStream implements grpc.ServerStream for testing
type mockServerStream struct {
	ctx         context.Context
	sentMsgs    []interface{}
	headersSent metadata.MD
}

func (m *mockServerStream) SetHeader(md metadata.MD) error {
	m.headersSent = metadata.Join(m.headersSent, md)
	return nil
}

func (m *mockServerStream) SendHeader(md metadata.MD) error {
	m.headersSent = metadata.Join(m.headersSent, md)
	return nil
}

func (m *mockServerStream) SetTrailer(md metadata.MD) {}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

func (m *mockServerStream) SendMsg(msg interface{}) error {
	m.sentMsgs = append(m.sentMsgs, msg)
	return nil
}

func (m *mockServerStream) RecvMsg(msg interface{}) error {
	return nil
}

// mockClientStream implements grpc.ClientStream for testing
type mockClientStream struct {
	ctx         context.Context
	header      metadata.MD
	recvMsgs    []interface{}
	recvIndex   int
	recvErr     error
	headerCalls int
}

func (m *mockClientStream) Header() (metadata.MD, error) {
	m.headerCalls++
	return m.header, nil
}

func (m *mockClientStream) Trailer() metadata.MD {
	return nil
}

func (m *mockClientStream) CloseSend() error {
	return nil
}

func (m *mockClientStream) Context() context.Context {
	return m.ctx
}

func (m *mockClientStream) SendMsg(msg interface{}) error {
	return nil
}

func (m *mockClientStream) RecvMsg(msg interface{}) error {
	if m.recvErr != nil {
		return m.recvErr
	}
	if m.recvIndex < len(m.recvMsgs) {
		m.recvIndex++
	}
	return nil
}

func TestStreamServerEpochInterceptor(t *testing.T) {
	tests := []struct {
		name       string
		hopCount   int
		expectErr  bool
		expectCode codes.Code
	}{
		{
			name:      "valid hop count allows stream",
			hopCount:  0,
			expectErr: false,
		},
		{
			name:      "max hop count (3) is allowed",
			hopCount:  3,
			expectErr: false,
		},
		{
			name:       "hop count exceeded rejects stream",
			hopCount:   4,
			expectErr:  true,
			expectCode: codes.ResourceExhausted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var currentEpoch uint64 = 42
			epochGetter := func() uint64 { return currentEpoch }

			interceptor := StreamServerEpochInterceptor(epochGetter)
			require.NotNil(t, interceptor)

			// Set up context with hop count
			md := metadata.Pairs(MetadataKeyHop, intToString(tt.hopCount))
			ctx := metadata.NewIncomingContext(context.Background(), md)

			mockStream := &mockServerStream{ctx: ctx}
			handlerCalled := false
			wrappedStreamReceived := false

			err := interceptor(nil, mockStream, nil, func(srv interface{}, ss grpc.ServerStream) error {
				handlerCalled = true
				// Verify the stream is wrapped with epochServerStream
				_, isWrapped := ss.(*epochServerStream)
				wrappedStreamReceived = isWrapped
				// Simulate sending a message (header setting may fail in mock context, that's OK)
				_ = ss.SendMsg("test message")
				return nil
			})

			if tt.expectErr {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, tt.expectCode, st.Code())
				assert.False(t, handlerCalled, "handler should not be called when hop limit exceeded")
			} else {
				require.NoError(t, err)
				assert.True(t, handlerCalled, "handler should be called")
				assert.True(t, wrappedStreamReceived, "stream should be wrapped with epochServerStream")
				// Verify the message was forwarded to the underlying stream
				assert.Equal(t, 1, len(mockStream.sentMsgs))
			}
		})
	}
}

func TestEpochServerStream_SendMsg(t *testing.T) {
	tests := []struct {
		name      string
		sendCount int
	}{
		{
			name:      "first send sets headerSent flag",
			sendCount: 1,
		},
		{
			name:      "subsequent sends maintain headerSent flag",
			sendCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var currentEpoch uint64 = 99
			epochGetter := func() uint64 { return currentEpoch }

			ctx := metadata.NewIncomingContext(context.Background(), nil)
			mockStream := &mockServerStream{ctx: ctx}

			wrapped := &epochServerStream{
				ServerStream: mockStream,
				epochGetter:  epochGetter,
				headerSent:   false,
			}

			// Verify headerSent starts false
			assert.False(t, wrapped.headerSent)

			// Send multiple messages
			for i := 0; i < tt.sendCount; i++ {
				err := wrapped.SendMsg("message")
				require.NoError(t, err)
			}

			// Verify messages were forwarded to underlying stream
			assert.Equal(t, tt.sendCount, len(mockStream.sentMsgs))

			// Verify headerSent flag is set to true after first send
			// Note: grpc.SetHeader requires a real gRPC context, so we can't verify
			// the actual header in unit tests. The headerSent flag confirms the logic was executed.
			assert.True(t, wrapped.headerSent, "headerSent should be true after sending")
		})
	}
}

func TestStreamClientEpochInterceptor(t *testing.T) {
	tests := []struct {
		name          string
		clientEpoch   uint64
		serverEpoch   string
		expectWrapped bool
	}{
		{
			name:          "creates wrapped stream",
			clientEpoch:   42,
			serverEpoch:   "42",
			expectWrapped: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			epochGetter := func() uint64 { return tt.clientEpoch }
			mismatchCalled := false
			onMismatch := func(client, server uint64) {
				mismatchCalled = true
			}

			interceptor := StreamClientEpochInterceptor(epochGetter, onMismatch)
			require.NotNil(t, interceptor)

			// Create a mock streamer that returns our mock stream
			mockStream := &mockClientStream{
				ctx: context.Background(),
				header: metadata.Pairs(
					MetadataKeyRingEpoch, tt.serverEpoch,
				),
			}
			streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				// Verify epoch was attached to context
				md, ok := metadata.FromOutgoingContext(ctx)
				require.True(t, ok)
				epochs := md.Get(MetadataKeyRingEpoch)
				require.Len(t, epochs, 1)
				assert.Equal(t, "42", epochs[0])
				return mockStream, nil
			}

			// Create outgoing context
			ctx := context.Background()

			stream, err := interceptor(ctx, nil, nil, "test.Method", streamer)
			require.NoError(t, err)
			require.NotNil(t, stream)

			// Verify the stream is wrapped
			_, ok := stream.(*epochClientStream)
			assert.True(t, ok, "stream should be wrapped with epochClientStream")

			// Mismatch callback should not be called until RecvMsg
			assert.False(t, mismatchCalled)
		})
	}
}

func TestEpochClientStream_RecvMsg(t *testing.T) {
	tests := []struct {
		name           string
		clientEpoch    uint64
		serverEpoch    string
		recvCount      int
		expectMismatch bool
		expectCallback int // number of times callback should be called
	}{
		{
			name:           "detects mismatch on first recv",
			clientEpoch:    42,
			serverEpoch:    "100",
			recvCount:      1,
			expectMismatch: true,
			expectCallback: 1,
		},
		{
			name:           "no callback when epochs match",
			clientEpoch:    42,
			serverEpoch:    "42",
			recvCount:      1,
			expectMismatch: false,
			expectCallback: 0,
		},
		{
			name:           "callback only on first recv",
			clientEpoch:    42,
			serverEpoch:    "100",
			recvCount:      3,
			expectMismatch: true,
			expectCallback: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callbackCount := 0
			var capturedClient, capturedServer uint64
			onMismatch := func(client, server uint64) {
				callbackCount++
				capturedClient = client
				capturedServer = server
			}

			mockStream := &mockClientStream{
				ctx: context.Background(),
				header: metadata.Pairs(
					MetadataKeyRingEpoch, tt.serverEpoch,
				),
				recvMsgs: make([]interface{}, tt.recvCount),
			}

			wrapped := &epochClientStream{
				ClientStream:    mockStream,
				clientEpoch:     tt.clientEpoch,
				onEpochMismatch: onMismatch,
				headerChecked:   false,
			}

			// Receive multiple messages
			for i := 0; i < tt.recvCount; i++ {
				err := wrapped.RecvMsg(nil)
				require.NoError(t, err)
			}

			// Verify callback count
			assert.Equal(t, tt.expectCallback, callbackCount, "callback should be called correct number of times")

			if tt.expectMismatch {
				assert.Equal(t, tt.clientEpoch, capturedClient)
				assert.Equal(t, uint64(100), capturedServer)
			}

			// Verify headerChecked is set
			assert.True(t, wrapped.headerChecked)
		})
	}
}

func TestEpochClientStream_CheckEpochMismatch(t *testing.T) {
	tests := []struct {
		name           string
		clientEpoch    uint64
		serverEpoch    string
		noHeader       bool
		invalidEpoch   bool
		expectCallback bool
	}{
		{
			name:           "calls callback on mismatch",
			clientEpoch:    42,
			serverEpoch:    "100",
			expectCallback: true,
		},
		{
			name:           "no callback when epochs match",
			clientEpoch:    42,
			serverEpoch:    "42",
			expectCallback: false,
		},
		{
			name:           "handles missing header gracefully",
			clientEpoch:    42,
			noHeader:       true,
			expectCallback: false,
		},
		{
			name:           "handles invalid epoch gracefully",
			clientEpoch:    42,
			serverEpoch:    "not-a-number",
			invalidEpoch:   true,
			expectCallback: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callbackCalled := false
			onMismatch := func(client, server uint64) {
				callbackCalled = true
			}

			var header metadata.MD
			if !tt.noHeader {
				header = metadata.Pairs(MetadataKeyRingEpoch, tt.serverEpoch)
			}

			mockStream := &mockClientStream{
				ctx:    context.Background(),
				header: header,
			}

			wrapped := &epochClientStream{
				ClientStream:    mockStream,
				clientEpoch:     tt.clientEpoch,
				onEpochMismatch: onMismatch,
				headerChecked:   false,
			}

			// Call checkEpochMismatch directly
			wrapped.checkEpochMismatch()

			assert.Equal(t, tt.expectCallback, callbackCalled, "callback should be called correctly")
		})
	}
}
