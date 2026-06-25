package coordinator

import (
	"context"
	"runtime/debug"
	"strconv"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tigrisdata/ocache/common/metrics"
)

const (
	// MetadataKeyRingEpoch is the key for the ring epoch in gRPC metadata
	MetadataKeyRingEpoch = "x-ring-epoch"

	// MetadataKeyHop is the key for the hop count in gRPC metadata
	MetadataKeyHop = "x-hop"

	// MetadataKeyForwarded indicates if the request was forwarded
	MetadataKeyForwarded = "x-forwarded"

	// MetadataKeyOrigin is the node that originally received the request
	MetadataKeyOrigin = "x-origin"

	// MetadataKeyOwner is the canonical owner of the key
	MetadataKeyOwner = "x-owner"

	// MetadataKeyForwardedBy is the node that forwarded the request
	MetadataKeyForwardedBy = "x-forwarded-by"

	// MaxHops is the maximum number of hops allowed for a request
	// Prevents infinite forwarding loops when nodes disagree on ownership
	MaxHops = 3
)

// EpochGetter is a function that returns the current ring epoch
type EpochGetter func() uint64

// RequestMetadata holds metadata extracted from incoming requests
type RequestMetadata struct {
	RingEpoch  uint64
	HopCount   int
	Forwarded  bool
	OriginNode string
}

// ResponseMetadata holds metadata to add to outgoing responses
type ResponseMetadata struct {
	RingEpoch   uint64
	OwnerAddr   string
	ForwardedBy string
}

// ExtractRequestMetadata extracts routing metadata from the incoming context
func ExtractRequestMetadata(ctx context.Context) RequestMetadata {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return RequestMetadata{}
	}

	rm := RequestMetadata{}

	// Extract epoch
	if epochs := md.Get(MetadataKeyRingEpoch); len(epochs) > 0 {
		if epoch, err := strconv.ParseUint(epochs[0], 10, 64); err == nil {
			rm.RingEpoch = epoch
		}
	}

	// Extract hop count
	if hops := md.Get(MetadataKeyHop); len(hops) > 0 {
		if hop, err := strconv.Atoi(hops[0]); err == nil {
			rm.HopCount = hop
		}
	}

	// Extract forwarded flag
	if forwarded := md.Get(MetadataKeyForwarded); len(forwarded) > 0 {
		rm.Forwarded = forwarded[0] == "true"
	}

	// Extract origin node
	if origins := md.Get(MetadataKeyOrigin); len(origins) > 0 {
		rm.OriginNode = origins[0]
	}

	return rm
}

// AttachForwardingMetadata attaches forwarding metadata to outgoing context
func AttachForwardingMetadata(ctx context.Context, rm RequestMetadata) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		MetadataKeyRingEpoch, strconv.FormatUint(rm.RingEpoch, 10),
		MetadataKeyHop, strconv.Itoa(rm.HopCount),
		MetadataKeyForwarded, strconv.FormatBool(rm.Forwarded),
		MetadataKeyOrigin, rm.OriginNode,
	)
}

// CheckHopCount validates that the hop count hasn't exceeded MaxHops.
// Returns an error if the limit is exceeded, nil otherwise.
// This is the single source of truth for hop count validation.
func CheckHopCount(hopCount int) error {
	if hopCount > MaxHops {
		return status.Errorf(codes.ResourceExhausted,
			"max hop count exceeded (%d hops, max %d)", hopCount, MaxHops)
	}
	return nil
}

// IncrementHopCount extracts metadata, increments hop count, and returns new context
func IncrementHopCount(ctx context.Context, localNodeID string) (context.Context, error) {
	rm := ExtractRequestMetadata(ctx)

	// Check hop limit using centralized validation
	if err := CheckHopCount(rm.HopCount); err != nil {
		return ctx, err
	}

	// Increment hop count and mark as forwarded
	rm.HopCount++
	rm.Forwarded = true
	if rm.OriginNode == "" {
		rm.OriginNode = localNodeID
	}

	return AttachForwardingMetadata(ctx, rm), nil
}

// SetResponseMetadata sets response metadata headers
func SetResponseMetadata(ctx context.Context, resp ResponseMetadata) error {
	header := metadata.Pairs(
		MetadataKeyRingEpoch, strconv.FormatUint(resp.RingEpoch, 10),
	)
	if resp.OwnerAddr != "" {
		header.Append(MetadataKeyOwner, resp.OwnerAddr)
	}
	if resp.ForwardedBy != "" {
		header.Append(MetadataKeyForwardedBy, resp.ForwardedBy)
	}
	return grpc.SetHeader(ctx, header)
}

// recoverPanic is the shared body of the unary and stream recovery
// interceptors. Called from a deferred closure, it converts a recovered panic
// into a codes.Internal error (assigned through errp) so a single bad request
// fails in isolation instead of unwinding and crashing the process.
func recoverPanic(method string, errp *error) {
	if r := recover(); r != nil {
		metrics.GRPCPanicsRecovered.WithLabelValues(method).Inc()
		log.Error().
			Str("method", method).
			Interface("panic", r).
			Bytes("stack", debug.Stack()).
			Msg("grpc: recovered from panic in handler")
		*errp = status.Errorf(codes.Internal, "internal error")
	}
}

// UnaryServerRecoveryInterceptor returns a gRPC unary interceptor that recovers
// from panics in downstream interceptors and handlers, failing only that single
// RPC with a codes.Internal error instead of letting the panic unwind and crash
// the whole process. gRPC does not recover handler panics by default, so without
// this a single poison request (e.g. a nil-deref on a corrupt key) takes down
// the node. This gives the inter-node gRPC path the same per-request isolation
// that net/http already provides on the gateway path (issue #150).
//
// Install it as the OUTERMOST interceptor so it also covers panics raised by
// inner interceptors.
func UnaryServerRecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer recoverPanic(info.FullMethod, &err)
		return handler(ctx, req)
	}
}

// StreamServerRecoveryInterceptor is the streaming counterpart to
// UnaryServerRecoveryInterceptor. Install it as the outermost stream interceptor.
func StreamServerRecoveryInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer recoverPanic(info.FullMethod, &err)
		return handler(srv, ss)
	}
}

// UnaryServerEpochInterceptor creates a gRPC unary interceptor that:
// 1. Checks hop count and rejects if exceeded
// 2. Adds epoch to response metadata
func UnaryServerEpochInterceptor(epochGetter EpochGetter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Extract and check hop count using centralized validation
		rm := ExtractRequestMetadata(ctx)
		if err := CheckHopCount(rm.HopCount); err != nil {
			return nil, err
		}

		// Call the handler
		resp, err := handler(ctx, req)

		// Add epoch to response (best effort - log failures at debug level)
		if headerErr := SetResponseMetadata(ctx, ResponseMetadata{
			RingEpoch: epochGetter(),
		}); headerErr != nil {
			log.Debug().Err(headerErr).Msg("failed to set ring epoch response header")
		}

		return resp, err
	}
}

// StreamServerEpochInterceptor creates a gRPC stream interceptor that:
// 1. Checks hop count and rejects if exceeded
// 2. Adds epoch to response metadata on first message
func StreamServerEpochInterceptor(epochGetter EpochGetter) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Extract and check hop count using centralized validation
		rm := ExtractRequestMetadata(ss.Context())
		if err := CheckHopCount(rm.HopCount); err != nil {
			return err
		}

		// Wrap the stream to inject epoch on first send
		wrapped := &epochServerStream{
			ServerStream: ss,
			epochGetter:  epochGetter,
			headerSent:   false,
		}

		return handler(srv, wrapped)
	}
}

// epochServerStream wraps a ServerStream to inject epoch metadata
type epochServerStream struct {
	grpc.ServerStream
	epochGetter EpochGetter
	headerSent  bool
}

func (s *epochServerStream) SendMsg(m interface{}) error {
	// Send header with epoch on first message
	if !s.headerSent {
		s.headerSent = true
		if headerErr := SetResponseMetadata(s.Context(), ResponseMetadata{
			RingEpoch: s.epochGetter(),
		}); headerErr != nil {
			log.Debug().Err(headerErr).Msg("failed to set ring epoch response header in stream")
		}
	}
	return s.ServerStream.SendMsg(m)
}

// UnaryClientEpochInterceptor creates a gRPC unary client interceptor that:
// 1. Attaches client epoch to outgoing requests
// 2. Extracts server epoch from responses (for cache invalidation)
func UnaryClientEpochInterceptor(epochGetter EpochGetter, onEpochMismatch func(clientEpoch, serverEpoch uint64)) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Attach client epoch
		clientEpoch := epochGetter()
		ctx = metadata.AppendToOutgoingContext(ctx, MetadataKeyRingEpoch, strconv.FormatUint(clientEpoch, 10))

		// Capture response headers
		var header metadata.MD
		opts = append(opts, grpc.Header(&header))

		// Make the call
		err := invoker(ctx, method, req, reply, cc, opts...)

		// Check for epoch mismatch
		if serverEpochs := header.Get(MetadataKeyRingEpoch); len(serverEpochs) > 0 {
			if serverEpoch, parseErr := strconv.ParseUint(serverEpochs[0], 10, 64); parseErr == nil {
				if serverEpoch != clientEpoch && onEpochMismatch != nil {
					onEpochMismatch(clientEpoch, serverEpoch)
				}
			}
		}

		return err
	}
}

// StreamClientEpochInterceptor creates a gRPC stream client interceptor that:
// 1. Attaches client epoch to outgoing requests
// 2. Extracts server epoch from response headers on first message (for cache invalidation)
func StreamClientEpochInterceptor(epochGetter EpochGetter, onEpochMismatch func(clientEpoch, serverEpoch uint64)) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Attach client epoch
		clientEpoch := epochGetter()
		ctx = metadata.AppendToOutgoingContext(ctx, MetadataKeyRingEpoch, strconv.FormatUint(clientEpoch, 10))

		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		// Wrap the stream to detect epoch mismatches on first receive
		return &epochClientStream{
			ClientStream:    stream,
			clientEpoch:     clientEpoch,
			onEpochMismatch: onEpochMismatch,
			headerChecked:   false,
		}, nil
	}
}

// epochClientStream wraps a ClientStream to detect epoch mismatches
type epochClientStream struct {
	grpc.ClientStream
	clientEpoch     uint64
	onEpochMismatch func(clientEpoch, serverEpoch uint64)
	headerChecked   bool
}

func (s *epochClientStream) RecvMsg(m interface{}) error {
	err := s.ClientStream.RecvMsg(m)

	// Check for epoch mismatch on first receive (after headers are available)
	if !s.headerChecked {
		s.headerChecked = true
		s.checkEpochMismatch()
	}

	return err
}

func (s *epochClientStream) checkEpochMismatch() {
	// Get response headers
	header, err := s.ClientStream.Header()
	if err != nil {
		return
	}

	// Check for epoch mismatch
	if serverEpochs := header.Get(MetadataKeyRingEpoch); len(serverEpochs) > 0 {
		if serverEpoch, parseErr := strconv.ParseUint(serverEpochs[0], 10, 64); parseErr == nil {
			if serverEpoch != s.clientEpoch && s.onEpochMismatch != nil {
				s.onEpochMismatch(s.clientEpoch, serverEpoch)
			}
		}
	}
}
