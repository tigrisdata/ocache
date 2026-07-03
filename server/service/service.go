package service

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/coordinator"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/operations"
	stor "github.com/tigrisdata/ocache/storage"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// CacheService implements pb.CacheServiceServer
type CacheService struct {
	pb.UnimplementedCacheServiceServer
	coordinator *coordinator.Coordinator
	storage     *stor.Storage
	ops         *operations.Operations
}

// NewCacheService creates a new cache service, optionally with clustering support
func NewCacheService(coord *coordinator.Coordinator, storage *stor.Storage) *CacheService {
	return &CacheService{
		coordinator: coord,
		storage:     storage,
		ops:         operations.New(storage, coord),
	}
}

// Operations returns the underlying operations layer for embedded use.
// This allows embedded clients to access the routing logic directly.
func (s *CacheService) Operations() *operations.Operations {
	return s.ops
}

// GetTopology returns the current cluster topology (for cluster-aware clients)
func (s *CacheService) GetTopology(ctx context.Context, req *pb.GetTopologyRequest) (*pb.GetTopologyResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("GetTopology").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Msg("gRPC GetTopology called")

	// If coordinator is not enabled (single node mode), return an error
	if s.coordinator == nil {
		metrics.RPCRequests.WithLabelValues("GetTopology", "not_clustered").Inc()
		return &pb.GetTopologyResponse{
			Error: "cluster mode not enabled",
		}, nil
	}

	// Get topology from coordinator
	topology, err := s.coordinator.GetClusterTopology(ctx, &clusterpb.Empty{})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("GetTopology", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "GetTopology").Inc()
		return &pb.GetTopologyResponse{
			Error: err.Error(),
		}, nil
	}

	// Return the topology directly since we're now using the same type
	metrics.RPCRequests.WithLabelValues("GetTopology", "success").Inc()
	return &pb.GetTopologyResponse{
		Topology: topology,
	}, nil
}

// logGRPCRequest is a helper for logging gRPC requests (unary and streaming)
func logGRPCRequest(method, remote string, duration time.Duration, err error, extra ...func(*zerolog.Event)) {
	logEvent := zlog.Info().
		Str("grpc_method", method).
		Str("remote", remote).
		Dur("duration_ms", duration).
		Bool("error", err != nil)
	for _, fn := range extra {
		fn(logEvent)
	}
	logEvent.Msg("gRPC request completed")
}

// grpcLoggingInterceptor logs each gRPC unary request using zerolog
func grpcLoggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	p, _ := peer.FromContext(ctx)
	resp, err := handler(ctx, req)
	duration := time.Since(start)
	remote := ""
	if p != nil {
		remote = p.Addr.String()
	}
	logGRPCRequest(info.FullMethod, remote, duration, err)
	return resp, err
}

// grpcStreamLoggingInterceptor logs each gRPC streaming request using zerolog
func grpcStreamLoggingInterceptor(
	service interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()
	p, _ := peer.FromContext(ss.Context())
	err := handler(service, ss)
	duration := time.Since(start)
	remote := ""
	if p != nil {
		remote = p.Addr.String()
	}
	logGRPCRequest(
		info.FullMethod,
		remote,
		duration,
		err,
		func(e *zerolog.Event) {
			e.Bool("is_client_stream", info.IsClientStream)
			e.Bool("is_server_stream", info.IsServerStream)
		},
	)
	return err
}

// DefaultMaxConcurrentStreams bounds the number of concurrent HTTP/2 streams a
// single client connection may open on the gRPC server. Since inter-node
// peer-forwards reuse one pooled connection per peer, this caps how many
// concurrent forwards any one peer can drive at a node — protecting a hot
// key-owner during a degraded ring from unbounded inbound fan-out.
const DefaultMaxConcurrentStreams uint32 = 256

// EffectiveMaxConcurrentStreams returns v, or DefaultMaxConcurrentStreams when v
// is 0 (unset). Shared by the standalone server and the embedded client so both
// apply the same default.
func EffectiveMaxConcurrentStreams(v uint32) uint32 {
	if v == 0 {
		return DefaultMaxConcurrentStreams
	}
	return v
}

func StartGRPCServer(coord *coordinator.Coordinator, storage *stor.Storage, listenAddr string, requestLogging bool, maxConcurrentStreams uint32) {
	maxConcurrentStreams = EffectiveMaxConcurrentStreams(maxConcurrentStreams)
	var opts []grpc.ServerOption

	// Build interceptor chains. Recovery is outermost so a panic in any handler
	// fails just that RPC instead of crashing the process (issue #150).
	unaryInterceptors := []grpc.UnaryServerInterceptor{coordinator.UnaryServerRecoveryInterceptor()}
	streamInterceptors := []grpc.StreamServerInterceptor{coordinator.StreamServerRecoveryInterceptor()}

	// Add logging interceptors if enabled
	if requestLogging {
		unaryInterceptors = append(unaryInterceptors, grpcLoggingInterceptor)
		streamInterceptors = append(streamInterceptors, grpcStreamLoggingInterceptor)
	}

	// Add epoch interceptors if cluster mode is enabled
	if coord != nil {
		unaryInterceptors = append(unaryInterceptors, coordinator.UnaryServerEpochInterceptor(coord.GetEpoch))
		streamInterceptors = append(streamInterceptors, coordinator.StreamServerEpochInterceptor(coord.GetEpoch))
	}

	// Chain interceptors if we have any
	if len(unaryInterceptors) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	}
	if len(streamInterceptors) > 0 {
		opts = append(opts, grpc.ChainStreamInterceptor(streamInterceptors...))
	}

	opts = append(opts,
		grpc.MaxRecvMsgSize(128*1024*1024), // 128MB - match client send limit
		grpc.MaxSendMsgSize(128*1024*1024), // 128MB - match client recv limit
		grpc.MaxConcurrentStreams(maxConcurrentStreams),
	)

	grpcServer := grpc.NewServer(opts...)
	// Create service with coordinator if clustering is enabled
	service := NewCacheService(coord, storage)
	pb.RegisterCacheServiceServer(grpcServer, service)

	// Register ClusterService on the same gRPC server if clustering is enabled
	// This allows clients to query cluster topology on the same port as cache operations
	if coord != nil {
		clusterpb.RegisterClusterServiceServer(grpcServer, coord)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		zlog.Fatal().Err(err).Msg("failed to listen for gRPC")
	}
	zlog.Info().Msgf("gRPC server listening on %s", listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		zlog.Fatal().Err(err).Msg("gRPC server failed")
	}
}

// healthHandler returns a simple HTTP handler for liveness checks.
// Returns 200 OK if the process is alive.
func healthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}
}

// readyHandler returns an HTTP handler for readiness checks.
// In cluster mode, checks if the coordinator is in ACTIVE state.
func readyHandler(coord *coordinator.Coordinator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// In cluster mode, check if coordinator is ready
		if coord != nil && !coord.IsReady() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"not_ready","reason":"coordinator not active"}`))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	}
}

func StartGRPCGatewayServer(coord *coordinator.Coordinator, grpcAddr string, listenHTTP string) {
	ctx := context.Background()
	mux := http.NewServeMux()

	gwMux := runtime.NewServeMux()
	// Register the gRPC service handler with grpc-gateway
	opts := []grpc.DialOption{grpc.WithInsecure()}
	if err := pb.RegisterCacheServiceHandlerFromEndpoint(ctx, gwMux, grpcAddr, opts); err != nil {
		zlog.Fatal().Err(err).Msg("failed to register grpc-gateway handler")
	}

	// Health check endpoints for Docker/Kubernetes
	mux.HandleFunc("/health", healthHandler())
	mux.HandleFunc("/healthz", healthHandler()) // Kubernetes convention
	mux.HandleFunc("/ready", readyHandler(coord))
	mux.HandleFunc("/readyz", readyHandler(coord)) // Kubernetes convention
	zlog.Info().Msg("Health endpoints available at /health, /healthz, /ready, /readyz")

	// Add Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())
	zlog.Info().Msg("Prometheus metrics available at /metrics")

	// Handle all other routes with the gRPC gateway
	mux.Handle("/", gwMux)

	zlog.Info().Msgf("Starting grpc-gateway HTTP server on %s", listenHTTP)
	if err := http.ListenAndServe(listenHTTP, mux); err != nil {
		zlog.Fatal().Err(err).Msg("grpc-gateway server failed")
	}
}

// mapStorageErrorToGRPC maps storage errors to appropriate gRPC status codes
func mapStorageErrorToGRPC(err error) error {
	if err == nil {
		return nil
	}

	errType, ok := storageErrors.GetType(err)
	if !ok {
		// Not a storage error, return as-is
		return err
	}

	switch errType {
	case storageErrors.TypeNotFound:
		return status.Error(codes.NotFound, "resource not found")
	case storageErrors.TypeInvalidRequest:
		return status.Error(codes.InvalidArgument, err.Error())
	case storageErrors.TypeStorageFull:
		return status.Error(codes.ResourceExhausted, "storage capacity exceeded")
	case storageErrors.TypeCorruption:
		return status.Error(codes.DataLoss, "data corruption detected")
	case storageErrors.TypeTemporary:
		return status.Error(codes.Unavailable, "service temporarily unavailable")
	case storageErrors.TypeIO:
		// Check if it's retryable
		if storageErrors.IsRetryable(err) {
			return status.Error(codes.Unavailable, "temporary I/O error")
		}
		return status.Error(codes.Internal, "storage I/O error")
	case storageErrors.TypeLock:
		return status.Error(codes.Aborted, "resource temporarily locked")
	case storageErrors.TypeTimeout:
		return status.Error(codes.DeadlineExceeded, "operation timed out")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
