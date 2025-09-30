package service

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/tigrisdata/ocache/common/bufferpool"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/coordinator"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/ocache/storage/retry"

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
}

// NewCacheService creates a new cache service, optionally with clustering support
func NewCacheService(coord *coordinator.Coordinator, storage *stor.Storage) *CacheService {
	return &CacheService{
		coordinator: coord,
		storage:     storage,
	}
}

// Streaming Put for large values
func (s *CacheService) Put(stream pb.CacheService_PutServer) error {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("Put").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Msg("gRPC Put called")
	metrics.StreamsActive.Inc()
	defer metrics.StreamsActive.Dec()

	// If clustering is enabled, handle routing
	if s.coordinator != nil {
		return s.handleClusteredPut(stream)
	}

	// Read the first chunk to get key and ttl
	firstChunk, err := stream.Recv()
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "Put").Inc()
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
	}

	key := firstChunk.Key
	ttl := int(firstChunk.TtlSeconds)

	if key == "" {
		metrics.RPCRequests.WithLabelValues("Put", "invalid").Inc()
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: "missing key"})
	}

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	// Start storage.Put in a goroutine so it can consume the pipe as we write to it
	go func() {
		// Note: We don't retry streaming Put operations at service layer since
		// the client would need to resend the entire stream
		errCh <- s.storage.Put(key, pr, ttl)
	}()

	// Write the first chunk's data if any
	if len(firstChunk.Data) > 0 {
		if _, err := pw.Write(firstChunk.Data); err != nil {
			pw.CloseWithError(err)
			<-errCh // wait for storage.Put to finish
			metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
			metrics.Errors.WithLabelValues("grpc", "Put").Inc()
			return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
		}
		metrics.StreamBytesTransferred.WithLabelValues("upload").Add(float64(len(firstChunk.Data)))
	}

	// Read remaining chunks from the stream and write to the pipe
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			pw.CloseWithError(err)
			<-errCh // wait for storage.Put to finish

			metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
			metrics.Errors.WithLabelValues("grpc", "Put").Inc()
			return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
		}
		if len(chunk.Data) > 0 {
			if _, err := pw.Write(chunk.Data); err != nil {
				pw.CloseWithError(err)
				<-errCh // wait for storage.Put to finish

				metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
				metrics.Errors.WithLabelValues("grpc", "Put").Inc()
				return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
			}
			metrics.StreamBytesTransferred.WithLabelValues("upload").Add(float64(len(chunk.Data)))
		}
	}
	pw.Close()

	err = <-errCh // wait for storage.Put to finish
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Put", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "Put").Inc()
		// Convert storage error to user-friendly message
		userErr := mapStorageErrorToGRPC(err)
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: userErr.Error()})
	}

	zlog.Debug().Str("key", key).Msg("Streaming put completed successfully")
	metrics.RPCRequests.WithLabelValues("Put", "success").Inc()
	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

// PutObject implements the unary REST/HTTP endpoint for cache put
func (s *CacheService) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("PutObject").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Str("key", req.Key).Int64("ttl", req.TtlSeconds).Int("data_len", len(req.Data)).Msg("PutObject called (unary for REST)")

	// If clustering is enabled, handle routing
	if s.coordinator != nil {
		return s.handleClusteredPutObject(ctx, req)
	}

	if req.Key == "" {
		metrics.RPCRequests.WithLabelValues("PutObject", "invalid").Inc()
		userErr := status.Error(codes.InvalidArgument, "missing key")
		return &pb.PutResponse{Success: false, Error: userErr.Error()}, nil
	}
	// Use the same logic as the streaming Put, but for a single chunk
	// Wrap with retry logic for retryable errors
	err := retry.DoWithKey(ctx, retry.DefaultConfig(), "PutObject", req.Key, func() error {
		return s.storage.Put(req.Key, bytes.NewReader(req.Data), int(req.TtlSeconds))
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("PutObject", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "PutObject").Inc()
		// Convert storage error to user-friendly message
		userErr := mapStorageErrorToGRPC(err)
		return &pb.PutResponse{Success: false, Error: userErr.Error()}, nil
	}
	metrics.RPCRequests.WithLabelValues("PutObject", "success").Inc()
	return &pb.PutResponse{Success: true}, nil
}

// Streaming Get for large values with byte-range support
func (s *CacheService) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	startTime := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("Get").Observe(float64(time.Since(startTime).Milliseconds()))
	}()

	zlog.Debug().Str("key", req.Key).Int64("start", req.Start).Int64("end", req.End).Msg("gRPC Get called")
	metrics.StreamsActive.Inc()
	defer metrics.StreamsActive.Dec()

	// If clustering is enabled, handle routing
	if s.coordinator != nil {
		return s.handleClusteredGet(req, stream)
	}

	// Wrap Get with retry logic for retryable errors
	var r io.Reader
	var found bool
	err := retry.DoWithKey(stream.Context(), retry.DefaultConfig(), "Get", req.Key, func() error {
		var getErr error
		r, found, getErr = s.storage.Get(req.Key, req.Start, req.End)
		return getErr
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Get", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "Get").Inc()
		return mapStorageErrorToGRPC(err)
	}
	if !found {
		metrics.RPCRequests.WithLabelValues("Get", "not_found").Inc()
		return status.Error(codes.NotFound, "key not found")
	}

	// Ensure the reader is closed to release any file locks
	if closer, ok := r.(io.Closer); ok {
		defer closer.Close()
	}

	// Stream the data in chunks
	buf, release := bufferpool.AcquireBuffer(1 << 20) // 1 MiB
	defer release()
	for {
		readN, err := r.Read(buf)
		if readN > 0 {
			if err := stream.Send(&pb.GetResponse{Data: buf[:readN]}); err != nil {
				metrics.RPCRequests.WithLabelValues("Get", "error").Inc()
				metrics.Errors.WithLabelValues("grpc", "Get").Inc()
				return err
			}
			metrics.StreamBytesTransferred.WithLabelValues("download").Add(float64(readN))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			metrics.RPCRequests.WithLabelValues("Get", "error").Inc()
			metrics.Errors.WithLabelValues("grpc", "Get").Inc()
			return err
		}
	}
	metrics.RPCRequests.WithLabelValues("Get", "success").Inc()
	return nil
}

// Delete implementation
func (s *CacheService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("Delete").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Str("key", req.Key).Msg("gRPC Delete called")

	// If clustering is enabled, handle routing
	if s.coordinator != nil {
		return s.handleClusteredDelete(ctx, req)
	}

	if req.Key == "" {
		metrics.RPCRequests.WithLabelValues("Delete", "invalid").Inc()
		userErr := status.Error(codes.InvalidArgument, "missing key")
		return &pb.DeleteResponse{Success: false, Error: userErr.Error()}, nil
	}

	err := retry.DoWithKey(ctx, retry.DefaultConfig(), "Delete", req.Key, func() error {
		return s.storage.DeleteKey(req.Key)
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("Delete", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "Delete").Inc()
		userErr := mapStorageErrorToGRPC(err)
		return &pb.DeleteResponse{Success: false, Error: userErr.Error()}, nil
	}
	metrics.RPCRequests.WithLabelValues("Delete", "success").Inc()
	return &pb.DeleteResponse{Success: true}, nil
}

// Streaming List implementation
func (s *CacheService) List(req *pb.ListRequest, stream pb.CacheService_ListServer) error {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("List").Observe(float64(time.Since(start).Milliseconds()))
	}()

	zlog.Debug().Str("prefix", req.Prefix).Msg("gRPC List called")
	// Wrap ListKeys with retry logic for retryable errors
	var keys []string
	err := retry.Do(stream.Context(), retry.DefaultConfig(), "ListKeys", func() error {
		var listErr error
		keys, listErr = s.storage.ListKeys(req.Prefix)
		return listErr
	})
	if err != nil {
		metrics.RPCRequests.WithLabelValues("List", "error").Inc()
		metrics.Errors.WithLabelValues("grpc", "List").Inc()
		return mapStorageErrorToGRPC(err)
	}
	for _, key := range keys {
		if err := stream.Send(&pb.ListResponse{Keys: []string{key}}); err != nil {
			metrics.RPCRequests.WithLabelValues("List", "error").Inc()
			metrics.Errors.WithLabelValues("grpc", "List").Inc()
			return err
		}
	}
	metrics.RPCRequests.WithLabelValues("List", "success").Inc()
	return nil
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

func StartGRPCServer(coord *coordinator.Coordinator, storage *stor.Storage, listenAddr string, requestLogging bool) {
	// If request logging is enabled, add the interceptors to the gRPC server
	var opts []grpc.ServerOption
	if requestLogging {
		opts = append(opts,
			grpc.UnaryInterceptor(grpcLoggingInterceptor),
			grpc.StreamInterceptor(grpcStreamLoggingInterceptor),
		)
	}

	opts = append(opts,
		grpc.MaxRecvMsgSize(128*1024*1024), // 128MB - match client send limit
		grpc.MaxSendMsgSize(128*1024*1024), // 128MB - match client recv limit
	)

	grpcServer := grpc.NewServer(opts...)
	// Create service with coordinator if clustering is enabled
	service := NewCacheService(coord, storage)
	pb.RegisterCacheServiceServer(grpcServer, service)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		zlog.Fatal().Err(err).Msg("failed to listen for gRPC")
	}
	zlog.Info().Msgf("gRPC server listening on %s", listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		zlog.Fatal().Err(err).Msg("gRPC server failed")
	}
}

func StartGRPCGatewayServer(grpcAddr string, listenHTTP string) {
	ctx := context.Background()
	mux := http.NewServeMux()

	gwMux := runtime.NewServeMux()
	// Register the gRPC service handler with grpc-gateway
	opts := []grpc.DialOption{grpc.WithInsecure()}
	if err := pb.RegisterCacheServiceHandlerFromEndpoint(ctx, gwMux, grpcAddr, opts); err != nil {
		zlog.Fatal().Err(err).Msg("failed to register grpc-gateway handler")
	}

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
