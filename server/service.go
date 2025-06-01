package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	pb "github.com/tigrisdata/cache_service/proto"
	stor "github.com/tigrisdata/cache_service/server/storage"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// cacheService implements pb.CacheServiceServer
type cacheService struct {
	pb.UnimplementedCacheServiceServer
}

// Streaming Put for large values
func (s *cacheService) Put(stream pb.CacheService_PutServer) error {
	zlog.Debug().Msg("gRPC Put called")
	var key string
	var ttl int
	pr, pw := io.Pipe()
	first := true
	errCh := make(chan error, 1)

	// Start storage.Put in a goroutine so it can consume the pipe as we write to it
	go func() {
		errCh <- stor.GetStorage().Put(key, pr, ttl)
	}()

	// Read chunks from the stream and write to the pipe
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			pw.CloseWithError(err)
			<-errCh // wait for storage.Put to finish
			return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
		}
		if first {
			key = chunk.Key
			ttl = int(chunk.TtlSeconds)
			first = false
		}
		if len(chunk.Data) > 0 {
			if _, err := pw.Write(chunk.Data); err != nil {
				pw.CloseWithError(err)
				<-errCh // wait for storage.Put to finish
				return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
			}
		}
	}
	pw.Close()
	err := <-errCh // wait for storage.Put to finish
	if err != nil {
		return stream.SendAndClose(&pb.PutResponse{Success: false, Error: err.Error()})
	}
	return stream.SendAndClose(&pb.PutResponse{Success: true})
}

// PutObject implements the unary REST/HTTP endpoint for cache put
func (s *cacheService) PutObject(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	zlog.Debug().Str("key", req.Key).Int64("ttl", req.TtlSeconds).Int("data_len", len(req.Data)).Msg("PutObject called (unary for REST)")
	if req.Key == "" {
		return &pb.PutResponse{Success: false, Error: "missing key"}, nil
	}
	// Use the same logic as the streaming Put, but for a single chunk
	if err := stor.GetStorage().Put(req.Key, bytes.NewReader(req.Data), int(req.TtlSeconds)); err != nil {
		return &pb.PutResponse{Success: false, Error: err.Error()}, nil
	}
	return &pb.PutResponse{Success: true}, nil
}

// Streaming Get for large values with byte-range support
func (s *cacheService) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	zlog.Debug().Str("key", req.Key).Int64("start", req.Start).Int64("end", req.End).Msg("gRPC Get called")
	key := req.Key
	start := req.Start
	end := req.End

	r, found, err := stor.GetStorage().Get(key)
	if err != nil {
		return err
	}
	if !found {
		return status.Error(codes.NotFound, "key not found")
	}

	// Seek to start if possible
	if start > 0 {
		buf := stor.GetBuffer()
		defer stor.PutBuffer(buf[:0])
		if seeker, ok := r.(io.Seeker); ok {
			_, err := seeker.Seek(start, io.SeekStart)
			if err != nil {
				return err
			}
		} else {
			// If not seekable, read and discard up to start
			toSkip := start
			for toSkip > 0 {
				n := int64(len(buf))
				if n > toSkip {
					n = toSkip
				}
				readN, err := r.Read(buf[:n])
				if readN > 0 {
					toSkip -= int64(readN)
				}
				if err != nil {
					return err
				}
			}
		}
	}

	buf := stor.GetBuffer()
	defer stor.PutBuffer(buf[:0])
	var toRead int64 = -1
	if end > 0 && end > start {
		toRead = end - start
	}
	for {
		readN, err := r.Read(buf)
		if readN > 0 {
			chunk := buf[:readN]
			if toRead >= 0 {
				if int64(readN) > toRead {
					chunk = chunk[:toRead]
					readN = int(toRead)
				}
				toRead -= int64(readN)
			}
			if err := stream.Send(&pb.GetResponse{Data: chunk}); err != nil {
				return err
			}
			if toRead == 0 {
				break
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete implementation
func (s *cacheService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	zlog.Debug().Str("key", req.Key).Msg("gRPC Delete called")
	if req.Key == "" {
		return &pb.DeleteResponse{Success: false, Error: "missing key"}, nil
	}
	stor.GetStorage().DeleteKey(req.Key)
	return &pb.DeleteResponse{Success: true}, nil
}

// Streaming List implementation
func (s *cacheService) List(req *pb.ListRequest, stream pb.CacheService_ListServer) error {
	zlog.Debug().Msg("gRPC List called")
	keys, err := stor.GetStorage().ListKeys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := stream.Send(&pb.ListResponse{Keys: []string{key}}); err != nil {
			return err
		}
	}
	return nil
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

func startGRPCServer() {
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpcLoggingInterceptor),
		grpc.StreamInterceptor(grpcStreamLoggingInterceptor),
	)
	pb.RegisterCacheServiceServer(grpcServer, &cacheService{})

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", GetPort()))
	if err != nil {
		zlog.Fatal().Err(err).Msg("failed to listen for gRPC")
	}
	zlog.Info().Msgf("gRPC server listening on :%d", GetPort())
	if err := grpcServer.Serve(lis); err != nil {
		zlog.Fatal().Err(err).Msg("gRPC server failed")
	}
}

func startGRPCGatewayServer(grpcAddr string, gatewayPort int) {
	ctx := context.Background()
	mux := http.NewServeMux()

	gwMux := runtime.NewServeMux()
	// Register the gRPC service handler with grpc-gateway
	opts := []grpc.DialOption{grpc.WithInsecure()}
	if err := pb.RegisterCacheServiceHandlerFromEndpoint(ctx, gwMux, grpcAddr, opts); err != nil {
		zlog.Fatal().Err(err).Msg("failed to register grpc-gateway handler")
	}
	mux.Handle("/", gwMux)

	zlog.Info().Msgf("Starting grpc-gateway HTTP server on :%d", gatewayPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", gatewayPort), mux); err != nil {
		zlog.Fatal().Err(err).Msg("grpc-gateway server failed")
	}
}
