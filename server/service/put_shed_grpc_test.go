// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !ocache_topology_benchmark

package service

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/server/operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// startBufconnService serves svc over an in-memory gRPC connection.
func startBufconnService(t *testing.T, svc *CacheService) pb.CacheServiceClient {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterCacheServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return pb.NewCacheServiceClient(conn)
}

// Over a real gRPC stream the owner's early refusal reaches the caller as the
// in-band shed response, not as the io.EOF the next Send returns.
func TestCacheService_Put_ShedOverGRPC(t *testing.T) {
	s := setupShedStorage(t)
	client := startBufconnService(t, NewCacheService(nil, s))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Fill past the cap with a plain streamed put.
	stream, err := client.Put(ctx)
	require.NoError(t, err)
	resp, err := operations.SendPutStream(stream, &pb.PutRequest{Key: "fill"}, bytes.NewReader(make([]byte, 8192)))
	require.NoError(t, err)
	require.True(t, resp.Success, "plain put must land: %+v", resp)
	require.True(t, s.OverShedRatio())

	// A multi-chunk sheddable body: the owner answers on the first message and
	// later Sends fail with EOF; the response must still come back marked shed.
	stream, err = client.Put(ctx)
	require.NoError(t, err)
	resp, err = operations.SendPutStream(stream, &pb.PutRequest{Key: "shed", ShedOverBudget: true}, bytes.NewReader(make([]byte, 4<<20)))
	require.NoError(t, err)
	require.False(t, resp.Success)
	require.True(t, resp.Shed, "resp=%+v", resp)
	require.Equal(t, int32(codes.ResourceExhausted), resp.Code)

	// Key first, then wait: the pre-check answers without a body.
	stream, err = client.Put(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.PutRequest{Key: "keyonly", ShedOverBudget: true}))
	resp, err = stream.CloseAndRecv()
	require.NoError(t, err)
	require.True(t, resp.Shed, "resp=%+v", resp)

	// Nothing from the refused writes landed; the plain key did.
	for _, key := range []string{"shed", "keyonly"} {
		getStream, err := client.Get(ctx, &pb.GetRequest{Key: key})
		require.NoError(t, err)
		_, err = getStream.Recv()
		require.Equal(t, codes.NotFound, codesOf(err), "key %s must not exist", key)
	}
}

func codesOf(err error) codes.Code {
	if err == nil || err == io.EOF {
		return codes.OK
	}
	return status.Code(err)
}
