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

// A refusal relayed from a remote owner keeps its marker and code in the unary
// response, and is not counted as an error.
func TestPutFailure_RemoteShedKeepsMarker(t *testing.T) {
	resp := putFailure("PutObject", &operations.PutError{Message: "storage capacity exceeded", Code: codes.ResourceExhausted, Shed: true})
	require.False(t, resp.Success)
	require.True(t, resp.Shed)
	require.Equal(t, int32(codes.ResourceExhausted), resp.Code)

	full := putFailure("PutObject", &operations.PutError{Message: "storage capacity exceeded", Code: codes.ResourceExhausted})
	require.False(t, full.Shed, "a real storage-full relayed from a peer stays an error")
}

// waitingPutServer is the client side of a relay whose caller sent the key and
// is waiting for an answer before sending more: Recv blocks until released.
type waitingPutServer struct {
	grpc.ServerStream
	ctx     context.Context
	release chan struct{}
	resp    *pb.PutResponse
	done    chan struct{}
}

func (w *waitingPutServer) Context() context.Context { return w.ctx }
func (w *waitingPutServer) Recv() (*pb.PutRequest, error) {
	<-w.release
	return nil, io.EOF
}
func (w *waitingPutServer) SendAndClose(r *pb.PutResponse) error {
	w.resp = r
	close(w.done)
	return nil
}

// The relay must deliver an owner's early refusal even though the client has
// sent nothing after the first message.
func TestRelayPut_EarlyRefusalWithoutMoreChunks(t *testing.T) {
	owner := setupShedStorage(t)
	ownerClient := startBufconnService(t, NewCacheService(nil, owner))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fill, err := ownerClient.Put(ctx)
	require.NoError(t, err)
	resp, err := operations.SendPutStream(fill, &pb.PutRequest{Key: "fill"}, bytes.NewReader(make([]byte, 8192)))
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.True(t, owner.OverShedRatio())

	remote, err := ownerClient.Put(ctx)
	require.NoError(t, err)
	local := &waitingPutServer{ctx: ctx, release: make(chan struct{}), done: make(chan struct{})}
	defer close(local.release)

	go func() { _ = relayPut(local, remote, &pb.PutRequest{Key: "shed", ShedOverBudget: true}) }()

	select {
	case <-local.done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not deliver the owner's refusal while the client was waiting")
	}
	require.False(t, local.resp.Success)
	require.True(t, local.resp.Shed, "resp=%+v", local.resp)
	require.Equal(t, int32(codes.ResourceExhausted), local.resp.Code)
}
