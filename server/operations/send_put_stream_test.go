// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
)

// recordingPutClient captures what SendPutStream puts on the wire.
type recordingPutClient struct {
	grpc.ClientStream
	sent []*pb.PutRequest
	resp *pb.PutResponse
}

func (r *recordingPutClient) Send(req *pb.PutRequest) error {
	// Copy the fields, not the message (proto messages carry a mutex).
	r.sent = append(r.sent, &pb.PutRequest{
		Key:            req.Key,
		TtlSeconds:     req.TtlSeconds,
		ShedOverBudget: req.ShedOverBudget,
		Data:           append([]byte(nil), req.Data...),
	})
	return nil
}
func (r *recordingPutClient) CloseAndRecv() (*pb.PutResponse, error) { return r.resp, nil }

// A sheddable first message carries the key only, so the owner can refuse on
// it before any body byte is sent; a plain first message carries the first chunk.
func TestSendPutStream_SheddableFirstMessageHasNoBody(t *testing.T) {
	ctx := context.Background()
	body := make([]byte, 10)

	shed := &recordingPutClient{resp: &pb.PutResponse{Success: true}}
	_, err := SendPutStream(ctx, shed, &pb.PutRequest{Key: "k", ShedOverBudget: true}, bytes.NewReader(body))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(shed.sent), 2)
	require.Empty(t, shed.sent[0].Data, "sheddable first message must carry no body")
	require.True(t, shed.sent[0].ShedOverBudget)
	require.Len(t, shed.sent[1].Data, 10)

	plain := &recordingPutClient{resp: &pb.PutResponse{Success: true}}
	_, err = SendPutStream(ctx, plain, &pb.PutRequest{Key: "k"}, bytes.NewReader(body))
	require.NoError(t, err)
	require.Len(t, plain.sent[0].Data, 10, "plain first message carries the first chunk")
}

// neverReadReader fails the test if the sender reads it.
type neverReadReader struct{ t *testing.T }

func (n neverReadReader) Read([]byte) (int, error) {
	n.t.Fatal("body must not be read after the context is canceled")
	return 0, io.EOF
}

// A canceled context stops the sender before the next body read.
func TestSendPutStream_StopsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &recordingPutClient{resp: &pb.PutResponse{Success: true}}
	_, err := SendPutStream(ctx, c, &pb.PutRequest{Key: "k"}, neverReadReader{t})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, c.sent)
}
