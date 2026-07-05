package operations

import (
	"bytes"
	"context"
	"io"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/logsample"
	"github.com/tigrisdata/ocache/coordinator"
	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Get retrieves data for the given key with automatic routing.
// If the key is local, it accesses storage directly.
// If the key is remote, it fetches via gRPC from the appropriate node.
// The start and end parameters specify byte range (0, 0 for full content).
// Returns (reader, found, error).
func (o *Operations) Get(ctx context.Context, key string, start, end int64) (io.Reader, bool, error) {
	done := recordOperationStart("Operations.Get")

	zlog.Debug().Str("key", key).Int64("start", start).Int64("end", end).Msg("Operations.Get called")

	if o.IsLocal(key) {
		r, found, err := o.GetLocal(ctx, key, start, end)
		done(err)
		return r, found, err
	}

	// Remote key - fetch via gRPC
	reader, found, err := o.getRemote(ctx, key, start, end)
	done(err)
	return reader, found, err
}

// GetLocal retrieves data from local storage directly.
// This is used by CacheService for streaming and by embedded clients for local access.
func (o *Operations) GetLocal(ctx context.Context, key string, start, end int64) (io.Reader, bool, error) {
	var r io.Reader
	var found bool

	err := retry.DoWithKey(ctx, retry.DefaultConfig(), "Get", key, func() error {
		var getErr error
		r, found, getErr = o.storage.Get(key, start, end)
		return getErr
	})

	if err != nil {
		return nil, false, err
	}

	return r, found, nil
}

// getRemote fetches data from a remote node via gRPC.
// Returns (reader, found, error) to be consistent with GetLocal.
//
// The returned reader streams chunks from the peer on demand rather than
// buffering the whole object in memory, so a cross-node read of a large object
// holds at most one chunk instead of the entire object (issue #162). The first
// message is received eagerly so found/not-found can be resolved synchronously
// (it is part of the return signature) without draining the rest of the stream.
// Callers that stop reading early should Close the reader to tear down the
// stream; GetBytes/GetStream/GetRange(Stream) already do.
func (o *Operations) getRemote(ctx context.Context, key string, start, end int64) (io.Reader, bool, error) {
	// Increment hop count for forwarding loop detection
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Hop count limit exceeded for get")
		return nil, false, err
	}

	client, err := o.Route(key)
	if err != nil {
		logsample.DegradedRing().Err(err).Str("key", key).Msg("Failed to route key")
		return nil, false, err
	}

	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}

	// Cancellable context so the returned reader can tear down the stream on
	// Close (e.g. a partial/ranged read that never drains to EOF) rather than
	// leaking it until the caller's context expires.
	streamCtx, cancel := context.WithCancel(ctx)

	stream, err := client.Get(streamCtx, req)
	if err != nil {
		cancel()
		// Check for NotFound status - return (nil, false, nil) for consistency
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nil, false, nil
		}
		return nil, false, err
	}

	// Receive the first message eagerly to resolve found/not-found before
	// returning, without buffering the rest of the object.
	first, err := stream.Recv()
	if err == io.EOF {
		// Object exists but is empty.
		cancel()
		return bytes.NewReader(nil), true, nil
	}
	if err != nil {
		cancel()
		// NotFound surfaces on the first Recv - treat as a miss for consistency.
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nil, false, nil
		}
		return nil, false, err
	}

	r := &grpcStreamReader{stream: stream, cancel: cancel}
	if len(first.Data) > 0 {
		recordBytesTransferred("download", int64(len(first.Data)))
		r.pending = first.Data
	}
	return r, true, nil
}

// grpcStreamReader adapts a server-streaming CacheService_GetClient into an
// io.Reader that pulls chunks from the peer on demand, so a cross-node read
// never holds more than one chunk of the object in memory (issue #162).
type grpcStreamReader struct {
	stream  pb.CacheService_GetClient
	cancel  context.CancelFunc
	pending []byte // bytes received but not yet consumed by Read
	done    bool
	err     error
}

// Read drains any pending bytes before pulling the next message from the stream.
func (r *grpcStreamReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.done {
			if r.err != nil {
				return 0, r.err
			}
			return 0, io.EOF
		}
		resp, err := r.stream.Recv()
		if err != nil {
			r.done = true
			if err != io.EOF {
				r.err = err
			}
			continue
		}
		if len(resp.Data) > 0 {
			recordBytesTransferred("download", int64(len(resp.Data)))
			r.pending = resp.Data
		}
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

// Close tears down the underlying stream. Safe to call multiple times and after
// the stream has been fully consumed.
func (r *grpcStreamReader) Close() error {
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil // mark teardown done; makes repeat Close a clear no-op
	}
	return nil
}

// GetBytes retrieves data as a byte slice with automatic routing.
// This is a convenience method that reads all data into memory.
func (o *Operations) GetBytes(ctx context.Context, key string) ([]byte, bool, error) {
	reader, found, err := o.Get(ctx, key, 0, 0)
	if err != nil || !found {
		return nil, found, err
	}

	// Ensure reader is closed even if ReadAll fails
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, false, err
	}

	return data, true, nil
}

// GetRange retrieves a byte range as a byte slice with automatic routing.
func (o *Operations) GetRange(ctx context.Context, key string, start, end int64) ([]byte, bool, error) {
	reader, found, err := o.Get(ctx, key, start, end)
	if err != nil || !found {
		return nil, found, err
	}

	// Ensure reader is closed even if ReadAll fails
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, false, err
	}

	return data, true, nil
}

// GetStream retrieves data and writes it to the provided writer with automatic routing.
func (o *Operations) GetStream(ctx context.Context, key string, w io.Writer) (bool, error) {
	reader, found, err := o.Get(ctx, key, 0, 0)
	if err != nil || !found {
		return found, err
	}

	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	_, err = io.Copy(w, reader)
	return true, err
}

// GetRangeStream retrieves a byte range and writes it to the provided writer.
func (o *Operations) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) (bool, error) {
	reader, found, err := o.Get(ctx, key, start, end)
	if err != nil || !found {
		return found, err
	}

	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	_, err = io.Copy(w, reader)
	return true, err
}
