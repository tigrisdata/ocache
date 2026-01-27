package operations

import (
	"bytes"
	"context"
	"io"

	zlog "github.com/rs/zerolog/log"
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
func (o *Operations) getRemote(ctx context.Context, key string, start, end int64) (io.Reader, bool, error) {
	// Increment hop count for forwarding loop detection
	ctx, err := coordinator.IncrementHopCount(ctx, o.GetLocalNodeID())
	if err != nil {
		zlog.Warn().Err(err).Str("key", key).Msg("Hop count limit exceeded for get")
		return nil, false, err
	}

	client, err := o.Route(key)
	if err != nil {
		zlog.Warn().Err(err).Str("key", key).Msg("Failed to route key")
		return nil, false, err
	}

	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}

	stream, err := client.Get(ctx, req)
	if err != nil {
		// Check for NotFound status - return (nil, false, nil) for consistency
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nil, false, nil
		}
		return nil, false, err
	}

	// Collect all data from the stream into a buffer
	// Note: For very large objects, we might want to implement a streaming reader
	// that reads from the gRPC stream on demand. For now, we buffer everything.
	var buf bytes.Buffer
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Check for NotFound status during streaming
			if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
				return nil, false, nil
			}
			return nil, false, err
		}
		if len(resp.Data) > 0 {
			buf.Write(resp.Data)
			recordBytesTransferred("download", int64(len(resp.Data)))
		}
	}

	return &buf, true, nil
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
