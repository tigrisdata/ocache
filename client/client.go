package cacheclient

import (
	"context"
	"fmt"
	"io"

	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.CacheServiceClient
}

func New(addr string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithInsecure())
	}
	// Set max message sizes for streaming
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(128*1024*1024), // 128MB
		grpc.MaxCallSendMsgSize(128*1024*1024), // 128MB
	))
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		client: pb.NewCacheServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	req := &pb.PutRequest{Key: key, Data: data, TtlSeconds: ttlSeconds}
	_, err := c.client.PutObject(ctx, req)
	return err
}

// PutStream streams data from an io.Reader to the cache service, efficient for large values.
func (c *Client) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	stream, err := c.client.Put(ctx)
	if err != nil {
		return err
	}

	buf := make([]byte, 64*1024) // 64KB chunks
	first := true
	totalBytes := 0

	for {
		// Check for context cancellation before reading
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := r.Read(buf)
		if n > 0 {
			req := &pb.PutRequest{Data: buf[:n]}
			if first {
				req.Key = key
				req.TtlSeconds = ttlSeconds
				first = false
			}

			// Check context before sending
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if sendErr := stream.Send(req); sendErr != nil {
				return sendErr
			}
			totalBytes += n
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp != nil && !resp.Success {
		return fmt.Errorf("put failed: %s", resp.Error)
	}
	return nil
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	req := &pb.GetRequest{Key: key}
	stream, err := c.client.Get(ctx, req)
	if err != nil {
		return nil, err
	}
	var result []byte
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, resp.Data...)
	}
	return result, nil
}

// GetStream streams the value directly to the provided writer, efficient for large values.
func (c *Client) GetStream(ctx context.Context, key string, w io.Writer) error {
	req := &pb.GetRequest{Key: key}
	stream, err := c.client.Get(ctx, req)
	if err != nil {
		return err
	}
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := w.Write(resp.Data); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.client.Delete(ctx, &pb.DeleteRequest{Key: key})
	return err
}

func (c *Client) List(ctx context.Context, prefix string) ([]string, error) {
	stream, err := c.client.List(ctx, &pb.ListRequest{Prefix: prefix})
	if err != nil {
		return nil, err
	}
	var keys []string
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		keys = append(keys, resp.Keys...)
	}
	return keys, nil
}
