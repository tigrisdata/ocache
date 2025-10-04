package cacheclient

import (
	"context"
	"fmt"
	"io"

	"github.com/tigrisdata/ocache/common/bufferpool"
	pb "github.com/tigrisdata/ocache/proto"
)

// Router is an interface for routing keys to connections
type Router interface {
	Route(key string) (*connection, error)
	RoundRobinRoute() (*connection, error)
}

// Operations provides shared implementation of cache operations
type Operations struct {
	router Router
}

// NewOperations creates a new Operations instance
func NewOperations(router Router) *Operations {
	return &Operations{router: router}
}

// Put stores a value in the cache
func (o *Operations) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	conn, err := o.router.Route(key)
	if err != nil {
		return err
	}

	client := conn.getClient()
	if client == nil {
		return fmt.Errorf("no healthy connections available")
	}

	req := &pb.PutRequest{Key: key, Data: data, TtlSeconds: ttlSeconds}
	_, err = client.PutObject(ctx, req)
	conn.recordError(err)
	return err
}

// PutStream streams data to the cache
func (o *Operations) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	conn, err := o.router.Route(key)
	if err != nil {
		return err
	}

	client := conn.getClient()
	if client == nil {
		return fmt.Errorf("no healthy connections available")
	}

	stream, err := client.Put(ctx)
	if err != nil {
		return err
	}

	// Get buffer from pool
	buf, release := bufferpool.AcquireBuffer(DefaultBufferSize)
	defer release()

	first := true
	for {
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
			if sendErr := stream.Send(req); sendErr != nil {
				return sendErr
			}
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

// Get retrieves a value from the cache
func (o *Operations) Get(ctx context.Context, key string) ([]byte, error) {
	conn, err := o.router.Route(key)
	if err != nil {
		return nil, err
	}

	client := conn.getClient()
	if client == nil {
		return nil, fmt.Errorf("no healthy connections available")
	}

	stream, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}

	// Pre-allocate result slice with initial capacity to reduce allocations
	result := make([]byte, 0, DefaultBufferSize)

	for {
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

// GetStream streams a value from the cache
func (o *Operations) GetStream(ctx context.Context, key string, w io.Writer) error {
	conn, err := o.router.Route(key)
	if err != nil {
		return err
	}

	client := conn.getClient()
	if client == nil {
		return fmt.Errorf("no healthy connections available")
	}

	stream, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return err
	}

	for {
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

// GetRange retrieves a byte range from the cache
func (o *Operations) GetRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	conn, err := o.router.Route(key)
	if err != nil {
		return nil, err
	}

	client := conn.getClient()
	if client == nil {
		return nil, fmt.Errorf("no healthy connections available")
	}

	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}

	stream, err := client.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	result := make([]byte, 0, DefaultBufferSize)
	for {
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

// GetRangeStream streams a byte range from the cache
func (o *Operations) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	conn, err := o.router.Route(key)
	if err != nil {
		return err
	}

	client := conn.getClient()
	if client == nil {
		return fmt.Errorf("no healthy connections available")
	}

	req := &pb.GetRequest{
		Key:   key,
		Start: start,
		End:   end,
	}

	stream, err := client.Get(ctx, req)
	if err != nil {
		return err
	}

	for {
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

// Delete removes a key from the cache
func (o *Operations) Delete(ctx context.Context, key string) error {
	conn, err := o.router.Route(key)
	if err != nil {
		return err
	}

	client := conn.getClient()
	if client == nil {
		return fmt.Errorf("no healthy connections available")
	}

	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: key})
	return err
}

// List lists keys with optional prefix
func (o *Operations) List(ctx context.Context, prefix string) ([]string, error) {
	conn, err := o.router.RoundRobinRoute()
	if err != nil {
		return nil, err
	}

	client := conn.getClient()
	if client == nil {
		return nil, fmt.Errorf("no healthy connections available")
	}

	stream, err := client.List(ctx, &pb.ListRequest{Prefix: prefix})
	if err != nil {
		return nil, err
	}

	var keys []string
	for {
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
