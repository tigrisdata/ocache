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
// Returns all keys matching the prefix (automatically handles pagination)
func (o *Operations) List(ctx context.Context, prefix string) ([]string, error) {
	conn, err := o.router.RoundRobinRoute()
	if err != nil {
		return nil, err
	}

	client := conn.getClient()
	if client == nil {
		return nil, fmt.Errorf("no healthy connections available")
	}

	var allKeys []string
	continuationToken := ""
	pageLimit := int32(MaxPageLimit)

	// Paginate through all results
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		req := &pb.ListRequest{
			Prefix:            prefix,
			Limit:             pageLimit,
			ContinuationToken: continuationToken,
		}

		resp, err := client.List(ctx, req)
		if err != nil {
			return nil, err
		}

		allKeys = append(allKeys, resp.Keys...)

		// Check if there are more pages
		if !resp.HasMore || resp.ContinuationToken == "" {
			break
		}

		continuationToken = resp.ContinuationToken
	}

	return allKeys, nil
}

// ListPage returns a single page of keys with pagination support
// Returns: (keys, continuationToken, hasMore, error)
func (o *Operations) ListPage(ctx context.Context, prefix string, limit int, continuationToken string) ([]string, string, bool, error) {
	conn, err := o.router.RoundRobinRoute()
	if err != nil {
		return nil, "", false, err
	}

	client := conn.getClient()
	if client == nil {
		return nil, "", false, fmt.Errorf("no healthy connections available")
	}

	if limit <= 0 || limit > MaxPageLimit {
		limit = MaxPageLimit
	}

	req := &pb.ListRequest{
		Prefix:            prefix,
		Limit:             int32(limit),
		ContinuationToken: continuationToken,
	}

	resp, err := client.List(ctx, req)
	if err != nil {
		return nil, "", false, err
	}

	return resp.Keys, resp.ContinuationToken, resp.HasMore, nil
}

// ListPageWithValues returns a single page of key-value pairs with pagination support.
// Returns: (entries, continuationToken, hasMore, error)
func (o *Operations) ListPageWithValues(ctx context.Context, prefix string, limit int, continuationToken string) ([]KeyValue, string, bool, error) {
	conn, err := o.router.RoundRobinRoute()
	if err != nil {
		return nil, "", false, err
	}

	client := conn.getClient()
	if client == nil {
		return nil, "", false, fmt.Errorf("no healthy connections available")
	}

	if limit <= 0 || limit > MaxPageLimit {
		limit = MaxPageLimit
	}

	req := &pb.ListRequest{
		Prefix:            prefix,
		Limit:             int32(limit),
		ContinuationToken: continuationToken,
	}

	resp, err := client.ListWithValues(ctx, req)
	if err != nil {
		return nil, "", false, err
	}

	// Convert proto entries to client KeyValue
	entries := make([]KeyValue, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = KeyValue{
			Key:          e.Key,
			Value:        e.Value,
			ValueLength:  e.ValueLength,
			ValueOmitted: e.ValueOmitted,
		}
	}

	return entries, resp.ContinuationToken, resp.HasMore, nil
}
