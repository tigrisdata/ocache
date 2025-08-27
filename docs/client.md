# Client Documentation

OCache provides multiple client options for interacting with the cache service:

- **Go Client Library** - A native Go library for programmatic access
- **CLI Client** - Command-line interface for manual operations and scripting
- **gRPC Clients** - Can be generated for any language supported by gRPC

## Go Client Library

The OCache Go client library (`cacheclient`) provides a high-performance, feature-rich interface for interacting with the OCache service via gRPC.

### Installation

```bash
go get github.com/tigrisdata/ocache/client
```

### Basic Usage

```go
package main

import (
    "context"
    "log"

    cacheclient "github.com/tigrisdata/ocache/client"
)

func main() {
    // Create client
    client, err := cacheclient.New("localhost:9000")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Store data
    err = client.Put(ctx, "mykey", []byte("hello world"), 3600)
    if err != nil {
        log.Fatal(err)
    }

    // Retrieve data
    data, err := client.Get(ctx, "mykey")
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Retrieved: %s\n", string(data))

    // Delete data
    err = client.Delete(ctx, "mykey")
    if err != nil {
        log.Fatal(err)
    }

    // List keys
    keys, err := client.List(ctx, "")
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Keys: %v\n", keys)
}
```

### Client Methods

#### New(addr string, opts ...grpc.DialOption) (\*Client, error)

Creates a new client connection to the OCache server.

**Parameters:**

- `addr`: Server address (e.g., "localhost:9000")
- `opts`: Optional gRPC dial options for customizing the connection

**Example:**

```go
// Default connection (insecure)
client, err := cacheclient.New("localhost:9000")

// With custom options
client, err := cacheclient.New(
    "cache.example.com:9000",
    grpc.WithTransportCredentials(creds),
    grpc.WithTimeout(10*time.Second),
)
```

#### Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error

Stores a key-value pair in the cache.

**Parameters:**

- `ctx`: Context for cancellation and timeout
- `key`: Cache key
- `data`: Value to store
- `ttlSeconds`: Time-to-live in seconds (0 for no expiration)

**Example:**

```go
// Store with TTL
err := client.Put(ctx, "session:123", sessionData, 3600)

// Store without TTL
err := client.Put(ctx, "config", configData, 0)
```

#### PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error

Streams data from an io.Reader to the cache service, efficient for large values.

**Parameters:**

- `ctx`: Context for cancellation and timeout
- `key`: Cache key
- `r`: Reader providing the data to store
- `ttlSeconds`: Time-to-live in seconds

**Example:**

```go
// Stream a file
file, err := os.Open("large-file.dat")
if err != nil {
    log.Fatal(err)
}
defer file.Close()

err = client.PutStream(ctx, "large-data", file, 0)

// Stream from HTTP response
resp, err := http.Get("https://example.com/data")
if err != nil {
    log.Fatal(err)
}
defer resp.Body.Close()

err = client.PutStream(ctx, "remote-data", resp.Body, 3600)
```

#### Get(ctx context.Context, key string) ([]byte, error)

Retrieves a value by key.

**Parameters:**

- `ctx`: Context for cancellation and timeout
- `key`: Cache key to retrieve

**Returns:**

- `[]byte`: Retrieved data
- `error`: Error if key not found or other issues

**Example:**

```go
data, err := client.Get(ctx, "mykey")
if err != nil {
    if strings.Contains(err.Error(), "not found") {
        log.Println("Key not found")
    } else {
        log.Fatal(err)
    }
}
```

#### GetStream(ctx context.Context, key string, w io.Writer) error

Streams the value directly to a writer, efficient for large values.

**Parameters:**

- `ctx`: Context for cancellation and timeout
- `key`: Cache key to retrieve
- `w`: Writer to receive the data

**Example:**

```go
// Stream to file
file, err := os.Create("output.dat")
if err != nil {
    log.Fatal(err)
}
defer file.Close()

err = client.GetStream(ctx, "large-data", file)

// Stream to HTTP response
func handler(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Query().Get("key")
    err := client.GetStream(r.Context(), key, w)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
    }
}
```

#### Delete(ctx context.Context, key string) error

Removes a key-value pair from the cache.

**Parameters:**

- `ctx`: Context for cancellation and timeout
- `key`: Cache key to delete

**Example:**

```go
err := client.Delete(ctx, "old-data")
if err != nil {
    log.Printf("Delete failed: %v\n", err)
}
```

#### List(ctx context.Context, prefix string) ([]string, error)

Lists keys in the cache, optionally filtered by prefix.

**Parameters:**

- `ctx`: Context for cancellation and timeout
- `prefix`: Prefix to filter keys (empty string for all keys)

**Returns:**

- `[]string`: List of matching keys
- `error`: Error if operation fails

**Example:**

```go
// List all keys
allKeys, err := client.List(ctx, "")

// List keys with prefix
userKeys, err := client.List(ctx, "user:")
sessionKeys, err := client.List(ctx, "session:")
```

#### Close() error

Closes the client connection. Should be called when done using the client.

**Example:**

```go
client, err := cacheclient.New("localhost:9000")
if err != nil {
    log.Fatal(err)
}
defer client.Close() // Ensure connection is closed
```

### Advanced Usage

#### Error Handling

```go
import (
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

data, err := client.Get(ctx, "mykey")
if err != nil {
    if st, ok := status.FromError(err); ok {
        switch st.Code() {
        case codes.NotFound:
            log.Println("Key not found")
        case codes.DeadlineExceeded:
            log.Println("Operation timed out")
        case codes.Unavailable:
            log.Println("Service unavailable")
        default:
            log.Printf("Operation failed: %v\n", err)
        }
    }
}
```

#### Context with Timeout

```go
// Create context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Operations will fail if they take longer than 5 seconds
err := client.Put(ctx, "key", largeData, 0)
if err != nil {
    if err == context.DeadlineExceeded {
        log.Println("Operation timed out")
    }
}
```

## gRPC Clients for Other Languages

The OCache gRPC service can be accessed from any language that supports gRPC. The protocol buffer definitions are in `proto/cache.proto`.

### Generating Client Code

```bash
# Python
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./python_client \
    --grpc_python_out=./python_client \
    proto/cache.proto

# Java
protoc \
    -I./proto \
    --java_out=./java_client \
    --grpc-java_out=./java_client \
    proto/cache.proto

# Node.js
grpc_tools_node_protoc \
    --js_out=import_style=commonjs,binary:./node_client \
    --grpc_out=./node_client \
    --plugin=protoc-gen-grpc=`which grpc_tools_node_protoc_plugin` \
    -I ./proto \
    proto/cache.proto
```

### Example: Python Client

```python
import grpc
import cache_pb2
import cache_pb2_grpc

# Create channel and stub
channel = grpc.insecure_channel('localhost:9000')
stub = cache_pb2_grpc.CacheServiceStub(channel)

# Put operation
put_request = cache_pb2.PutRequest(
    key='python-key',
    data=b'Hello from Python',
    ttl_seconds=3600
)
stub.PutObject(put_request)

# Get operation
get_request = cache_pb2.GetRequest(key='python-key')
response_iterator = stub.Get(get_request)
data = b''.join([chunk.data for chunk in response_iterator])
print(f"Retrieved: {data.decode()}")

# List operation
list_request = cache_pb2.ListRequest()
list_response_iterator = stub.List(list_request)
keys = []
for chunk in list_response_iterator:
    keys.extend(chunk.keys)
print(f"Keys: {keys}")
```

## See Also

- [HTTP API Documentation](http_api.md) - REST API reference
- [Configuration](configuration.md) - Server configuration options
- [Benchmark Guide](benchmark.md) - Performance testing guide
- [Testing](testing.md) - Testing strategies and tools
