# API Documentation

OCache provides both HTTP REST and gRPC interfaces for cache operations.

## HTTP API

The HTTP API runs on port 9001 by default (configurable with `-http-port`).

### Endpoints

#### Store an Item

`POST /v1/cache/{key}`

Stores a cache item with the specified key.

**Request Body Options:**

1. JSON format with base64-encoded data:
```json
{
  "key": "mykey",
  "ttl_seconds": 3600,
  "data": "aGVsbG8gd29ybGQ="
}
```

2. Raw binary data:
```bash
curl -X POST "http://localhost:9001/v1/cache/mykey" \
  --data-binary @myfile.bin
```

**Response:**
- `200 OK` on success
- `400 Bad Request` for invalid input
- `500 Internal Server Error` for server errors

#### Retrieve an Item

`GET /v1/cache/{key}`

Retrieves a cache item by key.

**Example:**
```bash
curl "http://localhost:9001/v1/cache/mykey"
```

**Response:**
- `200 OK` with item data
- `404 Not Found` if key doesn't exist
- `500 Internal Server Error` for server errors

#### Delete an Item

`DELETE /v1/cache/{key}`

Removes a cache item by key.

**Example:**
```bash
curl -X DELETE "http://localhost:9001/v1/cache/mykey"
```

**Response:**
- `200 OK` on success (even if key didn't exist)
- `500 Internal Server Error` for server errors

#### List Items

`GET /v1/cache`

Lists all cache keys.

**Example:**
```bash
curl "http://localhost:9001/v1/cache"
```

**Response:**
```json
{
  "keys": ["key1", "key2", "key3"]
}
```

## gRPC API

The gRPC API runs on port 9000 by default (configurable with `-port`).

### Service Definition

The gRPC service is defined in `proto/cache.proto`:

```protobuf
service CacheService {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc List(ListRequest) returns (ListResponse);
}
```

### Message Types

#### PutRequest
- `key` (string): Cache key
- `value` (bytes): Value to store
- `ttl_seconds` (int32): Time-to-live in seconds

#### GetRequest
- `key` (string): Cache key to retrieve

#### GetResponse
- `value` (bytes): Retrieved value
- `found` (bool): Whether the key was found

#### DeleteRequest
- `key` (string): Cache key to delete

#### ListRequest
- No fields (returns all keys)

#### ListResponse
- `keys` (repeated string): List of all cache keys

## Error Handling

Both APIs use standard error codes:

### HTTP Status Codes
- `200`: Success
- `400`: Bad request (invalid input)
- `404`: Not found (key doesn't exist)
- `500`: Internal server error

### gRPC Status Codes
- `OK`: Success
- `NOT_FOUND`: Key not found
- `INVALID_ARGUMENT`: Invalid input
- `INTERNAL`: Server error

## Performance Considerations

- Small objects (< 64KB by default) are stored in RocksDB for fast access
- Large objects are stored on disk with metadata in RocksDB
- TTL is enforced through background cleanup processes
- The service is optimized for high throughput and low latency