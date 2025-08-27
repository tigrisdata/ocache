# HTTP API Documentation

OCache provides an HTTP REST interface for cache operations.

## Configuration

The HTTP API runs on port 9001 by default (configurable with `-http-port`).

## Endpoints

### Store an Item

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

### Retrieve an Item

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

### Delete an Item

`DELETE /v1/cache/{key}`

Removes a cache item by key.

**Example:**

```bash
curl -X DELETE "http://localhost:9001/v1/cache/mykey"
```

**Response:**

- `200 OK` on success (even if key didn't exist)
- `500 Internal Server Error` for server errors

### List Items

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

## Error Handling

The HTTP API uses standard status codes:

- `200`: Success
- `400`: Bad request (invalid input)
- `404`: Not found (key doesn't exist)
- `500`: Internal server error

## Performance Considerations

- Small objects (< 64KB by default) are stored in RocksDB for fast access
- Large objects are stored on disk with metadata in RocksDB
- TTL is enforced through background cleanup processes
- The service is optimized for high throughput and low latency

## Examples

### Store and Retrieve Text Data

```bash
# Store text data
curl -X POST "http://localhost:9001/v1/cache/greeting" \
  -H "Content-Type: text/plain" \
  --data "Hello, World!"

# Retrieve the data
curl "http://localhost:9001/v1/cache/greeting"
```

### Store Binary Files

```bash
# Store an image
curl -X POST "http://localhost:9001/v1/cache/logo" \
  --data-binary @logo.png

# Store a PDF document
curl -X POST "http://localhost:9001/v1/cache/report" \
  --data-binary @report.pdf
```

### Store Data with TTL

```bash
# Store with 1 hour TTL
curl -X POST "http://localhost:9001/v1/cache/session" \
  -H "Content-Type: application/json" \
  --data '{
    "key": "session",
    "ttl_seconds": 3600,
    "data": "eyJ1c2VyX2lkIjogMTIzLCAicm9sZSI6ICJhZG1pbiJ9"
  }'
```

## See Also

- [Client Documentation](client.md) - Go client library and CLI client
- [Configuration](configuration.md) - Server configuration options
- [Installation](installation.md) - Installation and setup guide
