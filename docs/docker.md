# Docker Guide

OCache is available as a multi-platform Docker image supporting both `linux/amd64` and `linux/arm64` architectures.

## Quick Start

### Single Node

```bash
docker run -d \
  --name ocache \
  -p 9000:9000 \
  -p 9001:9001 \
  -v ocache-data:/data \
  tigrisdata/ocache:latest
```

Or using docker-compose:

```bash
docker-compose -f docker/docker-compose.yml up -d
```

### 3-Node Cluster

```bash
docker-compose -f docker/docker-compose-cluster.yml up -d
```

This starts three nodes:

| Node  | gRPC  | HTTP  | Cluster |
|-------|-------|-------|---------|
| node1 | :9001 | :9101 | :7001   |
| node2 | :9002 | :9102 | :7002   |
| node3 | :9003 | :9103 | :7003   |

Connect with the cluster-aware client:

```bash
./ocachecli --addr "localhost:9001,localhost:9002,localhost:9003" put mykey "value"
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OCACHE_TTL` | 0 | Default TTL in seconds (0 = no expiration) |
| `OCACHE_MAX_DISK_USAGE` | 0 | Max disk usage in bytes (0 = unlimited, uses LRU eviction) |
| `OCACHE_REQUEST_LOGGING` | false | Enable request logging |

Example with custom configuration:

```bash
OCACHE_TTL=3600 OCACHE_MAX_DISK_USAGE=10737418240 \
  docker-compose -f docker/docker-compose.yml up -d
```

### Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 9000 | gRPC | Primary API endpoint |
| 9001 | HTTP | REST API (grpc-gateway) |
| 7000 | TCP | Cluster communication (cluster mode only) |

## Testing the Deployment

```bash
# Using the CLI via docker exec
docker exec ocache ocachecli --addr localhost:9000 put test "hello"
docker exec ocache ocachecli --addr localhost:9000 get test

## Health Checks

The container includes a health check that verifies the gRPC server is responding:

```bash
docker inspect --format='{{.State.Health.Status}}' ocache
```

## Image Tags

- `tigrisdata/ocache:latest` - Latest stable release
- `tigrisdata/ocache:vX.Y.Z` - Specific version

## Docker Compose Files

### Single Node (`docker/docker-compose.yml`)

Basic single-node deployment with persistent storage.

### Cluster (`docker/docker-compose-cluster.yml`)

Three-node cluster with internal Docker network for cluster communication.