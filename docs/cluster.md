# Cluster Mode Documentation

OCache supports distributed caching through cluster mode, enabling horizontal scaling across multiple nodes using consistent hashing for data distribution.

## Overview

Cluster mode provides:

- **Horizontal Scaling**: Distribute cache across multiple nodes
- **Smart Routing**: Automatic request routing to the correct node
- **Gossip-Based Membership**: Nodes discover each other via gossip protocol using [memberlist](https://github.com/hashicorp/memberlist)
- **Consistent Hashing**: Even data distribution with minimal reshuffling using [dskit](https://github.com/grafana/dskit) ring
- **Token Persistence**: Stable key ownership across node restarts
- **Connection Pooling**: Efficient resource utilization with circuit breakers

## Architecture

### Components

1. **Coordinator**: Manages cluster membership, ring state, and request routing
2. **Ring Manager**: dskit-based consistent hash ring with lifecycle management
3. **Memberlist KV**: Gossip-based key-value store for ring state propagation
4. **Router**: Request forwarding with connection pooling and circuit breakers
5. **Cluster Client**: Client-side routing with topology caching and epoch tracking

### Data Distribution

- Uses Grafana dskit ring for consistent hashing with token-based ownership
- Hash function: FNV-1a (32-bit) for token computation
- Default tokens per node: 512 (provides good distribution across the ring)
- Replication factor: 1 (no replication by default, configurable)

## Setting Up a Cluster

### Prerequisites

- Multiple OCache server instances
- Network connectivity between nodes
- Unique node IDs for each instance

### Basic 3-Node Cluster Setup

Start three nodes with cluster mode enabled:

**Node 1:**

```bash
./ocache \
  -cluster-enabled \
  -node-id node1 \
  -cluster-addr :7000 \
  -seeds "node2:7000,node3:7000" \
  -listen-addr :9000 \
  -listen-http :9001 \
  -disk /var/cache/ocache/node1
```

**Node 2:**

```bash
./ocache \
  -cluster-enabled \
  -node-id node2 \
  -cluster-addr :7000 \
  -seeds "node1:7000,node3:7000" \
  -listen-addr :9000 \
  -listen-http :9001 \
  -disk /var/cache/ocache/node2
```

**Node 3:**

```bash
./ocache \
  -cluster-enabled \
  -node-id node3 \
  -cluster-addr :7000 \
  -seeds "node1:7000,node2:7000" \
  -listen-addr :9000 \
  -listen-http :9001 \
  -disk /var/cache/ocache/node3
```

### Configuration Parameters

**Core Cluster Flags:**

| Parameter          | Description                                | Default  |
| ------------------ | ------------------------------------------ | -------- |
| `-cluster-enabled` | Enable cluster mode                        | false    |
| `-node-id`         | Unique identifier for this node            | Required |
| `-cluster-addr`    | Address for cluster communication (gossip) | :7000    |
| `-listen-addr`     | Address for gRPC client requests           | :9000    |
| `-seeds`           | Comma-separated list of seed nodes         | ""       |

**Token Ownership Persistence:**

Token ownership is automatically persisted to `<disk-path>/coordinator/ring-tokens` to maintain stable key ownership across node restarts. This prevents unnecessary data movement when a node rejoins the cluster.

## Client Usage

See [Client Usage](client.md) for more details.

### Cluster Inspection Commands

The CLI provides commands for inspecting cluster state:

```bash
# Display full cluster topology (nodes + ring config)
./ocachecli --addr "localhost:9001,localhost:9002" cluster topology

# Display info about a specific node
./ocachecli cluster node node1

# Display the current topology epoch
./ocachecli cluster epoch
```

Use `--json` flag for JSON output:

```bash
./ocachecli cluster topology --json
```

## Node Lifecycle

Each node goes through a defined lifecycle managed by the dskit ring:

### Lifecycle States

| State   | Description                                                                                  |
| ------- | ------------------------------------------------------------------------------------------- |
| JOINING | Node has registered and is claiming tokens, but is **not yet routable** — peers send it no traffic. It stays here until it signals readiness (see [Readiness gating](#readiness-gating)). |
| ACTIVE  | Node is fully participating in the ring and serving requests                                 |
| LEAVING | Node has announced departure and is waiting for gossip to propagate                          |
| LEFT    | Node has left the ring completely                                                            |

### Node Join Process

1. Node starts and creates a memberlist gossip service
2. Ring manager registers instance with JOINING state
3. Tokens are generated (or loaded from persistence file)
4. Node **stays in JOINING** — present in the ring but not routable — until it can actually serve requests: its storage has finished booting and its gRPC server is listening. Only then does it signal readiness and transition to **ACTIVE**.
5. Gossip propagates the new membership to other nodes

### Readiness gating

A (re)joining node does not advertise `ACTIVE` until it can serve — readiness is signaled once storage has booted and the gRPC server is listening (internally, via `Coordinator.MarkReady()`, which the server calls after binding the listener; embedded callers get this from `StartGRPCServer()`). This prevents peers from routing a key's traffic to a node that is still opening a large on-disk store, which would otherwise flood a cold node and stall its recovery.

Operational implications:

- The `/ready` (and `/readyz`) endpoint — and `IsReady()` — reflects the `ACTIVE` state, so a node is only marked ready once it can serve.
- In Kubernetes, add a **`startupProbe`** (e.g. on `/health`) with a generous `failureThreshold`. During a warm-cache boot the HTTP server is not listening yet — storage opens before the server binds — so a plain liveness probe would kill the pod mid-boot and crash-loop it. The startupProbe tolerates the boot window and hands off to the liveness and readiness probes only once the server is up.
- Use the **readiness** probe (`/ready`, gated on `ACTIVE`) to gate client and peer traffic, and the **liveness** probe (`/health`) to detect a hung process after startup.
- A cluster-mode node that never starts its gRPC server never becomes routable — which is correct, since peers could not reach it anyway.

### Node Leave Process (Graceful Shutdown)

1. Node receives shutdown signal
2. Calls `AnnounceLeaving()` which transitions to LEAVING state
3. Gossip propagates the state change (~500ms)
4. Node unregisters from the ring and transitions to LEFT
5. Other nodes update their routing tables

## Node Failure Detection

- Gossip-based failure detection via memberlist
- Heartbeat timeout: 60s by default
- Automatic removal from routing table after timeout

### Current Behavior

When a node fails ungracefully:

- Other nodes detect missing heartbeats via gossip
- After heartbeat timeout, node is marked as unhealthy
- Keys owned by failed node become temporarily unavailable
- Failed node automatically removed from topology

### Future Enhancements

Planned improvements:

- Temporary ownership transfer to backup nodes
- Delta synchronization on recovery
- Hinted handoff for writes during failures

## Monitoring and Operations

### Health Checks

Each node monitors others via the gossip protocol:

- Regular heartbeat messages (default: every 5 seconds)
- Configurable heartbeat timeout (default: 60 seconds)
- Automatic topology updates propagated via memberlist
- Epoch tracking for efficient client topology cache invalidation

### Adding Nodes

To add a new node:

1. Start new node with existing nodes as seeds:

```bash
./ocache \
  -cluster-enabled \
  -node-id node4 \
  -cluster-addr :7000 \
  -seeds "node1:7000,node2:7000,node3:7000" \
  -listen-addr :9000 \
  -listen-http :9001 \
  -disk /var/cache/ocache/node4
```

2. Node automatically joins the cluster
3. Hash ring rebalances to include new node
4. Clients discover new topology automatically

### Removing Nodes

**Graceful Removal:**

1. Send SIGTERM or SIGINT to the node process
2. Node transitions to LEAVING state and announces departure
3. Gossip propagates state change to other nodes (~500ms)
4. Node unregisters from ring and shuts down
5. Clients update routing tables automatically

**Ungraceful Removal (node crash):**

1. Other nodes detect missing heartbeats
2. Node removed from topology after heartbeat timeout (default: 60s)
3. Clients update routing tables automatically

## Limitations

Current limitations:

- Keys unavailable during owner node failure (no automatic failover)
- No data replication (replication factor is 1 by default)
- No automatic data migration when nodes are added

## Future Roadmap

Planned enhancements:

- Configurable replication factor for high availability
- Temporary ownership transfer during failures
- Automatic data migration on topology changes
