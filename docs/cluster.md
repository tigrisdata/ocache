# Cluster Mode Documentation

OCache supports distributed caching through cluster mode, enabling horizontal scaling across multiple nodes using consistent hashing for data distribution.

## Overview

Cluster mode provides:

- **Horizontal Scaling**: Distribute cache across multiple nodes
- **Smart Routing**: Automatic request routing to the correct node
- **High Availability**: Temporary ownership transfer during failures
- **Consistent Hashing**: Even data distribution with minimal reshuffling
- **Connection Pooling**: Efficient resource utilization

## Architecture

### Components

1. **Coordinator**: Manages cluster membership and topology
2. **Consistent Hash Ring**: Determines data ownership using 16384 partitions
3. **Router**: Handles request forwarding between nodes
4. **Failure Detector**: Monitors node health via heartbeats
5. **Cluster Client**: Client-side routing with topology caching

### Data Distribution

- Uses consistent hashing with virtual nodes for even distribution
- Default partition count: 16384 (configurable)
- Hash function: xxhash64 for performance
- Replication factor: 20 virtual nodes per physical node

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

| Parameter             | Description                               | Default  |
| --------------------- | ----------------------------------------- | -------- |
| `-cluster-enabled`    | Enable cluster mode                       | false    |
| `-node-id`            | Unique identifier for this node           | Required |
| `-cluster-addr`       | Address for cluster communication         | :7000    |
| `-seeds`              | Comma-separated list of seed nodes        | ""       |
| `-partition-count`    | Number of hash ring partitions            | 16384    |
| `-heartbeat-interval` | Heartbeat frequency                       | 5s       |
| `-failure-threshold`  | Missed heartbeats before node marked down | 3        |

## Client Usage

See [Client Usage](client.md) for more details.

# Node Failure Detection

- Heartbeat-based failure detection
- Configurable detection threshold
- Automatic removal from routing table

### Current Behavior (Phase 1)

When a node fails:

- Keys owned by failed node become temporarily unavailable
- Other nodes continue operating normally
- Failed node automatically removed from topology

### Future Enhancements (Phase 2)

Planned improvements:

- Temporary ownership transfer to backup nodes
- Delta synchronization on recovery
- Hinted handoff for writes during failures

## Monitoring and Operations

### Health Checks

Each node monitors others via:

- Regular heartbeat messages
- Configurable failure threshold
- Automatic topology updates

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
  -listen-http :9001
```

2. Node automatically joins the cluster
3. Hash ring rebalances to include new node
4. Clients discover new topology automatically

### Removing Nodes

To remove a node gracefully:

1. Stop the node process
2. Other nodes detect failure via heartbeats
3. Node removed from topology after failure threshold
4. Clients update routing tables automatically

## Performance Considerations

### Partition Count

Impact of partition count:

- Lower (1024-4096): Less memory, coarser distribution
- Default (16384): Good balance for 3-20 nodes
- Higher (32768+): Better for large clusters (20+ nodes)

### Network Topology

Best practices:

- Keep cluster nodes in same network/datacenter
- Use private network for cluster communication
- Separate cluster traffic from client traffic

## Limitations

Current limitations (Phase 1):

- Keys unavailable during owner node failure

## Future Roadmap

Planned enhancements:

- Phase 2: Temporary ownership transfer during failures
