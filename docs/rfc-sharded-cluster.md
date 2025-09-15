# RFC-007: Distributed Sharding and High Availability

**RFC Number**: 007
**Status**: Implemented (Phase 1)  
**Author(s)**: Ovais Tariq
**Created**: 2025-08-20
**Last Updated**: 2025-09-15

## 1. Abstract

This RFC describes the design and implementation of a distributed sharding system for OCache that enables horizontal scaling across multiple nodes. The system uses consistent hashing for data distribution, provides high availability through temporary ownership transfer (without full replication), and implements efficient failure detection and recovery mechanisms.

## 2. Motivation

### 2.1 Problem Statement

OCache currently operates as a single-node cache service. This presents several limitations:

- **Scalability**: Limited by single machine resources (CPU, memory, disk)
- **Availability**: Single point of failure
- **Performance**: Cannot distribute load across multiple machines
- **Capacity**: Cannot exceed single machine storage limits

### 2.2 Goals

- Enable horizontal scaling across multiple nodes
- Provide high availability without full data replication
- Minimize data movement during topology changes
- Maintain consistent performance during node failures
- Support graceful node addition and removal
- Enable cluster-aware client with smart routing

### 2.3 Non-Goals

- Full data replication (traditional N-way replication)
- Strong consistency guarantees (eventual consistency is acceptable)
- Cross-datacenter replication
- Automatic data rebalancing on node addition

## 3. Design Overview

### 3.1 Core Principles

1. **No Data Replication**: Achieve HA through temporary ownership transfer during failures
2. **Minimal Data Movement**: Only transfer changed keys (deltas) during recovery
3. **Consistent Routing**: Use consistent hashing with virtual nodes for even distribution
4. **Graceful Degradation**: Maintain availability during partial failures
5. **Simple Operations**: Easy to understand and operate

### 3.2 High-Level Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client    │────▶│   OCache    │────▶│   OCache    │
│  (Cluster   │     │   Node 1    │     │   Node 2    │
│   Aware)    │     └─────────────┘     └─────────────┘
└─────────────┘             │                   │
                            ▼                   ▼
                    ┌─────────────┐     ┌─────────────┐
                    │  Coordinator│     │  Coordinator│
                    │   + Ring    │◀───▶│   + Ring    │
                    └─────────────┘     └─────────────┘
```

### 3.3 Key Components

1. **Consistent Hash Ring**: Determines data ownership
2. **Coordinator**: Manages membership and routing
3. **Router**: Handles request forwarding with retries and circuit breaking
4. **Failure Detector**: Monitors node health via heartbeats
5. **Seed Discovery**: Dynamic or static node discovery
6. **Cluster Client**: Client-side routing and connection pooling

## 4. Detailed Design

### 4.1 Consistent Hashing

- Uses xxhash for consistent hashing
- Virtual nodes (vnodes) for better distribution (ReplicationFactor: 20)
- Partition count: 16384 (configurable)
- Load factor: 1.25 for bounded loads

### 4.2 Request Routing

```
1. Client sends request to any node
2. Node calculates hash(key) → partition
3. Ring.GetNode(key) → owner node
4. If owner == local:
   - Process locally
5. If owner == remote:
   - Router.Route(key) with retry and circuit breaker
   - Forward request to owner
6. If owner == unavailable:
   - Phase 1: Return error
   - Phase 2: Route to temporary owner (hinted handoff)
```

### 4.3 Cluster Membership

#### 4.3.1 Join Protocol

```
New Node                    Seed Node
   │                           │
   ├──GetClusterState()────────▶
   │                           │
   ◀────ClusterState───────────┤
   │                           │
   ├──Join(NodeInfo)───────────▶
   │                           │
   ◀────JoinResponse────────────┤
   │                           │
   ├──Start Heartbeat──────────▶
```

#### 4.3.2 Failure Detection

- **Heartbeat Interval**: 5 seconds (configurable)
- **Failure Threshold**: 3 missed heartbeats
- **Detection Time**: ~15 seconds
- **State Transitions**: Active → Down (no intermediate states in Phase 1)

#### 4.3.3 Seed Discovery

- Static and DNS-based seed discovery.

### 4.4 Client Integration

- Maintains local copy of ring topology
- Routes requests directly to owner nodes
- Handles retries and failover
- Refreshes topology periodically (30s default)

### 4.5 Data Consistency Model

#### 4.5.1 Phase 1: Best Effort

- No replication
- Data loss on node failure
- Eventually consistent after recovery
- No stale reads (data is either available or not)

#### 4.5.2 Phase 2: Hinted Handoff (Planned)

- Temporary ownership transfer during failures
- Hint storage for mutations during downtime
- Replay protocol on recovery
- Bounded inconsistency window

## 5. Performance Considerations

### 5.1 Latency Impact

- **Local requests**: No additional latency
- **Remote requests**: +1 network hop (~1-2ms in same DC)
- **Failed node requests**: +retry backoff (100ms initial)

### 5.2 Throughput

- **Horizontal scaling**: Near-linear with node count
- **Connection pooling**: Reduces connection overhead
- **Circuit breaker**: Prevents cascade failures

### 5.3 Resource Usage

- **Memory**: ~100MB per node for ring + connections
- **CPU**: <1% for heartbeat and failure detection
- **Network**: Heartbeat traffic: N*(N-1) * 100 bytes/5s

## 6. Operational Considerations

### 6.1 Failure Scenarios

| Scenario               | Impact                            | Recovery                             |
| ---------------------- | --------------------------------- | ------------------------------------ |
| Single node failure    | Keys on failed node unavailable   | Automatic detection in ~15s          |
| Network partition      | Split brain possible              | Manual intervention required         |
| Cascading failures     | Circuit breakers prevent overload | Automatic recovery when nodes return |
| DNS resolution failure | New nodes cannot join             | Cached seeds used for 5 minutes      |

## 7. Security Considerations

### 7.1 Current State

- No authentication between nodes
- No encryption for inter-node communication
- Trust-based cluster membership

### 7.2 Future Enhancements

- mTLS for inter-node communication
- Token-based authentication for join operations
- Network isolation via private VPC
- Rate limiting for cluster operations

## 8. Alternatives Considered

### 8.1 Full Replication

- **Pros**: High availability, read scaling
- **Cons**: 2-3x storage overhead, complex consistency
- **Decision**: Rejected in favor of hinted handoff

### 8.2 Master-Slave Replication

- **Pros**: Simple consistency model
- **Cons**: Write bottleneck, failover complexity
- **Decision**: Rejected for poor write scaling

### 8.3 External Coordination (Zookeeper/etcd)

- **Pros**: Battle-tested coordination
- **Cons**: Additional dependency, operational complexity
- **Decision**: Rejected for simplicity

## 9. References

- [Amazon Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Consistent Hashing with Bounded Loads](https://ai.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html)
- [Cassandra Architecture](https://cassandra.apache.org/doc/latest/cassandra/architecture/)
- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html)
- [buraksezer/consistent](https://github.com/buraksezer/consistent)
