# Run Benchmarks

Run performance benchmarks against the cache service using the CLI benchmark command.

```bash
ocachecli bench [options]
```

## Global Options

These options control how the client connects to the cache:

| Flag                 | Description                                     | Default          |
| -------------------- | ----------------------------------------------- | ---------------- |
| `--addr`             | Cache server address(es), comma-separated       | `localhost:9000` |
| `--mode`             | Connection mode: `auto`, `simple`, or `cluster` | `auto`           |
| `--topology-refresh` | Topology refresh interval (cluster mode only)   | `30s`            |

## Benchmark Options

| Flag                     | Description                           | Default           |
| ------------------------ | ------------------------------------- | ----------------- |
| `--connection-pool-size` | Number of connections per server      | `4`               |
| `--num-keys`             | Number of unique keys                 | `1000`            |
| `--value-size`           | Size of each value in bytes           | `100`             |
| `--num-ops`              | Total number of operations            | `10000`           |
| `--concurrency`          | Number of concurrent workers          | `8`               |
| `--workload`             | Workload type (A, B, C) or custom mix | `A`               |
| `--seed`                 | Random seed for reproducibility       | Current timestamp |
| `--no-progress`          | Disable progress output               | `false`           |
| `--force-streaming`      | Force streaming for all operations    | `false`           |

**Workload Types:**

| Type   | Description  | Read %                         | Update % |
| ------ | ------------ | ------------------------------ | -------- |
| A      | Update heavy | 50                             | 50       |
| B      | Read mostly  | 95                             | 5        |
| C      | Read only    | 100                            | 0        |
| Custom | User defined | Specify as `read=70,update=30` |          |

**Examples:**

First, run the server with default settings:

```bash
make run
```

Then, run the benchmark:

```bash
# Basic benchmark with default settings
ocachecli bench

# Custom workload with more operations
ocachecli bench \
  --num-keys 10000 \
  --num-ops 100000 \
  --concurrency 16 \
  --value-size 1024 \
  --workload B

# Benchmark with multiple servers
ocachecli \
  --addr "node1:9001,node2:9002,node3:9003" \
  bench \
  --num-keys 50000 \
  --num-ops 500000 \
  --concurrency 32
```

**Benchmark Examples by Object Size:**

```bash
# Run all three sizes sequentially to test cache behavior across object sizes
echo "=== Testing 100 byte objects ==="
ocachecli bench --value-size 100 --num-ops 10000 --workload A

echo "=== Testing 100 KB objects ==="
ocachecli bench --value-size 100000 --num-ops 10000 --workload B

echo "=== Testing 1 MB objects ==="
ocachecli bench --value-size 1000000 --num-ops 10000 --workload C
```

## Output

The benchmark command provides detailed statistics including:

- Total operations completed
- Operations per second (throughput)
- Latency percentiles (p50, p95, p99)
- Operation breakdown by type
- Error count (if any)

## Performance Tuning Tips

### Compare Cluster vs Single-node Performance

Example:

```bash
# Single node baseline
ocachecli --mode simple --addr localhost:9001 bench \
  --num-ops 100000 --concurrency 32

# Cluster comparison
ocachecli --mode auto --addr "localhost:9001,localhost:9002,localhost:9003" bench \
  --num-ops 100000 --concurrency 32
```

### Realistic Workloads

Create custom workloads that match your application:

```bash
# Read-heavy workload (90% reads, 10% updates)
ocachecli bench --workload "read=90,update=10"

# Write-heavy workload (30% reads, 70% updates)
ocachecli bench --workload "read=30,update=70"

# Mixed with your actual data sizes
ocachecli bench \
  --value-size 4096 \     # 4KB values
  --num-keys 100000 \     # 100K unique keys
  --workload "read=70,update=30"
```
