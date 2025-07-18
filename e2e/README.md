# End-to-End Integration Tests

This directory contains end-to-end integration tests and demo scripts for ocache.

## Contents

- `demo_ttl_lru.sh` - Demonstrates TTL expiration and LRU eviction functionality

## Running Tests

Make sure ocache server is built and available in your PATH:

```bash
cd e2e
./demo_ttl_lru.sh
```

## Adding New Tests

When adding new e2e tests:
1. Create executable shell scripts or test programs
2. Document their purpose and usage
3. Ensure they clean up after themselves