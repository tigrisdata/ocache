# End-to-End Integration Tests

This directory contains end-to-end integration tests for ocache.

## Contents

- `ttl_lru_test.sh` - E2E test for TTL expiration and LRU eviction functionality

## Running Tests

Make sure ocache server is built and available in your PATH:

```bash
./tests/e2e/ttl_lru_test.sh
```

## Adding New Tests

When adding new e2e tests:

1. Create executable shell scripts or test programs
2. Document their purpose and usage
3. Ensure they clean up after themselves
