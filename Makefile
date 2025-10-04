# OCache Makefile

# Variables
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Allow custom RocksDB installation path
ROCKSDB_PREFIX ?= /usr/local

# Platform-specific settings
ifeq ($(UNAME_S),Darwin)
    # macOS (both Intel and Apple Silicon)
    BREW_PREFIX := $(shell brew --prefix 2>/dev/null || echo "/usr/local")
    # Check if custom RocksDB exists, otherwise use brew
    ifeq ($(wildcard $(ROCKSDB_PREFIX)/include/rocksdb/c.h),)
        CGO_CFLAGS := -I$(BREW_PREFIX)/include
        CGO_LDFLAGS := -L$(BREW_PREFIX)/lib
    else
        CGO_CFLAGS := -I$(ROCKSDB_PREFIX)/include
        CGO_LDFLAGS := -L$(ROCKSDB_PREFIX)/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd
    endif
else ifeq ($(UNAME_S),Linux)
    # Linux - prioritize custom RocksDB if available
    ifeq ($(wildcard $(ROCKSDB_PREFIX)/include/rocksdb/c.h),)
        # Fallback to system paths
        CGO_CFLAGS := -I/usr/include -I/usr/local/include
        ifeq ($(UNAME_M),x86_64)
            CGO_LDFLAGS := -L/usr/lib -L/usr/lib/x86_64-linux-gnu -L/usr/lib64 -L/usr/local/lib
        else ifeq ($(UNAME_M),aarch64)
            CGO_LDFLAGS := -L/usr/lib -L/usr/lib/aarch64-linux-gnu -L/usr/lib64 -L/usr/local/lib
        else ifeq ($(UNAME_M),arm64)
            CGO_LDFLAGS := -L/usr/lib -L/usr/lib/aarch64-linux-gnu -L/usr/lib64 -L/usr/local/lib
        else
            # Generic Linux fallback
            CGO_LDFLAGS := -L/usr/lib -L/usr/lib64 -L/usr/local/lib
        endif
    else
        # Use custom RocksDB installation
        CGO_CFLAGS := -I$(ROCKSDB_PREFIX)/include
        CGO_LDFLAGS := -L$(ROCKSDB_PREFIX)/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd
    endif
endif

# Build targets
.PHONY: all
all: build build-cli

# Static build configuration
STATIC_BUILD ?= false
ifeq ($(STATIC_BUILD),true)
    # Use static RocksDB from artifact
    ROCKSDB_STATIC_DIR ?= $(shell pwd)/rocksdb-static/artifact
    CGO_CFLAGS := -I$(ROCKSDB_STATIC_DIR)/include
    ifeq ($(UNAME_S),Darwin)
        # On macOS, also include Homebrew paths for compression libraries
        BREW_PREFIX := $(shell brew --prefix 2>/dev/null || echo "/usr/local")
        CGO_LDFLAGS := -L$(ROCKSDB_STATIC_DIR)/lib -L$(BREW_PREFIX)/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -pthread
    else
        CGO_LDFLAGS := -L$(ROCKSDB_STATIC_DIR)/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -pthread
    endif
    # macOS doesn't support fully static binaries, so we only statically link RocksDB
    ifeq ($(UNAME_S),Darwin)
        LDFLAGS := -ldflags "-s -w"
    else
        LDFLAGS := -ldflags "-linkmode external -extldflags '-static'"
    endif
endif

.PHONY: build
build: proto
	CGO_ENABLED=1 CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go build $(LDFLAGS) -o ocache ./server/

.PHONY: build-static
build-static:
	STATIC_BUILD=true $(MAKE) build

.PHONY: build-cli
build-cli:
	go build -o ocachecli ./client/cmd/

.PHONY: proto
proto: proto-api proto-storage proto-cluster

.PHONY: proto-api
proto-api:
	protoc -I ./proto \
		-I ./proto/google \
		-I . \
		--go_out=paths=source_relative:./proto \
		--go-grpc_out=paths=source_relative:./proto \
		--grpc-gateway_out=paths=source_relative:./proto \
		proto/cache.proto

.PHONY: proto-storage
proto-storage:
	protoc -I ./storage/proto \
		--go_out=paths=source_relative:./storage/proto \
		storage/proto/storage.proto

.PHONY: proto-cluster
proto-cluster:
	protoc -I ./coordinator/proto \
		--go_out=paths=source_relative:./coordinator/proto \
		--go-grpc_out=paths=source_relative:./coordinator/proto \
		coordinator/proto/cluster.proto

# Installation
.PHONY: install-deps
install-deps: install-protoc install-protoc-plugins

.PHONY: install-rocksdb
install-rocksdb:
ifeq ($(UNAME_S),Darwin)
	@echo "Installing RocksDB on macOS..."
	@if ! command -v brew &> /dev/null; then \
		echo "Homebrew is required but not installed. Please install it first."; \
		exit 1; \
	fi
	brew install rocksdb
else ifeq ($(UNAME_S),Linux)
	@echo "Installing RocksDB on Linux..."
	@if command -v apt-get &> /dev/null; then \
		sudo apt-get update && sudo apt-get install -y librocksdb-dev libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev; \
	elif command -v yum &> /dev/null; then \
		sudo yum install -y rocksdb-devel snappy-devel lz4-devel libzstd-devel zlib-devel; \
	elif command -v dnf &> /dev/null; then \
		sudo dnf install -y rocksdb-devel snappy-devel lz4-devel libzstd-devel zlib-devel; \
	else \
		echo "Unsupported Linux distribution. Please install RocksDB manually."; \
		exit 1; \
	fi
else
	@echo "Unsupported platform: $(UNAME_S)"
	@exit 1
endif

.PHONY: install-rocksdb-from-source
install-rocksdb-from-source:
	@echo "Installing RocksDB from source..."
	@./scripts/install-rocksdb.sh

.PHONY: build-rocksdb-static
build-rocksdb-static:
	@echo "Building RocksDB static library..."
	@./scripts/build-rocksdb-static.sh

.PHONY: install-protoc
install-protoc:
ifeq ($(UNAME_S),Darwin)
	@echo "Installing protoc on macOS..."
	@if ! command -v brew &> /dev/null; then \
		echo "Homebrew is required but not installed. Please install it first."; \
		exit 1; \
	fi
	brew install protobuf
else ifeq ($(UNAME_S),Linux)
	@echo "Installing protoc on Linux..."
	@if command -v apt-get &> /dev/null; then \
		sudo apt-get update && sudo apt-get install -y protobuf-compiler; \
	elif command -v yum &> /dev/null; then \
		sudo yum install -y protobuf-compiler; \
	elif command -v dnf &> /dev/null; then \
		sudo dnf install -y protobuf-compiler; \
	else \
		echo "Unsupported Linux distribution. Please install protoc manually."; \
		exit 1; \
	fi
else
	@echo "Unsupported platform: $(UNAME_S)"
	@exit 1
endif

.PHONY: install-protoc-plugins
install-protoc-plugins:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest

# Run targets
.PHONY: run
run: build
	./ocache -disk /tmp/ocache

.PHONY: run-verbose
run-verbose: build
	./ocache -disk /tmp/ocache -v

.PHONY: bench
bench: build build-cli run-background
	./ocachecli --addr localhost:9000 bench
	@$(MAKE) stop

.PHONY: run-background
run-background:
	@echo "Starting ocache in background..."
	@./ocache -disk /tmp/ocache > ocache.log 2>&1 & echo $$! > ocache.pid
	@sleep 2

.PHONY: stop
stop:
	@if [ -f ocache.pid ]; then \
		kill `cat ocache.pid` 2>/dev/null || true; \
		rm -f ocache.pid; \
		echo "Stopped ocache"; \
	fi

# Testing targets
# Allow specifying specific tests with TEST variable (e.g., make test TEST=TestMyFunction)
# or with TESTRUN for pattern matching (e.g., make test TESTRUN=MyFunction)
TEST ?=
TESTRUN ?=
TESTFLAGS := $(if $(TEST),-run $(TEST),$(if $(TESTRUN),-run $(TESTRUN),))

.PHONY: test
test: test-server test-storage test-client test-coordinator

.PHONY: test-all
test-all: test-server test-storage test-client test-coordinator test-integration test-e2e

.PHONY: test-server
test-server: proto
	@echo "Running server tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -timeout 60s $(TESTFLAGS) ./...

.PHONY: test-storage
test-storage: proto
	@echo "Running storage tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd storage && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -timeout 60s $(TESTFLAGS) ./...

.PHONY: test-client
test-client: proto
	@echo "Running client tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd client && go test -v -timeout 30s $(TESTFLAGS) ./...

.PHONY: test-coordinator
test-coordinator: proto
	@echo "Running coordinator tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd coordinator && go test -v -timeout 30s $(TESTFLAGS) ./...

.PHONY: test-race
test-race: proto
	@echo "Running race tests for server..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -race -v -timeout 60s $(TESTFLAGS) ./...
	@echo "Running race tests for coordinator..."
	@cd coordinator && go test -race -v -timeout 30s $(TESTFLAGS) ./...
	@echo "Running race tests for client..."
	@cd client && go test -race -v -timeout 30s $(TESTFLAGS) ./...

.PHONY: test-coverage
test-coverage: proto
	@echo "Running coverage tests for server..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -coverprofile=../coverage-server.out -timeout 60s $(TESTFLAGS) ./...
	@echo "Running coverage tests for client..."
	@cd client && go test -coverprofile=../coverage-client.out -timeout 30s $(TESTFLAGS) ./...
	@echo "Running coverage tests for coordinator..."
	@cd coordinator && go test -coverprofile=../coverage-coordinator.out -timeout 30s $(TESTFLAGS) ./...
	@echo "Combining coverage reports..."
	@echo "mode: set" > coverage.out
	@tail -n +2 coverage-server.out >> coverage.out 2>/dev/null || true
	@tail -n +2 coverage-client.out >> coverage.out 2>/dev/null || true
	@tail -n +2 coverage-coordinator.out >> coverage.out 2>/dev/null || true
	@rm -f coverage-*.out
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated at coverage.html"

.PHONY: test-e2e
test-e2e: build build-cli
	@echo "Running all E2E tests..."
	@$(MAKE) test-e2e-concurrent
	@$(MAKE) test-e2e-storage-layers
	@$(MAKE) test-e2e-ttl
	@$(MAKE) test-e2e-lru
	@$(MAKE) test-e2e-compaction
	@$(MAKE) test-e2e-recompaction
	@$(MAKE) test-e2e-data-validation

.PHONY: test-e2e-concurrent
test-e2e-concurrent: build build-cli
	@echo "Running concurrent operations E2E test..."
	./tests/e2e/concurrent_ops_test.sh

.PHONY: test-e2e-storage-layers
test-e2e-storage-layers: build build-cli
	@echo "Running storage layers E2E test..."
	./tests/e2e/storage_layers_test.sh

.PHONY: test-e2e-ttl
test-e2e-ttl: build build-cli
	@echo "Running TTL functionality E2E test..."
	./tests/e2e/ttl_cleaner_test.sh

.PHONY: test-e2e-lru
test-e2e-lru: build build-cli
	@echo "Running LRU eviction E2E test..."
	./tests/e2e/lru_eviction_test.sh

.PHONY: test-e2e-compaction
test-e2e-compaction: build build-cli
	@echo "Running compaction E2E test..."
	./tests/e2e/compaction_test.sh

.PHONY: test-e2e-recompaction
test-e2e-recompaction: build build-cli
	@echo "Running recompaction E2E test..."
	./tests/e2e/recompaction_test.sh

.PHONY: test-e2e-data-validation
test-e2e-data-validation: build build-cli
	@echo "Running comprehensive data validation E2E test..."
	./tests/e2e/data_validation_test.sh

.PHONY: test-integration
test-integration: proto
	@echo "Running integration tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -timeout 300s $(TESTFLAGS) ./...

.PHONY: test-integration-short
test-integration-short: proto
	@echo "Running integration tests (short mode)..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -short -timeout 30s $(TESTFLAGS) ./...

.PHONY: test-integration-race
test-integration-race: proto
	@echo "Running integration tests with race detector..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -race -v -timeout 180s $(TESTFLAGS) ./...

.PHONY: test-integration-coverage
test-integration-coverage: proto
	@echo "Running integration tests with coverage..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -coverprofile=../../coverage-integration.out -timeout 300s $(TESTFLAGS) ./...
	@go tool cover -html=coverage-integration.out -o coverage-integration.html
	@echo "Integration test coverage report generated at coverage-integration.html"

.PHONY: test-integration-objects
test-integration-objects:
	@echo "Running small, medium, and large objects integration tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -run $(if $(TEST)$(TESTRUN),$(if $(TEST),$(TEST),$(TESTRUN)),TestIntegration_Objects) -timeout 120s ./...

.PHONY: test-integration-compaction
test-integration-compaction:
	@echo "Running compaction integration tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -run $(if $(TEST)$(TESTRUN),$(if $(TEST),$(TEST),$(TESTRUN)),TestIntegration_Compaction) -timeout 300s ./...

.PHONY: test-integration-cleaner
test-integration-cleaner:
	@echo "Running cleaner integration tests (TTL and LRU)..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -run $(if $(TEST)$(TESTRUN),$(if $(TEST),$(TEST),$(TESTRUN)),TestIntegration_Cleaner) -timeout 120s ./...

.PHONY: test-integration-workflow
test-integration-workflow:
	@echo "Running workflow integration tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -run $(if $(TEST)$(TESTRUN),$(if $(TEST),$(TEST),$(TESTRUN)),TestIntegration_Workflow) -timeout 300s ./...

.PHONY: test-integration-coordinator
test-integration-coordinator:
	@echo "Running coordinator integration tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -run $(if $(TEST)$(TESTRUN),$(if $(TEST),$(TEST),$(TESTRUN)),TestIntegration_Coordinator) -timeout 600s ./...

.PHONY: test-integration-cluster
test-integration-cluster:
	@echo "Running cluster integration tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	@cd tests/integration && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test $(LDFLAGS) -v -run $(if $(TEST)$(TESTRUN),$(if $(TEST),$(TEST),$(TESTRUN)),TestIntegration_Cluster) -timeout 600s ./...

# Code quality targets
.PHONY: lint
lint:
	@echo "Running go vet..."
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go vet $(LDFLAGS) ./...
	@cd client && go vet ./...
	@echo "Running gofmt..."
	@gofmt -l -d $$(find . -name '*.go' -not -path './proto/*')
	@echo "Running go mod tidy..."
	@go work sync
	@cd server && go mod tidy
	@cd client && go mod tidy
	@cd proto && go mod tidy

.PHONY: lint-ci
lint-ci:
	@echo "Running gofmt..."
	@gofmt -l -d $$(find . -name '*.go' -not -path './proto/*')
	@echo "Running go mod tidy check..."
	@go work sync
	@cd server && go mod tidy
	@cd client && go mod tidy
	@cd proto && go mod tidy
	@git diff --exit-code go.mod go.sum || (echo "go.mod or go.sum is not tidy" && exit 1)

.PHONY: lint-fix
lint-fix:
	@echo "Fixing formatting issues..."
	@gofmt -w $$(find . -name '*.go' -not -path './proto/*')
	@echo "Running go mod tidy..."
	@go work sync
	@cd server && go mod tidy
	@cd client && go mod tidy
	@cd proto && go mod tidy

.PHONY: vet
vet:
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go vet $(LDFLAGS) ./...
	@cd client && go vet ./...

.PHONY: fmt
fmt:
	@gofmt -w $$(find . -name '*.go' -not -path './proto/*')

.PHONY: fmt-check
fmt-check:
	@gofmt -l $$(find . -name '*.go' -not -path './proto/*')

.PHONY: check
check: fmt-check vet test
	@echo "All checks passed!"

# Clean targets
.PHONY: clean
clean:
	rm -f ocache ocachecli ocache.log ocache.pid
	rm -f proto/*.pb.go proto/*.pb.gw.go
	rm -f coverage.out coverage.html coverage-integration.out coverage-integration.html
	rm -rf /tmp/ocache /tmp/ocache-demo /tmp/ocache-integration-test-*

# Help target
.PHONY: help
help:
	@echo "OCache Makefile targets:"
	@echo ""
	@echo "Build targets:"
	@echo "  all           - Build both server and CLI"
	@echo "  build         - Build the server"
	@echo "  build-static  - Build the server with static RocksDB"
	@echo "  build-cli     - Build the CLI client"
	@echo "  proto         - Generate Go code from protobuf"
	@echo ""
	@echo "Test targets:"
	@echo "  test                        - Run unit tests (server and client)"
	@echo "  test-all                    - Run all tests (unit and integration)"
	@echo "  test-server                 - Run server tests only"
	@echo "  test-client                 - Run client tests only"
	@echo "  test-coordinator            - Run coordinator tests only"
	@echo "  test-race                   - Run tests with race detector"
	@echo "  test-coverage               - Run tests with coverage report"
	@echo "  test-e2e                    - Run end-to-end tests"
	@echo "  test-integration            - Run integration tests (storage layer)"
	@echo "  test-integration-short      - Run integration tests in short mode"
	@echo "  test-integration-objects    - Run small, medium, and large objects integration tests"
	@echo "  test-integration-compaction - Run compaction integration tests"
	@echo "  test-integration-cleaner    - Run cleaner integration tests (TTL and LRU)"
	@echo "  test-integration-workflow   - Run cross-component integration tests"
	@echo "  test-integration-coordinator - Run coordinator/cluster integration tests"
	@echo "  test-integration-cluster    - Run cluster integration tests"
	@echo "  test-integration-race       - Run integration tests with race detector"
	@echo "  test-integration-coverage   - Run integration tests with coverage"
	@echo "  bench                       - Run benchmarks"
	@echo ""
	@echo "  To run specific tests, use TEST or TESTRUN variable:"
	@echo "    make test TEST=TestMyFunction      - Run exact test name"
	@echo "    make test TESTRUN=MyFunction       - Run tests matching pattern"
	@echo "    make test-server TEST=TestStorage  - Run specific server test"
	@echo ""
	@echo "Code quality targets:"
	@echo "  lint          - Run linters (vet, gofmt check, mod tidy)"
	@echo "  lint-fix      - Fix linting issues"
	@echo "  vet           - Run go vet"
	@echo "  fmt           - Format code with gofmt"
	@echo "  fmt-check     - Check code formatting"
	@echo "  check         - Run all quality checks (fmt, vet, test)"
	@echo ""
	@echo "Run targets:"
	@echo "  run           - Run the server with default options"
	@echo "  run-verbose   - Run the server with verbose logging"
	@echo ""
	@echo "Other targets:"
	@echo "  install-deps  - Install dependencies (RocksDB)"
	@echo "  install-protoc-plugins - Install protoc Go plugins"
	@echo "  build-rocksdb-static - Build RocksDB static library"
	@echo "  clean         - Remove built binaries and generated files"
	@echo "  help          - Show this help message"

.DEFAULT_GOAL := help