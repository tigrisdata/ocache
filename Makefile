# OCache Makefile

# Variables
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Check if pkg-config is available for RocksDB
HAS_PKG_CONFIG := $(shell command -v pkg-config 2> /dev/null)
ifdef HAS_PKG_CONFIG
    ROCKSDB_EXISTS := $(shell pkg-config --exists rocksdb 2> /dev/null && echo yes)
endif

# Platform-specific settings
ifeq ($(UNAME_S),Darwin)
    # macOS (both Intel and Apple Silicon)
    BREW_PREFIX := $(shell brew --prefix 2>/dev/null || echo "/usr/local")
    CGO_CFLAGS := -I$(BREW_PREFIX)/include
    CGO_LDFLAGS := -L$(BREW_PREFIX)/lib
else ifeq ($(UNAME_S),Linux)
    # Linux - handle different architectures
    ifdef ROCKSDB_EXISTS
        # Use pkg-config if available
        CGO_CFLAGS := $(shell pkg-config --cflags rocksdb)
        CGO_LDFLAGS := $(shell pkg-config --libs rocksdb)
    else
        # Fallback to manual paths
        CGO_CFLAGS := -I/usr/include -I/usr/local/include
        ifeq ($(UNAME_M),x86_64)
            CGO_LDFLAGS := -L/usr/lib -L/usr/lib/x86_64-linux-gnu -L/usr/lib64 -L/usr/local/lib -lrocksdb
        else ifeq ($(UNAME_M),aarch64)
            CGO_LDFLAGS := -L/usr/lib -L/usr/lib/aarch64-linux-gnu -L/usr/lib64 -L/usr/local/lib -lrocksdb
        else ifeq ($(UNAME_M),arm64)
            CGO_LDFLAGS := -L/usr/lib -L/usr/lib/aarch64-linux-gnu -L/usr/lib64 -L/usr/local/lib -lrocksdb
        else
            # Generic Linux fallback
            CGO_LDFLAGS := -L/usr/lib -L/usr/lib64 -L/usr/local/lib -lrocksdb
        endif
    endif
endif

# Build targets
.PHONY: all
all: build build-cli

.PHONY: build
build: proto
	CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go build -o ocache ./server/

.PHONY: build-cli
build-cli:
	go build -o ocachecli ./client/cmd/

.PHONY: proto
proto:
	protoc -I ./proto \
		-I ./proto/google \
		--go_out=paths=source_relative:./proto \
		--go-grpc_out=paths=source_relative:./proto \
		--grpc-gateway_out=paths=source_relative:./proto \
		proto/cache.proto

# Installation
.PHONY: install-deps
install-deps: install-protoc install-protoc-plugins install-rocksdb

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
		sudo apt-get update && sudo apt-get install -y librocksdb-dev pkg-config; \
	elif command -v yum &> /dev/null; then \
		sudo yum install -y rocksdb-devel pkgconfig; \
	elif command -v dnf &> /dev/null; then \
		sudo dnf install -y rocksdb-devel pkgconfig; \
	else \
		echo "Unsupported Linux distribution. Please install RocksDB manually."; \
		exit 1; \
	fi
else
	@echo "Unsupported platform: $(UNAME_S)"
	@exit 1
endif

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
.PHONY: test
test: test-server test-client

.PHONY: test-server
test-server:
	@echo "Running server tests..."
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test -v -timeout 60s ./...

.PHONY: test-client
test-client:
	@echo "Running client tests..."
	@cd client && go test -v -timeout 30s ./...

.PHONY: test-race
test-race:
	@echo "Running race tests for server..."
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test -race -v -timeout 60s ./...
	@echo "Running race tests for client..."
	@cd client && go test -race -v -timeout 30s ./...

.PHONY: test-coverage
test-coverage:
	@echo "Running coverage tests for server..."
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go test -coverprofile=../coverage-server.out -timeout 60s ./...
	@echo "Running coverage tests for client..."
	@cd client && go test -coverprofile=../coverage-client.out -timeout 30s ./...
	@echo "Combining coverage reports..."
	@echo "mode: set" > coverage.out
	@tail -n +2 coverage-server.out >> coverage.out 2>/dev/null || true
	@tail -n +2 coverage-client.out >> coverage.out 2>/dev/null || true
	@rm -f coverage-*.out
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated at coverage.html"

.PHONY: test-e2e
test-e2e: build build-cli
	@chmod +x e2e/*.sh
	@echo "Running E2E tests..."
	@cd e2e && ./ttl_lru_test.sh

# Code quality targets
.PHONY: lint
lint:
	@echo "Running go vet..."
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go vet ./...
	@cd client && go vet ./...
	@echo "Running gofmt..."
	@gofmt -l -d $$(find . -name '*.go' -not -path './proto/*')
	@echo "Running go mod tidy..."
	@go work sync
	@cd server && go mod tidy
	@cd client && go mod tidy
	@cd proto && go mod tidy

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
	@cd server && CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" go vet ./...
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
	rm -f coverage.out coverage.html
	rm -rf /tmp/ocache /tmp/ocache-demo

# Help target
.PHONY: help
help:
	@echo "OCache Makefile targets:"
	@echo ""
	@echo "Build targets:"
	@echo "  all           - Build both server and CLI"
	@echo "  build         - Build the server"
	@echo "  build-cli     - Build the CLI client"
	@echo "  proto         - Generate Go code from protobuf"
	@echo ""
	@echo "Test targets:"
	@echo "  test          - Run all unit tests"
	@echo "  test-server   - Run server tests only"
	@echo "  test-client   - Run client tests only"
	@echo "  test-race     - Run tests with race detector"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  test-e2e      - Run end-to-end tests"
	@echo "  bench         - Run benchmarks"
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
	@echo "  clean         - Remove built binaries and generated files"
	@echo "  help          - Show this help message"

.DEFAULT_GOAL := help