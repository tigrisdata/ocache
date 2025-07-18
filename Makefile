# OCache Makefile

# Variables
BREW_PREFIX := $(shell brew --prefix)
CGO_CFLAGS := -I$(BREW_PREFIX)/include
CGO_LDFLAGS := -L$(BREW_PREFIX)/lib

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
install-deps: install-protoc-plugins
	brew install rocksdb

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

# Testing targets
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

# Clean targets
.PHONY: clean
clean:
	rm -f ocache ocachecli ocache.log ocache.pid
	rm -f proto/*.pb.go proto/*.pb.gw.go

# Help target
.PHONY: help
help:
	@echo "OCache Makefile targets:"
	@echo "  all           - Build both server and CLI"
	@echo "  build         - Build the server"
	@echo "  build-cli     - Build the CLI client"
	@echo "  proto         - Generate Go code from protobuf"
	@echo "  install-deps  - Install dependencies (RocksDB)"
	@echo "  install-protoc-plugins - Install protoc Go plugins"
	@echo "  run           - Run the server with default options"
	@echo "  run-verbose   - Run the server with verbose logging"
	@echo "  bench         - Run benchmarks"
	@echo "  clean         - Remove built binaries and generated files"
	@echo "  help          - Show this help message"

.DEFAULT_GOAL := help