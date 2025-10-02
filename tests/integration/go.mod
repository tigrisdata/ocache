module github.com/tigrisdata/ocache/tests/integration

go 1.24.2

require (
	github.com/linxGnu/grocksdb v1.10.1
	github.com/rs/zerolog v1.34.0
	github.com/stretchr/testify v1.11.1
	github.com/tigrisdata/ocache/client v0.0.0-00010101000000-000000000000
	github.com/tigrisdata/ocache/common v0.0.0-00010101000000-000000000000
	github.com/tigrisdata/ocache/coordinator v0.0.0-00010101000000-000000000000
	github.com/tigrisdata/ocache/coordinator/proto v0.0.0-00010101000000-000000000000
	github.com/tigrisdata/ocache/proto v0.0.0-00010101000000-000000000000
	github.com/tigrisdata/ocache/server v0.0.0-00010101000000-000000000000
	github.com/tigrisdata/ocache/storage v0.0.0
	google.golang.org/grpc v1.72.2
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/buraksezer/consistent v0.10.0 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.23.2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/protobuf v1.36.8 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/tigrisdata/ocache/proto => ../../proto

replace github.com/tigrisdata/ocache/server => ../../server

replace github.com/tigrisdata/ocache/storage => ../../storage

replace github.com/tigrisdata/ocache/common => ../../common

replace github.com/tigrisdata/ocache/client => ../../client

replace github.com/tigrisdata/ocache/coordinator => ../../coordinator

replace github.com/tigrisdata/ocache/coordinator/proto => ../../coordinator/proto
