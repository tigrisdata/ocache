module github.com/tigrisdata/ocache/storage

go 1.24.2

require (
	github.com/google/uuid v1.6.0
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/linxGnu/grocksdb v1.10.1
	github.com/rs/zerolog v1.34.0
	github.com/stretchr/testify v1.10.0
	github.com/tigrisdata/ocache/proto v0.0.0-00010101000000-000000000000
	github.com/tigrisdata/ocache/server v0.0.0-00010101000000-000000000000
	golang.org/x/sys v0.33.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.23.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.65.0 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	golang.org/x/net v0.40.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/grpc v1.72.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// We want to import the local modules
replace github.com/tigrisdata/ocache/proto => ../proto

replace github.com/tigrisdata/ocache/server => ../server
