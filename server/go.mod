module github.com/tigrisdata/cache_service/server

go 1.24.2

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3
	github.com/linxGnu/grocksdb v1.10.1
	github.com/rs/zerolog v1.34.0
	github.com/stretchr/testify v1.10.0
	github.com/tigrisdata/cache_service/proto v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.72.2
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	golang.org/x/net v0.40.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.25.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/protobuf v1.36.5
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// We want to import the local `proto/` module
// We want to import the local `proto/` module
replace github.com/tigrisdata/cache_service/proto => ../proto
