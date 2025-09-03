module github.com/tigrisdata/ocache/tests/integration

go 1.24.2

require (
	github.com/linxGnu/grocksdb v1.10.1
	github.com/stretchr/testify v1.10.0
	github.com/tigrisdata/ocache/storage v0.0.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/tigrisdata/ocache/proto => ../../proto

replace github.com/tigrisdata/ocache/server => ../../server

replace github.com/tigrisdata/ocache/storage => ../../storage
