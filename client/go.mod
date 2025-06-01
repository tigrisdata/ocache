module github.com/tigrisdata/cache_service/client

go 1.24.2

require (
	github.com/tigrisdata/cache_service/proto v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.72.2
)

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	golang.org/x/net v0.40.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.25.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

// We want to import the local `proto/` module
replace github.com/tigrisdata/cache_service/proto => ../proto
