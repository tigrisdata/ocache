module github.com/tigrisdata/ocache/client

go 1.24.2

require (
	github.com/buraksezer/consistent v0.10.0
	github.com/pterm/pterm v0.12.81
	github.com/spf13/cobra v1.9.1
	github.com/stretchr/testify v1.11.1
	github.com/tigrisdata/ocache/common v0.0.0-00010101000000-000000000000
	github.com/tigrisdata/ocache/coordinator/proto v0.0.0-00010101000000-000000000000
	github.com/tigrisdata/ocache/proto v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.72.2
	google.golang.org/protobuf v1.36.8
)

require (
	atomicgo.dev/cursor v0.2.0 // indirect
	atomicgo.dev/keyboard v0.2.9 // indirect
	atomicgo.dev/schedule v0.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/containerd/console v1.0.5 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gookit/color v1.5.4 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/lithammer/fuzzysearch v1.1.8 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.23.2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/term v0.34.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// We want to import the local modules
replace (
	github.com/tigrisdata/ocache/common => ../common
	github.com/tigrisdata/ocache/coordinator/proto => ../coordinator/proto
	github.com/tigrisdata/ocache/proto => ../proto
)
