package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	cacheclient "github.com/tigrisdata/ocache/client"
	ycsb "github.com/tigrisdata/ocache/client/cmd/ycsb"
	clusterpb "github.com/tigrisdata/ocache/coordinator/proto"
)

var rootCmd = &cobra.Command{
	Use:   "ocachecli",
	Short: "CLI for interacting with the cache service",
}

func main() {
	rootCmd.AddCommand(putCmd, getCmd, delCmd, listCmd, benchCmd, clusterCmd, versionCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// flags for the CLI
var (
	addr            string
	ttl             int64
	connMode        string
	topologyRefresh time.Duration

	connectionPoolSize int
	numKeys            int
	valueSize          int
	numOps             int
	concurrency        int
	workload           string
	seed               int64
	noProgress         bool
	forceStreaming     bool
	listPrefix         string

	// cluster command flags
	jsonOutput bool
)

func newClient() *cacheclient.Client {
	// Parse comma-separated addresses
	addrs := strings.Split(addr, ",")
	for i, a := range addrs {
		addrs[i] = strings.TrimSpace(a)
	}

	config := &cacheclient.ClientConfig{
		Addrs:              addrs,
		Mode:               cacheclient.ConnectionMode(connMode),
		RefreshInterval:    topologyRefresh,
		ConnectionPoolSize: connectionPoolSize,
	}

	c, err := cacheclient.NewWithConfig(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}

	return c
}

// createContext creates a context that is cancelled on interrupt signal
func createContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	return ctx, cancel
}

var putCmd = &cobra.Command{
	Use:   "put <key> [value]",
	Short: "Put a value in the cache (reads from stdin if value not provided)",
	Long: `Put a value in the cache. The value can be provided as an argument or via stdin.
Examples:
  # Provide value as argument
  ocachecli put mykey "my value"
  
  # Read value from stdin
  echo "my value" | ocachecli put mykey
  cat file.txt | ocachecli put mykey`,
	Args: cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		c := newClient()
		defer c.Close()

		var err error

		if len(args) == 2 {
			// Value provided as argument - use regular Put for small values
			err = c.Put(ctx, args[0], []byte(args[1]), ttl)
		} else {
			// Read value from stdin - use streaming to avoid loading all into memory
			err = c.PutStream(ctx, args[0], os.Stdin, ttl)
		}

		if err != nil {
			if err == context.Canceled {
				fmt.Fprintf(os.Stderr, "Put cancelled\n")
			} else {
				fmt.Fprintf(os.Stderr, "Put failed: %v\n", err)
			}
			os.Exit(1)
		}
		fmt.Println("OK")
	},
}

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Get a value from the cache",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		c := newClient()
		defer c.Close()
		// Use streaming to output directly to stdout without loading into memory
		err := c.GetStream(ctx, args[0], os.Stdout)
		if err != nil {
			if err == context.Canceled {
				fmt.Fprintf(os.Stderr, "Get cancelled\n")
			} else {
				fmt.Fprintf(os.Stderr, "Get failed: %v\n", err)
			}
			os.Exit(1)
		}
		// Note: No need to print newline as data is written directly to stdout
	},
}

var delCmd = &cobra.Command{
	Use:     "del <key>",
	Aliases: []string{"delete"},
	Short:   "Delete a key from the cache",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		c := newClient()
		defer c.Close()
		err := c.Delete(ctx, args[0])
		if err != nil {
			if err == context.Canceled {
				fmt.Fprintf(os.Stderr, "Delete cancelled\n")
			} else {
				fmt.Fprintf(os.Stderr, "Delete failed: %v\n", err)
			}
			os.Exit(1)
		}
		fmt.Println("OK")
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all keys in the cache",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		c := newClient()
		defer c.Close()
		keys, err := c.List(ctx, listPrefix)
		if err != nil {
			if err == context.Canceled {
				fmt.Fprintf(os.Stderr, "List cancelled\n")
			} else {
				fmt.Fprintf(os.Stderr, "List failed: %v\n", err)
			}
			os.Exit(1)
		}
		for _, k := range keys {
			fmt.Println(k)
		}
	},
}

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run a YCSB-style benchmark against the cache service",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		cfg := ycsb.YCSBConfig{
			Addr:               addr,
			ConnMode:           connMode,
			TopologyRefresh:    topologyRefresh,
			ConnectionPoolSize: connectionPoolSize,
			NumKeys:            numKeys,
			ValueSize:          valueSize,
			NumOps:             numOps,
			Concurrency:        concurrency,
			Workload:           workload,
			Seed:               seed,
			NoProgress:         noProgress,
			ForceStreaming:     forceStreaming,
		}
		_, err := ycsb.RunYCSBWithContext(ctx, cfg)
		if err != nil {
			if err == context.Canceled {
				fmt.Fprintf(os.Stderr, "Benchmark cancelled\n")
			} else {
				fmt.Fprintf(os.Stderr, "Benchmark failed: %v\n", err)
			}
			os.Exit(1)
		}
	},
}

// cluster command and subcommands
var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster management commands",
	Long: `Commands for inspecting cluster topology and key ownership.
These commands only work when connected to a cluster-enabled ocache server.`,
}

var clusterTopologyCmd = &cobra.Command{
	Use:   "topology",
	Short: "Display full cluster topology (nodes + ring config)",
	Run: func(cmd *cobra.Command, args []string) {
		c := newClient()
		defer c.Close()

		if !c.IsClusterMode() {
			fmt.Fprintln(os.Stderr, "Error: cluster commands require cluster mode. Connected in simple mode.")
			os.Exit(1)
		}

		topology, err := c.FetchTopology()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to fetch topology: %v\n", err)
			os.Exit(1)
		}

		if jsonOutput {
			printJSON(topology)
		} else {
			printClusterTopology(topology)
		}
	},
}

var clusterNodeCmd = &cobra.Command{
	Use:   "node <key>",
	Short: "Get the node that owns a specific key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		c := newClient()
		defer c.Close()

		if !c.IsClusterMode() {
			fmt.Fprintln(os.Stderr, "Error: cluster commands require cluster mode. Connected in simple mode.")
			os.Exit(1)
		}

		key := args[0]
		nodeID, address, err := c.GetNodeInfoForKey(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get node for key: %v\n", err)
			os.Exit(1)
		}

		if jsonOutput {
			printJSON(map[string]string{"key": key, "node_id": nodeID, "address": address})
		} else {
			fmt.Printf("Key: %s\nNode: %s\nAddress: %s\n", key, nodeID, address)
		}
	},
}

var clusterEpochCmd = &cobra.Command{
	Use:   "epoch",
	Short: "Display the current topology epoch",
	Run: func(cmd *cobra.Command, args []string) {
		c := newClient()
		defer c.Close()

		if !c.IsClusterMode() {
			fmt.Fprintln(os.Stderr, "Error: cluster commands require cluster mode. Connected in simple mode.")
			os.Exit(1)
		}

		epoch := c.GetTopologyEpoch()
		if jsonOutput {
			printJSON(map[string]uint64{"epoch": epoch})
		} else {
			fmt.Printf("Epoch: %d\n", epoch)
		}
	},
}

// Helper functions for cluster command output

func printJSON(v interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func printClusterTopology(topology *clusterpb.ClusterTopology) {
	fmt.Printf("Cluster Topology (Epoch: %d)\n", topology.Epoch)
	fmt.Println()

	// Ring config
	if topology.RingConfig != nil {
		fmt.Println("Ring Configuration:")
		fmt.Printf("  Replication Factor: %d\n", topology.RingConfig.ReplicationFactor)
		if len(topology.RingConfig.NodeTokens) > 0 {
			totalTokens := 0
			for _, nt := range topology.RingConfig.NodeTokens {
				totalTokens += len(nt.Tokens)
			}
			fmt.Printf("  Total Tokens: %d\n", totalTokens)
		}
		fmt.Println()
	}

	// Nodes
	fmt.Println("Nodes:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NODE ID\tSTATUS\tLISTEN ADDRESS\tCLUSTER ADDRESS\tJOINED AT")
	fmt.Fprintln(w, "-------\t------\t--------------\t---------------\t---------")

	for _, node := range topology.Nodes {
		joinedAt := time.Unix(int64(node.JoinedAt), 0).Format(time.RFC3339)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			node.Id,
			nodeStatusString(node.Status),
			node.ListenAddress,
			node.Address,
			joinedAt,
		)
	}
	_ = w.Flush()
}

func nodeStatusString(status clusterpb.NodeStatus) string {
	switch status {
	case clusterpb.NodeStatus_NODE_STATUS_ACTIVE:
		return "ACTIVE"
	case clusterpb.NodeStatus_NODE_STATUS_JOINING:
		return "JOINING"
	case clusterpb.NodeStatus_NODE_STATUS_LEAVING:
		return "LEAVING"
	case clusterpb.NodeStatus_NODE_STATUS_DOWN:
		return "DOWN"
	default:
		return "UNKNOWN"
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&addr, "addr", "localhost:9000", "Cache server address (comma-separated for multiple servers)")
	rootCmd.PersistentFlags().StringVar(&connMode, "mode", "auto", "Connection mode: auto, simple, or cluster")
	rootCmd.PersistentFlags().DurationVar(&topologyRefresh, "topology-refresh", 30*time.Second, "Topology refresh interval (cluster mode only)")
	putCmd.Flags().Int64Var(&ttl, "ttl", 0, "TTL for the key in seconds (0 = no expiry)")
	listCmd.Flags().StringVar(&listPrefix, "prefix", "", "Optional prefix to filter keys")
	benchCmd.Flags().IntVar(&connectionPoolSize, "connection-pool-size", 4, "Number of connections per cache server")
	benchCmd.Flags().IntVar(&numKeys, "num-keys", 1000, "Number of unique keys")
	benchCmd.Flags().IntVar(&valueSize, "value-size", 100, "Value size in bytes")
	benchCmd.Flags().IntVar(&numOps, "num-ops", 10000, "Total number of operations")
	benchCmd.Flags().IntVar(&concurrency, "concurrency", 8, "Number of concurrent workers")
	benchCmd.Flags().StringVar(&workload, "workload", "A", "Workload type or custom mix (e.g. A, B, read=70,update=30)")
	benchCmd.Flags().Int64Var(&seed, "seed", time.Now().UnixNano(), "Random seed")
	benchCmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress output during benchmark")
	benchCmd.Flags().BoolVar(&forceStreaming, "force-streaming", false, "Force streaming for all operations regardless of size")

	// cluster command flags and subcommands
	clusterCmd.PersistentFlags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	clusterCmd.AddCommand(clusterTopologyCmd, clusterNodeCmd, clusterEpochCmd)
}
