package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	cacheclient "github.com/tigrisdata/ocache/client"
	ycsb "github.com/tigrisdata/ocache/client/cmd/ycsb"
)

var rootCmd = &cobra.Command{
	Use:   "cachecli",
	Short: "CLI for interacting with the cache service",
}

func main() {
	rootCmd.AddCommand(putCmd, getCmd, delCmd, listCmd, benchCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// flags for the CLI
var (
	addr string
	ttl  int64

	numKeys     int
	valueSize   int
	numOps      int
	concurrency int
	workload    string
	seed        int64
	noProgress  bool
)

func newClient() *cacheclient.Client {
	c, err := cacheclient.New(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	return c
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
		c := newClient()
		defer c.Close()

		var err error

		if len(args) == 2 {
			// Value provided as argument - use regular Put for small values
			err = c.Put(context.Background(), args[0], []byte(args[1]), ttl)
		} else {
			// Read value from stdin - use streaming to avoid loading all into memory
			err = c.PutStream(context.Background(), args[0], os.Stdin, ttl)
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "Put failed: %v\n", err)
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
		c := newClient()
		defer c.Close()
		// Use streaming to output directly to stdout without loading into memory
		err := c.GetStream(context.Background(), args[0], os.Stdout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Get failed: %v\n", err)
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
		c := newClient()
		defer c.Close()
		err := c.Delete(context.Background(), args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Delete failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all keys in the cache",
	Run: func(cmd *cobra.Command, args []string) {
		c := newClient()
		defer c.Close()
		keys, err := c.List(context.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "List failed: %v\n", err)
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
		cfg := ycsb.YCSBConfig{
			Addr:        addr,
			NumKeys:     numKeys,
			ValueSize:   valueSize,
			NumOps:      numOps,
			Concurrency: concurrency,
			Workload:    workload,
			Seed:        seed,
			NoProgress:  noProgress,
		}
		_, err := ycsb.RunYCSB(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Benchmark failed: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&addr, "addr", "localhost:9000", "Cache server address")
	putCmd.Flags().Int64Var(&ttl, "ttl", 0, "TTL for the key in seconds (0 = no expiry)")
	benchCmd.Flags().IntVar(&numKeys, "num-keys", 1000, "Number of unique keys")
	benchCmd.Flags().IntVar(&valueSize, "value-size", 100, "Value size in bytes")
	benchCmd.Flags().IntVar(&numOps, "num-ops", 10000, "Total number of operations")
	benchCmd.Flags().IntVar(&concurrency, "concurrency", 8, "Number of concurrent workers")
	benchCmd.Flags().StringVar(&workload, "workload", "A", "Workload type or custom mix (e.g. A, B, read=70,update=30)")
	benchCmd.Flags().Int64Var(&seed, "seed", time.Now().UnixNano(), "Random seed")
	benchCmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress output during benchmark")
}
