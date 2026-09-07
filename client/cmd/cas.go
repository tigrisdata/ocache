// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	cacheclient "github.com/tigrisdata/ocache/client"
)

// countingDiscard counts bytes written and discards them — lets get-with-version
// report a value's length without buffering it (issue #258).
type countingDiscard struct{ n int64 }

func (c *countingDiscard) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// Conditional (compare-and-swap) CLI commands — issue #254. Output is
// machine-parseable (key=value tokens) and a lost race exits with code 3,
// distinct from a transport/usage error (exit 1), so scripts and the E2E
// suite can branch on the outcome.

const casMismatchExitCode = 3

var (
	expectedVersion uint64
	casTTL          int64
)

var putIfVersionCmd = &cobra.Command{
	Use:   "put-if-version <key> [value]",
	Short: "Conditionally put a value only if the key's version matches --expected (0 = put-if-absent)",
	Long: `Conditionally put a value. The value can be given as an argument (small values)
or streamed from stdin (large values, above the unary message cap):

  ocachecli put-if-version mykey "value" --expected 3
  cat big.bin | ocachecli put-if-version mykey --expected 3`,
	Args: cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		c := newClient()
		defer c.Close()

		var newVersion uint64
		var err error
		if len(args) == 2 {
			newVersion, err = c.PutIfVersion(ctx, args[0], []byte(args[1]), casTTL, expectedVersion)
		} else {
			// Stream the value from stdin — avoids buffering large objects.
			newVersion, err = c.PutStreamIfVersion(ctx, args[0], os.Stdin, casTTL, expectedVersion)
		}
		if err != nil {
			if vm, ok := cacheclient.IsVersionMismatch(err); ok {
				fmt.Printf("success=false current_version=%d\n", vm.CurrentVersion)
				os.Exit(casMismatchExitCode)
			}
			fmt.Fprintf(os.Stderr, "PutIfVersion failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("success=true new_version=%d\n", newVersion)
	},
}

var getWithVersionValue bool

var getWithVersionCmd = &cobra.Command{
	Use:   "get-with-version <key>",
	Short: "Get a key's current version and presence; with --value, stream the value to stdout",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		c := newClient()
		defer c.Close()

		// Stream so a large value is never buffered: to stdout with --value, else
		// to a counter that reports length only.
		var w io.Writer
		counter := &countingDiscard{}
		if getWithVersionValue {
			w = os.Stdout
		} else {
			w = counter
		}
		version, found, err := c.GetStreamWithVersion(ctx, args[0], w)
		if err != nil {
			fmt.Fprintf(os.Stderr, "GetWithVersion failed: %v\n", err)
			os.Exit(1)
		}
		// Metadata goes to stderr so --value keeps stdout clean for the payload.
		fmt.Fprintf(os.Stderr, "version=%d\n", version)
		fmt.Fprintf(os.Stderr, "found=%t\n", found)
		if !getWithVersionValue {
			fmt.Printf("version=%d\n", version)
			fmt.Printf("found=%t\n", found)
			if found {
				fmt.Printf("length=%d\n", counter.n)
			}
		}
	},
}

var deleteIfVersionCmd = &cobra.Command{
	Use:     "delete-if-version <key>",
	Aliases: []string{"del-if-version"},
	Short:   "Conditionally delete a key only if its version matches --expected",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		c := newClient()
		defer c.Close()

		err := c.DeleteIfVersion(ctx, args[0], expectedVersion)
		if err != nil {
			if vm, ok := cacheclient.IsVersionMismatch(err); ok {
				fmt.Printf("success=false current_version=%d\n", vm.CurrentVersion)
				os.Exit(casMismatchExitCode)
			}
			fmt.Fprintf(os.Stderr, "DeleteIfVersion failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("success=true\n")
	},
}

func init() {
	putIfVersionCmd.Flags().Uint64Var(&expectedVersion, "expected", 0, "Expected current version (0 = put-if-absent)")
	putIfVersionCmd.Flags().Int64Var(&casTTL, "ttl", 0, "TTL for the key in seconds (0 = no expiry)")
	getWithVersionCmd.Flags().BoolVar(&getWithVersionValue, "value", false, "Stream the value to stdout (metadata goes to stderr)")
	deleteIfVersionCmd.Flags().Uint64Var(&expectedVersion, "expected", 0, "Expected current version")

	rootCmd.AddCommand(putIfVersionCmd, getWithVersionCmd, deleteIfVersionCmd)
}
