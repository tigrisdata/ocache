// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	cacheclient "github.com/tigrisdata/ocache/client"
)

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
	Use:   "put-if-version <key> <value>",
	Short: "Conditionally put a value only if the key's version matches --expected (0 = put-if-absent)",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		c := newClient()
		defer c.Close()

		newVersion, err := c.PutIfVersion(ctx, args[0], []byte(args[1]), casTTL, expectedVersion)
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

var getWithVersionCmd = &cobra.Command{
	Use:   "get-with-version <key>",
	Short: "Get a key's current version and presence (value via normal get)",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := createContext()
		defer cancel()

		c := newClient()
		defer c.Close()

		data, version, found, err := c.GetWithVersion(ctx, args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "GetWithVersion failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("version=%d\n", version)
		fmt.Printf("found=%t\n", found)
		if found {
			fmt.Printf("length=%d\n", len(data))
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
	deleteIfVersionCmd.Flags().Uint64Var(&expectedVersion, "expected", 0, "Expected current version")

	rootCmd.AddCommand(putIfVersionCmd, getWithVersionCmd, deleteIfVersionCmd)
}
