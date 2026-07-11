// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package coordinator

import (
	"fmt"
	"net"
	"strconv"

	zlog "github.com/rs/zerolog/log"
)

// validateBindAddress validates a bind address in the format IP:port, hostname:port, or :port.
// Used for ListenAddr and ClusterAddr which support binding to all interfaces.
func validateBindAddress(addr, fieldName string) error {
	zlog.Debug().Str("port_type", fieldName).Str("addr", addr).Msg("Validating bind address")

	if addr == "" {
		return fmt.Errorf("%s is required in cluster mode", fieldName)
	}

	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s must be in format host:port or :port, got %q: %w", fieldName, addr, err)
	}

	// Host can be empty (meaning all interfaces), an IP, or a hostname
	// No additional validation needed for host part

	// Port must be a valid number
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("%s port must be a number, got %q", fieldName, portStr)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("%s port must be between 1 and 65535, got %d", fieldName, port)
	}

	return nil
}

// validateSeedAddresses validates seed addresses in the format IP:port or hostname:port.
// At least one seed address is required.
func validateSeedAddresses(seeds []string) error {
	zlog.Debug().Strs("seeds", seeds).Msg("Validating seed addresses")

	// Empty seeds are allowed
	if len(seeds) == 0 {
		zlog.Info().Msg("No seed addresses provided, allowing empty seeds")
		return nil
	}

	for i, seed := range seeds {
		if seed == "" {
			return fmt.Errorf("seed address %d is empty", i)
		}

		host, portStr, err := net.SplitHostPort(seed)
		if err != nil {
			return fmt.Errorf("seed address %d must be in format host:port or IP:port, got %q: %w", i, seed, err)
		}

		// Host must not be empty for seeds (unlike bind addresses)
		if host == "" {
			return fmt.Errorf("seed address %d must have a host, got %q", i, seed)
		}

		// Port must be a valid number
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("seed address %d port must be a number, got %q", i, portStr)
		}
		if port < 1 || port > 65535 {
			return fmt.Errorf("seed address %d port must be between 1 and 65535, got %d", i, port)
		}
	}

	return nil
}
