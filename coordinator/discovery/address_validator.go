package discovery

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	zlog "github.com/rs/zerolog/log"
)

// ValidateClusterAddress validates a cluster listen address
// Valid cluster address can be:
// - IP address: `192.168.1.1:7000`
// - Hostname: `node1.cluster:7000`
// - All interfaces: `:7000`
func ValidateClusterAddress(addr string) error {
	host, portStr, err := splitAddress(addr)
	if err != nil {
		return fmt.Errorf("invalid cluster address: %w", err)
	}

	// Validate port
	_, err = validatePort(portStr)
	if err != nil {
		return fmt.Errorf("invalid port in cluster address: %w", err)
	}

	// Validate host (empty host is allowed for cluster address - means listen on all interfaces)
	if err := validateHost(host); err != nil {
		return fmt.Errorf("invalid host in cluster address: %w", err)
	}

	// Log debug info
	if host == "" {
		zlog.Debug().
			Str("address", addr).
			Msg("Cluster address will listen on all interfaces")
	} else if ip := net.ParseIP(host); ip != nil {
		zlog.Debug().
			Str("address", addr).
			Str("ip", ip.String()).
			Msg("Cluster address uses IP address")
	} else {
		zlog.Debug().
			Str("address", addr).
			Str("hostname", host).
			Msg("Cluster address uses hostname")
	}

	return nil
}

// ValidateNodeAddress validates a single node address
func ValidateNodeAddress(addr string) error {
	host, portStr, err := splitAddress(addr)
	if err != nil {
		return fmt.Errorf("invalid node address: %w", err)
	}

	// Validate port
	_, err = validatePort(portStr)
	if err != nil {
		return err
	}

	// Node addresses must have a host (unlike cluster addresses which can be ":port")
	if host == "" {
		return fmt.Errorf("host cannot be empty in node address")
	}

	// Validate host
	if err := validateHost(host); err != nil {
		return err
	}

	return nil
}

// validatePort validates a port number string and returns the integer value
func validatePort(portStr string) (int, error) {
	if portStr == "" {
		return 0, fmt.Errorf("port cannot be empty")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("invalid port number: %w", err)
	}

	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("port must be between 1 and 65535, got %d", port)
	}

	return port, nil
}

// validateHost validates a host string (IP or DNS name)
func validateHost(host string) error {
	if host == "" {
		return nil // Empty host means listen on all interfaces
	}

	// Check if it's a valid IP address
	if ip := net.ParseIP(host); ip != nil {
		return nil
	}

	// Check if it's a valid DNS name
	if isDNSName(host) {
		return nil
	}

	return fmt.Errorf("invalid host: must be valid IP address or DNS name")
}

// splitAddress splits an address into host and port components
// Returns empty strings if the address is invalid
func splitAddress(addr string) (host, port string, err error) {
	if addr == "" {
		return "", "", fmt.Errorf("address cannot be empty")
	}

	// Split host:port
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		// Check if it's missing port
		if !strings.Contains(addr, ":") {
			return addr, "", fmt.Errorf("address must include port (e.g., 'host:port' or ':port')")
		}
		return "", "", fmt.Errorf("invalid address format: %w", err)
	}

	return host, port, nil
}

// isDNSName checks if a string is a valid DNS name
func isDNSName(name string) bool {
	if name == "" {
		return false
	}

	// Check for invalid characters
	if strings.ContainsAny(name, " \t\r\n") {
		return false
	}

	// Simple DNS name validation
	// More complete validation would check label lengths, etc.
	parts := strings.Split(name, ".")
	for _, part := range parts {
		if part == "" {
			return false
		}
		// Check if part contains only valid DNS characters
		for _, ch := range part {
			if !((ch >= 'a' && ch <= 'z') ||
				(ch >= 'A' && ch <= 'Z') ||
				(ch >= '0' && ch <= '9') ||
				ch == '-' || ch == '_') {
				return false
			}
		}
		// DNS labels can't start or end with hyphen
		if strings.HasPrefix(part, "-") || strings.HasSuffix(part, "-") {
			return false
		}
	}

	return true
}
