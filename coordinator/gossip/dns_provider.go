package gossip

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/go-kit/log"
	zlog "github.com/rs/zerolog/log"
)

// simpleDNSProvider implements the DNS provider interface for memberlist.
// It resolves DNS names to IP addresses, supporting both static IPs and
// DNS names (e.g., Kubernetes headless services).
type simpleDNSProvider struct {
	// seedAddrs are the original addresses to resolve (may be DNS names or IPs)
	seedAddrs []string
	// resolvedAddrs are the resolved IP addresses
	resolvedAddrs []string
	// logger for DNS resolution errors
	logger log.Logger
	// mu protects resolvedAddrs
	mu sync.RWMutex
}

// newSimpleDNSProvider creates a new DNS provider with the given seed addresses
func newSimpleDNSProvider(seedAddrs []string, logger log.Logger) *simpleDNSProvider {
	return &simpleDNSProvider{
		seedAddrs: seedAddrs,
		logger:    logger,
	}
}

// Resolve resolves DNS names to IP addresses.
// Called by memberlist before attempting to join the cluster.
// Returns an aggregated error if all addresses fail to resolve.
func (p *simpleDNSProvider) Resolve(ctx context.Context, addrs []string) error {
	var resolved []string
	var resolveErrors []error

	zlog.Info().Strs("addrs", addrs).Msg("Resolving DNS addresses")

	for _, addr := range addrs {
		// Parse host and port
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			// No port specified, use the address as-is for lookup
			host = addr
			port = ""
		}

		// Check if the host is already an IP address
		if ip := net.ParseIP(host); ip != nil {
			// Already an IP address, no DNS resolution needed
			resolved = append(resolved, addr)
			zlog.Debug().Str("addr", host).Msg("Address is already an IP")
			continue
		}

		// Try to resolve as DNS name
		ips, err := net.DefaultResolver.LookupHost(ctx, host)
		if err != nil {
			// DNS lookup failed for a hostname - this is a real error that should be logged
			// prominently as it may indicate misconfiguration or network issues
			var dnsErr *net.DNSError
			if errors.As(err, &dnsErr) {
				if dnsErr.IsNotFound {
					// DNS name doesn't exist - configuration error
					zlog.Warn().Str("host", host).Err(err).Msg("DNS name not found")
				} else if dnsErr.IsTemporary {
					// Temporary DNS failure - will retry on next resolve
					zlog.Warn().Str("host", host).Err(err).Msg("temporary DNS resolution failure")
				} else {
					// Other DNS error (timeout, server failure, etc.)
					zlog.Error().Str("host", host).Err(err).Msg("DNS resolution failed")
				}
			} else {
				// Non-DNS error during resolution
				zlog.Error().Str("host", host).Err(err).Msg("failed to resolve address")
			}
			// Collect errors for aggregation
			resolveErrors = append(resolveErrors, fmt.Errorf("%s: %w", host, err))
			// Skip this address - don't add unresolvable hostnames
			continue
		}

		// Add all resolved IPs
		for _, ip := range ips {
			if port != "" {
				resolved = append(resolved, net.JoinHostPort(ip, port))
			} else {
				resolved = append(resolved, ip)
			}
		}

		zlog.Debug().Str("host", host).Strs("ips", ips).Msg("resolved DNS name")
	}

	p.mu.Lock()
	p.resolvedAddrs = resolved
	p.mu.Unlock()

	zlog.Info().Strs("resolved", resolved).Msg("DNS addresses resolved")

	// If all addresses failed to resolve, return an aggregated error.
	// Partial success is acceptable - memberlist can work with a subset of seeds.
	if len(resolved) == 0 && len(resolveErrors) > 0 {
		return fmt.Errorf("all DNS resolutions failed: %w", errors.Join(resolveErrors...))
	}

	return nil
}

// Addresses returns the most recently resolved addresses.
// Called by memberlist to get the list of addresses to join.
func (p *simpleDNSProvider) Addresses() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// If we haven't resolved yet, return the seed addresses
	if len(p.resolvedAddrs) == 0 {
		return p.seedAddrs
	}

	// Return a copy to avoid races
	result := make([]string, len(p.resolvedAddrs))
	copy(result, p.resolvedAddrs)
	return result
}
