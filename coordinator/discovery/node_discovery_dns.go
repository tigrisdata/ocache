package discovery

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	zlog "github.com/rs/zerolog/log"
)

const (
	// DNSCacheExpiry is how long to use cached results if DNS fails
	DNSCacheExpiry = 24 * time.Hour
)

// DNSNodeDiscovery provides DNS-based node discovery with periodic refresh
type DNSNodeDiscovery struct {
	dnsName         string
	port            string
	refreshInterval time.Duration

	// Caching for resilience
	lastResolved    []string
	lastResolveTime time.Time
	mu              sync.RWMutex
}

// NewDNSNodeDiscovery creates a new DNS node discovery
func NewDNSNodeDiscovery(dnsName, port string, refreshInterval time.Duration) (*DNSNodeDiscovery, error) {
	// Validate DNS name
	if dnsName == "" {
		return nil, fmt.Errorf("DNS name cannot be empty")
	}

	if !isDNSName(dnsName) {
		return nil, fmt.Errorf("invalid DNS name: %s", dnsName)
	}

	// Validate port
	if _, err := validatePort(port); err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	// Set default refresh interval if not provided
	if refreshInterval <= 0 {
		refreshInterval = DefaultDNSRefreshInterval
	}

	discovery := &DNSNodeDiscovery{
		dnsName:         dnsName,
		port:            port,
		refreshInterval: refreshInterval,
	}

	zlog.Info().
		Str("dns_name", dnsName).
		Str("port", port).
		Dur("refresh_interval", refreshInterval).
		Msg("DNS node discovery initialized")

	return discovery, nil
}

// Resolve returns the current list of node addresses from DNS
func (d *DNSNodeDiscovery) Resolve(ctx context.Context) ([]string, error) {
	addresses, err := resolveDNSWithContext(ctx, d.dnsName, d.port)
	if err != nil {
		// If resolution fails, use cached results if they're not too old
		d.mu.RLock()
		cached := d.lastResolved
		cacheAge := time.Since(d.lastResolveTime)
		d.mu.RUnlock()

		if len(cached) > 0 && cacheAge < DNSCacheExpiry {
			zlog.Warn().
				Err(err).
				Str("dns_name", d.dnsName).
				Dur("cache_age", cacheAge).
				Int("cached_count", len(cached)).
				Msg("DNS resolution failed, using cached nodes")
			return cached, nil
		}

		return nil, err
	}

	zlog.Debug().
		Str("dns_name", d.dnsName).
		Int("address_count", len(addresses)).
		Strs("addresses", addresses).
		Msg("DNS resolved successfully")

	// Update cache with successful resolution
	d.mu.Lock()
	d.lastResolved = addresses
	d.lastResolveTime = time.Now()
	d.mu.Unlock()

	return addresses, nil
}

// Mode returns the discovery mode type
func (d *DNSNodeDiscovery) Mode() NodeDiscoveryMode {
	return NodeDiscoveryDNS
}

// NeedsRefresh returns true since DNS nodes need periodic refresh
func (d *DNSNodeDiscovery) NeedsRefresh() bool {
	return true
}

// RefreshInterval returns how often to refresh DNS
func (d *DNSNodeDiscovery) RefreshInterval() time.Duration {
	return d.refreshInterval
}

// String returns a string representation for logging
func (d *DNSNodeDiscovery) String() string {
	d.mu.RLock()
	nodeCount := len(d.lastResolved)
	d.mu.RUnlock()

	return fmt.Sprintf("DNSNodeDiscovery{dns=%s:%s, interval=%s, nodes=%d}",
		d.dnsName, d.port, d.refreshInterval, nodeCount)
}

// resolveDNSWithContext is a context-aware version of resolveDNS
func resolveDNSWithContext(ctx context.Context, dnsName string, port string) ([]string, error) {
	// Create a channel for the result
	type result struct {
		addresses []string
		err       error
	}

	resultCh := make(chan result, 1)

	// Run DNS resolution in a goroutine
	go func() {
		addresses, err := resolveDNS(dnsName, port)
		resultCh <- result{addresses, err}
	}()

	// Wait for result or context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-resultCh:
		return r.addresses, r.err
	}
}

// resolveDNS resolves a DNS name to a list of addresses with a default port
func resolveDNS(dnsName string, port string) ([]string, error) {
	if port == "" {
		return nil, fmt.Errorf("port cannot be empty")
	}

	// First try SRV records (Kubernetes headless services often use these)
	_, srvRecords, err := net.LookupSRV("", "", dnsName)
	if err == nil && len(srvRecords) > 0 {
		addresses := make([]string, 0, len(srvRecords))
		for _, srv := range srvRecords {
			host := strings.TrimSuffix(srv.Target, ".")
			addr := fmt.Sprintf("%s:%d", host, srv.Port)
			addresses = append(addresses, addr)

			zlog.Debug().
				Str("host", host).
				Uint16("port", srv.Port).
				Uint16("priority", srv.Priority).
				Uint16("weight", srv.Weight).
				Msg("Discovered node from SRV record")
		}
		return addresses, nil
	}

	// Fall back to A/AAAA records
	ips, err := net.LookupIP(dnsName)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve DNS name %s: %w", dnsName, err)
	}

	if len(ips) == 0 {
		return nil, fmt.Errorf("DNS name %s resolved to no IP addresses", dnsName)
	}

	addresses := make([]string, 0, len(ips))
	for _, ip := range ips {
		var addr string
		if ip.To4() == nil {
			// IPv6 address
			addr = fmt.Sprintf("[%s]:%s", ip.String(), port)
		} else {
			// IPv4 address
			addr = fmt.Sprintf("%s:%s", ip.String(), port)
		}
		addresses = append(addresses, addr)

		zlog.Debug().
			Str("ip", ip.String()).
			Str("address", addr).
			Msg("Discovered node from A/AAAA record")
	}

	return addresses, nil
}
