package discovery

import (
	"context"
	"time"
)

// NodeDiscoveryMode defines how seeds are discovered
type NodeDiscoveryMode string

const (
	// NodeDiscovery modes
	NodeDiscoveryStatic NodeDiscoveryMode = "static" // NodeDiscoveryStatic uses a static list of seed addresses
	NodeDiscoveryDNS    NodeDiscoveryMode = "dns"    // NodeDiscoveryDNS uses DNS to discover seed nodes (e.g., Kubernetes headless service)

	// DefaultDNSRefreshInterval is the default interval for DNS refresh
	DefaultDNSRefreshInterval = 30 * time.Second
)

// NodeDiscovery provides an interface for discovering cluster nodes
type NodeDiscovery interface {
	// Resolve returns the current list of node addresses
	Resolve(ctx context.Context) ([]string, error)

	// Mode returns the discovery mode type
	Mode() NodeDiscoveryMode

	// NeedsRefresh returns true if this discovery method should be refreshed periodically
	NeedsRefresh() bool

	// RefreshInterval returns how often to refresh (only used if NeedsRefresh returns true)
	RefreshInterval() time.Duration

	// String returns a string representation for logging
	String() string
}

// CreateNodeDiscovery creates the appropriate NodeDiscovery implementation based on config
func CreateNodeDiscovery(nodes []string, dnsRefreshInterval time.Duration) (NodeDiscovery, error) {
	if len(nodes) == 0 {
		// No nodes - return empty static discovery
		return NewStaticNodeDiscovery([]string{})
	}

	// Check if it's a single node that might be DNS
	if len(nodes) == 1 {
		node := nodes[0]

		// Try to parse as address
		host, port, err := splitAddress(node)
		if err != nil {
			_, err := validatePort(port)
			if err != nil {
				return nil, err
			}

			// If it's a DNS name, use DNS discovery
			if isDNSName(node) {
				// Use DNS discovery with default port
				return NewDNSNodeDiscovery(host, port, dnsRefreshInterval)
			}
			return nil, err
		}
	}

	// Use static discovery for multiple nodes or single static node
	return NewStaticNodeDiscovery(nodes)
}

// DiffNodes compares two node lists and returns added/removed nodes
func DiffNodes(old, new []string) (added, removed []string) {
	oldMap := make(map[string]bool)
	for _, s := range old {
		oldMap[s] = true
	}

	newMap := make(map[string]bool)
	for _, s := range new {
		newMap[s] = true
	}

	for s := range newMap {
		if !oldMap[s] {
			added = append(added, s)
		}
	}

	for s := range oldMap {
		if !newMap[s] {
			removed = append(removed, s)
		}
	}

	return added, removed
}
