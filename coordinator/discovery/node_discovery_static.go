package discovery

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// StaticNodeDiscovery provides static node addresses that don't change
type StaticNodeDiscovery struct {
	nodes []string
}

// NewStaticNodeDiscovery creates a new static node discovery with validated addresses
func NewStaticNodeDiscovery(nodes []string) (*StaticNodeDiscovery, error) {
	// Validate all node addresses
	validatedNodes := make([]string, 0, len(nodes))

	for _, node := range nodes {
		if node == "" {
			continue
		}

		// Validate the node address
		if err := ValidateNodeAddress(node); err != nil {
			return nil, fmt.Errorf("invalid node address %s: %w", node, err)
		}
		validatedNodes = append(validatedNodes, node)
	}

	return &StaticNodeDiscovery{
		nodes: validatedNodes,
	}, nil
}

// Resolve returns the static list of node addresses
func (s *StaticNodeDiscovery) Resolve(ctx context.Context) ([]string, error) {
	// Static nodes never change, just return them
	return s.nodes, nil
}

// Mode returns the discovery mode type
func (s *StaticNodeDiscovery) Mode() NodeDiscoveryMode {
	return NodeDiscoveryStatic
}

// NeedsRefresh returns false since static nodes never need refresh
func (s *StaticNodeDiscovery) NeedsRefresh() bool {
	return false
}

// RefreshInterval returns 0 since static nodes don't refresh
func (s *StaticNodeDiscovery) RefreshInterval() time.Duration {
	return 0
}

// String returns a string representation for logging
func (s *StaticNodeDiscovery) String() string {
	return fmt.Sprintf("StaticNodeDiscovery{nodes=%s}", strings.Join(s.nodes, ","))
}
