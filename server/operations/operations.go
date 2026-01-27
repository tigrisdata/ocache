// Package operations provides cache operations with automatic routing.
// This package is used by both the gRPC service layer and the embedded client.
package operations

import (
	"github.com/tigrisdata/ocache/coordinator"
	"github.com/tigrisdata/ocache/coordinator/ring"
	pb "github.com/tigrisdata/ocache/proto"
	stor "github.com/tigrisdata/ocache/storage"
)

// Operations provides cache operations with automatic routing.
// For local keys, it accesses storage directly.
// For remote keys, it routes via gRPC to the appropriate node.
type Operations struct {
	storage     *stor.Storage
	coordinator *coordinator.Coordinator
}

// New creates a new Operations instance.
func New(storage *stor.Storage, coord *coordinator.Coordinator) *Operations {
	return &Operations{
		storage:     storage,
		coordinator: coord,
	}
}

// IsLocal checks if a key belongs to the local node.
// Returns true if clustering is disabled or if the key is owned by this node.
func (o *Operations) IsLocal(key string) bool {
	if o.coordinator == nil {
		return true
	}
	return o.coordinator.IsLocal(key)
}

// Route returns a gRPC client for the node that owns the given key.
// Returns an error if the key is local (caller should check IsLocal first).
func (o *Operations) Route(key string) (pb.CacheServiceClient, error) {
	if o.coordinator == nil {
		return nil, nil
	}
	return o.coordinator.Route(key)
}

// IsClusterMode returns true if clustering is enabled.
func (o *Operations) IsClusterMode() bool {
	return o.coordinator != nil
}

// GetStorage returns the underlying storage.
// This is primarily for testing and advanced use cases.
func (o *Operations) GetStorage() *stor.Storage {
	return o.storage
}

// GetCoordinator returns the underlying coordinator.
// This is primarily for testing and advanced use cases.
func (o *Operations) GetCoordinator() *coordinator.Coordinator {
	return o.coordinator
}

// GetLocalNodeID returns the local node's ID.
// Returns empty string if clustering is disabled.
func (o *Operations) GetLocalNodeID() string {
	if o.coordinator == nil {
		return ""
	}
	return o.coordinator.GetLocalNodeID()
}

// GetRing returns the ring manager for cluster topology.
// Returns nil if clustering is disabled.
func (o *Operations) GetRing() *ring.RingManager {
	if o.coordinator == nil {
		return nil
	}
	return o.coordinator.GetRing()
}

// GetRouter returns the router for getting remote clients.
// Returns nil if clustering is disabled.
func (o *Operations) GetRouter() *coordinator.Router {
	if o.coordinator == nil {
		return nil
	}
	return o.coordinator.GetRouter()
}

// IsReady returns true if the operations layer is ready to serve requests.
// In cluster mode, this checks if the coordinator is ready.
func (o *Operations) IsReady() bool {
	if o.coordinator == nil {
		return true
	}
	return o.coordinator.IsReady()
}
