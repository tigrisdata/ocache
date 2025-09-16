package hash

import (
	"github.com/cespare/xxhash"
)

// Hasher implements the consistent.Hasher interface using xxhash
type Hasher struct{}

// Sum64 returns the xxhash of the given data
func (h Hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}
