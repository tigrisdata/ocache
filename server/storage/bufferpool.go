package storage

import "sync"

// bufferPool is used to reduce allocations
var (
	defaultBufferSize = 64 * 1024 // 64KB default buffer size
	bufferPool        = sync.Pool{
		New: func() any {
			return make([]byte, 0, defaultBufferSize)
		},
	}
)

// GetBuffer returns a buffer of defaultBufferSize length from the pool, or allocates a new one if needed
func GetBuffer() []byte {
	buf := bufferPool.Get().([]byte)
	if cap(buf) < defaultBufferSize {
		return make([]byte, defaultBufferSize)
	}
	return buf[:defaultBufferSize]
}

// PutBuffer returns a buffer to the pool
func PutBuffer(buf []byte) {
	bufferPool.Put(buf)
}
