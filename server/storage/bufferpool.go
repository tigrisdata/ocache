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

	// A second pool for larger buffers (size > defaultBufferSize). Instead of keeping
	// many pools keyed by size, we keep a single pool that stores the
	// largest buffer we have seen so far.  This keeps memory usage bounded
	// while still avoiding an allocation on every call.
	// The slices taken from this pool always have their capacity preserved; callers
	// may reslice to their desired length.
	largeBufferPool = sync.Pool{}
)

// GetBuffer returns a buffer of defaultBufferSize length from the pool, or allocates a new one if needed
func GetBuffer() []byte {
	buf := bufferPool.Get().([]byte)
	if cap(buf) < defaultBufferSize {
		return make([]byte, defaultBufferSize)
	}
	return buf[:defaultBufferSize]
}

// GetSizedBuffer returns a buffer whose capacity is at least `size` bytes. It reuses
// pooled buffers where possible to reduce allocations. The returned slice has length
// equal to `size` (callers can reslice as needed).
func GetSizedBuffer(size int) []byte {
	if size <= defaultBufferSize {
		// Use the regular small-buffer pool
		buf := GetBuffer()
		return buf[:size]
	}

	// Try to get a large buffer from the pool
	if v := largeBufferPool.Get(); v != nil {
		buf := v.([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
		// Buffer is too small; let it be GC'ed and fall through to allocate
	}
	return make([]byte, size)
}

// PutSizedBuffer returns a buffer obtained from GetSizedBuffer back to the appropriate pool.
// Callers should pass the full slice (not a subslice) to avoid retaining references
// to large underlying arrays.
func PutSizedBuffer(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) <= defaultBufferSize {
		PutBuffer(buf[:defaultBufferSize])
		return
	}
	largeBufferPool.Put(buf[:cap(buf)])
}

// PutBuffer returns a buffer to the pool
func PutBuffer(buf []byte) {
	bufferPool.Put(buf)
}
