package bufferpool

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

// getBuffer returns a buffer of defaultBufferSize length from the pool, or allocates a new one if needed
func getBuffer() []byte {
	buf := bufferPool.Get().([]byte)
	if cap(buf) < defaultBufferSize {
		return make([]byte, defaultBufferSize)
	}
	return buf[:defaultBufferSize]
}

// PutBuffer returns a buffer to the pool
func putBuffer(buf []byte) {
	bufferPool.Put(buf)
}

// AcquireBuffer returns a buffer of at least `size` bytes from the pool, or
// allocates a new one if needed. The returned slice has length equal to `size`
// (callers can reslice as needed). The returned function must be called to
// release the buffer back to the pool.
func AcquireBuffer(size int) ([]byte, func()) {
	if size <= defaultBufferSize {
		buf := getBuffer()
		return buf[:size], func() { putBuffer(buf) }
	}

	if v := largeBufferPool.Get(); v != nil {
		buf := v.([]byte)
		if cap(buf) >= size {
			return buf[:size], func() { largeBufferPool.Put(buf) }
		}
	}

	buf := make([]byte, size)
	return buf, func() { putBuffer(buf) }
}
