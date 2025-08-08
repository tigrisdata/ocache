package bufferpool

import (
	"testing"
)

func TestGetBuffer(t *testing.T) {
	buf := getBuffer()
	if len(buf) != defaultBufferSize {
		t.Errorf("getBuffer returned buffer with length %d, want %d", len(buf), defaultBufferSize)
	}
	if cap(buf) < defaultBufferSize {
		t.Errorf("getBuffer returned buffer with capacity %d, want at least %d", cap(buf), defaultBufferSize)
	}
}

func TestPutBuffer(t *testing.T) {
	buf := make([]byte, defaultBufferSize)
	putBuffer(buf)
}

func TestAcquireBuffer_SmallSize(t *testing.T) {
	size := 1024
	buf, release := AcquireBuffer(size)
	defer release()

	if len(buf) != size {
		t.Errorf("AcquireBuffer returned buffer with length %d, want %d", len(buf), size)
	}
	if cap(buf) < size {
		t.Errorf("AcquireBuffer returned buffer with capacity %d, want at least %d", cap(buf), size)
	}
}

func TestAcquireBuffer_DefaultSize(t *testing.T) {
	size := defaultBufferSize
	buf, release := AcquireBuffer(size)
	defer release()

	if len(buf) != size {
		t.Errorf("AcquireBuffer returned buffer with length %d, want %d", len(buf), size)
	}
	if cap(buf) < size {
		t.Errorf("AcquireBuffer returned buffer with capacity %d, want at least %d", cap(buf), size)
	}
}

func TestAcquireBuffer_LargeSize(t *testing.T) {
	size := defaultBufferSize * 2
	buf, release := AcquireBuffer(size)
	defer release()

	if len(buf) != size {
		t.Errorf("AcquireBuffer returned buffer with length %d, want %d", len(buf), size)
	}
	if cap(buf) < size {
		t.Errorf("AcquireBuffer returned buffer with capacity %d, want at least %d", cap(buf), size)
	}
}

func TestAcquireBuffer_ReuseFromPool(t *testing.T) {
	size := 1024

	buf1, release1 := AcquireBuffer(size)
	buf1[0] = 42
	release1()

	buf2, release2 := AcquireBuffer(size)
	defer release2()

	if len(buf2) != size {
		t.Errorf("Second AcquireBuffer returned buffer with length %d, want %d", len(buf2), size)
	}
}

func TestAcquireBuffer_LargeBufferReuse(t *testing.T) {
	largeSize := defaultBufferSize * 3

	_, release1 := AcquireBuffer(largeSize)
	release1()

	buf2, release2 := AcquireBuffer(largeSize)
	defer release2()

	if len(buf2) != largeSize {
		t.Errorf("Reused large buffer has length %d, want %d", len(buf2), largeSize)
	}
	if cap(buf2) < largeSize {
		t.Errorf("Reused large buffer has capacity %d, want at least %d", cap(buf2), largeSize)
	}
}

func TestAcquireBuffer_MultipleReleases(t *testing.T) {
	const iterations = 100
	size := 1024

	for i := 0; i < iterations; i++ {
		buf, release := AcquireBuffer(size)
		if len(buf) != size {
			t.Fatalf("iteration %d: buffer length %d, want %d", i, len(buf), size)
		}
		release()
	}
}

func BenchmarkAcquireBufferSmall(b *testing.B) {
	size := 1024
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, release := AcquireBuffer(size)
		_ = buf
		release()
	}
}

func BenchmarkAcquireBufferDefault(b *testing.B) {
	size := defaultBufferSize
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, release := AcquireBuffer(size)
		_ = buf
		release()
	}
}

func BenchmarkAcquireBufferLarge(b *testing.B) {
	size := defaultBufferSize * 2
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, release := AcquireBuffer(size)
		_ = buf
		release()
	}
}
