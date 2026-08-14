package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPut_Overwrite_TotalSizeStaysExact reproduces the accounting leak where a
// Put on an existing key added the new value's bytes to the cleaner's totalSize
// without subtracting the row it replaced. The tracked total inflated with every
// overwrite, so enforceDiskLimit eventually chased bytes that did not exist and
// evicted the whole cache. Both the inline and the raw-file path leaked.
func TestPut_Overwrite_TotalSizeStaysExact(t *testing.T) {
	// maxDiskUsage > 0 so putLow also writes the LRU access index entries.
	s, cleanup := createTestStorage(t, 3600, 1024, 4*1024, 16*1024*1024, 1000, 1<<30)
	defer cleanup()

	inline := bytes.Repeat([]byte("x"), 1000)    // <= inlineThreshold  → INLINE
	rawFile := bytes.Repeat([]byte("y"), 8*1024) // >  compactThreshold → RAW_FILE

	for range 10 {
		require.NoError(t, s.Put("inline-key", bytes.NewReader(inline), 0))
		require.NoError(t, s.Put("rawfile-key", bytes.NewReader(rawFile), 0))
	}

	want := int64(len(inline) + len(rawFile))
	require.Equal(t, want, s.cleaner.totalSize.Load(),
		"overwrites must not accumulate the replaced values' bytes")

	// The metadata rescan is the source of truth (it is what a restart does);
	// the running total must be a fixpoint against it.
	s.cleaner.calculateTotalSize()
	require.Equal(t, want, s.cleaner.totalSize.Load(), "running total must agree with a full rescan")
}
