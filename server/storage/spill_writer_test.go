package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSpillWriter_SmallBuffer(t *testing.T) {
	dir := t.TempDir()
	sw := newSpillWriter(1024, dir, "key1")
	defer sw.Close()
	data := []byte("hello world")
	n, err := sw.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n, "bytes written")
	assert.False(t, sw.UsedFile(), "should not use file for small buffer")
	assert.Equal(t, string(data), string(sw.Buffer()), "buffer mismatch")
}

func TestSpillWriter_LargeBuffer(t *testing.T) {
	dir := t.TempDir()
	threshold := 8
	sw := newSpillWriter(threshold, dir, "key2")
	defer sw.Close()
	data := []byte("this is a long string that exceeds threshold")
	n, err := sw.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n, "bytes written")
	assert.True(t, sw.UsedFile(), "should use file for large buffer")
	path := sw.FilePath()
	assert.NotEmpty(t, path, "file path should not be empty")
	b, err := os.ReadFile(path)
	assert.NoError(t, err, "failed to read spilled file")
	assert.Equal(t, string(data), string(b), "file content mismatch")
}

func TestShardPath(t *testing.T) {
	base := "/tmp/test"
	key := "mykey"
	shard := shardPath(base, key)
	assert.Equal(t, base, filepath.Dir(filepath.Dir(filepath.Dir(shard))), "shardPath did not return correct base")
}
