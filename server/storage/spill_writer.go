package storage

import (
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"

	zlog "github.com/rs/zerolog/log"
)

// spillWriter buffers small items and spills to disk if threshold exceeded
// filePath holds the temporary file when used

type spillWriter struct {
	threshold int
	buffer    []byte
	file      *os.File
	filePath  string
	diskPath  string
}

func newSpillWriter(threshold int, diskPath string, key string) *spillWriter {
	// Create a sharded directory structure based on the key
	shardDir := shardPath(diskPath, key)

	buf, release := AcquireBuffer(1 << 20) // 1 MiB
	defer release()
	return &spillWriter{threshold: threshold, diskPath: shardDir, buffer: buf[:0]}
}

func (sw *spillWriter) Write(p []byte) (int, error) {
	if sw.file == nil {
		if len(sw.buffer)+len(p) <= sw.threshold {
			sw.buffer = append(sw.buffer, p...)
			return len(p), nil
		}
		// exceed threshold: create temp file
		os.MkdirAll(sw.diskPath, 0o755)
		f, err := os.CreateTemp(sw.diskPath, "cache_*")
		if err != nil {
			return 0, err
		}
		// write buffered and p
		if _, err := f.Write(sw.buffer); err != nil {
			f.Close()
			return 0, err
		}
		if _, err := f.Write(p); err != nil {
			f.Close()
			return 0, err
		}
		sw.file = f
		sw.filePath = f.Name()
		// drop buffer
		bufferPool.Put(sw.buffer[:0]) // return buffer to pool
		sw.buffer = nil
		return len(p), nil
	}
	// already writing to file
	n, err := sw.file.Write(p)
	return n, err
}

func (sw *spillWriter) UsedFile() bool   { return sw.file != nil }
func (sw *spillWriter) FilePath() string { return sw.filePath }
func (sw *spillWriter) Buffer() []byte {
	if sw.buffer != nil {
		max := len(sw.buffer)
		if max > 32 {
			max = 32
		}
		zlog.Debug().Int("buf_len", len(sw.buffer)).Msgf("spillWriter.Buffer: first bytes: %v", sw.buffer[:max])
	}
	return sw.buffer
}

func (sw *spillWriter) Close() {
	if sw.buffer != nil {
		bufferPool.Put(sw.buffer[:0])
		sw.buffer = nil
	}
	if sw.file != nil {
		sw.file.Close()
	}
}

func shardPath(base, key string) string {
	h := sha1.Sum([]byte(key))
	// Use first 3 bytes (6 hex chars) for 3-level sharding: xx/yy/zz/
	return filepath.Join(base,
		fmt.Sprintf("%02x", h[0]),
		fmt.Sprintf("%02x", h[1]),
		fmt.Sprintf("%02x", h[2]),
	)
}
