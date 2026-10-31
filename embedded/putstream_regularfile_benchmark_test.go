// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	stor "github.com/tigrisdata/ocache/storage"
)

const (
	// Keep values above the default 64 MiB compaction threshold.
	embeddedPutStreamRegularFileBenchmarkSize = 256 << 20
	// Match the replay's fresh-key batch while bounding queued cleanup.
	embeddedPutStreamRegularFileBenchmarkBatchSize = 8
)

// BenchmarkEmbeddedClientPutStreamRegularFile measures owner-local PutStream
// calls for raw-file inputs. Each bounded batch uses fresh keys; delete and
// client teardown happen outside the timer before the next batch starts.
func BenchmarkEmbeddedClientPutStreamRegularFile(b *testing.B) {
	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(previousLogLevel) })

	sourcePath := filepath.Join(b.TempDir(), "input.bin")
	out, err := os.Create(sourcePath)
	if err != nil {
		b.Fatal(err)
	}
	block := bytes.Repeat([]byte("abcdefgh"), (1<<20)/8)
	for written := 0; written < embeddedPutStreamRegularFileBenchmarkSize; written += len(block) {
		n, err := out.Write(block)
		if err != nil {
			_ = out.Close()
			b.Fatal(err)
		}
		if n != len(block) {
			_ = out.Close()
			b.Fatalf("write source file: wrote %d bytes, want %d", n, len(block))
		}
	}
	if err := out.Close(); err != nil {
		b.Fatal(err)
	}

	input, err := os.Open(sourcePath)
	if err != nil {
		b.Fatal(err)
	}
	defer input.Close()

	diskPath := b.TempDir()
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("benchmark:regular-file:%d", i)
	}

	ctx := context.Background()
	b.SetBytes(embeddedPutStreamRegularFileBenchmarkSize)
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for first := 0; first < b.N; first += embeddedPutStreamRegularFileBenchmarkBatchSize {
		end := min(first+embeddedPutStreamRegularFileBenchmarkBatchSize, b.N)
		client, err := New(&Config{
			DiskPath: diskPath,
			TTL:      time.Hour,
			Storage: &stor.StorageConfig{
				MetadataCacheSize:      16 << 20,
				MetadataBackgroundJobs: 2,
			},
		})
		if err != nil {
			_ = os.RemoveAll(diskPath)
			b.Fatalf("create embedded client: %v", err)
		}

		var putErr error
		for i := first; i < end; i++ {
			if _, err := input.Seek(0, io.SeekStart); err != nil {
				putErr = fmt.Errorf("rewind source file: %w", err)
				break
			}
			b.StartTimer()
			err := client.PutStream(ctx, keys[i], input, int64(time.Hour.Seconds()))
			b.StopTimer()
			if err != nil {
				putErr = fmt.Errorf("PutStream(%q): %w", keys[i], err)
				break
			}
		}

		var cleanupErr error
		for _, key := range keys[first:end] {
			if err := client.Delete(ctx, key); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("Delete(%q): %w", key, err))
			}
		}
		if err := client.Close(); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close embedded client: %w", err))
		}
		if err := os.RemoveAll(diskPath); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove benchmark storage: %w", err))
		}
		if err := errors.Join(putErr, cleanupErr); err != nil {
			b.Fatal(err)
		}
	}
}
