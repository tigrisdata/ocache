// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	stor "github.com/tigrisdata/ocache/storage"
)

const embeddedPutStreamRegularFileBenchmarkSize = 1 << 20

// BenchmarkEmbeddedClientPutStreamRegularFile measures owner-local PutStream
// calls that store a regular-file input as a raw file. It rewinds the reusable
// source outside the timer before each call; key deletion is also untimed.
func BenchmarkEmbeddedClientPutStreamRegularFile(b *testing.B) {
	previousLogLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(previousLogLevel) })

	content := bytes.Repeat([]byte("abcdefgh"), embeddedPutStreamRegularFileBenchmarkSize/8)
	sourcePath := filepath.Join(b.TempDir(), "input.bin")
	if err := os.WriteFile(sourcePath, content, 0o600); err != nil {
		b.Fatal(err)
	}
	input, err := os.Open(sourcePath)
	if err != nil {
		b.Fatal(err)
	}
	defer input.Close()

	client, err := New(&Config{
		DiskPath: b.TempDir(),
		TTL:      time.Hour,
		Storage: &stor.StorageConfig{
			MetadataCacheSize:      16 << 20,
			MetadataBackgroundJobs: 2,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := client.Close(); err != nil {
			b.Errorf("close embedded client: %v", err)
		}
	})

	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("benchmark:regular-file:%d", i)
	}

	ctx := context.Background()
	b.SetBytes(embeddedPutStreamRegularFileBenchmarkSize)
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for _, key := range keys {
		if _, err := input.Seek(0, io.SeekStart); err != nil {
			b.Fatalf("rewind source file: %v", err)
		}
		b.StartTimer()
		if err := client.PutStream(ctx, key, input, int64(time.Hour.Seconds())); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := client.Delete(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}
