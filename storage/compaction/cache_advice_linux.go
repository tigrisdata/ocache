//go:build linux

// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import (
	"os"

	"golang.org/x/sys/unix"
)

// dropFileCache asks Linux to discard clean pages that compaction will not reuse.
// The hint is deliberately best effort: unsupported filesystems, kernels, and
// dirty ranges may retain buffered I/O without affecting compaction correctness.
func dropFileCache(file *os.File, offset, length int64) {
	if file == nil || offset < 0 || length <= 0 {
		return
	}

	_ = unix.Fadvise(int(file.Fd()), offset, length, unix.FADV_DONTNEED)
}
