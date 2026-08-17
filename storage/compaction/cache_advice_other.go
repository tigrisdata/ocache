//go:build !linux

// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package compaction

import "os"

// dropFileCache is a no-op where POSIX_FADV_DONTNEED is unavailable.
func dropFileCache(_ *os.File, _, _ int64) {}
