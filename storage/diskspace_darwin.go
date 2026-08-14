// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build darwin

package storage

import "syscall"

// statfsBlockSize returns the fragment size that Bavail/Blocks are counted in.
// darwin's statfs has no Frsize; block counts are in Bsize units.
func statfsBlockSize(st *syscall.Statfs_t) uint64 {
	return uint64(st.Bsize)
}
