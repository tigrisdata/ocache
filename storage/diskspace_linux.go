// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package storage

import "syscall"

// statfsBlockSize returns the fragment size that Bavail/Blocks are counted in.
// On Linux this is Frsize; Bsize is only the preferred I/O transfer size and can
// differ from Frsize on some filesystems, over-reporting free space if used.
func statfsBlockSize(st *syscall.Statfs_t) uint64 {
	if st.Frsize > 0 {
		return uint64(st.Frsize)
	}
	return uint64(st.Bsize)
}
