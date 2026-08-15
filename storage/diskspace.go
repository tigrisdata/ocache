// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import "syscall"

// diskUsage reports the free and total bytes of the filesystem backing path.
// free uses the space available to an unprivileged process (Bavail, which
// already excludes root-reserved blocks); total is the usable capacity (Blocks).
// ok is false if the statfs call fails, in which case the caller should treat
// the reserve check as unavailable rather than as "no space free". Linux and
// darwin only (the platforms ocache builds on).
//
// The block counts (Bavail/Blocks) are in units of the filesystem's fragment
// size, which is Frsize on Linux and Bsize on darwin (darwin's statfs has no
// Frsize). statfsBlockSize returns the right one per platform; multiplying by
// Bsize on Linux would mis-scale free space where Bsize != Frsize (e.g. some
// FUSE / Docker Desktop mounts).
func diskUsage(path string) (free, total int64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, false
	}
	bs := statfsBlockSize(&st)
	total = clampToInt64(uint64(st.Blocks) * bs)
	// Some mounts (e.g. FUSE filesystems that omit statfs fields) report zero
	// block size or zero total blocks. Treat a zero-capacity result as
	// "measurement unavailable" rather than "disk full": free=0/total=0 would
	// otherwise read as critically low space and drive continuous eviction.
	// free=0 with a sane total remains a legitimate (genuinely full) reading.
	if total <= 0 {
		return 0, 0, false
	}
	return clampToInt64(uint64(st.Bavail) * bs), total, true
}

func clampToInt64(v uint64) int64 {
	if v > uint64(1)<<62 {
		return int64(1) << 62 // implausible; clamp rather than overflow
	}
	return int64(v)
}
