// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"os"
	"path/filepath"
	"time"

	zlog "github.com/rs/zerolog/log"
	"github.com/tigrisdata/ocache/common/metrics"
	"github.com/tigrisdata/ocache/storage/fd"
)

// orphanGraceWindow is how recently a raw file must have been written for the
// sweep to leave it alone even though no metadata row references it. Put
// writes files/<uuid> first and commits the row second, so a file whose last
// write is this recent may be an in-flight put the reconcile snapshot could
// not yet see. It is the outermost of three guards against an active write
// (see sweepOrphanRawFiles); on its own it only has to cover the instant
// between the writer releasing the file lock and registering the path as
// in-flight, so ten minutes is a very wide margin. A file that misses one
// sweep because it is recent is reconsidered on the next.
const orphanGraceWindow = 10 * time.Minute

// sweepOrphanRawFiles reclaims raw files in files/ that no metadata row
// references (issue #156). It runs after every complete reconcile scan — at
// startup and hourly — with the set of raw-file names that scan saw.
//
// An orphan is a file that is neither referenced nor written within
// orphanGraceWindow of the scan's start. Orphans arise from a crash between
// the file write and the metadata commit, from a CAS put that could not
// confirm its outcome, and historically from eviction (#155) and failed
// commits (#263); none of them are counted by the disk cap or reachable by
// eviction, so this sweep is the only thing that ever gives their space back.
//
// An unreferenced file may still belong to a write in progress, and queueing
// it would be fatal: the queue deletes a path once its lock is free, with no
// notion of the row that has referenced it since. Three guards, checked from
// cheapest to dearest, keep the sweep off active writes:
//
//   - the in-flight registry (Storage.inflightRaw): a writer holds its path
//     there from the moment the file is written until the row is committed
//     or the file is queued for reclaim on failure;
//   - the file lock: FileManager.Write holds it exclusively for the whole
//     write and fsync, however long a client stalls mid-stream, so a file
//     whose lock cannot be taken is being written (or read) right now;
//   - the grace window, which covers the instant between the writer
//     releasing the lock and registering the path.
//
// Nothing is removed here directly: each orphan is handed to the deletion
// queue, so a file a reader still holds open is skipped and retried, and a
// file already queued (by the compactor after a migration, or by a failed
// put) is simply queued twice, which the queue resolves as a no-op. The
// physical size of files/ is published alongside the logical cap so the two
// can be compared without a manual du.
func (c *Cleaner) sweepOrphanRawFiles(referenced map[string]struct{}, scanStart time.Time) {
	filesDir := filepath.Join(c.storage.diskPath, "files")
	entries, err := os.ReadDir(filesDir)
	if err != nil {
		zlog.Error().Err(err).Str("dir", filesDir).Msg("cleaner: orphan sweep could not read files directory")
		return
	}
	cutoff := scanStart.Add(-orphanGraceWindow)

	var physicalBytes, orphanBytes int64
	var orphans, recent, active int
	locks := fd.GetFileLockManager()
	for i, entry := range entries {
		if i%1000 == 0 {
			select {
			case <-c.closeCh:
				zlog.Info().Msg("cleaner: orphan sweep interrupted by shutdown")
				return
			default:
			}
		}
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue // deleted between the listing and now
		}
		physicalBytes += info.Size()
		if _, ok := referenced[entry.Name()]; ok {
			continue
		}
		if info.ModTime().After(cutoff) {
			recent++
			continue
		}
		path := filepath.Join(filesDir, entry.Name())
		if _, inflight := c.storage.inflightRaw.Load(path); inflight {
			active++
			continue
		}
		lock := locks.GetFileLock(path)
		if !lock.TryLock() {
			active++
			continue
		}
		lock.Unlock()
		c.storage.stageRawFileDeletion(path)
		orphans++
		orphanBytes += info.Size()
	}

	metrics.FilesDirBytes.Set(float64(physicalBytes))
	metrics.OrphanFilesQueued.Add(float64(orphans))
	metrics.OrphanBytesQueued.Add(float64(orphanBytes))

	event := zlog.Info().
		Int("files", len(entries)).
		Int("referenced", len(referenced)).
		Int("orphans_queued", orphans).
		Int64("orphan_bytes", orphanBytes).
		Int("recent_skipped", recent).
		Int("active_skipped", active).
		Int64("physical_bytes", physicalBytes).
		Dur("duration_ms", time.Since(scanStart))
	if orphans > 0 {
		event = event.Str("grace", orphanGraceWindow.String())
	}
	event.Msg("cleaner: orphan raw-file sweep completed")
}
