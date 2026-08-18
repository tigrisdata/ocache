// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package segment

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/tigrisdata/ocache/storage/fd"
	pb "github.com/tigrisdata/ocache/storage/proto"
)

// A finalized segment reloaded from disk must report its on-disk size, so the
// recompactor can compute a fragmentation ratio for it. loadSegments builds it
// with NewSegment(path, 0, 0, 0, ...) and restores only version/numEntries/
// dataBytes from the footer, leaving size == 0 — and GetFragmentationRatio
// returns 0.0 for a zero-size segment, so every segment written by an earlier
// process is permanently unreclaimable.
func TestReloadedFinalizedSegment_KeepsSizeForFragmentation(t *testing.T) {
	base := t.TempDir()
	_ = fd.NewFdCache(100)

	m, err := NewManager(base, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := m.AcquireOpenSegmentWithReservation("writer", 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 64*1024)
	for i := range 8 {
		vm := &pb.ValueMessage{ValueType: pb.ValueType_SEGMENT, ValueLength: int64(len(payload))}
		if _, err := seg.WriteEntry(fmt.Sprintf("k%d", i), bytes.NewReader(payload), vm); err != nil {
			t.Fatal(err)
		}
	}
	path := seg.Path()
	liveBytes := seg.GetSize()
	if liveBytes == 0 {
		t.Fatal("in-process segment reported size 0")
	}
	if err := m.FinalizeSegment(seg); err != nil {
		t.Fatal(err)
	}
	if err := seg.Release("writer"); err != nil {
		t.Fatal(err)
	}
	m.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("in-process size=%d, on-disk size=%d", liveBytes, fi.Size())

	// Reopen: this is what every pod restart does.
	m2, err := NewManager(base, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	reloaded := m2.GetSegmentByPath(path)
	if reloaded == nil {
		t.Fatalf("segment %s not loaded after restart", path)
	}
	t.Logf("after reload: GetSize()=%d numEntries=%d", reloaded.GetSize(), reloaded.GetNumEntries())

	// Every entry in the segment is deleted: the delete index would hold
	// liveBytes of dead payload, i.e. a fully dead segment.
	ratio := m2.GetFragmentationRatio(path, liveBytes)
	t.Logf("fragmentation ratio for a fully-dead reloaded segment = %v (recompaction threshold 0.5)", ratio)

	if reloaded.GetSize() == 0 {
		t.Errorf("BUG: reloaded finalized segment reports size 0 (file is %d bytes)", fi.Size())
	}
	if ratio <= 0.5 {
		t.Errorf("BUG: fully-dead reloaded segment has fragmentation %v <= 0.5, recompactor will skip it forever", ratio)
	}
}
