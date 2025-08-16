package segment

import (
	"os"
	"testing"

	pb "github.com/tigrisdata/ocache/proto"
)

// TestManager_MultipleConcurrentOpenSegments tests that the manager can handle
// multiple open segments for different purposes (compaction vs recompaction)
func TestManager_MultipleConcurrentOpenSegments(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := NewManager(tmpDir, 1024*1024) // 1MB segments
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Acquire segment for compaction
	compactionSeg, err := manager.AcquireOpenSegmentForPurpose(PurposeCompaction, 0)
	if err != nil {
		t.Fatalf("Failed to acquire compaction segment: %v", err)
	}
	if compactionSeg == nil {
		t.Fatal("Compaction segment should not be nil")
	}

	// Acquire segment for recompaction
	recompactionSeg, err := manager.AcquireOpenSegmentForPurpose(PurposeRecompaction, 0)
	if err != nil {
		t.Fatalf("Failed to acquire recompaction segment: %v", err)
	}
	if recompactionSeg == nil {
		t.Fatal("Recompaction segment should not be nil")
	}

	// Verify they are different segments
	if compactionSeg == recompactionSeg {
		t.Error("Compaction and recompaction should use different segments")
	}
	if compactionSeg.Path() == recompactionSeg.Path() {
		t.Error("Compaction and recompaction segments should have different paths")
	}

	// Verify GetOpenSegmentForPurpose returns correct segments
	if got := manager.GetOpenSegmentForPurpose(PurposeCompaction); got != compactionSeg {
		t.Error("GetOpenSegmentForPurpose(PurposeCompaction) returned wrong segment")
	}
	if got := manager.GetOpenSegmentForPurpose(PurposeRecompaction); got != recompactionSeg {
		t.Error("GetOpenSegmentForPurpose(PurposeRecompaction) returned wrong segment")
	}

	// Verify GetAllOpenSegments returns both
	allOpen := manager.GetAllOpenSegments()
	if len(allOpen) != 2 {
		t.Errorf("Expected 2 open segments, got %d", len(allOpen))
	}
	if allOpen[PurposeCompaction] != compactionSeg {
		t.Error("GetAllOpenSegments missing or wrong compaction segment")
	}
	if allOpen[PurposeRecompaction] != recompactionSeg {
		t.Error("GetAllOpenSegments missing or wrong recompaction segment")
	}

	// Write to both segments to verify they work independently
	// Write to compaction segment
	tempFile1, err := os.CreateTemp("", "test-compaction-")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile1.Name())
	defer tempFile1.Close()
	tempFile1.WriteString("compaction data")

	vm1 := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		ValueLength: 15,
		Checksum:    12345,
	}
	offset1, err := manager.WriteEntry(compactionSeg, "key1", tempFile1, vm1)
	if err != nil {
		t.Fatalf("Failed to write to compaction segment: %v", err)
	}
	if offset1 < 0 {
		t.Error("Invalid offset returned for compaction write")
	}

	// Write to recompaction segment
	tempFile2, err := os.CreateTemp("", "test-recompaction-")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile2.Name())
	defer tempFile2.Close()
	tempFile2.WriteString("recompaction data")

	vm2 := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		ValueLength: 17,
		Checksum:    67890,
	}
	offset2, err := manager.WriteEntry(recompactionSeg, "key2", tempFile2, vm2)
	if err != nil {
		t.Fatalf("Failed to write to recompaction segment: %v", err)
	}
	if offset2 < 0 {
		t.Error("Invalid offset returned for recompaction write")
	}

	// Finalize recompaction segment
	if err := manager.FinalizeSegment(recompactionSeg); err != nil {
		t.Fatalf("Failed to finalize recompaction segment: %v", err)
	}

	// Verify compaction segment is still open
	if !compactionSeg.HasOpenFile() {
		t.Error("Compaction segment should still be open after finalizing recompaction segment")
	}

	// Verify recompaction segment is closed
	if recompactionSeg.HasOpenFile() {
		t.Error("Recompaction segment should be closed after finalization")
	}

	// Verify GetOpenSegmentForPurpose reflects the change
	if got := manager.GetOpenSegmentForPurpose(PurposeRecompaction); got != nil {
		t.Error("GetOpenSegmentForPurpose(PurposeRecompaction) should return nil after finalization")
	}

	// Acquire new recompaction segment after finalizing the previous one
	newRecompactionSeg, err := manager.AcquireOpenSegmentForPurpose(PurposeRecompaction, 0)
	if err != nil {
		t.Fatalf("Failed to acquire new recompaction segment: %v", err)
	}
	if newRecompactionSeg == nil {
		t.Fatal("New recompaction segment should not be nil")
	}
	if newRecompactionSeg == recompactionSeg {
		t.Error("Should have created a new recompaction segment")
	}
	if newRecompactionSeg == compactionSeg {
		t.Error("New recompaction segment should not be the compaction segment")
	}
}

// TestManager_BackwardCompatibility tests that the deprecated AcquireOpenSegment
// still works and defaults to compaction purpose
func TestManager_BackwardCompatibility(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := NewManager(tmpDir, 1024*1024) // 1MB segments
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Use deprecated method
	seg, err := manager.AcquireOpenSegment(0)
	if err != nil {
		t.Fatalf("Failed to acquire segment: %v", err)
	}
	if seg == nil {
		t.Fatal("Segment should not be nil")
	}

	// Verify it's the same as compaction segment
	compactionSeg := manager.GetOpenSegmentForPurpose(PurposeCompaction)
	if seg != compactionSeg {
		t.Error("Deprecated AcquireOpenSegment should return compaction segment")
	}

	// Verify GetCurrentOpenSegment also returns it
	currentSeg := manager.GetCurrentOpenSegment()
	if seg != currentSeg {
		t.Error("GetCurrentOpenSegment should return the same segment")
	}
}
