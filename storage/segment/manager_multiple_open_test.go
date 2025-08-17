package segment

import (
	"fmt"
	"os"
	"testing"

	pb "github.com/tigrisdata/ocache/proto"
)

// TestManager_MultipleConcurrentOpenSegments tests that the manager can handle
// multiple open segments concurrently
func TestManager_MultipleConcurrentOpenSegments(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := NewManager(tmpDir, 1024*1024) // 1MB segments
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Acquire first segment
	seg1, err := manager.AcquireOpenSegmentWithReservation("test", 0)
	if err != nil {
		t.Fatalf("Failed to acquire first segment: %v", err)
	}
	if seg1 == nil {
		t.Fatal("First segment should not be nil")
	}

	// Write enough data to fill the first segment
	tempFile1, err := os.CreateTemp("", "test-seg1-")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile1.Name())
	defer tempFile1.Close()

	// Write a large amount of data to force creation of new segment
	largeData := make([]byte, 900*1024) // 900KB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	tempFile1.Write(largeData)

	vm1 := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		ValueLength: int64(len(largeData)),
		Checksum:    12345,
	}
	offset1, err := manager.WriteEntry(seg1, "key1", tempFile1, vm1)
	if err != nil {
		t.Fatalf("Failed to write to first segment: %v", err)
	}
	if offset1 < 0 {
		t.Error("Invalid offset returned for first write")
	}

	// Now acquire another segment - should get a new one since first is nearly full
	seg2, err := manager.AcquireOpenSegmentWithReservation("test", 200*1024) // Request 200KB
	if err != nil {
		t.Fatalf("Failed to acquire second segment: %v", err)
	}
	if seg2 == nil {
		t.Fatal("Second segment should not be nil")
	}

	// Verify they are different segments
	if seg1 == seg2 {
		t.Error("Should have gotten a different segment when first is nearly full")
	}
	if seg1.Path() == seg2.Path() {
		t.Error("Segments should have different paths")
	}

	// Verify GetOpenSegments returns both
	openSegs := manager.GetOpenSegments()
	if len(openSegs) != 2 {
		t.Errorf("Expected 2 open segments, got %d", len(openSegs))
	}

	// Write to second segment to verify it works
	tempFile2, err := os.CreateTemp("", "test-seg2-")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile2.Name())
	defer tempFile2.Close()
	tempFile2.WriteString("segment 2 data")

	vm2 := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		ValueLength: 14,
		Checksum:    67890,
	}
	offset2, err := manager.WriteEntry(seg2, "key2", tempFile2, vm2)
	if err != nil {
		t.Fatalf("Failed to write to second segment: %v", err)
	}
	if offset2 < 0 {
		t.Error("Invalid offset returned for second write")
	}

	// Finalize first segment
	if err := manager.FinalizeSegment(seg1); err != nil {
		t.Fatalf("Failed to finalize first segment: %v", err)
	}

	// Verify second segment is still open
	if !seg2.HasOpenFile() {
		t.Error("Second segment should still be open after finalizing first segment")
	}

	// Verify first segment is closed
	if seg1.HasOpenFile() {
		t.Error("First segment should be closed after finalization")
	}

	// Verify GetOpenSegments now returns only one
	openSegs = manager.GetOpenSegments()
	if len(openSegs) != 1 {
		t.Errorf("Expected 1 open segment after finalization, got %d", len(openSegs))
	}
	if openSegs[0] != seg2 {
		t.Error("Remaining open segment should be seg2")
	}

	// Acquire new segment after finalizing the first one
	seg3, err := manager.AcquireOpenSegmentWithReservation("test", 0)
	if err != nil {
		t.Fatalf("Failed to acquire third segment: %v", err)
	}
	if seg3 == nil {
		t.Fatal("Third segment should not be nil")
	}

	// Could be the same as seg2 if it has space, or a new one
	// Both are valid behaviors
}

// TestManager_MultipleThreadsAcquireSegments tests concurrent acquisition of segments
func TestManager_MultipleThreadsAcquireSegments(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := NewManager(tmpDir, 100*1024) // 100KB segments (small for testing)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Run multiple goroutines that acquire segments
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			seg, err := manager.AcquireOpenSegmentWithReservation("test", 10*1024) // Request 10KB
			if err != nil {
				done <- err
				return
			}
			if seg == nil {
				done <- fmt.Errorf("Thread %d: segment is nil", id)
				return
			}
			done <- nil
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}

	// Check that we have at least 1 open segment (could be more if threads needed more space)
	openSegs := manager.GetOpenSegments()
	if len(openSegs) < 1 {
		t.Errorf("Expected at least 1 open segment, got %d", len(openSegs))
	}
}
