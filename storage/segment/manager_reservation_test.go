package segment

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestSegmentReservation tests the segment reservation system
func TestSegmentReservation(t *testing.T) {
	seg := NewSegment("/tmp/test.seg", 0, 0, 0, 1024*1024)

	// Test initial state - not reserved
	if seg.IsReserved() {
		t.Error("New segment should not be reserved")
	}
	if seg.GetReservedBy() != "" {
		t.Error("New segment should have empty reservedBy")
	}

	// Test reservation
	if !seg.Reserve("thread1") {
		t.Error("Should be able to reserve unreserved segment")
	}
	if !seg.IsReserved() {
		t.Error("Segment should be reserved after Reserve()")
	}
	if seg.GetReservedBy() != "thread1" {
		t.Errorf("Expected reservedBy to be 'thread1', got '%s'", seg.GetReservedBy())
	}
	if !seg.IsReservedBy("thread1") {
		t.Error("IsReservedBy should return true for the reserver")
	}
	if seg.IsReservedBy("thread2") {
		t.Error("IsReservedBy should return false for other threads")
	}

	// Test that another thread cannot reserve
	if seg.Reserve("thread2") {
		t.Error("Should not be able to reserve segment already reserved by another thread")
	}
	if seg.GetReservedBy() != "thread1" {
		t.Error("Reservation should still be held by thread1")
	}

	// Test that same thread can re-reserve (idempotent)
	if !seg.Reserve("thread1") {
		t.Error("Same thread should be able to re-reserve")
	}
	if seg.GetReservedBy() != "thread1" {
		t.Error("Reservation should still be held by thread1")
	}

	// Test release by wrong thread (should not release)
	seg.Release("thread2")
	if !seg.IsReserved() {
		t.Error("Segment should still be reserved after release by wrong thread")
	}
	if seg.GetReservedBy() != "thread1" {
		t.Error("Reservation should still be held by thread1")
	}

	// Test release by correct thread
	seg.Release("thread1")
	if seg.IsReserved() {
		t.Error("Segment should not be reserved after release")
	}
	if seg.GetReservedBy() != "" {
		t.Error("ReservedBy should be empty after release")
	}

	// Test that another thread can now reserve
	if !seg.Reserve("thread2") {
		t.Error("Should be able to reserve after release")
	}
	if seg.GetReservedBy() != "thread2" {
		t.Error("Reservation should now be held by thread2")
	}
}

// TestManagerAcquireWithReservation tests the manager's reservation-aware acquisition
func TestManagerAcquireWithReservation(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := NewManager(tmpDir, 100*1024) // 100KB segments
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Thread 1 acquires a segment
	seg1, err := manager.AcquireOpenSegmentWithReservation("thread1", 10*1024)
	if err != nil {
		t.Fatalf("Thread1 failed to acquire segment: %v", err)
	}
	if !seg1.IsReservedBy("thread1") {
		t.Error("Segment should be reserved by thread1")
	}

	// Thread 2 tries to acquire - should get a different segment
	seg2, err := manager.AcquireOpenSegmentWithReservation("thread2", 10*1024)
	if err != nil {
		t.Fatalf("Thread2 failed to acquire segment: %v", err)
	}
	if seg1 == seg2 {
		t.Error("Thread2 should get a different segment than thread1")
	}
	if !seg2.IsReservedBy("thread2") {
		t.Error("Segment should be reserved by thread2")
	}

	// Thread 1 acquires again - should get the same segment
	seg1Again, err := manager.AcquireOpenSegmentWithReservation("thread1", 10*1024)
	if err != nil {
		t.Fatalf("Thread1 failed to re-acquire segment: %v", err)
	}
	if seg1 != seg1Again {
		t.Error("Thread1 should get the same segment when re-acquiring")
	}

	// Release thread1's segment
	manager.ReleaseSegment(seg1, "thread1")
	if seg1.IsReserved() {
		t.Error("Segment should not be reserved after release")
	}

	// Now thread3 can acquire thread1's former segment
	seg3, err := manager.AcquireOpenSegmentWithReservation("thread3", 10*1024)
	if err != nil {
		t.Fatalf("Thread3 failed to acquire segment: %v", err)
	}
	// Could be seg1 or a new segment, both are valid
	if seg3 == seg2 {
		t.Error("Thread3 should not get thread2's reserved segment")
	}
}

// TestConcurrentReservations tests that multiple threads don't interfere
func TestConcurrentReservations(t *testing.T) {
	tmpDir := t.TempDir()
	manager, err := NewManager(tmpDir, 50*1024) // Small segments to force multiple
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	const numThreads = 10
	const opsPerThread = 100

	var wg sync.WaitGroup
	errors := make(chan error, numThreads*opsPerThread)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			callerID := string(rune('A' + threadID)) // A, B, C, ...

			for op := 0; op < opsPerThread; op++ {
				// Acquire segment with reservation
				seg, err := manager.AcquireOpenSegmentWithReservation(callerID, 1024)
				if err != nil {
					errors <- err
					return
				}

				// Verify it's reserved by us
				if !seg.IsReservedBy(callerID) {
					errors <- fmt.Errorf("Thread %s got segment not reserved by it", callerID)
					return
				}

				// Simulate some work
				time.Sleep(time.Microsecond)

				// Randomly release (50% chance)
				if op%2 == 0 {
					manager.ReleaseSegment(seg, callerID)
				}
			}

			// Clean up: release all segments for this thread
			manager.ReleaseAllSegments(callerID)
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		if err != nil {
			t.Error(err)
		}
	}

	// Verify all segments are released at the end
	openSegs := manager.GetOpenSegments()
	for _, seg := range openSegs {
		if seg.IsReserved() {
			t.Errorf("Segment %s still reserved by %s after test", seg.Path(), seg.GetReservedBy())
		}
	}
}
