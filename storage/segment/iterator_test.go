package segment

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/tigrisdata/ocache/proto"
)

func TestSegmentIterator(t *testing.T) {
	// Create a temp directory for test segments
	tempDir := t.TempDir()
	segPath := filepath.Join(tempDir, "test_segment.seg")

	// Create a segment and write some entries
	seg := NewSegment(segPath, 0, 0, 0, 1024*1024)

	// Open the segment file for writing
	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("failed to create segment file: %v", err)
	}
	defer f.Close()

	seg.SetOpenFile(f)

	// Write several test entries
	testEntries := []struct {
		key      string
		value    []byte
		checksum uint32
	}{
		{"key1", []byte("value1"), 0x12345678},
		{"key2", []byte("value2-longer"), 0xABCDEF00},
		{"key3", []byte("v3"), 0x11223344},
		{"longkey4", []byte("value4 with more data"), 0xFFEEDDCC},
	}

	// Write entries to the segment
	offset := int64(0)
	for _, entry := range testEntries {
		header := BuildValueHeader(entry.key, int64(len(entry.value)), entry.checksum, CurrentValueHeaderVersion)

		if _, err := f.Write(header); err != nil {
			t.Fatalf("failed to write header for %s: %v", entry.key, err)
		}
		if _, err := f.Write(entry.value); err != nil {
			t.Fatalf("failed to write value for %s: %v", entry.key, err)
		}

		offset += int64(len(header) + len(entry.value))
		seg.IncrementSize(int64(len(header) + len(entry.value)))
		seg.IncrementEntries()
		seg.IncrementDataBytes(int64(len(entry.value)))
	}

	// Write footer
	footer := BuildSegmentFooterWithVersion(CurrentSegmentVersion, uint32(len(testEntries)), seg.dataBytes)
	if _, err := f.Write(footer); err != nil {
		t.Fatalf("failed to write footer: %v", err)
	}

	// Sync to ensure all data is written
	if err := f.Sync(); err != nil {
		t.Fatalf("failed to sync file: %v", err)
	}

	// Create an iterator and test reading
	iter, err := seg.NewIterator(f)
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	// Iterate and verify entries
	readEntries := 0
	for {
		entry, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error during iteration: %v", err)
		}

		// Verify we got the expected entry
		if readEntries >= len(testEntries) {
			t.Fatalf("read more entries than expected")
		}

		expected := testEntries[readEntries]
		if entry.Key != expected.key {
			t.Errorf("entry %d: expected key %q, got %q", readEntries, expected.key, entry.Key)
		}
		if entry.ValueLength != int64(len(expected.value)) {
			t.Errorf("entry %d: expected value length %d, got %d", readEntries, len(expected.value), entry.ValueLength)
		}
		if entry.Checksum != expected.checksum {
			t.Errorf("entry %d: expected checksum %x, got %x", readEntries, expected.checksum, entry.Checksum)
		}

		readEntries++
	}

	if readEntries != len(testEntries) {
		t.Errorf("expected to read %d entries, got %d", len(testEntries), readEntries)
	}
}

func TestSegmentIteratorReset(t *testing.T) {
	// Create a temp directory for test segments
	tempDir := t.TempDir()
	segPath := filepath.Join(tempDir, "test_segment.seg")

	// Create a segment with a few entries
	seg := NewSegment(segPath, 0, 0, 0, 1024*1024)

	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("failed to create segment file: %v", err)
	}
	defer f.Close()

	seg.SetOpenFile(f)

	// Write a couple of test entries
	for i := 0; i < 3; i++ {
		key := string(rune('a' + i))
		value := []byte("test")
		header := BuildValueHeader(key, int64(len(value)), 0, CurrentValueHeaderVersion)

		if _, err := f.Write(header); err != nil {
			t.Fatalf("failed to write header: %v", err)
		}
		if _, err := f.Write(value); err != nil {
			t.Fatalf("failed to write value: %v", err)
		}
	}

	// Write footer
	footer := BuildSegmentFooterWithVersion(CurrentSegmentVersion, 3, 12)
	if _, err := f.Write(footer); err != nil {
		t.Fatalf("failed to write footer: %v", err)
	}

	// Create an iterator
	iter, err := seg.NewIterator(f)
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	// Read first entry
	entry1, err := iter.Next()
	if err != nil {
		t.Fatalf("failed to read first entry: %v", err)
	}
	if entry1.Key != "a" {
		t.Errorf("expected first key to be 'a', got %q", entry1.Key)
	}

	// Read second entry
	entry2, err := iter.Next()
	if err != nil {
		t.Fatalf("failed to read second entry: %v", err)
	}
	if entry2.Key != "b" {
		t.Errorf("expected second key to be 'b', got %q", entry2.Key)
	}

	// Reset the iterator
	iter.Reset()

	// Should be able to read from the beginning again
	entry1Again, err := iter.Next()
	if err != nil {
		t.Fatalf("failed to read first entry after reset: %v", err)
	}
	if entry1Again.Key != "a" {
		t.Errorf("expected first key after reset to be 'a', got %q", entry1Again.Key)
	}
}

func TestSegmentIteratorEmptySegment(t *testing.T) {
	// Create a temp directory for test segments
	tempDir := t.TempDir()
	segPath := filepath.Join(tempDir, "empty_segment.seg")

	// Create an empty segment with just a footer
	seg := NewSegment(segPath, 0, 0, 0, 1024*1024)

	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("failed to create segment file: %v", err)
	}
	defer f.Close()

	seg.SetOpenFile(f)

	// Write just the footer
	footer := BuildSegmentFooterWithVersion(CurrentSegmentVersion, 0, 0)
	if _, err := f.Write(footer); err != nil {
		t.Fatalf("failed to write footer: %v", err)
	}

	// Create an iterator
	iter, err := seg.NewIterator(f)
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	// Should immediately return EOF
	_, err = iter.Next()
	if err != io.EOF {
		t.Errorf("expected EOF for empty segment, got %v", err)
	}
}

func TestSegmentIteratorWithManager(t *testing.T) {
	// Create a segment manager
	tempDir := t.TempDir()
	sm, err := NewManager(tempDir, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create segment manager: %v", err)
	}
	defer sm.Close()

	// Acquire an open segment
	seg, err := sm.AcquireOpenSegmentWithReservation("test-writer", 0)
	if err != nil {
		t.Fatalf("failed to acquire segment: %v", err)
	}
	defer seg.Release("test-writer")

	// Write some entries using the manager
	testData := []struct {
		key   string
		value []byte
	}{
		{"test1", []byte("data1")},
		{"test2", []byte("data2 with more content")},
		{"test3", []byte("data3")},
	}

	for _, td := range testData {
		vm := &pb.ValueMessage{
			ValueType:   pb.ValueType_SEGMENT,
			ValueLength: int64(len(td.value)),
			Checksum:    0,
		}

		// Create a temp file with the value
		tmpFile, err := os.CreateTemp(tempDir, "value-*.tmp")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		if _, err := tmpFile.Write(td.value); err != nil {
			tmpFile.Close()
			t.Fatalf("failed to write value: %v", err)
		}
		if _, err := tmpFile.Seek(0, 0); err != nil {
			tmpFile.Close()
			t.Fatalf("failed to seek: %v", err)
		}

		if _, err := seg.WriteEntry(td.key, tmpFile, vm); err != nil {
			tmpFile.Close()
			t.Fatalf("failed to write entry: %v", err)
		}
		tmpFile.Close()
	}

	// Get the file handle for iteration
	seg.Lock()
	file := seg.File()
	seg.Unlock()

	// Create an iterator and verify we can read the entries
	iter, err := seg.NewIterator(file)
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	count := 0
	for {
		entry, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error during iteration: %v", err)
		}

		if count >= len(testData) {
			t.Fatalf("read more entries than expected")
		}

		expected := testData[count]
		if entry.Key != expected.key {
			t.Errorf("entry %d: expected key %q, got %q", count, expected.key, entry.Key)
		}
		if entry.ValueLength != int64(len(expected.value)) {
			t.Errorf("entry %d: expected value length %d, got %d", count, len(expected.value), entry.ValueLength)
		}

		count++
	}

	if count != len(testData) {
		t.Errorf("expected to read %d entries, got %d", len(testData), count)
	}
}
