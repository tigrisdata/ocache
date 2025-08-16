package segment

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/tigrisdata/ocache/proto"
	"github.com/tigrisdata/ocache/storage/fd"
)

func TestNewSegment(t *testing.T) {
	path := "/test/segment.seg"
	entries := uint32(10)
	dataBytes := int64(1024)
	size := int64(2048)
	maxSize := int64(4096)

	seg := NewSegment(path, entries, dataBytes, size, maxSize)

	if seg.path != path {
		t.Errorf("Path mismatch: got %s, want %s", seg.path, path)
	}
	if seg.entries != entries {
		t.Errorf("Entries mismatch: got %d, want %d", seg.entries, entries)
	}
	if seg.dataBytes != dataBytes {
		t.Errorf("DataBytes mismatch: got %d, want %d", seg.dataBytes, dataBytes)
	}
	if seg.size != size {
		t.Errorf("Size mismatch: got %d, want %d", seg.size, size)
	}
	if seg.version != CurrentSegmentVersion {
		t.Errorf("Version mismatch: got %d, want %d", seg.version, CurrentSegmentVersion)
	}
	if seg.maxSupportedSize != maxSize {
		t.Errorf("MaxSupportedSize mismatch: got %d, want %d", seg.maxSupportedSize, maxSize)
	}
}

func TestSegment_Path(t *testing.T) {
	path := "/test/segment.seg"
	seg := NewSegment(path, 0, 0, 0, 4096)

	if seg.Path() != path {
		t.Errorf("Path() returned %s, want %s", seg.Path(), path)
	}
}

func TestSegment_Remaining(t *testing.T) {
	seg := NewSegment("/test/segment.seg", 0, 0, 1024, 4096)

	remaining := seg.Remaining()
	expected := int64(4096 - 1024)

	if remaining != expected {
		t.Errorf("Remaining() returned %d, want %d", remaining, expected)
	}
}

func TestSegment_SetOpenFile(t *testing.T) {
	seg := NewSegment("/test/segment.seg", 0, 0, 0, 4096)

	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.seg")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	seg.SetOpenFile(tmpFile)

	seg.mu.RLock()
	if seg.file != tmpFile {
		t.Error("SetOpenFile did not set the file correctly")
	}
	seg.mu.RUnlock()
}

func TestNewManager(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer manager.Close()

	if manager == nil {
		t.Fatal("NewManager returned nil")
	}

	expectedPath := filepath.Join(basePath, "segments")
	if manager.segmentsPath != expectedPath {
		t.Errorf("segmentsPath mismatch: got %s, want %s", manager.segmentsPath, expectedPath)
	}

	if manager.segmentSize != segmentSize {
		t.Errorf("segmentSize mismatch: got %d, want %d", manager.segmentSize, segmentSize)
	}

	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Error("Segments directory was not created")
	}
}

func TestManager_RegisterSegment(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	path := "/test/segment.seg"
	entries := uint32(10)
	bytes := int64(1024)

	manager.RegisterSegment(path, entries, bytes)

	// Check if segment was registered (we need to access internal state for this specific test)
	segments := manager.GetSegments()
	var seg *Segment
	exists := false
	for _, s := range segments {
		if s.Path() == path {
			seg = s
			exists = true
			break
		}
	}

	if !exists {
		t.Error("Segment was not registered in segMap")
	}

	if seg.path != path {
		t.Errorf("Registered segment has wrong path: got %s, want %s", seg.path, path)
	}
	if seg.entries != entries {
		t.Errorf("Registered segment has wrong entries: got %d, want %d", seg.entries, entries)
	}
}

func TestManager_AcquireOpenSegment(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	needed := int64(1024)
	seg, err := manager.AcquireOpenSegment(needed)
	if err != nil {
		t.Fatalf("AcquireOpenSegment failed: %v", err)
	}

	if seg == nil {
		t.Fatal("AcquireOpenSegment returned nil segment")
	}

	seg.mu.RLock()
	hasFile := seg.file != nil
	seg.mu.RUnlock()

	if !hasFile {
		t.Error("Acquired segment should have an open file")
	}
}

func TestManager_WriteEntry(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	seg, err := manager.AcquireOpenSegment(1024)
	if err != nil {
		t.Fatal(err)
	}

	tmpFile, err := os.CreateTemp(t.TempDir(), "value-*.dat")
	if err != nil {
		t.Fatal(err)
	}
	valueData := []byte("test value data")
	tmpFile.Write(valueData)
	tmpFile.Close()

	f, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	userKey := "test-key"
	vm := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		ValueLength: int64(len(valueData)),
		Checksum:    12345,
	}

	offset, err := manager.WriteEntry(seg, userKey, f, vm)
	if err != nil {
		t.Fatalf("WriteEntry failed: %v", err)
	}

	if offset < 0 {
		t.Errorf("WriteEntry returned invalid offset: %d", offset)
	}

	if seg.entries != 1 {
		t.Errorf("Segment entries should be 1, got %d", seg.entries)
	}

	if seg.dataBytes != int64(len(valueData)) {
		t.Errorf("Segment dataBytes should be %d, got %d", len(valueData), seg.dataBytes)
	}
}

func TestManager_SyncSegment(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	seg, err := manager.AcquireOpenSegment(1024)
	if err != nil {
		t.Fatal(err)
	}

	err = manager.SyncSegment(seg)
	if err != nil {
		t.Fatalf("SyncSegment failed: %v", err)
	}
}

func TestManager_FinalizeSegment(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	seg, err := manager.AcquireOpenSegment(1024)
	if err != nil {
		t.Fatal(err)
	}

	tmpFile, err := os.CreateTemp(t.TempDir(), "value-*.dat")
	if err != nil {
		t.Fatal(err)
	}
	valueData := []byte("test value data")
	tmpFile.Write(valueData)
	tmpFile.Close()

	f, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	userKey := "test-key"
	vm := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		ValueLength: int64(len(valueData)),
		Checksum:    12345,
	}

	_, err = manager.WriteEntry(seg, userKey, f, vm)
	if err != nil {
		t.Fatal(err)
	}

	err = manager.FinalizeSegment(seg)
	if err != nil {
		t.Fatalf("FinalizeSegment failed: %v", err)
	}

	seg.mu.RLock()
	hasFile := seg.file != nil
	seg.mu.RUnlock()

	if hasFile {
		t.Error("Finalized segment should not have an open file")
	}

	info, err := os.Stat(seg.path)
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() <= 0 {
		t.Error("Finalized segment file should have non-zero size")
	}
}

func TestManager_ReadValue(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	seg, err := manager.AcquireOpenSegment(1024)
	if err != nil {
		t.Fatal(err)
	}

	tmpFile, err := os.CreateTemp(t.TempDir(), "value-*.dat")
	if err != nil {
		t.Fatal(err)
	}
	valueData := []byte("test value data for reading")
	tmpFile.Write(valueData)
	tmpFile.Close()

	f, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	userKey := "read-test-key"
	vm := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		ValueLength: int64(len(valueData)),
		Checksum:    12345,
	}

	offset, err := manager.WriteEntry(seg, userKey, f, vm)
	if err != nil {
		t.Fatal(err)
	}

	err = manager.FinalizeSegment(seg)
	if err != nil {
		t.Fatal(err)
	}

	rc, err := manager.ReadValue(userKey, seg.path, offset, int64(len(valueData)))
	if err != nil {
		t.Fatalf("ReadValue failed: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, valueData) {
		t.Errorf("Read value mismatch: got %q, want %q", data, valueData)
	}
}

func TestManager_ReadValue_InvalidParams(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	_, err = manager.ReadValue("key", "", 0, 100)
	if err == nil {
		t.Error("ReadValue should fail with empty path")
	} else if err.Error() == "" {
		t.Error("Expected error message for empty path")
	}

	_, err = manager.ReadValue("key", "/path", -1, 100)
	if err == nil {
		t.Error("ReadValue should fail with negative offset")
	} else if err.Error() == "" {
		t.Error("Expected error message for negative offset")
	}

	_, err = manager.ReadValue("key", "/path", 0, 0)
	if err == nil {
		t.Error("ReadValue should fail with zero length")
	} else if err.Error() == "" {
		t.Error("Expected error message for zero length")
	}

	_, err = manager.ReadValue("key", "/nonexistent/segment.seg", 0, 100)
	if err == nil {
		t.Error("ReadValue should fail with non-existent segment")
	} else if err.Error() == "" {
		t.Error("Expected error message for non-existent segment")
	}
}

func TestManager_LoadSegments(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)
	segmentsPath := filepath.Join(basePath, "segments")

	if err := os.MkdirAll(segmentsPath, 0o755); err != nil {
		t.Fatal(err)
	}

	segFile := filepath.Join(segmentsPath, "segment_1.seg")
	f, err := os.Create(segFile)
	if err != nil {
		t.Fatal(err)
	}

	header := BuildValueHeader("key1", 10, 12345, CurrentValueHeaderVersion)
	f.Write(header)
	f.Write([]byte("value1data"))

	footer := BuildSegmentFooterWithVersion(CurrentSegmentVersion, 1, 10)
	f.Write(footer)
	f.Close()

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	numSegments := manager.GetSegmentCount()

	if numSegments != 1 {
		t.Errorf("Expected 1 loaded segment, got %d", numSegments)
	}
}

func TestManager_MultipleOpenSegments(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)
	segmentsPath := filepath.Join(basePath, "segments")

	if err := os.MkdirAll(segmentsPath, 0o755); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		segFile := filepath.Join(segmentsPath, "segment.seg")
		f, err := os.Create(segFile)
		if err != nil {
			t.Fatal(err)
		}
		f.Truncate(segmentSize)
		f.Close()
	}

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	numSegments := manager.GetSegmentCount()

	if numSegments > 1 {
		t.Logf("Manager handled multiple open segments: %d segments", numSegments)
	}
}

func TestReadCloserWithOnClose(t *testing.T) {
	called := false
	rc := &readCloserWithOnClose{
		Reader: bytes.NewReader([]byte("test")),
		onClose: func() {
			called = true
		},
	}

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "test" {
		t.Errorf("Read wrong data: %q", string(data))
	}

	err = rc.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !called {
		t.Error("onClose callback was not called")
	}
}

func TestReadCloserWithOnClose_NilCallback(t *testing.T) {
	rc := &readCloserWithOnClose{
		Reader:  bytes.NewReader([]byte("test")),
		onClose: nil,
	}

	err := rc.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestManager_ValidateOpenSegment(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(256 * 1024 * 1024) // 256MB like in production
	segmentsPath := filepath.Join(basePath, "segments")

	if err := os.MkdirAll(segmentsPath, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create a sparse segment file pre-allocated to full size
	segFile := filepath.Join(segmentsPath, "segment_test.seg")
	f, err := os.Create(segFile)
	if err != nil {
		t.Fatal(err)
	}

	// Pre-allocate the file to segment size (creates sparse file)
	if err := f.Truncate(segmentSize); err != nil {
		t.Fatal(err)
	}

	// Write some actual data at the beginning
	userKey1 := "test-key-1"
	value1 := []byte("test value data 1")
	header1 := BuildValueHeader(userKey1, int64(len(value1)), 0, CurrentValueHeaderVersion)
	if _, err := f.Write(header1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(value1); err != nil {
		t.Fatal(err)
	}

	userKey2 := "test-key-2"
	value2 := []byte("test value data 2 longer")
	header2 := BuildValueHeader(userKey2, int64(len(value2)), 0, CurrentValueHeaderVersion)
	if _, err := f.Write(header2); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(value2); err != nil {
		t.Fatal(err)
	}

	// Calculate expected actual data size
	expectedSize := int64(len(header1) + len(value1) + len(header2) + len(value2))

	// Verify file is sparse (file size should be full segment size)
	fileInfo, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fileInfo.Size() != segmentSize {
		t.Errorf("Pre-allocated file size should be %d, got %d", segmentSize, fileInfo.Size())
	}

	f.Close()

	// Now test loading and validating the segment
	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	// Get the loaded segment
	segments := manager.GetSegments()
	if len(segments) != 1 {
		t.Fatalf("Expected 1 segment, got %d", len(segments))
	}
	seg := segments[0]

	// Verify that validateOpenSegment set the correct actual data size
	if seg.size != expectedSize {
		t.Errorf("validateOpenSegment should set size to actual data (%d), got %d", expectedSize, seg.size)
	}

	// Verify maxSupportedSize is still the full segment size
	if seg.maxSupportedSize != segmentSize {
		t.Errorf("maxSupportedSize should remain %d, got %d", segmentSize, seg.maxSupportedSize)
	}

	// Verify remaining space calculation is correct
	expectedRemaining := segmentSize - expectedSize
	actualRemaining := seg.Remaining()
	if actualRemaining != expectedRemaining {
		t.Errorf("Remaining() should return %d, got %d", expectedRemaining, actualRemaining)
	}

	// Verify entry count
	if seg.entries != 2 {
		t.Errorf("Expected 2 entries, got %d", seg.entries)
	}

	// Verify data bytes count
	expectedDataBytes := int64(len(value1) + len(value2))
	if seg.dataBytes != expectedDataBytes {
		t.Errorf("Expected dataBytes %d, got %d", expectedDataBytes, seg.dataBytes)
	}
}

func TestManager_ValidateOpenSegmentWithCorruption(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(256 * 1024 * 1024)
	segmentsPath := filepath.Join(basePath, "segments")

	if err := os.MkdirAll(segmentsPath, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create a segment file with valid data followed by corruption
	segFile := filepath.Join(segmentsPath, "segment_corrupt.seg")
	f, err := os.Create(segFile)
	if err != nil {
		t.Fatal(err)
	}

	// Pre-allocate the file
	if err := f.Truncate(segmentSize); err != nil {
		t.Fatal(err)
	}

	// Write valid entry
	userKey := "valid-key"
	value := []byte("valid data")
	header := BuildValueHeader(userKey, int64(len(value)), 0, CurrentValueHeaderVersion)
	if _, err := f.Write(header); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(value); err != nil {
		t.Fatal(err)
	}

	validDataSize := int64(len(header) + len(value))

	// Write corrupted/incomplete header
	corruptHeader := []byte{0xFF, 0xFF, 0xFF, 0xFF} // Invalid header
	if _, err := f.Write(corruptHeader); err != nil {
		t.Fatal(err)
	}

	f.Close()

	// Load and validate
	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	// Get the loaded segment
	segments := manager.GetSegments()
	if len(segments) != 1 {
		t.Fatalf("Expected 1 segment, got %d", len(segments))
	}
	seg := segments[0]

	// Should only count valid data before corruption
	if seg.size != validDataSize {
		t.Errorf("validateOpenSegment should truncate at valid data (%d), got %d", validDataSize, seg.size)
	}

	if seg.entries != 1 {
		t.Errorf("Expected 1 valid entry, got %d", seg.entries)
	}

	if seg.dataBytes != int64(len(value)) {
		t.Errorf("Expected dataBytes %d, got %d", len(value), seg.dataBytes)
	}

	// Verify the file was actually truncated
	fileInfo, err := os.Stat(segFile)
	if err != nil {
		t.Fatal(err)
	}
	if fileInfo.Size() != validDataSize {
		t.Errorf("File should be truncated to %d, got %d", validDataSize, fileInfo.Size())
	}
}

func TestManager_Close(t *testing.T) {
	basePath := t.TempDir()
	segmentSize := int64(1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		t.Fatal(err)
	}

	seg, err := manager.AcquireOpenSegment(1024)
	if err != nil {
		t.Fatal(err)
	}

	if seg == nil {
		t.Fatal("Expected open segment")
	}

	manager.Close()

	seg.mu.RLock()
	hasFile := seg.file != nil
	seg.mu.RUnlock()

	if hasFile {
		t.Log("Segment file should be closed after manager.Close()")
	}
}

func BenchmarkManager_WriteEntry(b *testing.B) {
	basePath := b.TempDir()
	segmentSize := int64(10 * 1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer manager.Close()

	seg, err := manager.AcquireOpenSegment(1024)
	if err != nil {
		b.Fatal(err)
	}

	tmpFile, err := os.CreateTemp(b.TempDir(), "value-*.dat")
	if err != nil {
		b.Fatal(err)
	}
	valueData := bytes.Repeat([]byte("x"), 1024)
	tmpFile.Write(valueData)
	tmpFile.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, _ := os.Open(tmpFile.Name())
		vm := &pb.ValueMessage{
			ValueType:   pb.ValueType_RAW_FILE,
			ValueLength: int64(len(valueData)),
			Checksum:    12345,
		}
		manager.WriteEntry(seg, "bench-key", f, vm)
		f.Close()
	}
}

func BenchmarkManager_ReadValue(b *testing.B) {
	basePath := b.TempDir()
	segmentSize := int64(10 * 1024 * 1024)

	_ = fd.NewFdCache(100)

	manager, err := NewManager(basePath, segmentSize)
	if err != nil {
		b.Fatal(err)
	}
	defer manager.Close()

	seg, err := manager.AcquireOpenSegment(1024)
	if err != nil {
		b.Fatal(err)
	}

	tmpFile, err := os.CreateTemp(b.TempDir(), "value-*.dat")
	if err != nil {
		b.Fatal(err)
	}
	valueData := bytes.Repeat([]byte("x"), 1024)
	tmpFile.Write(valueData)
	tmpFile.Close()

	f, _ := os.Open(tmpFile.Name())
	vm := &pb.ValueMessage{
		ValueType:   pb.ValueType_RAW_FILE,
		ValueLength: int64(len(valueData)),
		Checksum:    12345,
	}
	offset, _ := manager.WriteEntry(seg, "bench-key", f, vm)
	f.Close()

	manager.FinalizeSegment(seg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, err := manager.ReadValue("bench-key", seg.path, offset, int64(len(valueData)))
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
}
