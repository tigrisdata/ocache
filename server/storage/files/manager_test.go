package files

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tigrisdata/ocache/server/storage/fd"
)

func TestNewFileManager(t *testing.T) {
	basePath := t.TempDir()

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatalf("NewFileManager failed: %v", err)
	}
	if fm == nil {
		t.Fatal("NewFileManager returned nil")
	}

	expectedPath := filepath.Join(basePath, "files")
	if fm.filesPath != expectedPath {
		t.Errorf("Expected filesPath %s, got %s", expectedPath, fm.filesPath)
	}

	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Error("Files directory was not created")
	}
}

func TestNewFileManager_ExistingDirectory(t *testing.T) {
	basePath := t.TempDir()
	filesPath := filepath.Join(basePath, "files")

	if err := os.MkdirAll(filesPath, 0o755); err != nil {
		t.Fatal(err)
	}

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatalf("NewFileManager failed: %v", err)
	}
	if fm == nil {
		t.Fatal("NewFileManager returned nil")
	}
}

func TestFileManager_Write(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	key := "test-key"
	content := "test content data"
	reader := strings.NewReader(content)

	path, checksum, bytesWritten, err := fm.Write(key, reader)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if path == "" {
		t.Error("Write returned empty path")
	}

	if !strings.HasPrefix(path, fm.filesPath) {
		t.Errorf("Path %s does not start with filesPath %s", path, fm.filesPath)
	}

	if checksum == 0 {
		t.Error("Write returned zero checksum")
	}

	if bytesWritten != int64(len(content)) {
		t.Errorf("Write returned %d bytes, expected %d", bytesWritten, len(content))
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read written file: %v", err)
	}

	if string(data) != content {
		t.Errorf("File content mismatch: got %q, want %q", string(data), content)
	}
}

func TestFileManager_WriteLargeFile(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	key := "large-key"
	size := 2 * 1024 * 1024
	content := bytes.Repeat([]byte("a"), size)
	reader := bytes.NewReader(content)

	path, checksum, bytesWritten, err := fm.Write(key, reader)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if bytesWritten != int64(size) {
		t.Errorf("Write returned %d bytes, expected %d", bytesWritten, size)
	}

	if checksum == 0 {
		t.Error("Write returned zero checksum")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() != int64(size) {
		t.Errorf("File size is %d, expected %d", info.Size(), size)
	}
}

func TestFileManager_Read(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	key := "read-test"
	content := "test read content"
	reader := strings.NewReader(content)

	path, _, bytesWritten, err := fm.Write(key, reader)
	if err != nil {
		t.Fatal(err)
	}

	rc, err := fm.Read(path, bytesWritten)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != content {
		t.Errorf("Read content mismatch: got %q, want %q", string(data), content)
	}
}

func TestFileManager_ReadInvalidPath(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fm.Read("", 100)
	if err == nil {
		t.Error("Expected error for empty path")
	} else if err.Error() == "" {
		t.Error("Expected error message for empty path")
	}

	_, err = fm.Read("/some/path", 0)
	if err == nil {
		t.Error("Expected error for zero length")
	} else if err.Error() == "" {
		t.Error("Expected error message for zero length")
	}

	_, err = fm.Read("/some/path", -1)
	if err == nil {
		t.Error("Expected error for negative length")
	} else if err.Error() == "" {
		t.Error("Expected error message for negative length")
	}
}

func TestFileManager_ReadNonExistent(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fm.Read("/nonexistent/file.txt", 100)
	if err == nil {
		t.Error("Expected error for non-existent file")
	} else if err.Error() == "" {
		t.Error("Expected error message for non-existent file")
	}
}

func TestFileManager_Remove(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	key := "remove-test"
	content := "test remove content"
	reader := strings.NewReader(content)

	path, _, _, err := fm.Write(key, reader)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("File should exist before removal")
	}

	err = fm.Remove(path)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("File should not exist after removal")
	}
}

func TestFileManager_RemoveNonExistent(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	err = fm.Remove("/nonexistent/file.txt")
	if err != nil {
		t.Error("Remove should not error for non-existent file")
	}
}

func TestFileManager_ConcurrentWrites(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	numWorkers := 10

	paths := make([]string, numWorkers)
	checksums := make([]uint32, numWorkers)

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(idx int) {
			defer wg.Done()
			key := "concurrent-key"
			content := strings.Repeat("x", idx+1)
			reader := strings.NewReader(content)

			path, checksum, bytes, err := fm.Write(key, reader)
			if err != nil {
				t.Errorf("Worker %d: Write failed: %v", idx, err)
				return
			}

			if bytes != int64(len(content)) {
				t.Errorf("Worker %d: wrong bytes written", idx)
			}

			paths[idx] = path
			checksums[idx] = checksum
		}(i)
	}

	wg.Wait()

	for i := 0; i < numWorkers; i++ {
		if paths[i] == "" {
			t.Errorf("Worker %d produced empty path", i)
		}
		if checksums[i] == 0 {
			t.Errorf("Worker %d produced zero checksum", i)
		}
	}
}

func TestFileManager_ConcurrentReadWrite(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	key := "concurrent-rw"
	content := "test concurrent read write"
	reader := strings.NewReader(content)

	path, _, length, err := fm.Write(key, reader)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	numReaders := 5
	numWriters := 3

	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func(idx int) {
			defer wg.Done()
			rc, err := fm.Read(path, length)
			if err != nil {
				t.Errorf("Reader %d: Read failed: %v", idx, err)
				return
			}
			defer rc.Close()

			data, err := io.ReadAll(rc)
			if err != nil {
				t.Errorf("Reader %d: ReadAll failed: %v", idx, err)
				return
			}

			if string(data) != content {
				t.Errorf("Reader %d: content mismatch", idx)
			}
		}(i)
	}

	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(idx int) {
			defer wg.Done()
			newKey := "new-key"
			newContent := "new content"
			newReader := strings.NewReader(newContent)

			_, _, _, err := fm.Write(newKey, newReader)
			if err != nil {
				t.Errorf("Writer %d: Write failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()
}

func TestFileReadCloser(t *testing.T) {
	called := false
	rc := &fileReadCloser{
		Reader: strings.NewReader("test"),
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

func TestFileReadCloser_NilCallback(t *testing.T) {
	rc := &fileReadCloser{
		Reader:  strings.NewReader("test"),
		onClose: nil,
	}

	err := rc.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestFileManager_WriteReadRemoveFlow(t *testing.T) {
	basePath := t.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		t.Fatal(err)
	}

	key := "flow-test"
	content := "complete flow test content"

	path, checksum1, length, err := fm.Write(key, strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	rc, err := fm.Read(path, length)
	if err != nil {
		t.Fatal(err)
	}

	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != content {
		t.Error("Content mismatch after read")
	}

	path2, checksum2, _, err := fm.Write(key, strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	if checksum1 != checksum2 {
		t.Error("Same content should produce same checksum")
	}

	err = fm.Remove(path)
	if err != nil {
		t.Fatal(err)
	}

	err = fm.Remove(path2)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkFileManager_Write(b *testing.B) {
	basePath := b.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		b.Fatal(err)
	}

	content := bytes.Repeat([]byte("x"), 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(content)
		_, _, _, err := fm.Write("bench-key", reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileManager_Read(b *testing.B) {
	basePath := b.TempDir()
	_ = fd.NewFdCache(100)

	fm, err := NewFileManager(basePath)
	if err != nil {
		b.Fatal(err)
	}

	content := bytes.Repeat([]byte("x"), 1024)
	path, _, length, err := fm.Write("bench-key", bytes.NewReader(content))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, err := fm.Read(path, length)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
}
