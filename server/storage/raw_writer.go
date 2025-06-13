package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// RawWriter manages all raw files in the raw directory
type RawWriter struct {
	rawFilesPath string
	// Use a map of mutexes to lock individual files
	fileLocks sync.Map
	// Global mutex only for directory operations
	dirMu sync.RWMutex
}

// NewRawWriter creates a new RawWriter for managing raw files
func NewRawWriter(rawFilesPath string) (*RawWriter, error) {
	// Create the raw files directory if it doesn't exist
	if err := os.MkdirAll(rawFilesPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create raw files directory: %w", err)
	}

	return &RawWriter{
		rawFilesPath: rawFilesPath,
	}, nil
}

// getFileLock returns a mutex for the given key, creating it if it doesn't exist
func (rw *RawWriter) getFileLock(key string) *sync.Mutex {
	lock, _ := rw.fileLocks.LoadOrStore(key, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// Write writes a value to a raw file for the given key
func (rw *RawWriter) Write(key string, reader io.Reader) (string, error) {
	// Get file-specific lock
	fileLock := rw.getFileLock(key)
	fileLock.Lock()
	defer fileLock.Unlock()

	// Create a new file for this key
	filePath := filepath.Join(rw.rawFilesPath, key)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return "", fmt.Errorf("failed to create raw file for key %s: %w", key, err)
	}
	defer file.Close()

	// Write the value
	if _, err := io.Copy(file, reader); err != nil {
		// Clean up the file if write fails
		os.Remove(filePath)
		return "", fmt.Errorf("failed to write value to raw file: %w", err)
	}

	return filePath, nil
}

// Read reads a value from a raw file for the given key
func (rw *RawWriter) Read(key string) (io.ReadCloser, error) {
	// Get file-specific lock
	fileLock := rw.getFileLock(key)
	fileLock.Lock()
	defer fileLock.Unlock()

	filePath := filepath.Join(rw.rawFilesPath, key)
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("raw file not found for key %s", key)
		}
		return nil, fmt.Errorf("failed to open raw file for key %s: %w", key, err)
	}

	return file, nil
}

// Delete removes a raw file for the given key
func (rw *RawWriter) Delete(key string) error {
	// Get file-specific lock
	fileLock := rw.getFileLock(key)
	fileLock.Lock()
	defer fileLock.Unlock()

	filePath := filepath.Join(rw.rawFilesPath, key)
	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			return nil // File doesn't exist, consider it deleted
		}
		return fmt.Errorf("failed to delete raw file for key %s: %w", key, err)
	}

	// Remove the lock from the map
	rw.fileLocks.Delete(key)

	return nil
}

// List returns all keys that have raw files
func (rw *RawWriter) List() ([]string, error) {
	rw.dirMu.RLock()
	defer rw.dirMu.RUnlock()

	entries, err := os.ReadDir(rw.rawFilesPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read raw files directory: %w", err)
	}

	var keys []string
	for _, entry := range entries {
		if !entry.IsDir() {
			keys = append(keys, entry.Name())
		}
	}

	return keys, nil
}

// Exists checks if a raw file exists for the given key
func (rw *RawWriter) Exists(key string) bool {
	rw.dirMu.RLock()
	defer rw.dirMu.RUnlock()

	filePath := filepath.Join(rw.rawFilesPath, key)
	_, err := os.Stat(filePath)
	return err == nil
}

// Cleanup removes all raw files
func (rw *RawWriter) Cleanup() error {
	rw.dirMu.Lock()
	defer rw.dirMu.Unlock()

	entries, err := os.ReadDir(rw.rawFilesPath)
	if err != nil {
		return fmt.Errorf("failed to read raw files directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			filePath := filepath.Join(rw.rawFilesPath, entry.Name())
			if err := os.Remove(filePath); err != nil {
				return fmt.Errorf("failed to delete raw file %s: %w", entry.Name(), err)
			}
			// Remove the lock from the map
			rw.fileLocks.Delete(entry.Name())
		}
	}

	return nil
}
