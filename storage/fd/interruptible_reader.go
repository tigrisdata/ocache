// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package fd

import (
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// InterruptibleReadCloser owns a private file descriptor. Interrupt closes only
// that descriptor; Close also runs the optional ownership callback. The
// descriptor must not be shared with another reader. Callers that need a cached
// descriptor should continue to use the regular FdCache APIs instead.
type InterruptibleReadCloser struct {
	io.ReadSeeker
	file    *os.File
	onClose func()

	interruptOnce sync.Once
	closeOnce     sync.Once
}

// DuplicateFile makes a private descriptor for a cached file. The duplicate
// has its own lifetime, so closing it cannot invalidate the cached descriptor
// held by another reader.
func DuplicateFile(file *os.File) (*os.File, error) {
	if file == nil {
		return nil, fmt.Errorf("cannot duplicate a nil file")
	}
	duplicateFD, err := unix.Dup(int(file.Fd()))
	if err != nil {
		return nil, err
	}
	duplicate := os.NewFile(uintptr(duplicateFD), file.Name())
	if duplicate == nil {
		_ = unix.Close(duplicateFD)
		return nil, fmt.Errorf("failed to create duplicate file descriptor for %s", file.Name())
	}
	return duplicate, nil
}

// NewInterruptibleReadCloser creates a read closer for a private descriptor.
// onClose is called exactly once by Close, after the descriptor close is
// initiated. Callers should use Interrupt before Close when a read is blocked.
func NewInterruptibleReadCloser(file *os.File, reader io.ReadSeeker, onClose func()) *InterruptibleReadCloser {
	return &InterruptibleReadCloser{
		ReadSeeker: reader,
		file:       file,
		onClose:    onClose,
	}
}

// Interrupt closes the private descriptor. Any read which outlives the close
// cannot affect another reader's cached descriptor because the descriptor is
// not shared.
func (r *InterruptibleReadCloser) Interrupt() {
	if r == nil {
		return
	}
	r.interruptOnce.Do(func() {
		if r.file != nil {
			_ = r.file.Close()
		}
	})
}

// Close is idempotent so it can safely race with Interrupt. It runs the
// optional ownership callback only once, even if a private read is still
// unwinding.
func (r *InterruptibleReadCloser) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if r.file != nil {
			_ = r.file.Close()
		}
		if r.onClose != nil {
			r.onClose()
		}
	})
	return nil
}
