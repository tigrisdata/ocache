// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingListReader struct {
	started    chan struct{}
	finish     chan struct{}
	closed     chan struct{}
	done       chan struct{}
	finishOnce sync.Once
	closeOnce  sync.Once
}

func newBlockingListReader() *blockingListReader {
	return &blockingListReader{
		started: make(chan struct{}),
		finish:  make(chan struct{}),
		closed:  make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (r *blockingListReader) Read([]byte) (int, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
	}
	<-r.finish
	close(r.done)
	return 0, errors.New("injected read completion")
}

func (r *blockingListReader) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}

func (r *blockingListReader) Interrupt() {}

func (r *blockingListReader) finishRead() {
	r.finishOnce.Do(func() { close(r.finish) })
}

func TestReadListPayloadCancelsAndClosesNeverReturningRead(t *testing.T) {
	reader := newBlockingListReader()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payloadReader := newListPayloadReader()
	defer payloadReader.close()

	resultCh := make(chan error, 1)
	go func() {
		_, err := payloadReader.read(ctx, reader)
		resultCh <- err
	}()

	select {
	case <-reader.started:
	case <-time.After(time.Second):
		t.Fatal("injected payload read did not start")
	}
	cancel()

	select {
	case err := <-resultCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("payload read did not return after cancellation")
	}

	select {
	case <-reader.closed:
	case <-time.After(time.Second):
		t.Fatal("payload reader was not closed after cancellation")
	}

	// The injected read ignores Interrupt and Close until its source is
	// released. The list operation must still return after closing its private
	// reader, while the worker is allowed to finish independently.
	reader.finishRead()
	select {
	case <-reader.done:
	case <-time.After(time.Second):
		t.Fatal("injected payload reader did not finish cleanup")
	}
}

var _ io.ReadCloser = (*blockingListReader)(nil)
