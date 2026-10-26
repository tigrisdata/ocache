// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	stor "github.com/tigrisdata/ocache/storage"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
)

const putStreamReaderPositionInlineThreshold = 8

// readerOnly hides the seek methods of its underlying reader.
type readerOnly struct {
	io.Reader
}

type blockingPositionSeeker struct {
	*bytes.Reader
	seekEntered chan struct{}
	releaseSeek <-chan struct{}
}

func (r *blockingPositionSeeker) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekCurrent {
		select {
		case r.seekEntered <- struct{}{}:
		default:
		}
		<-r.releaseSeek
	}
	return r.Reader.Seek(offset, whence)
}

func TestPutStreamReaderPosition(t *testing.T) {
	client, err := New(&Config{
		DiskPath:        t.TempDir(),
		TTL:             time.Hour,
		InlineThreshold: putStreamReaderPositionInlineThreshold,
		Storage: &stor.StorageConfig{
			CleanupInterval:     time.Hour,
			DisableRecompaction: true,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	storeAndCheck := func(t *testing.T, key string, body io.Reader, want []byte) {
		t.Helper()
		require.NoError(t, client.PutStream(ctx, key, body, 0))
		got, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.NotNil(t, got, "PutStream should create the key even when the unread suffix is empty")
		require.Equal(t, want, got)
	}

	t.Run("bytes reader starts at current position", func(t *testing.T) {
		data := []byte("consumed-prefix|unread-bytes")
		position := int64(len("consumed-prefix|"))
		body := bytes.NewReader(data)
		_, err := body.Seek(position, io.SeekStart)
		require.NoError(t, err)
		storeAndCheck(t, "bytes-position", body, data[position:])
	})

	t.Run("file starts at current position", func(t *testing.T) {
		data := []byte("file-prefix|remaining-file-data")
		position := int64(len("file-prefix|"))
		path := filepath.Join(t.TempDir(), "body")
		require.NoError(t, os.WriteFile(path, data, 0o600))
		body, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = body.Close() })
		_, err = body.Seek(position, io.SeekStart)
		require.NoError(t, err)
		storeAndCheck(t, "file-position", body, data[position:])
	})

	t.Run("zero offset", func(t *testing.T) {
		data := []byte("whole value from zero")
		storeAndCheck(t, "zero-position", bytes.NewReader(data), data)
	})

	t.Run("end of input", func(t *testing.T) {
		body := bytes.NewReader([]byte("already consumed"))
		_, err := body.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		storeAndCheck(t, "end-position", body, []byte{})
	})

	t.Run("non-seekable reader starts at current position", func(t *testing.T) {
		data := []byte("wrapped-prefix|non-seekable-remainder")
		position := int64(len("wrapped-prefix|"))
		body := bytes.NewReader(data)
		_, err := body.Seek(position, io.SeekStart)
		require.NoError(t, err)
		storeAndCheck(t, "non-seekable-position", readerOnly{Reader: body}, data[position:])
	})

	t.Run("pipe-backed file is readable when seek is unsupported", func(t *testing.T) {
		data := []byte("pipe data from the reader's current position")
		readEnd, writeEnd, err := os.Pipe()
		require.NoError(t, err)
		writerDone := make(chan error, 1)
		go func() {
			n, writeErr := writeEnd.Write(data)
			closeErr := writeEnd.Close()
			if writeErr == nil && n != len(data) {
				writeErr = io.ErrShortWrite
			}
			if writeErr != nil {
				writerDone <- writeErr
				return
			}
			writerDone <- closeErr
		}()

		putErr := client.PutStream(ctx, "pipe-file", readEnd, 0)
		readCloseErr := readEnd.Close()
		writeErr := <-writerDone
		require.NoError(t, putErr)
		require.NoError(t, readCloseErr)
		require.NoError(t, writeErr)
		got, err := client.Get(ctx, "pipe-file")
		require.NoError(t, err)
		require.Equal(t, data, got)
	})

	t.Run("canceled request does not consume or store an unpositionable seeker", func(t *testing.T) {
		data := []byte("already-canceled pipe payload")
		readEnd, writeEnd, err := os.Pipe()
		require.NoError(t, err)
		t.Cleanup(func() { _ = readEnd.Close() })
		t.Cleanup(func() { _ = writeEnd.Close() })

		n, err := writeEnd.Write(data)
		require.NoError(t, err)
		require.Equal(t, len(data), n)
		require.NoError(t, writeEnd.Close())

		canceled, cancel := context.WithCancel(ctx)
		cancel()
		putErr := client.PutStream(canceled, "canceled-pipe", readEnd, 0)
		gotType, ok := storageErrors.GetType(putErr)
		require.True(t, ok, "a canceled seekable input must retain its timeout error")
		require.Equal(t, storageErrors.TypeTimeout, gotType)

		remaining, err := io.ReadAll(readEnd)
		require.NoError(t, err)
		require.Equal(t, data, remaining, "cancellation before the storage attempt must not consume the reader")
		got, err := client.Get(ctx, "canceled-pipe")
		require.NoError(t, err)
		require.Nil(t, got, "cancellation before the storage attempt must not create the key")
	})

	t.Run("pre-canceled request returns before a blocking position query", func(t *testing.T) {
		data := []byte("already-canceled blocking-seeker payload")
		releaseSeek := make(chan struct{})
		body := &blockingPositionSeeker{
			Reader:      bytes.NewReader(data),
			seekEntered: make(chan struct{}, 1),
			releaseSeek: releaseSeek,
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		done := make(chan error, 1)
		go func() {
			done <- client.PutStream(canceled, "canceled-blocking-seeker", body, 0)
		}()

		select {
		case putErr := <-done:
			gotType, ok := storageErrors.GetType(putErr)
			require.True(t, ok, "a canceled seekable input must retain its timeout error")
			require.Equal(t, storageErrors.TypeTimeout, gotType)
		case <-body.seekEntered:
			close(releaseSeek)
			putErr := <-done
			t.Fatalf("PutStream queried the reader before the canceled-context check: %v", putErr)
		case <-time.After(5 * time.Second):
			close(releaseSeek)
			putErr := <-done
			t.Fatalf("PutStream did not return before a blocked position query: %v", putErr)
		}

		remaining, err := io.ReadAll(body.Reader)
		require.NoError(t, err)
		require.Equal(t, data, remaining, "the canceled request must not consume the reader")
		got, err := client.Get(ctx, "canceled-blocking-seeker")
		require.NoError(t, err)
		require.Nil(t, got, "the canceled request must not create the key")
	})
}
