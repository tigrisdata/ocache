// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	storageErrors "github.com/tigrisdata/ocache/storage/errors"
)

var errRetryPosition = errors.New("injected retry reposition failure")

type unavailablePositionReader struct {
	*bytes.Reader
}

func (r *unavailablePositionReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent && offset == 0 {
		return 0, errRetryPosition
	}
	return r.Reader.Seek(offset, whence)
}

type failRetrySeekReader struct {
	*bytes.Reader
	start      int64
	consumed   bool
	restoreErr error
}

func (r *failRetrySeekReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.consumed = true
	}
	return n, err
}

func (r *failRetrySeekReader) Seek(offset int64, whence int) (int64, error) {
	if r.consumed && whence == io.SeekStart && offset == r.start {
		return 0, r.restoreErr
	}
	return r.Reader.Seek(offset, whence)
}

func TestPutLocalReaderRetryPosition(t *testing.T) {
	data := []byte("already-consumed|replay-this-suffix")
	start := int64(len("already-consumed|"))
	body := bytes.NewReader(data)
	_, err := body.Seek(start, io.SeekStart)
	require.NoError(t, err)

	var attempts int
	var firstAttempt, retryAttempt []byte
	put := func() error {
		attempts++
		if attempts == 1 {
			partial := make([]byte, 5)
			n, err := body.Read(partial)
			require.NoError(t, err)
			firstAttempt = append([]byte(nil), partial[:n]...)
			return storageErrors.NewTemporaryError("Put", "key", errors.New("injected storage failure"))
		}
		var err error
		retryAttempt, err = io.ReadAll(body)
		return err
	}

	require.NoError(t, putLocalWithRetry(context.Background(), "key", body, put))
	require.Equal(t, 2, attempts)
	require.Equal(t, data[start:start+5], firstAttempt, "the initial attempt must read from the call-entry position")
	require.Equal(t, data[start:], retryAttempt, "the retry must replay the full unread suffix")
}

func TestPutLocalReaderWithoutPositionAttemptsOnce(t *testing.T) {
	data := []byte("already-consumed|read-once-suffix")
	start := int64(len("already-consumed|"))
	reader := bytes.NewReader(data)
	_, err := reader.Seek(start, io.SeekStart)
	require.NoError(t, err)
	body := &unavailablePositionReader{Reader: reader}

	var attempts int
	var firstRead []byte
	storageFailure := errors.New("injected storage failure")
	put := func() error {
		attempts++
		buf := make([]byte, 5)
		n, err := body.Read(buf)
		require.NoError(t, err)
		if attempts == 1 {
			firstRead = append([]byte(nil), buf[:n]...)
			return storageErrors.NewTemporaryError("Put", "key", storageFailure)
		}
		return nil
	}

	err = putLocalWithRetry(context.Background(), "key", body, put)
	require.ErrorIs(t, err, storageFailure)
	require.Equal(t, 1, attempts, "an unavailable starting position must disable retries")
	require.Equal(t, data[start:start+5], firstRead, "the single attempt must read from the current position")
}

func TestPutLocalReaderRetrySeekFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		seekError error
	}{
		{name: "ordinary seek error", seekError: errRetryPosition},
		{name: "retryable-typed seek error", seekError: storageErrors.NewTemporaryError("Seek", "key", errors.New("injected seek failure"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte("prefix|body-that-fails-before-commit")
			start := int64(len("prefix|"))
			body := &failRetrySeekReader{
				Reader:     bytes.NewReader(data),
				start:      start,
				restoreErr: tc.seekError,
			}
			_, err := body.Seek(start, io.SeekStart)
			require.NoError(t, err)

			var attempts int
			put := func() error {
				attempts++
				if attempts == 1 {
					partial := make([]byte, 5)
					_, err := body.Read(partial)
					require.NoError(t, err)
					return storageErrors.NewTemporaryError("Put", "key", errors.New("injected storage failure"))
				}
				return nil
			}

			err = putLocalWithRetry(context.Background(), "key", body, put)
			require.EqualError(t, err, tc.seekError.Error())
			if tc.seekError == errRetryPosition {
				require.ErrorIs(t, err, errRetryPosition)
			} else {
				require.False(t, storageErrors.IsRetryable(err), "a failed reposition must stop the storage retry loop")
			}
			require.True(t, body.consumed)
			require.Equal(t, 1, attempts, "storage must not be called after a failed retry reposition")
		})
	}
}
