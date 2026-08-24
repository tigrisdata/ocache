// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package files

import (
	"hash/crc32"
	"io"

	"github.com/tigrisdata/ocache/common/bufferpool"
)

// copyRawFilePayload streams a raw-file payload while calculating its checksum.
func copyRawFilePayload(writer io.Writer, reader io.Reader) (uint32, int64, error) {
	buf, release := bufferpool.AcquireBuffer(1 << 20) // 1 MiB
	defer release()

	hash := crc32.NewIEEE()
	bytesWritten, err := io.CopyBuffer(io.MultiWriter(writer, hash), reader, buf)
	if err != nil {
		return 0, 0, err
	}

	return hash.Sum32(), bytesWritten, nil
}
