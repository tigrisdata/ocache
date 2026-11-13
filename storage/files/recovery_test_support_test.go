// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package files

import (
	"os"

	"github.com/tigrisdata/ocache/storage/metadata"
	pb "github.com/tigrisdata/ocache/storage/proto"
)

// NewRecoveryManagerForTest constructs a recovery manager with injectable
// validation dependencies for tests in external packages.
func NewRecoveryManagerForTest(
	meta *metadata.MetaDB,
	filesPath string,
	numWorkers int,
	getMetadata func(*metadata.MetaDB, string) (*pb.ValueMessage, error),
	statFile func(string) (os.FileInfo, error),
) *RecoveryManager {
	recovery := NewRecoveryManager(meta, filesPath, numWorkers)
	recovery.getMetadata = getMetadata
	recovery.statFile = statFile
	return recovery
}
