// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package deletion

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

const linuxFileGenerationPrefix = "file:*syscall.Stat_t:Dev="

// fileGeneration records the pathname identity used by the deletion queue. The
// Linux syscall path avoids constructing FileInfo and reflectively formatting
// its identity fields for every deletion attempt.
func fileGeneration(filepath string) string {
	var stat syscall.Stat_t
	if err := syscall.Stat(filepath, &stat); err != nil {
		if os.IsNotExist(err) {
			return missingFileGeneration
		}
		return ""
	}

	generation := make([]byte, 0, 64)
	generation = append(generation, linuxFileGenerationPrefix...)
	generation = strconv.AppendUint(generation, uint64(stat.Dev), 10)
	generation = append(generation, ",Ino="...)
	generation = strconv.AppendUint(generation, uint64(stat.Ino), 10)
	return string(generation)
}

// sameFileGeneration compares a known generation without allocating a new
// formatted generation string. The second result is false only when stat could
// not establish the current pathname identity.
func sameFileGeneration(filepath, expected string) (matches, comparable bool) {
	var stat syscall.Stat_t
	if err := syscall.Stat(filepath, &stat); err != nil {
		if os.IsNotExist(err) {
			return expected == missingFileGeneration, true
		}
		return false, false
	}
	if expected == missingFileGeneration {
		return false, true
	}

	if !strings.HasPrefix(expected, linuxFileGenerationPrefix) {
		// Preserve compatibility with an older platform-specific representation.
		return fileGeneration(filepath) == expected, true
	}
	remainder := expected[len(linuxFileGenerationPrefix):]
	separator := strings.Index(remainder, ",Ino=")
	if separator < 0 {
		return fileGeneration(filepath) == expected, true
	}
	dev, err := strconv.ParseUint(remainder[:separator], 10, 64)
	if err != nil {
		return fileGeneration(filepath) == expected, true
	}
	ino, err := strconv.ParseUint(remainder[separator+len(",Ino="):], 10, 64)
	if err != nil {
		return fileGeneration(filepath) == expected, true
	}
	return uint64(stat.Dev) == dev && uint64(stat.Ino) == ino, true
}
