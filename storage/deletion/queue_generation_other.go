// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package deletion

import (
	"fmt"
	"os"
	"reflect"
	"strings"
)

// fileGeneration records the pathname identity used by the deletion queue.
// Device/inode-style fields are selected reflectively so this fallback remains
// portable across the FileInfo implementations used by supported platforms.
func fileGeneration(filepath string) string {
	info, err := os.Stat(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return missingFileGeneration
		}
		return ""
	}

	sys := reflect.ValueOf(info.Sys())
	if sys.IsValid() {
		if sys.Kind() == reflect.Ptr {
			if sys.IsNil() {
				sys = reflect.Value{}
			} else {
				sys = sys.Elem()
			}
		}
	}
	identityFields := make([]string, 0, 4)
	if sys.IsValid() && sys.Kind() == reflect.Struct {
		for _, name := range []string{"Dev", "Ino", "VolumeSerialNumber", "FileIndexHigh", "FileIndexLow"} {
			field := sys.FieldByName(name)
			if field.IsValid() && field.CanInterface() {
				identityFields = append(identityFields, name+"="+fmt.Sprint(field.Interface()))
			}
		}
	}
	if len(identityFields) > 0 {
		return "file:" + fmt.Sprintf("%T", info.Sys()) + ":" + strings.Join(identityFields, ",")
	}
	return fmt.Sprintf("file:%T:size=%d:mtime=%d:mode=%d", info.Sys(), info.Size(), info.ModTime().UnixNano(), info.Mode())
}

func sameFileGeneration(filepath, expected string) (matches, comparable bool) {
	actual := fileGeneration(filepath)
	if actual == "" {
		return false, false
	}
	return actual == expected, true
}
