// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
)

func TestSortCPUPartsPerMillion(t *testing.T) {
	profile := testProfile(
		testMessage(
			testPackedField(1, 1, 3),
			testPackedField(2, 1, 30),
		),
		testMessage(
			testBytesField(1, testVarintField(1, 2)),
			testPackedField(2, 1, 70),
		),
	)

	path := filepath.Join(t.TempDir(), "cpu.pprof")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := gzip.NewWriter(file)
	if _, err := writer.Write(profile); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	parsed, err := readTopologyProfile(path)
	if err != nil {
		t.Fatalf("readTopologyProfile: %v", err)
	}
	ppm, err := parsed.sortCPUPartsPerMillion()
	if err != nil {
		t.Fatalf("sortCPUPartsPerMillion: %v", err)
	}
	if ppm != 300_000 {
		t.Errorf("sortCPUPartsPerMillion() = %d, want 300000", ppm)
	}
}

func testProfile(samples ...[]byte) []byte {
	var fields [][]byte
	fields = append(fields,
		testBytesField(1, testVarintField(1, 4)),
		testBytesField(1, testVarintField(1, 1)),
	)
	for _, sample := range samples {
		fields = append(fields, testBytesField(2, sample))
	}
	fields = append(fields,
		testBytesField(4, testMessage(testVarintField(1, 1), testBytesField(4, testVarintField(1, 1)))),
		testBytesField(4, testMessage(testVarintField(1, 2), testBytesField(4, testVarintField(1, 2)))),
		testBytesField(4, testMessage(testVarintField(1, 3), testBytesField(4, testVarintField(1, 3)))),
		testBytesField(5, testMessage(testVarintField(1, 1), testVarintField(2, 2))),
		testBytesField(5, testMessage(testVarintField(1, 2), testVarintField(2, 3))),
		testBytesField(5, testMessage(testVarintField(1, 3), testVarintField(2, 5))),
	)
	for _, value := range []string{"", "cpu", "sort.Slice", "application", "samples", "github.com/tigrisdata/ocache/coordinator/ring.(*RingManager).GetNodeTokens"} {
		fields = append(fields, testBytesField(6, []byte(value)))
	}
	return testMessage(fields...)
}

func testMessage(fields ...[]byte) []byte {
	return bytes.Join(fields, nil)
}

func testVarintField(number int, value uint64) []byte {
	return append(testKey(number, 0), testVarint(value)...)
}

func testPackedField(number int, values ...uint64) []byte {
	var data []byte
	for _, value := range values {
		data = append(data, testVarint(value)...)
	}
	return testBytesField(number, data)
}

func testBytesField(number int, data []byte) []byte {
	field := testKey(number, 2)
	field = append(field, testVarint(uint64(len(data)))...)
	return append(field, data...)
}

func testKey(number, wire int) []byte {
	return testVarint(uint64(number<<3 | wire))
}

func testVarint(value uint64) []byte {
	var data []byte
	for value >= 0x80 {
		data = append(data, byte(value)|0x80)
		value >>= 7
	}
	return append(data, byte(value))
}
