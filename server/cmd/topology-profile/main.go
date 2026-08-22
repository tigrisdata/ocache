// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

// topology-profile reports the CPU-profile share attributable to the token sort
// used by the CacheService.GetTopology benchmark. It reads the pprof protobuf
// directly so benchmark collection does not depend on an external pprof tool.
package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

type protobufField struct {
	number int
	wire   int
	value  uint64
	data   []byte
}

type profileSample struct {
	locations []uint64
	values    []int64
}

type profileSampleType struct {
	typeStringIndex uint64
}

type profileFunction struct {
	id              uint64
	nameStringIndex uint64
}

type topologyProfile struct {
	sampleTypes []profileSampleType
	samples     []profileSample
	locations   map[uint64][]uint64
	functions   map[uint64]profileFunction
	strings     []string
}

func main() {
	profilePath := flag.String("profile", "", "path to a gzip-compressed CPU profile")
	flag.Parse()
	if *profilePath == "" {
		fatal(errors.New("-profile is required"))
	}

	profile, err := readTopologyProfile(*profilePath)
	if err != nil {
		fatal(err)
	}
	ppm, err := profile.sortCPUPartsPerMillion()
	if err != nil {
		fatal(err)
	}
	fmt.Println(ppm)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "topology-profile:", err)
	os.Exit(1)
}

func readTopologyProfile(path string) (*topologyProfile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open profile: %w", err)
	}
	defer file.Close()

	reader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("open compressed profile: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read profile: %w", err)
	}

	profile := &topologyProfile{
		locations: make(map[uint64][]uint64),
		functions: make(map[uint64]profileFunction),
	}
	if err := walkProtobuf(data, func(field protobufField) error {
		switch field.number {
		case 1:
			sampleType, err := readSampleType(field)
			if err != nil {
				return err
			}
			profile.sampleTypes = append(profile.sampleTypes, sampleType)
		case 2:
			sample, err := readSample(field)
			if err != nil {
				return err
			}
			profile.samples = append(profile.samples, sample)
		case 4:
			locationID, functions, err := readLocation(field)
			if err != nil {
				return err
			}
			profile.locations[locationID] = functions
		case 5:
			function, err := readFunction(field)
			if err != nil {
				return err
			}
			profile.functions[function.id] = function
		case 6:
			if field.wire != 2 {
				return fmt.Errorf("string table has wire type %d", field.wire)
			}
			profile.strings = append(profile.strings, string(field.data))
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("decode profile: %w", err)
	}

	if len(profile.samples) == 0 {
		return nil, errors.New("profile contains no samples")
	}
	if len(profile.strings) == 0 || profile.strings[0] != "" {
		return nil, errors.New("profile has an invalid string table")
	}
	return profile, nil
}

func (p *topologyProfile) sortCPUPartsPerMillion() (uint64, error) {
	cpuIndex := -1
	for index, sampleType := range p.sampleTypes {
		name, err := p.stringAt(sampleType.typeStringIndex)
		if err != nil {
			return 0, fmt.Errorf("read sample type: %w", err)
		}
		if name == "cpu" {
			cpuIndex = index
			break
		}
	}
	if cpuIndex == -1 {
		return 0, errors.New("profile has no cpu sample type")
	}

	var totalCPU, sortCPU uint64
	for _, sample := range p.samples {
		if len(sample.values) <= cpuIndex {
			return 0, fmt.Errorf("sample has %d values, need cpu index %d", len(sample.values), cpuIndex)
		}
		if sample.values[cpuIndex] <= 0 {
			continue
		}

		cpu := uint64(sample.values[cpuIndex])
		totalCPU += cpu
		if p.sampleHasSortFrame(sample) {
			sortCPU += cpu
		}
	}
	if totalCPU == 0 {
		return 0, errors.New("profile recorded no CPU time")
	}

	return (sortCPU*1_000_000 + totalCPU/2) / totalCPU, nil
}

func (p *topologyProfile) sampleHasSortFrame(sample profileSample) bool {
	hasSortFrame := false
	hasGetNodeTokensFrame := false
	for _, locationID := range sample.locations {
		for _, functionID := range p.locations[locationID] {
			function, ok := p.functions[functionID]
			if !ok {
				continue
			}
			name, err := p.stringAt(function.nameStringIndex)
			if err != nil {
				continue
			}
			hasSortFrame = hasSortFrame || isTopologySortFunction(name)
			hasGetNodeTokensFrame = hasGetNodeTokensFrame || isGetNodeTokensFunction(name)
		}
	}
	return hasSortFrame && hasGetNodeTokensFrame
}

func isTopologySortFunction(name string) bool {
	return strings.HasPrefix(name, "sort.") ||
		strings.HasPrefix(name, "slices.") ||
		strings.Contains(name, "/sort.") ||
		strings.Contains(name, "RingManager).GetNodeTokens.func")
}

func isGetNodeTokensFunction(name string) bool {
	return strings.Contains(name, "RingManager).GetNodeTokens")
}

func (p *topologyProfile) stringAt(index uint64) (string, error) {
	if index >= uint64(len(p.strings)) {
		return "", fmt.Errorf("string index %d is outside [0,%d)", index, len(p.strings))
	}
	return p.strings[index], nil
}

func readSampleType(field protobufField) (profileSampleType, error) {
	if field.wire != 2 {
		return profileSampleType{}, fmt.Errorf("sample type has wire type %d", field.wire)
	}

	var sampleType profileSampleType
	if err := walkProtobuf(field.data, func(field protobufField) error {
		if field.number == 1 {
			if field.wire != 0 {
				return fmt.Errorf("sample type name has wire type %d", field.wire)
			}
			sampleType.typeStringIndex = field.value
		}
		return nil
	}); err != nil {
		return profileSampleType{}, err
	}
	return sampleType, nil
}

func readSample(field protobufField) (profileSample, error) {
	if field.wire != 2 {
		return profileSample{}, fmt.Errorf("sample has wire type %d", field.wire)
	}

	var sample profileSample
	if err := walkProtobuf(field.data, func(field protobufField) error {
		switch field.number {
		case 1:
			values, err := readUint64s(field)
			if err != nil {
				return fmt.Errorf("read sample locations: %w", err)
			}
			sample.locations = append(sample.locations, values...)
		case 2:
			values, err := readInt64s(field)
			if err != nil {
				return fmt.Errorf("read sample values: %w", err)
			}
			sample.values = append(sample.values, values...)
		}
		return nil
	}); err != nil {
		return profileSample{}, err
	}
	return sample, nil
}

func readLocation(field protobufField) (uint64, []uint64, error) {
	if field.wire != 2 {
		return 0, nil, fmt.Errorf("location has wire type %d", field.wire)
	}

	var locationID uint64
	var functionIDs []uint64
	if err := walkProtobuf(field.data, func(field protobufField) error {
		switch field.number {
		case 1:
			if field.wire != 0 {
				return fmt.Errorf("location ID has wire type %d", field.wire)
			}
			locationID = field.value
		case 4:
			if field.wire != 2 {
				return fmt.Errorf("location line has wire type %d", field.wire)
			}
			functionID, err := readLineFunctionID(field.data)
			if err != nil {
				return err
			}
			if functionID != 0 {
				functionIDs = append(functionIDs, functionID)
			}
		}
		return nil
	}); err != nil {
		return 0, nil, err
	}
	if locationID == 0 {
		return 0, nil, errors.New("location has no ID")
	}
	return locationID, functionIDs, nil
}

func readLineFunctionID(data []byte) (uint64, error) {
	var functionID uint64
	if err := walkProtobuf(data, func(field protobufField) error {
		if field.number == 1 {
			if field.wire != 0 {
				return fmt.Errorf("line function ID has wire type %d", field.wire)
			}
			functionID = field.value
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return functionID, nil
}

func readFunction(field protobufField) (profileFunction, error) {
	if field.wire != 2 {
		return profileFunction{}, fmt.Errorf("function has wire type %d", field.wire)
	}

	var function profileFunction
	if err := walkProtobuf(field.data, func(field protobufField) error {
		switch field.number {
		case 1:
			if field.wire != 0 {
				return fmt.Errorf("function ID has wire type %d", field.wire)
			}
			function.id = field.value
		case 2:
			if field.wire != 0 {
				return fmt.Errorf("function name has wire type %d", field.wire)
			}
			function.nameStringIndex = field.value
		}
		return nil
	}); err != nil {
		return profileFunction{}, err
	}
	if function.id == 0 {
		return profileFunction{}, errors.New("function has no ID")
	}
	return function, nil
}

func readUint64s(field protobufField) ([]uint64, error) {
	switch field.wire {
	case 0:
		return []uint64{field.value}, nil
	case 2:
		var values []uint64
		for data := field.data; len(data) > 0; {
			value, remaining, err := readVarint(data)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
			data = remaining
		}
		return values, nil
	default:
		return nil, fmt.Errorf("integer list has wire type %d", field.wire)
	}
}

func readInt64s(field protobufField) ([]int64, error) {
	values, err := readUint64s(field)
	if err != nil {
		return nil, err
	}
	result := make([]int64, len(values))
	for index, value := range values {
		result[index] = int64(value)
	}
	return result, nil
}

func walkProtobuf(data []byte, visit func(protobufField) error) error {
	for len(data) > 0 {
		field, remaining, err := nextProtobufField(data)
		if err != nil {
			return err
		}
		if err := visit(field); err != nil {
			return err
		}
		data = remaining
	}
	return nil
}

func nextProtobufField(data []byte) (protobufField, []byte, error) {
	key, remaining, err := readVarint(data)
	if err != nil {
		return protobufField{}, nil, err
	}
	field := protobufField{number: int(key >> 3), wire: int(key & 7)}
	if field.number == 0 {
		return protobufField{}, nil, errors.New("field number is zero")
	}

	switch field.wire {
	case 0:
		field.value, remaining, err = readVarint(remaining)
		if err != nil {
			return protobufField{}, nil, err
		}
		return field, remaining, nil
	case 1:
		if len(remaining) < 8 {
			return protobufField{}, nil, errors.New("truncated fixed64 field")
		}
		return field, remaining[8:], nil
	case 2:
		length, afterLength, err := readVarint(remaining)
		if err != nil {
			return protobufField{}, nil, err
		}
		if length > uint64(len(afterLength)) {
			return protobufField{}, nil, errors.New("truncated length-delimited field")
		}
		field.data = afterLength[:int(length)]
		return field, afterLength[int(length):], nil
	case 5:
		if len(remaining) < 4 {
			return protobufField{}, nil, errors.New("truncated fixed32 field")
		}
		return field, remaining[4:], nil
	default:
		return protobufField{}, nil, fmt.Errorf("unsupported wire type %d", field.wire)
	}
}

func readVarint(data []byte) (uint64, []byte, error) {
	var value uint64
	for index, byteValue := range data {
		if index == 10 || (index == 9 && byteValue > 1) {
			return 0, nil, errors.New("varint overflows uint64")
		}
		value |= uint64(byteValue&0x7f) << (7 * index)
		if byteValue&0x80 == 0 {
			return value, data[index+1:], nil
		}
	}
	return 0, nil, errors.New("truncated varint")
}
