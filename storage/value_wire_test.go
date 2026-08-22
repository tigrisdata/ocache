package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	pb "github.com/tigrisdata/ocache/storage/proto"
)

// Test that the wire-level value_length extractor agrees exactly with a full
// proto.Unmarshal across the ValueMessage shapes Put actually writes, so the
// size-accounting paths can skip decoding the inline Data payload.
func TestValueMessageValueLength_MatchesFullDecode(t *testing.T) {
	cases := []struct {
		name string
		msg  *pb.ValueMessage
	}{
		{"empty", &pb.ValueMessage{}},
		{"inline zero-length", &pb.ValueMessage{ValueType: pb.ValueType_INLINE, ValueLength: 0}},
		{"inline small", &pb.ValueMessage{
			ValueType: pb.ValueType_INLINE, Data: []byte("hello"), ValueLength: 5,
		}},
		{"inline at 64KiB threshold", &pb.ValueMessage{
			ValueType:   pb.ValueType_INLINE,
			Data:        bytes.Repeat([]byte("x"), 64*1024),
			ValueLength: 64 * 1024,
			Expiry:      1786728207,
		}},
		{"raw file (no data)", &pb.ValueMessage{
			ValueType: pb.ValueType_RAW_FILE, RawFilePath: "/disk/files/abc.dat",
			ValueLength: 8 * 1024 * 1024, Checksum: 0xdeadbeef,
		}},
		{"segment", &pb.ValueMessage{
			ValueType: pb.ValueType_SEGMENT, SegmentPath: "/disk/segments/seg_1.seg",
			SegmentOffset: 4096, ValueLength: 262144,
		}},
		{"all fields set", &pb.ValueMessage{
			ValueType: pb.ValueType_INLINE, Data: []byte("payload"), Expiry: 42,
			RawFilePath: "/r", SegmentPath: "/s", SegmentOffset: 7,
			ValueLength: 7, Checksum: 99,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := proto.Marshal(tc.msg)
			require.NoError(t, err)

			got, ok := valueMessageValueLength(buf)
			require.True(t, ok, "well-formed message must decode")
			require.Equal(t, tc.msg.ValueLength, got, "extracted value_length must match full decode")
		})
	}
}

// A repeated field 7 must take the last value, matching proto scalar merge.
func TestValueMessageValueLength_LastWinsOnDuplicateField(t *testing.T) {
	tag := protowireTagVarint(t, 7)
	buf := append(append(append([]byte{}, tag...), 10), append(append([]byte{}, tag...), 20)...)

	// Cross-check the "last wins" expectation against the real decoder.
	var msg pb.ValueMessage
	require.NoError(t, proto.Unmarshal(buf, &msg))

	got, ok := valueMessageValueLength(buf)
	require.True(t, ok)
	require.Equal(t, msg.ValueLength, got)
	require.Equal(t, int64(20), got)
}

// Structurally malformed input must be reported as not-ok, mirroring the error
// path the callers already handle (return 0 / skip the row).
func TestValueMessageValueLength_MalformedIsNotOK(t *testing.T) {
	// Truncated field 7 varint: valid tag, then a value byte that never terminates.
	tag := protowireTagVarint(t, 7)
	buf := append(append([]byte{}, tag...), 0x80) // continuation bit set, no follow-on byte

	_, ok := valueMessageValueLength(buf)
	require.False(t, ok, "truncated varint must be rejected")
	_, ok = valueMessageExpiry(buf)
	require.False(t, ok, "truncated varint must be rejected")

	// A lone continuation byte with no valid tag is also malformed.
	_, ok = valueMessageValueLength([]byte{0x80})
	require.False(t, ok)
	_, ok = valueMessageExpiry([]byte{0x80})
	require.False(t, ok)
}

func TestValueMessageValueLength_InvalidFieldNumberIsNotOK(t *testing.T) {
	buf := protowire.AppendTag(nil, protowire.MaxValidNumber+1, protowire.VarintType)
	buf = protowire.AppendVarint(buf, 1)

	var msg pb.ValueMessage
	require.Error(t, proto.Unmarshal(buf, &msg))
	_, ok := valueMessageValueLength(buf)
	require.False(t, ok)
	_, ok = valueMessageExpiry(buf)
	require.False(t, ok)
}

// String fields reject invalid UTF-8 during proto.Unmarshal. The wire scanner
// must reject every such row too, even though it only reads value_length.
func TestValueMessageValueLength_InvalidStringIsNotOK(t *testing.T) {
	for _, tc := range []struct {
		name        string
		field       byte
		lengthFirst bool
	}{
		{name: "raw_file_path_before_length", field: 4},
		{name: "raw_file_path_after_length", field: 4, lengthFirst: true},
		{name: "segment_path_before_length", field: 5},
		{name: "segment_path_after_length", field: 5, lengthFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stringField := []byte{tc.field<<3 | 2, 1, 0xff}
			lengthField := []byte{7 << 3, 1}
			buf := append(stringField, lengthField...)
			if tc.lengthFirst {
				buf = append(lengthField, stringField...)
			}

			var msg pb.ValueMessage
			require.Error(t, proto.Unmarshal(buf, &msg))
			_, ok := valueMessageValueLength(buf)
			require.False(t, ok)
			_, ok = valueMessageExpiry(buf)
			require.False(t, ok)
			_, _, ok = valueMessageSegmentRef(buf)
			require.False(t, ok)
		})
	}
}

// valueMessageExpiry must agree with a full decode's Expiry field across shapes.
func TestValueMessageExpiry_MatchesFullDecode(t *testing.T) {
	msgs := []*pb.ValueMessage{
		{},
		{ValueType: pb.ValueType_INLINE, Data: []byte("hi"), ValueLength: 2}, // no expiry
		{ValueType: pb.ValueType_INLINE, Data: bytes.Repeat([]byte("x"), 64*1024), ValueLength: 64 * 1024, Expiry: 1786728207},
		{ValueType: pb.ValueType_RAW_FILE, RawFilePath: "/r", ValueLength: 1 << 20, Expiry: 42},
	}
	for _, m := range msgs {
		buf, err := proto.Marshal(m)
		require.NoError(t, err)
		got, ok := valueMessageExpiry(buf)
		require.True(t, ok)
		require.Equal(t, m.Expiry, got)
	}
}

// A repeated expiry field must take its final value, matching proto scalar merge.
func TestValueMessageExpiry_LastWinsOnDuplicateField(t *testing.T) {
	tag := protowireTagVarint(t, 3)
	buf := append(append(append([]byte{}, tag...), 10), append(append([]byte{}, tag...), 20)...)

	var msg pb.ValueMessage
	require.NoError(t, proto.Unmarshal(buf, &msg))

	got, ok := valueMessageExpiry(buf)
	require.True(t, ok)
	require.Equal(t, msg.Expiry, got)
	require.Equal(t, int64(20), got)
}

// unmarshalValueMessageSkippingData must reproduce a full proto.Unmarshal on
// every field except Data, which it must drop.
func TestUnmarshalValueMessageSkippingData_MatchesFullDecodeExceptData(t *testing.T) {
	msgs := map[string]*pb.ValueMessage{
		"empty":   {},
		"inline":  {ValueType: pb.ValueType_INLINE, Data: bytes.Repeat([]byte("x"), 64*1024), ValueLength: 64 * 1024, Expiry: 9},
		"rawfile": {ValueType: pb.ValueType_RAW_FILE, RawFilePath: "/disk/files/abc.dat", ValueLength: 8 << 20, Checksum: 0xabcd},
		"segment": {ValueType: pb.ValueType_SEGMENT, SegmentPath: "/disk/segments/seg_1.seg", SegmentOffset: 4096, ValueLength: 262144},
	}
	for name, m := range msgs {
		t.Run(name, func(t *testing.T) {
			buf, err := proto.Marshal(m)
			require.NoError(t, err)

			var got pb.ValueMessage
			require.True(t, unmarshalValueMessageSkippingData(buf, &got))

			require.Empty(t, got.Data, "Data must be dropped")
			require.Equal(t, m.ValueType, got.ValueType)
			require.Equal(t, m.Expiry, got.Expiry)
			require.Equal(t, m.RawFilePath, got.RawFilePath)
			require.Equal(t, m.SegmentPath, got.SegmentPath)
			require.Equal(t, m.SegmentOffset, got.SegmentOffset)
			require.Equal(t, m.ValueLength, got.ValueLength)
			require.Equal(t, m.Checksum, got.Checksum)
		})
	}

	// Malformed input is rejected, matching the callers' proto.Unmarshal-error path.
	var msg pb.ValueMessage
	require.False(t, unmarshalValueMessageSkippingData([]byte{0x80}, &msg))
}

// valueMessageSegmentRef must agree with a full decode on both fields it reads,
// for every stored-value shape — the reconciliation scan derives the total size
// from its value_length for EVERY row, so a mis-parse would skew the cap.
func TestValueMessageSegmentRef_MatchesFullDecode(t *testing.T) {
	msgs := map[string]*pb.ValueMessage{
		"empty":       {},
		"inline":      {ValueType: pb.ValueType_INLINE, Data: bytes.Repeat([]byte("x"), 64*1024), ValueLength: 64 * 1024, Expiry: 9},
		"rawfile":     {ValueType: pb.ValueType_RAW_FILE, RawFilePath: "/disk/files/abc.dat", ValueLength: 8 << 20, Checksum: 0xabcd},
		"segment":     {ValueType: pb.ValueType_SEGMENT, SegmentPath: "/disk/segments/seg_1.seg", SegmentOffset: 4096, ValueLength: 262144},
		"segment_ttl": {ValueType: pb.ValueType_SEGMENT, SegmentPath: "/d/s/seg_2.seg", SegmentOffset: 1 << 30, ValueLength: 1, Expiry: 1786728207, Checksum: 7},
	}
	for name, m := range msgs {
		t.Run(name, func(t *testing.T) {
			buf, err := proto.Marshal(m)
			require.NoError(t, err)

			segPath, length, ok := valueMessageSegmentRef(buf)
			require.True(t, ok)
			require.Equal(t, m.SegmentPath, segPath)
			require.Equal(t, m.ValueLength, length)
		})
	}

	// Malformed input is rejected rather than silently contributing 0.
	_, _, ok := valueMessageSegmentRef([]byte{0x80})
	require.False(t, ok)
}

// Benchmarks the allocation/CPU delta the size-accounting paths gain by
// extracting value_length off the wire instead of a full decode of an inline
// row at the 64 KiB threshold.
func BenchmarkValueLength_Inline64KiB(b *testing.B) {
	msg := &pb.ValueMessage{
		ValueType:   pb.ValueType_INLINE,
		Data:        bytes.Repeat([]byte("x"), 64*1024),
		ValueLength: 64 * 1024,
	}
	buf, err := proto.Marshal(msg)
	require.NoError(b, err)

	b.Run("full-unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var vm pb.ValueMessage
			if err := proto.Unmarshal(buf, &vm); err != nil {
				b.Fatal(err)
			}
			_ = vm.ValueLength
		}
	})

	b.Run("wire-extract", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := valueMessageValueLength(buf); !ok {
				b.Fatal("decode failed")
			}
		}
	})
}

// protowireTagVarint builds the tag byte(s) for a varint-typed field number.
func protowireTagVarint(t *testing.T, field int) []byte {
	t.Helper()
	// wire type 0 (varint); tag = (field << 3) | 0. Field numbers < 16 fit one byte.
	require.Less(t, field, 16)
	return []byte{byte(field << 3)}
}
