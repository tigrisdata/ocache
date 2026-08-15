package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
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

	// A lone continuation byte with no valid tag is also malformed.
	_, ok = valueMessageValueLength([]byte{0x80})
	require.False(t, ok)
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
