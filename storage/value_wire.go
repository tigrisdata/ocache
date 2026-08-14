package storage

import (
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	pb "github.com/tigrisdata/ocache/storage/proto"
)

// ValueMessage protobuf field numbers used by the wire-level extractors below.
const (
	valueDataField   protowire.Number = 2 // bytes data — the large payload we skip
	valueExpiryField protowire.Number = 3
	valueLengthField protowire.Number = 7
)

// valueMessageVarintField extracts a varint-typed scalar field (identified by
// want) from an encoded ValueMessage without decoding or copying the inline Data
// payload (field 2), which can be as large as the inline threshold (64 KiB by
// default).
//
// It scans and validates the entire message the way proto.Unmarshal would: a
// structurally malformed record returns ok=false, a message with no such field
// returns (0, true), and a repeated field takes the last value (matching proto's
// scalar last-wins merge). buf is never retained — the caller may Free the
// backing RocksDB slice immediately after this returns.
func valueMessageVarintField(buf []byte, want protowire.Number) (val int64, ok bool) {
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return 0, false
		}
		buf = buf[n:]

		if num == want && typ == protowire.VarintType {
			v, vn := protowire.ConsumeVarint(buf)
			if vn < 0 {
				return 0, false
			}
			val = int64(v)
			buf = buf[vn:]
			continue
		}

		// Skip every other field — including the large Data payload — by
		// advancing past its wire value without materializing it.
		n = protowire.ConsumeFieldValue(num, typ, buf)
		if n < 0 {
			return 0, false
		}
		buf = buf[n:]
	}
	return val, true
}

// valueMessageValueLength extracts ValueMessage.value_length (field 7) off the
// wire without copying Data. The size-accounting paths (the Put-overwrite delta
// in existingValueLength and the cleaner's total-size scan) consume only
// value_length. See Perfloop cases case_2j2veg0hs5 and case_0pe7dc8ta7.
func valueMessageValueLength(buf []byte) (length int64, ok bool) {
	return valueMessageVarintField(buf, valueLengthField)
}

// valueMessageExpiry extracts ValueMessage.expiry (field 3) off the wire without
// copying Data. The key-listing scan reads only expiry to skip expired rows. See
// Perfloop case case_527g56fg8z.
func valueMessageExpiry(buf []byte) (expiry int64, ok bool) {
	return valueMessageVarintField(buf, valueExpiryField)
}

// unmarshalValueMessageSkippingData decodes buf into msg exactly as
// proto.Unmarshal would, except it drops the Data field (field 2) — up to the
// 64 KiB inline threshold — which the delete and eviction callers never read. It
// rebuilds the message wire without field 2 and defers to the generated decoder
// for the remaining (small) fields, so their semantics (last-wins merge, wire
// validation) are identical to a full decode. Returns false on a malformed
// record — the same fallback those callers already took when proto.Unmarshal
// errored. buf is not retained.
//
// See Perfloop case case_1mzwqcjjvr.
func unmarshalValueMessageSkippingData(buf []byte, msg *pb.ValueMessage) bool {
	// Control fields (type/paths/lengths) are small; 128 covers the common case
	// and append grows it only for unusually long raw-file/segment paths.
	stripped := make([]byte, 0, 128)
	for len(buf) > 0 {
		num, typ, tn := protowire.ConsumeTag(buf)
		if tn < 0 {
			return false
		}
		vn := protowire.ConsumeFieldValue(num, typ, buf[tn:])
		if vn < 0 {
			return false
		}
		if num != valueDataField {
			stripped = append(stripped, buf[:tn+vn]...)
		}
		buf = buf[tn+vn:]
	}
	return proto.Unmarshal(stripped, msg) == nil
}
