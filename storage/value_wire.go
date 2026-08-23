package storage

import (
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	pb "github.com/tigrisdata/ocache/storage/proto"
)

// ValueMessage protobuf field numbers used by the wire-level extractors below.
const (
	valueTypeField    protowire.Number = 1
	valueDataField    protowire.Number = 2 // bytes data — the large payload we skip
	valueExpiryField  protowire.Number = 3
	valueRawPathField protowire.Number = 4
	valueSegPathField protowire.Number = 5
	valueLengthField  protowire.Number = 7
)

// valueMessageVarintField extracts a varint-typed scalar field (identified by
// want) from an encoded ValueMessage without decoding or copying the inline Data
// payload (field 2), which can be as large as the inline threshold (64 KiB by
// default).
//
// It scans and validates the entire message the way proto.Unmarshal would,
// including UTF-8 validity for ValueMessage's string fields: a structurally
// malformed record returns ok=false, a message with no such field
// returns (0, true), and a repeated field takes the last value (matching proto's
// scalar last-wins merge). buf is never retained — the caller may Free the
// backing RocksDB slice immediately after this returns.
func valueMessageVarintField(buf []byte, want protowire.Number) (val int64, ok bool) {
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 || !num.IsValid() {
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
		if (num == valueRawPathField || num == valueSegPathField) && typ == protowire.BytesType {
			v, vn := protowire.ConsumeBytes(buf)
			if vn < 0 || !utf8.Valid(v) {
				return 0, false
			}
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

// valueMessageSegmentRef extracts value_length (field 7) and segment_path
// (field 5) in one pass, without copying Data. segmentPath is empty for values
// that do not live in a segment: the stored-value invariant documented on
// ValueMessage.raw_file_path is that segment_path is non-empty iff value_type is
// SEGMENT, so the path alone identifies the class and value_type need not be
// read. Malformed records return ok=false, matching valueMessageVarintField.
//
// Used by the reconciliation scan, which needs both fields for every metadata
// row (total size, plus per-segment live bytes) and must not pay a payload copy
// per row.
func valueMessageSegmentRef(buf []byte) (segmentPath string, valueLength int64, ok bool) {
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 || !num.IsValid() {
			return "", 0, false
		}
		buf = buf[n:]

		switch {
		case num == valueLengthField && typ == protowire.VarintType:
			v, vn := protowire.ConsumeVarint(buf)
			if vn < 0 {
				return "", 0, false
			}
			valueLength = int64(v)
			buf = buf[vn:]
			continue
		case (num == valueRawPathField || num == valueSegPathField) && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(buf)
			if vn < 0 || !utf8.Valid(v) {
				return "", 0, false
			}
			if num == valueSegPathField {
				segmentPath = string(v)
			}
			buf = buf[vn:]
			continue
		}

		n = protowire.ConsumeFieldValue(num, typ, buf)
		if n < 0 {
			return "", 0, false
		}
		buf = buf[n:]
	}
	return segmentPath, valueLength, true
}

// valueMessageValueLength extracts ValueMessage.value_length (field 7) off the
// wire without copying Data. The size-accounting paths (the Put-overwrite delta
// in existingValueLength and the cleaner's total-size scan) consume only
// value_length. See Perfloop cases case_2j2veg0hs5 and case_0pe7dc8ta7.
func valueMessageValueLength(buf []byte) (length int64, ok bool) {
	return valueMessageVarintField(buf, valueLengthField)
}

// valueMessageExpiry extracts ValueMessage.expiry (field 3) off the wire without
// copying Data. Key-listing and TTL cleanup scans read only expiry to skip
// ordinary rows without reconstructing their control messages. See Perfloop case
// case_527g56fg8z.
func valueMessageExpiry(buf []byte) (expiry int64, ok bool) {
	return valueMessageVarintField(buf, valueExpiryField)
}

// valueMessageCleanupFields is the part of ValueMessage needed after a row is
// deleted or replaced. Paths are owned strings because callers may retain this
// value after releasing the RocksDB slice that supplied the wire data.
type valueMessageCleanupFields struct {
	valueType   pb.ValueType
	valueLength int64
	rawFilePath string
	segmentPath string
}

// decodeValueMessageCleanupFields extracts the control fields used to account
// for and reclaim a deleted or replaced value without materializing Data. It
// validates the complete wire message, including UTF-8 for stored paths. Scalar
// fields are last-wins, matching protobuf decoding; fields with an unexpected
// wire type are skipped as unknown fields are by proto.Unmarshal.
func decodeValueMessageCleanupFields(buf []byte) (fields valueMessageCleanupFields, ok bool) {
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 || !num.IsValid() {
			return valueMessageCleanupFields{}, false
		}
		buf = buf[n:]

		switch {
		case num == valueTypeField && typ == protowire.VarintType:
			value, vn := protowire.ConsumeVarint(buf)
			if vn < 0 {
				return valueMessageCleanupFields{}, false
			}
			fields.valueType = pb.ValueType(value)
			buf = buf[vn:]
			continue
		case num == valueLengthField && typ == protowire.VarintType:
			value, vn := protowire.ConsumeVarint(buf)
			if vn < 0 {
				return valueMessageCleanupFields{}, false
			}
			fields.valueLength = int64(value)
			buf = buf[vn:]
			continue
		case num == valueRawPathField && typ == protowire.BytesType:
			value, vn := protowire.ConsumeBytes(buf)
			if vn < 0 || !utf8.Valid(value) {
				return valueMessageCleanupFields{}, false
			}
			fields.rawFilePath = string(value)
			buf = buf[vn:]
			continue
		case num == valueSegPathField && typ == protowire.BytesType:
			value, vn := protowire.ConsumeBytes(buf)
			if vn < 0 || !utf8.Valid(value) {
				return valueMessageCleanupFields{}, false
			}
			fields.segmentPath = string(value)
			buf = buf[vn:]
			continue
		}

		// This also advances past Data without allocating a Data-sized slice.
		n = protowire.ConsumeFieldValue(num, typ, buf)
		if n < 0 {
			return valueMessageCleanupFields{}, false
		}
		buf = buf[n:]
	}
	return fields, true
}

// unmarshalValueMessageSkippingData decodes buf into msg exactly as
// proto.Unmarshal would, except it drops the Data field (field 2) — up to the
// 64 KiB inline threshold — which cleaner callers never read. It rebuilds the
// message wire without field 2 and defers to the generated decoder for the
// remaining fields. Returns false on a malformed record.
func unmarshalValueMessageSkippingData(buf []byte, msg *pb.ValueMessage) bool {
	// Control fields (type/paths/lengths) are small; 128 covers the common case
	// and append grows it only for unusually long raw-file/segment paths.
	stripped := make([]byte, 0, 128)
	for len(buf) > 0 {
		num, typ, tn := protowire.ConsumeTag(buf)
		if tn < 0 || !num.IsValid() {
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
