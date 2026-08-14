package storage

import "google.golang.org/protobuf/encoding/protowire"

// valueLengthField is ValueMessage.value_length's protobuf field number.
const valueLengthField protowire.Number = 7

// valueMessageValueLength extracts ValueMessage.value_length (field 7) from an
// encoded ValueMessage without decoding or copying the inline Data payload
// (field 2), which can be as large as the inline threshold (64 KiB by default).
//
// It scans and validates the entire message the way proto.Unmarshal would: a
// structurally malformed record returns ok=false, an empty message returns
// (0, true), and a repeated field 7 takes the last value (matching proto's
// scalar last-wins merge). buf is never retained — the caller may Free the
// backing RocksDB slice immediately after this returns.
//
// The size-accounting paths (the Put-overwrite delta in existingValueLength and
// the cleaner's total-size scan) consume only value_length, so using this in
// place of a full proto.Unmarshal removes one payload-sized allocation per
// inline row. Callers that also need Data, RawFilePath, etc. must still decode
// the full message. See Perfloop cases case_2j2veg0hs5 and case_0pe7dc8ta7.
func valueMessageValueLength(buf []byte) (length int64, ok bool) {
	for len(buf) > 0 {
		num, typ, n := protowire.ConsumeTag(buf)
		if n < 0 {
			return 0, false
		}
		buf = buf[n:]

		if num == valueLengthField && typ == protowire.VarintType {
			v, vn := protowire.ConsumeVarint(buf)
			if vn < 0 {
				return 0, false
			}
			length = int64(v)
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
	return length, true
}
