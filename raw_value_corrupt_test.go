package aerospike

import "testing"

func TestForEachRawListCorrupt(t *testing.T) {
	for _, buf := range [][]byte{
		{0x92, 0xcb, 0, 0},    // truncated float64
		{0xdc, 0x00},          // truncated header
		{0x91, 0xc5, 0x00},    // truncated blob length
		{0x91, 0xdb, 0, 0, 0}, // truncated blob length
		{0x92, 0x01},          // fewer items than count
		{0x91, 0xc4},          // missing blob length
	} {
		if err := forEachRawList(buf, func(RawValue) Error { return nil }); err == nil {
			t.Errorf("% x: expected error", buf)
		}
	}
}
