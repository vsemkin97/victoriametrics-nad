// NAD modification: this file is not part of upstream VictoriaMetrics.

package prometheus

import (
	"math"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

// writeExportColumnarLine appends xb to dst for /api/v1/export?format=columnar (NAD):
// the same series as the JSON export, without float<->text conversion. Big-endian:
//
//	u32 label count, then per label u32 len + name, u32 len + value (__name__ first, if any)
//	u32 n, n x i64 timestamps in ms (plain, not zig-zag), n x f64 values as IEEE 754 bits (NaN where JSON has null)
func writeExportColumnarLine(dst []byte, xb *exportBlock) []byte {
	if len(xb.timestamps) == 0 {
		return dst
	}
	mn := xb.mn
	n := len(mn.Tags)
	if len(mn.MetricGroup) > 0 {
		n++
	}
	dst = encoding.MarshalUint32(dst, uint32(n))
	if len(mn.MetricGroup) > 0 {
		dst = appendColumnarString(dst, "__name__")
		dst = appendColumnarBytes(dst, mn.MetricGroup)
	}
	for i := range mn.Tags {
		dst = appendColumnarBytes(dst, mn.Tags[i].Key)
		dst = appendColumnarBytes(dst, mn.Tags[i].Value)
	}
	dst = encoding.MarshalUint32(dst, uint32(len(xb.timestamps)))
	for _, ts := range xb.timestamps {
		dst = encoding.MarshalUint64(dst, uint64(ts)) // plain two's complement: encoding.MarshalInt64 zig-zags (NAD #75)
	}
	for _, v := range xb.values {
		dst = encoding.MarshalUint64(dst, math.Float64bits(v))
	}
	return dst
}

func appendColumnarBytes(dst, b []byte) []byte {
	dst = encoding.MarshalUint32(dst, uint32(len(b)))
	return append(dst, b...)
}

func appendColumnarString(dst []byte, s string) []byte {
	dst = encoding.MarshalUint32(dst, uint32(len(s)))
	return append(dst, s...)
}
