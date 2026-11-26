package prometheus

import (
	"math"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestWriteExportColumnarLine(t *testing.T) {
	mn := &storage.MetricName{MetricGroup: []byte("up")}
	mn.AddTag("job", "a=b")
	xb := &exportBlock{mn: mn, timestamps: []int64{1000, 61000}, values: []float64{1.5, math.NaN()}}
	b := writeExportColumnarLine(nil, xb)

	u32 := func() int { v := encoding.UnmarshalUint32(b); b = b[4:]; return int(v) }
	str := func() string { n := u32(); s := string(b[:n]); b = b[n:]; return s }
	labels := map[string]string{}
	for i := u32(); i > 0; i-- {
		k := str()
		labels[k] = str()
	}
	if !reflect.DeepEqual(labels, map[string]string{"__name__": "up", "job": "a=b"}) {
		t.Fatalf("labels %v", labels)
	}
	n := u32()
	if n != 2 || encoding.UnmarshalInt64(b) != 1000 || encoding.UnmarshalInt64(b[8:]) != 61000 {
		t.Fatalf("timestamps n=%d", n)
	}
	b = b[16:]
	if math.Float64frombits(encoding.UnmarshalUint64(b)) != 1.5 || !math.IsNaN(math.Float64frombits(encoding.UnmarshalUint64(b[8:]))) || len(b) != 16 {
		t.Fatalf("values")
	}
	if got := writeExportColumnarLine(nil, &exportBlock{mn: mn}); len(got) != 0 {
		t.Fatalf("empty series must write nothing")
	}
}
