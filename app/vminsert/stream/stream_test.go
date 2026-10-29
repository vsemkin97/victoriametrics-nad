package stream

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestStreamSkipsBackfill(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(Handler))
	defer srv.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("cannot dial: %s", err)
	}
	defer conn.Close()
	for subscribersCount.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	name := func(labels ...string) []byte {
		var ls []prompbmarshal.Label
		for i := 0; i < len(labels); i += 2 {
			ls = append(ls, prompbmarshal.Label{Name: labels[i], Value: labels[i+1]})
		}
		return storage.MarshalMetricNameRaw(nil, ls)
	}
	now := time.Now().UnixMilli()
	mrs := []storage.MetricRow{
		{MetricNameRaw: name("__name__", "cpu", "host", `a"b`), Timestamp: now, Value: 1.5},
		{MetricNameRaw: name("__name__", "cpu", "host", "old"), Timestamp: now - (16 * time.Minute).Milliseconds(), Value: 2},
		{MetricNameRaw: name("__name__", "cpu", "host", "stale"), Timestamp: now, Value: decimal.StaleNaN},
		{MetricNameRaw: name("__name__", "cpu", "host", "inf"), Timestamp: now, Value: math.Inf(1)},
		{MetricNameRaw: name("__name__", "mem"), Timestamp: now - (14 * time.Minute).Milliseconds(), Value: 3e6},
	}
	Publish(mrs)
	// The caller may reuse the buffers after Publish returns.
	for i := range mrs {
		for j := range mrs[i].MetricNameRaw {
			mrs[i].MetricNameRaw[j] = 0
		}
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("cannot read message: %s", err)
	}
	want := `[{"metric":{"__name__":"cpu","host":"a\"b"},"value":1.5,"timestamp":` + strconv.FormatInt(now, 10) +
		`},{"metric":{"__name__":"mem"},"value":3e+06,"timestamp":` + strconv.FormatInt(now-(14*time.Minute).Milliseconds(), 10) + `}]`
	if string(msg) != want {
		t.Fatalf("unexpected message\ngot  %s\nwant %s", msg, want)
	}
}
