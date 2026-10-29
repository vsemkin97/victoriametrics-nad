// Package stream broadcasts freshly ingested samples to websocket subscribers at /api/v1/stream.
//
// NAD modification: this package is not part of upstream VictoriaMetrics.
//
// Every successfully stored batch from any ingestion protocol is passed to Publish.
// Samples older than -stream.maxLag (historical backfill), staleness markers and non-finite values
// are not streamed. Slow subscribers lose batches instead of slowing down ingestion.
package stream

import (
	"flag"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/gorilla/websocket"
	"github.com/valyala/quicktemplate"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

var (
	maxLag = flag.Duration("stream.maxLag", 15*time.Minute, "Only samples with timestamps not older than now-stream.maxLag are sent to /api/v1/stream subscribers. "+
		"This prevents historical backfill from flooding subscribers")
	queueSize = flag.Int("stream.queueSize", 1024, "The maximum number of pending batches per /api/v1/stream subscriber and in the shared encode queue. "+
		"Batches are dropped when the queue is full, so ingestion is never slowed down by subscribers")
)

const (
	writeTimeout = 10 * time.Second
	pingInterval = 30 * time.Second
	pongTimeout  = 3 * pingInterval
)

var (
	rowsSent           = metrics.NewCounter(`vm_stream_rows_sent_total`)
	rowsSkippedLag     = metrics.NewCounter(`vm_stream_rows_skipped_total{reason="lag"}`)
	batchesDroppedQ    = metrics.NewCounter(`vm_stream_batches_dropped_total{reason="encode_queue"}`)
	batchesDroppedSub  = metrics.NewCounter(`vm_stream_batches_dropped_total{reason="subscriber_queue"}`)
	subscribersTotal   = metrics.NewCounter(`vm_stream_connections_total`)
	_                  = metrics.NewGauge(`vm_stream_subscribers`, func() float64 { return float64(subscribersCount.Load()) })
	subscribersCount   atomic.Int64
	subscribersLock    sync.Mutex
	subscribers        = map[chan []byte]struct{}{}
	encodeQueue        chan *batch
	encodeQueueInitOne sync.Once
)

type batch struct {
	buf  []byte
	rows []storage.MetricRow
}

// Publish copies fresh samples from mrs and queues them for subscribers. It never blocks.
//
// mrs may be reused by the caller after the return.
func Publish(mrs []storage.MetricRow) {
	if subscribersCount.Load() == 0 || len(mrs) == 0 {
		return
	}
	minTimestamp := time.Now().UnixMilli() - maxLag.Milliseconds()
	b := &batch{}
	ends := make([]int, 0, len(mrs))
	var skipped uint64
	for i := range mrs {
		mr := &mrs[i]
		if mr.Timestamp < minTimestamp {
			skipped++
			continue
		}
		if math.IsNaN(mr.Value) || math.IsInf(mr.Value, 0) {
			// Staleness markers and non-finite values cannot be represented in JSON.
			continue
		}
		b.buf = append(b.buf, mr.MetricNameRaw...)
		ends = append(ends, len(b.buf))
		b.rows = append(b.rows, storage.MetricRow{Timestamp: mr.Timestamp, Value: mr.Value})
	}
	rowsSkippedLag.Add(int(skipped))
	if len(b.rows) == 0 {
		return
	}
	start := 0
	for i, end := range ends {
		b.rows[i].MetricNameRaw = b.buf[start:end:end]
		start = end
	}
	select {
	case getEncodeQueue() <- b:
	default:
		batchesDroppedQ.Inc()
	}
}

func getEncodeQueue() chan *batch {
	encodeQueueInitOne.Do(func() {
		encodeQueue = make(chan *batch, *queueSize)
		go encodeLoop()
	})
	return encodeQueue
}

// encodeLoop encodes every batch once and fans it out to all the subscribers.
func encodeLoop() {
	var mn storage.MetricName
	for b := range encodeQueue {
		data, n := marshalBatch(nil, b, &mn)
		if n == 0 {
			continue
		}
		subscribersLock.Lock()
		for ch := range subscribers {
			select {
			case ch <- data:
				rowsSent.Add(n)
			default:
				batchesDroppedSub.Inc()
			}
		}
		subscribersLock.Unlock()
	}
}

// marshalBatch appends b as a JSON array of {"metric":{...},"value":v,"timestamp":ms} objects to dst.
func marshalBatch(dst []byte, b *batch, mn *storage.MetricName) ([]byte, int) {
	n := 0
	dst = append(dst, '[')
	for i := range b.rows {
		r := &b.rows[i]
		if err := mn.UnmarshalRaw(r.MetricNameRaw); err != nil {
			logger.Errorf("cannot unmarshal metric name for /api/v1/stream: %s", err)
			continue
		}
		if n > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"metric":{"__name__":`...)
		dst = quicktemplate.AppendJSONString(dst, string(mn.MetricGroup), true)
		for j := range mn.Tags {
			t := &mn.Tags[j]
			dst = append(dst, ',')
			dst = quicktemplate.AppendJSONString(dst, string(t.Key), true)
			dst = append(dst, ':')
			dst = quicktemplate.AppendJSONString(dst, string(t.Value), true)
		}
		dst = append(dst, `},"value":`...)
		dst = strconv.AppendFloat(dst, r.Value, 'g', -1, 64)
		dst = append(dst, `,"timestamp":`...)
		dst = strconv.AppendInt(dst, r.Timestamp, 10)
		dst = append(dst, '}')
		n++
	}
	dst = append(dst, ']')
	return dst, n
}

var upgrader = websocket.Upgrader{
	// The endpoint is protected by the same -httpAuth.* / -tls settings as the rest of the HTTP API.
	CheckOrigin: func(*http.Request) bool { return true },
}

// Handler serves websocket subscribers at /api/v1/stream.
//
// Every text message is a JSON array of samples ingested since the previous message.
// Client messages are ignored.
func Handler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade already replied with an error status.
		return
	}
	defer conn.Close()
	getEncodeQueue()

	ch := make(chan []byte, *queueSize)
	subscribersLock.Lock()
	subscribers[ch] = struct{}{}
	subscribersLock.Unlock()
	subscribersCount.Add(1)
	subscribersTotal.Inc()
	defer func() {
		subscribersLock.Lock()
		delete(subscribers, ch)
		subscribersLock.Unlock()
		subscribersCount.Add(-1)
	}()

	// The reader detects closed and dead connections; gorilla handles ping/pong/close frames inside NextReader.
	done := make(chan struct{})
	_ = conn.SetReadDeadline(time.Now().Add(pongTimeout))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(pongTimeout)) })
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case <-done:
			return
		case data := <-ch:
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-ping.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				return
			}
		}
	}
}
