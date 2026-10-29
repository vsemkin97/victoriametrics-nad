# VictoriaMetrics — NAD fork

Single-node VictoriaMetrics **v1.110.0** with a websocket stream of freshly ingested samples,
used by the NAD webserver instead of polling `/api/v1/query` for every metric.

This is a modified version of [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics),
licensed under Apache License 2.0 (see `LICENSE`). Modified places are marked with `NAD modification`.

## What changed

| File | Change |
|---|---|
| `app/vminsert/stream/` | New package: `/api/v1/stream` websocket endpoint, backfill filter, fan-out, metrics |
| `app/vminsert/common/insert_ctx.go` | `FlushBufs` passes successfully stored rows to `stream.Publish` |
| `app/vminsert/main.go` | Routes `/api/v1/stream` to `stream.Handler` |
| `lib/httpserver/httpserver.go` | `responseWriterWithAbort` implements `http.Hijacker` (needed for the websocket upgrade) |
| `go.mod`, `vendor/` | `github.com/gorilla/websocket v1.5.3` (vendored) |
| `.github/workflows/` | Upstream workflows replaced with `nad.yml` (build, test, release) |

All ingestion protocols (`/api/v1/import/prometheus`, `/api/v1/write`, `/api/v1/import`, native, influx,
csv, OpenTelemetry, Datadog, Graphite, OpenTSDB, NewRelic) go through `FlushBufs`, so every source is streamed.

## `/api/v1/stream`

Websocket. Each text message is a JSON array of samples stored since the previous message:

```json
[{"metric":{"__name__":"cpu","host":"a"},"value":1.5,"timestamp":1793281222183}]
```

- `timestamp` is in milliseconds. Client messages are ignored; the server pings every 30s and drops
  connections that do not answer within 90s.
- Samples older than `now - stream.maxLag` are **not** streamed, so historical backfill is stored
  but never reaches subscribers. Staleness markers and NaN/Inf values are not streamed either.
- Ingestion is never blocked: if the shared encode queue or a subscriber queue is full, the batch is dropped
  and counted. Points sent while a subscriber is disconnected are not replayed.
- Protected by the same `-httpAuth.*` and `-tls` flags as the rest of the HTTP API.

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `-stream.maxLag` | `15m` | Maximum sample age (relative to arrival time) to be streamed |
| `-stream.queueSize` | `1024` | Pending batches per subscriber and in the shared encode queue |

Metrics at `/metrics`: `vm_stream_subscribers`, `vm_stream_connections_total`, `vm_stream_rows_sent_total`,
`vm_stream_rows_skipped_total{reason="lag"}`, `vm_stream_batches_dropped_total{reason="encode_queue|subscriber_queue"}`.

## Build and release

- `make victoria-metrics-prod` → `bin/victoria-metrics-prod` (static, built in Docker exactly like upstream releases).
- CI (`.github/workflows/nad.yml`) runs vet, tests and the build on every push to `nad`.
- Push a tag `v1.110.0-nad.N` to publish a GitHub release with the binary, a tarball and `checksums.txt`.
