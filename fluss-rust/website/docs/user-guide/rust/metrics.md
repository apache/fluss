---
sidebar_position: 5
---
# Metrics

The Fluss Rust client is instrumented with client-side metrics for the connection
layer, the write pipeline, and the read (scanner) pipeline. Metrics are emitted
through the [`metrics`](https://docs.rs/metrics) crate facade, so collecting them is
opt-in and costs nothing until you install a recorder.

## How it works

The client never decides *where* metrics go. It only emits them via the `metrics`
facade. Your application installs a global **recorder** (for example a Prometheus
exporter), and that recorder decides how to store and expose the values.

- **No recorder installed** — every metric call is a zero-cost no-op. This is the
  default, so the client adds no overhead unless you opt in.
- **Recorder installed** — values flow to whatever backend the recorder represents
  (Prometheus, StatsD, OpenTelemetry, a test recorder, etc.).

This differs from the Fluss Java client, where metric reporters are configured
server-side in `conf/server.yaml` (`metrics.reporters: jmx,prometheus`) and
discovered through plugins. The Rust client instead follows the idiomatic Rust
`metrics` ecosystem: the application owns recorder installation, and rate
computation is left to the backend (e.g. PromQL `rate()`) instead of a built-in
background rate thread.

## Installing a recorder

Use any [`metrics`-compatible exporter](https://docs.rs/metrics/latest/metrics/#related-crates).
The example below uses `metrics-exporter-prometheus` to expose a scrape endpoint:

```rust
use metrics_exporter_prometheus::PrometheusBuilder;

PrometheusBuilder::new()
    .with_http_listener(([0, 0, 0, 0], 9000))
    .install()
    .expect("failed to install Prometheus recorder");
```

A full, runnable program is available as the `example-prometheus-metrics` example
in the `fluss-examples` crate.

:::warning Install the recorder before writing or scanning
The client caches metric handles the first time a writer or scanner is created,
binding them to whichever recorder is installed at that moment. Install your global
recorder **before** calling `FlussConnection::new` (ideally as the very first thing
in `main`). If you install it after creating a writer or scanner, those metrics will
be bound to the no-op recorder and never appear.
:::

## Metric catalog

All metric names use the `fluss.client.` prefix. A Prometheus exporter translates
`.` to `_`, so `fluss.client.writer.send_latency_ms` is scraped as
`fluss_client_writer_send_latency_ms`.

### Connection / RPC

Recorded per RPC for the four reportable API keys. Labeled with `api_key`
(`produce_log`, `fetch_log`, `put_kv`, `lookup`).

| Metric | Type | Description |
| --- | --- | --- |
| `fluss.client.requests.total` | Counter | Requests sent to a server. |
| `fluss.client.responses.total` | Counter | Responses received from a server. |
| `fluss.client.bytes_sent.total` | Counter | Request body bytes sent (excludes protocol framing). |
| `fluss.client.bytes_received.total` | Counter | Response body bytes received (excludes protocol framing). |
| `fluss.client.request_latency_ms` | Histogram | Round-trip latency per request, in milliseconds. |
| `fluss.client.requests_in_flight` | Gauge | Requests currently awaiting a response. |

### Writer

Recorded in the write pipeline. These metrics are **unlabeled** (one series per
process), matching Java's `WriterMetricGroup`, which carries no table label.

| Metric | Type | Description |
| --- | --- | --- |
| `fluss.client.writer.send_latency_ms` | Histogram | Round-trip latency of each write request (ProduceLog / PutKv). |
| `fluss.client.writer.batch_queue_time_ms` | Histogram | Time a batch spent queued in the accumulator before being drained. |
| `fluss.client.writer.records_send.total` | Counter | Records handed to the cluster across all sent batches. |
| `fluss.client.writer.bytes_send.total` | Counter | Serialized batch bytes sent. |
| `fluss.client.writer.records_retry.total` | Counter | Records re-enqueued for retry. |
| `fluss.client.writer.records_per_batch` | Histogram | Records per sent batch. |
| `fluss.client.writer.bytes_per_batch` | Histogram | Serialized bytes per sent batch. |
| `fluss.client.writer.buffer_total_bytes` | Gauge | Total writer buffer memory, in bytes. |
| `fluss.client.writer.buffer_available_bytes` | Gauge | Currently available writer buffer memory, in bytes. |
| `fluss.client.writer.buffer_waiting_threads` | Gauge | Producer threads blocked waiting for buffer memory (backpressure signal). |

### Scanner

Recorded in the read pipeline. Labeled with `database` and `table`, so each scanned
table gets its own series.

| Metric | Type | Description |
| --- | --- | --- |
| `fluss.client.scanner.time_between_poll_ms` | Gauge | Milliseconds between the start of consecutive `poll()` calls. |
| `fluss.client.scanner.poll_idle_ratio` | Gauge | Fraction of wall-clock time spent inside `poll()` (near 1.0 means starved for data). |
| `fluss.client.scanner.last_poll_seconds_ago` | Gauge | Seconds since the most recent `poll()` started (stuck-consumer signal). |
| `fluss.client.scanner.fetch_latency_ms` | Histogram | Latency of each successful FetchLog RPC, in milliseconds. |
| `fluss.client.scanner.fetch_requests.total` | Counter | FetchLog RPC requests attempted. |
| `fluss.client.scanner.bytes_per_request` | Histogram | Serialized bytes per successful FetchLog response. |
| `fluss.client.scanner.remote_fetch_requests.total` | Counter | Remote log download attempts (includes per-segment retries). |
| `fluss.client.scanner.remote_fetch_bytes.total` | Counter | Bytes downloaded from remote log storage. |
| `fluss.client.scanner.remote_fetch_errors.total` | Counter | Remote log download failures (each retry counts). |

## Differences from the Java client

The Rust client records the same events at the same points as Java, but uses
metric types better suited to the `metrics` ecosystem:

| Aspect | Java | Rust |
| --- | --- | --- |
| Latency metrics (`send_latency_ms`, `batch_queue_time_ms`, `fetch_latency_ms`) | Volatile-long gauge (latest value only) | Histogram (full p50/p95/p99 distribution) |
| Throughput / retry metrics | `MeterView` rate computed on a background thread | Raw counter; compute the rate with PromQL `rate()` |
| Writer table label | No table label | No table label (kept identical) |

These are client-internal implementation choices; the values reported to the server
are unaffected.

## Tuning histogram buckets

The default buckets in `metrics-exporter-prometheus` may not give meaningful
percentiles for sub-millisecond or multi-second latencies. Configure per-metric
buckets when installing the recorder:

```rust
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics::Unit;

PrometheusBuilder::new()
    .set_buckets_for_metric(
        metrics_exporter_prometheus::Matcher::Suffix("_ms".to_string()),
        &[0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0],
    )
    .expect("invalid bucket configuration")
    .with_http_listener(([0, 0, 0, 0], 9000))
    .install()
    .expect("failed to install Prometheus recorder");

let _ = Unit::Milliseconds; // optional: register units for richer metadata
```

## Cardinality

Scanner metrics carry `database` and `table` labels, so the number of scanner
series scales with the number of tables a single client scans. This is normally
small, but if your client scans many short-lived tables, the series count can grow
over time. Connection metrics are bounded (four API keys) and writer metrics are
unlabeled, so neither contributes meaningfully to cardinality.

## Grafana / PromQL tips

By default, `metrics-exporter-prometheus` emits histogram metrics as Prometheus
**summaries** (no `_bucket` series). The `_bucket`-based `histogram_quantile`
query only works if you enabled bucket mode via `set_buckets(...)` or
`set_buckets_for_metric(...)` in the previous section.

```promql
# Write throughput (records/sec), Java-style rate from the raw counter
rate(fluss_client_writer_records_send_total[1m])

# p99 send latency (default summary mode)
fluss_client_writer_send_latency_ms{quantile="0.99"}

# p99 send latency (bucket mode enabled)
histogram_quantile(0.99, sum(rate(fluss_client_writer_send_latency_ms_bucket[5m])) by (le))

# Backpressure: producers blocked waiting for buffer memory
fluss_client_writer_buffer_waiting_threads

# Per-table fetch rate
sum(rate(fluss_client_scanner_fetch_requests_total[1m])) by (database, table)

# Stuck-consumer alert: no poll in the last 60s
fluss_client_scanner_last_poll_seconds_ago > 60
```
