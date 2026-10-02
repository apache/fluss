// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Metric name constants and helpers for fluss-rust client instrumentation.
//!
//! Uses the [`metrics`] crate facade pattern: library code emits metrics via
//! `counter!`/`gauge!`/`histogram!` macros, and the application installs a
//! recorder (e.g. `metrics-exporter-prometheus`) to collect them. When no
//! recorder is installed, all metric calls are no-ops with zero overhead.

use crate::metadata::TablePath;
use crate::rpc::ApiKey;

// ---------------------------------------------------------------------------
// Label keys
// ---------------------------------------------------------------------------

pub const LABEL_API_KEY: &str = "api_key";

/// Identifies the database and table for per-table scanner metrics.
pub const LABEL_DATABASE: &str = "database";
pub const LABEL_TABLE: &str = "table";

// ---------------------------------------------------------------------------
// Connection / RPC metrics
//
// Java reference: ConnectionMetrics.java, ClientMetricGroup.java, MetricNames.java
//
// Byte counting matches Java semantics: both sides count only the API message
// body, excluding the protocol header and framing.
// Java: rawRequest.totalSize() / response.totalSize() (see MessageCodec.java).
// Rust: buf.len() - REQUEST_HEADER_LENGTH for sent bytes,
//       buffer.len() - cursor.position() for received bytes.
// ---------------------------------------------------------------------------

pub const CLIENT_REQUESTS_TOTAL: &str = "fluss.client.requests.total";
pub const CLIENT_RESPONSES_TOTAL: &str = "fluss.client.responses.total";
pub const CLIENT_BYTES_SENT_TOTAL: &str = "fluss.client.bytes_sent.total";
pub const CLIENT_BYTES_RECEIVED_TOTAL: &str = "fluss.client.bytes_received.total";
pub const CLIENT_REQUEST_LATENCY_MS: &str = "fluss.client.request_latency_ms";
pub const CLIENT_REQUESTS_IN_FLIGHT: &str = "fluss.client.requests_in_flight";

// ---------------------------------------------------------------------------
// Scanner poll-timing metrics
//
// Java reference: ScannerMetricGroup.java, LogScannerImpl.java
//
// These track consumer liveness and processing efficiency at the `poll()`
// boundary. Java records via `volatile long` fields read by gauge suppliers
// at scrape time; Rust pushes values via the `metrics` facade.
//
// `time_between_poll_ms` and `poll_idle_ratio` are snapshot at poll
// start / poll end. `last_poll_seconds_ago` must keep advancing between
// polls (it measures elapsed time, not activity), so it is emitted by a
// per-scanner 1-second background tokio task spawned in
// `LogScannerInner::new`. The task is aborted when the last scanner
// `Arc` is dropped, matching Java's `ScannerMetricGroup.close()`.
// ---------------------------------------------------------------------------

/// Gauge: milliseconds between the start of consecutive `poll()` calls. A
/// large value usually means the consumer's downstream processing is slow.
pub const SCANNER_TIME_BETWEEN_POLL_MS: &str = "fluss.client.scanner.time_between_poll_ms";

/// Gauge: fraction of wall-clock time spent inside `poll()` —
/// `poll_time_ms / (poll_time_ms + time_between_poll_ms)`. A value near 1.0
/// means the scanner is starved for data; a low value means the consumer is
/// the bottleneck.
pub const SCANNER_POLL_IDLE_RATIO: &str = "fluss.client.scanner.poll_idle_ratio";

/// Gauge: integer seconds since the most recent `poll()` started. Advances
/// monotonically between polls — the primary stuck-consumer signal.
///
/// Pushed every second by a per-scanner background tokio task. Emission is
/// skipped until the first `poll()` happens; Java's equivalent
/// `lastPollSecondsAgo` returns roughly the current Unix-epoch seconds
/// before the first poll (an unguarded `(now - 0)/1000`), which would trip
/// every consumer-liveness alert on startup.
///
pub const SCANNER_LAST_POLL_SECONDS_AGO: &str = "fluss.client.scanner.last_poll_seconds_ago";

// ---------------------------------------------------------------------------
// Scanner fetch + remote download metrics
//
// Fetch metrics are recorded in the LogFetcher fetch loop on response
// completion. Remote metrics are recorded inside RemoteLogDownloader's
// download task.
//
// Java uses a volatile-long gauge for fetch latency and Counter+MeterView
// for rates. Rust uses a histogram for latency (richer percentile data)
// and counters for throughput; the recorder/exporter handles rate
// computation (e.g. Prometheus `rate()`).
//
// Java emits one `ScannerMetricGroup` per (database, table); Rust matches
// that by attaching `database` + `table` labels to every scanner metric
// (see `ScannerMetrics` below).
// ---------------------------------------------------------------------------

/// Histogram: elapsed ms for each successful FetchLog RPC.
pub const SCANNER_FETCH_LATENCY_MS: &str = "fluss.client.scanner.fetch_latency_ms";

/// Counter: total FetchLog RPC requests attempted after connection acquisition.
pub const SCANNER_FETCH_REQUESTS_TOTAL: &str = "fluss.client.scanner.fetch_requests.total";

/// Histogram: serialized bytes per successful FetchLog response.
pub const SCANNER_BYTES_PER_REQUEST: &str = "fluss.client.scanner.bytes_per_request";

/// Counter: total remote log download attempts (includes per-segment retries).
pub const SCANNER_REMOTE_FETCH_REQUESTS_TOTAL: &str =
    "fluss.client.scanner.remote_fetch_requests.total";

/// Counter: total bytes downloaded from remote log storage.
pub const SCANNER_REMOTE_FETCH_BYTES_TOTAL: &str = "fluss.client.scanner.remote_fetch_bytes.total";

/// Counter: total remote log download failures (each retry attempt counts).
pub const SCANNER_REMOTE_FETCH_ERRORS_TOTAL: &str =
    "fluss.client.scanner.remote_fetch_errors.total";

// ---------------------------------------------------------------------------
// Per-table scanner metric handles
// ---------------------------------------------------------------------------

/// Cached `(database, table)`-labeled scanner metric handles.
///
/// Adding a new scanner metric: declare the constant above, add one
/// field plus an initializer line in [`Self::new`] using the matching
/// `scanner_{gauge,counter,histogram}` helper, and a `record_*` method.
/// The helpers are the single source of truth for the label set, so a
/// future label addition (e.g. `cluster_id`) is a one-line change.
///
/// # Recorder binding
///
/// `metrics::counter!(...)` / `gauge!(...)` / `histogram!(...)` resolve
/// the recorder at the macro callsite. Because this struct caches the
/// returned handles, every cached handle is bound to whichever recorder
/// is installed when [`Self::new`] runs. Construct the scanner *after*
/// installing the production recorder; in tests, construct it inside
/// the `metrics::with_local_recorder(...)` closure. With no recorder
/// installed, all `record_*` calls are zero-overhead no-ops.
pub(crate) struct ScannerMetrics {
    time_between_poll_ms: metrics::Gauge,
    poll_idle_ratio: metrics::Gauge,
    last_poll_seconds_ago: metrics::Gauge,
    fetch_requests_total: metrics::Counter,
    fetch_latency_ms: metrics::Histogram,
    bytes_per_request: metrics::Histogram,
    remote_fetch_requests_total: metrics::Counter,
    remote_fetch_bytes_total: metrics::Counter,
    remote_fetch_errors_total: metrics::Counter,
}

impl ScannerMetrics {
    /// Build a fresh handle cache for `table_path`. Resolves the
    /// currently installed recorder once per metric.
    pub(crate) fn new(table_path: &TablePath) -> Self {
        let database = table_path.database();
        let table = table_path.table();
        Self {
            time_between_poll_ms: scanner_gauge(SCANNER_TIME_BETWEEN_POLL_MS, database, table),
            poll_idle_ratio: scanner_gauge(SCANNER_POLL_IDLE_RATIO, database, table),
            last_poll_seconds_ago: scanner_gauge(SCANNER_LAST_POLL_SECONDS_AGO, database, table),
            fetch_requests_total: scanner_counter(SCANNER_FETCH_REQUESTS_TOTAL, database, table),
            fetch_latency_ms: scanner_histogram(SCANNER_FETCH_LATENCY_MS, database, table),
            bytes_per_request: scanner_histogram(SCANNER_BYTES_PER_REQUEST, database, table),
            remote_fetch_requests_total: scanner_counter(
                SCANNER_REMOTE_FETCH_REQUESTS_TOTAL,
                database,
                table,
            ),
            remote_fetch_bytes_total: scanner_counter(
                SCANNER_REMOTE_FETCH_BYTES_TOTAL,
                database,
                table,
            ),
            remote_fetch_errors_total: scanner_counter(
                SCANNER_REMOTE_FETCH_ERRORS_TOTAL,
                database,
                table,
            ),
        }
    }

    pub(crate) fn record_time_between_poll_ms(&self, value: f64) {
        self.time_between_poll_ms.set(value);
    }

    pub(crate) fn record_poll_idle_ratio(&self, value: f64) {
        self.poll_idle_ratio.set(value);
    }

    pub(crate) fn record_last_poll_seconds_ago(&self, value: f64) {
        self.last_poll_seconds_ago.set(value);
    }

    pub(crate) fn record_fetch_request(&self) {
        self.fetch_requests_total.increment(1);
    }

    pub(crate) fn record_fetch_latency_ms(&self, value: f64) {
        self.fetch_latency_ms.record(value);
    }

    pub(crate) fn record_bytes_per_request(&self, value: f64) {
        self.bytes_per_request.record(value);
    }

    pub(crate) fn record_remote_fetch_request(&self) {
        self.remote_fetch_requests_total.increment(1);
    }

    pub(crate) fn record_remote_fetch_bytes(&self, bytes: u64) {
        self.remote_fetch_bytes_total.increment(bytes);
    }

    pub(crate) fn record_remote_fetch_error(&self) {
        self.remote_fetch_errors_total.increment(1);
    }
}

// Per-table scanner handle factories. These centralize the
// `(database, table)` label set so a future schema change (renaming a
// label, adding `cluster_id`, etc.) is a one-line edit instead of
// touching every callsite in `ScannerMetrics::new`.

fn scanner_gauge(name: &'static str, database: &str, table: &str) -> metrics::Gauge {
    metrics::gauge!(
        name,
        LABEL_DATABASE => database.to_string(),
        LABEL_TABLE => table.to_string(),
    )
}

fn scanner_counter(name: &'static str, database: &str, table: &str) -> metrics::Counter {
    metrics::counter!(
        name,
        LABEL_DATABASE => database.to_string(),
        LABEL_TABLE => table.to_string(),
    )
}

fn scanner_histogram(name: &'static str, database: &str, table: &str) -> metrics::Histogram {
    metrics::histogram!(
        name,
        LABEL_DATABASE => database.to_string(),
        LABEL_TABLE => table.to_string(),
    )
}

// ---------------------------------------------------------------------------
// Writer pipeline metrics
//
//
// Java's `WriterMetricGroup` carries only the `client_id`
// variable inherited from `ClientMetricGroup` -- no table or bucket label
// (one series per client). The Rust `metrics` facade has no `client_id`
// concept, so writer metrics are emitted UNLABELED (global per process).
// TODO: A future `client.id` config option can attach a `client_id` label without
// breaking these series (it only splits the existing global series).
//
// Semantic deviations from Java:
//   * Java `sendLatencyMs` / `batchQueueTimeMs` are volatile-long gauges
//     (latest sample only); Rust uses histograms for full p50/p95/p99.
//   * Java `recordSendPerSecond` / `bytesSendPerSecond` / `recordsRetryPerSecond`
//     are `MeterView` rates; Rust emits raw counters and lets the exporter
//     compute `rate()`.
// ---------------------------------------------------------------------------

/// Histogram: elapsed ms for each write request (ProduceLog / PutKv) round
/// trip.
pub const WRITER_SEND_LATENCY_MS: &str = "fluss.client.writer.send_latency_ms";

/// Histogram: ms a batch spent queued in the accumulator (`drained_ms -
/// create_ms`).
pub const WRITER_BATCH_QUEUE_TIME_MS: &str = "fluss.client.writer.batch_queue_time_ms";

/// Counter: total records handed to the cluster across all sent batches.
pub const WRITER_RECORDS_SEND_TOTAL: &str = "fluss.client.writer.records_send.total";

/// Counter: total serialized batch bytes sent.
pub const WRITER_BYTES_SEND_TOTAL: &str = "fluss.client.writer.bytes_send.total";

/// Counter: total records re-enqueued for retry.
pub const WRITER_RECORDS_RETRY_TOTAL: &str = "fluss.client.writer.records_retry.total";

/// Histogram: records per sent batch.
pub const WRITER_RECORDS_PER_BATCH: &str = "fluss.client.writer.records_per_batch";

/// Histogram: serialized bytes per sent batch.
pub const WRITER_BYTES_PER_BATCH: &str = "fluss.client.writer.bytes_per_batch";

/// Gauge: total writer buffer memory in bytes (constant).
pub const WRITER_BUFFER_TOTAL_BYTES: &str = "fluss.client.writer.buffer_total_bytes";

/// Gauge: currently-available writer buffer memory in bytes.
pub const WRITER_BUFFER_AVAILABLE_BYTES: &str = "fluss.client.writer.buffer_available_bytes";

/// Gauge: number of producer threads blocked waiting for buffer memory --
/// a high-signal backpressure indicator.
pub const WRITER_BUFFER_WAITING_THREADS: &str = "fluss.client.writer.buffer_waiting_threads";

/// Cached, unlabeled writer-pipeline metric handles.
///
/// Constructed once per [`crate::client::write::WriterClient`] and shared
/// (`Arc`) into the `Sender`.
/// Like [`ScannerMetrics`], every cached handle is bound to whichever
/// recorder is installed when [`Self::new`] runs. Construct it *after*
/// installing the production recorder; in tests, construct it inside the
/// `metrics::with_local_recorder(...)` closure. With no recorder installed,
/// all `record_*` calls are zero-overhead no-ops.
pub(crate) struct WriterMetrics {
    send_latency_ms: metrics::Histogram,
    batch_queue_time_ms: metrics::Histogram,
    records_send_total: metrics::Counter,
    bytes_send_total: metrics::Counter,
    records_retry_total: metrics::Counter,
    records_per_batch: metrics::Histogram,
    bytes_per_batch: metrics::Histogram,
    buffer_total_bytes: metrics::Gauge,
    buffer_available_bytes: metrics::Gauge,
    buffer_waiting_threads: metrics::Gauge,
}

impl WriterMetrics {
    /// Build a fresh handle cache. Resolves the currently installed recorder
    /// once per metric.
    pub(crate) fn new() -> Self {
        Self {
            send_latency_ms: metrics::histogram!(WRITER_SEND_LATENCY_MS),
            batch_queue_time_ms: metrics::histogram!(WRITER_BATCH_QUEUE_TIME_MS),
            records_send_total: metrics::counter!(WRITER_RECORDS_SEND_TOTAL),
            bytes_send_total: metrics::counter!(WRITER_BYTES_SEND_TOTAL),
            records_retry_total: metrics::counter!(WRITER_RECORDS_RETRY_TOTAL),
            records_per_batch: metrics::histogram!(WRITER_RECORDS_PER_BATCH),
            bytes_per_batch: metrics::histogram!(WRITER_BYTES_PER_BATCH),
            buffer_total_bytes: metrics::gauge!(WRITER_BUFFER_TOTAL_BYTES),
            buffer_available_bytes: metrics::gauge!(WRITER_BUFFER_AVAILABLE_BYTES),
            buffer_waiting_threads: metrics::gauge!(WRITER_BUFFER_WAITING_THREADS),
        }
    }

    pub(crate) fn record_send_latency_ms(&self, value: f64) {
        self.send_latency_ms.record(value);
    }

    /// Record per-batch send statistics (records, bytes, queue time) for one
    /// built+sent batch.
    pub(crate) fn record_sent_batch(
        &self,
        record_count: i32,
        batch_bytes: usize,
        queue_time_ms: i64,
    ) {
        let records = record_count.max(0) as u64;
        let bytes = batch_bytes as u64;
        self.records_send_total.increment(records);
        self.bytes_send_total.increment(bytes);
        self.records_per_batch.record(record_count.max(0) as f64);
        self.bytes_per_batch.record(batch_bytes as f64);
        self.batch_queue_time_ms.record(queue_time_ms.max(0) as f64);
    }

    pub(crate) fn record_records_retry(&self, record_count: i32) {
        self.records_retry_total
            .increment(record_count.max(0) as u64);
    }

    /// Push the current buffer-pool gauges. Called once per sender poll-loop
    /// iteration (Java registers these as lazy suppliers on the accumulator).
    pub(crate) fn record_buffer_state(
        &self,
        total_bytes: usize,
        available_bytes: usize,
        waiting_threads: usize,
    ) {
        self.buffer_total_bytes.set(total_bytes as f64);
        self.buffer_available_bytes.set(available_bytes as f64);
        self.buffer_waiting_threads.set(waiting_threads as f64);
    }
}

/// Returns a label value for reportable API keys, matching Java's
/// `ConnectionMetrics.REPORT_API_KEYS` filter (`ProduceLog`, `FetchLog`,
/// `PutKv`, `Lookup`). Returns `None` for admin/metadata/auth calls to
/// avoid metric cardinality bloat.
pub(crate) fn api_key_label(api_key: ApiKey) -> Option<&'static str> {
    match api_key {
        ApiKey::ProduceLog => Some("produce_log"),
        ApiKey::FetchLog => Some("fetch_log"),
        ApiKey::PutKv => Some("put_kv"),
        ApiKey::Lookup => Some("lookup"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::assert_scanner_entries_labeled;
    use metrics_util::debugging::DebuggingRecorder;

    macro_rules! find_counter {
        ($entries:expr, $name:expr) => {
            $entries.iter().find_map(|(key, _, _, val)| {
                if key.key().name() == $name {
                    match val {
                        metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                        _ => None,
                    }
                } else {
                    None
                }
            })
        };
    }

    macro_rules! find_histogram {
        ($entries:expr, $name:expr) => {
            $entries.iter().find_map(|(key, _, _, val)| {
                if key.key().name() == $name {
                    match val {
                        metrics_util::debugging::DebugValue::Histogram(v) => {
                            Some(v.iter().map(|f| f.into_inner()).collect::<Vec<_>>())
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            })
        };
    }

    macro_rules! find_gauge {
        ($entries:expr, $name:expr) => {
            $entries.iter().find_map(|(key, _, _, val)| {
                if key.key().name() == $name {
                    match val {
                        metrics_util::debugging::DebugValue::Gauge(g) => Some(g.into_inner()),
                        _ => None,
                    }
                } else {
                    None
                }
            })
        };
    }

    #[test]
    fn reportable_api_keys_return_label() {
        assert_eq!(api_key_label(ApiKey::ProduceLog), Some("produce_log"));
        assert_eq!(api_key_label(ApiKey::FetchLog), Some("fetch_log"));
        assert_eq!(api_key_label(ApiKey::PutKv), Some("put_kv"));
        assert_eq!(api_key_label(ApiKey::Lookup), Some("lookup"));
    }

    #[test]
    fn non_reportable_api_keys_return_none() {
        assert_eq!(api_key_label(ApiKey::MetaData), None);
        assert_eq!(api_key_label(ApiKey::CreateTable), None);
        assert_eq!(api_key_label(ApiKey::Authenticate), None);
        assert_eq!(api_key_label(ApiKey::ListDatabases), None);
        assert_eq!(api_key_label(ApiKey::GetTable), None);
    }

    #[test]
    fn reportable_request_records_all_connection_metrics() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let label = api_key_label(ApiKey::ProduceLog).unwrap();

            metrics::counter!(CLIENT_REQUESTS_TOTAL, LABEL_API_KEY => label).increment(1);
            metrics::counter!(CLIENT_BYTES_SENT_TOTAL, LABEL_API_KEY => label).increment(256);
            metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).increment(1.0);

            metrics::counter!(CLIENT_RESPONSES_TOTAL, LABEL_API_KEY => label).increment(1);
            metrics::counter!(CLIENT_BYTES_RECEIVED_TOTAL, LABEL_API_KEY => label).increment(128);
            metrics::histogram!(CLIENT_REQUEST_LATENCY_MS, LABEL_API_KEY => label).record(42.5);
            metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).decrement(1.0);
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        assert_eq!(find_counter!(entries, CLIENT_REQUESTS_TOTAL), Some(1));
        assert_eq!(find_counter!(entries, CLIENT_RESPONSES_TOTAL), Some(1));
        assert_eq!(find_counter!(entries, CLIENT_BYTES_SENT_TOTAL), Some(256));
        assert_eq!(
            find_counter!(entries, CLIENT_BYTES_RECEIVED_TOTAL),
            Some(128)
        );
        assert_eq!(
            find_histogram!(entries, CLIENT_REQUEST_LATENCY_MS),
            Some(vec![42.5])
        );
        assert_eq!(find_gauge!(entries, CLIENT_REQUESTS_IN_FLIGHT), Some(0.0));

        let has_label = entries.iter().all(|(key, _, _, _)| {
            key.key()
                .labels()
                .any(|l| l.key() == LABEL_API_KEY && l.value() == "produce_log")
        });
        assert!(has_label, "all metrics must carry the api_key label");
    }

    #[test]
    fn non_reportable_request_records_no_metrics() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let label = api_key_label(ApiKey::MetaData);
            assert!(label.is_none());
            // When label is None, no metrics calls are made (matching request() logic).
        });

        let snapshot = snapshotter.snapshot();
        assert!(
            snapshot.into_vec().is_empty(),
            "non-reportable API keys must not produce metrics"
        );
    }

    #[test]
    fn inflight_gauge_nets_to_zero_after_balanced_calls() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let label = api_key_label(ApiKey::FetchLog).unwrap();

            // Simulate 3 concurrent requests completing
            for _ in 0..3 {
                metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).increment(1.0);
            }
            for _ in 0..3 {
                metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).decrement(1.0);
            }
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();
        assert_eq!(
            find_gauge!(entries, CLIENT_REQUESTS_IN_FLIGHT),
            Some(0.0),
            "in-flight gauge should be 0 after balanced inc/dec"
        );
    }

    #[test]
    fn different_api_keys_produce_separate_metric_series() {
        use std::collections::HashMap;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let produce_label = api_key_label(ApiKey::ProduceLog).unwrap();
            let fetch_label = api_key_label(ApiKey::FetchLog).unwrap();

            metrics::counter!(CLIENT_REQUESTS_TOTAL, LABEL_API_KEY => produce_label).increment(5);
            metrics::counter!(CLIENT_REQUESTS_TOTAL, LABEL_API_KEY => fetch_label).increment(3);
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        let request_entries: Vec<_> = entries
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == CLIENT_REQUESTS_TOTAL)
            .collect();

        assert_eq!(
            request_entries.len(),
            2,
            "produce_log and fetch_log should be separate metric series"
        );

        let mut counter_by_api_key: HashMap<String, u64> = HashMap::new();
        for (key, _, _, val) in request_entries {
            let api_key = key
                .key()
                .labels()
                .find(|label| label.key() == LABEL_API_KEY)
                .map(|label| label.value())
                .expect("requests total metric must include api_key label");

            let counter_value = match val {
                metrics_util::debugging::DebugValue::Counter(v) => *v,
                other => panic!("expected Counter, got {other:?}"),
            };

            counter_by_api_key.insert(api_key.to_string(), counter_value);
        }

        assert_eq!(counter_by_api_key.get("produce_log"), Some(&5));
        assert_eq!(counter_by_api_key.get("fetch_log"), Some(&3));
    }

    #[test]
    fn scanner_poll_timing_metrics_emit_correctly() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let table_path = TablePath::new("db", "tbl");
            let m = ScannerMetrics::new(&table_path);
            m.record_time_between_poll_ms(200.0);
            m.record_poll_idle_ratio(0.8);
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        assert_eq!(
            find_gauge!(entries, SCANNER_TIME_BETWEEN_POLL_MS),
            Some(200.0)
        );
        assert_eq!(find_gauge!(entries, SCANNER_POLL_IDLE_RATIO), Some(0.8));
        assert_scanner_entries_labeled(&entries, "db", "tbl");
    }

    #[test]
    fn scanner_last_poll_seconds_ago_emits_correctly() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let table_path = TablePath::new("db", "tbl");
            let m = ScannerMetrics::new(&table_path);
            m.record_last_poll_seconds_ago(42.0);
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        assert_eq!(
            find_gauge!(entries, SCANNER_LAST_POLL_SECONDS_AGO),
            Some(42.0)
        );
        assert_scanner_entries_labeled(&entries, "db", "tbl");
    }

    #[test]
    fn scanner_fetch_metrics_emit_correctly() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let table_path = TablePath::new("db", "tbl");
            let m = ScannerMetrics::new(&table_path);
            m.record_fetch_request();
            m.record_fetch_latency_ms(15.5);
            m.record_bytes_per_request(4096.0);
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        assert_eq!(
            find_counter!(entries, SCANNER_FETCH_REQUESTS_TOTAL),
            Some(1)
        );
        assert_eq!(
            find_histogram!(entries, SCANNER_FETCH_LATENCY_MS),
            Some(vec![15.5])
        );
        assert_eq!(
            find_histogram!(entries, SCANNER_BYTES_PER_REQUEST),
            Some(vec![4096.0])
        );
        assert_scanner_entries_labeled(&entries, "db", "tbl");
    }

    #[test]
    fn scanner_remote_fetch_metrics_emit_correctly() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let table_path = TablePath::new("db", "tbl");
            let m = ScannerMetrics::new(&table_path);
            m.record_remote_fetch_request();
            m.record_remote_fetch_request();
            m.record_remote_fetch_request();
            m.record_remote_fetch_bytes(1024);
            m.record_remote_fetch_error();
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        assert_eq!(
            find_counter!(entries, SCANNER_REMOTE_FETCH_REQUESTS_TOTAL),
            Some(3)
        );
        assert_eq!(
            find_counter!(entries, SCANNER_REMOTE_FETCH_BYTES_TOTAL),
            Some(1024)
        );
        assert_eq!(
            find_counter!(entries, SCANNER_REMOTE_FETCH_ERRORS_TOTAL),
            Some(1)
        );
        assert_scanner_entries_labeled(&entries, "db", "tbl");
    }

    /// Two scanners on different tables must produce independent metric
    /// series.
    #[test]
    fn different_table_paths_produce_separate_metric_series() {
        use std::collections::HashMap;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let m1 = ScannerMetrics::new(&TablePath::new("db1", "t1"));
            let m2 = ScannerMetrics::new(&TablePath::new("db2", "t2"));

            for _ in 0..5 {
                m1.record_fetch_request();
            }
            for _ in 0..3 {
                m2.record_fetch_request();
            }
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        let request_entries: Vec<_> = entries
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == SCANNER_FETCH_REQUESTS_TOTAL)
            .collect();

        assert_eq!(
            request_entries.len(),
            2,
            "(db1,t1) and (db2,t2) must be separate metric series"
        );

        let mut counter_by_table: HashMap<(String, String), u64> = HashMap::new();
        for (key, _, _, val) in request_entries {
            let mut database = None;
            let mut table = None;
            for label in key.key().labels() {
                if label.key() == LABEL_DATABASE {
                    database = Some(label.value().to_string());
                } else if label.key() == LABEL_TABLE {
                    table = Some(label.value().to_string());
                }
            }
            let database = database.expect("scanner metric must include database label");
            let table = table.expect("scanner metric must include table label");
            let counter_value = match val {
                metrics_util::debugging::DebugValue::Counter(v) => *v,
                other => panic!("expected Counter, got {other:?}"),
            };
            counter_by_table.insert((database, table), counter_value);
        }

        assert_eq!(
            counter_by_table.get(&("db1".to_string(), "t1".to_string())),
            Some(&5),
        );
        assert_eq!(
            counter_by_table.get(&("db2".to_string(), "t2".to_string())),
            Some(&3),
        );
    }

    #[test]
    fn writer_metrics_emit_all_writer_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let m = WriterMetrics::new();
            // Two sent batches: (3 records, 300 bytes, 12ms queue) and
            // (2 records, 200 bytes, 8ms queue).
            m.record_sent_batch(3, 300, 12);
            m.record_sent_batch(2, 200, 8);
            m.record_send_latency_ms(5.5);
            m.record_records_retry(4);
            m.record_buffer_state(64 * 1024 * 1024, 32 * 1024 * 1024, 2);
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        // Counters accumulate across both batches.
        assert_eq!(find_counter!(entries, WRITER_RECORDS_SEND_TOTAL), Some(5));
        assert_eq!(find_counter!(entries, WRITER_BYTES_SEND_TOTAL), Some(500));
        assert_eq!(find_counter!(entries, WRITER_RECORDS_RETRY_TOTAL), Some(4));

        // Histograms capture one sample per batch.
        assert_eq!(
            find_histogram!(entries, WRITER_RECORDS_PER_BATCH),
            Some(vec![3.0, 2.0])
        );
        assert_eq!(
            find_histogram!(entries, WRITER_BYTES_PER_BATCH),
            Some(vec![300.0, 200.0])
        );
        assert_eq!(
            find_histogram!(entries, WRITER_BATCH_QUEUE_TIME_MS),
            Some(vec![12.0, 8.0])
        );
        assert_eq!(
            find_histogram!(entries, WRITER_SEND_LATENCY_MS),
            Some(vec![5.5])
        );

        // Buffer gauges hold the latest pushed value.
        assert_eq!(
            find_gauge!(entries, WRITER_BUFFER_TOTAL_BYTES),
            Some((64 * 1024 * 1024) as f64)
        );
        assert_eq!(
            find_gauge!(entries, WRITER_BUFFER_AVAILABLE_BYTES),
            Some((32 * 1024 * 1024) as f64)
        );
        assert_eq!(
            find_gauge!(entries, WRITER_BUFFER_WAITING_THREADS),
            Some(2.0)
        );
    }

    /// Writer metrics carry no labels.
    #[test]
    fn writer_metrics_are_unlabeled() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let m = WriterMetrics::new();
            m.record_sent_batch(1, 10, 1);
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        let writer_entries: Vec<_> = entries
            .iter()
            .filter(|(key, _, _, _)| key.key().name().starts_with("fluss.client.writer."))
            .collect();
        assert!(
            !writer_entries.is_empty(),
            "expected writer metrics to be emitted"
        );
        for (key, _, _, _) in writer_entries {
            assert_eq!(
                key.key().labels().count(),
                0,
                "writer metric {} must be unlabeled",
                key.key().name()
            );
        }
    }
}
