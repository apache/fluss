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

//! Process logging and the gateway metric inventory.
//!
//! [`METRIC_DEFINITIONS`] lists implemented metric families with their kinds, units, descriptions,
//! and label sets. Families for future capabilities are added alongside their implementations.
//!
//! Labels describe an operation or a bounded outcome. `cluster`, sourced from validated configuration, is the
//! only resource-name label the gateway itself emits. Configured Gateway and instance identities are attached
//! to every family as global labels.

use crate::config::ServerConfig;
use log::{LevelFilter, Log, Metadata, Record};
use metrics::Unit;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;
use std::time::Duration;

/// Logger that writes one line per record to standard error, with no filtering beyond the global level.
struct StderrLogger;

impl Log for StderrLogger {
    /// Returns whether a record is within the configured global level.
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= log::max_level()
    }

    /// Writes one enabled record to standard error.
    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            eprintln!("{} {} {}", record.level(), record.target(), record.args());
        }
    }

    /// Flushes buffered output, which is a no-op for direct standard-error writes.
    fn flush(&self) {}
}

static LOGGER: StderrLogger = StderrLogger;
static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

const GATEWAY_ID_LABEL: &str = "gateway_id";
const INSTANCE_ID_LABEL: &str = "instance_id";
const HOST_LABEL: &str = "host";
#[cfg(test)]
const IDENTITY_LABELS: &[&str] = &[GATEWAY_ID_LABEL, INSTANCE_ID_LABEL, HOST_LABEL];

/// Buckets for the duration histograms, spanning a fast local answer to a request that runs into the
/// configured deadline.
///
/// Without explicit buckets `metrics-exporter-prometheus` renders every histogram as a summary with
/// pre-computed quantiles, which cannot be aggregated across gateway instances.
const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Which Prometheus instrument a metric family uses.
#[derive(Clone, Copy)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// One declared metric family and its complete label set.
pub struct MetricDefinition {
    /// Fully qualified Prometheus family name.
    pub name: &'static str,
    pub kind: MetricKind,
    pub unit: Option<Unit>,
    pub description: &'static str,
    /// Every label key the family may carry. Values must come from a bounded vocabulary.
    pub labels: &'static [&'static str],
}

/// Implemented metric families and their labels.
pub const METRIC_DEFINITIONS: &[MetricDefinition] = &[
    metric(
        "fluss_gateway_rest_requests_total",
        MetricKind::Counter,
        None,
        "Completed REST requests. `operation` is the matched route template, `code` \
         the HTTP status, and `cluster` the bounded configured-cluster label (`none` for \
         cluster-free routes, `unknown` for unconfigured IDs).",
        &["cluster", "method", "operation", "code"],
    ),
    metric(
        "fluss_gateway_rest_request_duration_seconds",
        MetricKind::Histogram,
        Some(Unit::Seconds),
        "REST request duration.",
        &["cluster", "method", "operation"],
    ),
    // Connection families, reported per configured cluster by the backend runtime. The per-user
    // act-as pool of the user identity mode reports into the same families when it arrives: they count the
    // Fluss connections the gateway holds for a cluster, whoever they are opened for.
    metric(
        "fluss_gateway_connections_active",
        MetricKind::Gauge,
        None,
        "Fluss connections the gateway currently holds for a configured cluster.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_connections_created_total",
        MetricKind::Counter,
        None,
        "Fluss connections opened for a configured cluster.",
        &["cluster"],
    ),
    metric(
        "fluss_gateway_connections_closed_total",
        MetricKind::Counter,
        None,
        "Fluss connections released for a configured cluster. `reason` is `idle` or `shutdown`.",
        &["cluster", "reason"],
    ),
    // Process and Tokio runtime families, sampled periodically by the runtime sampler.
    metric(
        "process_cpu_seconds_total",
        MetricKind::Counter,
        Some(Unit::Seconds),
        "Total user and system CPU time spent by the gateway process.",
        &[],
    ),
    metric(
        "process_resident_memory_bytes",
        MetricKind::Gauge,
        Some(Unit::Bytes),
        "Resident memory of the gateway process. Linux only; absent elsewhere.",
        &[],
    ),
    metric(
        "process_open_fds",
        MetricKind::Gauge,
        None,
        "Open file descriptors of the gateway process.",
        &[],
    ),
    metric(
        "process_max_fds",
        MetricKind::Gauge,
        None,
        "File descriptor limit of the gateway process.",
        &[],
    ),
    metric(
        "tokio_alive_tasks",
        MetricKind::Gauge,
        None,
        "Tokio tasks spawned but not yet finished.",
        &[],
    ),
    metric(
        "tokio_global_queue_depth",
        MetricKind::Gauge,
        None,
        "Tasks waiting in the Tokio injection queue.",
        &[],
    ),
    // `tokio_worker_busy_seconds_total` needs the `tokio_unstable` runtime
    // metrics and is added once the build enables them.
];

const fn metric(
    name: &'static str,
    kind: MetricKind,
    unit: Option<Unit>,
    description: &'static str,
    labels: &'static [&'static str],
) -> MetricDefinition {
    MetricDefinition {
        name,
        kind,
        unit,
        description,
        labels,
    }
}

/// Initializes the process logger. Repeated calls refresh the global level.
pub fn init_logging() {
    let level = std::env::var("RUST_LOG")
        .ok()
        .as_deref()
        .map(parse_level)
        .unwrap_or(LevelFilter::Info);
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(level);
}

/// Installs the process-wide Prometheus recorder before the Fluss client creates metric handles.
pub fn init_metrics(server: &ServerConfig) -> Result<(), String> {
    if !server.metrics.enabled || METRICS_HANDLE.get().is_some() {
        return Ok(());
    }
    let recorder = prometheus_builder(server)?.build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|error| format!("failed to install Prometheus recorder: {error}"))?;
    let _ = METRICS_HANDLE.set(handle);
    describe_metrics();
    Ok(())
}

fn prometheus_builder(server: &ServerConfig) -> Result<PrometheusBuilder, String> {
    let mut builder = PrometheusBuilder::new()
        .set_buckets(DURATION_BUCKETS)
        .map_err(|error| format!("failed to configure histogram buckets: {error}"))?;
    for (key, value) in [
        (GATEWAY_ID_LABEL, server.gateway_id.as_deref()),
        (INSTANCE_ID_LABEL, server.instance_id.as_deref()),
        (HOST_LABEL, server.host.as_deref()),
    ] {
        if let Some(value) = value {
            builder = builder.add_global_label(key, value.to_string());
        }
    }
    Ok(builder)
}

/// Records one completed REST request against the matched route template, never the raw URI.
///
/// `operation` is the matched route template; `code` is the HTTP status. The caller bounds
/// `cluster` to a configured ID, `unknown` for unconfigured IDs, or `none` for cluster-free routes.
pub fn http_request(cluster: &str, method: &str, operation: &str, code: u16, duration: Duration) {
    metrics::counter!(
        "fluss_gateway_rest_requests_total",
        "cluster" => cluster.to_string(),
        "method" => method.to_string(),
        "operation" => operation.to_string(),
        "code" => code.to_string()
    )
    .increment(1);
    metrics::histogram!(
        "fluss_gateway_rest_request_duration_seconds",
        "cluster" => cluster.to_string(),
        "method" => method.to_string(),
        "operation" => operation.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Returns the installed recorder handle for the dedicated metrics listener.
pub fn metrics_handle() -> Option<PrometheusHandle> {
    METRICS_HANDLE.get().cloned()
}

/// Records one Fluss connection opened for a configured cluster.
pub fn connection_created(cluster: &str) {
    metrics::counter!(
        "fluss_gateway_connections_created_total",
        "cluster" => cluster.to_string()
    )
    .increment(1);
}

/// Records one Fluss connection released for a configured cluster.
///
/// `reason` comes from a fixed vocabulary, never from an error message, so the label stays bounded.
pub fn connection_closed(cluster: &str, reason: &'static str) {
    metrics::counter!(
        "fluss_gateway_connections_closed_total",
        "cluster" => cluster.to_string(),
        "reason" => reason
    )
    .increment(1);
}

/// Sets how many Fluss connections the gateway currently holds for a configured cluster.
pub fn connections_active(cluster: &str, active: usize) {
    metrics::gauge!(
        "fluss_gateway_connections_active",
        "cluster" => cluster.to_string()
    )
    .set(active as f64);
}

/// Records rows submitted to a configured cluster, including indeterminate completions.
pub fn write_rows(cluster: &str, rows: u64) {
    metrics::counter!(
        "fluss_gateway_backend_write_rows_total",
        "cluster" => cluster.to_string()
    )
    .increment(rows);
}

/// Records REST write-body bytes for a configured cluster.
pub fn write_bytes(cluster: &str, bytes: u64) {
    metrics::counter!(
        "fluss_gateway_backend_write_bytes_total",
        "cluster" => cluster.to_string()
    )
    .increment(bytes);
}

/// Samples process and Tokio runtime gauges once.
///
/// Called periodically by the lifecycle's runtime sampler; each source that a platform cannot
/// provide is skipped rather than published as zero.
pub fn sample_runtime_metrics() {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let runtime = handle.metrics();
        metrics::gauge!("tokio_alive_tasks").set(runtime.num_alive_tasks() as f64);
        metrics::gauge!("tokio_global_queue_depth").set(runtime.global_queue_depth() as f64);
    }
    if let Some(cpu_seconds) = process_cpu_seconds() {
        // Whole seconds: the `metrics` counter API is integral, and a `_total` family must keep counter
        // semantics so `rate()` and the Prometheus/OTLP conversion stay correct. The sub-second remainder
        // is carried into the next sample rather than lost, since the source is an absolute total.
        metrics::counter!("process_cpu_seconds_total").absolute(cpu_seconds as u64);
    }
    if let Some(resident) = process_resident_memory_bytes() {
        metrics::gauge!("process_resident_memory_bytes").set(resident);
    }
    if let Some(fds) = process_open_fds() {
        metrics::gauge!("process_open_fds").set(fds);
    }
    if let Some(limit) = process_max_fds() {
        metrics::gauge!("process_max_fds").set(limit);
    }
}

/// Total user plus system CPU seconds of this process, from `getrusage(2)`.
#[cfg(unix)]
fn process_cpu_seconds() -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` fills the buffer we own; a non-zero return leaves it unread.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: `getrusage` returned 0, so the buffer is initialized.
    let usage = unsafe { usage.assume_init() };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
    Some(seconds(usage.ru_utime) + seconds(usage.ru_stime))
}

#[cfg(not(unix))]
fn process_cpu_seconds() -> Option<f64> {
    None
}

/// Current resident set size in bytes, from `/proc/self/statm`. Linux only.
#[cfg(target_os = "linux")]
fn process_resident_memory_bytes() -> Option<f64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: f64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: `sysconf(_SC_PAGESIZE)` reads a process constant.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0).then_some(resident_pages * page_size as f64)
}

#[cfg(not(target_os = "linux"))]
fn process_resident_memory_bytes() -> Option<f64> {
    None
}

/// Number of open file descriptors, counted from the per-process descriptor directory.
#[cfg(unix)]
fn process_open_fds() -> Option<f64> {
    let directory = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    let entries = std::fs::read_dir(directory).ok()?;
    // The directory handle itself is one of the entries; excluding it keeps the count honest.
    Some(entries.count().saturating_sub(1) as f64)
}

#[cfg(not(unix))]
fn process_open_fds() -> Option<f64> {
    None
}

/// The soft file descriptor limit from `getrlimit(2)`, paired with `process_open_fds`.
#[cfg(unix)]
fn process_max_fds() -> Option<f64> {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::zeroed();
    // SAFETY: `getrlimit` fills the buffer we own; a non-zero return leaves it unread.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: `getrlimit` returned 0, so the buffer is initialized.
    let limit = unsafe { limit.assume_init() };
    Some(limit.rlim_cur as f64)
}

#[cfg(not(unix))]
fn process_max_fds() -> Option<f64> {
    None
}

fn describe_metrics() {
    for definition in METRIC_DEFINITIONS {
        match (definition.kind, definition.unit) {
            (MetricKind::Counter, Some(unit)) => {
                metrics::describe_counter!(definition.name, unit, definition.description)
            }
            (MetricKind::Counter, None) => {
                metrics::describe_counter!(definition.name, definition.description)
            }
            (MetricKind::Gauge, Some(unit)) => {
                metrics::describe_gauge!(definition.name, unit, definition.description)
            }
            (MetricKind::Gauge, None) => {
                metrics::describe_gauge!(definition.name, definition.description)
            }
            (MetricKind::Histogram, Some(unit)) => {
                metrics::describe_histogram!(definition.name, unit, definition.description)
            }
            (MetricKind::Histogram, None) => {
                metrics::describe_histogram!(definition.name, definition.description)
            }
        }
        debug_assert!(definition.labels.iter().all(|label| !label.is_empty()));
    }
}

/// Parses a supported global level name, defaulting unknown directives to `info`.
fn parse_level(value: &str) -> LevelFilter {
    match value.trim().to_ascii_lowercase().as_str() {
        "off" => LevelFilter::Off,
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_global_levels() {
        assert_eq!(parse_level("off"), LevelFilter::Off);
        assert_eq!(parse_level("ERROR"), LevelFilter::Error);
        assert_eq!(parse_level("warn"), LevelFilter::Warn);
        assert_eq!(parse_level("info"), LevelFilter::Info);
        assert_eq!(parse_level("debug"), LevelFilter::Debug);
        assert_eq!(parse_level("trace"), LevelFilter::Trace);
        assert_eq!(parse_level("module=debug"), LevelFilter::Info);
    }

    /// Allowed metric families, including those reserved for future capabilities.
    const ALLOWED_METRIC_FAMILIES: &[&str] = &[
        "fluss_gateway_rest_requests_total",
        "fluss_gateway_rest_request_duration_seconds",
        "fluss_gateway_backend_write_rows_total",
        "fluss_gateway_backend_write_bytes_total",
        "fluss_gateway_connections_active",
        "fluss_gateway_connections_created_total",
        "fluss_gateway_connections_closed_total",
        "fluss_client_writer_kv_backpressure_pressure",
        "fluss_client_writer_kv_backpressure_throttle_seconds_total",
        "process_cpu_seconds_total",
        "process_resident_memory_bytes",
        "process_open_fds",
        "process_max_fds",
        "tokio_alive_tasks",
        "tokio_global_queue_depth",
        "tokio_worker_busy_seconds_total",
    ];

    #[test]
    fn the_inventory_uses_only_allowed_families() {
        for definition in METRIC_DEFINITIONS {
            assert!(
                ALLOWED_METRIC_FAMILIES.contains(&definition.name),
                "{} is not an allowed metric family",
                definition.name
            );
        }
    }

    /// A `_total` family must be a counter, or `rate()` and the Prometheus/OTLP conversion misread it.
    #[test]
    fn total_families_are_counters() {
        for definition in METRIC_DEFINITIONS {
            if definition.name.ends_with("_total") {
                assert!(
                    matches!(definition.kind, MetricKind::Counter),
                    "{} carries the _total suffix without counter semantics",
                    definition.name
                );
            }
        }
    }

    #[test]
    fn metric_family_names_are_unique() {
        let mut names: Vec<&str> = METRIC_DEFINITIONS
            .iter()
            .map(|definition| definition.name)
            .collect();
        names.sort_unstable();
        let total = names.len();
        names.dedup();
        assert_eq!(names.len(), total, "duplicate metric family declared");
    }

    /// Gateway-owned families keep their label sets bounded. Re-exported
    /// `fluss_client_writer_kv_backpressure_*` families do carry `database` / `table`; they come from the
    /// client recorder, not from here, and this rule is relaxed for them when they arrive.
    #[test]
    fn metric_labels_cannot_contain_unbounded_resource_names() {
        const FORBIDDEN: &[&str] = &[
            "database",
            "table",
            "partition",
            "cursor",
            "entry_id",
            "request_id",
            "raw_uri",
            "row",
        ];
        for definition in METRIC_DEFINITIONS {
            for label in definition.labels {
                assert!(
                    !FORBIDDEN.contains(label),
                    "metric {} has forbidden label {label}",
                    definition.name
                );
                assert!(
                    !IDENTITY_LABELS.contains(label),
                    "metric {} shadows global identity label {label}",
                    definition.name
                );
            }
            let resource_labels = definition
                .labels
                .iter()
                .filter(|label| matches!(**label, "cluster" | "database" | "table" | "partition"))
                .copied()
                .collect::<Vec<_>>();
            assert!(
                resource_labels.is_empty() || resource_labels == ["cluster"],
                "metric {} has invalid resource labels {resource_labels:?}",
                definition.name
            );
        }
    }
}
