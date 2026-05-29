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

use crate::client::ClientSchemaGetter;
use crate::client::connection::FlussConnection;
use crate::client::credentials::SecurityTokenManager;
use crate::client::metadata::Metadata;
use crate::client::table::batch_scanner::LimitBatchScanner;
use crate::client::table::log_fetch_buffer::{
    CompletedFetch, DefaultCompletedFetch, FetchErrorAction, FetchErrorContext, FetchErrorLogLevel,
    LogFetchBuffer, RemotePendingFetch,
};
use crate::client::table::remote_log::{RemoteLogDownloader, RemoteLogFetchInfo};
use crate::config::Config;
use crate::error::Error::UnsupportedOperation;
use crate::error::{ApiError, Error, FlussError, Result};
use crate::metadata::{
    LogFormat, PhysicalTablePath, RowType, SchemaInfo, TableBucket, TableInfo, TablePath,
};
use crate::metrics::ScannerMetrics;
use crate::proto::{
    ErrorResponse, FetchLogRequest, FetchLogResponse, PbFetchLogReqForBucket, PbFetchLogReqForTable,
};
use crate::record::{
    LogRecordsBatches, ReadContext, ScanBatch, ScanRecord, ScanRecords, to_arrow_schema,
};
use crate::rpc::{RpcClient, RpcError, message};
use crate::util::FairBucketStatusMap;
use crate::{PartitionId, TableId};
use arrow_schema::SchemaRef;
use log::{debug, warn};
use parking_lot::{Mutex, RwLock};
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    slice::from_ref,
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;

pub struct TableScan<'a> {
    conn: &'a FlussConnection,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
    /// Column indices to project. None means all columns, Some(vec) means only the specified columns (non-empty).
    projected_fields: Option<Vec<usize>>,
    /// Optional row limit. When set, callers may construct a [`BatchScanner`] for a one-shot bounded scan.
    limit: Option<i32>,
}

impl<'a> TableScan<'a> {
    pub fn new(conn: &'a FlussConnection, table_info: TableInfo, metadata: Arc<Metadata>) -> Self {
        Self {
            conn,
            table_info,
            metadata,
            projected_fields: None,
            limit: None,
        }
    }

    /// Sets a row limit for the scan, enabling [`Self::create_bucket_batch_scanner`].
    ///
    /// The limit must be positive. A limit is incompatible with the log
    /// scanners, which reject it.
    pub fn limit(mut self, n: i32) -> Result<Self> {
        if n <= 0 {
            return Err(Error::IllegalArgument {
                message: format!("Scan limit must be positive, got {n}"),
            });
        }
        self.limit = Some(n);
        Ok(self)
    }

    /// Log scanners don't support limit pushdown; reject a configured limit
    /// rather than silently ignoring it.
    fn reject_limit(&self, scanner: &str) -> Result<()> {
        if let Some(limit) = self.limit {
            return Err(Error::UnsupportedOperation {
                message: format!(
                    "{scanner} doesn't support limit pushdown. Table: {}, requested limit: {limit}",
                    self.table_info.table_path
                ),
            });
        }
        Ok(())
    }

    /// Creates a one-shot bounded scan of `table_bucket`.
    ///
    /// Requires a previously-configured limit via [`Self::limit`]. Creation is
    /// cheap; the `LimitScanRequest` runs on the first
    /// [`LimitBatchScanner::next_batch`].
    pub fn create_bucket_batch_scanner(
        self,
        table_bucket: TableBucket,
    ) -> Result<LimitBatchScanner> {
        let limit = self.limit.ok_or_else(|| Error::IllegalArgument {
            message: "create_bucket_batch_scanner requires a limit configured via .limit(n)"
                .to_string(),
        })?;
        if table_bucket.table_id() != self.table_info.table_id {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Bucket table_id {} does not match scan table_id {}",
                    table_bucket.table_id(),
                    self.table_info.table_id
                ),
            });
        }
        let num_buckets = self.table_info.get_num_buckets();
        if table_bucket.bucket_id() < 0 || table_bucket.bucket_id() >= num_buckets {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Bucket id {} out of range for table with {num_buckets} buckets",
                    table_bucket.bucket_id()
                ),
            });
        }
        // Log tables decode as Arrow IPC, so only ARROW format is supported (KV
        // tables use the value-record path and are exempt).
        if !self.table_info.has_primary_key() {
            validate_scan_support(&self.table_info.table_path, &self.table_info)?;
        }
        // Pre-seed the current schema; older versions are fetched lazily during
        // KV decode. Mirrors `Table::new_lookup`.
        let latest = SchemaInfo::new(
            self.table_info.get_schema().clone(),
            self.table_info.get_schema_id(),
        );
        let schema_getter = Arc::new(ClientSchemaGetter::new(
            self.table_info.table_path.clone(),
            self.conn.get_admin()?,
            latest,
        ));
        Ok(LimitBatchScanner::new(
            self.conn.get_connections(),
            self.metadata.clone(),
            self.table_info,
            schema_getter,
            self.projected_fields,
            table_bucket,
            limit,
        ))
    }

    /// Projects the scan to only include specified columns by their indices.
    ///
    /// # Arguments
    /// * `column_indices` - Zero-based indices of columns to include in the scan
    ///
    /// # Errors
    /// Returns an error if `column_indices` is empty or if any column index is out of range.
    ///
    /// # Example
    /// ```
    /// # use fluss::client::FlussConnection;
    /// # use fluss::config::Config;
    /// # use fluss::error::Result;
    /// # use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    /// # use fluss::row::InternalRow;
    /// # use std::time::Duration;
    ///
    /// # pub async fn example() -> Result<()> {
    ///     let mut config = Config::default();
    ///     config.bootstrap_servers = "127.0.0.1:9123".to_string();
    ///     let conn = FlussConnection::new(config).await?;
    ///
    ///     let table_descriptor = TableDescriptor::builder()
    ///         .schema(
    ///             Schema::builder()
    ///                 .column("col1", DataTypes::int())
    ///                 .column("col2", DataTypes::string())
    ///                 .column("col3", DataTypes::string())
    ///                 .column("col4", DataTypes::string())
    ///             .build()?,
    ///         ).build()?;
    ///     let table_path = TablePath::new("fluss".to_owned(), "rust_test_long".to_owned());
    ///     let admin = conn.get_admin()?;
    ///     admin.create_table(&table_path, &table_descriptor, true)
    ///         .await?;
    ///     let table_info = admin.get_table_info(&table_path).await?;
    ///     let table = conn.get_table(&table_path).await?;
    ///
    ///     // Project columns by indices
    ///     let scanner = table.new_scan().project(&[0, 2, 3])?.create_log_scanner()?;
    ///     let scan_records = scanner.poll(Duration::from_secs(10)).await?;
    ///     for record in scan_records {
    ///         let row = record.row();
    ///         println!(
    ///             "{{{}, {}, {}}}@{}",
    ///             row.get_int(0)?,
    ///             row.get_string(2)?,
    ///             row.get_string(3)?,
    ///             record.offset()
    ///         );
    ///     }
    ///     # Ok(())
    /// # }
    /// ```
    pub fn project(mut self, column_indices: &[usize]) -> Result<Self> {
        if column_indices.is_empty() {
            return Err(Error::IllegalArgument {
                message: "Column indices cannot be empty".to_string(),
            });
        }
        let field_count = self.table_info.row_type().fields().len();
        for &idx in column_indices {
            if idx >= field_count {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Column index {} out of range (max: {})",
                        idx,
                        field_count - 1
                    ),
                });
            }
        }
        self.projected_fields = Some(column_indices.to_vec());
        Ok(self)
    }

    /// Projects the scan to only include specified columns by their names.
    ///
    /// # Arguments
    /// * `column_names` - Names of columns to include in the scan
    ///
    /// # Errors
    /// Returns an error if `column_names` is empty or if any column name is not found in the table schema.
    ///
    /// # Example
    /// ```
    /// # use fluss::client::FlussConnection;
    /// # use fluss::config::Config;
    /// # use fluss::error::Result;
    /// # use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    /// # use fluss::row::InternalRow;
    /// # use std::time::Duration;
    ///
    /// # pub async fn example() -> Result<()> {
    ///     let mut config = Config::default();
    ///     config.bootstrap_servers = "127.0.0.1:9123".to_string();
    ///     let conn = FlussConnection::new(config).await?;
    ///
    ///     let table_descriptor = TableDescriptor::builder()
    ///         .schema(
    ///             Schema::builder()
    ///                 .column("col1", DataTypes::int())
    ///                 .column("col2", DataTypes::string())
    ///                 .column("col3", DataTypes::string())
    ///             .build()?,
    ///         ).build()?;
    ///     let table_path = TablePath::new("fluss".to_owned(), "rust_test_long".to_owned());
    ///     let admin = conn.get_admin()?;
    ///     admin.create_table(&table_path, &table_descriptor, true)
    ///         .await?;
    ///     let table = conn.get_table(&table_path).await?;
    ///
    ///     // Project columns by column names
    ///     let scanner = table.new_scan().project_by_name(&["col1", "col3"])?.create_log_scanner()?;
    ///     let scan_records = scanner.poll(Duration::from_secs(10)).await?;
    ///     for record in scan_records {
    ///         let row = record.row();
    ///         println!(
    ///             "{{{}, {}}}@{}",
    ///             row.get_int(0)?,
    ///             row.get_string(1)?,
    ///             record.offset()
    ///         );
    ///     }
    ///     # Ok(())
    /// # }
    /// ```
    pub fn project_by_name(mut self, column_names: &[&str]) -> Result<Self> {
        if column_names.is_empty() {
            return Err(Error::IllegalArgument {
                message: "Column names cannot be empty".to_string(),
            });
        }
        let row_type = self.table_info.row_type();
        let mut indices = Vec::new();

        for name in column_names {
            let idx = row_type
                .fields()
                .iter()
                .position(|f| f.name() == *name)
                .ok_or_else(|| Error::IllegalArgument {
                    message: format!("Column '{name}' not found"),
                })?;
            indices.push(idx);
        }

        self.projected_fields = Some(indices);
        Ok(self)
    }

    pub fn create_log_scanner(self) -> Result<LogScanner> {
        self.reject_limit("LogScanner")?;
        validate_scan_support(&self.table_info.table_path, &self.table_info)?;
        let inner = LogScannerInner::new(
            &self.table_info,
            self.metadata.clone(),
            self.conn.get_connections(),
            self.conn.config(),
            self.projected_fields,
        )?;
        Ok(LogScanner {
            inner: Arc::new(inner),
        })
    }

    pub fn create_record_batch_log_scanner(self) -> Result<RecordBatchLogScanner> {
        self.reject_limit("RecordBatchLogScanner")?;
        validate_scan_support(&self.table_info.table_path, &self.table_info)?;
        let inner = LogScannerInner::new(
            &self.table_info,
            self.metadata.clone(),
            self.conn.get_connections(),
            self.conn.config(),
            self.projected_fields,
        )?;
        Ok(RecordBatchLogScanner {
            inner: Arc::new(inner),
        })
    }
}

/// Scanner for reading log records one at a time with per-record metadata.
///
/// Use this scanner when you need access to individual record offsets and timestamps.
/// For batch-level access, use [`RecordBatchLogScanner`] instead.
pub struct LogScanner {
    inner: Arc<LogScannerInner>,
}

/// Scanner for reading log data as Arrow RecordBatches.
///
/// More efficient than [`LogScanner`] for batch-level analytics where per-record
/// metadata (offsets, timestamps) is not needed.
///
/// This type is intentionally **not** `Clone`. To perform a bounded read, move
/// the scanner into a [`crate::client::RecordBatchLogReader`] — the compiler
/// then prevents concurrent polls by construction.
pub struct RecordBatchLogScanner {
    inner: Arc<LogScannerInner>,
}

/// Private shared implementation for both scanner types
struct LogScannerInner {
    table_path: TablePath,
    table_id: TableId,
    metadata: Arc<Metadata>,
    log_scanner_status: Arc<LogScannerStatus>,
    log_fetcher: LogFetcher,
    is_partitioned_table: bool,
    arrow_schema: SchemaRef,
    /// Guards against subscription changes while a
    /// [`crate::client::RecordBatchLogReader`] is iterating.
    reader_active: std::sync::atomic::AtomicBool,
    /// Holds the snapshot fields used by [`PollGuard`] to derive the
    /// scanner poll-timing metrics. The mutex makes the state updates
    /// in `record_poll_start` / `record_poll_end` atomic; metric
    /// emission and `log::warn!` calls happen after the lock is
    /// released. The start↔end pairing depends on the single-consumer
    /// contract documented on [`LogScanner::poll`] and
    /// [`RecordBatchLogScanner::poll`] (mirrors Java's
    /// `LogScannerImpl.acquire()`). Overlapping polls on the same
    /// scanner trip a `debug_assert!` in `record_poll_start` (debug
    /// builds) or emit a `log::warn!` (release builds).
    poll_state: Mutex<PollState>,
    /// Per-table scanner metric handles, pre-bound with `database`/`table`
    /// labels.
    metrics: Arc<ScannerMetrics>,
}

/// Snapshot state used to derive the scanner poll-timing metrics.
///
/// The mutex makes the state updates in `record_poll_start` /
/// `record_poll_end` atomic with respect to themselves; metric
/// emission (`metrics::gauge!(...).set(...)`) and `log::warn!` calls
/// happen after the lock is released so a user-installed recorder or
/// logger cannot stall the critical section. The mutex does **not** by
/// itself preserve start↔end pairing across overlapping `poll()` calls
/// — that invariant relies on the single-consumer contract that
/// mirrors Java's `LogScannerImpl.acquire()`. Concurrent polls on the
/// same scanner are detected by a `debug_assert!` in
/// `record_poll_start` (panics in debug / tests) and a `log::warn!` on
/// both anomalous paths (`record_poll_start` sees a stale `Some`;
/// `record_poll_end` sees `None`) for release-build observability.
#[derive(Default, Debug)]
struct PollState {
    /// Instant captured at the most recent `record_poll_start()`. `None`
    /// before the first poll.
    last_poll_at: Option<Instant>,
    /// Instant captured at the start of the in-flight poll. `None` after
    /// the last `record_poll_end()`.
    poll_start_at: Option<Instant>,
    /// Cached ms between the two most recent poll starts, used to compute
    /// `poll_idle_ratio` in `record_poll_end`.
    time_between_poll_ms: f64,
}

/// Pairs `record_poll_start` with `record_poll_end`. Created
/// at the top of `poll_records` / `poll_batches`; `record_poll_end` runs on
/// drop, including the cancellation path (caller drops the future).
struct PollGuard<'a> {
    inner: &'a LogScannerInner,
}

impl<'a> PollGuard<'a> {
    fn new(inner: &'a LogScannerInner) -> Self {
        inner.record_poll_start();
        Self { inner }
    }
}

impl Drop for PollGuard<'_> {
    fn drop(&mut self) {
        self.inner.record_poll_end();
    }
}

impl LogScannerInner {
    fn new(
        table_info: &TableInfo,
        metadata: Arc<Metadata>,
        connections: Arc<RpcClient>,
        config: &Config,
        projected_fields: Option<Vec<usize>>,
    ) -> Result<Self> {
        let log_scanner_status = Arc::new(LogScannerStatus::new());

        let full_row_type = table_info.get_row_type();
        let arrow_schema = match &projected_fields {
            Some(indices) => {
                let projected_fields_vec: Vec<_> = indices
                    .iter()
                    .map(|&i| full_row_type.fields()[i].clone())
                    .collect();
                let projected_row_type = crate::metadata::RowType::new(projected_fields_vec);
                to_arrow_schema(&projected_row_type)?
            }
            None => to_arrow_schema(full_row_type)?,
        };

        let metrics = Arc::new(ScannerMetrics::new(&table_info.table_path));
        Ok(Self {
            table_path: table_info.table_path.clone(),
            table_id: table_info.table_id,
            is_partitioned_table: table_info.is_partitioned(),
            metadata: metadata.clone(),
            log_scanner_status: log_scanner_status.clone(),
            log_fetcher: LogFetcher::new(
                table_info.clone(),
                connections,
                metadata,
                log_scanner_status.clone(),
                config,
                projected_fields,
                Arc::clone(&metrics),
            )?,
            arrow_schema,
            reader_active: std::sync::atomic::AtomicBool::new(false),
            poll_state: Mutex::new(PollState::default()),
            metrics,
        })
    }

    fn check_no_active_reader(&self) -> Result<()> {
        if self
            .reader_active
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::IllegalArgument {
                message: "Cannot modify subscriptions while a RecordBatchLogReader is active. \
                          Drop the reader first."
                    .to_string(),
            });
        }
        Ok(())
    }

    async fn poll_records(&self, timeout: Duration) -> Result<ScanRecords> {
        // Pairs record_poll_start (now) with record_poll_end
        // (drop). Runs on every exit, including the cancellation path
        // where the caller drops this future.
        let _poll_guard = PollGuard::new(self);
        let start = Instant::now();
        let deadline = start + timeout;

        loop {
            // Try to collect fetches
            let fetch_result = self.poll_for_fetches().await?;

            if !fetch_result.is_empty() {
                // We have data, send next round of fetches and return
                // This enables pipelining while user processes the data
                self.log_fetcher.send_fetches().await?;
                return Ok(ScanRecords::new(fetch_result));
            }

            // No data available, check if we should wait
            let now = Instant::now();
            if now >= deadline {
                // Timeout reached, return empty result
                return Ok(ScanRecords::new(HashMap::new()));
            }

            // Wait for buffer to become non-empty with remaining time
            let remaining = deadline - now;
            let has_data = self
                .log_fetcher
                .log_fetch_buffer
                .await_not_empty(remaining)
                .await?;

            if !has_data {
                // Timeout while waiting
                return Ok(ScanRecords::new(HashMap::new()));
            }

            // Buffer became non-empty, try again
        }
    }

    /// Records the start of a `poll()` call and emits
    /// `SCANNER_TIME_BETWEEN_POLL_MS`. The first poll emits `0.0`,
    /// matching Java's `ScannerMetricGroup.recordPollStart`
    /// (`timeMsBetweenPoll = lastPollMs != 0L ? pollStartMs - lastPollMs : 0L`).
    ///
    /// Single-consumer contract: a previous poll must have recorded its
    /// end before the next start. Java enforces this with
    /// `LogScannerImpl.acquire()` (throws `ConcurrentModificationException`).
    /// Rust surfaces violations as:
    /// - debug builds: `debug_assert!` panics (caught by tests),
    /// - release builds: `log::warn!` + the in-flight `poll_start_at` is
    ///   overwritten so the metric series keeps moving; the resulting
    ///   `time_between_poll_ms` / `poll_idle_ratio` values for the
    ///   overlapping polls are not meaningful until the overlap clears.
    fn record_poll_start(&self) {
        let now = Instant::now();
        // Compute under the lock; emit the metric outside the critical
        // section so a user-installed recorder cannot stall the next poll.
        let (between_ms, overlap) = {
            let mut state = self.poll_state.lock();
            let overlap = state.poll_start_at.is_some();
            debug_assert!(
                !overlap,
                "concurrent poll() detected on the same scanner; \
                 LogScanner / RecordBatchLogScanner are single-consumer \
                 (see LogScannerImpl.acquire() for Java parity)"
            );
            let between_ms = match state.last_poll_at {
                Some(prev) => now.duration_since(prev).as_secs_f64() * 1000.0,
                None => 0.0,
            };
            state.time_between_poll_ms = between_ms;
            state.last_poll_at = Some(now);
            state.poll_start_at = Some(now);
            (between_ms, overlap)
        };
        if overlap {
            warn!(
                "concurrent poll() detected on scanner; single-consumer \
                 contract violated, poll-timing metrics will be inaccurate \
                 until the overlap clears"
            );
        }
        self.metrics.record_time_between_poll_ms(between_ms);
    }

    /// Computes `poll_idle_ratio = poll_time / (poll_time + between_time)`.
    /// On the first poll, `between_time` is 0 so the ratio is 1.0
    /// (poll-bound).
    ///
    /// Orphan call: if no matching `record_poll_start` is in flight,
    /// emits a `log::warn!` (single-consumer contract may have been
    /// violated, e.g. in release builds where the start-side
    /// `debug_assert!` is compiled out) and skips the metric update.
    fn record_poll_end(&self) {
        let now = Instant::now();
        // Compute under the lock; emit metric / warn outside the critical
        // section so neither the user-installed recorder nor the logger
        // can stall the next poll.
        let (orphan, ratio) = {
            let mut state = self.poll_state.lock();
            match state.poll_start_at.take() {
                None => (true, None),
                Some(start) => {
                    let poll_time_ms = now.duration_since(start).as_secs_f64() * 1000.0;
                    let total = poll_time_ms + state.time_between_poll_ms;
                    let r = (total > 0.0).then_some(poll_time_ms / total);
                    (false, r)
                }
            }
        };
        if orphan {
            warn!(
                "record_poll_end called without a matching record_poll_start; \
                 single-consumer contract may have been violated, idle ratio \
                 for this poll is not emitted"
            );
            return;
        }
        if let Some(r) = ratio {
            self.metrics.record_poll_idle_ratio(r);
        }
    }

    async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        self.check_no_active_reader()?;
        if self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message: "The table is a partitioned table, please use \"subscribe_partition\" to \
                subscribe a partitioned bucket instead."
                    .to_string(),
            });
        }
        let table_bucket = TableBucket::new(self.table_id, bucket);
        self.metadata
            .check_and_update_table_metadata(from_ref(&self.table_path))
            .await?;
        self.log_scanner_status
            .assign_scan_bucket(table_bucket, offset);
        Ok(())
    }

    async fn subscribe_buckets(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()> {
        self.check_no_active_reader()?;
        if self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message:
                    "The table is a partitioned table, please use \"subscribe_partition_buckets\" instead."
                        .to_string(),
            });
        }

        let mut scan_bucket_offsets = HashMap::new();
        for (bucket_id, offset) in bucket_offsets {
            let table_bucket = TableBucket::new(self.table_id, *bucket_id);
            scan_bucket_offsets.insert(table_bucket, *offset);
        }
        self.do_subscribe_buckets(scan_bucket_offsets).await
    }

    async fn subscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
        offset: i64,
    ) -> Result<()> {
        self.check_no_active_reader()?;
        if !self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message: "The table is not a partitioned table, please use \"subscribe\" to \
                subscribe a non-partitioned bucket instead."
                    .to_string(),
            });
        }
        let table_bucket =
            TableBucket::new_with_partition(self.table_id, Some(partition_id), bucket);
        self.metadata
            .check_and_update_table_metadata(from_ref(&self.table_path))
            .await?;
        self.log_scanner_status
            .assign_scan_bucket(table_bucket, offset);
        Ok(())
    }

    async fn subscribe_partition_buckets(
        &self,
        partition_bucket_offsets: &HashMap<(PartitionId, i32), i64>,
    ) -> Result<()> {
        self.check_no_active_reader()?;
        if !self.is_partitioned_table {
            return Err(UnsupportedOperation {
                message: "The table is not a partitioned table, please use \"subscribe_buckets\" \
                    to subscribe to non-partitioned buckets instead."
                    .to_string(),
            });
        }

        let mut scan_bucket_offsets = HashMap::new();
        for (&(partition_id, bucket_id), &offset) in partition_bucket_offsets {
            let table_bucket =
                TableBucket::new_with_partition(self.table_id, Some(partition_id), bucket_id);
            scan_bucket_offsets.insert(table_bucket, offset);
        }
        self.do_subscribe_buckets(scan_bucket_offsets).await
    }

    async fn do_subscribe_buckets(&self, bucket_offsets: HashMap<TableBucket, i64>) -> Result<()> {
        if bucket_offsets.is_empty() {
            return Err(Error::UnexpectedError {
                message: "Bucket offsets are empty.".to_string(),
                source: None,
            });
        }

        self.metadata
            .check_and_update_table_metadata(from_ref(&self.table_path))
            .await?;

        self.log_scanner_status.assign_scan_buckets(bucket_offsets);
        Ok(())
    }

    async fn unsubscribe(&self, bucket: i32) -> Result<()> {
        self.check_no_active_reader()?;
        if self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message:
                    "The table is a partitioned table, please use \"unsubscribe_partition\" to \
                    unsubscribe a partitioned bucket instead."
                        .to_string(),
            });
        }
        let table_bucket = TableBucket::new(self.table_id, bucket);
        self.log_scanner_status
            .unassign_scan_buckets(from_ref(&table_bucket));
        Ok(())
    }

    async fn unsubscribe_partition(&self, partition_id: PartitionId, bucket: i32) -> Result<()> {
        self.check_no_active_reader()?;
        if !self.is_partitioned_table {
            return Err(Error::UnsupportedOperation {
                message: "Can't unsubscribe a partition for a non-partitioned table.".to_string(),
            });
        }
        let table_bucket =
            TableBucket::new_with_partition(self.table_id, Some(partition_id), bucket);
        self.log_scanner_status
            .unassign_scan_buckets(from_ref(&table_bucket));
        Ok(())
    }

    async fn poll_for_fetches(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        let result = self.log_fetcher.collect_fetches()?;
        if !result.is_empty() {
            return Ok(result);
        }

        // send any new fetches (won't resend pending fetches).
        self.log_fetcher.send_fetches().await?;

        // Collect completed fetches from buffer
        self.log_fetcher.collect_fetches()
    }

    async fn poll_batches(&self, timeout: Duration) -> Result<Vec<ScanBatch>> {
        let _poll_guard = PollGuard::new(self);
        let start = Instant::now();
        let deadline = start + timeout;

        loop {
            let batches = self.poll_for_batches().await?;

            if !batches.is_empty() {
                self.log_fetcher.send_fetches().await?;
                return Ok(batches);
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(Vec::new());
            }

            let remaining = deadline - now;
            let has_data = self
                .log_fetcher
                .log_fetch_buffer
                .await_not_empty(remaining)
                .await?;

            if !has_data {
                return Ok(Vec::new());
            }
        }
    }

    async fn poll_for_batches(&self) -> Result<Vec<ScanBatch>> {
        let result = self.log_fetcher.collect_batches()?;
        if !result.is_empty() {
            return Ok(result);
        }

        self.log_fetcher.send_fetches().await?;
        self.log_fetcher.collect_batches()
    }
}

// Implementation for LogScanner (records mode)
impl LogScanner {
    pub async fn poll(&self, timeout: Duration) -> Result<ScanRecords> {
        self.inner.poll_records(timeout).await
    }

    pub async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        self.inner.subscribe(bucket, offset).await
    }

    pub async fn subscribe_buckets(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()> {
        self.inner.subscribe_buckets(bucket_offsets).await
    }

    pub async fn subscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
        offset: i64,
    ) -> Result<()> {
        self.inner
            .subscribe_partition(partition_id, bucket, offset)
            .await
    }

    pub async fn subscribe_partition_buckets(
        &self,
        partition_bucket_offsets: &HashMap<(PartitionId, i32), i64>,
    ) -> Result<()> {
        self.inner
            .subscribe_partition_buckets(partition_bucket_offsets)
            .await
    }

    pub async fn unsubscribe(&self, bucket: i32) -> Result<()> {
        self.inner.unsubscribe(bucket).await
    }

    pub async fn unsubscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
    ) -> Result<()> {
        self.inner.unsubscribe_partition(partition_id, bucket).await
    }
}

// Implementation for RecordBatchLogScanner (batches mode)
impl RecordBatchLogScanner {
    /// Poll for batches with metadata (bucket and offset information).
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ScanBatch>> {
        self.inner.poll_batches(timeout).await
    }

    pub async fn subscribe(&self, bucket: i32, offset: i64) -> Result<()> {
        self.inner.subscribe(bucket, offset).await
    }

    pub async fn subscribe_buckets(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()> {
        self.inner.subscribe_buckets(bucket_offsets).await
    }

    pub async fn subscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
        offset: i64,
    ) -> Result<()> {
        self.inner
            .subscribe_partition(partition_id, bucket, offset)
            .await
    }

    /// Returns whether the table is partitioned
    pub fn is_partitioned(&self) -> bool {
        self.inner.is_partitioned_table
    }

    /// Returns all subscribed buckets with their current offsets
    pub fn get_subscribed_buckets(&self) -> Vec<(TableBucket, i64)> {
        self.inner.log_scanner_status.get_all_subscriptions()
    }

    pub async fn subscribe_partition_buckets(
        &self,
        partition_bucket_offsets: &HashMap<(PartitionId, i32), i64>,
    ) -> Result<()> {
        self.inner
            .subscribe_partition_buckets(partition_bucket_offsets)
            .await
    }

    pub async fn unsubscribe(&self, bucket: i32) -> Result<()> {
        self.inner.unsubscribe(bucket).await
    }

    pub async fn unsubscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket: i32,
    ) -> Result<()> {
        self.inner.unsubscribe_partition(partition_id, bucket).await
    }

    /// Returns the Arrow schema for batches produced by this scanner.
    pub fn schema(&self) -> SchemaRef {
        self.inner.arrow_schema.clone()
    }

    pub fn table_path(&self) -> &TablePath {
        &self.inner.table_path
    }

    pub fn table_id(&self) -> TableId {
        self.inner.table_id
    }

    /// Creates a new handle to the same underlying scanner state.
    ///
    /// Binding layers (Python, C++) that hold the scanner behind shared
    /// ownership (`Arc`) cannot move it into a [`crate::client::RecordBatchLogReader`].
    /// This method produces a second handle so the reader can take ownership
    /// while the binding retains its reference for subscription management.
    ///
    /// **Not intended for general use** — prefer moving the scanner directly.
    #[doc(hidden)]
    pub fn new_shared_handle(&self) -> Self {
        RecordBatchLogScanner {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Atomically marks the scanner as having an active reader.
    ///
    /// Returns `Err(IllegalArgument)` if another reader is already active on
    /// this scanner — only one [`crate::client::RecordBatchLogReader`] may
    /// iterate per scanner at a time. This mirrors Java's
    /// `LogScannerImpl.acquire()` single-consumer guard.
    pub(crate) fn try_set_reader_active(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        self.inner
            .reader_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| Error::IllegalArgument {
                message: "Another RecordBatchLogReader is already active on this scanner. \
                          Drop the existing reader first."
                    .to_string(),
            })
    }

    /// Clears the active-reader guard, re-enabling subscription changes.
    pub(crate) fn clear_reader_active(&self) {
        self.inner
            .reader_active
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Synchronous, infallible counterpart to [`unsubscribe`](Self::unsubscribe).
    ///
    /// Exists so [`crate::client::RecordBatchLogReader`]'s `Drop` impl can
    /// release lingering subscriptions without `.await`. The async version is
    /// also synchronous under the hood (it only acquires a lock and removes
    /// from a map — no IO), so this exposes the same work without the
    /// async wrapper. Silently no-ops on partitioned/non-partitioned mismatch
    /// because `Drop` cannot return errors; callers must pick the correct
    /// variant.
    ///
    /// **Not intended for general use** — prefer the async [`unsubscribe`].
    pub(crate) fn unsubscribe_sync(&self, bucket: i32) {
        if self.inner.is_partitioned_table {
            return;
        }
        let table_bucket = TableBucket::new(self.inner.table_id, bucket);
        self.inner
            .log_scanner_status
            .unassign_scan_buckets(from_ref(&table_bucket));
    }

    /// Synchronous, infallible counterpart to
    /// [`unsubscribe_partition`](Self::unsubscribe_partition). See
    /// [`unsubscribe_sync`](Self::unsubscribe_sync) for rationale.
    pub(crate) fn unsubscribe_partition_sync(&self, partition_id: PartitionId, bucket: i32) {
        if !self.inner.is_partitioned_table {
            return;
        }
        let table_bucket =
            TableBucket::new_with_partition(self.inner.table_id, Some(partition_id), bucket);
        self.inner
            .log_scanner_status
            .unassign_scan_buckets(from_ref(&table_bucket));
    }
}

struct LogFetcher {
    conns: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    table_path: TablePath,
    is_partitioned: bool,
    log_scanner_status: Arc<LogScannerStatus>,
    read_context: ReadContext,
    remote_read_context: ReadContext,
    remote_log_downloader: Arc<RemoteLogDownloader>,
    /// Background security token manager for remote filesystem access.
    /// Kept alive to run the background refresh task; stopped on drop.
    #[allow(dead_code)]
    security_token_manager: Arc<SecurityTokenManager>,
    log_fetch_buffer: Arc<LogFetchBuffer>,
    nodes_with_pending_fetch_requests: Arc<Mutex<HashSet<i32>>>,
    /// Per-table scanner metric handles shared with the owning
    /// `LogScannerInner` and `RemoteLogDownloader`.
    metrics: Arc<ScannerMetrics>,
    max_poll_records: usize,
    fetch_max_bytes: i32,
    fetch_min_bytes: i32,
    fetch_wait_max_time_ms: i32,
    fetch_max_bytes_for_bucket: i32,
}

struct FetchResponseContext {
    metadata: Arc<Metadata>,
    log_fetch_buffer: Arc<LogFetchBuffer>,
    log_scanner_status: Arc<LogScannerStatus>,
    read_context: ReadContext,
    remote_read_context: ReadContext,
    remote_log_downloader: Arc<RemoteLogDownloader>,
    /// Per-table scanner metric handles for `scanner.fetch_*` recording.
    metrics: Arc<ScannerMetrics>,
    /// `Instant` captured immediately before the FetchLog RPC; used to compute
    /// `scanner.fetch_latency_ms` on a successful response.
    request_start_time: Instant,
}

impl LogFetcher {
    pub fn new(
        table_info: TableInfo,
        conns: Arc<RpcClient>,
        metadata: Arc<Metadata>,
        log_scanner_status: Arc<LogScannerStatus>,
        config: &Config,
        projected_fields: Option<Vec<usize>>,
        metrics: Arc<ScannerMetrics>,
    ) -> Result<Self> {
        let full_row_type = table_info.get_row_type();
        let full_arrow_schema = to_arrow_schema(full_row_type)?;
        let projected_row_type = match &projected_fields {
            None => Arc::new(full_row_type.clone()),
            Some(fields) => Arc::new(RowType::new(
                fields
                    .iter()
                    .map(|&i| full_row_type.fields()[i].clone())
                    .collect(),
            )),
        };
        let read_context = Self::create_read_context(
            full_arrow_schema.clone(),
            projected_row_type.clone(),
            projected_fields.clone(),
            false,
        )?
        .with_fluss_row_type(projected_row_type.clone());
        let remote_read_context = Self::create_read_context(
            full_arrow_schema,
            projected_row_type.clone(),
            projected_fields.clone(),
            true,
        )?
        .with_fluss_row_type(projected_row_type);

        let tmp_dir = TempDir::with_prefix("fluss-remote-logs")?;
        let log_fetch_buffer = Arc::new(LogFetchBuffer::new(read_context.clone()));

        // Create security token manager for background token refresh
        let security_token_manager =
            Arc::new(SecurityTokenManager::new(conns.clone(), metadata.clone()));

        // Subscribe to credentials updates and pass to remote log downloader
        let credentials_rx = security_token_manager.subscribe();

        let remote_log_downloader = Arc::new(RemoteLogDownloader::new(
            tmp_dir,
            config.scanner_remote_log_prefetch_num,
            config.remote_file_download_thread_num,
            config.scanner_remote_log_read_concurrency,
            credentials_rx,
            Arc::clone(&metrics),
        )?);

        // Start the background token refresh task
        security_token_manager.start();

        Ok(LogFetcher {
            conns: conns.clone(),
            metadata: metadata.clone(),
            table_path: table_info.table_path.clone(),
            is_partitioned: table_info.is_partitioned(),
            log_scanner_status,
            read_context,
            remote_read_context,
            remote_log_downloader,
            security_token_manager,
            log_fetch_buffer,
            nodes_with_pending_fetch_requests: Arc::new(Mutex::new(HashSet::new())),
            metrics,
            max_poll_records: config.scanner_log_max_poll_records,
            fetch_max_bytes: config.scanner_log_fetch_max_bytes,
            fetch_min_bytes: config.scanner_log_fetch_min_bytes,
            fetch_wait_max_time_ms: config.scanner_log_fetch_wait_max_time_ms,
            fetch_max_bytes_for_bucket: config.scanner_log_fetch_max_bytes_for_bucket,
        })
    }

    fn create_read_context(
        full_arrow_schema: SchemaRef,
        row_type: Arc<RowType>,
        projected_fields: Option<Vec<usize>>,
        is_from_remote: bool,
    ) -> Result<ReadContext> {
        match projected_fields {
            None => Ok(ReadContext::new(
                full_arrow_schema,
                row_type,
                is_from_remote,
            )),
            Some(fields) => ReadContext::with_projection_pushdown(
                full_arrow_schema,
                row_type,
                fields,
                is_from_remote,
            ),
        }
    }

    fn describe_fetch_error(
        error: FlussError,
        table_bucket: &TableBucket,
        fetch_offset: i64,
        error_message: &str,
    ) -> FetchErrorContext {
        match error {
            FlussError::NotLeaderOrFollower
            | FlussError::LogStorageException
            | FlussError::KvStorageException
            | FlussError::StorageException
            | FlussError::FencedLeaderEpochException
            | FlussError::LeaderNotAvailableException => FetchErrorContext {
                action: FetchErrorAction::Ignore,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Error in fetch for bucket {table_bucket}: {error:?}: {error_message}"
                ),
            },
            FlussError::UnknownTableOrBucketException => FetchErrorContext {
                action: FetchErrorAction::Ignore,
                log_level: FetchErrorLogLevel::Warn,
                log_message: format!(
                    "Received unknown table or bucket error in fetch for bucket {table_bucket}"
                ),
            },
            FlussError::LogOffsetOutOfRangeException => FetchErrorContext {
                action: FetchErrorAction::LogOffsetOutOfRange,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "The fetching offset {fetch_offset} is out of range for bucket {table_bucket}: {error_message}"
                ),
            },
            FlussError::AuthorizationException => FetchErrorContext {
                action: FetchErrorAction::Authorization,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Authorization error while fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                ),
            },
            FlussError::UnknownServerError => FetchErrorContext {
                action: FetchErrorAction::Ignore,
                log_level: FetchErrorLogLevel::Warn,
                log_message: format!(
                    "Unknown server error while fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                ),
            },
            FlussError::CorruptMessage => FetchErrorContext {
                action: FetchErrorAction::CorruptMessage,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Encountered corrupt message when fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                ),
            },
            _ => FetchErrorContext {
                action: FetchErrorAction::Unexpected,
                log_level: FetchErrorLogLevel::Debug,
                log_message: format!(
                    "Unexpected error code {error:?} while fetching at offset {fetch_offset} from bucket {table_bucket}: {error_message}"
                ),
            },
        }
    }

    fn should_invalidate_table_meta(error: FlussError) -> bool {
        matches!(
            error,
            FlussError::NotLeaderOrFollower
                | FlussError::LeaderNotAvailableException
                | FlussError::FencedLeaderEpochException
                | FlussError::UnknownTableOrBucketException
                | FlussError::InvalidCoordinatorException
        )
    }

    async fn check_and_update_metadata(&self, table_buckets: &[TableBucket]) -> Result<()> {
        let mut partition_ids = Vec::new();
        let mut need_update = false;

        for tb in table_buckets {
            if self.get_table_bucket_leader(tb).is_some() {
                continue;
            }

            if self.is_partitioned {
                partition_ids.push(tb.partition_id().unwrap());
            } else {
                need_update = true;
                break;
            }
        }

        let update_result = if self.is_partitioned && !partition_ids.is_empty() {
            self.metadata
                .update_tables_metadata(
                    &HashSet::from([&self.table_path]),
                    &HashSet::new(),
                    partition_ids,
                )
                .await
        } else if need_update {
            self.metadata.update_table_metadata(&self.table_path).await
        } else {
            Ok(())
        };

        // TODO: Handle PartitionNotExist error like java side
        update_result.or_else(|e| {
            if let Error::RpcError { source, .. } = &e
                && matches!(source, RpcError::ConnectionError(_) | RpcError::Poisoned(_))
            {
                warn!("Retrying after encountering error while updating table metadata: {e}");
                Ok(())
            } else {
                Err(e)
            }
        })?;
        Ok(())
    }

    /// Send fetch requests asynchronously without waiting for responses
    async fn send_fetches(&self) -> Result<()> {
        self.check_and_update_metadata(self.fetchable_buckets().as_slice())
            .await?;
        let fetch_request = self.prepare_fetch_log_requests().await;

        for (leader, fetch_request) in fetch_request {
            debug!("Adding pending request for node id {leader}");
            // Check if we already have a pending request for this node
            {
                self.nodes_with_pending_fetch_requests.lock().insert(leader);
            }

            let cluster = self.metadata.get_cluster().clone();

            let conns = Arc::clone(&self.conns);
            let log_fetch_buffer = self.log_fetch_buffer.clone();
            let log_scanner_status = self.log_scanner_status.clone();
            let read_context = self.read_context.clone();
            let remote_read_context = self.remote_read_context.clone();
            let remote_log_downloader = Arc::clone(&self.remote_log_downloader);
            let nodes_with_pending = self.nodes_with_pending_fetch_requests.clone();
            let metadata = self.metadata.clone();
            let metrics = Arc::clone(&self.metrics);
            // Spawn async task to handle the fetch request
            // Note: These tasks are not explicitly tracked or cancelled when LogFetcher is dropped.
            // This is acceptable because:
            // 1. Tasks will naturally complete (network requests will return or timeout)
            // 2. Tasks use Arc references, so resources are properly shared
            // 3. When the program exits, tokio runtime will clean up all tasks
            // 4. Tasks are short-lived (network I/O operations)
            tokio::spawn(async move {
                // make sure it will always remove leader from pending nodes
                let _guard = scopeguard::guard((), |_| {
                    nodes_with_pending.lock().remove(&leader);
                });

                let server_node = match cluster.get_tablet_server(leader) {
                    Some(node) => node,
                    None => {
                        warn!("No server node found for leader {leader}, retrying");
                        Self::handle_fetch_failure(metadata, &leader, &fetch_request).await;
                        return;
                    }
                };

                let con = match conns.get_connection(server_node).await {
                    Ok(con) => con,
                    Err(e) => {
                        warn!("Retrying after error getting connection to destination node: {e:?}");
                        Self::handle_fetch_failure(metadata, &leader, &fetch_request).await;
                        return;
                    }
                };

                // Java increment the fetch counter and capture `requestStartTime` immediately
                // before the RPC. Failed connection acquisition above is not counted.
                let request_start_time = Instant::now();
                metrics.record_fetch_request();

                let fetch_response = match con
                    .request(message::FetchLogRequest::new(fetch_request.clone()))
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!(
                            "Retrying after error fetching log from destination node {server_node:?}: {e:?}"
                        );
                        Self::handle_fetch_failure(metadata, &leader, &fetch_request).await;
                        return;
                    }
                };

                // Build the context after the RPC so `request_start_time` measures only RPC wall-clock
                // — not tablet-server lookup or connection acquisition, which is matching Java's bebaviour
                // Building it here also skips the allocation on the early-return error paths above.
                let response_context = FetchResponseContext {
                    metadata: metadata.clone(),
                    log_fetch_buffer,
                    log_scanner_status,
                    read_context,
                    remote_read_context,
                    remote_log_downloader,
                    metrics,
                    request_start_time,
                };
                Self::handle_fetch_response(fetch_response, response_context).await;
            });
        }

        Ok(())
    }

    async fn handle_fetch_failure(
        metadata: Arc<Metadata>,
        server_id: &i32,
        request: &FetchLogRequest,
    ) {
        let table_ids = request.tables_req.iter().map(|r| r.table_id).collect();
        metadata.invalidate_server(server_id, table_ids);
    }

    /// Handle fetch response and add completed fetches to buffer
    async fn handle_fetch_response(
        fetch_response: FetchLogResponse,
        context: FetchResponseContext,
    ) {
        let FetchResponseContext {
            metadata,
            log_fetch_buffer,
            log_scanner_status,
            read_context,
            remote_read_context,
            remote_log_downloader,
            metrics,
            request_start_time,
        } = context;

        // `encoded_len()` mirrors Java's `fetchLogResponse.totalSize()`:
        // both report the serialized API message body size, excluding protocol
        // headers and framing. Recorded unconditionally (including zero-record
        // responses) to match Java's histogram semantics.
        metrics.record_fetch_latency_ms(request_start_time.elapsed().as_secs_f64() * 1000.0);
        metrics.record_bytes_per_request(fetch_response.encoded_len() as f64);

        for pb_fetch_log_resp in fetch_response.tables_resp {
            let table_id = pb_fetch_log_resp.table_id;
            let fetch_log_for_buckets = pb_fetch_log_resp.buckets_resp;

            for fetch_log_for_bucket in fetch_log_for_buckets {
                let bucket: i32 = fetch_log_for_bucket.bucket_id;
                let table_bucket = TableBucket::new_with_partition(
                    table_id,
                    fetch_log_for_bucket.partition_id,
                    bucket,
                );

                // todo: check fetch result code for per-bucket
                let Some(fetch_offset) = log_scanner_status.get_bucket_offset(&table_bucket) else {
                    debug!(
                        "Ignoring fetch log response for bucket {table_bucket} because the bucket has been unsubscribed."
                    );
                    continue;
                };

                if let Some(error_code) = fetch_log_for_bucket.error_code
                    && error_code != FlussError::None.code()
                {
                    let api_error: ApiError = ErrorResponse {
                        error_code,
                        error_message: fetch_log_for_bucket.error_message.clone(),
                    }
                    .into();

                    let error = FlussError::for_code(error_code);
                    if Self::should_invalidate_table_meta(error) {
                        // TODO: Consider triggering table meta invalidation from sender/lookup paths.
                        let table_id = table_bucket.table_id();
                        let cluster = metadata.get_cluster();
                        if let Some(table_path) = cluster.get_table_path_by_id(table_id) {
                            let physical_tables = HashSet::from([PhysicalTablePath::of(Arc::new(
                                table_path.clone(),
                            ))]);
                            metadata.invalidate_physical_table_meta(&physical_tables);
                        } else {
                            warn!(
                                "Table id {table_id} is missing from table_path_by_id while invalidating table metadata"
                            );
                        }
                    }
                    let error_context = Self::describe_fetch_error(
                        error,
                        &table_bucket,
                        fetch_offset,
                        api_error.message.as_str(),
                    );
                    log_scanner_status.move_bucket_to_end(table_bucket.clone());
                    match error_context.log_level {
                        FetchErrorLogLevel::Debug => {
                            debug!("{}", error_context.log_message);
                        }
                        FetchErrorLogLevel::Warn => {
                            warn!("{}", error_context.log_message);
                        }
                    }
                    log_fetch_buffer.add_api_error(
                        table_bucket.clone(),
                        api_error,
                        error_context,
                        fetch_offset,
                    );
                    continue;
                }

                // Check if this is a remote log fetch
                if let Some(ref remote_log_fetch_info) = fetch_log_for_bucket.remote_log_fetch_info
                {
                    // Remote fs props are already set by the background SecurityTokenManager
                    let remote_fetch_info =
                        RemoteLogFetchInfo::from_proto(remote_log_fetch_info, table_bucket.clone());

                    let high_watermark = fetch_log_for_bucket.high_watermark.unwrap_or(-1);
                    Self::pending_remote_fetches(
                        remote_log_downloader.clone(),
                        log_fetch_buffer.clone(),
                        remote_read_context.clone(),
                        &table_bucket,
                        remote_fetch_info,
                        fetch_offset,
                        high_watermark,
                    );
                } else if fetch_log_for_bucket.records.is_some() {
                    // Handle regular in-memory records - create completed fetch directly
                    let high_watermark = fetch_log_for_bucket.high_watermark.unwrap_or(-1);
                    let records = fetch_log_for_bucket.records.unwrap_or(vec![]);
                    let size_in_bytes = records.len();
                    let log_record_batch = LogRecordsBatches::new(records);

                    let completed_fetch = DefaultCompletedFetch::new(
                        table_bucket.clone(),
                        log_record_batch,
                        size_in_bytes,
                        read_context.clone(),
                        fetch_offset,
                        high_watermark,
                    );
                    log_fetch_buffer.add(Box::new(completed_fetch));
                }
            }
        }
    }

    fn pending_remote_fetches(
        remote_log_downloader: Arc<RemoteLogDownloader>,
        log_fetch_buffer: Arc<LogFetchBuffer>,
        read_context: ReadContext,
        table_bucket: &TableBucket,
        remote_fetch_info: RemoteLogFetchInfo,
        fetch_offset: i64,
        high_watermark: i64,
    ) {
        // Download and process remote log segments
        let mut pos_in_log_segment = remote_fetch_info.first_start_pos;
        let mut current_fetch_offset = fetch_offset;
        for (i, segment) in remote_fetch_info.remote_log_segments.iter().enumerate() {
            if i > 0 {
                pos_in_log_segment = 0;
                current_fetch_offset = segment.start_offset;
            }

            // todo:
            // 1: control the max threads to download remote segment
            // 2: introduce priority queue to priority highest for earliest segment
            let download_future = remote_log_downloader
                .request_remote_log(&remote_fetch_info.remote_log_tablet_dir, segment);

            // Register callback to be called when download completes
            // (similar to Java's downloadFuture.onComplete)
            // This must be done before creating RemotePendingFetch to avoid move issues
            let table_bucket = table_bucket.clone();
            let log_fetch_buffer_clone = log_fetch_buffer.clone();
            download_future.on_complete(move || {
                log_fetch_buffer_clone.try_complete(&table_bucket);
            });

            let pending_fetch = RemotePendingFetch::new(
                segment.clone(),
                download_future,
                pos_in_log_segment,
                current_fetch_offset,
                high_watermark,
                read_context.clone(),
            );
            // Add to pending fetches in buffer (similar to Java's logFetchBuffer.pend)
            log_fetch_buffer.pend(Box::new(pending_fetch));
        }
    }

    /// Collect completed fetches from buffer
    /// Reference: LogFetchCollector.collectFetch in Java
    fn collect_fetches(&self) -> Result<HashMap<TableBucket, Vec<ScanRecord>>> {
        let mut result: HashMap<TableBucket, Vec<ScanRecord>> = HashMap::new();
        let mut records_remaining = self.max_poll_records;

        let collect_result: Result<()> = {
            while records_remaining > 0 {
                // Get the next in line fetch, or get a new one from buffer
                let next_in_line = self.log_fetch_buffer.next_in_line_fetch();

                if next_in_line.is_none() || next_in_line.as_ref().unwrap().is_consumed() {
                    // Get a new fetch from buffer
                    if let Some(completed_fetch) = self.log_fetch_buffer.poll() {
                        // Initialize the fetch if not already initialized
                        if !completed_fetch.is_initialized() {
                            let size_in_bytes = completed_fetch.size_in_bytes();
                            match self.initialize_fetch(completed_fetch) {
                                Ok(initialized) => {
                                    self.log_fetch_buffer.set_next_in_line_fetch(initialized);
                                    continue;
                                }
                                Err(e) => {
                                    // Remove a completedFetch upon a parse with exception if
                                    // (1) it contains no records, and
                                    // (2) there are no fetched records with actual content preceding this
                                    // exception.
                                    if result.is_empty() && size_in_bytes == 0 {
                                        // todo: do we need to consider it like java ?
                                        // self.log_fetch_buffer.poll();
                                    }
                                    return Err(e);
                                }
                            }
                        } else {
                            self.log_fetch_buffer
                                .set_next_in_line_fetch(Some(completed_fetch));
                        }
                        // Note: poll() already removed the fetch from buffer, so no need to call poll()
                    } else {
                        // No more fetches available
                        break;
                    }
                } else {
                    // Fetch records from next_in_line
                    if let Some(mut next_fetch) = next_in_line {
                        let records = match self
                            .fetch_records_from_fetch(&mut next_fetch, records_remaining)
                        {
                            Ok(records) => records,
                            Err(e) => {
                                if !next_fetch.is_consumed() {
                                    self.log_fetch_buffer
                                        .set_next_in_line_fetch(Some(next_fetch));
                                }
                                return Err(e);
                            }
                        };

                        if !records.is_empty() {
                            let table_bucket = next_fetch.table_bucket().clone();
                            // Merge with existing records for this bucket
                            let existing = result.entry(table_bucket).or_default();
                            let records_count = records.len();
                            existing.extend(records);

                            records_remaining = records_remaining.saturating_sub(records_count);
                        }

                        // If the fetch is not fully consumed, put it back for the next round
                        if !next_fetch.is_consumed() {
                            self.log_fetch_buffer
                                .set_next_in_line_fetch(Some(next_fetch));
                        }
                        // If consumed, next_fetch will be dropped here (which is correct)
                    }
                }
            }
            Ok(())
        };

        match collect_result {
            Ok(()) => Ok(result),
            Err(e) => {
                if result.is_empty() {
                    Err(e)
                } else {
                    Ok(result)
                }
            }
        }
    }

    /// Initialize a completed fetch, checking offset match and updating high watermark
    fn initialize_fetch(
        &self,
        mut completed_fetch: Box<dyn CompletedFetch>,
    ) -> Result<Option<Box<dyn CompletedFetch>>> {
        if let Some(error) = completed_fetch.take_error() {
            return Err(error);
        }

        let table_bucket = completed_fetch.table_bucket().clone();
        let fetch_offset = completed_fetch.next_fetch_offset();

        if let Some(api_error) = completed_fetch.api_error() {
            let error = FlussError::for_code(api_error.code);
            let error_message = api_error.message.as_str();
            self.log_scanner_status
                .move_bucket_to_end(table_bucket.clone());
            let action = completed_fetch
                .fetch_error_context()
                .map(|context| context.action)
                .unwrap_or(FetchErrorAction::Unexpected);
            match action {
                FetchErrorAction::Ignore => {
                    return Ok(None);
                }
                FetchErrorAction::LogOffsetOutOfRange => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "The fetching offset {fetch_offset} is out of range: {error_message}"
                        ),
                        source: None,
                    });
                }
                FetchErrorAction::Authorization => {
                    return Err(Error::FlussAPIError {
                        api_error: ApiError {
                            code: api_error.code,
                            message: api_error.message.to_string(),
                        },
                    });
                }
                FetchErrorAction::CorruptMessage => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "Encountered corrupt message when fetching offset {fetch_offset} for bucket {table_bucket}: {error_message}"
                        ),
                        source: None,
                    });
                }
                FetchErrorAction::Unexpected => {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "Unexpected error code {error:?} while fetching at offset {fetch_offset} from bucket {table_bucket}: {error_message}"
                        ),
                        source: None,
                    });
                }
            }
        }

        // Check if bucket is still subscribed
        let Some(current_offset) = self.log_scanner_status.get_bucket_offset(&table_bucket) else {
            warn!(
                "Discarding stale fetch response for bucket {table_bucket:?} since the bucket has been unsubscribed"
            );
            return Ok(None);
        };

        // Check if offset matches
        if fetch_offset != current_offset {
            warn!(
                "Discarding stale fetch response for bucket {table_bucket:?} since its offset {fetch_offset} does not match the expected offset {current_offset}"
            );
            return Ok(None);
        }

        // Update high watermark
        let high_watermark = completed_fetch.high_watermark();
        if high_watermark >= 0 {
            self.log_scanner_status
                .update_high_watermark(&table_bucket, high_watermark);
        }

        completed_fetch.set_initialized();
        Ok(Some(completed_fetch))
    }

    /// Fetch records from a completed fetch, checking offset match
    fn fetch_records_from_fetch(
        &self,
        next_in_line_fetch: &mut Box<dyn CompletedFetch>,
        max_records: usize,
    ) -> Result<Vec<ScanRecord>> {
        let table_bucket = next_in_line_fetch.table_bucket().clone();
        let current_offset = self.log_scanner_status.get_bucket_offset(&table_bucket);

        if current_offset.is_none() {
            warn!(
                "Ignoring fetched records for {table_bucket:?} since the bucket has been unsubscribed"
            );
            next_in_line_fetch.drain();
            return Ok(Vec::new());
        }

        let current_offset = current_offset.unwrap();
        let fetch_offset = next_in_line_fetch.next_fetch_offset();

        // Check if this fetch is next in line
        if fetch_offset == current_offset {
            let records = next_in_line_fetch.fetch_records(max_records)?;
            let next_fetch_offset = next_in_line_fetch.next_fetch_offset();

            if next_fetch_offset > current_offset {
                self.log_scanner_status
                    .update_offset(&table_bucket, next_fetch_offset);
            }

            if next_in_line_fetch.is_consumed() && next_in_line_fetch.records_read() > 0 {
                self.log_scanner_status
                    .move_bucket_to_end(table_bucket.clone());
            }

            Ok(records)
        } else {
            // These records aren't next in line, ignore them
            warn!(
                "Ignoring fetched records for {table_bucket:?} at offset {fetch_offset} since the current offset is {current_offset}"
            );
            next_in_line_fetch.drain();
            Ok(Vec::new())
        }
    }

    /// Collect completed fetches as ScanBatches (with bucket and offset metadata)
    fn collect_batches(&self) -> Result<Vec<ScanBatch>> {
        // Limit memory usage with both batch count and byte size constraints.
        // Max 100 batches per poll, but also check total bytes (soft cap ~64MB).
        const MAX_BATCHES: usize = 100;
        const MAX_BYTES: usize = 64 * 1024 * 1024; // 64MB soft cap
        let mut result: Vec<ScanBatch> = Vec::new();
        let mut batches_remaining = MAX_BATCHES;
        let mut bytes_consumed: usize = 0;

        let collect_result: Result<()> = {
            while batches_remaining > 0 && bytes_consumed < MAX_BYTES {
                let next_in_line = self.log_fetch_buffer.next_in_line_fetch();

                match next_in_line {
                    Some(mut next_fetch) if !next_fetch.is_consumed() => {
                        let scan_batches =
                            self.fetch_batches_from_fetch(&mut next_fetch, batches_remaining)?;
                        let batch_count = scan_batches.len();

                        if !scan_batches.is_empty() {
                            // Track bytes consumed (soft cap - may exceed by one fetch)
                            let batch_bytes: usize = scan_batches
                                .iter()
                                .map(|sb| sb.batch().get_array_memory_size())
                                .sum();
                            bytes_consumed += batch_bytes;

                            result.extend(scan_batches);
                            batches_remaining = batches_remaining.saturating_sub(batch_count);
                        }

                        if !next_fetch.is_consumed() {
                            self.log_fetch_buffer
                                .set_next_in_line_fetch(Some(next_fetch));
                        }
                    }
                    _ => {
                        if let Some(completed_fetch) = self.log_fetch_buffer.poll() {
                            if !completed_fetch.is_initialized() {
                                let size_in_bytes = completed_fetch.size_in_bytes();
                                match self.initialize_fetch(completed_fetch) {
                                    Ok(initialized) => {
                                        self.log_fetch_buffer.set_next_in_line_fetch(initialized);
                                        continue;
                                    }
                                    Err(e) => {
                                        if result.is_empty() && size_in_bytes == 0 {
                                            continue;
                                        }
                                        return Err(e);
                                    }
                                }
                            } else {
                                self.log_fetch_buffer
                                    .set_next_in_line_fetch(Some(completed_fetch));
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
            Ok(())
        };

        match collect_result {
            Ok(()) => Ok(result),
            Err(e) => {
                if result.is_empty() {
                    Err(e)
                } else {
                    Ok(result)
                }
            }
        }
    }

    fn fetch_batches_from_fetch(
        &self,
        next_in_line_fetch: &mut Box<dyn CompletedFetch>,
        max_batches: usize,
    ) -> Result<Vec<ScanBatch>> {
        let table_bucket = next_in_line_fetch.table_bucket().clone();
        let current_offset = self.log_scanner_status.get_bucket_offset(&table_bucket);

        if current_offset.is_none() {
            warn!(
                "Ignoring fetched batches for {table_bucket:?} since the bucket has been unsubscribed"
            );
            next_in_line_fetch.drain();
            return Ok(Vec::new());
        }

        let current_offset = current_offset.unwrap();
        let fetch_offset = next_in_line_fetch.next_fetch_offset();

        if fetch_offset == current_offset {
            let batches_with_offsets = next_in_line_fetch.fetch_batches(max_batches)?;
            let next_fetch_offset = next_in_line_fetch.next_fetch_offset();

            if next_fetch_offset > current_offset {
                self.log_scanner_status
                    .update_offset(&table_bucket, next_fetch_offset);
            }

            // Convert to ScanBatch with bucket info
            Ok(batches_with_offsets
                .into_iter()
                .map(|(batch, base_offset)| {
                    ScanBatch::new(table_bucket.clone(), batch, base_offset)
                })
                .collect())
        } else {
            warn!(
                "Ignoring fetched batches for {table_bucket:?} at offset {fetch_offset} since the current offset is {current_offset}"
            );
            next_in_line_fetch.drain();
            Ok(Vec::new())
        }
    }

    async fn prepare_fetch_log_requests(&self) -> HashMap<i32, FetchLogRequest> {
        let mut fetch_log_req_for_buckets = HashMap::new();
        let mut table_id = None;
        let mut ready_for_fetch_count = 0;
        for bucket in self.fetchable_buckets() {
            if table_id.is_none() {
                table_id = Some(bucket.table_id());
            }

            let offset = match self.log_scanner_status.get_bucket_offset(&bucket) {
                Some(offset) => offset,
                None => {
                    debug!(
                        "Skipping fetch request for bucket {bucket} because the bucket has been unsubscribed."
                    );
                    continue;
                }
            };

            match self.get_table_bucket_leader(&bucket) {
                None => {
                    log::trace!(
                        "Skipping fetch request for bucket {bucket} because leader is not available."
                    )
                }
                Some(leader) => {
                    if self
                        .nodes_with_pending_fetch_requests
                        .lock()
                        .contains(&leader)
                    {
                        log::trace!(
                            "Skipping fetch request for bucket {bucket} because previous request to server {leader} has not been processed."
                        )
                    } else {
                        let fetch_log_req_for_bucket = PbFetchLogReqForBucket {
                            partition_id: bucket.partition_id(),
                            bucket_id: bucket.bucket_id(),
                            fetch_offset: offset,
                            max_fetch_bytes: self.fetch_max_bytes_for_bucket,
                        };

                        fetch_log_req_for_buckets
                            .entry(leader)
                            .or_insert_with(Vec::new)
                            .push(fetch_log_req_for_bucket);
                        ready_for_fetch_count += 1;
                    }
                }
            }
        }

        if ready_for_fetch_count == 0 {
            HashMap::new()
        } else {
            let (projection_enabled, projected_fields) =
                match self.read_context.project_fields_in_order() {
                    None => (false, vec![]),
                    Some(fields) => (true, fields.iter().map(|&i| i as i32).collect()),
                };

            fetch_log_req_for_buckets
                .into_iter()
                .map(|(leader_id, feq_for_buckets)| {
                    let req_for_table = PbFetchLogReqForTable {
                        table_id: table_id.unwrap(),
                        projection_pushdown_enabled: projection_enabled,
                        projected_fields: projected_fields.clone(),
                        buckets_req: feq_for_buckets,
                        filter_predicate: None,
                        filter_schema_id: None,
                    };

                    let fetch_log_request = FetchLogRequest {
                        follower_server_id: -1,
                        max_bytes: self.fetch_max_bytes,
                        tables_req: vec![req_for_table],
                        max_wait_ms: Some(self.fetch_wait_max_time_ms),
                        min_bytes: Some(self.fetch_min_bytes),
                    };
                    (leader_id, fetch_log_request)
                })
                .collect()
        }
    }

    fn fetchable_buckets(&self) -> Vec<TableBucket> {
        // Get buckets that are not already in the buffer
        let buffered = self.log_fetch_buffer.buffered_buckets();
        let buffered_set: HashSet<TableBucket> = buffered.into_iter().collect();
        self.log_scanner_status
            .fetchable_buckets(|tb| !buffered_set.contains(tb))
    }

    fn get_table_bucket_leader(&self, tb: &TableBucket) -> Option<i32> {
        let cluster = self.metadata.get_cluster();
        cluster.leader_for(tb).map(|leader| leader.id())
    }
}

pub struct LogScannerStatus {
    bucket_status_map: Arc<RwLock<FairBucketStatusMap<BucketScanStatus>>>,
}

#[allow(dead_code)]
impl LogScannerStatus {
    pub fn new() -> Self {
        Self {
            bucket_status_map: Arc::new(RwLock::new(FairBucketStatusMap::new())),
        }
    }

    pub fn prepare_to_poll(&self) -> bool {
        let map = self.bucket_status_map.read();
        map.size() > 0
    }

    pub fn move_bucket_to_end(&self, table_bucket: TableBucket) {
        let mut map = self.bucket_status_map.write();
        map.move_to_end(table_bucket);
    }

    /// Gets the offset of a bucket if it exists
    pub fn get_bucket_offset(&self, table_bucket: &TableBucket) -> Option<i64> {
        let map = self.bucket_status_map.read();
        map.status_value(table_bucket).map(|status| status.offset())
    }

    pub fn update_high_watermark(&self, table_bucket: &TableBucket, high_watermark: i64) {
        if let Some(status) = self.get_status(table_bucket) {
            status.set_high_watermark(high_watermark);
        }
    }

    pub fn update_offset(&self, table_bucket: &TableBucket, offset: i64) {
        if let Some(status) = self.get_status(table_bucket) {
            status.set_offset(offset);
        }
    }

    pub fn assign_scan_buckets(&self, scan_bucket_offsets: HashMap<TableBucket, i64>) {
        let mut map = self.bucket_status_map.write();
        for (bucket, offset) in scan_bucket_offsets {
            let status = map
                .status_value(&bucket)
                .cloned()
                .unwrap_or_else(|| Arc::new(BucketScanStatus::new(offset)));
            status.set_offset(offset);
            map.update(bucket, status);
        }
    }

    pub fn assign_scan_bucket(&self, table_bucket: TableBucket, offset: i64) {
        let status = Arc::new(BucketScanStatus::new(offset));
        self.bucket_status_map.write().update(table_bucket, status);
    }

    /// Unassigns scan buckets
    pub fn unassign_scan_buckets(&self, buckets: &[TableBucket]) {
        let mut map = self.bucket_status_map.write();
        for bucket in buckets {
            map.remove(bucket);
        }
    }

    /// Gets fetchable buckets based on availability predicate
    pub fn fetchable_buckets<F>(&self, is_available: F) -> Vec<TableBucket>
    where
        F: Fn(&TableBucket) -> bool,
    {
        let map = self.bucket_status_map.read();
        let mut result = Vec::new();
        map.for_each(|bucket, _| {
            if is_available(bucket) {
                result.push(bucket.clone());
            }
        });
        result
    }

    /// Returns all subscribed buckets with their current offsets
    pub fn get_all_subscriptions(&self) -> Vec<(TableBucket, i64)> {
        let map = self.bucket_status_map.read();
        let mut result = Vec::new();
        map.for_each(|bucket, status| {
            result.push((bucket.clone(), status.offset()));
        });
        result
    }

    /// Helper to get bucket status
    fn get_status(&self, table_bucket: &TableBucket) -> Option<Arc<BucketScanStatus>> {
        let map = self.bucket_status_map.read();
        map.status_value(table_bucket).cloned()
    }
}

impl Default for LogScannerStatus {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct BucketScanStatus {
    offset: RwLock<i64>,
    high_watermark: RwLock<i64>,
}

#[allow(dead_code)]
impl BucketScanStatus {
    pub fn new(offset: i64) -> Self {
        Self {
            offset: RwLock::new(offset),
            high_watermark: RwLock::new(0),
        }
    }

    pub fn offset(&self) -> i64 {
        *self.offset.read()
    }

    pub fn set_offset(&self, offset: i64) {
        *self.offset.write() = offset
    }

    pub fn high_watermark(&self) -> i64 {
        *self.high_watermark.read()
    }

    pub fn set_high_watermark(&self, high_watermark: i64) {
        *self.high_watermark.write() = high_watermark
    }
}

fn validate_scan_support(table_path: &TablePath, table_info: &TableInfo) -> Result<()> {
    if table_info.schema.primary_key().is_some() {
        return Err(UnsupportedOperation {
            message: format!("Table {table_path} is not a Log Table and doesn't support scan."),
        });
    }

    let log_format = table_info.table_config.get_log_format()?;
    if LogFormat::ARROW != log_format {
        return Err(UnsupportedOperation {
            message: format!(
                "Scan is only supported for ARROW format and table {table_path} uses {log_format} format"
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::WriteRecord;
    use crate::client::metadata::Metadata;
    use crate::compression::{
        ArrowCompressionInfo, ArrowCompressionRatioEstimator, ArrowCompressionType,
        DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
    };
    use crate::metadata::{DataTypes, PhysicalTablePath, Schema, TableInfo, TablePath};
    use crate::proto::{PbFetchLogRespForBucket, PbFetchLogRespForTable};
    use crate::record::MemoryLogRecordsArrowBuilder;
    use crate::row::{Datum, GenericRow};
    use crate::rpc::FlussError;
    use crate::test_utils::{
        assert_scanner_entries_labeled, build_cluster_arc, build_table_info, test_scanner_metrics,
    };

    fn build_records(table_info: &TableInfo, table_path: Arc<TablePath>) -> Result<Vec<u8>> {
        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            table_info.get_row_type(),
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
            usize::MAX,
            Arc::new(ArrowCompressionRatioEstimator::default()),
        )?;
        let physical_table_path = Arc::new(PhysicalTablePath::of(table_path));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };
        let record =
            WriteRecord::for_append(Arc::new(table_info.clone()), physical_table_path, 1, &row);
        builder.append(&record)?;
        builder.build()
    }

    #[tokio::test]
    async fn collect_fetches_updates_offset() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = build_table_info(table_path.clone(), 1, 1);
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let status = Arc::new(LogScannerStatus::new());
        let fetcher = LogFetcher::new(
            table_info.clone(),
            Arc::new(RpcClient::new()),
            metadata,
            status.clone(),
            &Config::default(),
            None,
            test_scanner_metrics(&table_path),
        )?;

        let bucket = TableBucket::new(1, 0);
        status.assign_scan_bucket(bucket.clone(), 0);

        let data = build_records(&table_info, Arc::new(table_path))?;
        let log_records = LogRecordsBatches::new(data.clone());
        let row_type = Arc::new(table_info.get_row_type().clone());
        let read_context = ReadContext::new(to_arrow_schema(&row_type)?, row_type, false);
        let completed =
            DefaultCompletedFetch::new(bucket.clone(), log_records, data.len(), read_context, 0, 0);
        fetcher.log_fetch_buffer.add(Box::new(completed));

        let fetched = fetcher.collect_fetches()?;
        assert_eq!(fetched.get(&bucket).unwrap().len(), 1);
        assert_eq!(status.get_bucket_offset(&bucket), Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn fetch_records_from_fetch_drains_unassigned_bucket() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = build_table_info(table_path.clone(), 1, 1);
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let status = Arc::new(LogScannerStatus::new());
        let fetcher = LogFetcher::new(
            table_info.clone(),
            Arc::new(RpcClient::new()),
            metadata,
            status,
            &Config::default(),
            None,
            test_scanner_metrics(&table_path),
        )?;

        let bucket = TableBucket::new(1, 0);
        let data = build_records(&table_info, Arc::new(table_path))?;
        let log_records = LogRecordsBatches::new(data.clone());
        let row_type = Arc::new(table_info.get_row_type().clone());
        let read_context = ReadContext::new(to_arrow_schema(&row_type)?, row_type, false);
        let mut completed: Box<dyn CompletedFetch> = Box::new(DefaultCompletedFetch::new(
            bucket,
            log_records,
            data.len(),
            read_context,
            0,
            0,
        ));

        let records = fetcher.fetch_records_from_fetch(&mut completed, 10)?;
        assert!(records.is_empty());
        assert!(completed.is_consumed());
        Ok(())
    }

    #[tokio::test]
    async fn prepare_fetch_log_requests_skips_pending() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = build_table_info(table_path.clone(), 1, 1);
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let status = Arc::new(LogScannerStatus::new());
        status.assign_scan_bucket(TableBucket::new(1, 0), 0);
        let fetcher = LogFetcher::new(
            table_info,
            Arc::new(RpcClient::new()),
            metadata,
            status,
            &Config::default(),
            None,
            test_scanner_metrics(&table_path),
        )?;

        fetcher.nodes_with_pending_fetch_requests.lock().insert(1);

        let requests = fetcher.prepare_fetch_log_requests().await;
        assert!(requests.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn handle_fetch_response_sets_error() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = build_table_info(table_path.clone(), 1, 1);
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let status = Arc::new(LogScannerStatus::new());
        status.assign_scan_bucket(TableBucket::new(1, 0), 5);
        let fetcher = LogFetcher::new(
            table_info.clone(),
            Arc::new(RpcClient::new()),
            metadata.clone(),
            status.clone(),
            &Config::default(),
            None,
            test_scanner_metrics(&table_path),
        )?;

        let response = FetchLogResponse {
            tables_resp: vec![PbFetchLogRespForTable {
                table_id: 1,
                buckets_resp: vec![PbFetchLogRespForBucket {
                    partition_id: None,
                    bucket_id: 0,
                    error_code: Some(FlussError::AuthorizationException.code()),
                    error_message: Some("denied".to_string()),
                    high_watermark: None,
                    log_start_offset: None,
                    remote_log_fetch_info: None,
                    records: None,
                    filtered_end_offset: None,
                }],
            }],
        };

        let response_context = FetchResponseContext {
            metadata: metadata.clone(),
            log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
            log_scanner_status: fetcher.log_scanner_status.clone(),
            read_context: fetcher.read_context.clone(),
            remote_read_context: fetcher.remote_read_context.clone(),
            remote_log_downloader: fetcher.remote_log_downloader.clone(),
            metrics: Arc::clone(&fetcher.metrics),
            request_start_time: Instant::now(),
        };

        LogFetcher::handle_fetch_response(response, response_context).await;

        let completed = fetcher.log_fetch_buffer.poll().expect("completed fetch");
        let api_error = completed.api_error().expect("api error");
        assert_eq!(api_error.code, FlussError::AuthorizationException.code());
        Ok(())
    }

    #[tokio::test]
    async fn handle_fetch_response_invalidates_table_meta() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = build_table_info(table_path.clone(), 1, 1);
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let status = Arc::new(LogScannerStatus::new());
        status.assign_scan_bucket(TableBucket::new(1, 0), 5);
        let fetcher = LogFetcher::new(
            table_info.clone(),
            Arc::new(RpcClient::new()),
            metadata.clone(),
            status.clone(),
            &Config::default(),
            None,
            test_scanner_metrics(&table_path),
        )?;

        let bucket = TableBucket::new(1, 0);
        assert!(metadata.leader_for(&table_path, &bucket).await?.is_some());

        let response = FetchLogResponse {
            tables_resp: vec![PbFetchLogRespForTable {
                table_id: 1,
                buckets_resp: vec![PbFetchLogRespForBucket {
                    partition_id: None,
                    bucket_id: 0,
                    error_code: Some(FlussError::NotLeaderOrFollower.code()),
                    error_message: Some("not leader".to_string()),
                    high_watermark: None,
                    log_start_offset: None,
                    remote_log_fetch_info: None,
                    records: None,
                    filtered_end_offset: None,
                }],
            }],
        };

        let response_context = FetchResponseContext {
            metadata: metadata.clone(),
            log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
            log_scanner_status: fetcher.log_scanner_status.clone(),
            read_context: fetcher.read_context.clone(),
            remote_read_context: fetcher.remote_read_context.clone(),
            remote_log_downloader: fetcher.remote_log_downloader.clone(),
            metrics: Arc::clone(&fetcher.metrics),
            request_start_time: Instant::now(),
        };

        LogFetcher::handle_fetch_response(response, response_context).await;

        assert!(metadata.get_cluster().leader_for(&bucket).is_none());
        Ok(())
    }

    fn create_test_table_info(
        has_primary_key: bool,
        log_format: Option<&str>,
    ) -> (TableInfo, TablePath) {
        let mut schema_builder = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string());

        if has_primary_key {
            schema_builder = schema_builder.primary_key(vec!["id"]);
        }

        let schema = schema_builder.build().unwrap();
        let table_path = TablePath::new("test_db", "test_table");

        let mut properties = HashMap::new();
        if let Some(format) = log_format {
            properties.insert("table.log.format".to_string(), format.to_string());
        }

        let table_info = TableInfo::new(
            table_path.clone(),
            1,
            1,
            schema,
            vec![],
            Arc::from(vec![]),
            1,
            properties,
            HashMap::new(),
            None,
            0,
            0,
        );

        (table_info, table_path)
    }

    #[test]
    fn test_validate_scan_support() {
        // Primary key table
        let (table_info, table_path) = create_test_table_info(true, Some("ARROW"));
        let result = validate_scan_support(&table_path, &table_info);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, UnsupportedOperation { .. }));
        assert!(err.to_string().contains(
            format!("Table {table_path} is not a Log Table and doesn't support scan.").as_str()
        ));

        // Indexed format
        let (table_info, table_path) = create_test_table_info(false, Some("INDEXED"));
        let result = validate_scan_support(&table_path, &table_info);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, UnsupportedOperation { .. }));
        assert!(err.to_string().contains(format!("Scan is only supported for ARROW format and table {table_path} uses INDEXED format").as_str()));

        // Default format
        let (table_info, table_path) = create_test_table_info(false, None);
        let result = validate_scan_support(&table_path, &table_info);
        assert!(result.is_ok());

        // Arrow format
        let (table_info, table_path) = create_test_table_info(false, Some("ARROW"));
        let result = validate_scan_support(&table_path, &table_info);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn prepare_fetch_log_requests_uses_configured_fetch_params() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = build_table_info(table_path.clone(), 1, 1);
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster));
        let status = Arc::new(LogScannerStatus::new());
        status.assign_scan_bucket(TableBucket::new(1, 0), 0);

        let config = Config {
            scanner_log_fetch_max_bytes: 1234,
            scanner_log_fetch_min_bytes: 7,
            scanner_log_fetch_wait_max_time_ms: 89,
            scanner_log_fetch_max_bytes_for_bucket: 512,
            ..Config::default()
        };

        let fetcher = LogFetcher::new(
            table_info,
            Arc::new(RpcClient::new()),
            metadata,
            status,
            &config,
            None,
            test_scanner_metrics(&table_path),
        )?;

        let requests = fetcher.prepare_fetch_log_requests().await;
        // In this test cluster, leader id should exist; but even if it changes,
        // assert over all built requests.
        assert!(!requests.is_empty());
        for req in requests.values() {
            assert_eq!(req.max_bytes, 1234);
            assert_eq!(req.min_bytes, Some(7));
            assert_eq!(req.max_wait_ms, Some(89));

            for table_req in &req.tables_req {
                for bucket_req in &table_req.buckets_req {
                    assert_eq!(bucket_req.max_fetch_bytes, 512);
                }
            }
        }
        Ok(())
    }

    /// Builds a self-contained `LogScannerInner` for poll-timing tests
    /// inside a `current_thread` runtime so callers can drive `PollGuard`
    /// lifecycles synchronously.
    fn with_test_log_scanner_inner<F: FnOnce(&LogScannerInner)>(body: F) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current_thread runtime");
        rt.block_on(async {
            let table_path = TablePath::new("db".to_string(), "tbl".to_string());
            let table_info = build_table_info(table_path.clone(), 1, 1);
            let cluster = build_cluster_arc(&table_path, 1, 1);
            let metadata = Arc::new(Metadata::new_for_test(cluster));
            let inner = LogScannerInner::new(
                &table_info,
                metadata,
                Arc::new(RpcClient::new()),
                &Config::default(),
                None,
            )
            .expect("build LogScannerInner");
            body(&inner);
        });
    }

    fn snapshot_gauge(
        snapshotter: &metrics_util::debugging::Snapshotter,
        name: &str,
    ) -> Option<f64> {
        use metrics_util::debugging::DebugValue;
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(key, _, _, val)| {
                if key.key().name() == name {
                    if let DebugValue::Gauge(g) = val {
                        return Some(g.into_inner());
                    }
                }
                None
            })
    }

    /// Exercises the `PollGuard` lifecycle across two consecutive
    /// `record_poll_start` calls. Asserts both poll-timing gauges are
    /// emitted at the right moments and `record_poll_end` runs on guard
    /// drop (also the cancellation-safety path, since dropping the
    /// `poll()` future drops the guard).
    #[test]
    fn poll_guard_emits_time_between_poll_and_idle_ratio() {
        use crate::metrics::{SCANNER_POLL_IDLE_RATIO, SCANNER_TIME_BETWEEN_POLL_MS};
        use metrics_util::debugging::DebuggingRecorder;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            with_test_log_scanner_inner(|inner| {
                // First poll: emits time_between_poll_ms=0 (Java parity:
                // ScannerMetricGroup.recordPollStart emits 0 when there is
                // no previous poll). Idle ratio is also emitted as 1.0
                // on drop (poll_time / (poll_time + 0) = 1.0).
                {
                    let _g = PollGuard::new(inner);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }

                // Brief gap so time_between_poll_ms is observably > 0.
                std::thread::sleep(std::time::Duration::from_millis(5));

                // Second poll: refreshes both time_between_poll_ms (>0)
                // and a fresh idle ratio.
                {
                    let _g = PollGuard::new(inner);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            });
        });

        let between = snapshot_gauge(&snapshotter, SCANNER_TIME_BETWEEN_POLL_MS)
            .expect("time_between_poll_ms must be emitted on every poll");
        assert!(
            between > 0.0,
            "second-poll time_between_poll_ms must be positive, got {between}"
        );

        let ratio = snapshot_gauge(&snapshotter, SCANNER_POLL_IDLE_RATIO)
            .expect("poll_idle_ratio must be emitted on poll end");
        assert!(
            (0.0..=1.0).contains(&ratio),
            "poll_idle_ratio must be in [0, 1], got {ratio}"
        );

        // Both gauges must carry `database=db` / `table=tbl` (the fixture
        // values from `with_test_log_scanner_inner`).
        assert_scanner_entries_labeled(&snapshotter.snapshot().into_vec(), "db", "tbl");
    }

    /// Java parity: `ScannerMetricGroup.recordPollStart` emits
    /// `timeMsBetweenPoll = 0` on the very first poll. The Rust gauge
    /// must do the same so dashboards see the metric series from poll #1.
    #[test]
    fn time_between_poll_ms_emits_zero_on_first_poll() {
        use crate::metrics::SCANNER_TIME_BETWEEN_POLL_MS;
        use metrics_util::debugging::DebuggingRecorder;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            with_test_log_scanner_inner(|inner| {
                let _g = PollGuard::new(inner);
                // Drop at end of scope completes the poll; the value of
                // SCANNER_TIME_BETWEEN_POLL_MS was emitted at start, not end.
            });
        });

        let between = snapshot_gauge(&snapshotter, SCANNER_TIME_BETWEEN_POLL_MS)
            .expect("time_between_poll_ms must be emitted on the first poll");
        assert_eq!(
            between, 0.0,
            "first-poll time_between_poll_ms must be 0.0 (Java parity), got {between}"
        );
        assert_scanner_entries_labeled(&snapshotter.snapshot().into_vec(), "db", "tbl");
    }

    /// Pins the single-consumer contract: overlapping `PollGuard`s on the
    /// same scanner trip the `debug_assert!` in `record_poll_start`.
    /// Release builds skip the check, so the test is gated on
    /// `debug_assertions`.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "concurrent poll() detected")]
    fn overlapping_polls_panic_in_debug_builds() {
        with_test_log_scanner_inner(|inner| {
            let _g1 = PollGuard::new(inner);
            // _g1 has not been dropped → poll_start_at is still Some,
            // so the second start must panic.
            let _g2 = PollGuard::new(inner);
        });
    }

    /// Drives `handle_fetch_response` against a local metrics recorder and
    /// asserts that latency + bytes-per-request histograms are emitted with
    /// values that mirror what Java would record. This complements the unit
    /// tests in `metrics.rs` (which only verify the facade) by exercising
    /// the actual instrumented call path.
    ///
    /// Note: uses a `current_thread` runtime inside `with_local_recorder`
    /// (rather than `#[tokio::test]`) because the metrics facade installs a
    /// thread-local recorder; running the async work on the same thread is
    /// the only way to observe the emitted metrics in the snapshot. Both
    /// the fetcher construction and the `handle_fetch_response` call run
    /// inside the runtime (the security-token manager and remote-log
    /// downloader require a Tokio reactor).
    #[test]
    fn handle_fetch_response_emits_latency_and_bytes_metrics() {
        use crate::metrics::{SCANNER_BYTES_PER_REQUEST, SCANNER_FETCH_LATENCY_MS};
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        let expected_bytes = metrics::with_local_recorder(&recorder, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current_thread runtime");

            rt.block_on(async {
                let table_path = TablePath::new("db".to_string(), "tbl".to_string());
                let table_info = build_table_info(table_path.clone(), 1, 1);
                let cluster = build_cluster_arc(&table_path, 1, 1);
                let metadata = Arc::new(Metadata::new_for_test(cluster));
                let status = Arc::new(LogScannerStatus::new());
                status.assign_scan_bucket(TableBucket::new(1, 0), 5);
                let fetcher = LogFetcher::new(
                    table_info,
                    Arc::new(RpcClient::new()),
                    metadata.clone(),
                    status,
                    &Config::default(),
                    None,
                    test_scanner_metrics(&table_path),
                )
                .expect("build LogFetcher");

                let response = FetchLogResponse {
                    tables_resp: vec![PbFetchLogRespForTable {
                        table_id: 1,
                        buckets_resp: vec![PbFetchLogRespForBucket {
                            partition_id: None,
                            bucket_id: 0,
                            error_code: Some(FlussError::None.code()),
                            error_message: None,
                            high_watermark: Some(7),
                            log_start_offset: Some(0),
                            remote_log_fetch_info: None,
                            records: None,
                            filtered_end_offset: None,
                        }],
                    }],
                };
                let expected_bytes = response.encoded_len() as f64;
                let response_context = FetchResponseContext {
                    metadata: metadata.clone(),
                    log_fetch_buffer: fetcher.log_fetch_buffer.clone(),
                    log_scanner_status: fetcher.log_scanner_status.clone(),
                    read_context: fetcher.read_context.clone(),
                    remote_read_context: fetcher.remote_read_context.clone(),
                    remote_log_downloader: fetcher.remote_log_downloader.clone(),
                    metrics: Arc::clone(&fetcher.metrics),
                    request_start_time: Instant::now(),
                };

                LogFetcher::handle_fetch_response(response, response_context).await;
                expected_bytes
            })
        });

        let entries: Vec<_> = snapshotter.snapshot().into_vec();
        let find_histogram = |name: &str| -> Vec<f64> {
            entries
                .iter()
                .find_map(|(key, _, _, val)| {
                    if key.key().name() == name {
                        if let DebugValue::Histogram(v) = val {
                            return Some(v.iter().map(|f| f.into_inner()).collect());
                        }
                    }
                    None
                })
                .unwrap_or_default()
        };

        let latency_samples = find_histogram(SCANNER_FETCH_LATENCY_MS);
        assert_eq!(latency_samples.len(), 1, "expected one latency sample");
        assert!(
            latency_samples[0] >= 0.0,
            "latency must be non-negative, got {}",
            latency_samples[0]
        );

        let bytes_samples = find_histogram(SCANNER_BYTES_PER_REQUEST);
        assert_eq!(
            bytes_samples,
            vec![expected_bytes],
            "bytes histogram must record encoded_len() for parity with Java fetchLogResponse.totalSize()",
        );

        // Every emitted scanner metric must carry both `database` and `table`
        // labels — that's the whole point of `ScannerMetrics`. If a future
        // contributor adds a new `metrics::*!` macro inline (bypassing
        // `ScannerMetrics`), this assertion catches it.
        assert_scanner_entries_labeled(&entries, "db", "tbl");
    }
}
