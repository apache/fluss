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

use arrow::array::RecordBatch;
use parking_lot::Mutex;

use crate::client::table::read_context_resolver::ReadContextResolver;
use crate::client::table::remote_log::{
    PrefetchPermit, RemoteLogDownloadFuture, RemoteLogFile, RemoteLogSegment,
};
use crate::error::{ApiError, Error, Result};
use crate::metadata::TableBucket;
use crate::record::{
    ChangeType, LogRecordBatch, LogRecordIterator, LogRecordsBatches, ReadContext, ScanRecord,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FetchErrorAction {
    Ignore,
    LogOffsetOutOfRange,
    Authorization,
    CorruptMessage,
    Unexpected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FetchErrorLogLevel {
    Debug,
    Warn,
}

#[derive(Clone, Debug)]
pub(crate) struct FetchErrorContext {
    pub(crate) action: FetchErrorAction,
    pub(crate) log_level: FetchErrorLogLevel,
    pub(crate) log_message: String,
}

/// Represents a completed fetch that can be consumed
pub trait CompletedFetch: Send + Sync {
    fn table_bucket(&self) -> &TableBucket;
    fn api_error(&self) -> Option<&ApiError>;
    fn fetch_error_context(&self) -> Option<&FetchErrorContext>;
    fn take_error(&mut self) -> Option<Error>;
    fn fetch_records(&mut self, max_records: usize) -> Result<FetchResult<Vec<ScanRecord>>>;
    fn fetch_batches(&mut self, max_batches: usize)
    -> Result<FetchResult<Vec<(RecordBatch, i64)>>>;
    fn is_consumed(&self) -> bool;
    fn records_read(&self) -> usize;
    fn drain(&mut self);
    fn size_in_bytes(&self) -> usize;
    fn high_watermark(&self) -> i64;
    fn is_initialized(&self) -> bool;
    fn set_initialized(&mut self);
    fn next_fetch_offset(&self) -> i64;
}

/// Result of synchronously decoding data already available in a completed
/// fetch. Missing schemas are surfaced to the async scanner layer instead of
/// blocking the current Tokio worker from inside this synchronous trait.
pub(crate) enum FetchResult<T> {
    /// Decoded data which is ready to return. The value may be empty when the
    /// completed fetch has reached its end.
    Data(T),
    /// Decoding is paused on the current raw batch. The async scanner must
    /// fetch/register this schema and then retry the same completed fetch.
    SchemaRequired(i16),
}
enum FetchStep<T> {
    /// The current raw batch cannot be decoded until this schema ID is loaded.
    SchemaRequired(i16),

    /// One record or Arrow batch was produced, and this completed fetch may
    /// still contain more data to read.
    InProgress(T),
    /// The completed fetch has no more raw batches or records.
    End,
}

/// Represents a pending fetch that is waiting to be completed
pub trait PendingFetch: Send + Sync {
    fn table_bucket(&self) -> &TableBucket;
    fn is_completed(&self) -> bool;
    fn to_completed_fetch(self: Box<Self>) -> Result<Box<dyn CompletedFetch>>;
}

/// Thread-safe buffer for completed fetches
pub struct LogFetchBuffer {
    resolver: Arc<ReadContextResolver>,
    completed_fetches: Mutex<VecDeque<Box<dyn CompletedFetch>>>,
    pending_fetches: Mutex<HashMap<TableBucket, VecDeque<Box<dyn PendingFetch>>>>,
    next_in_line_fetch: Mutex<Option<Box<dyn CompletedFetch>>>,
    not_empty_notify: Notify,
    woken_up: Arc<AtomicBool>,
}

impl LogFetchBuffer {
    pub fn new(resolver: Arc<ReadContextResolver>) -> Self {
        Self {
            resolver,
            completed_fetches: Mutex::new(VecDeque::new()),
            pending_fetches: Mutex::new(HashMap::new()),
            next_in_line_fetch: Mutex::new(None),
            not_empty_notify: Notify::new(),
            woken_up: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.completed_fetches.lock().is_empty()
    }

    /// Wait for the buffer to become non-empty, with timeout.
    /// Returns true if data became available, false if timeout.
    pub async fn await_not_empty(&self, timeout: Duration) -> Result<bool> {
        let deadline = Instant::now() + timeout;

        // Arm before checking: notify_waiters() stores no permit and only wakes already-armed
        // waiters, so arming late would drop a notification that races the emptiness check.
        let notified = self.not_empty_notify.notified();
        tokio::pin!(notified);

        loop {
            notified.as_mut().enable();

            if !self.is_empty() {
                return Ok(true);
            }

            if self.woken_up.swap(false, Ordering::Acquire) {
                return Err(Error::WakeupError {
                    message: "The await operation was interrupted by wakeup.".to_string(),
                });
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }

            let remaining = deadline - now;
            tokio::select! {
                _ = tokio::time::sleep(remaining) => return Ok(false),
                _ = notified.as_mut() => {
                    notified.set(self.not_empty_notify.notified()); // re-arm
                }
            }
        }
    }

    #[allow(dead_code)]
    /// Wake up any waiting threads
    pub fn wakeup(&self) {
        self.woken_up.store(true, Ordering::Release);
        self.not_empty_notify.notify_waiters();
    }

    pub(crate) fn add_api_error(
        &self,
        table_bucket: TableBucket,
        api_error: ApiError,
        fetch_error_context: FetchErrorContext,
        fetch_offset: i64,
    ) {
        let error_fetch = DefaultCompletedFetch::from_api_error(
            table_bucket,
            api_error,
            fetch_error_context,
            fetch_offset,
            Arc::clone(&self.resolver),
        );
        self.completed_fetches
            .lock()
            .push_back(Box::new(error_fetch));
        self.not_empty_notify.notify_waiters();
    }

    /// Add a pending fetch to the buffer
    pub fn pend(&self, pending_fetch: Box<dyn PendingFetch>) {
        let table_bucket = pending_fetch.table_bucket().clone();
        self.pending_fetches
            .lock()
            .entry(table_bucket)
            .or_default()
            .push_back(pending_fetch);
    }

    /// Try to complete pending fetches in order, converting them to completed fetches
    pub fn try_complete(&self, table_bucket: &TableBucket) {
        // Collect completed fetches while holding the pending_fetches lock,
        // then push them to completed_fetches after releasing it to avoid
        // holding both locks simultaneously.
        let mut completed_to_push: Vec<Box<dyn CompletedFetch>> = Vec::new();
        let mut has_completed = false;
        let mut pending_error: Option<Error> = None;
        {
            let mut pending_map = self.pending_fetches.lock();
            if let Some(pendings) = pending_map.get_mut(table_bucket) {
                while let Some(front) = pendings.front() {
                    if front.is_completed() {
                        let pending = pendings.pop_front().unwrap();
                        match pending.to_completed_fetch() {
                            Ok(completed) => {
                                completed_to_push.push(completed);
                                has_completed = true;
                            }
                            Err(e) => {
                                pending_error = Some(e);
                                has_completed = true;
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }
                if has_completed && pendings.is_empty() {
                    pending_map.remove(table_bucket);
                }
            }
        }

        if let Some(error) = pending_error {
            let error_fetch = DefaultCompletedFetch::from_error(
                table_bucket.clone(),
                error,
                -1,
                Arc::clone(&self.resolver),
            );
            completed_to_push.push(Box::new(error_fetch));
        }

        if !completed_to_push.is_empty() {
            let mut completed_queue = self.completed_fetches.lock();
            for completed in completed_to_push {
                completed_queue.push_back(completed);
            }
            has_completed = true;
        }

        if has_completed {
            // Signal that buffer is not empty
            self.not_empty_notify.notify_waiters();
        }
    }

    /// Add a completed fetch to the buffer
    pub fn add(&self, completed_fetch: Box<dyn CompletedFetch>) {
        let table_bucket = completed_fetch.table_bucket();
        let mut pending_map = self.pending_fetches.lock();

        if let Some(pendings) = pending_map.get_mut(table_bucket)
            && !pendings.is_empty()
        {
            pendings.push_back(Box::new(CompletedPendingFetch::new(completed_fetch)));
            return;
        }
        // If there's no pending fetch for this table_bucket,
        // directly add to completed_fetches
        self.completed_fetches.lock().push_back(completed_fetch);
        self.not_empty_notify.notify_waiters();
    }

    /// Poll the next completed fetch
    pub fn poll(&self) -> Option<Box<dyn CompletedFetch>> {
        self.completed_fetches.lock().pop_front()
    }

    /// Get the next in line fetch
    pub fn next_in_line_fetch(&self) -> Option<Box<dyn CompletedFetch>> {
        self.next_in_line_fetch.lock().take()
    }

    /// Set the next in line fetch
    pub fn set_next_in_line_fetch(&self, fetch: Option<Box<dyn CompletedFetch>>) {
        *self.next_in_line_fetch.lock() = fetch;
    }

    /// Get the set of buckets that have buffered data
    pub fn buffered_buckets(&self) -> Vec<TableBucket> {
        let mut buckets = Vec::new();

        // Avoid holding multiple locks at once to prevent lock-order inversion.
        {
            let next_in_line_fetch = self.next_in_line_fetch.lock();
            if let Some(complete_fetch) = next_in_line_fetch.as_ref() {
                if !complete_fetch.is_consumed() {
                    buckets.push(complete_fetch.table_bucket().clone());
                }
            }
        }

        {
            let completed = self.completed_fetches.lock();
            for fetch in completed.iter() {
                buckets.push(fetch.table_bucket().clone());
            }
        }

        {
            let pending = self.pending_fetches.lock();
            buckets.extend(pending.keys().cloned());
        }
        buckets
    }
}

/// A wrapper that makes a completed fetch look like a pending fetch
struct CompletedPendingFetch {
    completed_fetch: Box<dyn CompletedFetch>,
}

impl CompletedPendingFetch {
    fn new(completed_fetch: Box<dyn CompletedFetch>) -> Self {
        Self { completed_fetch }
    }
}

impl PendingFetch for CompletedPendingFetch {
    fn table_bucket(&self) -> &TableBucket {
        self.completed_fetch.table_bucket()
    }

    fn is_completed(&self) -> bool {
        true
    }

    fn to_completed_fetch(self: Box<Self>) -> Result<Box<dyn CompletedFetch>> {
        Ok(self.completed_fetch)
    }
}

/// Default implementation of CompletedFetch for in-memory log records
/// Used for local fetches from tablet server
pub struct DefaultCompletedFetch {
    table_bucket: TableBucket,
    api_error: Option<ApiError>,
    fetch_error_context: Option<FetchErrorContext>,
    error: Option<Error>,
    log_record_batch: LogRecordsBatches,
    resolver: Arc<ReadContextResolver>,
    is_remote: bool,
    next_fetch_offset: i64,
    high_watermark: i64,
    size_in_bytes: usize,
    consumed: bool,
    initialized: bool,
    records_read: usize,
    current_record_iterator: Option<LogRecordIterator>,
    current_record_batch: Option<LogRecordBatch>,
    /// A raw batch which has been read from memory/the remote file but cannot
    /// yet be decoded because its schema is not cached. Keeping it here makes
    /// schema fetching cancellation-safe and avoids advancing through the
    /// rest of a remote segment.
    pending_record_batch: Option<LogRecordBatch>,
    /// Records decoded before discovering that the matching `+U` needs a
    /// schema which is not cached yet. They remain uncommitted until the pair
    /// can be returned together.
    pending_records: Vec<ScanRecord>,
    last_record: Option<ScanRecord>,
    cached_record_error: Option<String>,
    corrupt_last_record: bool,
}

impl DefaultCompletedFetch {
    pub fn new(
        table_bucket: TableBucket,
        log_record_batch: LogRecordsBatches,
        size_in_bytes: usize,
        resolver: Arc<ReadContextResolver>,
        is_remote: bool,
        fetch_offset: i64,
        high_watermark: i64,
    ) -> Self {
        Self {
            table_bucket,
            api_error: None,
            fetch_error_context: None,
            error: None,
            log_record_batch,
            resolver,
            is_remote,
            next_fetch_offset: fetch_offset,
            high_watermark,
            size_in_bytes,
            consumed: false,
            initialized: false,
            records_read: 0,
            current_record_iterator: None,
            current_record_batch: None,
            pending_record_batch: None,
            pending_records: Vec::new(),
            last_record: None,
            cached_record_error: None,
            corrupt_last_record: false,
        }
    }

    pub(crate) fn from_error(
        table_bucket: TableBucket,
        error: Error,
        fetch_offset: i64,
        resolver: Arc<ReadContextResolver>,
    ) -> Self {
        Self {
            table_bucket,
            api_error: None,
            fetch_error_context: None,
            error: Some(error),
            log_record_batch: LogRecordsBatches::new(Vec::new()),
            resolver,
            is_remote: false,
            next_fetch_offset: fetch_offset,
            high_watermark: -1,
            size_in_bytes: 0,
            consumed: false,
            initialized: false,
            records_read: 0,
            current_record_iterator: None,
            current_record_batch: None,
            pending_record_batch: None,
            pending_records: Vec::new(),
            last_record: None,
            cached_record_error: None,
            corrupt_last_record: false,
        }
    }

    pub(crate) fn from_api_error(
        table_bucket: TableBucket,
        api_error: ApiError,
        fetch_error_context: FetchErrorContext,
        fetch_offset: i64,
        resolver: Arc<ReadContextResolver>,
    ) -> Self {
        Self {
            table_bucket,
            api_error: Some(api_error),
            fetch_error_context: Some(fetch_error_context),
            error: None,
            log_record_batch: LogRecordsBatches::new(Vec::new()),
            resolver,
            is_remote: false,
            next_fetch_offset: fetch_offset,
            high_watermark: -1,
            size_in_bytes: 0,
            consumed: false,
            initialized: false,
            records_read: 0,
            current_record_iterator: None,
            current_record_batch: None,
            pending_record_batch: None,
            pending_records: Vec::new(),
            last_record: None,
            cached_record_error: None,
            corrupt_last_record: false,
        }
    }

    /// Get the next fetched record, handling batch iteration and record skipping
    fn next_fetched_record(&mut self) -> Result<FetchStep<ScanRecord>> {
        loop {
            if let Some(record) = self
                .current_record_iterator
                .as_mut()
                .and_then(Iterator::next)
            {
                if record.offset() >= self.next_fetch_offset {
                    return Ok(FetchStep::InProgress(record));
                }
            } else {
                if self.pending_record_batch.is_none() {
                    let Some(batch_result) = self.log_record_batch.next() else {
                        if let Some(batch) = self.current_record_batch.take() {
                            self.next_fetch_offset = batch.next_log_offset();
                        }
                        self.drain();
                        return Ok(FetchStep::End);
                    };
                    self.pending_record_batch = Some(batch_result?);
                }

                let batch = self
                    .pending_record_batch
                    .as_ref()
                    .expect("pending batch must be set before schema resolution");
                let Some(read_context) = self.resolve_context_for_batch(batch) else {
                    return Ok(FetchStep::SchemaRequired(batch.schema_id()));
                };

                let batch = self
                    .pending_record_batch
                    .take()
                    .expect("pending batch must still be present after schema resolution");
                self.current_record_iterator = Some(batch.records(&read_context)?);
                self.current_record_batch = Some(batch);
            }
        }
    }

    /// Pulls the next decoded record into `out`, returning whether one was
    /// pushed. A fetch error is cached and surfaced on a later call rather than
    /// propagated mid-batch (mirrors one iteration of Java
    /// `CompletedFetch.fetchRecord`).
    fn fetch_one_record(&mut self, out: &mut Vec<ScanRecord>) -> FetchStep<()> {
        if self.cached_record_error.is_none() {
            self.corrupt_last_record = true;
            match self.next_fetched_record() {
                Ok(FetchStep::InProgress(record)) => {
                    self.corrupt_last_record = false;
                    self.last_record = Some(record);
                }
                Ok(FetchStep::End) => {
                    self.corrupt_last_record = false;
                    self.last_record = None;
                }
                Ok(FetchStep::SchemaRequired(schema_id)) => {
                    self.corrupt_last_record = false;
                    return FetchStep::SchemaRequired(schema_id);
                }
                Err(e) => {
                    self.cached_record_error = Some(e.to_string());
                }
            }
        }

        let Some(record) = self.last_record.take() else {
            return FetchStep::End;
        };

        out.push(record);
        FetchStep::InProgress(())
    }

    fn finish_records(&mut self, records: Vec<ScanRecord>) -> FetchResult<Vec<ScanRecord>> {
        if let Some(last) = records.last() {
            // Reaching the end of the underlying fetch may advance past the
            // last visible record (for example over an empty/control tail).
            // Preserve that terminal offset instead of moving it backwards.
            if !self.consumed {
                self.next_fetch_offset = last.offset() + 1;
            }
            self.records_read += records.len();
        }
        FetchResult::Data(records)
    }

    fn fetch_error(&self) -> Error {
        let mut message = format!(
            "Received exception when fetching the next record from {table_bucket}. If needed, please back to past the record to continue scanning.",
            table_bucket = self.table_bucket
        );
        if let Some(cause) = self.cached_record_error.as_deref() {
            message.push_str(&format!(" Cause: {cause}"));
        }
        Error::UnexpectedError {
            message,
            source: None,
        }
    }
    /// Get the next batch with its base offset.
    /// Returns (RecordBatch, base_offset) where base_offset is the offset of the first record.
    fn next_fetched_batch(&mut self) -> Result<FetchStep<(RecordBatch, i64)>> {
        loop {
            if self.pending_record_batch.is_none() {
                let Some(log_batch_result) = self.log_record_batch.next() else {
                    self.drain();
                    return Ok(FetchStep::End);
                };
                self.pending_record_batch = Some(log_batch_result?);
            }

            let log_batch = self
                .pending_record_batch
                .as_ref()
                .expect("pending batch must be set before schema resolution");
            let Some(read_context) = self.resolve_context_for_batch(log_batch) else {
                return Ok(FetchStep::SchemaRequired(log_batch.schema_id()));
            };

            let log_batch = self
                .pending_record_batch
                .take()
                .expect("pending batch must still be present after schema resolution");
            let mut record_batch = log_batch.record_batch(&read_context)?;

            // Skip empty batches
            if record_batch.num_rows() == 0 {
                continue;
            }

            // Calculate the effective base offset for this batch
            let log_base_offset = log_batch.base_log_offset();
            let effective_base_offset = if self.next_fetch_offset > log_base_offset {
                let skip_count = (self.next_fetch_offset - log_base_offset) as usize;
                if skip_count >= record_batch.num_rows() {
                    continue;
                }
                // Slice the batch to skip the first skip_count rows
                record_batch = record_batch.slice(skip_count, record_batch.num_rows() - skip_count);
                self.next_fetch_offset
            } else {
                log_base_offset
            };

            self.next_fetch_offset = log_batch.next_log_offset();
            self.records_read += record_batch.num_rows();
            return Ok(FetchStep::InProgress((record_batch, effective_base_offset)));
        }
    }

    /// Resolve the ReadContext for a given batch based on its schema_id.
    fn resolve_context_for_batch(&self, batch: &LogRecordBatch) -> Option<Arc<ReadContext>> {
        let schema_id = batch.schema_id();
        self.resolver.resolve(schema_id, self.is_remote)
    }
}

impl CompletedFetch for DefaultCompletedFetch {
    fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    fn api_error(&self) -> Option<&ApiError> {
        self.api_error.as_ref()
    }

    fn fetch_error_context(&self) -> Option<&FetchErrorContext> {
        self.fetch_error_context.as_ref()
    }

    fn take_error(&mut self) -> Option<Error> {
        self.error.take()
    }

    fn fetch_records(&mut self, max_records: usize) -> Result<FetchResult<Vec<ScanRecord>>> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }

        if let Some(api_error) = self.api_error.as_ref() {
            return Err(Error::FlussAPIError {
                api_error: ApiError {
                    code: api_error.code,
                    message: api_error.message.clone(),
                },
            });
        }

        if self.corrupt_last_record {
            return Err(self.fetch_error());
        }

        if self.consumed {
            return Ok(FetchResult::Data(Vec::new()));
        }

        let mut scan_records = std::mem::take(&mut self.pending_records);

        while scan_records.len() < max_records {
            match self.fetch_one_record(&mut scan_records) {
                FetchStep::InProgress(()) => {}
                FetchStep::End => break,
                FetchStep::SchemaRequired(schema_id) => {
                    if scan_records.is_empty()
                        || scan_records
                            .last()
                            .is_some_and(|record| *record.change_type() == ChangeType::UpdateBefore)
                    {
                        self.pending_records = scan_records;
                        return Ok(FetchResult::SchemaRequired(schema_id));
                    }
                    return Ok(self.finish_records(scan_records));
                }
            }
        }

        // Keep a -U paired with its +U in the same poll batch: a KV changelog
        // writes the pair as consecutive records, and splitting it across polls
        // would expose an orphaned -U to retract-based CDC consumers. Mirrors
        // Java's `CompletedFetch.fetchRecords`.
        if scan_records
            .last()
            .is_some_and(|record| *record.change_type() == ChangeType::UpdateBefore)
        {
            match self.fetch_one_record(&mut scan_records) {
                FetchStep::InProgress(()) | FetchStep::End => {}
                FetchStep::SchemaRequired(schema_id) => {
                    self.pending_records = scan_records;
                    return Ok(FetchResult::SchemaRequired(schema_id));
                }
            }
        }

        if self.cached_record_error.is_some() && scan_records.is_empty() {
            return Err(self.fetch_error());
        }

        Ok(self.finish_records(scan_records))
    }

    fn fetch_batches(
        &mut self,
        max_batches: usize,
    ) -> Result<FetchResult<Vec<(RecordBatch, i64)>>> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }

        if let Some(api_error) = self.api_error.as_ref() {
            return Err(Error::FlussAPIError {
                api_error: ApiError {
                    code: api_error.code,
                    message: api_error.message.clone(),
                },
            });
        }

        if self.consumed {
            return Ok(FetchResult::Data(Vec::new()));
        }

        let mut batches = Vec::with_capacity(max_batches.min(16));

        for _ in 0..max_batches {
            match self.next_fetched_batch()? {
                FetchStep::InProgress(batch_with_offset) => batches.push(batch_with_offset),
                FetchStep::End => break,
                FetchStep::SchemaRequired(schema_id) => {
                    if batches.is_empty() {
                        return Ok(FetchResult::SchemaRequired(schema_id));
                    }
                    return Ok(FetchResult::Data(batches));
                }
            }
        }

        Ok(FetchResult::Data(batches))
    }

    fn is_consumed(&self) -> bool {
        self.consumed
    }

    fn records_read(&self) -> usize {
        self.records_read
    }

    fn drain(&mut self) {
        self.consumed = true;
        self.api_error = None;
        self.fetch_error_context = None;
        self.error = None;
        self.cached_record_error = None;
        self.corrupt_last_record = false;
        self.last_record = None;
        self.pending_record_batch = None;
        self.pending_records.clear();
    }

    fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn set_initialized(&mut self) {
        self.initialized = true;
    }

    fn next_fetch_offset(&self) -> i64 {
        self.next_fetch_offset
    }
}

/// Completed fetch for remote log segments
/// Matches Java's RemoteCompletedFetch design - separate class for remote vs local
/// Holds RAII permit until consumed (data is in inner)
pub struct RemoteCompletedFetch {
    inner: DefaultCompletedFetch,
    permit: Option<PrefetchPermit>,
}

impl RemoteCompletedFetch {
    pub fn new(inner: DefaultCompletedFetch, permit: PrefetchPermit) -> Self {
        Self {
            inner,
            permit: Some(permit),
        }
    }
}

impl CompletedFetch for RemoteCompletedFetch {
    fn table_bucket(&self) -> &TableBucket {
        self.inner.table_bucket()
    }

    fn api_error(&self) -> Option<&ApiError> {
        self.inner.api_error()
    }

    fn fetch_error_context(&self) -> Option<&FetchErrorContext> {
        self.inner.fetch_error_context()
    }

    fn take_error(&mut self) -> Option<Error> {
        self.inner.take_error()
    }

    fn fetch_records(&mut self, max_records: usize) -> Result<FetchResult<Vec<ScanRecord>>> {
        self.inner.fetch_records(max_records)
    }

    fn fetch_batches(
        &mut self,
        max_batches: usize,
    ) -> Result<FetchResult<Vec<(RecordBatch, i64)>>> {
        self.inner.fetch_batches(max_batches)
    }

    fn is_consumed(&self) -> bool {
        self.inner.is_consumed()
    }

    fn records_read(&self) -> usize {
        self.inner.records_read()
    }

    fn drain(&mut self) {
        self.inner.drain();
        // Release permit immediately (don't wait for struct drop)
        // Critical: allows prefetch to continue even if Box<dyn CompletedFetch> kept around
        self.permit.take(); // drops permit here, triggers recycle notification
    }

    fn size_in_bytes(&self) -> usize {
        self.inner.size_in_bytes()
    }

    fn high_watermark(&self) -> i64 {
        self.inner.high_watermark()
    }

    fn is_initialized(&self) -> bool {
        self.inner.is_initialized()
    }

    fn set_initialized(&mut self) {
        self.inner.set_initialized()
    }

    fn next_fetch_offset(&self) -> i64 {
        self.inner.next_fetch_offset()
    }
}
// Permit released explicitly in drain() or automatically when struct drops

/// Pending fetch that waits for remote log file to be downloaded
pub struct RemotePendingFetch {
    segment: RemoteLogSegment,
    download_future: RemoteLogDownloadFuture,
    pos_in_log_segment: i32,
    fetch_offset: i64,
    high_watermark: i64,
    resolver: Arc<ReadContextResolver>,
}

impl RemotePendingFetch {
    pub fn new(
        segment: RemoteLogSegment,
        download_future: RemoteLogDownloadFuture,
        pos_in_log_segment: i32,
        fetch_offset: i64,
        high_watermark: i64,
        resolver: Arc<ReadContextResolver>,
    ) -> Self {
        Self {
            segment,
            download_future,
            pos_in_log_segment,
            fetch_offset,
            high_watermark,
            resolver,
        }
    }
}

impl PendingFetch for RemotePendingFetch {
    fn table_bucket(&self) -> &TableBucket {
        &self.segment.table_bucket
    }

    fn is_completed(&self) -> bool {
        self.download_future.is_done()
    }

    fn to_completed_fetch(self: Box<Self>) -> Result<Box<dyn CompletedFetch>> {
        // Take the RemoteLogFile and destructure
        let remote_log_file = self.download_future.take_remote_log_file()?;
        let RemoteLogFile {
            file_path,
            file_size: _,
            permit,
        } = remote_log_file;

        // Open file for streaming (no memory allocation for entire file)
        let file = std::fs::File::open(&file_path)?;
        let file_size = file.metadata()?.len() as usize;

        // Create file-backed LogRecordsBatches with cleanup (streaming!)
        // Data will be read batch-by-batch on-demand, not all at once
        // FileSource will delete the file when dropped (after file is closed)
        let log_record_batch =
            LogRecordsBatches::from_file(file, self.pos_in_log_segment as usize, file_path)?;

        // Calculate size based on position offset
        let size_in_bytes = if self.pos_in_log_segment > 0 {
            let pos = self.pos_in_log_segment as usize;
            if pos >= file_size {
                return Err(Error::UnexpectedError {
                    message: format!("Position {pos} exceeds file size {file_size}"),
                    source: None,
                });
            }
            file_size - pos
        } else {
            file_size
        };

        // Create DefaultCompletedFetch
        let inner_fetch = DefaultCompletedFetch::new(
            self.segment.table_bucket.clone(),
            log_record_batch,
            size_in_bytes,
            self.resolver,
            true, // is_remote
            self.fetch_offset,
            self.high_watermark,
        );

        // Wrap it with RemoteCompletedFetch to hold the permit
        // Permit manages the prefetch slot (releases semaphore and notifies coordinator) when dropped;
        // file deletion is handled by FileCleanupGuard in the file-backed source created via from_file
        Ok(Box::new(RemoteCompletedFetch::new(inner_fetch, permit)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::WriteRecord;
    use crate::client::table::read_context_resolver::ReadContextResolver;
    use crate::compression::{
        ArrowCompressionInfo, ArrowCompressionRatioEstimator, ArrowCompressionType,
        DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
    };
    use crate::metadata::{DataField, DataTypes, PhysicalTablePath, RowType, TablePath};
    use crate::record::{
        APPEND_ONLY_FLAG_MASK, ATTRIBUTES_OFFSET, LENGTH_LENGTH, LENGTH_OFFSET, LOG_OVERHEAD,
        MemoryLogRecordsArrowBuilder, RECORDS_OFFSET, ReadContext, to_arrow_schema,
    };
    use crate::row::GenericRow;
    use crate::test_utils::{build_table_info, build_table_info_with_columns};
    use std::sync::Arc;

    fn expect_data<T>(result: FetchResult<T>) -> T {
        match result {
            FetchResult::Data(data) => data,
            FetchResult::SchemaRequired(schema_id) => {
                panic!("unexpected missing schema {schema_id}")
            }
        }
    }

    fn test_resolver() -> Result<Arc<ReadContextResolver>> {
        let row_type = RowType::new(vec![DataField::new("id", DataTypes::int(), None)]);
        let arrow_schema = to_arrow_schema(&row_type)?;
        let row_type_arc = Arc::new(row_type);
        let local_ctx = Arc::new(ReadContext::new(
            arrow_schema.clone(),
            row_type_arc.clone(),
            false,
        ));
        let remote_ctx = Arc::new(ReadContext::new(arrow_schema, row_type_arc, true));
        Ok(Arc::new(ReadContextResolver::new(
            1, local_ctx, remote_ctx, None,
        )))
    }

    struct ErrorPendingFetch {
        table_bucket: TableBucket,
    }

    impl PendingFetch for ErrorPendingFetch {
        fn table_bucket(&self) -> &TableBucket {
            &self.table_bucket
        }

        fn is_completed(&self) -> bool {
            true
        }

        fn to_completed_fetch(self: Box<Self>) -> Result<Box<dyn CompletedFetch>> {
            Err(Error::UnexpectedError {
                message: "pending fetch failure".to_string(),
                source: None,
            })
        }
    }

    #[tokio::test]
    async fn await_not_empty_returns_wakeup_error() {
        let buffer = LogFetchBuffer::new(test_resolver().unwrap());
        buffer.wakeup();

        let result = buffer.await_not_empty(Duration::from_millis(10)).await;
        assert!(matches!(result, Err(Error::WakeupError { .. })));
    }

    #[tokio::test]
    async fn await_not_empty_returns_pending_error() {
        let buffer = LogFetchBuffer::new(test_resolver().unwrap());
        let table_bucket = TableBucket::new(1, 0);
        buffer.pend(Box::new(ErrorPendingFetch {
            table_bucket: table_bucket.clone(),
        }));
        buffer.try_complete(&table_bucket);

        let result = buffer.await_not_empty(Duration::from_millis(10)).await;
        assert!(matches!(result, Ok(true)));

        let mut completed = buffer.poll().expect("completed fetch");
        assert!(completed.take_error().is_some());
    }

    #[tokio::test]
    async fn await_not_empty_returns_true_when_data_arrives_during_wait() {
        let buffer = Arc::new(LogFetchBuffer::new(test_resolver().unwrap()));
        let table_bucket = TableBucket::new(1, 0);

        let producer = Arc::clone(&buffer);
        let producer_bucket = table_bucket.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            producer.pend(Box::new(ErrorPendingFetch {
                table_bucket: producer_bucket.clone(),
            }));
            producer.try_complete(&producer_bucket);
        });

        // Timeout far exceeds the producer delay: must return as soon as data lands, and must
        // not strand until the deadline even if the notification races with the internal check.
        let start = Instant::now();
        let result = buffer.await_not_empty(Duration::from_secs(5)).await;
        assert!(matches!(result, Ok(true)));
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn default_completed_fetch_reads_records() -> Result<()> {
        let row_type = RowType::new(vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("name", DataTypes::string(), None),
        ]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            &row_type,
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
            usize::MAX,
            Arc::new(ArrowCompressionRatioEstimator::default()),
        )?;

        let mut row = GenericRow::new(2);
        row.set_field(0, 1_i32);
        row.set_field(1, "alice");
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);
        builder.append(&record)?;

        let data = builder.build()?;
        let log_records = LogRecordsBatches::new(data.clone());
        let arrow_schema = to_arrow_schema(&row_type)?;
        let row_type_arc = Arc::new(row_type);
        let local_ctx = Arc::new(ReadContext::new(
            arrow_schema.clone(),
            row_type_arc.clone(),
            false,
        ));
        let remote_ctx = Arc::new(ReadContext::new(arrow_schema, row_type_arc, true));
        let resolver = Arc::new(ReadContextResolver::new(1, local_ctx, remote_ctx, None));
        let mut fetch = DefaultCompletedFetch::new(
            TableBucket::new(1, 0),
            log_records,
            data.len(),
            resolver,
            false,
            0,
            0,
        );

        let records = expect_data(fetch.fetch_records(10)?);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset(), 0);

        let empty = expect_data(fetch.fetch_records(10)?);
        assert!(empty.is_empty());

        Ok(())
    }

    #[test]
    fn fixed_schema_fetch_batches_pads_missing_columns() -> Result<()> {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let old_fields = vec![DataField::new("id", DataTypes::int(), None)];
        let new_fields = vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("name", DataTypes::string(), None),
        ];
        let old_table_info = Arc::new(build_table_info_with_columns(
            table_path.clone(),
            1,
            1,
            old_fields,
        ));
        let old_row_type = old_table_info.get_row_type().clone();
        let new_table_info = build_table_info_with_columns(table_path.clone(), 1, 1, new_fields);
        let new_row_type = new_table_info.get_row_type().clone();
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        let mut builder = MemoryLogRecordsArrowBuilder::new(
            0,
            &old_row_type,
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
            usize::MAX,
            Arc::new(ArrowCompressionRatioEstimator::default()),
        )?;

        let mut row = GenericRow::new(1);
        row.set_field(0, 1_i32);
        let record = WriteRecord::for_append(old_table_info.clone(), physical_table_path, 1, &row);
        builder.append(&record)?;

        let data = builder.build()?;
        let new_arrow_schema = to_arrow_schema(&new_row_type)?;
        let new_row_type_arc = Arc::new(new_row_type);
        let local_ctx = Arc::new(
            ReadContext::new(new_arrow_schema.clone(), new_row_type_arc.clone(), false)
                .with_fluss_row_type(new_row_type_arc.clone()),
        );
        let remote_ctx = Arc::new(
            ReadContext::new(new_arrow_schema.clone(), new_row_type_arc.clone(), true)
                .with_fluss_row_type(new_row_type_arc),
        );
        let resolver = Arc::new(
            ReadContextResolver::new(1, local_ctx, remote_ctx, None).with_fixed_schema(true),
        );

        let mut fetch = DefaultCompletedFetch::new(
            TableBucket::new(1, 0),
            LogRecordsBatches::new(data.clone()),
            data.len(),
            Arc::clone(&resolver),
            false,
            0,
            0,
        );

        assert!(matches!(
            fetch.fetch_batches(10)?,
            FetchResult::SchemaRequired(0)
        ));
        resolver.register_schema(0, old_table_info.get_schema())?;

        let batches = expect_data(fetch.fetch_batches(10)?);
        assert_eq!(batches.len(), 1);
        let batch = &batches[0].0;
        assert_eq!(batch.schema(), new_arrow_schema);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.column(1).null_count(), 1);

        Ok(())
    }

    /// A `-U`/`+U` pair must not be split across polls: even when `max_records`
    /// falls between them, `fetch_records` pulls the matching `+U` so the batch
    /// ends on a complete pair (mirrors Java `CompletedFetch.fetchRecords`).
    #[test]
    fn fetch_records_keeps_update_before_after_pair_together() -> Result<()> {
        let row_type = RowType::new(vec![DataField::new("id", DataTypes::int(), None)]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            &row_type,
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
            usize::MAX,
            Arc::new(ArrowCompressionRatioEstimator::default()),
        )?;
        for id in [10_i32, 20, 20] {
            let mut row = GenericRow::new(1);
            row.set_field(0, id);
            let record = WriteRecord::for_append(
                Arc::clone(&table_info),
                physical_table_path.clone(),
                1,
                &row,
            );
            builder.append(&record)?;
        }
        let append_only = builder.build()?;

        // Synthesize a changelog batch carrying +I, -U, +U.
        let change_types = [
            ChangeType::Insert,
            ChangeType::UpdateBefore,
            ChangeType::UpdateAfter,
        ];
        let mut data = Vec::with_capacity(append_only.len() + change_types.len());
        data.extend_from_slice(&append_only[..RECORDS_OFFSET]);
        data.extend(change_types.iter().map(ChangeType::to_byte_value));
        data.extend_from_slice(&append_only[RECORDS_OFFSET..]);
        data[ATTRIBUTES_OFFSET] &= !APPEND_ONLY_FLAG_MASK;
        let new_len = ((data.len() - LOG_OVERHEAD) as i32).to_le_bytes();
        data[LENGTH_OFFSET..LENGTH_OFFSET + LENGTH_LENGTH].copy_from_slice(&new_len);

        let read_context = Arc::new(ReadContext::new(
            to_arrow_schema(&row_type)?,
            Arc::new(row_type.clone()),
            false,
        ));
        let remote_ctx = Arc::new(ReadContext::new(
            to_arrow_schema(&row_type)?,
            Arc::new(row_type),
            true,
        ));
        let resolver = Arc::new(ReadContextResolver::new(1, read_context, remote_ctx, None));
        let mut fetch = DefaultCompletedFetch::new(
            TableBucket::new(1, 0),
            LogRecordsBatches::new(data.clone()),
            data.len(),
            resolver,
            false,
            0,
            0,
        );

        // max_records=2 cuts between -U (offset 1) and +U (offset 2); the pairing
        // guard pulls the +U so the batch ends on a complete pair.
        let records = expect_data(fetch.fetch_records(2)?);
        assert_eq!(records.len(), 3, "pair guard should append the matching +U");
        assert_eq!(*records[0].change_type(), ChangeType::Insert);
        assert_eq!(*records[1].change_type(), ChangeType::UpdateBefore);
        assert_eq!(*records[2].change_type(), ChangeType::UpdateAfter);

        let rest = expect_data(fetch.fetch_records(10)?);
        assert!(rest.is_empty(), "all records consumed");

        Ok(())
    }
}
