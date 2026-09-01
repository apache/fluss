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

//! Bounded log reader that polls until stopping offsets, then terminates.
//!
//! Unlike [`RecordBatchLogScanner`] which is unbounded (continuous streaming),
//! [`RecordBatchLogReader`] reads log data up to a finite set of stopping
//! offsets and then signals completion. This enables "snapshot-style" reads
//! from a streaming log: capture the latest offsets, then consume all data
//! up to those offsets.
//!
//! The reader **takes ownership** of the scanner (move, not clone). Once the
//! scanner is moved into a reader, the compiler prevents concurrent polls.
//!
//! The reader also provides a synchronous [`arrow::record_batch::RecordBatchReader`]
//! adapter via [`RecordBatchLogReader::to_record_batch_reader`] for Arrow
//! ecosystem interop and FFI consumers (Python, C++).

use crate::client::admin::FlussAdmin;
use crate::client::table::RecordBatchLogScanner;
use crate::error::{Error, Result};
use crate::metadata::{TableBucket, TablePath};
use crate::record::ScanBatch;
use crate::rpc::message::OffsetSpec;
use crate::{PartitionId, TableId};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{Stream, future::try_join_all};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Outcome of a bounded record-batch read with a caller-supplied timeout.
#[derive(Debug)]
pub enum RecordBatchReadOutcome {
    /// A batch is available.
    Batch(ScanBatch),
    /// No batch became available before the timeout elapsed.
    TimedOut,
    /// Every subscribed bucket reached its stopping offset.
    Finished,
}

/// A `[starting_offset, stopping_offset)` read range for a single bucket.
///
/// Query engines that already know the offsets to read describe the whole scan
/// as a list of these ranges and hand it to
/// [`RecordBatchLogReader::new_from_ranges`], which subscribes the buckets and
/// installs the stopping offsets in one step.
#[derive(Debug, Clone)]
pub struct BoundedLogReadRange {
    /// Bucket to read. Its partition mode must match the scanned table.
    pub bucket: TableBucket,
    /// First offset to read, inclusive. Must be non-negative or
    /// [`crate::client::EARLIEST_OFFSET`].
    pub starting_offset: i64,
    /// Non-negative offset to stop at, exclusive. Must not be below
    /// `starting_offset`; an equal value is an empty range that completes
    /// immediately.
    pub stopping_offset: i64,
}

/// Outcome of draining a bounded reader under a total time budget.
#[derive(Debug)]
pub struct BoundedCollectOutcome {
    /// Complete batches collected before the budget was exhausted.
    pub batches: Vec<ScanBatch>,
    /// `true` when every stopping offset was reached, so `batches` holds the
    /// whole bounded result. `false` when the budget expired first and
    /// `batches` is a partial result.
    pub complete: bool,
}

/// Bounded log reader that consumes log data up to specified stopping offsets.
///
/// This type wraps a [`RecordBatchLogScanner`] and adds stopping semantics:
/// it polls batches from the scanner, filters/slices them against per-bucket
/// stopping offsets, and signals completion when all buckets are caught up.
///
/// The reader takes **ownership** of the scanner. Once moved in, no other code
/// can poll the same scanner concurrently.
///
/// # Construction
///
/// Use [`RecordBatchLogReader::new_until_latest`] for the common case of
/// reading all currently-available data, [`RecordBatchLogReader::new_from_ranges`]
/// or [`RecordBatchLogReader::new_between_timestamps`] to describe the whole
/// scan (starting offsets included) up front, or
/// [`RecordBatchLogReader::new_until_offsets`] to bound a scanner that is
/// already subscribed.
///
/// # Async iteration
///
/// Call [`next_batch`](RecordBatchLogReader::next_batch) repeatedly to get
/// [`ScanBatch`]es lazily, one at a time. Returns `None` when all buckets
/// have reached their stopping offsets.
///
/// # Sync adapter
///
/// Call [`to_record_batch_reader`](RecordBatchLogReader::to_record_batch_reader)
/// to get a synchronous [`arrow::record_batch::RecordBatchReader`] suitable
/// for Arrow FFI consumers.
pub struct RecordBatchLogReader {
    scanner: RecordBatchLogScanner,
    stopping_offsets: HashMap<TableBucket, i64>,
    buffer: VecDeque<ScanBatch>,
    schema: SchemaRef,
}

/// Clears the scanner's active-reader flag if reader construction exits early.
///
/// This makes async constructors cancellation-safe: dropping a constructor
/// future while it is awaiting metadata or offsets must not leave a shared
/// binding-layer scanner permanently locked.
struct ReaderActivationGuard<'a> {
    scanner: &'a RecordBatchLogScanner,
    clear_on_drop: bool,
}

impl<'a> ReaderActivationGuard<'a> {
    fn acquire(scanner: &'a RecordBatchLogScanner) -> Result<Self> {
        scanner.try_set_reader_active()?;
        Ok(Self {
            scanner,
            clear_on_drop: true,
        })
    }

    fn keep_active(mut self) {
        self.clear_on_drop = false;
    }
}

impl Drop for ReaderActivationGuard<'_> {
    fn drop(&mut self) {
        if self.clear_on_drop {
            self.scanner.clear_reader_active();
        }
    }
}

impl RecordBatchLogReader {
    /// Create a reader that reads until the latest offsets at the time of creation.
    ///
    /// Queries the server for the current latest offset of each subscribed
    /// bucket, then reads until those offsets are reached. Buckets whose
    /// subscribed offset already meets or exceeds the latest offset are
    /// excluded (nothing to read).
    ///
    /// Partition metadata is fetched once during construction; no caching
    /// is needed since each reader is typically short-lived.
    pub async fn new_until_latest(
        scanner: RecordBatchLogScanner,
        admin: &FlussAdmin,
    ) -> Result<Self> {
        // Acquire the guard first so no concurrent unsubscribe can mutate
        // state between reading subscriptions and using them.
        let activation = ReaderActivationGuard::acquire(&scanner)?;

        let subscribed = scanner.get_subscribed_buckets();
        if subscribed.is_empty() {
            return Err(Error::IllegalArgument {
                message: "No buckets subscribed. Call subscribe() before creating a reader."
                    .to_string(),
            });
        }
        validate_read_buckets(
            scanner.table_id(),
            scanner.is_partitioned(),
            scanner.num_buckets(),
            subscribed.iter().map(|(bucket, _)| bucket),
        )?;

        let stopping_offsets = query_latest_offsets(admin, &scanner, &subscribed).await?;
        unsubscribe_completed_buckets(
            &scanner,
            subscribed
                .iter()
                .map(|(bucket, _)| bucket)
                .filter(|bucket| !stopping_offsets.contains_key(*bucket)),
        );
        let schema = scanner.schema();
        activation.keep_active();

        Ok(Self {
            scanner,
            stopping_offsets,
            buffer: VecDeque::new(),
            schema,
        })
    }

    /// Create a reader with explicit stopping offsets per bucket.
    ///
    /// # NOTE: Every key in `stopping_offsets` **must** correspond to a bucket
    /// currently subscribed on the `scanner`, every subscribed bucket must
    /// have a stopping offset, every subscribed starting offset must be
    /// non-negative or [`crate::client::EARLIEST_OFFSET`], and every stopping
    /// offset must be non-negative; construction fails otherwise.
    /// Concrete subscriptions that already meet their stop point are treated
    /// as empty ranges and complete immediately.
    ///
    /// Use [`new_until_latest`](Self::new_until_latest) for the common case;
    /// it queries the server and builds a validated stopping-offset map
    /// automatically.
    pub fn new_until_offsets(
        scanner: RecordBatchLogScanner,
        mut stopping_offsets: HashMap<TableBucket, i64>,
    ) -> Result<Self> {
        let activation = ReaderActivationGuard::acquire(&scanner)?;

        validate_read_buckets(
            scanner.table_id(),
            scanner.is_partitioned(),
            scanner.num_buckets(),
            stopping_offsets.keys(),
        )?;
        let completed =
            validate_stopping_offsets(scanner.get_subscribed_buckets(), &mut stopping_offsets)?;
        unsubscribe_completed_buckets(&scanner, &completed);

        let schema = scanner.schema();
        activation.keep_active();
        Ok(Self {
            scanner,
            stopping_offsets,
            buffer: VecDeque::new(),
            schema,
        })
    }

    /// Create a reader from explicit `[starting_offset, stopping_offset)`
    /// ranges, subscribing every non-empty range on the way.
    ///
    /// This is the entry point for callers that already know the offsets to
    /// read (query engines planning a bounded scan, bindings receiving ranges
    /// from a foreign language). Ranges are validated against the scanned
    /// table before anything is subscribed: every bucket must belong to the
    /// table, match its partition mode, appear once, and have
    /// a valid bucket id, `starting_offset <= stopping_offset`, and a
    /// non-negative stopping offset. The scanner must not have existing
    /// subscriptions. Empty ranges need no subscription and complete
    /// immediately.
    pub async fn new_from_ranges(
        scanner: RecordBatchLogScanner,
        ranges: Vec<BoundedLogReadRange>,
    ) -> Result<Self> {
        // Acquire the guard before inspecting or changing subscriptions. The
        // internal subscribe helpers below require this guard and bypass the
        // public subscription check that intentionally rejects active readers.
        let activation = ReaderActivationGuard::acquire(&scanner)?;

        validate_read_ranges(
            scanner.table_id(),
            scanner.is_partitioned(),
            scanner.num_buckets(),
            &ranges,
        )?;
        if !scanner.get_subscribed_buckets().is_empty() {
            return Err(Error::IllegalArgument {
                message: "new_from_ranges requires a scanner without existing subscriptions."
                    .to_string(),
            });
        }

        let mut stopping_offsets = HashMap::with_capacity(ranges.len());
        let mut bucket_offsets: HashMap<i32, i64> = HashMap::new();
        let mut partition_bucket_offsets: HashMap<(PartitionId, i32), i64> = HashMap::new();
        for range in ranges {
            if range.stopping_offset == 0 || range.starting_offset == range.stopping_offset {
                continue;
            }
            match range.bucket.partition_id() {
                Some(partition_id) => {
                    partition_bucket_offsets.insert(
                        (partition_id, range.bucket.bucket_id()),
                        range.starting_offset,
                    );
                }
                None => {
                    bucket_offsets.insert(range.bucket.bucket_id(), range.starting_offset);
                }
            }
            stopping_offsets.insert(range.bucket, range.stopping_offset);
        }

        if !bucket_offsets.is_empty() {
            scanner
                .subscribe_buckets_for_reader(&bucket_offsets)
                .await?;
        }
        if !partition_bucket_offsets.is_empty() {
            scanner
                .subscribe_partition_buckets_for_reader(&partition_bucket_offsets)
                .await?;
        }

        let schema = scanner.schema();
        activation.keep_active();
        Ok(Self {
            scanner,
            stopping_offsets,
            buffer: VecDeque::new(),
            schema,
        })
    }

    /// Create a reader for a `[starting_timestamp_ms, stopping_timestamp_ms)`
    /// window over the requested buckets.
    ///
    /// Both timestamps are resolved to offsets per bucket, then read with
    /// `[starting_offset, stopping_offset)` semantics. Partition metadata is
    /// fetched once, and independent partition lookups are issued concurrently.
    pub async fn new_between_timestamps(
        scanner: RecordBatchLogScanner,
        admin: &FlussAdmin,
        buckets: &[TableBucket],
        starting_timestamp_ms: i64,
        stopping_timestamp_ms: i64,
    ) -> Result<Self> {
        if starting_timestamp_ms > stopping_timestamp_ms {
            return Err(Error::IllegalArgument {
                message: "starting_timestamp_ms must not exceed stopping_timestamp_ms.".to_string(),
            });
        }

        validate_read_buckets(
            scanner.table_id(),
            scanner.is_partitioned(),
            scanner.num_buckets(),
            buckets,
        )?;
        if buckets.is_empty() {
            // Nothing to resolve, so skip the offset lookups entirely.
            return Self::new_from_ranges(scanner, Vec::new()).await;
        }

        let resolver = OffsetResolver::new(admin, &scanner).await?;
        let starting_offsets = resolver
            .resolve(buckets, OffsetSpec::Timestamp(starting_timestamp_ms))
            .await?;
        let stopping_offsets = resolver
            .resolve(buckets, OffsetSpec::Timestamp(stopping_timestamp_ms))
            .await?;

        let mut ranges = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let (Some(&starting_offset), Some(&stopping_offset)) =
                (starting_offsets.get(bucket), stopping_offsets.get(bucket))
            else {
                return Err(Error::UnexpectedError {
                    message: format!(
                        "Timestamp offset lookup did not return an offset for {bucket:?}."
                    ),
                    source: None,
                });
            };
            ranges.push(BoundedLogReadRange {
                bucket: bucket.clone(),
                starting_offset,
                stopping_offset,
            });
        }

        Self::new_from_ranges(scanner, ranges).await
    }

    /// Returns the Arrow schema for batches produced by this reader.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Drain all remaining batches, waiting at most `timeout` for the whole
    /// operation.
    ///
    /// The timeout is a budget for this call rather than a per-poll timeout, so
    /// a stalled bucket cannot keep the caller blocked forever. Once the budget
    /// is exhausted, the reader no longer waits for scanner data, but it still
    /// drains already-buffered batches and observes completion before reporting
    /// a timeout. When unread work remains, the batches collected so far are
    /// returned with [`BoundedCollectOutcome::complete`] set to `false`; the
    /// reader stays valid, so resuming is an explicit caller policy.
    pub async fn collect_all_batches_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<BoundedCollectOutcome> {
        let start = Instant::now();
        let mut batches = Vec::new();
        loop {
            // next_batch_with_timeout checks buffered batches and completion
            // before checking its timeout. Passing a zero remaining duration
            // therefore stops network waiting without misreporting a complete
            // result as timed out.
            let remaining = timeout.saturating_sub(start.elapsed());
            match self.next_batch_with_timeout(remaining).await? {
                RecordBatchReadOutcome::Batch(batch) => batches.push(batch),
                RecordBatchReadOutcome::TimedOut => {
                    return Ok(BoundedCollectOutcome {
                        batches,
                        complete: false,
                    });
                }
                RecordBatchReadOutcome::Finished => {
                    return Ok(BoundedCollectOutcome {
                        batches,
                        complete: true,
                    });
                }
            }
        }
    }

    /// Drain all remaining batches until stopping offsets are satisfied.
    ///
    /// This is a convenience for callers (e.g. bindings building a single Arrow
    /// table) that want to materialize the full result in Rust without per-batch
    /// iteration.
    pub async fn collect_all_batches(&mut self) -> Result<Vec<ScanBatch>> {
        let mut out = Vec::new();
        while let Some(b) = self.next_batch().await? {
            out.push(b);
        }
        Ok(out)
    }

    /// Fetch the next [`ScanBatch`], or `None` if all buckets are caught up.
    ///
    /// Each call may internally poll multiple batches from the scanner,
    /// buffer them, and return one at a time. Batches that cross a stopping
    /// offset boundary are sliced to exclude records at or beyond the stop point.
    ///
    /// Completed buckets are unsubscribed from the scanner to avoid wasting
    /// network traffic on data the reader will discard.
    pub async fn next_batch(&mut self) -> Result<Option<ScanBatch>> {
        loop {
            match self.next_batch_with_timeout(DEFAULT_POLL_TIMEOUT).await? {
                RecordBatchReadOutcome::Batch(batch) => return Ok(Some(batch)),
                RecordBatchReadOutcome::TimedOut => continue,
                RecordBatchReadOutcome::Finished => return Ok(None),
            }
        }
    }

    /// Fetch the next [`ScanBatch`] while waiting for at most `timeout`.
    ///
    /// Unlike [`next_batch`](Self::next_batch), this method returns
    /// [`RecordBatchReadOutcome::TimedOut`] when no data becomes available
    /// before the timeout. The reader remains valid and the caller may retry.
    pub async fn next_batch_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<RecordBatchReadOutcome> {
        let start = Instant::now();
        loop {
            if let Some(batch) = self.buffer.pop_front() {
                return Ok(RecordBatchReadOutcome::Batch(batch));
            }

            if self.stopping_offsets.is_empty() {
                return Ok(RecordBatchReadOutcome::Finished);
            }

            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Ok(RecordBatchReadOutcome::TimedOut);
            }

            let scan_batches = self.scanner.poll(timeout - elapsed).await?;

            if scan_batches.is_empty() {
                return Ok(RecordBatchReadOutcome::TimedOut);
            }

            let completed =
                filter_batches(scan_batches, &mut self.stopping_offsets, &mut self.buffer);

            // Use the `_sync` unsubscribe variants here: the active-reader
            // guard rejects calls to the async `unsubscribe*` methods, but
            // the reader is allowed to clean up its own completed buckets.
            // The sync variants do the same map removal without the guard
            // check, and the partitioned/non-partitioned mismatch they
            // silently ignore is unreachable since the reader inherits the
            // scanner's partition mode.
            for tb in completed {
                if let Some(partition_id) = tb.partition_id() {
                    self.scanner
                        .unsubscribe_partition_sync(partition_id, tb.bucket_id());
                } else {
                    self.scanner.unsubscribe_sync(tb.bucket_id());
                }
            }
        }
    }

    /// Consume this reader into a [`Stream`] of [`ScanBatch`]es, one per
    /// [`next_batch`](Self::next_batch) call, ending when all buckets reach
    /// their stopping offsets or on the first error.
    ///
    /// Dropping the stream early runs the reader's [`Drop`] cleanup. The stream
    /// is `Send` but `!Unpin`; pin it before polling.
    pub fn into_stream(self) -> impl Stream<Item = Result<ScanBatch>> + Send {
        futures::stream::try_unfold(self, |mut reader| async move {
            Ok(reader.next_batch().await?.map(|batch| (batch, reader)))
        })
    }

    /// Convert this async reader into a synchronous [`arrow::record_batch::RecordBatchReader`].
    ///
    /// The returned adapter calls [`tokio::runtime::Handle::block_on`] on each
    /// iterator step. **Do not** call this from inside a Tokio worker thread
    /// while the same runtime is driving async work (nested `block_on` can
    /// panic or deadlock). Prefer [`next_batch`](RecordBatchLogReader::next_batch)
    /// in async Rust code. This is intended for sync/FFI boundaries (C++, some
    /// Python call paths).
    pub fn to_record_batch_reader(
        self,
        handle: tokio::runtime::Handle,
    ) -> SyncRecordBatchLogReader {
        SyncRecordBatchLogReader {
            reader: self,
            handle,
        }
    }
}

/// Best-effort cleanup when the reader is dropped before all buckets reach
/// their stopping offsets (early `break`, an exception in the consumer, etc.).
///
/// Why this matters even though we own the scanner:
///
/// In pure Rust, dropping the reader drops the owned `RecordBatchLogScanner`,
/// which decrements the `Arc<LogScannerInner>` to zero and frees the inner
/// state. Subscriptions die with it, so this `Drop` is a no-op in that path.
///
/// In the binding layer (Python today, C++/Elixir later), the binding holds
/// its own `Arc<LogScannerInner>` and uses
/// [`RecordBatchLogScanner::new_shared_handle`] to obtain a second handle for
/// the reader. When the reader is dropped mid-iteration the inner state stays
/// alive — and any buckets the reader hadn't yet completed remain in
/// `LogScannerStatus.bucket_status_map`. The user's next operations on the
/// original `LogScanner` would then see "ghost" subscriptions (extra buckets
/// being polled, stale offsets, etc.).
///
/// The `next_batch` loop already calls `unsubscribe` on each completed bucket,
/// so `stopping_offsets` accurately reflects the still-active set when `Drop`
/// runs. We unsubscribe each remaining bucket synchronously via the
/// `_sync` escape hatches (the underlying `LogScannerStatus` ops don't await),
/// so this is safe to call from any context — sync, async, a Tokio worker, or
/// a Python thread holding the GIL.
///
/// After cleanup, the `reader_active` guard is cleared so that the original
/// scanner (held by the binding layer) can accept new subscriptions again.
///
/// Caveats:
/// - Batches already buffered in `LogFetcher.log_fetch_buffer` for an
///   unsubscribed bucket are not drained here. They'll either be filtered out
///   by the next `RecordBatchLogReader` (via the "bucket not in
///   stopping_offsets" branch) or surface to a direct `poll_arrow` caller, who
///   was sharing scanner state in the first place.
/// - `Drop` cannot return errors. The `_sync` variants no-op on
///   partitioned/non-partitioned mismatch, but that mismatch is unreachable
///   here because the reader was constructed from this scanner and inherited
///   its partition mode.
impl Drop for RecordBatchLogReader {
    fn drop(&mut self) {
        for (tb, _) in self.stopping_offsets.drain() {
            if let Some(partition_id) = tb.partition_id() {
                self.scanner
                    .unsubscribe_partition_sync(partition_id, tb.bucket_id());
            } else {
                self.scanner.unsubscribe_sync(tb.bucket_id());
            }
        }
        self.scanner.clear_reader_active();
    }
}

/// Synchronous adapter that implements [`arrow::record_batch::RecordBatchReader`].
///
/// Created via [`RecordBatchLogReader::to_record_batch_reader`].
/// Blocks the current thread on each `next()` call using the provided
/// Tokio runtime handle.
///
/// The iterator yields plain [`RecordBatch`]es (bucket/offset metadata from
/// [`ScanBatch`] is stripped to satisfy the Arrow trait contract).
pub struct SyncRecordBatchLogReader {
    reader: RecordBatchLogReader,
    handle: tokio::runtime::Handle,
}

impl Iterator for SyncRecordBatchLogReader {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.handle.block_on(self.reader.next_batch()) {
            Ok(Some(scan_batch)) => Some(Ok(scan_batch.into_batch())),
            Ok(None) => None,
            Err(e) => Some(Err(arrow::error::ArrowError::ExternalError(Box::new(e)))),
        }
    }
}

impl arrow::record_batch::RecordBatchReader for SyncRecordBatchLogReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

/// Validate that every bucket in a bounded read belongs to the scanned table
/// and appears exactly once.
fn validate_read_buckets<'a>(
    table_id: TableId,
    is_partitioned: bool,
    num_buckets: i32,
    buckets: impl IntoIterator<Item = &'a TableBucket>,
) -> Result<()> {
    let mut seen = HashSet::new();
    for bucket in buckets {
        if bucket.table_id() != table_id {
            return Err(Error::IllegalArgument {
                message: format!("Bounded read bucket {bucket:?} is not part of table {table_id}."),
            });
        }
        if bucket.partition_id().is_some() != is_partitioned {
            return Err(Error::IllegalArgument {
                message: if is_partitioned {
                    format!("Bounded read bucket {bucket:?} is missing a partition id.")
                } else {
                    format!(
                        "Bounded read bucket {bucket:?} carries a partition id for a non-partitioned table."
                    )
                },
            });
        }
        if bucket.bucket_id() < 0 || bucket.bucket_id() >= num_buckets {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Bounded read bucket id {} is out of range for a table with {num_buckets} buckets.",
                    bucket.bucket_id()
                ),
            });
        }
        if !seen.insert(bucket) {
            return Err(Error::IllegalArgument {
                message: format!("Duplicate bucket {bucket:?} in a bounded read."),
            });
        }
    }
    Ok(())
}

/// Validate read ranges against the scanned table before any subscription
/// happens.
fn validate_read_ranges(
    table_id: TableId,
    is_partitioned: bool,
    num_buckets: i32,
    ranges: &[BoundedLogReadRange],
) -> Result<()> {
    validate_read_buckets(
        table_id,
        is_partitioned,
        num_buckets,
        ranges.iter().map(|range| &range.bucket),
    )?;
    for range in ranges {
        if range.starting_offset < 0 && range.starting_offset != crate::client::EARLIEST_OFFSET {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Read range for {:?} has unsupported negative starting offset {}.",
                    range.bucket, range.starting_offset
                ),
            });
        }
        if range.stopping_offset < 0 {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Read range for {:?} has negative stopping offset {}.",
                    range.bucket, range.stopping_offset
                ),
            });
        }
        if range.starting_offset > range.stopping_offset {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Read range for {:?} has starting offset {} above stopping offset {}.",
                    range.bucket, range.starting_offset, range.stopping_offset
                ),
            });
        }
    }
    Ok(())
}

fn validate_stopping_offsets(
    subscriptions: Vec<(TableBucket, i64)>,
    stopping_offsets: &mut HashMap<TableBucket, i64>,
) -> Result<Vec<TableBucket>> {
    let subscribed: HashMap<TableBucket, i64> = subscriptions.into_iter().collect();
    for (bucket, start) in &subscribed {
        if *start < 0 && *start != crate::client::EARLIEST_OFFSET {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Scanner subscription for {bucket:?} has unsupported negative starting offset {start}."
                ),
            });
        }
    }
    for bucket in stopping_offsets.keys() {
        if !subscribed.contains_key(bucket) {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Stopping offset for {bucket:?} has no matching scanner subscription."
                ),
            });
        }
    }
    for bucket in subscribed.keys() {
        if !stopping_offsets.contains_key(bucket) {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Scanner subscription for {bucket:?} has no matching stopping offset."
                ),
            });
        }
    }
    for (bucket, stop) in stopping_offsets.iter() {
        if *stop < 0 {
            return Err(Error::IllegalArgument {
                message: format!("Stopping offset for {bucket:?} must not be negative."),
            });
        }
    }

    // A stop at offset 0 is always empty because log offsets are non-negative.
    // Otherwise, a concrete subscription that already meets the stop point is
    // also empty. Remove both up front so the reader can finish without waiting
    // for a server batch that may never arrive. Negative starting offsets are
    // symbolic values such as EARLIEST_OFFSET and cannot otherwise be compared
    // until the server resolves them.
    let mut completed = Vec::new();
    stopping_offsets.retain(|bucket, stop| {
        let should_read = *stop > 0
            && subscribed
                .get(bucket)
                .is_none_or(|start| *start < 0 || start < stop);
        if !should_read {
            completed.push(bucket.clone());
        }
        should_read
    });
    Ok(completed)
}

fn unsubscribe_completed_buckets<'a>(
    scanner: &RecordBatchLogScanner,
    buckets: impl IntoIterator<Item = &'a TableBucket>,
) {
    for bucket in buckets {
        if let Some(partition_id) = bucket.partition_id() {
            scanner.unsubscribe_partition_sync(partition_id, bucket.bucket_id());
        } else {
            scanner.unsubscribe_sync(bucket.bucket_id());
        }
    }
}

/// Resolves an [`OffsetSpec`] into per-bucket offsets, hiding the partitioned
/// and non-partitioned lookup paths from callers.
///
/// Partition metadata is fetched once per resolver, so a construction that
/// resolves several specs (a timestamp range resolves two) still issues a
/// single `list_partition_infos` call. The resolver is not cached across
/// readers since each [`RecordBatchLogReader`] is typically short-lived.
struct OffsetResolver<'a> {
    admin: &'a FlussAdmin,
    table_path: &'a TablePath,
    table_id: TableId,
    /// Partition names by id, or `None` for a non-partitioned table.
    partition_names: Option<HashMap<PartitionId, String>>,
}

impl<'a> OffsetResolver<'a> {
    async fn new(admin: &'a FlussAdmin, scanner: &'a RecordBatchLogScanner) -> Result<Self> {
        let partition_names = if scanner.is_partitioned() {
            let partition_infos = admin.list_partition_infos(scanner.table_path()).await?;
            Some(
                partition_infos
                    .into_iter()
                    .map(|info| (info.get_partition_id(), info.get_partition_name()))
                    .collect(),
            )
        } else {
            None
        };

        Ok(Self {
            admin,
            table_path: scanner.table_path(),
            table_id: scanner.table_id(),
            partition_names,
        })
    }

    /// Resolve `spec` for `buckets`. Buckets the server does not report are
    /// absent from the result.
    async fn resolve(
        &self,
        buckets: &[TableBucket],
        spec: OffsetSpec,
    ) -> Result<HashMap<TableBucket, i64>> {
        let Some(partition_names) = self.partition_names.as_ref() else {
            let bucket_ids: Vec<i32> = buckets.iter().map(|tb| tb.bucket_id()).collect();
            let offsets = self
                .admin
                .list_offsets(self.table_path, &bucket_ids, spec)
                .await?;
            return Ok(offsets
                .into_iter()
                .map(|(bucket_id, offset)| (TableBucket::new(self.table_id, bucket_id), offset))
                .collect());
        };

        let mut bucket_ids_by_partition: HashMap<PartitionId, Vec<i32>> = HashMap::new();
        for bucket in buckets {
            if let Some(partition_id) = bucket.partition_id() {
                bucket_ids_by_partition
                    .entry(partition_id)
                    .or_default()
                    .push(bucket.bucket_id());
            }
        }

        let fetches = bucket_ids_by_partition
            .into_iter()
            .map(|(partition_id, bucket_ids)| {
                let spec = spec.clone();
                async move {
                    let partition_name = partition_names.get(&partition_id).ok_or_else(|| {
                        Error::UnexpectedError {
                            message: format!("Unknown partition_id: {partition_id}"),
                            source: None,
                        }
                    })?;
                    let offsets = self
                        .admin
                        .list_partition_offsets(self.table_path, partition_name, &bucket_ids, spec)
                        .await?;
                    Ok::<_, Error>((partition_id, offsets))
                }
            });

        let mut resolved: HashMap<TableBucket, i64> = HashMap::new();
        for (partition_id, offsets) in try_join_all(fetches).await? {
            for (bucket_id, offset) in offsets {
                let bucket =
                    TableBucket::new_with_partition(self.table_id, Some(partition_id), bucket_id);
                resolved.insert(bucket, offset);
            }
        }

        Ok(resolved)
    }
}

/// Query latest offsets for all subscribed buckets, handling both partitioned
/// and non-partitioned tables.
///
/// Buckets whose subscribed offset already meets or exceeds the latest offset
/// are excluded from the result (there is nothing to read). A `latest_offset`
/// of `0` means the bucket is empty. Missing or negative offsets are treated as
/// errors rather than silently reporting a potentially incomplete read as
/// finished.
async fn query_latest_offsets(
    admin: &FlussAdmin,
    scanner: &RecordBatchLogScanner,
    subscribed: &[(TableBucket, i64)],
) -> Result<HashMap<TableBucket, i64>> {
    let table_id = scanner.table_id();
    let buckets: Vec<TableBucket> = subscribed.iter().map(|(tb, _)| tb.clone()).collect();
    let latest_offsets = OffsetResolver::new(admin, scanner)
        .await?
        .resolve(&buckets, OffsetSpec::Latest)
        .await?;

    let mut stopping_offsets = HashMap::with_capacity(latest_offsets.len());
    for (bucket, subscribed_offset) in subscribed {
        let latest_offset =
            latest_offsets
                .get(bucket)
                .copied()
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!(
                        "Latest offset lookup did not return an offset for {bucket:?}."
                    ),
                    source: None,
                })?;
        if latest_offset < 0 {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Server returned negative latest offset {latest_offset} for {bucket:?} of table {table_id}."
                ),
                source: None,
            });
        }
        if latest_offset == 0 {
            continue;
        }
        if *subscribed_offset < latest_offset {
            stopping_offsets.insert(bucket.clone(), latest_offset);
        }
    }

    Ok(stopping_offsets)
}

/// Filter and slice scan batches against per-bucket stopping offsets.
///
/// For each batch:
/// - If the batch's bucket is not in `stopping_offsets`, skip it.
/// - If `base_offset >= stop_at`, the bucket is exhausted; remove from map.
/// - If `last_offset >= stop_at`, slice to keep only records before stop_at.
/// - Otherwise, keep the full batch.
///
/// Accepted batches with at least one row are pushed to `buffer`; empty
/// batches (e.g. a server-emitted batch containing no rows, or a slice that
/// reduces to zero rows) are dropped so consumers never observe an empty
/// `ScanBatch`. Returns the list of buckets that completed (were removed
/// from `stopping_offsets`).
fn filter_batches(
    scan_batches: Vec<ScanBatch>,
    stopping_offsets: &mut HashMap<TableBucket, i64>,
    buffer: &mut VecDeque<ScanBatch>,
) -> Vec<TableBucket> {
    let mut completed = Vec::new();

    for scan_batch in scan_batches {
        let bucket = scan_batch.bucket().clone();
        let Some(&stop_at) = stopping_offsets.get(&bucket) else {
            continue;
        };

        let base_offset = scan_batch.base_offset();
        let last_offset = scan_batch.last_offset();

        if base_offset >= stop_at {
            stopping_offsets.remove(&bucket);
            completed.push(bucket);
            continue;
        }

        let kept_batch = if last_offset >= stop_at {
            let num_to_keep = (stop_at - base_offset) as usize;
            let b = scan_batch.into_batch();
            let limit = num_to_keep.min(b.num_rows());
            ScanBatch::new(bucket.clone(), b.slice(0, limit), base_offset)
        } else {
            scan_batch
        };

        if kept_batch.batch().num_rows() > 0 {
            buffer.push_back(kept_batch);
        }

        if last_offset >= stop_at - 1 {
            stopping_offsets.remove(&bucket);
            completed.push(bucket);
        }
    }

    completed
}

// Rust-level end-to-end coverage for `new_until_latest`, partitioned tables,
// and `new_until_offsets` stopping semantics lives in
// `crates/fluss/tests/integration/record_batch_log_reader.rs`. Drop cleanup and the
// reader-active guard remain covered by the Python integration test
// `test_to_arrow_batch_reader_drop_and_guard`.
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn make_batch(values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn make_scan_batch(bucket: TableBucket, base_offset: i64, values: &[i32]) -> ScanBatch {
        ScanBatch::new(bucket, make_batch(values), base_offset)
    }

    fn bucket(id: i32) -> TableBucket {
        TableBucket::new(1, id)
    }

    #[test]
    fn validate_stopping_offsets_rejects_unsubscribed_bucket() {
        let mut offsets = HashMap::from([(bucket(1), 10)]);
        let result = validate_stopping_offsets(vec![(bucket(0), 0)], &mut offsets);

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    fn range(
        bucket: TableBucket,
        starting_offset: i64,
        stopping_offset: i64,
    ) -> BoundedLogReadRange {
        BoundedLogReadRange {
            bucket,
            starting_offset,
            stopping_offset,
        }
    }

    #[test]
    fn validate_read_ranges_accepts_empty_and_non_empty_ranges() {
        let ranges = vec![range(bucket(0), 5, 5), range(bucket(1), 0, 10)];

        validate_read_ranges(1, false, 2, &ranges).unwrap();
    }

    #[test]
    fn validate_read_ranges_rejects_bucket_of_another_table() {
        let ranges = vec![range(TableBucket::new(2, 0), 0, 10)];

        let result = validate_read_ranges(1, false, 1, &ranges);

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn validate_read_ranges_rejects_partition_mode_mismatch() {
        let unpartitioned = vec![range(bucket(0), 0, 10)];
        let partitioned = vec![range(TableBucket::new_with_partition(1, Some(7), 0), 0, 10)];

        assert!(matches!(
            validate_read_ranges(1, true, 1, &unpartitioned),
            Err(Error::IllegalArgument { .. })
        ));
        assert!(matches!(
            validate_read_ranges(1, false, 1, &partitioned),
            Err(Error::IllegalArgument { .. })
        ));
    }

    #[test]
    fn validate_read_ranges_rejects_out_of_range_bucket() {
        let ranges = vec![range(bucket(2), 0, 10)];

        let result = validate_read_ranges(1, false, 2, &ranges);

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn validate_read_ranges_rejects_duplicate_bucket() {
        let ranges = vec![range(bucket(0), 0, 10), range(bucket(0), 10, 20)];

        let result = validate_read_ranges(1, false, 1, &ranges);

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn validate_read_ranges_rejects_inverted_range() {
        let ranges = vec![range(bucket(0), 20, 10)];

        let result = validate_read_ranges(1, false, 1, &ranges);

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn validate_read_ranges_rejects_negative_stopping_offset() {
        let ranges = vec![range(bucket(0), crate::client::EARLIEST_OFFSET, -1)];

        let result = validate_read_ranges(1, false, 1, &ranges);

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn validate_read_ranges_rejects_unknown_negative_starting_offset() {
        let ranges = vec![range(bucket(0), -1, 10)];

        let result = validate_read_ranges(1, false, 1, &ranges);

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn validate_stopping_offsets_prunes_completed_range() {
        let mut offsets = HashMap::from([(bucket(0), 10), (bucket(1), 20)]);
        let completed =
            validate_stopping_offsets(vec![(bucket(0), 10), (bucket(1), 15)], &mut offsets)
                .unwrap();

        assert!(!offsets.contains_key(&bucket(0)));
        assert_eq!(offsets.get(&bucket(1)), Some(&20));
        assert_eq!(completed, vec![bucket(0)]);
    }

    #[test]
    fn validate_stopping_offsets_prunes_zero_stop_with_symbolic_start() {
        let mut offsets = HashMap::from([(bucket(0), 0)]);
        let completed = validate_stopping_offsets(
            vec![(bucket(0), crate::client::EARLIEST_OFFSET)],
            &mut offsets,
        )
        .unwrap();

        assert!(!offsets.contains_key(&bucket(0)));
        assert_eq!(completed, vec![bucket(0)]);
    }

    #[test]
    fn validate_stopping_offsets_rejects_subscription_without_stop() {
        let mut offsets = HashMap::from([(bucket(0), 10)]);

        let result = validate_stopping_offsets(vec![(bucket(0), 0), (bucket(1), 0)], &mut offsets);

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn filter_batch_entirely_before_stop() {
        let mut offsets = HashMap::from([(bucket(0), 100)]);
        let mut buffer = VecDeque::new();

        let batches = vec![make_scan_batch(bucket(0), 10, &[1, 2, 3])];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].batch().num_rows(), 3);
        assert!(offsets.contains_key(&bucket(0)));
        assert!(completed.is_empty());
    }

    #[test]
    fn filter_batch_crossing_stop_offset_is_sliced() {
        let mut offsets = HashMap::from([(bucket(0), 12)]);
        let mut buffer = VecDeque::new();

        // base_offset=10, 5 rows -> offsets 10,11,12,13,14; stop_at=12 -> keep 2
        let batches = vec![make_scan_batch(bucket(0), 10, &[1, 2, 3, 4, 5])];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].batch().num_rows(), 2);
        assert!(!offsets.contains_key(&bucket(0)));
        assert_eq!(completed, vec![bucket(0)]);
    }

    #[test]
    fn filter_batch_at_or_after_stop_offset_is_skipped() {
        let mut offsets = HashMap::from([(bucket(0), 10)]);
        let mut buffer = VecDeque::new();

        // base_offset=10, stop_at=10 -> base >= stop, skip entirely
        let batches = vec![make_scan_batch(bucket(0), 10, &[1, 2, 3])];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert!(buffer.is_empty());
        assert!(!offsets.contains_key(&bucket(0)));
        assert_eq!(completed, vec![bucket(0)]);
    }

    #[test]
    fn filter_batch_ending_exactly_at_stop_minus_one() {
        let mut offsets = HashMap::from([(bucket(0), 13)]);
        let mut buffer = VecDeque::new();

        // base_offset=10, 3 rows -> offsets 10,11,12; last_offset=12, stop_at=13
        // last_offset (12) >= stop_at - 1 (12) => bucket done
        let batches = vec![make_scan_batch(bucket(0), 10, &[1, 2, 3])];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].batch().num_rows(), 3);
        assert!(!offsets.contains_key(&bucket(0)));
        assert_eq!(completed, vec![bucket(0)]);
    }

    #[test]
    fn filter_unknown_bucket_is_ignored() {
        let mut offsets = HashMap::from([(bucket(0), 100)]);
        let mut buffer = VecDeque::new();

        let batches = vec![make_scan_batch(bucket(99), 0, &[1, 2])];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert!(buffer.is_empty());
        assert!(offsets.contains_key(&bucket(0)));
        assert!(completed.is_empty());
    }

    #[test]
    fn filter_multiple_buckets_independent_tracking() {
        let mut offsets = HashMap::from([(bucket(0), 12), (bucket(1), 5)]);
        let mut buffer = VecDeque::new();

        let batches = vec![
            make_scan_batch(bucket(0), 10, &[1, 2, 3]), // last=12, stop=12 -> keep 2, done
            make_scan_batch(bucket(1), 0, &[10, 20, 30]), // last=2, stop=5 -> keep all, not done
        ];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer[0].batch().num_rows(), 2); // bucket 0: sliced
        assert_eq!(buffer[1].batch().num_rows(), 3); // bucket 1: full
        assert!(!offsets.contains_key(&bucket(0))); // bucket 0: done
        assert!(offsets.contains_key(&bucket(1))); // bucket 1: still tracking
        assert_eq!(completed, vec![bucket(0)]);
    }

    #[test]
    fn filter_empty_batch_at_stop() {
        let mut offsets = HashMap::from([(bucket(0), 5)]);
        let mut buffer = VecDeque::new();

        // empty batch: base_offset=5, 0 rows -> last_offset = base-1 = 4
        // base_offset (5) >= stop_at (5) -> skip, remove
        let batches = vec![make_scan_batch(bucket(0), 5, &[])];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert!(buffer.is_empty());
        assert!(!offsets.contains_key(&bucket(0)));
        assert_eq!(completed, vec![bucket(0)]);
    }

    #[test]
    fn filter_drops_empty_batch_before_stop() {
        // Empty batch well below the stop offset: base=5, 0 rows -> last=4, stop=100.
        // base_offset (5) < stop_at (100) and last_offset (4) < stop_at (100),
        // so it falls into the "keep full batch" branch but must not surface to
        // the consumer because it has zero rows.
        let mut offsets = HashMap::from([(bucket(0), 100)]);
        let mut buffer = VecDeque::new();

        let batches = vec![make_scan_batch(bucket(0), 5, &[])];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert!(buffer.is_empty());
        assert!(offsets.contains_key(&bucket(0)));
        assert!(completed.is_empty());
    }

    #[test]
    fn filter_single_row_batch_before_stop() {
        let mut offsets = HashMap::from([(bucket(0), 10)]);
        let mut buffer = VecDeque::new();

        let batches = vec![make_scan_batch(bucket(0), 5, &[42])];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].batch().num_rows(), 1);
        assert!(offsets.contains_key(&bucket(0)));
        assert!(completed.is_empty());
    }

    #[test]
    fn filter_single_row_batch_at_stop_boundary() {
        let mut offsets = HashMap::from([(bucket(0), 5)]);
        let mut buffer = VecDeque::new();

        // base_offset=4, 1 row -> last_offset=4, stop=5
        // last < stop -> keep all; last (4) >= stop-1 (4) -> done
        let batches = vec![make_scan_batch(bucket(0), 4, &[42])];
        let completed = filter_batches(batches, &mut offsets, &mut buffer);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].batch().num_rows(), 1);
        assert!(!offsets.contains_key(&bucket(0)));
        assert_eq!(completed, vec![bucket(0)]);
    }

    #[test]
    fn filter_preserves_scan_batch_metadata() {
        let mut offsets = HashMap::from([(bucket(3), 100)]);
        let mut buffer = VecDeque::new();

        let batches = vec![make_scan_batch(bucket(3), 42, &[1, 2])];
        filter_batches(batches, &mut offsets, &mut buffer);

        let sb = &buffer[0];
        assert_eq!(*sb.bucket(), bucket(3));
        assert_eq!(sb.base_offset(), 42);
    }

    #[test]
    fn filter_sliced_batch_preserves_base_offset() {
        let mut offsets = HashMap::from([(bucket(0), 12)]);
        let mut buffer = VecDeque::new();

        let batches = vec![make_scan_batch(bucket(0), 10, &[1, 2, 3, 4, 5])];
        filter_batches(batches, &mut offsets, &mut buffer);

        let sb = &buffer[0];
        assert_eq!(sb.base_offset(), 10);
        assert_eq!(*sb.bucket(), bucket(0));
    }
}
