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
use crate::metadata::TableBucket;
use crate::record::ScanBatch;
use crate::rpc::message::OffsetSpec;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;
use log::warn;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_millis(500);

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
/// reading all currently-available data, or [`RecordBatchLogReader::new_until_offsets`]
/// for custom stopping offsets.
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
        scanner.try_set_reader_active()?;

        let subscribed = scanner.get_subscribed_buckets();
        if subscribed.is_empty() {
            scanner.clear_reader_active();
            return Err(Error::IllegalArgument {
                message: "No buckets subscribed. Call subscribe() before creating a reader."
                    .to_string(),
            });
        }

        let stopping_offsets = match query_latest_offsets(admin, &scanner, &subscribed).await {
            Ok(o) => o,
            Err(e) => {
                scanner.clear_reader_active();
                return Err(e);
            }
        };
        let schema = scanner.schema();

        Ok(Self {
            scanner,
            stopping_offsets,
            buffer: VecDeque::new(),
            schema,
        })
    }

    /// Create a reader with explicit stopping offsets per bucket.
    ///
    /// # NOTE: Every key in `stopping_offsets` **must** correspond to a bucket that is
    /// currently subscribed on the `scanner`. If a stopping offset refers to a
    /// bucket that will never appear in polled batches, the reader will loop
    /// indefinitely waiting for data that never arrives.
    ///
    /// Use [`new_until_latest`](Self::new_until_latest) for the common case;
    /// it queries the server and builds a validated stopping-offset map
    /// automatically.
    pub fn new_until_offsets(
        scanner: RecordBatchLogScanner,
        stopping_offsets: HashMap<TableBucket, i64>,
    ) -> Result<Self> {
        scanner.try_set_reader_active()?;
        let schema = scanner.schema();
        Ok(Self {
            scanner,
            stopping_offsets,
            buffer: VecDeque::new(),
            schema,
        })
    }

    /// Returns the Arrow schema for batches produced by this reader.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
            if let Some(batch) = self.buffer.pop_front() {
                return Ok(Some(batch));
            }

            if self.stopping_offsets.is_empty() {
                return Ok(None);
            }

            let scan_batches = self.scanner.poll(DEFAULT_POLL_TIMEOUT).await?;

            if scan_batches.is_empty() {
                continue;
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

/// Query latest offsets for all subscribed buckets, handling both partitioned
/// and non-partitioned tables.
///
/// Buckets whose subscribed offset already meets or exceeds the latest offset
/// are excluded from the result (there is nothing to read). A `latest_offset`
/// of `0` means the bucket is empty and is silently skipped; a negative value
/// is unexpected from the server and is logged as a warning before being
/// skipped.
async fn query_latest_offsets(
    admin: &FlussAdmin,
    scanner: &RecordBatchLogScanner,
    subscribed: &[(TableBucket, i64)],
) -> Result<HashMap<TableBucket, i64>> {
    let table_path = scanner.table_path();

    if !scanner.is_partitioned() {
        let bucket_ids: Vec<i32> = subscribed.iter().map(|(tb, _)| tb.bucket_id()).collect();

        let offsets = admin
            .list_offsets(table_path, &bucket_ids, OffsetSpec::Latest)
            .await?;

        let subscribed_offset_by_bucket: HashMap<i32, i64> = subscribed
            .iter()
            .map(|(tb, off)| (tb.bucket_id(), *off))
            .collect();

        let table_id = scanner.table_id();
        Ok(offsets
            .into_iter()
            .filter(|(bucket_id, latest_offset)| {
                if *latest_offset < 0 {
                    warn!(
                        "Server returned negative latest offset {latest_offset} for bucket {bucket_id} of table {table_id}; skipping bucket."
                    );
                    return false;
                }
                if *latest_offset == 0 {
                    return false;
                }
                let Some(&subscribed_offset) = subscribed_offset_by_bucket.get(bucket_id)
                else {
                    return false;
                };
                subscribed_offset < *latest_offset
            })
            .map(|(bucket_id, offset)| (TableBucket::new(table_id, bucket_id), offset))
            .collect())
    } else {
        query_partitioned_offsets(admin, scanner, subscribed).await
    }
}

/// Query offsets for partitioned table subscriptions.
///
/// Partition metadata is fetched once per reader construction (not cached),
/// since each [`RecordBatchLogReader`] is typically short-lived and consumed.
async fn query_partitioned_offsets(
    admin: &FlussAdmin,
    scanner: &RecordBatchLogScanner,
    subscribed: &[(TableBucket, i64)],
) -> Result<HashMap<TableBucket, i64>> {
    let table_path = scanner.table_path();
    let table_id = scanner.table_id();

    let partition_infos = admin.list_partition_infos(table_path).await?;
    let partition_id_to_name: HashMap<i64, String> = partition_infos
        .into_iter()
        .map(|info| (info.get_partition_id(), info.get_partition_name()))
        .collect();

    let subscribed_offset_map: HashMap<TableBucket, i64> = subscribed.iter().cloned().collect();

    let mut by_partition: HashMap<i64, Vec<i32>> = HashMap::new();
    for (tb, _) in subscribed {
        if let Some(partition_id) = tb.partition_id() {
            by_partition
                .entry(partition_id)
                .or_default()
                .push(tb.bucket_id());
        }
    }

    let mut result: HashMap<TableBucket, i64> = HashMap::new();

    for (partition_id, bucket_ids) in by_partition {
        let partition_name =
            partition_id_to_name
                .get(&partition_id)
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!("Unknown partition_id: {partition_id}"),
                    source: None,
                })?;

        let offsets = admin
            .list_partition_offsets(table_path, partition_name, &bucket_ids, OffsetSpec::Latest)
            .await?;

        for (bucket_id, latest_offset) in offsets {
            if latest_offset < 0 {
                warn!(
                    "Server returned negative latest offset {latest_offset} for bucket {bucket_id} of partition {partition_id} (table {table_id}); skipping bucket."
                );
                continue;
            }
            if latest_offset == 0 {
                continue;
            }
            let tb = TableBucket::new_with_partition(table_id, Some(partition_id), bucket_id);
            let Some(&subscribed_offset) = subscribed_offset_map.get(&tb) else {
                continue;
            };
            if subscribed_offset < latest_offset {
                result.insert(tb, latest_offset);
            }
        }
    }

    Ok(result)
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
