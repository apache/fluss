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

use super::decode_kv_batch;
use crate::client::ClientSchemaGetter;
use crate::client::metadata::Metadata;
use crate::error::{ApiError, Error, FlussError, Result};
use crate::metadata::{TableBucket, TableInfo};
use crate::proto::{PbScanReqForBucket, ScanKvResponse};
use crate::record::ScanBatch;
use crate::rpc::message::ScanKvRequest;
use crate::rpc::{RpcClient, ServerConnection};
use bytes::Bytes;
use log::debug;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const DEFAULT_KV_POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Streams every live row of a single primary-key bucket from the tablet
/// server's KV state via a sequence of `ScanKv` RPCs. The scan has snapshot
/// isolation: rows reflect the KV state at the moment the server opened its
/// snapshot; concurrent writes after that point are invisible.
///
/// At most one RPC is in flight. The RPC runs in an owned Tokio task so a caller
/// timeout or cancellation does not discard a response after the server may
/// have advanced its RocksDB iterator. Once a response indicates more data, the
/// next continuation is started immediately so its network round-trip overlaps
/// decoding and consuming the current batch.
///
/// Empty intermediate responses are skipped internally, so a returned batch
/// always carries at least one row.
pub struct KvBatchScanner {
    bucket: TableBucket,
    rpc_client: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    table_info: TableInfo,
    schema_getter: Arc<ClientSchemaGetter>,
    projected_fields: Option<Vec<usize>>,
    batch_size_bytes: i32,
    /// Leader connection, resolved lazily on the first `next_batch`.
    connection: Option<ServerConnection>,
    /// Server-assigned session id; `None` until the first response.
    scanner_id: Option<Vec<u8>>,
    /// Monotonic sequence number: 0 for the open request, incremented per
    /// continuation (matches the server's in-order delivery validation).
    call_seq_id: i32,
    /// Number of `TooManyScanners` open retries already attempted.
    open_retries: u32,
    /// Earliest instant at which a `TooManyScanners` open retry may be sent.
    /// Stored in scanner state so caller timeouts or cancellation do not shorten
    /// the exponential backoff.
    open_retry_at: Option<Instant>,
    /// Log high-watermark captured when the server opened the snapshot.
    log_offset: Option<i64>,
    /// The single open or continuation RPC currently executing.
    in_flight: Option<JoinHandle<Result<ScanKvRpcResult>>>,
    /// Raw records from the last completed RPC. Retained until decoding succeeds
    /// so cancellation during an asynchronous schema lookup cannot lose rows.
    pending_records: Option<Bytes>,
    drained: bool,
    closed: bool,
}

/// Result of an owned ScanKV task. The open task also returns the resolved
/// connection so subsequent continuations stay on the server that owns the
/// scanner session.
struct ScanKvRpcResult {
    connection: ServerConnection,
    response: ScanKvResponse,
}

/// Outcome of a KV batch read with a caller-supplied timeout.
#[derive(Debug)]
pub enum KvBatchReadOutcome {
    /// A batch is available.
    Batch(ScanBatch),
    /// The current RPC or record decoding did not complete before the timeout.
    /// The scanner remains valid and the caller may retry.
    TimedOut,
    /// The bucket has been fully drained.
    Finished,
}

impl KvBatchScanner {
    const MAX_OPEN_RETRIES: u32 = 3;
    const BASE_RETRY_DELAY_MS: u64 = 100;

    pub(crate) fn new(
        rpc_client: Arc<RpcClient>,
        metadata: Arc<Metadata>,
        table_info: TableInfo,
        schema_getter: Arc<ClientSchemaGetter>,
        projected_fields: Option<Vec<usize>>,
        bucket: TableBucket,
        batch_size_bytes: i32,
    ) -> Self {
        Self {
            bucket,
            rpc_client,
            metadata,
            table_info,
            schema_getter,
            projected_fields,
            batch_size_bytes: batch_size_bytes.max(1),
            connection: None,
            scanner_id: None,
            call_seq_id: 0,
            open_retries: 0,
            open_retry_at: None,
            log_offset: None,
            in_flight: None,
            pending_records: None,
            drained: false,
            closed: false,
        }
    }

    /// The bucket scanned by this `KvBatchScanner`.
    pub fn bucket(&self) -> &TableBucket {
        &self.bucket
    }

    /// The log high-watermark captured when the server opened the KV snapshot,
    /// available after the first `next_batch`. It marks the log offset from
    /// which a changelog tail can resume to read rows written after the
    /// snapshot (snapshot + changelog handoff).
    pub fn snapshot_log_offset(&self) -> Option<i64> {
        self.log_offset
    }

    /// Returns the next decoded batch, waiting until data arrives or the scan is
    /// drained.
    ///
    /// Transient continuation failures are not retried because the server may
    /// already have advanced the scanner cursor. An error leaves this scanner
    /// spent; create a new scanner to restart the bucket from the beginning.
    pub async fn next_batch(&mut self) -> Result<Option<ScanBatch>> {
        loop {
            match self
                .next_batch_with_timeout(DEFAULT_KV_POLL_TIMEOUT)
                .await?
            {
                KvBatchReadOutcome::Batch(batch) => return Ok(Some(batch)),
                KvBatchReadOutcome::TimedOut => continue,
                KvBatchReadOutcome::Finished => return Ok(None),
            }
        }
    }

    /// Returns the next batch while waiting for at most `timeout`.
    ///
    /// A timeout never cancels or resends the current `ScanKv` request. The
    /// owned RPC task remains stored in the scanner, and a later call continues
    /// waiting for that exact request and `call_seq_id`.
    pub async fn next_batch_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<KvBatchReadOutcome> {
        let start = Instant::now();

        loop {
            if self.is_fully_drained() {
                return Ok(KvBatchReadOutcome::Finished);
            }
            if self.closed {
                return Err(Error::UnexpectedError {
                    message: format!(
                        "KvBatchScanner for bucket {} was closed before it was fully drained",
                        self.bucket
                    ),
                    source: None,
                });
            }

            if let Some(raw) = self.pending_records.clone() {
                let Some(remaining) = remaining_timeout(start, timeout) else {
                    return Ok(KvBatchReadOutcome::TimedOut);
                };
                let decoded = tokio::time::timeout(
                    remaining,
                    decode_kv_batch(
                        &self.table_info,
                        &self.schema_getter,
                        self.projected_fields.as_deref(),
                        raw,
                        usize::MAX,
                    ),
                )
                .await;
                let batch = match decoded {
                    Ok(Ok(batch)) => batch,
                    Ok(Err(error)) => {
                        self.terminate();
                        return Err(error);
                    }
                    Err(_) => return Ok(KvBatchReadOutcome::TimedOut),
                };

                // Clear only after decoding succeeds. If the decode future is
                // cancelled while fetching an older schema, the original bytes
                // remain available for the next call.
                self.pending_records = None;
                return Ok(KvBatchReadOutcome::Batch(ScanBatch::new(
                    self.bucket.clone(),
                    batch,
                    0,
                )));
            }

            if let Some(retry_at) = self.open_retry_at {
                let now = Instant::now();
                if now < retry_at {
                    let Some(remaining) = remaining_timeout(start, timeout) else {
                        return Ok(KvBatchReadOutcome::TimedOut);
                    };
                    let retry_delay = retry_at - now;
                    if retry_delay >= remaining {
                        tokio::time::sleep(remaining).await;
                        return Ok(KvBatchReadOutcome::TimedOut);
                    }
                    tokio::time::sleep(retry_delay).await;
                }
                self.open_retry_at = None;
            }

            if self.in_flight.is_none() {
                if self.scanner_id.is_none() {
                    self.start_open_request();
                } else {
                    self.start_continuation_or_terminate()?;
                }
            }

            let Some(remaining) = remaining_timeout(start, timeout) else {
                return Ok(KvBatchReadOutcome::TimedOut);
            };
            let task_result = {
                let task = self
                    .in_flight
                    .as_mut()
                    .expect("ScanKV request must be in flight");
                wait_for_task(task, remaining).await
            };

            let joined = match task_result {
                Some(joined) => joined,
                None => return Ok(KvBatchReadOutcome::TimedOut),
            };
            self.in_flight = None;

            let rpc_result = match joined {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => {
                    self.terminate();
                    return Err(error);
                }
                Err(error) => {
                    self.terminate();
                    return Err(Error::UnexpectedError {
                        message: format!("ScanKV RPC task failed: {error}"),
                        source: Some(Box::new(error)),
                    });
                }
            };
            self.connection = Some(rpc_result.connection);
            let mut response = rpc_result.response;

            if let Some(code) = response.error_code
                && code != FlussError::None.code()
            {
                let retry_delay = self
                    .handle_error_response(code, response.error_message.take())
                    .await?;
                if let Some(delay) = retry_delay {
                    self.open_retry_at = Some(Instant::now() + delay);
                    continue;
                }
            }

            let response_has_scanner_id = response.scanner_id.is_some();
            if let Some(id) = response.scanner_id.take() {
                self.scanner_id = Some(id);
            }
            if self.log_offset.is_none() {
                self.log_offset = response.log_offset;
            }

            let Some(has_more_results) = response.has_more_results else {
                self.terminate();
                return Err(Error::UnexpectedError {
                    message: "ScanKV response did not include has_more_results".to_string(),
                    source: None,
                });
            };
            if has_more_results && !response_has_scanner_id {
                self.terminate();
                return Err(Error::UnexpectedError {
                    message: "ScanKV response reported more results without a scanner id"
                        .to_string(),
                    source: None,
                });
            }

            self.pending_records = response.records.take().filter(|raw| !raw.is_empty());

            if has_more_results {
                // Pipeline the next continuation before decoding or returning
                // the current records, matching the Java scanner.
                self.start_continuation_or_terminate()?;
            } else {
                // A terminal response means the server has already closed the
                // session; no explicit close request is needed.
                self.drained = true;
            }
        }
    }

    /// Drains the scanner into all of its batches.
    pub async fn collect_all_batches(&mut self) -> Result<Vec<ScanBatch>> {
        let mut batches = Vec::new();
        while let Some(batch) = self.next_batch().await? {
            batches.push(batch);
        }
        Ok(batches)
    }

    /// Best-effort close of the server-side scanner session. Only meaningful for
    /// a scanner abandoned mid-scan; a drained scanner is already closed by the
    /// server, and any failure here is reclaimed by the server-side session TTL.
    /// A scanner closed before it is drained remains incomplete; subsequent
    /// reads return an error rather than reporting normal end-of-scan.
    pub async fn close(&mut self) -> Result<()> {
        if self.closed || self.is_fully_drained() {
            return Ok(());
        }
        self.closed = true;
        // A terminal response may already have arrived while its records are
        // still waiting to be decoded. Discarding those records is an
        // incomplete close, not a successfully drained scan.
        self.drained = false;
        if let Some(task) = self.in_flight.take() {
            task.abort();
        }
        self.pending_records = None;
        // Dispatch the close in an owned task. The RPC is best effort, and this
        // keeps close cancellation-safe if the caller drops this future.
        self.send_best_effort_close();
        Ok(())
    }

    fn start_continuation_or_terminate(&mut self) -> Result<()> {
        match self.start_continuation() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.terminate();
                Err(error)
            }
        }
    }

    fn start_open_request(&mut self) {
        let bucket_req = PbScanReqForBucket {
            table_id: self.bucket.table_id(),
            partition_id: self.bucket.partition_id(),
            bucket_id: self.bucket.bucket_id(),
            limit: None,
        };
        // call_seq_id stays 0 for the open request (including open retries).
        self.call_seq_id = 0;
        let request = ScanKvRequest::new(
            None,
            Some(bucket_req),
            Some(self.call_seq_id),
            Some(self.batch_size_bytes),
            None,
        );

        let metadata = Arc::clone(&self.metadata);
        let rpc_client = Arc::clone(&self.rpc_client);
        let table_path = self.table_info.table_path.clone();
        let bucket = self.bucket.clone();
        self.in_flight = Some(tokio::spawn(async move {
            let leader = metadata
                .leader_for(&table_path, &bucket)
                .await?
                .ok_or_else(|| {
                    Error::leader_not_available(format!(
                        "No leader found for table bucket: {bucket}"
                    ))
                })?;
            let connection = rpc_client.get_connection(&leader).await?;
            let response = connection.request(request).await?;
            Ok(ScanKvRpcResult {
                connection,
                response,
            })
        }));
    }

    fn start_continuation(&mut self) -> Result<()> {
        if self.in_flight.is_some() {
            return Err(Error::UnexpectedError {
                message: "KvBatchScanner attempted to start concurrent ScanKV requests".to_string(),
                source: None,
            });
        }
        let connection = self
            .connection
            .clone()
            .ok_or_else(|| Error::UnexpectedError {
                message: "KvBatchScanner continuation issued without an open connection"
                    .to_string(),
                source: None,
            })?;
        let scanner_id = self
            .scanner_id
            .clone()
            .ok_or_else(|| Error::UnexpectedError {
                message: "KvBatchScanner continuation issued without a scanner id".to_string(),
                source: None,
            })?;
        self.call_seq_id += 1;
        let request = ScanKvRequest::new(
            Some(scanner_id),
            None,
            Some(self.call_seq_id),
            Some(self.batch_size_bytes),
            None,
        );
        self.in_flight = Some(tokio::spawn(async move {
            let response = connection.request(request).await?;
            Ok(ScanKvRpcResult {
                connection,
                response,
            })
        }));
        Ok(())
    }

    /// Handles an error response. `Ok(Some(delay))` means the caller should
    /// retry opening after the delay; all other errors are terminal.
    async fn handle_error_response(
        &mut self,
        code: i32,
        message: Option<String>,
    ) -> Result<Option<Duration>> {
        let error = FlussError::for_code(code);
        let api_error = ApiError {
            code,
            message: message.unwrap_or_else(|| error.message().to_string()),
        };

        match error {
            // Retry the open with exponential backoff — only before a session
            // was established, so no rows can be skipped or repeated.
            FlussError::TooManyScanners
                if self.scanner_id.is_none() && self.open_retries < Self::MAX_OPEN_RETRIES =>
            {
                let delay_ms = Self::BASE_RETRY_DELAY_MS * (1u64 << self.open_retries);
                self.open_retries += 1;
                Ok(Some(Duration::from_millis(delay_ms)))
            }
            // Stale leader: refresh metadata so a fresh scan resolves the new
            // leader. Auto-restarting here would silently swap the RocksDB
            // snapshot and break snapshot isolation.
            FlussError::NotLeaderOrFollower => {
                self.terminate();
                self.refresh_bucket_metadata().await;
                Err(Error::FlussAPIError { api_error })
            }
            // The server-side session is already gone; skip the close request.
            FlussError::ScannerExpired | FlussError::UnknownScannerId => {
                self.scanner_id = None;
                self.closed = true;
                Err(Error::FlussAPIError { api_error })
            }
            _ => {
                self.terminate();
                Err(Error::FlussAPIError { api_error })
            }
        }
    }

    async fn refresh_bucket_metadata(&self) {
        let partition_ids = metadata_refresh_partition_ids(&self.bucket);
        let result = if partition_ids.is_empty() {
            self.metadata
                .update_table_metadata(&self.table_info.table_path)
                .await
        } else {
            self.metadata
                .update_tables_metadata(
                    &HashSet::from([&self.table_info.table_path]),
                    &HashSet::new(),
                    partition_ids,
                )
                .await
        };
        if let Err(error) = result {
            debug!(
                "Failed to refresh metadata after NotLeaderOrFollower for bucket {}: {}",
                self.bucket, error
            );
        }
    }

    /// Marks the scanner closed, aborts the local RPC task and sends a
    /// best-effort close request. The server-side TTL is the final fallback.
    fn terminate(&mut self) {
        if self.closed {
            return;
        }
        let fully_drained = self.is_fully_drained();
        self.closed = true;
        if let Some(task) = self.in_flight.take() {
            task.abort();
        }
        self.open_retry_at = None;
        self.pending_records = None;
        if !fully_drained {
            self.drained = false;
        }
        self.send_best_effort_close();
    }

    fn send_best_effort_close(&self) {
        if self.drained {
            return;
        }
        let (Some(connection), Some(scanner_id)) =
            (self.connection.clone(), self.scanner_id.clone())
        else {
            return;
        };
        let batch_size_bytes = self.batch_size_bytes;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let request = ScanKvRequest::new(
                    Some(scanner_id),
                    None,
                    None,
                    Some(batch_size_bytes),
                    Some(true),
                );
                let _ = connection.request(request).await;
            });
        }
    }

    fn is_fully_drained(&self) -> bool {
        self.drained && self.pending_records.is_none()
    }
}

impl Drop for KvBatchScanner {
    fn drop(&mut self) {
        self.terminate();
    }
}

/// Full primary-key table scan: scans a fixed set of buckets sequentially, each
/// with its own [`KvBatchScanner`]. Buckets are scanned lazily and one at a
/// time, so only a single `ScanKv` session is open at any moment.
pub struct KvSnapshotScanner {
    pending: VecDeque<KvBatchScanner>,
    current: Option<KvBatchScanner>,
    terminal_failure: Option<String>,
}

impl KvSnapshotScanner {
    pub(crate) fn new(scanners: Vec<KvBatchScanner>) -> Self {
        Self {
            pending: scanners.into(),
            current: None,
            terminal_failure: None,
        }
    }

    /// Returns the next non-empty [`ScanBatch`] across all buckets, advancing to
    /// the next bucket as each one drains, or `None` once every bucket is done.
    pub async fn next_batch(&mut self) -> Result<Option<ScanBatch>> {
        loop {
            match self
                .next_batch_with_timeout(DEFAULT_KV_POLL_TIMEOUT)
                .await?
            {
                KvBatchReadOutcome::Batch(batch) => return Ok(Some(batch)),
                KvBatchReadOutcome::TimedOut => continue,
                KvBatchReadOutcome::Finished => return Ok(None),
            }
        }
    }

    /// Returns the next batch across all buckets while waiting for at most
    /// `timeout`. A timeout leaves the current bucket scanner resumable.
    pub async fn next_batch_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<KvBatchReadOutcome> {
        let start = Instant::now();
        loop {
            if let Some(failure) = &self.terminal_failure {
                return Err(Error::UnexpectedError {
                    message: format!(
                        "KvSnapshotScanner cannot be resumed: {failure}. Create a new scanner to \
                         restart the whole-table scan"
                    ),
                    source: None,
                });
            }
            if self.current.is_none() {
                self.current = self.pending.pop_front();
                if self.current.is_none() {
                    return Ok(KvBatchReadOutcome::Finished);
                }
            }
            let Some(remaining) = remaining_timeout(start, timeout) else {
                return Ok(KvBatchReadOutcome::TimedOut);
            };
            let result = self
                .current
                .as_mut()
                .expect("current scanner present")
                .next_batch_with_timeout(remaining)
                .await;
            match result {
                Err(error) => {
                    let bucket = self
                        .current
                        .as_ref()
                        .expect("current scanner present")
                        .bucket()
                        .clone();
                    self.fail(format!("bucket {bucket}: {error}"));
                    return Err(error);
                }
                Ok(KvBatchReadOutcome::Batch(batch)) => {
                    return Ok(KvBatchReadOutcome::Batch(batch));
                }
                Ok(KvBatchReadOutcome::TimedOut) => {
                    return Ok(KvBatchReadOutcome::TimedOut);
                }
                Ok(KvBatchReadOutcome::Finished) => self.current = None,
            }
        }
    }

    fn fail(&mut self, failure: String) {
        self.current = None;
        self.pending.clear();
        self.terminal_failure = Some(failure);
    }

    /// Drains the whole-table scan into all of its batches.
    pub async fn collect_all_batches(&mut self) -> Result<Vec<ScanBatch>> {
        let mut batches = Vec::new();
        while let Some(batch) = self.next_batch().await? {
            batches.push(batch);
        }
        Ok(batches)
    }

    /// Closes the active bucket scanner. Pending scanners are unopened and
    /// therefore hold no server-side resources. Closing before every bucket is
    /// fully consumed leaves this whole-table scanner incomplete, so subsequent
    /// reads return an error rather than reporting normal end-of-scan.
    pub async fn close(&mut self) -> Result<()> {
        let incomplete = !self.pending.is_empty()
            || self
                .current
                .as_ref()
                .map(|scanner| !scanner.is_fully_drained())
                .unwrap_or(false);
        if let Some(scanner) = self.current.as_mut() {
            scanner.close().await?;
        }
        self.current = None;
        self.pending.clear();
        if incomplete && self.terminal_failure.is_none() {
            self.terminal_failure =
                Some("scanner was closed before every bucket was fully drained".to_string());
        }
        Ok(())
    }
}

fn metadata_refresh_partition_ids(bucket: &TableBucket) -> Vec<i64> {
    bucket.partition_id().into_iter().collect()
}

fn remaining_timeout(start: Instant, timeout: Duration) -> Option<Duration> {
    let elapsed = start.elapsed();
    if elapsed >= timeout {
        None
    } else {
        Some(timeout - elapsed)
    }
}

async fn wait_for_task<T>(
    task: &mut JoinHandle<T>,
    timeout: Duration,
) -> Option<std::result::Result<T, tokio::task::JoinError>> {
    tokio::time::timeout(timeout, task).await.ok()
}

#[cfg(test)]
mod tests;
