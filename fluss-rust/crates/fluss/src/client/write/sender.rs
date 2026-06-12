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

use crate::client::broadcast;
use crate::client::metadata::Metadata;
use crate::client::write::IdempotenceManager;
use crate::client::write::batch::WriteBatch;
use crate::client::{ReadyWriteBatch, RecordAccumulator};
use crate::error::Error::UnexpectedError;
use crate::error::{FlussError, Result};
use crate::metadata::{PhysicalTablePath, TableBucket, TablePath};
use crate::proto::{
    PbProduceLogRespForBucket, PbPutKvRespForBucket, PbTablePath, ProduceLogResponse, PutKvResponse,
};
use crate::record::{NO_BATCH_SEQUENCE, NO_WRITER_ID};
use crate::rpc::ServerConnection;
use crate::rpc::message::{InitWriterRequest, ProduceLogRequest, PutKvRequest};
use crate::{PartitionId, TableId};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::{debug, warn};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

type SendFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

/// Result of a synchronous drain: send futures, optional delay, and unknown leader tables.
type DrainResult<'a> = (
    Vec<SendFuture<'a>>,
    Option<u64>,
    HashSet<Arc<PhysicalTablePath>>,
);

#[allow(dead_code)]
pub struct Sender {
    running: AtomicBool,
    metadata: Arc<Metadata>,
    accumulator: Arc<RecordAccumulator>,
    in_flight_batches: Mutex<HashMap<TableBucket, Vec<i64>>>,
    max_request_size: i32,
    ack: i16,
    max_request_timeout_ms: i32,
    retries: i32,
    idempotence_manager: Arc<IdempotenceManager>,
}

impl Sender {
    pub fn new(
        metadata: Arc<Metadata>,
        accumulator: Arc<RecordAccumulator>,
        max_request_size: i32,
        max_request_timeout_ms: i32,
        ack: i16,
        retries: i32,
        idempotence_manager: Arc<IdempotenceManager>,
    ) -> Self {
        Self {
            running: AtomicBool::new(true),
            metadata,
            accumulator,
            in_flight_batches: Default::default(),
            max_request_size,
            ack,
            max_request_timeout_ms,
            retries,
            idempotence_manager,
        }
    }

    const WRITER_ID_RETRY_TIMES: u32 = 3;
    const WRITER_ID_RETRY_INTERVAL_MS: u64 = 100;

    async fn maybe_wait_for_writer_id(&self) -> Result<()> {
        if !self.idempotence_manager.is_enabled() || self.idempotence_manager.has_writer_id() {
            return Ok(());
        }
        let mut retry_count = 0u32;
        loop {
            match self.try_init_writer_id().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Authorization errors are not transient — fail immediately.
                    if e.api_error() == Some(FlussError::AuthorizationException) {
                        return Err(e);
                    }
                    if retry_count >= Self::WRITER_ID_RETRY_TIMES {
                        return Err(e);
                    }
                    if e.api_error().is_some_and(Self::is_invalid_metadata_error) {
                        let physical_paths = self.accumulator.get_physical_table_paths_in_batches();
                        let physical_refs: HashSet<&Arc<PhysicalTablePath>> =
                            physical_paths.iter().collect();
                        if let Err(meta_err) = self
                            .metadata
                            .update_tables_metadata(&HashSet::new(), &physical_refs, vec![])
                            .await
                        {
                            warn!("Failed to refresh metadata after writer ID error: {meta_err}");
                        }
                    }
                    retry_count += 1;
                    let delay_ms = Self::WRITER_ID_RETRY_INTERVAL_MS * 2u64.pow(retry_count);
                    warn!(
                        "Failed to allocate writer ID (attempt {retry_count}/{}), retrying in {delay_ms}ms: {e}",
                        Self::WRITER_ID_RETRY_TIMES,
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }

    async fn try_init_writer_id(&self) -> Result<()> {
        // Deduplicate by (database, table) since multiple physical paths (partitions)
        // may share the same table. Matches Java's Set<TablePath> dedup.
        let mut seen = HashSet::new();
        let table_paths: Vec<PbTablePath> = self
            .accumulator
            .get_physical_table_paths_in_batches()
            .iter()
            .filter_map(|path| {
                let key = (
                    path.get_database_name().to_string(),
                    path.get_table_name().to_string(),
                );
                if seen.insert(key.clone()) {
                    Some(PbTablePath {
                        database_name: key.0,
                        table_name: key.1,
                    })
                } else {
                    None
                }
            })
            .collect();
        if table_paths.is_empty() {
            debug!("No table paths in batches, skipping writer ID allocation");
            return Ok(());
        }
        let cluster = self.metadata.get_cluster();
        let server = cluster.get_one_available_server().ok_or(UnexpectedError {
            message: "No tablet server available to allocate writer ID".to_string(),
            source: None,
        })?;
        let connection = self.metadata.get_connection(server).await?;
        let response = connection
            .request(InitWriterRequest::new(table_paths))
            .await?;
        self.idempotence_manager.set_writer_id(response.writer_id);
        debug!(
            "Allocated writer ID {} for idempotent writes",
            response.writer_id
        );
        Ok(())
    }

    fn maybe_abort_batches(&self, error: &crate::error::Error) {
        if self.accumulator.has_incomplete() {
            warn!("Aborting write batches due to fatal error: {error}");
            self.accumulator.abort_batches(broadcast::Error::Client {
                message: format!("Writer ID allocation failed: {error}"),
            });
        }
    }

    /// Sequential init + drain + metadata refresh. Used by `run_once` (shutdown)
    /// where blocking is acceptable.
    async fn prepare_sends(&self) -> Result<(Vec<SendFuture<'_>>, Option<u64>)> {
        if let Err(e) = self.maybe_wait_for_writer_id().await {
            warn!("Failed to allocate writer ID after retries: {e}");
            self.maybe_abort_batches(&e);
            return Ok((vec![], None));
        }
        let (futures, delay, unknown_leaders) = self.drain_ready_sends()?;
        if !unknown_leaders.is_empty() {
            if let Err(e) = self.refresh_unknown_leaders(&unknown_leaders).await {
                warn!("Metadata refresh for unknown leaders failed: {e}");
            }
        }
        Ok((futures, delay))
    }

    /// Fully synchronous drain: `ready()` → `drain()` → build send futures.
    /// No async work — safe to call on the hot path without starving
    /// `pending.next()`. Returns unknown leader tables so the caller can
    /// schedule a concurrent metadata refresh.
    fn drain_ready_sends(&self) -> Result<DrainResult<'_>> {
        let cluster = self.metadata.get_cluster();
        let ready_check_result = self.accumulator.ready(&cluster)?;

        let unknown_leaders = ready_check_result.unknown_leader_tables;

        if ready_check_result.ready_nodes.is_empty() {
            return Ok((
                vec![],
                Some(ready_check_result.next_ready_check_delay_ms as u64),
                unknown_leaders,
            ));
        }

        let batches = self.accumulator.drain(
            cluster.clone(),
            &ready_check_result.ready_nodes,
            self.max_request_size,
        )?;

        let mut futures = Vec::new();
        if !batches.is_empty() {
            self.add_to_inflight_batches(&batches);
            for (leader_id, leader_batches) in batches {
                futures.push(
                    Box::pin(self.send_write_request(leader_id, self.ack, leader_batches))
                        as SendFuture<'_>,
                );
            }
        }

        Ok((futures, None, unknown_leaders))
    }

    /// Refresh metadata for buckets with unknown leaders. Runs as a concurrent
    /// maintenance task so it never blocks the response-processing hot path.
    async fn refresh_unknown_leaders(
        &self,
        unknown_leaders: &HashSet<Arc<PhysicalTablePath>>,
    ) -> Result<()> {
        let mut table_paths: HashSet<&TablePath> = HashSet::new();
        let mut physical_table_paths: HashSet<&Arc<PhysicalTablePath>> = HashSet::new();

        for path in unknown_leaders {
            if path.get_partition_name().is_some() {
                physical_table_paths.insert(path);
            } else {
                table_paths.insert(path.get_table_path());
            }
        }

        if let Err(e) = self
            .metadata
            .update_tables_metadata(&table_paths, &physical_table_paths, vec![])
            .await
        {
            match e.api_error() {
                Some(FlussError::PartitionNotExists) => {
                    warn!("Partition does not exist during metadata update, continuing: {e}");
                }
                _ => return Err(e),
            }
        }

        debug!("Updated metadata for unknown leader tables: {unknown_leaders:?}");
        Ok(())
    }

    /// Blocking version of drain + send, used during shutdown drain.
    async fn run_once(&self) -> Result<()> {
        let (futures, delay) = self.prepare_sends().await?;
        if let Some(ms) = delay {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            return Ok(());
        }
        for result in futures::future::join_all(futures).await {
            result?;
        }
        Ok(())
    }

    fn add_to_inflight_batches(&self, batches: &HashMap<i32, Vec<ReadyWriteBatch>>) {
        let mut in_flight = self.in_flight_batches.lock();
        for batch_list in batches.values() {
            for batch in batch_list {
                in_flight
                    .entry(batch.table_bucket.clone())
                    .or_default()
                    .push(batch.write_batch.batch_id());
            }
        }
    }

    async fn send_write_request(
        &self,
        destination: i32,
        acks: i16,
        batches: Vec<ReadyWriteBatch>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut records_by_bucket = HashMap::new();
        let mut write_batch_by_table: HashMap<TableId, Vec<TableBucket>> = HashMap::new();

        for batch in batches {
            let table_bucket = batch.table_bucket.clone();
            write_batch_by_table
                .entry(table_bucket.table_id())
                .or_default()
                .push(table_bucket.clone());
            records_by_bucket.insert(table_bucket, batch);
        }

        let cluster = self.metadata.get_cluster();

        let destination_node = match cluster.get_tablet_server(destination) {
            Some(node) => node,
            None => {
                self.handle_batches_with_error(
                    records_by_bucket.into_values().collect(),
                    FlussError::LeaderNotAvailableException,
                    format!("Destination node not found in metadata cache {destination}."),
                )
                .await?;
                return Ok(());
            }
        };
        let connection = match self.metadata.get_connection(destination_node).await {
            Ok(connection) => connection,
            Err(e) => {
                self.handle_batches_with_error(
                    records_by_bucket.into_values().collect(),
                    FlussError::NetworkException,
                    format!("Failed to connect destination node {destination}: {e}"),
                )
                .await?;
                return Ok(());
            }
        };

        for (table_id, table_buckets) in write_batch_by_table {
            let mut request_batches: Vec<ReadyWriteBatch> = table_buckets
                .iter()
                .filter_map(|bucket| records_by_bucket.remove(bucket))
                .collect();

            if request_batches.is_empty() {
                continue;
            }

            let write_request = match Self::build_write_request(
                table_id,
                acks,
                self.max_request_timeout_ms,
                &mut request_batches,
            ) {
                Ok(req) => req,
                Err(e) => {
                    self.handle_batches_with_local_error(
                        request_batches,
                        format!("Failed to build write request: {e}"),
                    )?;
                    continue;
                }
            };

            // let's put in back into records_by_bucket
            // since response handle will use it.
            for request_batch in request_batches {
                records_by_bucket.insert(request_batch.table_bucket.clone(), request_batch);
            }

            self.send_and_handle_response(
                &connection,
                write_request,
                table_id,
                &table_buckets,
                &mut records_by_bucket,
            )
            .await?;
        }

        Ok(())
    }

    fn build_write_request(
        table_id: i64,
        acks: i16,
        timeout_ms: i32,
        request_batches: &mut [ReadyWriteBatch],
    ) -> Result<WriteRequest> {
        let first_batch = &request_batches.first().unwrap().write_batch;

        let request = match first_batch {
            WriteBatch::ArrowLog(_) => {
                let req = ProduceLogRequest::new(table_id, acks, timeout_ms, request_batches)?;
                WriteRequest::ProduceLog(req)
            }
            WriteBatch::Kv(kv_write_batch) => {
                let target_columns = kv_write_batch.target_columns();
                for batch in request_batches.iter().skip(1) {
                    match &batch.write_batch {
                        WriteBatch::ArrowLog(_) => {
                            return Err(UnexpectedError {
                                message: "Expecting KvWriteBatch but found ArrowLogWriteBatch"
                                    .to_string(),
                                source: None,
                            });
                        }
                        WriteBatch::Kv(kvb) => {
                            if target_columns != kvb.target_columns() {
                                return Err(UnexpectedError {
                                    message: format!(
                                        "All the write batches to make put kv request should have the same target columns, but got {:?} and {:?}.",
                                        target_columns,
                                        kvb.target_columns()
                                    ),
                                    source: None,
                                });
                            }
                        }
                    }
                }
                let cols = target_columns
                    .map(|arc| arc.iter().map(|&c| c as i32).collect())
                    .unwrap_or_default();
                let req = PutKvRequest::new(table_id, acks, timeout_ms, cols, request_batches)?;
                WriteRequest::PutKv(req)
            }
        };

        Ok(request)
    }

    async fn send_and_handle_response(
        &self,
        connection: &ServerConnection,
        write_request: WriteRequest,
        table_id: i64,
        table_buckets: &[TableBucket],
        records_by_bucket: &mut HashMap<TableBucket, ReadyWriteBatch>,
    ) -> Result<()> {
        macro_rules! send {
            ($request:expr) => {
                match connection.request($request).await {
                    Ok(response) => {
                        self.handle_write_response(
                            table_id,
                            table_buckets,
                            records_by_bucket,
                            response,
                        )
                        .await
                    }
                    Err(e) => {
                        self.handle_batches_with_error(
                            table_buckets
                                .iter()
                                .filter_map(|b| records_by_bucket.remove(b))
                                .collect(),
                            FlussError::NetworkException,
                            format!("Failed to send write request: {e}"),
                        )
                        .await
                    }
                }
            };
        }

        match write_request {
            WriteRequest::ProduceLog(req) => send!(req),
            WriteRequest::PutKv(req) => send!(req),
        }
    }

    async fn handle_write_response<R: WriteResponse>(
        &self,
        table_id: i64,
        request_buckets: &[TableBucket],
        records_by_bucket: &mut HashMap<TableBucket, ReadyWriteBatch>,
        response: R,
    ) -> Result<()> {
        let mut invalid_metadata_tables: HashSet<TablePath> = HashSet::new();
        let mut invalid_physical_table_paths: HashSet<Arc<PhysicalTablePath>> = HashSet::new();
        let mut pending_buckets: HashSet<TableBucket> = request_buckets.iter().cloned().collect();

        for bucket_resp in response.buckets_resp() {
            let tb = TableBucket::new_with_partition(
                table_id,
                bucket_resp.partition_id(),
                bucket_resp.bucket_id(),
            );
            let Some(ready_batch) = records_by_bucket.remove(&tb) else {
                panic!("Missing ready batch for table bucket {tb}");
            };
            pending_buckets.remove(&tb);

            match bucket_resp.error_code() {
                Some(code) if code != FlussError::None.code() => {
                    let error = FlussError::for_code(code);
                    let message = bucket_resp
                        .error_message()
                        .cloned()
                        .unwrap_or_else(|| error.message().to_string());
                    if let Some(physical_table_path) =
                        self.handle_write_batch_error(ready_batch, error, message)?
                    {
                        invalid_metadata_tables
                            .insert(physical_table_path.get_table_path().clone());
                        invalid_physical_table_paths.insert(physical_table_path);
                    }
                }
                _ => self.complete_batch(ready_batch),
            }
        }

        for bucket in pending_buckets {
            if let Some(ready_batch) = records_by_bucket.remove(&bucket) {
                if let Some(physical_table_path) = self.handle_write_batch_error(
                    ready_batch,
                    FlussError::UnknownServerError,
                    format!("Missing response for table bucket {bucket}"),
                )? {
                    invalid_metadata_tables.insert(physical_table_path.get_table_path().clone());
                    invalid_physical_table_paths.insert(physical_table_path);
                }
            }
        }

        self.update_metadata_if_needed(invalid_metadata_tables, invalid_physical_table_paths)
            .await;
        Ok(())
    }

    // TODO: Java has a second overload `completeBatch(batch, bucket, logEndOffset)` used for
    // KV responses. When callers need write offset info, change BatchWriteResult to carry
    // optional offset metadata and plumb it through BroadcastOnce → ResultHandle → WriteResultFuture.
    fn complete_batch(&self, ready_write_batch: ReadyWriteBatch) {
        if self.idempotence_manager.is_enabled()
            && ready_write_batch.write_batch.batch_sequence() != NO_BATCH_SEQUENCE
        {
            self.idempotence_manager.handle_completed_batch(
                &ready_write_batch.table_bucket,
                ready_write_batch.write_batch.batch_id(),
                ready_write_batch.write_batch.writer_id(),
            );
        }
        self.finish_batch(ready_write_batch, Ok(()));
    }

    fn fail_batch(
        &self,
        ready_write_batch: ReadyWriteBatch,
        error: broadcast::Error,
        fluss_error: Option<FlussError>,
        adjust_sequences: bool,
    ) {
        if self.idempotence_manager.is_enabled()
            && ready_write_batch.write_batch.batch_sequence() != NO_BATCH_SEQUENCE
        {
            self.idempotence_manager.handle_failed_batch(
                &ready_write_batch.table_bucket,
                ready_write_batch.write_batch.batch_id(),
                ready_write_batch.write_batch.writer_id(),
                fluss_error,
                adjust_sequences,
            );
        }
        self.finish_batch(ready_write_batch, Err(error));
    }

    fn finish_batch(&self, ready_write_batch: ReadyWriteBatch, result: broadcast::Result<()>) {
        if ready_write_batch.write_batch.complete(result) {
            self.remove_from_inflight_batches(&ready_write_batch);
            // remove from incomplete batches
            self.accumulator
                .remove_incomplete_batches(ready_write_batch.write_batch.batch_id())
        }
    }

    async fn handle_batches_with_error(
        &self,
        batches: Vec<ReadyWriteBatch>,
        error: FlussError,
        message: String,
    ) -> Result<()> {
        let mut invalid_metadata_tables: HashSet<TablePath> = HashSet::new();
        let mut invalid_physical_table_paths: HashSet<Arc<PhysicalTablePath>> = HashSet::new();

        for batch in batches {
            if let Some(physical_table_path) =
                self.handle_write_batch_error(batch, error, message.clone())?
            {
                invalid_metadata_tables.insert(physical_table_path.get_table_path().clone());
                invalid_physical_table_paths.insert(physical_table_path);
            }
        }
        self.update_metadata_if_needed(invalid_metadata_tables, invalid_physical_table_paths)
            .await;
        Ok(())
    }

    fn handle_batches_with_local_error(
        &self,
        batches: Vec<ReadyWriteBatch>,
        message: String,
    ) -> Result<()> {
        for batch in batches {
            // Local errors (e.g. build failure) — server never saw the batch,
            // so it's always safe to adjust sequences.
            self.fail_batch(
                batch,
                broadcast::Error::Client {
                    message: message.clone(),
                },
                None,
                true,
            );
        }
        Ok(())
    }

    fn handle_write_batch_error(
        &self,
        ready_write_batch: ReadyWriteBatch,
        error: FlussError,
        message: String,
    ) -> Result<Option<Arc<PhysicalTablePath>>> {
        let physical_table_path = Arc::clone(ready_write_batch.write_batch.physical_table_path());

        if error == FlussError::DuplicateSequenceException {
            warn!(
                "Duplicate sequence for {} on bucket {}: {message}",
                physical_table_path.as_ref(),
                ready_write_batch.table_bucket.bucket_id()
            );
            self.complete_batch(ready_write_batch);
            return Ok(None);
        }

        if error == FlussError::OutOfOrderSequenceException
            && self.idempotence_manager.is_enabled()
            && self.idempotence_manager.is_already_committed(
                &ready_write_batch.table_bucket,
                ready_write_batch.write_batch.batch_sequence(),
            )
        {
            warn!(
                "Batch for {} on bucket {} with sequence {} received OutOfOrderSequenceException \
                 but has already been committed. Treating as success due to lost response.",
                physical_table_path.as_ref(),
                ready_write_batch.table_bucket.bucket_id(),
                ready_write_batch.write_batch.batch_sequence(),
            );
            self.complete_batch(ready_write_batch);
            return Ok(None);
        }

        if self.can_retry(&ready_write_batch, error) {
            warn!(
                "Retrying write batch for {} on bucket {} after error {error:?}: {message}",
                physical_table_path.as_ref(),
                ready_write_batch.table_bucket.bucket_id()
            );

            // If idempotence is enabled, only retry if the current writer ID still matches
            // the batch's writer ID. If the writer ID was reset (e.g., by another bucket's
            // error), fail the batch instead of retrying with stale state.
            if self.idempotence_manager.is_enabled() {
                let batch_writer_id = ready_write_batch.write_batch.writer_id();
                if batch_writer_id != NO_WRITER_ID
                    && self.idempotence_manager.writer_id() != batch_writer_id
                {
                    warn!(
                        "Writer ID changed from {} to {} since batch was sent, failing instead of retrying",
                        batch_writer_id,
                        self.idempotence_manager.writer_id()
                    );
                    self.fail_batch(
                        ready_write_batch,
                        broadcast::Error::WriteFailed {
                            code: FlussError::UnknownWriterIdException.code(),
                            message: format!(
                                "Attempted to retry sending a batch but the writer id has changed from {} to {}. This batch will be dropped.",
                                batch_writer_id,
                                self.idempotence_manager.writer_id()
                            ),
                        },
                        Some(FlussError::UnknownWriterIdException),
                        false,
                    );
                    return Ok(
                        Self::is_invalid_metadata_error(error).then_some(physical_table_path)
                    );
                }
            }

            self.re_enqueue_batch(ready_write_batch);
            return Ok(Self::is_invalid_metadata_error(error).then_some(physical_table_path));
        }

        // Generic error path. handle_failed_batch will detect remaining
        // OutOfOrderSequence (not already committed) / UnknownWriterId cases and
        // reset all writer state internally (matching Java).
        // For other errors, only adjust sequences if the batch didn't exhaust its retries.
        let can_adjust = ready_write_batch.write_batch.attempts() < self.retries;
        self.fail_batch(
            ready_write_batch,
            broadcast::Error::WriteFailed {
                code: error.code(),
                message,
            },
            Some(error),
            can_adjust,
        );
        Ok(Self::is_invalid_metadata_error(error).then_some(physical_table_path))
    }

    fn re_enqueue_batch(&self, ready_write_batch: ReadyWriteBatch) {
        self.remove_from_inflight_batches(&ready_write_batch);
        // TODO: add retry metrics (Java: writerMetricGroup.recordsRetryTotal().inc(recordCount))
        self.accumulator.re_enqueue(ready_write_batch);
    }

    fn remove_from_inflight_batches(&self, ready_write_batch: &ReadyWriteBatch) {
        let batch_id = ready_write_batch.write_batch.batch_id();
        let mut in_flight_guard = self.in_flight_batches.lock();
        if let Some(in_flight) = in_flight_guard.get_mut(&ready_write_batch.table_bucket) {
            in_flight.retain(|id| *id != batch_id);
            if in_flight.is_empty() {
                in_flight_guard.remove(&ready_write_batch.table_bucket);
            }
        }
    }

    fn can_retry(&self, ready_write_batch: &ReadyWriteBatch, error: FlussError) -> bool {
        if ready_write_batch.write_batch.attempts() >= self.retries
            || ready_write_batch.write_batch.is_done()
        {
            return false;
        }
        if Self::is_retriable_error(error) {
            return true;
        }
        // Idempotent-specific retry logic
        let seq = ready_write_batch.write_batch.batch_sequence();
        if self.idempotence_manager.is_enabled() && seq != NO_BATCH_SEQUENCE {
            return self.idempotence_manager.can_retry_for_error(
                &ready_write_batch.table_bucket,
                seq,
                ready_write_batch.write_batch.batch_id(),
                error,
            );
        }
        false
    }

    async fn update_metadata_if_needed(
        &self,
        table_paths: HashSet<TablePath>,
        physical_table_path: HashSet<Arc<PhysicalTablePath>>,
    ) {
        if table_paths.is_empty() {
            return;
        }
        let table_path_refs: HashSet<&TablePath> = table_paths.iter().collect();
        let physical_table_path_refs: HashSet<&Arc<PhysicalTablePath>> =
            physical_table_path.iter().collect();
        if let Err(e) = self
            .metadata
            .update_tables_metadata(&table_path_refs, &physical_table_path_refs, vec![])
            .await
        {
            warn!("Failed to update metadata after write error: {e:?}");
        }
    }

    fn is_invalid_metadata_error(error: FlussError) -> bool {
        matches!(
            error,
            FlussError::NotLeaderOrFollower
                | FlussError::UnknownTableOrBucketException
                | FlussError::LeaderNotAvailableException
                | FlussError::NetworkException
        )
    }

    fn is_retriable_error(error: FlussError) -> bool {
        matches!(
            error,
            FlussError::NetworkException
                | FlussError::NotLeaderOrFollower
                | FlussError::UnknownTableOrBucketException
                | FlussError::LeaderNotAvailableException
                | FlussError::LogStorageException
                | FlussError::KvStorageException
                | FlussError::StorageException
                | FlussError::RequestTimeOut
                | FlussError::NotEnoughReplicasAfterAppendException
                | FlussError::NotEnoughReplicasException
                | FlussError::CorruptMessage
                | FlussError::CorruptRecordException
        )
    }

    /// Event-loop sender: drain batches and fire RPCs into a `FuturesUnordered`,
    /// then process responses as they arrive. This interleaves drain cycles with
    /// response handling — when a fast leader responds, we immediately drain and
    /// send more batches for its buckets while slow leaders are still in-flight.
    ///
    /// Slow work (writer-ID init with retry backoff, metadata refresh for
    /// unknown leaders) runs as concurrent maintenance tasks so it never blocks
    /// `pending.next()`. The drain path (`drain_ready_sends`) is fully
    /// synchronous — no `.await` on the hot path. Without this separation,
    /// backoff sleeps during writer-ID init could stall response processing
    /// and cause severe backpressure when the accumulator memory budget is full
    /// (responses not polled → memory not freed → writers block).
    /// Single-select event loop with `need_drain` tick.
    ///
    /// Invariants:
    /// - `need_drain` is a one-shot "try a drain tick ASAP" flag.
    /// - Each iteration either performs a sync drain tick (if flagged) or blocks
    ///   in a single `tokio::select!`.
    /// - `accumulator.notified()` is always listened to (producer wakeups).
    /// - The idle timer is only armed when truly idle (no futures in any pool).
    /// - When writer_id isn't ready, a drain tick is a no-op but the loop stays
    ///   responsive (notified/init/meta can still wake it).
    pub async fn run_with_shutdown(&self, mut shutdown_rx: mpsc::Receiver<()>) -> Result<()> {
        let mut pending: FuturesUnordered<SendFuture<'_>> = FuturesUnordered::new();
        let mut init_futs: FuturesUnordered<SendFuture<'_>> = FuturesUnordered::new();
        let mut meta_futs: FuturesUnordered<SendFuture<'_>> = FuturesUnordered::new();
        let mut pending_unknown: HashSet<Arc<PhysicalTablePath>> = HashSet::new();

        let mut need_drain = true; // drain on first iteration to pick up any pre-existing batches
        let mut next_delay_ms: u64 = 1;

        loop {
            // Spawn writer-ID init task if needed and not already running.
            if init_futs.is_empty()
                && self.idempotence_manager.is_enabled()
                && !self.idempotence_manager.has_writer_id()
                && self.accumulator.has_undrained()
            {
                init_futs.push(Box::pin(self.maybe_wait_for_writer_id()));
            }

            // Spawn metadata refresh if we have accumulated unknown leaders
            // and no refresh is currently running.
            if !pending_unknown.is_empty() && meta_futs.is_empty() {
                let leaders = std::mem::take(&mut pending_unknown);
                meta_futs.push(Box::pin(async move {
                    self.refresh_unknown_leaders(&leaders).await
                }));
            }

            // Drain tick: synchronous, never blocks response processing.
            // Clear unconditionally — "need_drain" means "try", not "must succeed".
            if need_drain {
                need_drain = false;

                if !self.idempotence_manager.is_enabled()
                    || self.idempotence_manager.has_writer_id()
                {
                    match self.drain_ready_sends() {
                        Ok((futures, delay, unknown_leaders)) => {
                            if let Some(d) = delay {
                                next_delay_ms = d;
                            }
                            pending_unknown.extend(unknown_leaders);
                            for f in futures {
                                pending.push(f);
                            }
                        }
                        Err(e) => {
                            warn!("Error in drain cycle: {e}");
                        }
                    }
                }
            }

            let truly_idle = pending.is_empty() && init_futs.is_empty() && meta_futs.is_empty();
            debug_assert!(next_delay_ms >= 1);

            // One select to rule them all.
            tokio::select! {
                _ = shutdown_rx.recv() => break,

                // Always listen for producer wakeups.
                _ = self.accumulator.notified() => {
                    need_drain = true;
                }

                // Process in-flight send responses.
                Some(result) = pending.next(), if !pending.is_empty() => {
                    if let Err(e) = result {
                        warn!("Uncaught error in send request, continuing: {e}");
                    }
                    need_drain = true;
                }

                // Writer-ID init completed.
                Some(result) = init_futs.next(), if !init_futs.is_empty() => {
                    match result {
                        Ok(()) => need_drain = true,
                        Err(e) => {
                            warn!("Failed to allocate writer ID after retries: {e}");
                            self.maybe_abort_batches(&e);
                        }
                    }
                }

                // Metadata refresh completed — new leaders may now be known.
                Some(result) = meta_futs.next(), if !meta_futs.is_empty() => {
                    if let Err(e) = result {
                        warn!("Metadata refresh for unknown leaders failed: {e}");
                    }
                    need_drain = true;
                }

                // Idle timer: batch timeout / linger expiry.
                _ = tokio::time::sleep(Duration::from_millis(next_delay_ms)), if truly_idle => {
                    need_drain = true;
                }
            }
        }

        // Graceful shutdown: drain remaining batches, then wait for all
        // in-flight sends to complete.
        while self.accumulator.has_undrained() {
            if let Err(e) = self.run_once().await {
                warn!("Error during shutdown drain, continuing: {e}");
            }
        }
        while let Some(result) = pending.next().await {
            if let Err(e) = result {
                warn!("Error in send during shutdown, continuing: {e}");
            }
        }
        self.close();
        Ok(())
    }

    pub fn close(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

enum WriteRequest {
    ProduceLog(ProduceLogRequest),
    PutKv(PutKvRequest),
}

trait BucketResponse {
    fn bucket_id(&self) -> i32;
    fn error_code(&self) -> Option<i32>;
    fn error_message(&self) -> Option<&String>;

    fn partition_id(&self) -> Option<PartitionId>;
}

impl BucketResponse for PbProduceLogRespForBucket {
    fn bucket_id(&self) -> i32 {
        self.bucket_id
    }
    fn error_code(&self) -> Option<i32> {
        self.error_code
    }
    fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }

    fn partition_id(&self) -> Option<PartitionId> {
        self.partition_id
    }
}

impl BucketResponse for PbPutKvRespForBucket {
    fn bucket_id(&self) -> i32 {
        self.bucket_id
    }
    fn error_code(&self) -> Option<i32> {
        self.error_code
    }
    fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }

    fn partition_id(&self) -> Option<PartitionId> {
        self.partition_id
    }
}

trait WriteResponse {
    type BucketResp: BucketResponse;
    fn buckets_resp(&self) -> &[Self::BucketResp];
}

impl WriteResponse for ProduceLogResponse {
    type BucketResp = PbProduceLogRespForBucket;
    fn buckets_resp(&self) -> &[Self::BucketResp] {
        &self.buckets_resp
    }
}

impl WriteResponse for PutKvResponse {
    type BucketResp = PbPutKvRespForBucket;
    fn buckets_resp(&self) -> &[Self::BucketResp] {
        &self.buckets_resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::WriteRecord;
    use crate::cluster::Cluster;
    use crate::config::Config;
    use crate::metadata::{PhysicalTablePath, TablePath};
    use crate::proto::{PbProduceLogRespForBucket, ProduceLogResponse};
    use crate::row::{Datum, GenericRow};
    use crate::rpc::FlussError;
    use crate::test_utils::{build_cluster_arc, build_table_info};
    use std::collections::{HashMap, HashSet};

    fn disabled_idempotence() -> Arc<IdempotenceManager> {
        Arc::new(IdempotenceManager::new(false, 5))
    }

    fn enabled_idempotence() -> Arc<IdempotenceManager> {
        Arc::new(IdempotenceManager::new(true, 5))
    }

    fn build_ready_batch(
        accumulator: &RecordAccumulator,
        cluster: Arc<Cluster>,
        table_path: Arc<TablePath>,
    ) -> Result<(ReadyWriteBatch, crate::client::ResultHandle)> {
        let table_info = Arc::new(build_table_info(table_path.as_ref().clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(table_path));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);
        let result = accumulator.append(&record, 0, &cluster, false)?;
        let result_handle = result.result_handle.expect("result handle");
        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator.drain(cluster, &nodes, 1024 * 1024)?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        Ok((batch, result_handle))
    }

    #[tokio::test]
    async fn handle_write_batch_error_retries() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let idempotence = disabled_idempotence();
        let accumulator = Arc::new(RecordAccumulator::new(
            Config::default(),
            Arc::clone(&idempotence),
        ));
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            1,
            1,
            idempotence,
        );

        let (batch, _handle) =
            build_ready_batch(accumulator.as_ref(), cluster.clone(), table_path.clone())?;
        let mut inflight = HashMap::new();
        inflight.insert(1, vec![batch]);
        sender.add_to_inflight_batches(&inflight);
        let batch = inflight.remove(&1).unwrap().pop().unwrap();

        sender.handle_write_batch_error(
            batch,
            FlussError::RequestTimeOut,
            "timeout".to_string(),
        )?;

        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator.drain(cluster, &nodes, 1024 * 1024)?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        assert_eq!(batch.write_batch.attempts(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn handle_write_batch_error_fails() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let idempotence = disabled_idempotence();
        let accumulator = Arc::new(RecordAccumulator::new(
            Config::default(),
            Arc::clone(&idempotence),
        ));
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            1,
            0,
            idempotence,
        );

        let (batch, handle) = build_ready_batch(accumulator.as_ref(), cluster.clone(), table_path)?;
        sender.handle_write_batch_error(
            batch,
            FlussError::InvalidTableException,
            "invalid".to_string(),
        )?;

        let batch_result = handle.wait().await?;
        assert!(matches!(
            batch_result,
            Err(broadcast::Error::WriteFailed { code, .. })
                if code == FlussError::InvalidTableException.code()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn handle_produce_response_duplicate_sequence_completes() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let idempotence = disabled_idempotence();
        let accumulator = Arc::new(RecordAccumulator::new(
            Config::default(),
            Arc::clone(&idempotence),
        ));
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            1,
            0,
            idempotence,
        );

        let (batch, handle) = build_ready_batch(accumulator.as_ref(), cluster, table_path)?;
        let request_buckets = vec![batch.table_bucket.clone()];
        let mut records_by_bucket = HashMap::new();
        records_by_bucket.insert(batch.table_bucket.clone(), batch);

        let response = ProduceLogResponse {
            buckets_resp: vec![PbProduceLogRespForBucket {
                bucket_id: 0,
                error_code: Some(FlussError::DuplicateSequenceException.code()),
                error_message: Some("dup".to_string()),
                ..Default::default()
            }],
        };

        sender
            .handle_write_response(1, &request_buckets, &mut records_by_bucket, response)
            .await?;

        let batch_result = handle.wait().await?;
        assert!(matches!(batch_result, Ok(())));
        Ok(())
    }

    #[tokio::test]
    async fn test_unknown_writer_id_resets() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let idempotence = enabled_idempotence();
        let accumulator = Arc::new(RecordAccumulator::new(
            Config::default(),
            Arc::clone(&idempotence),
        ));
        idempotence.set_writer_id(42);
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            -1,
            i32::MAX,
            Arc::clone(&idempotence),
        );

        // build_ready_batch drains the batch, which assigns seq=0 and adds in-flight
        let (batch, handle) = build_ready_batch(accumulator.as_ref(), cluster.clone(), table_path)?;
        assert_eq!(batch.write_batch.batch_sequence(), 0);
        assert_eq!(batch.write_batch.writer_id(), 42);

        sender.handle_write_batch_error(
            batch,
            FlussError::UnknownWriterIdException,
            "unknown writer".to_string(),
        )?;

        // Writer ID should be reset
        assert!(!idempotence.has_writer_id());

        // Batch should be failed (not retried)
        let batch_result = handle.wait().await?;
        assert!(matches!(
            batch_result,
            Err(broadcast::Error::WriteFailed { code, .. })
                if code == FlussError::UnknownWriterIdException.code()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_out_of_order_sequence_non_retriable_resets() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let idempotence = enabled_idempotence();
        let accumulator = Arc::new(RecordAccumulator::new(
            Config::default(),
            Arc::clone(&idempotence),
        ));
        idempotence.set_writer_id(42);
        // retries=0 means can_retry returns false immediately (attempts >= retries)
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            -1,
            0,
            Arc::clone(&idempotence),
        );

        // build_ready_batch drains the batch, which assigns seq=0 and adds in-flight
        let (batch, handle) = build_ready_batch(accumulator.as_ref(), cluster.clone(), table_path)?;
        assert_eq!(batch.write_batch.batch_sequence(), 0);

        // OutOfOrderSequence with retries exhausted → non-retriable → resets writer ID
        sender.handle_write_batch_error(
            batch,
            FlussError::OutOfOrderSequenceException,
            "out of order".to_string(),
        )?;

        // Writer ID should be reset (matching Java behavior)
        assert!(!idempotence.has_writer_id());

        // Batch should be failed
        let batch_result = handle.wait().await?;
        assert!(matches!(
            batch_result,
            Err(broadcast::Error::WriteFailed { code, .. })
                if code == FlussError::OutOfOrderSequenceException.code()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_stale_writer_id_prevents_retry() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let metadata = Arc::new(Metadata::new_for_test(cluster.clone()));
        let idempotence = enabled_idempotence();
        let accumulator = Arc::new(RecordAccumulator::new(
            Config::default(),
            Arc::clone(&idempotence),
        ));
        idempotence.set_writer_id(42);
        let sender = Sender::new(
            metadata,
            accumulator.clone(),
            1024 * 1024,
            1000,
            -1,
            i32::MAX,
            Arc::clone(&idempotence),
        );

        // build_ready_batch drains the batch, which assigns seq=0 and adds in-flight
        let (batch, handle) = build_ready_batch(accumulator.as_ref(), cluster.clone(), table_path)?;
        assert_eq!(batch.write_batch.writer_id(), 42);
        let mut inflight = HashMap::new();
        inflight.insert(1, vec![batch]);
        sender.add_to_inflight_batches(&inflight);
        let batch = inflight.remove(&1).unwrap().pop().unwrap();

        // Simulate writer ID reset (e.g., another bucket got UnknownWriterIdException)
        idempotence.reset_writer_id();
        idempotence.set_writer_id(99); // new writer ID allocated

        // NetworkException is normally retriable, but writer ID changed
        sender.handle_write_batch_error(
            batch,
            FlussError::NetworkException,
            "connection reset".to_string(),
        )?;

        // Batch should be failed (not retried) because writer ID is stale
        let batch_result = handle.wait().await?;
        assert!(matches!(
            batch_result,
            Err(broadcast::Error::WriteFailed { code, .. })
                if code == FlussError::UnknownWriterIdException.code()
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_state_assigned_on_drain() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let idempotence = enabled_idempotence();
        let accumulator = Arc::new(RecordAccumulator::new(
            Config::default(),
            Arc::clone(&idempotence),
        ));
        idempotence.set_writer_id(99);

        // Append a record to the accumulator
        let table_info = Arc::new(build_table_info(table_path.as_ref().clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(table_path));
        let row = GenericRow {
            values: vec![Datum::Int32(42)],
        };
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);
        accumulator.append(&record, 0, &cluster, false)?;

        // Drain the batches — accumulator now assigns writer state during drain
        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let batches = accumulator.drain(cluster, &nodes, 1024 * 1024)?;

        // Verify the batch got writer state assigned by the accumulator
        let batch_list = batches.values().next().unwrap();
        let batch = &batch_list[0];
        assert_eq!(batch.write_batch.batch_sequence(), 0);
        assert_eq!(batch.write_batch.writer_id(), 99);
        Ok(())
    }

    #[tokio::test]
    async fn test_reenqueued_batch_keeps_sequence_on_redrain() -> Result<()> {
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));
        let cluster = build_cluster_arc(table_path.as_ref(), 1, 1);
        let idempotence = enabled_idempotence();
        let accumulator = Arc::new(RecordAccumulator::new(
            Config::default(),
            Arc::clone(&idempotence),
        ));
        idempotence.set_writer_id(99);

        // build_ready_batch drains the batch, which now assigns writer state
        // (seq=0) during drain since idempotence is enabled.
        let (batch, _handle) =
            build_ready_batch(accumulator.as_ref(), cluster.clone(), table_path)?;

        let writer_id = idempotence.writer_id();
        assert_eq!(batch.write_batch.batch_sequence(), 0);
        assert!(batch.write_batch.has_batch_sequence());
        assert_eq!(batch.write_batch.writer_id(), writer_id);

        // Re-enqueue the batch (simulating a retriable error)
        accumulator.re_enqueue(batch);

        // Drain again
        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator.drain(cluster, &nodes, 1024 * 1024)?;
        let batch_list = batches.values_mut().next().unwrap();
        let ready_batch = &mut batch_list[0];

        // Re-enqueued batch keeps its original sequence
        assert!(ready_batch.write_batch.has_batch_sequence());
        assert_eq!(ready_batch.write_batch.writer_id(), writer_id);
        assert_eq!(ready_batch.write_batch.batch_sequence(), 0);
        // Only one sequence was allocated (during the first drain)
        assert_eq!(
            idempotence.next_sequence_and_increment(&ready_batch.table_bucket),
            1
        );
        Ok(())
    }
}
