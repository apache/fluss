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
use crate::client::write::IdempotenceManager;
use crate::client::write::batch::WriteBatch::{ArrowLog, Kv};
use crate::client::write::batch::{ArrowLogWriteBatch, KvWriteBatch, WriteBatch};
use crate::client::write::dynamic_batch_size::DynamicWriteBatchSizeEstimator;
use crate::client::{LogWriteRecord, Record, ResultHandle, WriteRecord};
use crate::cluster::{BucketLocation, Cluster, ServerNode};
use crate::compression::ArrowCompressionRatioEstimator;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::metadata::{PhysicalTablePath, TableBucket};
use crate::record::{NO_BATCH_SEQUENCE, NO_WRITER_ID};
use crate::util::current_time_ms;
use crate::{BucketId, PartitionId, TableId};
use dashmap::DashMap;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Byte-counting semaphore that blocks producers when total buffered memory
/// exceeds the configured limit. Matches Java's `LazyMemorySegmentPool` behavior.
///
/// TODO: Replace `notify_all()` with per-waiter FIFO signaling (Java uses per-request
/// Condition objects in a Deque) to avoid thundering herd under high contention.
///
/// TODO: Track actual batch memory usage instead of reserving a fixed `writer_batch_size`
/// per batch. This over-counts when batches don't fill completely, reducing effective
/// throughput. Requires tighter coupling with batch internals.
pub(crate) struct MemoryLimiter {
    state: Mutex<usize>,
    cond: Condvar,
    max_memory: usize,
    wait_timeout: Duration,
    closed: AtomicBool,
    waiting_count: AtomicUsize,
}

impl MemoryLimiter {
    pub fn new(max_memory: usize, wait_timeout: Duration) -> Self {
        Self {
            state: Mutex::new(0),
            cond: Condvar::new(),
            max_memory,
            wait_timeout,
            closed: AtomicBool::new(false),
            waiting_count: AtomicUsize::new(0),
        }
    }

    /// Try to acquire `size` bytes. Blocks until memory is available,
    /// the timeout expires, or the limiter is closed.
    /// Returns a `MemoryPermit` on success.
    pub fn acquire(self: &Arc<Self>, size: usize) -> Result<MemoryPermit> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::WriterClosed {
                message: "Memory limiter is closed".to_string(),
            });
        }

        if size > self.max_memory {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Batch size {} exceeds total buffer memory limit {}",
                    size, self.max_memory
                ),
            });
        }

        let mut used = self.state.lock();
        let deadline = Instant::now() + self.wait_timeout;
        while *used + size > self.max_memory {
            self.waiting_count.fetch_add(1, Ordering::Relaxed);
            let result = self.cond.wait_until(&mut used, deadline);
            self.waiting_count.fetch_sub(1, Ordering::Relaxed);

            if self.closed.load(Ordering::Acquire) {
                return Err(Error::WriterClosed {
                    message: "Memory limiter is closed".to_string(),
                });
            }
            if result.timed_out() && *used + size > self.max_memory {
                return Err(Error::BufferExhausted {
                    message: format!(
                        "Failed to allocate {} bytes for write batch within {}ms. \
                         {} of {} bytes in use, {} threads waiting.",
                        size,
                        self.wait_timeout.as_millis(),
                        *used,
                        self.max_memory,
                        self.waiting_count.load(Ordering::Relaxed),
                    ),
                });
            }
        }

        *used += size;
        Ok(MemoryPermit {
            limiter: Arc::clone(self),
            size,
        })
    }

    fn release(&self, size: usize) {
        let mut used = self.state.lock();
        *used = used.saturating_sub(size);
        self.cond.notify_all();
    }

    /// Returns true if any producers are currently blocked waiting for memory.
    /// Used by `ready()` to mark all batches as immediately sendable when
    /// memory is exhausted (matching Java's `exhausted` flag).
    pub fn has_waiters(&self) -> bool {
        self.waiting_count.load(Ordering::Relaxed) > 0
    }

    /// Mark the limiter as closed and wake all blocked producers.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.cond.notify_all();
    }
}

/// RAII guard that releases memory back to the `MemoryLimiter` on drop.
pub(crate) struct MemoryPermit {
    limiter: Arc<MemoryLimiter>,
    size: usize,
}

impl std::fmt::Debug for MemoryPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPermit")
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl Drop for MemoryPermit {
    fn drop(&mut self) {
        if self.size > 0 {
            self.limiter.release(self.size);
        }
    }
}

// Type alias to simplify complex nested types
type BucketBatches = Vec<(BucketId, Arc<Mutex<VecDeque<WriteBatch>>>)>;

#[allow(dead_code)]
pub struct RecordAccumulator {
    config: Config,
    write_batches: DashMap<Arc<PhysicalTablePath>, BucketAndWriteBatches>,
    // batch_id -> (complete callback, memory permit)
    incomplete_batches: RwLock<HashMap<i64, (ResultHandle, MemoryPermit)>>,
    batch_timeout_ms: i64,
    closed: AtomicBool,
    flushes_in_progress: AtomicI32,
    appends_in_progress: i32,
    nodes_drain_index: Mutex<HashMap<i32, usize>>,
    batch_id: AtomicI64,
    idempotence_manager: Arc<IdempotenceManager>,
    memory_limiter: Arc<MemoryLimiter>,
    /// Wakes the sender task when new batches are created or existing batches
    /// become full, so the sender can drain them immediately instead of waiting
    /// for its next poll cycle. This is the Rust equivalent of Java's
    /// `Sender.wakeup()` / Kafka's `RecordAccumulator.wakeup()`.
    sender_wakeup: Notify,
}

impl RecordAccumulator {
    pub fn new(config: Config, idempotence_manager: Arc<IdempotenceManager>) -> Self {
        let batch_timeout_ms = config.writer_batch_timeout_ms;
        let memory_limiter = Arc::new(MemoryLimiter::new(
            config.writer_buffer_memory_size,
            Duration::from_millis(config.writer_buffer_wait_timeout_ms),
        ));
        RecordAccumulator {
            config,
            write_batches: Default::default(),
            incomplete_batches: Default::default(),
            batch_timeout_ms,
            closed: Default::default(),
            flushes_in_progress: Default::default(),
            appends_in_progress: Default::default(),
            nodes_drain_index: Default::default(),
            batch_id: Default::default(),
            idempotence_manager,
            memory_limiter,
            sender_wakeup: Notify::new(),
        }
    }

    fn try_append(
        &self,
        record: &WriteRecord,
        dq: &mut VecDeque<WriteBatch>,
    ) -> Result<Option<RecordAppendResult>> {
        let dq_size = dq.len();
        if let Some(last_batch) = dq.back_mut() {
            return if let Some(result_handle) = last_batch.try_append(record)? {
                Ok(Some(RecordAppendResult::new(
                    result_handle,
                    dq_size > 1 || last_batch.is_closed(),
                    false,
                    false,
                )))
            } else {
                Ok(None)
            };
        }
        Ok(None)
    }

    fn append_new_batch(
        &self,
        cluster: &Cluster,
        record: &WriteRecord,
        dq: &mut VecDeque<WriteBatch>,
        permit: MemoryPermit,
        alloc_size: usize,
        compression_ratio_estimator: Arc<ArrowCompressionRatioEstimator>,
    ) -> Result<RecordAppendResult> {
        let physical_table_path = &record.physical_table_path;
        let table_path = physical_table_path.get_table_path();
        let table_info = cluster.get_table(table_path)?;
        let arrow_compression_info = table_info.get_table_config().get_arrow_compression_info()?;
        let row_type = &table_info.row_type;

        let schema_id = table_info.schema_id;

        let mut batch: WriteBatch = match record.record() {
            Record::Log(_) => ArrowLog(ArrowLogWriteBatch::new(
                self.batch_id.fetch_add(1, Ordering::Relaxed),
                Arc::clone(physical_table_path),
                schema_id,
                arrow_compression_info,
                row_type,
                current_time_ms(),
                matches!(&record.record, Record::Log(LogWriteRecord::RecordBatch(_))),
                alloc_size,
                compression_ratio_estimator,
            )?),
            Record::Kv(kv_record) => Kv(KvWriteBatch::new(
                self.batch_id.fetch_add(1, Ordering::Relaxed),
                Arc::clone(physical_table_path),
                schema_id,
                alloc_size,
                record.write_format.to_kv_format()?,
                kv_record.target_columns.clone(),
                current_time_ms(),
            )),
        };

        let batch_id = batch.batch_id();

        let result_handle = batch
            .try_append(record)?
            .expect("must append to a new batch");

        let batch_is_closed = batch.is_closed();
        dq.push_back(batch);

        self.incomplete_batches
            .write()
            .insert(batch_id, (result_handle.clone(), permit));
        Ok(RecordAppendResult::new(
            result_handle,
            dq.len() > 1 || batch_is_closed,
            true,
            false,
        ))
    }

    pub fn append(
        &self,
        record: &WriteRecord<'_>,
        bucket_id: BucketId,
        cluster: &Cluster,
        abort_if_batch_full: bool,
    ) -> Result<RecordAppendResult> {
        let physical_table_path = &record.physical_table_path;
        let table_path = physical_table_path.get_table_path();
        let table_info = cluster.get_table(table_path)?;
        let is_partitioned_table = table_info.is_partitioned();

        let partition_id = if is_partitioned_table {
            cluster.get_partition_id(physical_table_path)
        } else {
            None
        };

        let (dq, compression_ratio_estimator, dynamic_target) = {
            let mut binding = self
                .write_batches
                .entry(Arc::clone(physical_table_path))
                .or_insert_with(|| {
                    BucketAndWriteBatches::new(
                        table_info.table_id,
                        is_partitioned_table,
                        partition_id,
                        &self.config,
                    )
                });
            let bucket_and_batches = binding.value_mut();
            let dq = bucket_and_batches
                .batches
                .entry(bucket_id)
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                .clone();
            let dynamic_target = bucket_and_batches
                .dynamic_batch_size
                .as_ref()
                .map(|est| est.current());
            (
                dq,
                Arc::clone(&bucket_and_batches.compression_ratio_estimator),
                dynamic_target,
            )
        };

        let mut dq_guard = dq.lock();
        if let Some(append_result) = self.try_append(record, &mut dq_guard)? {
            return Ok(append_result);
        }

        if abort_if_batch_full {
            return Ok(RecordAppendResult::new_without_result_handle(
                true, false, true,
            ));
        }

        // Drop dq lock before blocking on memory to prevent deadlock:
        // producer holds dq + blocks on memory, while sender needs dq to drain.
        drop(dq_guard);

        let batch_size = dynamic_target.unwrap_or(self.config.writer_batch_size as usize);
        let record_size = record.estimated_record_size();
        let alloc_size = batch_size.max(record_size);
        let permit = self.memory_limiter.acquire(alloc_size)?;

        // Re-acquire dq lock after memory is available
        let mut dq_guard = dq.lock();
        // Re-try: another thread may have created a batch while we waited
        if let Some(append_result) = self.try_append(record, &mut dq_guard)? {
            return Ok(append_result); // permit drops here, memory released
        }

        self.append_new_batch(
            cluster,
            record,
            &mut dq_guard,
            permit,
            alloc_size,
            compression_ratio_estimator,
        )
    }

    pub fn ready(&self, cluster: &Arc<Cluster>) -> Result<ReadyCheckResult> {
        // Snapshot just the Arcs we need, avoiding cloning the entire BucketAndWriteBatches struct
        let entries: Vec<(Arc<PhysicalTablePath>, Option<PartitionId>, BucketBatches)> = self
            .write_batches
            .iter()
            .map(|entry| {
                let physical_table_path = Arc::clone(entry.key());
                let partition_id = entry.value().partition_id;
                let bucket_batches: Vec<_> = entry
                    .value()
                    .batches
                    .iter()
                    .map(|(bucket_id, batch_arc)| (*bucket_id, batch_arc.clone()))
                    .collect();
                (physical_table_path, partition_id, bucket_batches)
            })
            .collect();

        let mut ready_nodes = HashSet::new();
        let mut next_ready_check_delay_ms = self.batch_timeout_ms;
        let mut unknown_leader_tables = HashSet::new();
        let exhausted = self.memory_limiter.has_waiters();

        for (physical_table_path, mut partition_id, bucket_batches) in entries {
            next_ready_check_delay_ms = self.bucket_ready(
                &physical_table_path,
                physical_table_path.get_partition_name().is_some(),
                &mut partition_id,
                bucket_batches,
                &mut ready_nodes,
                &mut unknown_leader_tables,
                cluster,
                next_ready_check_delay_ms,
                exhausted,
            )?
        }

        Ok(ReadyCheckResult {
            ready_nodes,
            next_ready_check_delay_ms,
            unknown_leader_tables,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn bucket_ready(
        &self,
        physical_table_path: &Arc<PhysicalTablePath>,
        is_partitioned_table: bool,
        partition_id: &mut Option<PartitionId>,
        bucket_batches: BucketBatches,
        ready_nodes: &mut HashSet<ServerNode>,
        unknown_leader_tables: &mut HashSet<Arc<PhysicalTablePath>>,
        cluster: &Cluster,
        next_ready_check_delay_ms: i64,
        exhausted: bool,
    ) -> Result<i64> {
        let mut next_delay = next_ready_check_delay_ms;

        // First check this table has partitionId.
        if is_partitioned_table && partition_id.is_none() {
            let partition_id = cluster.get_partition_id(physical_table_path);

            if partition_id.is_some() {
                // Update the cached partition_id
                if let Some(mut entry) = self.write_batches.get_mut(physical_table_path) {
                    entry.partition_id = partition_id;
                }
            } else {
                log::debug!(
                    "Partition does not exist for {}, bucket will not be set to ready",
                    physical_table_path.as_ref()
                );

                // TODO: we shouldn't add unready partitions to unknownLeaderTables,
                // because it cases PartitionNotExistException later
                unknown_leader_tables.insert(Arc::clone(physical_table_path));
                return Ok(next_delay);
            }
        }

        for (bucket_id, batch) in bucket_batches {
            let batch_guard = batch.lock();
            if batch_guard.is_empty() {
                continue;
            }

            let batch = batch_guard.front().unwrap();
            let waited_time_ms = batch.waited_time_ms(current_time_ms());
            let deque_size = batch_guard.len();
            let full = deque_size > 1 || batch.is_closed();
            let table_bucket = cluster.get_table_bucket(physical_table_path, bucket_id)?;
            if let Some(leader) = cluster.leader_for(&table_bucket) {
                next_delay = self.batch_ready(
                    leader,
                    waited_time_ms,
                    full,
                    exhausted,
                    ready_nodes,
                    next_delay,
                );
            } else {
                unknown_leader_tables.insert(Arc::clone(physical_table_path));
            }
        }
        Ok(next_delay)
    }

    fn batch_ready(
        &self,
        leader: &ServerNode,
        waited_time_ms: i64,
        full: bool,
        exhausted: bool,
        ready_nodes: &mut HashSet<ServerNode>,
        next_ready_check_delay_ms: i64,
    ) -> i64 {
        if !ready_nodes.contains(leader) {
            let expired = waited_time_ms >= self.batch_timeout_ms;
            let sendable = full
                || expired
                || exhausted
                || self.closed.load(Ordering::Acquire)
                || self.flush_in_progress();

            if sendable {
                ready_nodes.insert(leader.clone());
            } else {
                let time_left_ms = self.batch_timeout_ms.saturating_sub(waited_time_ms);
                return next_ready_check_delay_ms.min(time_left_ms);
            }
        }
        next_ready_check_delay_ms
    }

    pub fn drain(
        &self,
        cluster: Arc<Cluster>,
        nodes: &HashSet<ServerNode>,
        max_size: i32,
    ) -> Result<HashMap<i32, Vec<ReadyWriteBatch>>> {
        if nodes.is_empty() {
            return Ok(HashMap::new());
        }
        let mut batches = HashMap::new();
        for node in nodes {
            let ready = self.drain_batches_for_one_node(&cluster, node, max_size)?;
            if !ready.is_empty() {
                batches.insert(node.id(), ready);
            }
        }

        Ok(batches)
    }

    /// Matches Java's `shouldStopDrainBatchesForBucket`. Returns true if
    /// this bucket should be skipped during drain.
    fn should_stop_drain_batches_for_bucket(
        &self,
        first: &WriteBatch,
        table_bucket: &TableBucket,
    ) -> bool {
        if !self.idempotence_manager.is_enabled() {
            return false;
        }
        if !self.idempotence_manager.is_writer_id_valid() {
            return true;
        }

        // Use batch_id comparison instead of sequence comparison. After
        // handle_failed_batch adjusts InFlightBatch sequences, the WriteBatch's
        // stored sequence may be stale (re_enqueue syncs it, but this is more
        // robust). Java can compare sequences because resetWriterState mutates
        // the batch directly; Rust uses lightweight InFlightBatch proxies.
        let is_first_in_flight = self.idempotence_manager.in_flight_count(table_bucket) == 0
            || (first.has_batch_sequence()
                && self
                    .idempotence_manager
                    .is_first_in_flight_batch(table_bucket, first.batch_id()));

        if is_first_in_flight {
            return false;
        }

        if !first.has_batch_sequence() {
            // Fresh batch: respect max in-flight limit
            !self
                .idempotence_manager
                .can_send_more_requests(table_bucket)
        } else {
            // Re-enqueued batch that's NOT first in-flight: stop
            true
        }
    }

    fn drain_batches_for_one_node(
        &self,
        cluster: &Cluster,
        node: &ServerNode,
        max_size: i32,
    ) -> Result<Vec<ReadyWriteBatch>> {
        let mut size: usize = 0;
        let buckets = self.get_all_buckets_in_current_node(node, cluster);
        let mut ready = Vec::new();

        if buckets.is_empty() {
            return Ok(ready);
        }

        let start = {
            let mut nodes_drain_index_guard = self.nodes_drain_index.lock();
            let drain_index = nodes_drain_index_guard.entry(node.id()).or_insert(0);
            *drain_index % buckets.len()
        };

        let mut current_index = start;
        let mut last_processed_index;

        loop {
            let bucket = &buckets[current_index];
            let table_path = bucket.physical_table_path();
            let table_bucket = bucket.table_bucket.clone();
            last_processed_index = current_index;
            current_index = (current_index + 1) % buckets.len();

            let deque = self
                .write_batches
                .get(table_path)
                .and_then(|bucket_and_write_batches| {
                    bucket_and_write_batches
                        .batches
                        .get(&table_bucket.bucket_id())
                        .cloned()
                });

            if let Some(deque) = deque {
                let mut maybe_batch = None;
                {
                    let mut batch_lock = deque.lock();
                    if !batch_lock.is_empty() {
                        let first_batch = batch_lock.front().unwrap();

                        if size + first_batch.estimated_size_in_bytes() > max_size as usize
                            && !ready.is_empty()
                        {
                            // there is a rare case that a single batch size is larger than the request size
                            // due to compression; in this case we will still eventually send this batch in
                            // a single request.
                            break;
                        }

                        // Improvement: `continue` instead of `break` to skip
                        // only this bucket, not all buckets for the node.
                        if self.should_stop_drain_batches_for_bucket(first_batch, &table_bucket) {
                            if current_index == start {
                                break;
                            }
                            continue;
                        }

                        maybe_batch = Some(batch_lock.pop_front().unwrap());
                    }
                }

                if let Some(ref mut batch) = maybe_batch {
                    // Assign writer state to fresh batches (matching Java's drain loop)
                    let writer_id = if self.idempotence_manager.is_enabled() {
                        self.idempotence_manager.writer_id()
                    } else {
                        NO_WRITER_ID
                    };
                    if writer_id != NO_WRITER_ID && !batch.has_batch_sequence() {
                        self.idempotence_manager
                            .maybe_update_writer_id(&table_bucket);
                        let seq = self
                            .idempotence_manager
                            .next_sequence_and_increment(&table_bucket);
                        batch.set_writer_state(writer_id, seq);
                        self.idempotence_manager.add_in_flight_batch(
                            &table_bucket,
                            seq,
                            batch.batch_id(),
                        );
                    }
                }

                if let Some(mut batch) = maybe_batch {
                    let current_batch_size = batch.estimated_size_in_bytes();
                    size += current_batch_size;

                    self.record_actual_batch_size(table_path, current_batch_size);

                    // mark the batch as drained.
                    batch.drained(current_time_ms());
                    ready.push(ReadyWriteBatch {
                        table_bucket,
                        write_batch: batch,
                    });
                }
            }
            if current_index == start {
                break;
            }
        }

        // Store the last processed index to maintain round-robin fairness
        {
            let mut nodes_drain_index_guard = self.nodes_drain_index.lock();
            nodes_drain_index_guard.insert(node.id(), last_processed_index);
        }

        Ok(ready)
    }

    pub fn remove_incomplete_batches(&self, batch_id: i64) {
        self.incomplete_batches.write().remove(&batch_id);
    }

    fn record_actual_batch_size(&self, table_path: &Arc<PhysicalTablePath>, actual: usize) {
        let Some(entry) = self.write_batches.get(table_path) else {
            return;
        };
        let Some(estimator) = entry.dynamic_batch_size.as_ref() else {
            return;
        };
        let prev = estimator.current();
        let next = estimator.update(actual);
        if next != prev {
            log::debug!(
                "Set estimated batch size for {} from {} to {}",
                table_path.as_ref(),
                prev,
                next
            );
        }
    }

    #[cfg(test)]
    fn estimated_batch_size(&self, table_path: &Arc<PhysicalTablePath>) -> Option<usize> {
        self.write_batches
            .get(table_path)?
            .dynamic_batch_size
            .as_ref()
            .map(|est| est.current())
    }

    pub fn re_enqueue(&self, mut ready_write_batch: ReadyWriteBatch) {
        ready_write_batch.write_batch.re_enqueued();

        // Sync WriteBatch sequence with IdempotenceManager's adjusted sequence.
        // When handle_failed_batch adjusts InFlightBatch sequences (after a prior
        // batch fails), the WriteBatch is not updated (unlike Java which calls
        // resetWriterState on the actual batch). We must sync here so that:
        // 1. should_stop_drain_batches_for_bucket comparisons work correctly
        // 2. build() produces bytes with the correct (adjusted) sequence
        if self.idempotence_manager.is_enabled()
            && ready_write_batch.write_batch.has_batch_sequence()
        {
            if let Some(adjusted_seq) = self.idempotence_manager.get_adjusted_sequence(
                &ready_write_batch.table_bucket,
                ready_write_batch.write_batch.batch_id(),
            ) {
                if adjusted_seq != ready_write_batch.write_batch.batch_sequence() {
                    let writer_id = ready_write_batch.write_batch.writer_id();
                    ready_write_batch
                        .write_batch
                        .set_writer_state(writer_id, adjusted_seq);
                }
            }
        }

        let dq = self.get_or_create_deque(&ready_write_batch);
        let mut dq_guard = dq.lock();
        if self.idempotence_manager.is_enabled() {
            self.insert_in_sequence_order(&mut dq_guard, ready_write_batch);
        } else {
            dq_guard.push_front(ready_write_batch.write_batch);
        }
    }

    /// Insert a re-enqueued batch in sequence order. Matches Java's
    /// `insertInSequenceOrder`. If the batch is the next expected in-flight,
    /// push to front; otherwise, find the correct sorted position.
    fn insert_in_sequence_order(
        &self,
        dq: &mut VecDeque<WriteBatch>,
        ready_write_batch: ReadyWriteBatch,
    ) {
        debug_assert!(
            ready_write_batch.write_batch.batch_sequence() != NO_BATCH_SEQUENCE,
            "Re-enqueuing a batch without a sequence (batch_id={})",
            ready_write_batch.write_batch.batch_id()
        );
        debug_assert!(
            self.idempotence_manager
                .in_flight_count(&ready_write_batch.table_bucket)
                > 0,
            "Re-enqueuing a batch not tracked in in-flight (batch_id={}, bucket={})",
            ready_write_batch.write_batch.batch_id(),
            ready_write_batch.table_bucket
        );

        if dq.is_empty() {
            dq.push_front(ready_write_batch.write_batch);
            return;
        }

        // If it's the first in-flight batch for its bucket, push to front
        if self.idempotence_manager.is_first_in_flight_batch(
            &ready_write_batch.table_bucket,
            ready_write_batch.write_batch.batch_id(),
        ) {
            dq.push_front(ready_write_batch.write_batch);
            return;
        }

        // Find the correct position sorted by batch_sequence
        let batch_seq = ready_write_batch.write_batch.batch_sequence();
        let mut insert_pos = dq.len();
        for (i, existing) in dq.iter().enumerate() {
            if existing.has_batch_sequence() && existing.batch_sequence() > batch_seq {
                insert_pos = i;
                break;
            }
        }
        dq.insert(insert_pos, ready_write_batch.write_batch);
    }

    fn get_or_create_deque(
        &self,
        ready_write_batch: &ReadyWriteBatch,
    ) -> Arc<Mutex<VecDeque<WriteBatch>>> {
        let physical_table_path = ready_write_batch.write_batch.physical_table_path();
        let bucket_id = ready_write_batch.table_bucket.bucket_id();
        let table_id = ready_write_batch.table_bucket.table_id();
        let partition_id = ready_write_batch.table_bucket.partition_id();
        let is_partitioned_table = partition_id.is_some();

        let mut binding = self
            .write_batches
            .entry(Arc::clone(physical_table_path))
            .or_insert_with(|| {
                BucketAndWriteBatches::new(
                    table_id,
                    is_partitioned_table,
                    partition_id,
                    &self.config,
                )
            });
        let bucket_and_batches = binding.value_mut();
        bucket_and_batches
            .batches
            .entry(bucket_id)
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone()
    }

    /// Mark the accumulator as closed. All batches become immediately ready
    /// (sendable) in `batch_ready`, triggering a full drain without waiting
    /// for `batch_timeout_ms`. Matches Java's `RecordAccumulator.close()`.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.wakeup_sender();
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn abort_batches(&self, error: broadcast::Error) {
        self.memory_limiter.close();
        // Complete batches still in deques (not yet drained).
        for mut entry in self.write_batches.iter_mut() {
            for (_bucket_id, deque) in entry.value_mut().batches.iter_mut() {
                let mut dq = deque.lock();
                while let Some(batch) = dq.pop_front() {
                    batch.complete(Err(error.clone()));
                }
            }
        }
        // Fail any remaining handles (including in-flight batches that were
        // drained but not yet completed). This is a no-op for handles already
        // completed above via WriteBatch::complete.
        let mut incomplete = self.incomplete_batches.write();
        for (handle, _permit) in incomplete.values() {
            handle.fail(error.clone());
        }
        incomplete.clear();
    }

    pub fn has_incomplete(&self) -> bool {
        !self.incomplete_batches.read().is_empty()
    }

    /// Wake the sender task so it can drain ready batches immediately.
    pub fn wakeup_sender(&self) {
        self.sender_wakeup.notify_one();
    }

    /// Returns a future that completes when `wakeup_sender()` is called.
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.sender_wakeup.notified()
    }

    fn get_all_buckets_in_current_node(
        &self,
        current: &ServerNode,
        cluster: &Cluster,
    ) -> Vec<BucketLocation> {
        let mut buckets = vec![];
        for bucket_locations in cluster.get_bucket_locations_by_path().values() {
            for bucket_location in bucket_locations {
                if let Some(leader) = bucket_location.leader() {
                    if current.id() == leader.id() {
                        buckets.push(bucket_location.clone());
                    }
                }
            }
        }
        buckets
    }

    pub fn has_undrained(&self) -> bool {
        for entry in self.write_batches.iter() {
            for (_, batch_deque) in entry.value().batches.iter() {
                if !batch_deque.lock().is_empty() {
                    return true;
                }
            }
        }
        false
    }

    pub fn get_physical_table_paths_in_batches(&self) -> Vec<Arc<PhysicalTablePath>> {
        self.write_batches
            .iter()
            .map(|entry| Arc::clone(entry.key()))
            .collect()
    }

    fn flush_in_progress(&self) -> bool {
        self.flushes_in_progress.load(Ordering::SeqCst) > 0
    }

    pub fn begin_flush(&self) {
        self.flushes_in_progress.fetch_add(1, Ordering::SeqCst);
        self.wakeup_sender();
    }

    #[allow(unused_must_use)]
    pub async fn await_flush_completion(&self) -> Result<()> {
        // Clone handles before awaiting to avoid holding RwLock read guard across await points
        let handles: Vec<_> = self
            .incomplete_batches
            .read()
            .values()
            .map(|(h, _)| h.clone())
            .collect();

        // Await on all handles
        let result = async {
            for result_handle in handles {
                result_handle.wait().await?;
            }
            Ok(())
        }
        .await;

        // Always decrement flushes_in_progress, even if an error occurred
        // This mimics the Java finally block behavior
        self.flushes_in_progress.fetch_sub(1, Ordering::SeqCst);

        result
    }
}

pub struct ReadyWriteBatch {
    pub table_bucket: TableBucket,
    pub write_batch: WriteBatch,
}

impl ReadyWriteBatch {
    pub fn write_batch(&self) -> &WriteBatch {
        &self.write_batch
    }
}

#[allow(dead_code)]
struct BucketAndWriteBatches {
    table_id: TableId,
    is_partitioned_table: bool,
    partition_id: Option<PartitionId>,
    batches: HashMap<BucketId, Arc<Mutex<VecDeque<WriteBatch>>>>,
    /// Compression ratio estimator shared across Arrow log batches for this table.
    compression_ratio_estimator: Arc<ArrowCompressionRatioEstimator>,
    /// `None` when `writer_dynamic_batch_size_enabled` is false.
    dynamic_batch_size: Option<DynamicWriteBatchSizeEstimator>,
}

impl BucketAndWriteBatches {
    fn new(
        table_id: TableId,
        is_partitioned_table: bool,
        partition_id: Option<PartitionId>,
        config: &Config,
    ) -> Self {
        let dynamic_batch_size = config.writer_dynamic_batch_size_enabled.then(|| {
            DynamicWriteBatchSizeEstimator::new(
                config.writer_dynamic_batch_size_min as usize,
                config.writer_batch_size as usize,
            )
        });
        Self {
            table_id,
            is_partitioned_table,
            partition_id,
            batches: Default::default(),
            compression_ratio_estimator: Arc::new(ArrowCompressionRatioEstimator::default()),
            dynamic_batch_size,
        }
    }
}

pub struct RecordAppendResult {
    pub batch_is_full: bool,
    pub new_batch_created: bool,
    pub abort_record_for_new_batch: bool,
    pub result_handle: Option<ResultHandle>,
}

impl RecordAppendResult {
    fn new(
        result_handle: ResultHandle,
        batch_is_full: bool,
        new_batch_created: bool,
        abort_record_for_new_batch: bool,
    ) -> Self {
        Self {
            batch_is_full,
            new_batch_created,
            abort_record_for_new_batch,
            result_handle: Some(result_handle),
        }
    }

    fn new_without_result_handle(
        batch_is_full: bool,
        new_batch_created: bool,
        abort_record_for_new_batch: bool,
    ) -> Self {
        Self {
            batch_is_full,
            new_batch_created,
            abort_record_for_new_batch,
            result_handle: None,
        }
    }
}

pub struct ReadyCheckResult {
    pub ready_nodes: HashSet<ServerNode>,
    pub next_ready_check_delay_ms: i64,
    pub unknown_leader_tables: HashSet<Arc<PhysicalTablePath>>,
}

impl ReadyCheckResult {
    pub fn new(
        ready_nodes: HashSet<ServerNode>,
        next_ready_check_delay_ms: i64,
        unknown_leader_tables: HashSet<Arc<PhysicalTablePath>>,
    ) -> Self {
        ReadyCheckResult {
            ready_nodes,
            next_ready_check_delay_ms,
            unknown_leader_tables,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::write::write_format::WriteFormat;
    use crate::client::write::{RowBytes, WriteRecord};
    use crate::metadata::TablePath;
    use crate::row::{Datum, GenericRow};
    use crate::test_utils::{build_cluster, build_table_info};
    use bytes::Bytes;
    use std::sync::Arc;

    fn disabled_idempotence() -> Arc<IdempotenceManager> {
        Arc::new(IdempotenceManager::new(false, 5))
    }

    fn enabled_idempotence() -> Arc<IdempotenceManager> {
        Arc::new(IdempotenceManager::new(true, 5))
    }

    #[tokio::test]
    async fn re_enqueue_increments_attempts() -> Result<()> {
        let config = Config::default();
        let accumulator = RecordAccumulator::new(config, disabled_idempotence());
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);

        accumulator.append(&record, 0, &cluster, false)?;

        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator.drain(cluster.clone(), &nodes, 1024 * 1024)?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        assert_eq!(batch.write_batch.attempts(), 0);

        accumulator.re_enqueue(batch);

        let mut batches = accumulator.drain(cluster, &nodes, 1024 * 1024)?;
        let mut drained = batches.remove(&1).expect("drained batches");
        let batch = drained.pop().expect("batch");
        assert_eq!(batch.write_batch.attempts(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn flush_counter_decremented_on_error() -> Result<()> {
        use crate::client::write::broadcast::BroadcastOnce;
        use std::sync::atomic::Ordering;

        let config = Config::default();
        let accumulator = RecordAccumulator::new(config, disabled_idempotence());

        accumulator.begin_flush();
        assert_eq!(accumulator.flushes_in_progress.load(Ordering::SeqCst), 1);

        // Create a failing batch by dropping the BroadcastOnce without broadcasting
        {
            let broadcast = BroadcastOnce::default();
            let receiver = broadcast.receiver();
            let handle = ResultHandle::new(receiver);
            let permit = accumulator.memory_limiter.acquire(1024).unwrap();
            accumulator
                .incomplete_batches
                .write()
                .insert(1, (handle, permit));
            // broadcast is dropped here, causing an error
        }

        // Await flush completion should fail but still decrement counter
        let result = accumulator.await_flush_completion().await;
        assert!(result.is_err());

        // Counter should still be decremented (this is the critical fix!)
        assert_eq!(accumulator.flushes_in_progress.load(Ordering::SeqCst), 0);
        assert!(!accumulator.flush_in_progress());

        Ok(())
    }

    fn append_and_drain(
        accumulator: &RecordAccumulator,
        cluster: &Arc<crate::cluster::Cluster>,
        table_path: &TablePath,
        bucket_id: i32,
    ) -> Result<ReadyWriteBatch> {
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 2));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);
        accumulator.append(&record, bucket_id, cluster, false)?;
        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator.drain(cluster.clone(), &nodes, 1024 * 1024)?;
        let mut drained = batches.remove(&1).expect("drained batches");
        Ok(drained.pop().expect("batch"))
    }

    #[test]
    fn test_should_stop_drain_for_fresh_batch_over_limit() {
        let idempotence = Arc::new(IdempotenceManager::new(true, 2));
        idempotence.set_writer_id(42);
        let config = Config::default();
        let accumulator = RecordAccumulator::new(config, Arc::clone(&idempotence));
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);
        accumulator
            .append(&record, 0, &cluster, false)
            .expect("append");

        let table_bucket = TableBucket::new(1, 0);

        // Add 2 in-flight batches (reaching the max_in_flight=2)
        idempotence.add_in_flight_batch(&table_bucket, 0, 100);
        idempotence.add_in_flight_batch(&table_bucket, 1, 101);

        // Get the front batch from the deque
        let entry = accumulator
            .write_batches
            .get(&PhysicalTablePath::of(Arc::new(table_path)))
            .unwrap();
        let dq = entry.batches.get(&0).unwrap();
        let dq_guard = dq.lock();
        let first_batch = dq_guard.front().unwrap();

        // Fresh batch (no batch_sequence) with in-flight at limit → should stop
        assert!(!first_batch.has_batch_sequence());
        assert!(accumulator.should_stop_drain_batches_for_bucket(first_batch, &table_bucket));

        // Remove one in-flight → under limit → should not stop
        drop(dq_guard);
        idempotence.remove_in_flight_batch(&table_bucket, 101);
        let dq_guard = entry.batches.get(&0).unwrap().lock();
        let first_batch = dq_guard.front().unwrap();
        assert!(!accumulator.should_stop_drain_batches_for_bucket(first_batch, &table_bucket));
    }

    #[test]
    fn test_should_stop_drain_for_retry_not_first_inflight() {
        let idempotence = enabled_idempotence();
        idempotence.set_writer_id(42);
        let config = Config::default();
        let accumulator = RecordAccumulator::new(config, Arc::clone(&idempotence));
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));

        // Drain two separate batches to get batch0(seq=0) and batch1(seq=1)
        let batch0 =
            append_and_drain(&accumulator, &cluster, &table_path, 0).expect("drain batch0");
        let batch1 =
            append_and_drain(&accumulator, &cluster, &table_path, 0).expect("drain batch1");

        assert_eq!(batch0.write_batch.batch_sequence(), 0);
        assert_eq!(batch1.write_batch.batch_sequence(), 1);

        let batch1_id = batch1.write_batch.batch_id();
        let table_bucket = batch0.table_bucket.clone();

        // Re-enqueue only batch1 (simulating batch0 still in-flight, batch1 got error)
        accumulator.re_enqueue(batch1);

        let entry = accumulator
            .write_batches
            .get(&PhysicalTablePath::of(Arc::new(table_path)))
            .unwrap();
        let dq = entry.batches.get(&0).unwrap();
        let dq_guard = dq.lock();
        let first_batch = dq_guard.front().unwrap();

        // Batch1 is re-enqueued with seq=1, but batch0 (seq=0) is the first in-flight.
        // batch1's batch_id != first in-flight batch_id → should stop.
        assert!(first_batch.has_batch_sequence());
        assert_eq!(first_batch.batch_id(), batch1_id);
        assert!(accumulator.should_stop_drain_batches_for_bucket(first_batch, &table_bucket));
    }

    #[tokio::test]
    async fn test_insert_in_sequence_order() -> Result<()> {
        let idempotence = enabled_idempotence();
        idempotence.set_writer_id(42);
        let config = Config::default();
        let accumulator = RecordAccumulator::new(config, Arc::clone(&idempotence));
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = Arc::new(build_cluster(&table_path, 1, 2));

        // Create and drain 3 batches to get them with sequences 0, 1, 2
        let batch0 = append_and_drain(&accumulator, &cluster, &table_path, 0)?;
        let batch1 = append_and_drain(&accumulator, &cluster, &table_path, 0)?;
        let batch2 = append_and_drain(&accumulator, &cluster, &table_path, 0)?;

        assert_eq!(batch0.write_batch.batch_sequence(), 0);
        assert_eq!(batch1.write_batch.batch_sequence(), 1);
        assert_eq!(batch2.write_batch.batch_sequence(), 2);

        let batch0_id = batch0.write_batch.batch_id();
        let batch1_id = batch1.write_batch.batch_id();
        let batch2_id = batch2.write_batch.batch_id();
        let table_bucket = batch0.table_bucket.clone();

        // Re-enqueue in reverse order: 2, 0, 1
        // insert_in_sequence_order should sort them as: 0, 1, 2
        accumulator.re_enqueue(batch2);
        accumulator.re_enqueue(batch0);
        accumulator.re_enqueue(batch1);

        // Verify the deque order directly
        let entry = accumulator
            .write_batches
            .get(&PhysicalTablePath::of(Arc::new(table_path)))
            .unwrap();
        let dq = entry.batches.get(&0).unwrap();
        let dq_guard = dq.lock();
        assert_eq!(dq_guard.len(), 3);
        // batch0 (seq=0) is the first in-flight, so it should be at front
        assert_eq!(dq_guard[0].batch_id(), batch0_id);
        assert_eq!(dq_guard[0].batch_sequence(), 0);
        assert_eq!(dq_guard[1].batch_id(), batch1_id);
        assert_eq!(dq_guard[1].batch_sequence(), 1);
        assert_eq!(dq_guard[2].batch_id(), batch2_id);
        assert_eq!(dq_guard[2].batch_sequence(), 2);
        drop(dq_guard);

        // Drain: first in-flight is seq=0, so batch0 passes should_stop check
        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut batches = accumulator.drain(cluster.clone(), &nodes, 1024 * 1024)?;
        let drained = batches.remove(&1).expect("drained batches");
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].write_batch.batch_sequence(), 0);

        // Complete batch0 so batch1 becomes first in-flight
        idempotence.handle_completed_batch(&table_bucket, batch0_id, 42);

        let mut batches = accumulator.drain(cluster.clone(), &nodes, 1024 * 1024)?;
        let drained = batches.remove(&1).expect("drained");
        assert_eq!(drained[0].write_batch.batch_sequence(), 1);

        idempotence.handle_completed_batch(&table_bucket, batch1_id, 42);

        let mut batches = accumulator.drain(cluster, &nodes, 1024 * 1024)?;
        let drained = batches.remove(&1).expect("drained");
        assert_eq!(drained[0].write_batch.batch_sequence(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_abort_batches() -> Result<()> {
        let idempotence = disabled_idempotence();
        let config = Config::default();
        let accumulator = RecordAccumulator::new(config, Arc::clone(&idempotence));
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);

        let result = accumulator.append(&record, 0, &cluster, false)?;
        let handle = result.result_handle.expect("handle");
        assert!(accumulator.has_incomplete());

        accumulator.abort_batches(broadcast::Error::Client {
            message: "test abort".to_string(),
        });

        assert!(!accumulator.has_incomplete());
        assert!(!accumulator.has_undrained());

        // The handle should receive the error
        let batch_result = handle.wait().await?;
        assert!(matches!(
            batch_result,
            Err(broadcast::Error::Client { message }) if message == "test abort"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_drain_skips_blocked_bucket_continues_others() -> Result<()> {
        // Use max_in_flight=1 so that one in-flight batch blocks further draining
        let idempotence = Arc::new(IdempotenceManager::new(true, 1));
        idempotence.set_writer_id(42);
        let config = Config::default();
        let accumulator = RecordAccumulator::new(config, Arc::clone(&idempotence));
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = Arc::new(build_cluster(&table_path, 1, 2));

        // Append to both buckets
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 2));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };

        // Append to bucket 0
        let record =
            WriteRecord::for_append(table_info.clone(), physical_table_path.clone(), 1, &row);
        accumulator.append(&record, 0, &cluster, false)?;

        // Append to bucket 1
        let record =
            WriteRecord::for_append(table_info.clone(), physical_table_path.clone(), 1, &row);
        accumulator.append(&record, 1, &cluster, false)?;

        // Drain once — both buckets get batches assigned with sequences
        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let batches = accumulator.drain(cluster.clone(), &nodes, 1024 * 1024)?;
        let drained = batches.get(&1).expect("drained");
        // Both buckets should produce batches
        assert_eq!(drained.len(), 2);

        // Now: both buckets have 1 in-flight each (added during drain).
        // Append another record to each bucket.
        let record =
            WriteRecord::for_append(table_info.clone(), physical_table_path.clone(), 1, &row);
        accumulator.append(&record, 0, &cluster, false)?;
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);
        accumulator.append(&record, 1, &cluster, false)?;

        // With max_in_flight=1, both buckets are at limit → should_stop returns true
        // for fresh batches. The drain should skip both (continue, not break).
        let batches2 = accumulator.drain(cluster.clone(), &nodes, 1024 * 1024)?;
        // No batches should be drained (both blocked)
        assert!(
            batches2.is_empty() || batches2.get(&1).is_none_or(|b| b.is_empty()),
            "Expected no batches when all buckets are blocked"
        );

        // Complete the in-flight for bucket 0
        let bucket0_batch = &drained[0];
        idempotence.handle_completed_batch(
            &bucket0_batch.table_bucket,
            bucket0_batch.write_batch.batch_id(),
            42,
        );

        // Now bucket 0 is unblocked but bucket 1 is still blocked
        let batches3 = accumulator.drain(cluster, &nodes, 1024 * 1024)?;
        let drained3 = batches3.get(&1).expect("some drained");
        // Only bucket 0 should produce a batch (continue skipped bucket 1)
        assert_eq!(drained3.len(), 1);
        assert_eq!(drained3[0].table_bucket.bucket_id(), 0);

        Ok(())
    }

    #[test]
    fn test_memory_limiter_acquire_release() {
        let limiter = Arc::new(MemoryLimiter::new(1024, Duration::from_secs(1)));

        let permit1 = limiter.acquire(512).unwrap();
        let permit2 = limiter.acquire(512).unwrap();

        // At capacity — verify used is 1024
        assert_eq!(*limiter.state.lock(), 1024);

        // Release one permit, verify used drops
        drop(permit1);
        assert_eq!(*limiter.state.lock(), 512);

        drop(permit2);
        assert_eq!(*limiter.state.lock(), 0);
    }

    #[test]
    fn test_memory_limiter_oversized_batch_fails_immediately() {
        let limiter = Arc::new(MemoryLimiter::new(1024, Duration::from_secs(60)));

        let result = limiter.acquire(2048);
        assert!(matches!(result.unwrap_err(), Error::IllegalArgument { .. }));
    }

    #[test]
    fn test_memory_limiter_blocks_then_unblocks() {
        let limiter = Arc::new(MemoryLimiter::new(1024, Duration::from_secs(5)));

        let permit = limiter.acquire(1024).unwrap();
        assert_eq!(*limiter.state.lock(), 1024);

        // Spawn a thread that tries to acquire — it should block
        let limiter2 = Arc::clone(&limiter);
        let handle = std::thread::spawn(move || limiter2.acquire(512));

        // Give the thread time to block
        std::thread::sleep(Duration::from_millis(50));
        // Still at capacity (thread is blocked)
        assert_eq!(*limiter.state.lock(), 1024);

        // Release the permit — thread should unblock
        drop(permit);

        let result = handle.join().unwrap();
        assert!(result.is_ok());
        let _permit2 = result.unwrap();
        assert_eq!(*limiter.state.lock(), 512);
    }

    #[test]
    fn test_memory_limiter_timeout() {
        let limiter = Arc::new(MemoryLimiter::new(1024, Duration::from_millis(100)));

        let _permit = limiter.acquire(1024).unwrap();

        // This should timeout
        let start = Instant::now();
        let result = limiter.acquire(512);
        let elapsed = start.elapsed();

        assert!(matches!(result.unwrap_err(), Error::BufferExhausted { .. }));
        assert!(elapsed >= Duration::from_millis(80)); // allow some timing slack
    }

    #[test]
    fn test_memory_limiter_close_fails_immediately() {
        let limiter = Arc::new(MemoryLimiter::new(1024, Duration::from_secs(60)));

        let _permit = limiter.acquire(512).unwrap();

        limiter.close();

        // New acquire should fail immediately, not block for 60s
        let start = Instant::now();
        let result = limiter.acquire(256);
        let elapsed = start.elapsed();

        assert!(matches!(result.unwrap_err(), Error::WriterClosed { .. }));
        assert!(elapsed < Duration::from_millis(50));
    }

    #[test]
    fn test_memory_limiter_close_unblocks_waiting_threads() {
        let limiter = Arc::new(MemoryLimiter::new(1024, Duration::from_secs(60)));

        // Fill the limiter completely
        let _permit = limiter.acquire(1024).unwrap();

        // Spawn a thread that blocks waiting for memory
        let limiter2 = Arc::clone(&limiter);
        let handle = std::thread::spawn(move || {
            let start = Instant::now();
            let result = limiter2.acquire(512);
            (result, start.elapsed())
        });

        // Give the thread time to block
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(limiter.waiting_count.load(Ordering::Relaxed), 1);

        // Close the limiter — should unblock the waiting thread
        limiter.close();

        let (result, elapsed) = handle.join().unwrap();
        assert!(matches!(result.unwrap_err(), Error::WriterClosed { .. }));
        assert!(elapsed < Duration::from_secs(5)); // should not wait the full 60s
    }

    #[test]
    fn test_oversized_kv_record_does_not_panic() {
        use crate::client::write::write_format::WriteFormat;
        use crate::client::write::{RowBytes, WriteRecord};
        use bytes::Bytes;

        // Use a tiny batch size so the KV record exceeds it
        let config = Config {
            writer_batch_size: 64,
            writer_buffer_memory_size: 1024 * 1024,
            ..Config::default()
        };

        let accumulator = RecordAccumulator::new(config, disabled_idempotence());
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));

        // Create a KV record larger than batch_size (64 bytes)
        let key = Bytes::from(vec![0u8; 32]);
        let value = vec![0u8; 256];
        let record = WriteRecord::for_upsert(
            table_info,
            physical_table_path,
            1,
            key,
            None,
            WriteFormat::CompactedKv,
            None,
            Some(RowBytes::Owned(Bytes::from(value))),
        );

        // This used to panic with "must append to a new batch" because
        // the KV write limit was hardcoded to DEFAULT_WRITE_LIMIT (256 bytes)
        // instead of using alloc_size = max(batch_size, record_size).
        let result = accumulator.append(&record, 0, &cluster, false);
        assert!(result.is_ok(), "oversized KV record should not panic");
    }

    #[test]
    fn test_memory_permit_accounts_for_oversized_record() {
        use crate::client::write::write_format::WriteFormat;
        use crate::client::write::{RowBytes, WriteRecord};
        use bytes::Bytes;

        let config = Config {
            writer_batch_size: 64,
            writer_buffer_memory_size: 1024 * 1024,
            ..Config::default()
        };

        let accumulator = RecordAccumulator::new(config, disabled_idempotence());
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));

        let key = Bytes::from(vec![0u8; 32]);
        let value = vec![0u8; 256];
        let record = WriteRecord::for_upsert(
            table_info,
            physical_table_path,
            1,
            key,
            None,
            WriteFormat::CompactedKv,
            None,
            Some(RowBytes::Owned(Bytes::from(value))),
        );

        // estimated_record_size includes batch header overhead
        let expected_alloc = record.estimated_record_size();
        assert!(expected_alloc > 64, "record should exceed batch_size=64");

        accumulator.append(&record, 0, &cluster, false).unwrap();

        // The permit should reserve max(batch_size, estimated_record_size) bytes.
        let used = *accumulator.memory_limiter.state.lock();
        assert_eq!(
            used, expected_alloc,
            "memory limiter should reserve max(batch_size, estimated_record_size)"
        );
    }

    #[tokio::test]
    async fn test_sender_wakeup_notifies() {
        let accumulator = RecordAccumulator::new(Config::default(), disabled_idempotence());

        // notified() should complete when wakeup_sender() is called
        let notified = accumulator.notified();
        accumulator.wakeup_sender();
        // If wakeup doesn't work, this would hang forever.
        tokio::time::timeout(Duration::from_millis(100), notified)
            .await
            .expect("notified should complete after wakeup_sender");
    }

    #[test]
    fn dynamic_batch_size_shrinks_after_small_drained_batch() {
        let target = 256 * 1024;
        let config = Config {
            writer_dynamic_batch_size_enabled: true,
            writer_batch_size: target,
            writer_dynamic_batch_size_min: 4 * 1024,
            writer_buffer_memory_size: 1024 * 1024,
            ..Config::default()
        };
        let accumulator = RecordAccumulator::new(config, disabled_idempotence());
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);

        accumulator.append(&record, 0, &cluster, false).unwrap();
        assert_eq!(*accumulator.memory_limiter.state.lock(), target as usize);

        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        let mut drained = accumulator
            .drain(cluster.clone(), &nodes, 1024 * 1024)
            .unwrap();
        let mut batches = drained.remove(&1).expect("drained batches");
        let batch = batches.pop().expect("batch");
        accumulator.remove_incomplete_batches(batch.write_batch.batch_id());
        assert_eq!(*accumulator.memory_limiter.state.lock(), 0);

        accumulator.append(&record, 0, &cluster, false).unwrap();
        let second = *accumulator.memory_limiter.state.lock();
        assert!(second < target as usize, "{second} >= {target}");
    }

    #[test]
    fn dynamic_batch_size_grows_after_full_drained_batch() {
        let max = 256 * 1024;
        let config = Config {
            writer_dynamic_batch_size_enabled: true,
            writer_batch_size: max,
            writer_dynamic_batch_size_min: 4 * 1024,
            writer_buffer_memory_size: 4 * 1024 * 1024,
            ..Config::default()
        };
        let accumulator = RecordAccumulator::new(config, disabled_idempotence());
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));
        let nodes = HashSet::from([cluster.get_tablet_server(1).unwrap().clone()]);

        let kv = |size: usize| {
            WriteRecord::for_upsert(
                Arc::clone(&table_info),
                Arc::clone(&physical_table_path),
                1,
                Bytes::from(vec![0u8; 32]),
                None,
                WriteFormat::CompactedKv,
                None,
                Some(RowBytes::Owned(Bytes::from(vec![0u8; size]))),
            )
        };
        let drain_one = || {
            let mut d = accumulator.drain(cluster.clone(), &nodes, max).unwrap();
            let b = d.remove(&1).unwrap().pop().unwrap();
            accumulator.remove_incomplete_batches(b.write_batch.batch_id());
        };
        let target = || {
            accumulator
                .estimated_batch_size(&physical_table_path)
                .unwrap()
        };

        accumulator.append(&kv(1), 0, &cluster, false).unwrap();
        drain_one();
        let after_shrink = target();
        assert!(
            after_shrink < max as usize,
            "shrink failed: after_shrink={after_shrink} max={max}"
        );

        // 0.9 sits safely above GROW_THRESHOLD (0.8) to avoid f64 boundary noise.
        accumulator
            .append(&kv(after_shrink * 9 / 10), 0, &cluster, false)
            .unwrap();
        drain_one();
        let after_grow = target();
        assert!(
            after_grow > after_shrink,
            "grow failed: after_grow={after_grow} after_shrink={after_shrink}"
        );
    }

    #[test]
    fn dynamic_batch_size_disabled_keeps_static_target() {
        let target = 256 * 1024;
        let config = Config {
            writer_dynamic_batch_size_enabled: false,
            writer_batch_size: target,
            writer_dynamic_batch_size_min: 4 * 1024,
            writer_buffer_memory_size: 1024 * 1024,
            ..Config::default()
        };
        let accumulator = RecordAccumulator::new(config, disabled_idempotence());
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let cluster = Arc::new(build_cluster(&table_path, 1, 1));
        let row = GenericRow {
            values: vec![Datum::Int32(1)],
        };
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);

        let server = cluster.get_tablet_server(1).expect("server");
        let nodes = HashSet::from([server.clone()]);
        for _ in 0..3 {
            accumulator.append(&record, 0, &cluster, false).unwrap();
            assert_eq!(*accumulator.memory_limiter.state.lock(), target as usize);

            let mut drained = accumulator
                .drain(cluster.clone(), &nodes, 1024 * 1024)
                .unwrap();
            let mut batches = drained.remove(&1).expect("drained batches");
            let batch = batches.pop().expect("batch");
            accumulator.remove_incomplete_batches(batch.write_batch.batch_id());
        }
    }
}
