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

use super::leader_waits::{LeaderRefresh, LeaderWaits};
use super::{LookupQueue, QueuedLookup};
use crate::client::lookup::lookup_query::LookupQuery;
use crate::client::metadata::Metadata;
use crate::error::{Error, FlussError, Result};
use crate::metadata::{TableBucket, TablePath};
use crate::proto::{LookupResponse, PrefixLookupResponse};
use crate::rpc::message::{
    BucketLookupKeys, LookupRequest, PrefixLookupRequest, ReadType, RequestBody, WriteType,
};
use crate::rpc::{ApiError, ServerConnection};
use crate::util::retry_backoff_ms;
use crate::{BucketId, PartitionId, TableId};
use bytes::Bytes;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;

type ServerId = i32;

type BatchesByLeader<T> = HashMap<ServerId, HashMap<TableBucket, LookupBatch<T>>>;
type PrimaryBatches = BatchesByLeader<Option<Vec<u8>>>;
type PrefixBatches = BatchesByLeader<Vec<Vec<u8>>>;

struct BucketResponse<V> {
    partition_id: Option<PartitionId>,
    bucket_id: BucketId,
    error_code: Option<i32>,
    error_message: Option<String>,
    values: Vec<V>,
}

trait LookupProtocol {
    type Request: RequestBody<ResponseBody = Self::Response> + Send + WriteType<Vec<u8>>;
    type Response: ReadType<Cursor<Vec<u8>>> + Send;
    type Value: Send + 'static;

    const OP_NAME: &'static str;

    fn build_request(table_id: TableId, keys_by_bucket: Vec<BucketLookupKeys>) -> Self::Request;

    fn decode_buckets(
        response: Self::Response,
    ) -> impl Iterator<Item = BucketResponse<Self::Value>>;
}

struct Primary;
impl LookupProtocol for Primary {
    type Request = LookupRequest;
    type Response = LookupResponse;
    type Value = Option<Vec<u8>>;

    const OP_NAME: &'static str = "Lookup";

    fn build_request(table_id: TableId, keys_by_bucket: Vec<BucketLookupKeys>) -> Self::Request {
        LookupRequest::new_batched(table_id, keys_by_bucket)
    }

    fn decode_buckets(
        response: Self::Response,
    ) -> impl Iterator<Item = BucketResponse<Self::Value>> {
        response.buckets_resp.into_iter().map(|r| BucketResponse {
            partition_id: r.partition_id,
            bucket_id: r.bucket_id,
            error_code: r.error_code,
            error_message: r.error_message,
            values: r.values.into_iter().map(|pb| pb.values).collect(),
        })
    }
}

struct Prefix;
impl LookupProtocol for Prefix {
    type Request = PrefixLookupRequest;
    type Response = PrefixLookupResponse;
    type Value = Vec<Vec<u8>>;

    const OP_NAME: &'static str = "Prefix lookup";

    fn build_request(table_id: TableId, keys_by_bucket: Vec<BucketLookupKeys>) -> Self::Request {
        PrefixLookupRequest::new_batched(table_id, keys_by_bucket)
    }

    fn decode_buckets(
        response: Self::Response,
    ) -> impl Iterator<Item = BucketResponse<Self::Value>> {
        response.buckets_resp.into_iter().map(|r| BucketResponse {
            partition_id: r.partition_id,
            bucket_id: r.bucket_id,
            error_code: r.error_code,
            error_message: r.error_message,
            values: r.value_lists.into_iter().map(|pb| pb.values).collect(),
        })
    }
}

struct GroupingResult {
    primary: PrimaryBatches,
    prefix: PrefixBatches,
    leaderless_lookups: Vec<QueuedLookup>,
}

pub struct LookupSender {
    queue: LookupQueue,
    dispatcher: Arc<LookupDispatcher>,
    tasks: SenderTasks,
    running: AtomicBool,
    force_close: AtomicBool,
    shutdown_rx: watch::Receiver<bool>,
}

/// The tasks of a sender, aborted once it is dropped.
#[derive(Default)]
struct SenderTasks {
    /// Requests in flight, each ending once its response is handled.
    requests: JoinSet<()>,
    /// Runs [`LookupDispatcher::refresh_leaders`] while lookups wait for a leader.
    leader_refresher: JoinSet<()>,
}

/// Sends lookups to their leaders and completes, retries or fails them. Shared by the sender and
/// its tasks.
///
/// A lookup not yet completed is held by exactly one of: the queue, a request in flight, or
/// `leader_waits`. Requests and the leader refresher hand lookups back through the queue.
struct LookupDispatcher {
    metadata: Arc<Metadata>,
    re_enqueue_tx: mpsc::UnboundedSender<QueuedLookup>,
    inflight_semaphore: Arc<Semaphore>,
    /// Lookups whose bucket has no cached leader, held until [`Self::refresh_leaders`] finds it.
    leader_waits: Mutex<LeaderWaits>,
    /// Wakes the leader refresher before the refresh it planned: a bucket started to wait, or the
    /// sender is closing.
    leader_refresh_due: Notify,
    /// Set once the sender stops taking new lookups. From then on, no lookup is retried, and
    /// lookups whose bucket still has no leader after one more refresh fail, so closing ends.
    closing: AtomicBool,
    max_retries: i32,
    retry_backoff_ms: u64,
    retry_max_backoff_ms: u64,
}

struct LookupBatch<T> {
    table_bucket: TableBucket,
    lookups: Vec<LookupQuery<T>>,
    keys: Vec<Bytes>,
}

impl<T> LookupBatch<T> {
    fn new(table_bucket: TableBucket) -> Self {
        Self {
            table_bucket,
            lookups: Vec::new(),
            keys: Vec::new(),
        }
    }

    fn add_lookup(&mut self, lookup: LookupQuery<T>) {
        self.keys.push(lookup.key().clone());
        self.lookups.push(lookup);
    }

    fn complete(&mut self, values: Vec<T>) {
        if values.len() != self.lookups.len() {
            let err_msg = format!(
                "The number of return values ({}) does not match the number of lookups ({})",
                values.len(),
                self.lookups.len()
            );
            for lookup in &mut self.lookups {
                lookup.complete_with_error(Error::UnexpectedError {
                    message: err_msg.clone(),
                    source: None,
                });
            }
            return;
        }

        for (lookup, value) in self.lookups.iter_mut().zip(values) {
            lookup.complete(Ok(value));
        }
    }

    fn complete_all_with_error(&mut self, make_error: impl Fn() -> Error) {
        for lookup in &mut self.lookups {
            lookup.complete_with_error(make_error());
        }
    }

    fn keys_tuple(&mut self) -> BucketLookupKeys {
        // All lookups of a bucket share one routing count.
        let routing_bucket_count = self
            .lookups
            .first()
            .map(|lookup| lookup.routing_bucket_count())
            .filter(|count| *count > 0);
        (
            self.table_bucket.bucket_id(),
            self.table_bucket.partition_id(),
            routing_bucket_count,
            std::mem::take(&mut self.keys),
        )
    }
}

impl LookupSender {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metadata: Arc<Metadata>,
        queue: LookupQueue,
        re_enqueue_tx: mpsc::UnboundedSender<QueuedLookup>,
        max_inflight_requests: usize,
        max_waiting_per_bucket: usize,
        max_retries: i32,
        retry_backoff_ms: u64,
        retry_max_backoff_ms: u64,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            queue,
            dispatcher: Arc::new(LookupDispatcher {
                metadata,
                re_enqueue_tx,
                inflight_semaphore: Arc::new(Semaphore::new(max_inflight_requests)),
                leader_waits: Mutex::new(LeaderWaits::new(
                    max_waiting_per_bucket,
                    retry_backoff_ms,
                    retry_max_backoff_ms,
                )),
                leader_refresh_due: Notify::new(),
                closing: AtomicBool::new(false),
                max_retries,
                retry_backoff_ms,
                retry_max_backoff_ms,
            }),
            tasks: SenderTasks::default(),
            running: AtomicBool::new(true),
            force_close: AtomicBool::new(false),
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) {
        debug!("Starting Fluss lookup sender");

        let mut shutdown_rx = self.shutdown_rx.clone();

        while self.running.load(Ordering::Acquire) {
            if *shutdown_rx.borrow() {
                debug!("Lookup sender received shutdown signal");
                self.initiate_close();
                break;
            }

            let lookups = tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        self.initiate_close();
                    }
                    continue;
                }
                lookups = self.queue.drain() => lookups,
            };
            let dispatcher = Arc::clone(&self.dispatcher);
            let shutdown = {
                let sending = self.send_lookups(lookups);
                tokio::pin!(sending);
                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        let shutdown = changed.is_err() || *shutdown_rx.borrow();
                        if shutdown {
                            dispatcher.start_closing();
                        }
                        // Sending may hold batches waiting for a permit. Finish handing them to
                        // the request tasks before closing.
                        sending.await;
                        shutdown
                    }
                    () = &mut sending => false,
                }
            };
            if shutdown {
                self.initiate_close();
            }
        }

        debug!("Beginning shutdown of lookup sender, sending remaining lookups");

        if !self.force_close.load(Ordering::Acquire) {
            self.close_gracefully().await;
        }

        // TODO: If force close failed, add logic to abort incomplete lookup requests.
        debug!("Lookup sender shutdown complete");
    }

    async fn send_lookups(&mut self, lookups: Vec<QueuedLookup>) {
        while self.tasks.requests.try_join_next().is_some() {}
        while self.tasks.leader_refresher.try_join_next().is_some() {}
        self.dispatcher.send_lookups(lookups, &mut self.tasks).await
    }

    /// Sends the lookups left once the sender stopped taking new ones, and returns once every
    /// lookup queued, in flight or waiting for a leader has completed.
    async fn close_gracefully(&mut self) {
        self.dispatcher.start_closing();
        loop {
            if self.queue.has_undrained() {
                let lookups = self.queue.drain_all();
                self.dispatcher.send_lookups(lookups, &mut self.tasks).await;
            }
            let SenderTasks {
                requests,
                leader_refresher,
            } = &mut self.tasks;
            if requests.is_empty() && leader_refresher.is_empty() {
                break;
            }
            // The leader refresher puts back the lookups whose leader it finds while it goes on
            // refreshing other buckets, so those are sent without waiting for it to end.
            tokio::select! {
                Some(_) = requests.join_next() => {}
                Some(_) = leader_refresher.join_next() => {}
                _ = self.queue.wait_for_re_enqueued() => {}
            }
        }
    }

    pub fn initiate_close(&mut self) {
        self.running.store(false, Ordering::Release);
    }

    #[allow(dead_code)]
    pub fn force_close(&mut self) {
        self.force_close.store(true, Ordering::Release);
        self.initiate_close();
    }
}

impl LookupDispatcher {
    fn start_closing(&self) {
        self.closing.store(true, Ordering::Release);
        self.leader_refresh_due.notify_one();
    }

    /// Sends `lookups` to their leaders, and hands those without a cached leader to the leader
    /// refresher.
    async fn send_lookups(
        self: &Arc<Self>,
        mut lookups: Vec<QueuedLookup>,
        tasks: &mut SenderTasks,
    ) {
        lookups.retain(|lookup| !lookup.is_done());
        if lookups.is_empty() {
            return;
        }

        let GroupingResult {
            primary,
            prefix,
            leaderless_lookups,
        } = self.group_by_leader(lookups);

        if !leaderless_lookups.is_empty() {
            self.wait_for_leaders(leaderless_lookups, &mut tasks.leader_refresher);
        }

        for (server, batches) in primary {
            self.send_requests::<Primary>(server, batches, &mut tasks.requests)
                .await;
        }
        for (server, batches) in prefix {
            self.send_requests::<Prefix>(server, batches, &mut tasks.requests)
                .await;
        }
    }

    fn group_by_leader(&self, lookups: Vec<QueuedLookup>) -> GroupingResult {
        let cluster = self.metadata.get_cluster();
        let mut primary: PrimaryBatches = HashMap::new();
        let mut prefix: PrefixBatches = HashMap::new();
        let mut leaderless_lookups: Vec<QueuedLookup> = Vec::new();

        for query in lookups {
            let table_bucket = query.table_bucket().clone();

            let leader = match cluster.leader_for(&table_bucket) {
                Some(leader) => leader.id(),
                None => {
                    leaderless_lookups.push(query);
                    continue;
                }
            };

            match query {
                QueuedLookup::Primary(q) => {
                    primary
                        .entry(leader)
                        .or_default()
                        .entry(table_bucket.clone())
                        .or_insert_with(|| LookupBatch::new(table_bucket))
                        .add_lookup(q);
                }
                QueuedLookup::Prefix(q) => {
                    prefix
                        .entry(leader)
                        .or_default()
                        .entry(table_bucket.clone())
                        .or_insert_with(|| LookupBatch::new(table_bucket))
                        .add_lookup(q);
                }
            }
        }

        GroupingResult {
            primary,
            prefix,
            leaderless_lookups,
        }
    }

    /// Holds `lookups`, whose bucket has no cached leader, until the leader refresher finds it.
    /// The refresher runs as a task of its own, as a refresh can take long, e.g. when it first
    /// picks a server that is down, and must not hold up the lookups whose leader is known.
    fn wait_for_leaders(
        self: &Arc<Self>,
        lookups: Vec<QueuedLookup>,
        leader_refresher: &mut JoinSet<()>,
    ) {
        let (start_refresher, has_new_buckets) = {
            let mut leader_waits = self.leader_waits.lock();
            let has_new_buckets = leader_waits.add(lookups, Instant::now());
            (leader_waits.claim_refresher(), has_new_buckets)
        };
        if start_refresher {
            leader_refresher.spawn(Arc::clone(self).refresh_leaders());
        } else if has_new_buckets {
            self.leader_refresh_due.notify_one();
        }
    }

    /// Refreshes the metadata of the buckets that lookups wait for, until none waits, and puts
    /// their lookups back to the queue once their leader is cached, by these refreshes or any
    /// other, e.g. the writer's. At most one refresh runs at a time, for the buckets due when it
    /// starts. While closing, every bucket is due, and lookups still without a leader fail.
    async fn refresh_leaders(self: Arc<Self>) {
        let mut cluster_changes = self.metadata.subscribe_cluster_changes();
        let mut refresh_failures = None;
        loop {
            let closing = self.closing.load(Ordering::Acquire);
            let (found, refresh, idle, next_refresh_at) = {
                let mut leader_waits = self.leader_waits.lock();
                let found = leader_waits.take_found(&self.metadata.get_cluster());
                let refresh = leader_waits.due_refresh(Instant::now(), closing);
                let idle = leader_waits.release_refresher_if_idle();
                (found, refresh, idle, leader_waits.next_refresh_at())
            };
            self.re_enqueue_lookups(found);
            if idle {
                return;
            }

            if let Some(refresh) = refresh {
                let refreshing = self.refresh_metadata(&refresh);
                tokio::pin!(refreshing);
                let RefreshOutcome {
                    result,
                    dropped_partitions,
                    failed_tables,
                } = loop {
                    tokio::select! {
                        outcome = &mut refreshing => break outcome,
                        Ok(()) = cluster_changes.changed() => self.re_enqueue_found(),
                    }
                };
                let terminal_error =
                    terminal_refresh_error(&refresh, result, &mut refresh_failures);
                self.finish_refresh(
                    &refresh,
                    &dropped_partitions,
                    &failed_tables,
                    terminal_error,
                );
                // A refresh may fail without waiting, e.g. without a server to ask; with no
                // backoff configured, this keeps the next one from starving the runtime.
                tokio::task::yield_now().await;
            } else if let Some(next_refresh_at) = next_refresh_at {
                tokio::select! {
                    _ = tokio::time::sleep_until(next_refresh_at) => {}
                    _ = self.leader_refresh_due.notified() => {}
                    Ok(()) = cluster_changes.changed() => {}
                }
            }
        }
    }

    /// Isolates table-specific failures so a dropped or inaccessible table cannot keep the other
    /// tables of a combined request waiting for their leaders.
    async fn refresh_metadata(&self, refresh: &LeaderRefresh) -> RefreshOutcome {
        let mut outcome = self.refresh_tables_and_partitions(refresh).await;
        if !outcome.result.as_ref().is_err_and(is_table_refresh_error) {
            return outcome;
        }
        if refresh.table_paths.len() == 1 {
            if !outcome.result.as_ref().is_err_and(Error::is_retriable) {
                let table_path = refresh.table_paths.iter().next().unwrap().clone();
                let error = std::mem::replace(&mut outcome.result, Ok(())).unwrap_err();
                outcome.failed_tables.insert(table_path, error);
            }
            return outcome;
        }

        outcome.result = Ok(());
        for table_path in &refresh.table_paths {
            let table_refresh = LeaderRefresh {
                table_paths: HashSet::from([table_path.clone()]),
                partitions: refresh
                    .partitions
                    .iter()
                    .filter(|(_, path)| *path == table_path)
                    .map(|(&partition_id, path)| (partition_id, path.clone()))
                    .collect(),
                ..Default::default()
            };
            let table_outcome = self.refresh_tables_and_partitions(&table_refresh).await;
            outcome
                .dropped_partitions
                .extend(table_outcome.dropped_partitions);
            outcome.failed_tables.extend(table_outcome.failed_tables);
            match table_outcome.result {
                Err(error) if is_table_refresh_error(&error) && error.is_retriable() => {
                    // A table's schema may not be visible yet. Keep its backoff while the other
                    // tables refresh, instead of making them wait for that schema too.
                    outcome.result = Err(error);
                }
                Err(error) if is_table_refresh_error(&error) => {
                    outcome.failed_tables.insert(table_path.clone(), error);
                }
                Err(error) => {
                    outcome.result = Err(error);
                    return outcome;
                }
                Ok(()) => {}
            }
        }
        outcome
    }

    /// Refreshes the metadata of `refresh`'s tables and partitions in one request. The server fails
    /// all of it if a partition no longer exists, without saying which, so each partition is then
    /// asked for on its own, as the Java client always does: the dropped ones are told apart, and
    /// the others still find their leaders.
    async fn refresh_tables_and_partitions(&self, refresh: &LeaderRefresh) -> RefreshOutcome {
        let table_paths: HashSet<&TablePath> = refresh.table_paths.iter().collect();
        let mut outcome = RefreshOutcome::from_metadata_result(
            self.metadata
                .update_lookup_metadata(&table_paths, refresh.partitions.keys().copied().collect())
                .await,
        );
        if refresh.partitions.is_empty()
            || !outcome.result.as_ref().is_err_and(is_partition_not_exists)
        {
            return outcome;
        }
        outcome.result = Ok(());
        for (&partition_id, table_path) in &refresh.partitions {
            let partition_outcome = RefreshOutcome::from_metadata_result(
                self.metadata
                    .update_lookup_metadata(&HashSet::from([table_path]), vec![partition_id])
                    .await,
            );
            outcome
                .failed_tables
                .extend(partition_outcome.failed_tables);
            match partition_outcome.result {
                Ok(()) => {}
                Err(error) if is_partition_not_exists(&error) => {
                    outcome.dropped_partitions.insert(partition_id, error);
                }
                Err(error) => {
                    outcome.result = Err(error);
                    return outcome;
                }
            }
        }
        let partitioned_tables: HashSet<&TablePath> = refresh.partitions.values().collect();
        let non_partitioned_tables: HashSet<&TablePath> = table_paths
            .difference(&partitioned_tables)
            .copied()
            .collect();
        if !non_partitioned_tables.is_empty() {
            let table_outcome = RefreshOutcome::from_metadata_result(
                self.metadata
                    .update_lookup_metadata(&non_partitioned_tables, vec![])
                    .await,
            );
            outcome.failed_tables.extend(table_outcome.failed_tables);
            outcome.result = table_outcome.result;
        }
        outcome
    }

    fn re_enqueue_found(&self) {
        let found = self
            .leader_waits
            .lock()
            .take_found(&self.metadata.get_cluster());
        self.re_enqueue_lookups(found);
    }

    /// Puts back to the queue the lookups whose leader `refresh` found, and fails those of the
    /// partitions and tables it found dropped or inaccessible with their errors. The other buckets
    /// of `refresh` fail their lookups with `terminal_error` or while closing, and otherwise back
    /// off before their next refresh.
    fn finish_refresh(
        &self,
        refresh: &LeaderRefresh,
        dropped_partitions: &HashMap<PartitionId, Error>,
        failed_tables: &HashMap<TablePath, Error>,
        terminal_error: Option<Error>,
    ) {
        let closing = self.closing.load(Ordering::Acquire);
        let found = {
            let mut leader_waits = self.leader_waits.lock();
            let found = leader_waits.take_found(&self.metadata.get_cluster());
            for (&partition_id, error) in dropped_partitions {
                let failed = leader_waits.fail_partition(partition_id, || copy_error(error));
                if failed > 0 {
                    warn!(
                        "Partition {partition_id} of table {} no longer exists, failing {failed} \
                         lookups: {error}",
                        refresh.partitions[&partition_id]
                    );
                }
            }
            for (table_path, error) in failed_tables {
                let failed =
                    leader_waits.fail_table(table_path, &refresh.table_ids, || copy_error(error));
                if failed > 0 {
                    warn!(
                        "Failed to refresh table {table_path}, failing {failed} lookups: {error}"
                    );
                }
            }
            if let Some(error) = terminal_error {
                leader_waits.fail(&refresh.table_buckets, || copy_error(&error));
            } else if closing {
                leader_waits.fail_on_close(&refresh.table_buckets);
            } else {
                leader_waits.refresh_failed(&refresh.table_buckets, Instant::now());
            }
            found
        };
        self.re_enqueue_lookups(found);
    }

    /// Drops the cached leaders of `batches`' buckets that `destination` still leads, after their
    /// request to it failed, e.g. because the server is down, so their retries look the leader up
    /// again. The other buckets keep their leaders, so lookups to them go on.
    fn invalidate_leaders<T>(&self, destination: ServerId, batches: &[LookupBatch<T>]) {
        let table_buckets: HashSet<TableBucket> = batches
            .iter()
            .map(|batch| batch.table_bucket.clone())
            .collect();
        self.metadata
            .invalidate_bucket_leaders(&table_buckets, destination);
    }

    /// Sends `destination` one request per table, each on a task of its own in `requests`, so a
    /// server that is slow to answer holds up only the lookups sent to it. Returns once every
    /// request has started; while all in-flight permits are taken, it waits for one.
    async fn send_requests<P: LookupProtocol>(
        self: &Arc<Self>,
        destination: ServerId,
        batches_by_bucket: HashMap<TableBucket, LookupBatch<P::Value>>,
        requests: &mut JoinSet<()>,
    ) where
        LookupQuery<P::Value>: Into<QueuedLookup>,
    {
        for (table_id, mut batches) in group_by_table(batches_by_bucket) {
            let permit = match self.acquire_inflight_permit(&mut batches).await {
                Some(p) => p,
                None => continue,
            };
            let dispatcher = Arc::clone(self);
            requests.spawn(async move {
                dispatcher
                    .send_table_lookup::<P>(table_id, destination, batches)
                    .await;
                drop(permit);
            });
        }
    }

    async fn connect_or_fail<T>(
        &self,
        destination: ServerId,
        batches: &mut [LookupBatch<T>],
    ) -> Option<ServerConnection>
    where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        let cluster = self.metadata.get_cluster();
        let tablet_server = match cluster.get_tablet_server(destination) {
            Some(server) => server.clone(),
            None => {
                let error = Error::UnexpectedError {
                    message: format!("Server {destination} is not found in metadata cache"),
                    source: None,
                };
                self.invalidate_leaders(destination, batches);
                self.fail_all_batches(&error, true, batches);
                return None;
            }
        };

        match self.metadata.get_connection(&tablet_server).await {
            Ok(conn) => Some(conn),
            Err(e) => {
                self.handle_request_error(
                    destination,
                    batches,
                    e,
                    format!("Failed to get connection to server {destination}"),
                );
                None
            }
        }
    }

    fn fail_all_batches<T>(&self, error: &Error, is_retriable: bool, batches: &mut [LookupBatch<T>])
    where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        for batch in batches {
            self.handle_batch_error(error, is_retriable, batch);
        }
    }

    async fn send_table_lookup<P: LookupProtocol>(
        &self,
        table_id: TableId,
        destination: ServerId,
        mut batches: Vec<LookupBatch<P::Value>>,
    ) where
        LookupQuery<P::Value>: Into<QueuedLookup>,
    {
        let connection = match self.connect_or_fail(destination, &mut batches).await {
            Some(conn) => conn,
            None => return,
        };
        let keys_by_bucket: Vec<_> = batches.iter_mut().map(|b| b.keys_tuple()).collect();
        let request = P::build_request(table_id, keys_by_bucket);

        match connection.request(request).await {
            Ok(response) => {
                self.handle_response::<P>(table_id, destination, response, &mut batches);
            }
            Err(e) => {
                self.handle_request_error(
                    destination,
                    &mut batches,
                    e,
                    format!("{} request failed", P::OP_NAME),
                );
            }
        }
    }

    fn handle_request_error<T>(
        &self,
        destination: ServerId,
        batches: &mut [LookupBatch<T>],
        error: Error,
        context: String,
    ) where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        // A closed response channel is a missing answer even though its error variant is not
        // retriable on its own. API errors still follow their retry policy: an invalid bucket
        // count, for example, cannot be fixed by replaying the same request.
        let should_invalidate_leader = leader_may_have_moved(&error);
        if should_invalidate_leader {
            self.invalidate_leaders(destination, batches);
        }
        let is_retriable = error.is_retriable()
            || (should_invalidate_leader && !matches!(error, Error::FlussAPIError { .. }));
        let error = with_context(error, context);
        self.fail_all_batches(&error, is_retriable, batches);
    }

    async fn acquire_inflight_permit<T>(
        &self,
        batches: &mut [LookupBatch<T>],
    ) -> Option<OwnedSemaphorePermit> {
        match self.inflight_semaphore.clone().acquire_owned().await {
            Ok(p) => Some(p),
            Err(_) => {
                error!("Semaphore closed during lookup");
                for batch in batches.iter_mut() {
                    batch.complete_all_with_error(|| Error::LookupClosed {
                        message: "the lookup sender has stopped".to_string(),
                    });
                }
                None
            }
        }
    }

    fn handle_response<P: LookupProtocol>(
        &self,
        table_id: TableId,
        destination: ServerId,
        response: P::Response,
        batches: &mut [LookupBatch<P::Value>],
    ) where
        LookupQuery<P::Value>: Into<QueuedLookup>,
    {
        let bucket_to_index = build_bucket_index(batches);
        let mut processed = vec![false; batches.len()];

        for bucket_resp in P::decode_buckets(response) {
            let table_bucket = TableBucket::new_with_partition(
                table_id,
                bucket_resp.partition_id,
                bucket_resp.bucket_id,
            );
            let Some(&idx) = bucket_to_index.get(&table_bucket) else {
                error!(
                    "Received {} response for unknown bucket {} from server {}",
                    P::OP_NAME,
                    table_bucket,
                    destination
                );
                continue;
            };
            processed[idx] = true;
            let batch = &mut batches[idx];

            if let Some(api_error) = bucket_error(bucket_resp.error_code, bucket_resp.error_message)
            {
                if FlussError::for_code(api_error.code).invalidates_metadata() {
                    self.invalidate_leaders(destination, std::slice::from_ref(batch));
                }
                let is_retriable = api_error.is_retriable();
                self.handle_batch_error(&Error::FlussAPIError { api_error }, is_retriable, batch);
                continue;
            }

            batch.complete(bucket_resp.values);
        }

        self.fail_unprocessed_batches(&processed, batches, destination, P::OP_NAME);
    }

    fn fail_unprocessed_batches<T>(
        &self,
        processed: &[bool],
        batches: &mut [LookupBatch<T>],
        destination: ServerId,
        op_name: &'static str,
    ) where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        for (idx, was_processed) in processed.iter().enumerate() {
            if !was_processed {
                let batch = &mut batches[idx];
                let error = Error::UnexpectedError {
                    message: format!(
                        "Bucket {} {} response missing from server {}",
                        batch.table_bucket.bucket_id(),
                        op_name,
                        destination
                    ),
                    source: None,
                };
                self.handle_batch_error(&error, true, batch);
            }
        }
    }

    fn handle_batch_error<T>(&self, error: &Error, is_retriable: bool, batch: &mut LookupBatch<T>)
    where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        let mut retried = 0usize;
        let mut failed = 0usize;
        let mut max_retry_delay_ms = 0u64;
        let table_bucket = batch.table_bucket.clone();
        let now = Instant::now();
        // While closing, retries could keep the sender from ever stopping, e.g. against a server
        // that is down, so the lookups fail with this error instead.
        let closing = self.closing.load(Ordering::Acquire);

        for mut lookup in batch.lookups.drain(..) {
            if is_retriable && !closing && lookup.retries() < self.max_retries && !lookup.is_done()
            {
                // Resending right away does not help while the lookup keeps failing, e.g. during a
                // leader restart; it only floods the server and the log.
                let retry_delay_ms = retry_backoff_ms(
                    self.retry_backoff_ms,
                    self.retry_max_backoff_ms,
                    lookup.retries(),
                );
                max_retry_delay_ms = max_retry_delay_ms.max(retry_delay_ms);
                lookup.prepare_retry(now + Duration::from_millis(retry_delay_ms));
                self.re_enqueue_lookup(lookup.into());
                retried += 1;
            } else {
                lookup.complete_with_error(copy_error(error));
                failed += 1;
            }
        }

        if retried > 0 {
            warn!(
                "Lookup error for bucket {}, retrying {} lookups with retry backoff up to {} ms: {}",
                table_bucket, retried, max_retry_delay_ms, error
            );
        }
        if failed > 0 {
            warn!(
                "Lookup failed for bucket {} ({} lookups): {}",
                table_bucket, failed, error
            );
        }
    }

    fn re_enqueue_lookups(&self, lookups: Vec<QueuedLookup>) {
        for lookup in lookups {
            self.re_enqueue_lookup(lookup);
        }
    }

    fn re_enqueue_lookup(&self, lookup: QueuedLookup) {
        if let Err(e) = self.re_enqueue_tx.send(lookup) {
            error!("Failed to re-enqueue lookup: {}", e);
            let mut failed_lookup = e.0;
            failed_lookup.complete_with_error(Error::LookupClosed {
                message: "the lookup queue has stopped accepting retries".to_string(),
            });
        }
    }
}

fn group_by_table<T>(
    batches_by_bucket: HashMap<TableBucket, LookupBatch<T>>,
) -> HashMap<TableId, Vec<LookupBatch<T>>> {
    let mut out: HashMap<TableId, Vec<LookupBatch<T>>> = HashMap::new();
    for (table_bucket, batch) in batches_by_bucket {
        out.entry(table_bucket.table_id()).or_default().push(batch);
    }
    out
}

fn build_bucket_index<T>(batches: &[LookupBatch<T>]) -> HashMap<TableBucket, usize> {
    batches
        .iter()
        .enumerate()
        .map(|(idx, batch)| (batch.table_bucket.clone(), idx))
        .collect()
}

fn bucket_error(error_code: Option<i32>, error_message: Option<String>) -> Option<ApiError> {
    let code = error_code?;
    let fluss_error = FlussError::for_code(code);
    if fluss_error == FlussError::None {
        return None;
    }
    let message = error_message.unwrap_or_else(|| fluss_error.message().to_string());
    Some(ApiError { code, message })
}

/// What a leader refresh found out.
struct RefreshOutcome {
    result: Result<()>,
    /// The partitions of the refresh that no longer exist, with the error that tells so.
    dropped_partitions: HashMap<PartitionId, Error>,
    /// Tables rejected independently of other tables, e.g. because they were dropped.
    failed_tables: HashMap<TablePath, Error>,
}

impl RefreshOutcome {
    fn from_metadata_result(result: Result<HashSet<TablePath>>) -> Self {
        let mut outcome = Self {
            result: Ok(()),
            dropped_partitions: HashMap::new(),
            failed_tables: HashMap::new(),
        };
        match result {
            Ok(missing_tables) => {
                for table_path in missing_tables {
                    let error = Error::FlussAPIError {
                        api_error: ApiError {
                            code: FlussError::AuthorizationException.code(),
                            message: format!(
                                "Metadata for table {table_path} was omitted because access was denied"
                            ),
                        },
                    };
                    outcome.failed_tables.insert(table_path, error);
                }
            }
            Err(error) => outcome.result = Err(error),
        }
        outcome
    }
}

fn is_table_refresh_error(error: &Error) -> bool {
    matches!(
        error.api_error(),
        Some(
            FlussError::TableNotExist
                | FlussError::SchemaNotExist
                | FlussError::AuthorizationException
        )
    )
}

fn is_partition_not_exists(error: &Error) -> bool {
    error.api_error() == Some(FlussError::PartitionNotExists)
}

/// Leader refreshes that failed in a row, so that a metadata service that stays unreachable warns
/// only once.
struct RefreshFailures {
    count: u32,
    since: Instant,
}

/// Returns failures another refresh cannot fix, see [`is_refusal`]. Other failures leave lookups
/// waiting; table-specific errors are isolated by `refresh_metadata`. Counts consecutive failures,
/// logging only the first warning and eventual recovery.
fn terminal_refresh_error(
    refresh: &LeaderRefresh,
    result: Result<()>,
    failures: &mut Option<RefreshFailures>,
) -> Option<Error> {
    let error = match result {
        Ok(()) => {
            if let Some(failures) = failures.take() {
                info!(
                    "Refreshed the metadata of table buckets without a leader after {} failed \
                     refreshes over {:?}",
                    failures.count,
                    failures.since.elapsed()
                );
            }
            return None;
        }
        Err(error) => error,
    };
    if is_refusal(&error) {
        warn!(
            "Failed to refresh the metadata of {} table buckets without a leader, failing their \
             lookups: {error}",
            refresh.table_buckets.len()
        );
        return Some(error);
    }
    match failures {
        Some(failures) => {
            failures.count = failures.count.saturating_add(1);
            debug!(
                "Failed to refresh the metadata of {} table buckets without a leader {} times in a \
                 row, refreshing again after a backoff: {error}",
                refresh.table_buckets.len(),
                failures.count
            );
        }
        None => {
            *failures = Some(RefreshFailures {
                count: 1,
                since: Instant::now(),
            });
            warn!(
                "Failed to refresh the metadata of {} table buckets without a leader, refreshing \
                 again after a backoff; further failures in a row are logged at debug level: \
                 {error}",
                refresh.table_buckets.len()
            );
        }
    }
    None
}

/// Whether a failed leader refresh refuses the client, so another one cannot succeed: rejected
/// credentials or API version, no shared API version, the wrong server type, or an invalid API
/// version advertisement. Other errors leave the lookups waiting, e.g. `UNKNOWN_SERVER_ERROR`,
/// which the server answers for a schema or bucket assignment that is not visible yet, or
/// `IllegalArgument` for a bootstrap DNS failure.
fn is_refusal(error: &Error) -> bool {
    match error {
        Error::FlussAPIError { .. } => matches!(
            error.api_error(),
            Some(FlussError::AuthenticateException | FlussError::UnsupportedVersion)
        ),
        Error::IllegalArgument { .. } => false,
        _ => !error.is_retriable() && !leader_may_have_moved(error),
    }
}

/// Whether the destination of a failed request may no longer lead its buckets: the request didn't
/// get through, e.g. because the connection dropped, or the server answered that the client's
/// metadata is stale. Errors the client raises itself, e.g. finding no API version it shares with
/// the server, say nothing about the leader.
fn leader_may_have_moved(error: &Error) -> bool {
    match error {
        Error::FlussAPIError { .. } => error
            .api_error()
            .is_some_and(|api_error| api_error.invalidates_metadata()),
        Error::RpcError { .. } | Error::IoUnexpectedError { .. } => true,
        Error::UnexpectedError {
            source: Some(source),
            ..
        } => {
            // PendingResponse uses this error when its response channel closes without an answer.
            // Other unexpected errors, e.g. an invalid API version, are not transport failures.
            source.is::<tokio::sync::oneshot::error::RecvError>()
        }
        _ => false,
    }
}

/// Preserves errors callers can identify by their API code or variant.
fn with_context(error: Error, context: String) -> Error {
    match error {
        Error::FlussAPIError { .. }
        | Error::UnsupportedVersion { .. }
        | Error::InvalidServerType { .. } => error,
        other => Error::UnexpectedError {
            message: format!("{context}: {other}"),
            source: None,
        },
    }
}

fn copy_error(error: &Error) -> Error {
    match error {
        Error::UnsupportedVersion { message } => Error::UnsupportedVersion {
            message: message.clone(),
        },
        Error::InvalidServerType { message } => Error::InvalidServerType {
            message: message.clone(),
        },
        Error::FlussAPIError { api_error } => Error::FlussAPIError {
            api_error: api_error.clone(),
        },
        Error::UnexpectedError { message, .. } => Error::UnexpectedError {
            message: message.clone(),
            source: None,
        },
        other => Error::UnexpectedError {
            message: other.to_string(),
            source: None,
        },
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
