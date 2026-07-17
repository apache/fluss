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

use super::{LookupQueue, QueuedLookup};
use crate::client::lookup::lookup_query::LookupQuery;
use crate::client::metadata::Metadata;
use crate::error::{Error, FlussError, Result};
use crate::metadata::{TableBucket, TablePath};
use crate::proto::{LookupResponse, PrefixLookupResponse};
use crate::rpc::ServerConnection;
use crate::rpc::message::{LookupRequest, PrefixLookupRequest, ReadType, RequestBody, WriteType};
use crate::{BucketId, PartitionId, TableId};
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use log::{debug, error, warn};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

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
    type Value: Send;

    const OP_NAME: &'static str;

    fn build_request(
        table_id: TableId,
        keys_by_bucket: Vec<(BucketId, Option<PartitionId>, Vec<Bytes>)>,
    ) -> Self::Request;

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

    fn build_request(
        table_id: TableId,
        keys_by_bucket: Vec<(BucketId, Option<PartitionId>, Vec<Bytes>)>,
    ) -> Self::Request {
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

    fn build_request(
        table_id: TableId,
        keys_by_bucket: Vec<(BucketId, Option<PartitionId>, Vec<Bytes>)>,
    ) -> Self::Request {
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

struct GroupByLeaderResult {
    primary: PrimaryBatches,
    prefix: PrefixBatches,
    unknown_leader_tables: HashSet<TablePath>,
    unknown_leader_partition_ids: HashSet<PartitionId>,
}

impl GroupByLeaderResult {
    fn is_empty(&self) -> bool {
        self.primary.is_empty() && self.prefix.is_empty()
    }

    /// Assumes no `(server, bucket)` overlap — safe because the second pass only
    /// re-groups items unknown in the first.
    fn merge_batches(&mut self, other: GroupByLeaderResult) {
        for (server, inner) in other.primary {
            self.primary.entry(server).or_default().extend(inner);
        }
        for (server, inner) in other.prefix {
            self.prefix.entry(server).or_default().extend(inner);
        }
    }
}

struct GroupingResult {
    groups: GroupByLeaderResult,
    unknowns: Vec<QueuedLookup>,
}

pub struct LookupSender {
    metadata: Arc<Metadata>,
    queue: LookupQueue,
    re_enqueue_tx: mpsc::UnboundedSender<QueuedLookup>,
    inflight_semaphore: Arc<Semaphore>,
    max_retries: i32,
    running: AtomicBool,
    force_close: AtomicBool,
    shutdown_rx: watch::Receiver<bool>,
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

    fn complete_all_with_error(&mut self, error_msg: &str) {
        for lookup in &mut self.lookups {
            lookup.complete_with_error(Error::UnexpectedError {
                message: error_msg.to_string(),
                source: None,
            });
        }
    }

    fn keys_tuple(&mut self) -> (BucketId, Option<PartitionId>, Vec<Bytes>) {
        (
            self.table_bucket.bucket_id(),
            self.table_bucket.partition_id(),
            std::mem::take(&mut self.keys),
        )
    }
}

impl LookupSender {
    pub fn new(
        metadata: Arc<Metadata>,
        queue: LookupQueue,
        re_enqueue_tx: mpsc::UnboundedSender<QueuedLookup>,
        max_inflight_requests: usize,
        max_retries: i32,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            metadata,
            queue,
            re_enqueue_tx,
            inflight_semaphore: Arc::new(Semaphore::new(max_inflight_requests)),
            max_retries,
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

            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("Lookup sender received shutdown signal during select");
                        self.initiate_close();
                    }
                }
                result = self.run_once(false) => {
                    if let Err(e) = result {
                        error!("Error in lookup sender: {}", e);
                    }
                }
            }
        }

        debug!("Beginning shutdown of lookup sender, sending remaining lookups");

        // TODO: Check the in-flight request count in the accumulator.
        if !self.force_close.load(Ordering::Acquire) && self.queue.has_undrained() {
            if let Err(e) = self.run_once(true).await {
                error!("Error during lookup sender shutdown: {}", e);
            }
        }

        // TODO: If force close failed, add logic to abort incomplete lookup requests.
        debug!("Lookup sender shutdown complete");
    }

    async fn run_once(&mut self, drain_all: bool) -> Result<()> {
        let lookups = if drain_all {
            self.queue.drain_all()
        } else {
            self.queue.drain().await
        };

        self.send_lookups(lookups).await
    }

    async fn send_lookups(&self, lookups: Vec<QueuedLookup>) -> Result<()> {
        if lookups.is_empty() {
            return Ok(());
        }

        let GroupingResult {
            mut groups,
            unknowns,
        } = self.group_by_leader(lookups);

        if !unknowns.is_empty() {
            let table_paths_refs: HashSet<&TablePath> =
                groups.unknown_leader_tables.iter().collect();
            let partition_ids: Vec<PartitionId> = groups
                .unknown_leader_partition_ids
                .iter()
                .copied()
                .collect();
            if let Err(e) = self
                .metadata
                .update_tables_metadata(&table_paths_refs, &HashSet::new(), partition_ids)
                .await
            {
                warn!("Failed to update metadata for unknown leader tables: {}", e);
            } else {
                debug!(
                    "Updated metadata due to unknown leader tables during lookup: {:?}",
                    groups.unknown_leader_tables
                );
            }

            // Re-group with fresh cluster state; dispatch what resolved, re-enqueue the rest.
            let retry = self.group_by_leader(unknowns);
            groups.merge_batches(retry.groups);
            for item in retry.unknowns {
                self.re_enqueue_lookup(item);
            }

            // Nothing to dispatch even after refresh — back off to avoid a tight RPC loop.
            if groups.is_empty() {
                let mut cluster_rx = self.metadata.subscribe_cluster_changes();
                tokio::select! {
                    _ = cluster_rx.changed() => {}
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
                return Ok(());
            }
        }

        let primary_fut = async {
            let mut pending = FuturesUnordered::new();
            for (server, batches) in groups.primary {
                pending.push(self.send_request::<Primary>(server, batches));
            }
            while pending.next().await.is_some() {}
        };
        let prefix_fut = async {
            let mut pending = FuturesUnordered::new();
            for (server, batches) in groups.prefix {
                pending.push(self.send_request::<Prefix>(server, batches));
            }
            while pending.next().await.is_some() {}
        };
        tokio::join!(primary_fut, prefix_fut);

        Ok(())
    }

    fn group_by_leader(&self, lookups: Vec<QueuedLookup>) -> GroupingResult {
        let cluster = self.metadata.get_cluster();
        let mut primary: PrimaryBatches = HashMap::new();
        let mut prefix: PrefixBatches = HashMap::new();
        let mut unknown_leader_tables: HashSet<TablePath> = HashSet::new();
        let mut unknown_leader_partition_ids: HashSet<PartitionId> = HashSet::new();
        let mut unknowns: Vec<QueuedLookup> = Vec::new();

        for query in lookups {
            let table_bucket = query.table_bucket().clone();

            let leader = match cluster.leader_for(&table_bucket) {
                Some(leader) => leader.id(),
                None => {
                    warn!(
                        "No leader found for table bucket {} during lookup",
                        table_bucket
                    );
                    unknown_leader_tables.insert(query.table_path().clone());
                    if let Some(partition_id) = table_bucket.partition_id() {
                        unknown_leader_partition_ids.insert(partition_id);
                    }
                    unknowns.push(query);
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
            groups: GroupByLeaderResult {
                primary,
                prefix,
                unknown_leader_tables,
                unknown_leader_partition_ids,
            },
            unknowns,
        }
    }

    async fn send_request<P: LookupProtocol>(
        &self,
        destination: ServerId,
        batches_by_bucket: HashMap<TableBucket, LookupBatch<P::Value>>,
    ) where
        LookupQuery<P::Value>: Into<QueuedLookup>,
    {
        let mut batches_by_table = group_by_table(batches_by_bucket);
        let connection = match self
            .connect_or_fail(destination, &mut batches_by_table)
            .await
        {
            Some(conn) => conn,
            None => return,
        };

        let mut pending = FuturesUnordered::new();
        for (table_id, mut batches) in batches_by_table {
            let keys_by_bucket: Vec<_> = batches.iter_mut().map(|b| b.keys_tuple()).collect();
            let request = P::build_request(table_id, keys_by_bucket);
            pending.push(self.send_single_table_lookup::<P>(
                table_id,
                destination,
                connection.clone(),
                request,
                batches,
            ));
        }
        while pending.next().await.is_some() {}
    }

    async fn connect_or_fail<T>(
        &self,
        destination: ServerId,
        batches_by_table: &mut HashMap<TableId, Vec<LookupBatch<T>>>,
    ) -> Option<ServerConnection>
    where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        let cluster = self.metadata.get_cluster();
        let tablet_server = match cluster.get_tablet_server(destination) {
            Some(server) => server.clone(),
            None => {
                let err_msg = format!("Server {} is not found in metadata cache", destination);
                self.fail_all_batches(&err_msg, true, batches_by_table);
                return None;
            }
        };

        match self.metadata.get_connection(&tablet_server).await {
            Ok(conn) => Some(conn),
            Err(e) => {
                let err_msg = format!("Failed to get connection to server {}: {}", destination, e);
                self.fail_all_batches(&err_msg, true, batches_by_table);
                None
            }
        }
    }

    fn fail_all_batches<T>(
        &self,
        err_msg: &str,
        is_retriable: bool,
        batches_by_table: &mut HashMap<TableId, Vec<LookupBatch<T>>>,
    ) where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        for batches in batches_by_table.values_mut() {
            for batch in batches.iter_mut() {
                self.handle_batch_error(err_msg, is_retriable, batch);
            }
        }
    }

    async fn send_single_table_lookup<P: LookupProtocol>(
        &self,
        table_id: TableId,
        destination: ServerId,
        connection: ServerConnection,
        request: P::Request,
        mut batches: Vec<LookupBatch<P::Value>>,
    ) where
        LookupQuery<P::Value>: Into<QueuedLookup>,
    {
        let _permit = match self.acquire_inflight_permit(&mut batches).await {
            Some(p) => p,
            None => return,
        };

        match connection.request(request).await {
            Ok(response) => {
                self.handle_response::<P>(table_id, destination, response, &mut batches);
            }
            Err(e) => {
                let err_msg = format!("{} request failed: {}", P::OP_NAME, e);
                let is_retriable = e.is_retriable();
                for batch in &mut batches {
                    self.handle_batch_error(&err_msg, is_retriable, batch);
                }
            }
        }
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
                    batch.complete_all_with_error("Lookup sender shutdown");
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

            if let Some(err) = extract_bucket_error(
                bucket_resp.error_code,
                bucket_resp.error_message,
                &table_bucket,
                P::OP_NAME,
            ) {
                self.handle_batch_error(&err.message, err.is_retriable, batch);
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
                let err_msg = format!(
                    "Bucket {} {} response missing from server {}",
                    batch.table_bucket.bucket_id(),
                    op_name,
                    destination
                );
                self.handle_batch_error(&err_msg, true, batch);
            }
        }
    }

    fn handle_batch_error<T>(&self, error_msg: &str, is_retriable: bool, batch: &mut LookupBatch<T>)
    where
        LookupQuery<T>: Into<QueuedLookup>,
    {
        let mut retried = 0usize;
        let mut failed = 0usize;
        let table_bucket = batch.table_bucket.clone();

        for mut lookup in batch.lookups.drain(..) {
            if is_retriable && lookup.retries() < self.max_retries && !lookup.is_done() {
                lookup.increment_retries();
                self.re_enqueue_lookup(lookup.into());
                retried += 1;
            } else {
                lookup.complete_with_error(Error::UnexpectedError {
                    message: error_msg.to_string(),
                    source: None,
                });
                failed += 1;
            }
        }

        if retried > 0 {
            warn!(
                "Lookup error for bucket {}, retrying {} lookups: {}",
                table_bucket, retried, error_msg
            );
        }
        if failed > 0 {
            warn!(
                "Lookup failed for bucket {} ({} lookups): {}",
                table_bucket, failed, error_msg
            );
        }
    }

    fn re_enqueue_lookup(&self, lookup: QueuedLookup) {
        if let Err(e) = self.re_enqueue_tx.send(lookup) {
            error!("Failed to re-enqueue lookup: {}", e);
            let mut failed_lookup = e.0;
            failed_lookup.complete_with_error(Error::UnexpectedError {
                message: "Failed to re-enqueue lookup: channel closed".to_string(),
                source: None,
            });
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

struct BucketError {
    message: String,
    is_retriable: bool,
}

fn extract_bucket_error(
    error_code: Option<i32>,
    error_message: Option<String>,
    table_bucket: &TableBucket,
    op: &str,
) -> Option<BucketError> {
    let code = error_code?;
    let fluss_error = FlussError::for_code(code);
    if fluss_error == FlussError::None {
        return None;
    }
    Some(BucketError {
        message: format!(
            "{} error for bucket {}: code={}, message={}",
            op,
            table_bucket,
            code,
            error_message.unwrap_or_default()
        ),
        is_retriable: fluss_error.is_retriable(),
    })
}
