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

use crate::client::Metadata;
use crate::cluster::{ServerNode, ServerType};
use crate::error::{Error, Result};
use crate::metadata::TableBucket;
use crate::proto;
use crate::rpc::{ApiError, RequestFailure, RpcClient, message};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Consumer group configuration for a manually assigned scanner.
/// Commits are explicit; callers must finish processing before committing.
#[derive(Debug, Clone)]
pub struct ConsumerGroupOptions {
    /// Nonempty group ID, sent unchanged and subject to the server's naming rules.
    pub group_id: String,
    /// Deadline after preceding commits finish, including discovery, connections and retries.
    pub timeout: Duration,
}

impl ConsumerGroupOptions {
    /// Creates options with a 30-second operation deadline.
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            timeout: Duration::from_secs(30),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.group_id.is_empty() || self.timeout.is_zero() {
            return Err(invalid(
                "Consumer group ID and operation timeout must be nonempty/nonzero",
            ));
        }
        Ok(())
    }
}

/// The next offset to consume, and optional application metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndMetadata {
    /// Next record to consume, not the last processed record's offset.
    pub offset: i64,
    /// `None` and an empty string are distinct stored values.
    pub metadata: Option<String>,
}

impl OffsetAndMetadata {
    /// Creates an offset without application metadata.
    pub fn new(offset: i64) -> Self {
        Self {
            offset,
            metadata: None,
        }
    }
}

/// Result for a single bucket. A successful RPC may contain failed buckets.
#[derive(Debug)]
pub enum OffsetCommitOutcome {
    /// The server acknowledged persistence to all in-sync replicas.
    Committed,
    /// The server rejected this bucket before writing it.
    Rejected(ApiError),
    /// The write may have taken effect. Do not blindly retry an older offset.
    Unknown(ApiError),
}

/// Per-bucket commit outcomes; contains exactly the requested buckets.
pub type OffsetCommitResult = HashMap<TableBucket, OffsetCommitOutcome>;

/// Awaitable commit completion. Dropping or aborting this handle stops waiting;
/// the queued operation continues and preserves commit ordering.
pub type OffsetCommitHandle =
    tokio::task::JoinHandle<std::result::Result<OffsetCommitResult, OffsetCommitError>>;

/// Request-level commit failure, distinct from per-bucket results.
#[derive(Debug, thiserror::Error)]
pub enum OffsetCommitError {
    /// No write was dispatched, or the server explicitly rejected the request.
    #[error("Offset commit rejected: {0}")]
    Rejected(#[source] Error),
    /// The request may have been applied. A late write can cause replay on recovery.
    #[error("Offset commit outcome unknown: {0}")]
    Unknown(#[source] Error),
    /// A late write could undo the requested rewind or replace metadata at the same offset.
    /// Fetching offsets does not fence a late write. Settle the previous writer
    /// before resuming with a new scanner.
    #[error(
        "A late offset commit could overwrite this position or its metadata; settle it before rewinding"
    )]
    NeedsRecovery,
}

impl From<tokio::task::JoinError> for OffsetCommitError {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::Unknown(Error::UnexpectedError {
            message: "Offset commit task failed".into(),
            source: Some(Box::new(error)),
        })
    }
}

pub(crate) fn invalid(message: &str) -> Error {
    Error::IllegalArgument {
        message: message.to_owned(),
    }
}

fn timeout_error() -> Error {
    Error::UnexpectedError {
        message: "Consumer offset operation deadline exceeded".into(),
        source: None,
    }
}

fn routing_error(error: &Error) -> bool {
    matches!(error, Error::FlussAPIError { api_error } if matches!(api_error.code, 76..=78))
}

fn api_error(code: i32, message: Option<String>) -> ApiError {
    ApiError {
        code,
        message: message.unwrap_or_else(|| crate::rpc::FlussError::for_code(code).message().into()),
    }
}

fn definite_rejection(error: &Error) -> bool {
    // Only known validation/authentication failures establish that no write occurred.
    // A future, unrecognized error code must not silently clear uncertainty.
    matches!(error, Error::FlussAPIError { api_error } if matches!(api_error.code, 2 | 4 | 7 | 15 | 16 | 36 | 37 | 43 | 46 | 47 | 48 | 75 | 79 | 80 | 81))
        || matches!(
            error,
            Error::UnsupportedOperation { .. }
                | Error::UnsupportedVersion { .. }
                | Error::IllegalArgument { .. }
        )
}

const MAX_PENDING_COMMITS: usize = 64;

pub(crate) struct ConsumerGroup {
    slots: Arc<tokio::sync::Semaphore>,
    client: ConsumerGroupClient,
    // Held through the response, not just dispatch: the server may reorder concurrent commits.
    late_writes: tokio::sync::Mutex<HashMap<TableBucket, OffsetAndMetadata>>,
    tail: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub(crate) delivered: Mutex<HashMap<TableBucket, OffsetAndMetadata>>,
}

impl ConsumerGroup {
    pub(crate) fn new(
        options: ConsumerGroupOptions,
        connections: Arc<RpcClient>,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self {
            slots: Arc::new(tokio::sync::Semaphore::new(MAX_PENDING_COMMITS)),
            client: ConsumerGroupClient::new(options, connections, metadata),
            late_writes: tokio::sync::Mutex::new(HashMap::new()),
            tail: Mutex::new(None),
            delivered: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn await_pending_commits(&self) {
        // Each queued operation releases its permit only after its outcome is known locally.
        let drain = self.slots.acquire_many(MAX_PENDING_COMMITS as u32);
        if tokio::time::timeout(self.client.options.timeout, drain)
            .await
            .is_err()
        {
            log::warn!("Timed out waiting for pending consumer offset commits during close");
        }
    }

    pub(crate) async fn fetch_positions(
        &self,
        buckets: &[TableBucket],
    ) -> Result<HashMap<TableBucket, Option<OffsetAndMetadata>>> {
        self.client.fetch(buckets).await
    }

    pub(crate) fn client_for(&self, group_id: &str) -> Option<&ConsumerGroupClient> {
        (self.client.options.group_id == group_id).then_some(&self.client)
    }

    pub(crate) fn enqueue(
        self: &Arc<Self>,
        offsets: HashMap<TableBucket, OffsetAndMetadata>,
    ) -> Result<OffsetCommitHandle> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| invalid("Consumer offset commits require a Tokio runtime"))?;
        let slot = self.slots.clone().try_acquire_owned().map_err(|_| Error::BufferExhausted {
            message: format!("All {MAX_PENDING_COMMITS} consumer commit slots are occupied; wait for a pending commit before retrying"),
        })?;
        let (done, next) = tokio::sync::oneshot::channel();
        let previous = self.tail.lock().replace(next);
        let group = self.clone();
        let operation = runtime.spawn(async move {
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            let result = group.commit(offsets).await;
            drop(slot);
            let _ = done.send(());
            result
        });
        // Keep the queue link alive when a caller cancels its waiting handle.
        Ok(runtime.spawn(async move { operation.await? }))
    }

    async fn commit(
        &self,
        offsets: HashMap<TableBucket, OffsetAndMetadata>,
    ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
        if offsets.is_empty() {
            return Ok(HashMap::new());
        }
        let request = self
            .client
            .commit_request(&offsets)
            .map_err(OffsetCommitError::Rejected)?;
        let mut late = self.late_writes.lock().await;
        if offsets.iter().any(|(bucket, next)| {
            late.get(bucket).is_some_and(|old| {
                next.offset < old.offset
                    || (next.offset == old.offset && next.metadata != old.metadata)
            })
        }) {
            return Err(OffsetCommitError::NeedsRecovery);
        }
        let mut attempt = CommitAttempt {
            offsets: &offsets,
            late: &mut late,
            started: false,
        };
        let mut lost_reply = false;
        let result = tokio::time::timeout(self.client.options.timeout, async {
            loop {
                let owner = self.client.owner().await?;
                let response = match self.client.connections.get_connection(&owner).await {
                    Ok(connection) => {
                        connection
                            .request_with_send_status(
                                message::CommitConsumerOffsetsRequest {
                                    inner_request: request.clone(),
                                },
                                &mut attempt.started,
                            )
                            .await
                    }
                    Err(error) => Err(RequestFailure::NotSent(error)),
                };
                match response {
                    Ok(response) => {
                        let mut results = decode_commit(response, &offsets)?;
                        attempt.started = false;
                        for (bucket, outcome) in &mut results {
                            match outcome {
                                OffsetCommitOutcome::Unknown(_) => {
                                    attempt.remember(bucket);
                                }
                                OffsetCommitOutcome::Rejected(error) if lost_reply => {
                                    *outcome = OffsetCommitOutcome::Unknown(error.clone());
                                }
                                _ => {}
                            }
                        }
                        return Ok(results);
                    }
                    Err(RequestFailure::NotSent(error)) => {
                        attempt.started = false;
                        if !error.is_retriable() {
                            return Err(error);
                        }
                    }
                    Err(RequestFailure::MaybeSent(error)) if routing_error(&error) => {
                        attempt.started = false;
                    }
                    Err(RequestFailure::MaybeSent(error)) if definite_rejection(&error) => {
                        attempt.started = false;
                        return Err(error);
                    }
                    Err(RequestFailure::MaybeSent(Error::RpcError { source, .. }))
                        if source.is_transport_failure() =>
                    {
                        // A successful replay cannot rule out a late copy of the first attempt.
                        if !lost_reply {
                            attempt.remember_all();
                        }
                        attempt.started = false;
                        lost_reply = true;
                    }
                    Err(RequestFailure::MaybeSent(error)) => return Err(error),
                }
                self.client.invalidate_owner();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            // A stale owner may never finish the handshake, even before a commit is sent.
            self.client.invalidate_owner();
            Err(timeout_error())
        });
        result.map_err(|error| {
            if attempt.started || lost_reply {
                self.client.invalidate_owner();
                OffsetCommitError::Unknown(error)
            } else {
                OffsetCommitError::Rejected(error)
            }
        })
    }
}

pub(crate) struct ConsumerGroupClient {
    options: ConsumerGroupOptions,
    connections: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    owner: Mutex<Option<ServerNode>>,
}

impl ConsumerGroupClient {
    pub(crate) fn new(
        options: ConsumerGroupOptions,
        connections: Arc<RpcClient>,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self {
            options,
            connections,
            metadata,
            owner: Mutex::new(None),
        }
    }

    async fn owner(&self) -> Result<ServerNode> {
        if let Some(owner) = self.owner.lock().clone() {
            return Ok(owner);
        }
        loop {
            let node = self
                .metadata
                .get_cluster()
                .get_one_available_server()
                .cloned();
            if let Some(node) = node {
                let response = async {
                    self.connections
                        .get_connection(&node)
                        .await?
                        .request(message::FindConsumerGroupOwnerRequest {
                            inner_request: proto::FindConsumerGroupOwnerRequest {
                                group_id: self.options.group_id.clone(),
                            },
                        })
                        .await
                }
                .await;
                match response {
                    Ok(response) => {
                        let pb = response.owner;
                        if pb.host.is_empty() || !(1..=65535).contains(&pb.port) {
                            return Err(invalid("Invalid consumer group owner endpoint"));
                        }
                        let owner = ServerNode::new(
                            pb.node_id,
                            pb.host,
                            pb.port as u32,
                            ServerType::TabletServer,
                        );
                        *self.owner.lock() = Some(owner.clone());
                        return Ok(owner);
                    }
                    Err(error) if error.is_retriable() || routing_error(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            match self.metadata.reinit_cluster().await {
                Err(error) if !error.is_retriable() => return Err(error),
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn invalidate_owner(&self) {
        *self.owner.lock() = None;
    }

    pub(crate) async fn fetch(
        &self,
        buckets: &[TableBucket],
    ) -> Result<HashMap<TableBucket, Option<OffsetAndMetadata>>> {
        if buckets.is_empty() {
            return Ok(HashMap::new());
        }
        self.fetch_offsets(buckets, false).await
    }

    pub(crate) async fn fetch_all(
        &self,
    ) -> Result<HashMap<TableBucket, Option<OffsetAndMetadata>>> {
        self.fetch_offsets(&[], true).await
    }

    async fn fetch_offsets(
        &self,
        buckets: &[TableBucket],
        all: bool,
    ) -> Result<HashMap<TableBucket, Option<OffsetAndMetadata>>> {
        let mut requested: HashMap<i64, Vec<proto::PbGetConsumerOffsetsRequestBucket>> =
            HashMap::new();
        let expected: HashSet<_> = buckets.iter().cloned().collect();
        if expected.len() != buckets.len() {
            return Err(invalid("Duplicate requested consumer offset bucket"));
        }
        for bucket in buckets {
            validate_bucket(bucket)?;
            requested.entry(bucket.table_id()).or_default().push(
                proto::PbGetConsumerOffsetsRequestBucket {
                    partition_id: bucket.partition_id(),
                    bucket_id: bucket.bucket_id(),
                },
            );
        }
        let request = proto::GetConsumerOffsetsRequest {
            group_id: self.options.group_id.clone(),
            all_offsets: Some(all),
            tables: requested
                .into_iter()
                .map(
                    |(table_id, buckets)| proto::PbGetConsumerOffsetsRequestTable {
                        table_id,
                        buckets,
                    },
                )
                .collect(),
        };
        tokio::time::timeout(self.options.timeout, async {
            loop {
                let owner = self.owner().await?;
                let response = async {
                    self.connections
                        .get_connection(&owner)
                        .await?
                        .request(message::GetConsumerOffsetsRequest {
                            inner_request: request.clone(),
                        })
                        .await
                }
                .await;
                match response.and_then(|response| {
                    decode_fetch(response, if all { None } else { Some(&expected) })
                }) {
                    Err(error) if error.is_retriable() || routing_error(&error) => {
                        self.invalidate_owner();
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    result => return result,
                }
            }
        })
        .await
        .map_err(|_| timeout_error())?
    }

    fn commit_request(
        &self,
        offsets: &HashMap<TableBucket, OffsetAndMetadata>,
    ) -> Result<proto::CommitConsumerOffsetsRequest> {
        let mut requested: HashMap<i64, Vec<proto::PbCommitConsumerOffsetsRequestBucket>> =
            HashMap::new();
        for (bucket, value) in offsets {
            validate_bucket(bucket)?;
            if value.offset < 0 {
                return Err(invalid("Committed offset must be nonnegative"));
            }
            requested.entry(bucket.table_id()).or_default().push(
                proto::PbCommitConsumerOffsetsRequestBucket {
                    partition_id: bucket.partition_id(),
                    bucket_id: bucket.bucket_id(),
                    offset: value.offset,
                    metadata: value.metadata.clone(),
                },
            );
        }
        Ok(proto::CommitConsumerOffsetsRequest {
            group_id: self.options.group_id.clone(),
            tables: requested
                .into_iter()
                .map(
                    |(table_id, buckets)| proto::PbCommitConsumerOffsetsRequestTable {
                        table_id,
                        buckets,
                    },
                )
                .collect(),
        })
    }

    pub(crate) async fn alter_offsets(
        &self,
        offsets: HashMap<TableBucket, OffsetAndMetadata>,
    ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
        if offsets.is_empty() {
            return Ok(HashMap::new());
        }
        let request = self
            .commit_request(&offsets)
            .map_err(OffsetCommitError::Rejected)?;
        let response = self
            .mutate(&mut false, || message::CommitConsumerOffsetsRequest {
                inner_request: request.clone(),
            })
            .await?;
        decode_commit(response, &offsets).map_err(OffsetCommitError::Unknown)
    }
}

// Retain only the highest potentially late position per affected bucket. A later ACK
// is not a fence. Dropping a request after its socket write began is ambiguous too.
struct CommitAttempt<'a> {
    offsets: &'a HashMap<TableBucket, OffsetAndMetadata>,
    late: &'a mut HashMap<TableBucket, OffsetAndMetadata>,
    started: bool,
}

impl CommitAttempt<'_> {
    fn remember(&mut self, bucket: &TableBucket) {
        self.late
            .insert(bucket.clone(), self.offsets[bucket].clone());
    }

    fn remember_all(&mut self) {
        for bucket in self.offsets.keys() {
            self.remember(bucket);
        }
    }
}

impl Drop for CommitAttempt<'_> {
    fn drop(&mut self) {
        if self.started {
            self.remember_all();
        }
    }
}

fn validate_bucket(bucket: &TableBucket) -> Result<()> {
    if bucket.bucket_id() < 0 || bucket.partition_id().is_some_and(|id| id < 0) {
        return Err(invalid(
            "Bucket and explicit partition IDs must be nonnegative; omit partition ID for nonpartitioned tables",
        ));
    }
    Ok(())
}

fn decode_fetch(
    response: proto::GetConsumerOffsetsResponse,
    expected: Option<&HashSet<TableBucket>>,
) -> Result<HashMap<TableBucket, Option<OffsetAndMetadata>>> {
    let mut result = HashMap::new();
    for table in response.tables {
        for bucket in table.buckets {
            let key = TableBucket::new_with_partition(
                table.table_id,
                bucket.partition_id,
                bucket.bucket_id,
            );
            if expected.is_some_and(|keys| !keys.contains(&key)) || result.contains_key(&key) {
                return Err(invalid(
                    "Unexpected or duplicate consumer offset response bucket",
                ));
            }
            if let Some(code) = bucket.error_code.filter(|code| *code != 0) {
                return Err(Error::FlussAPIError {
                    api_error: api_error(code, bucket.error_message),
                });
            }
            if bucket.offset.is_some_and(|offset| offset < 0) {
                return Err(invalid("Negative committed offset in server response"));
            }
            result.insert(
                key,
                bucket.offset.map(|offset| OffsetAndMetadata {
                    offset,
                    metadata: bucket.metadata,
                }),
            );
        }
    }
    if expected.is_some_and(|keys| result.len() != keys.len()) {
        return Err(invalid("Missing consumer offset response bucket"));
    }
    Ok(result)
}

fn decode_commit(
    response: proto::CommitConsumerOffsetsResponse,
    expected: &HashMap<TableBucket, OffsetAndMetadata>,
) -> Result<OffsetCommitResult> {
    let mut result = HashMap::new();
    for table in response.tables {
        for bucket in table.buckets {
            let key = TableBucket::new_with_partition(
                table.table_id,
                bucket.partition_id,
                bucket.bucket_id,
            );
            if !expected.contains_key(&key) || result.contains_key(&key) {
                return Err(invalid(
                    "Unexpected or duplicate consumer commit response bucket",
                ));
            }
            let outcome = match bucket.error_code.filter(|code| *code != 0) {
                None => OffsetCommitOutcome::Committed,
                Some(code) => {
                    let error = api_error(code, bucket.error_message);
                    // Metadata validation happens before the append. Other write errors may
                    // follow a successful local append, even when replication did not finish.
                    if code == 80 {
                        OffsetCommitOutcome::Rejected(error)
                    } else {
                        OffsetCommitOutcome::Unknown(error)
                    }
                }
            };
            result.insert(key, outcome);
        }
    }
    if result.len() != expected.len() {
        return Err(invalid("Missing consumer commit response bucket"));
    }
    Ok(result)
}

/// Per-bucket deletion results. An unknown result must not be retried while writers
/// are active: a second deletion could remove a newer commit.
pub type OffsetDeleteResult = HashMap<TableBucket, std::result::Result<(), OffsetCommitError>>;
/// Per-group deletion results, including failures from independent owners.
pub type GroupDeleteResult = HashMap<String, std::result::Result<(), OffsetCommitError>>;

impl ConsumerGroupClient {
    // A mutation can be repeated only with proof that the previous attempt did not write.
    async fn mutate<R>(
        &self,
        sent: &mut bool,
        make_request: impl Fn() -> R,
    ) -> std::result::Result<R::ResponseBody, OffsetCommitError>
    where
        R: message::RequestBody + Send + message::WriteType<Vec<u8>>,
        R::ResponseBody: message::ReadType<std::io::Cursor<Vec<u8>>>,
    {
        let operation = tokio::time::timeout(self.options.timeout, async {
            loop {
                let owner = self.owner().await.map_err(OffsetCommitError::Rejected)?;
                let connection = match self.connections.get_connection(&owner).await {
                    Ok(connection) => connection,
                    Err(error) if error.is_retriable() => {
                        self.invalidate_owner();
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(error) => return Err(OffsetCommitError::Rejected(error)),
                };
                match connection
                    .request_with_send_status(make_request(), sent)
                    .await
                {
                    Ok(response) => return Ok(response),
                    Err(RequestFailure::NotSent(error)) => {
                        *sent = false;
                        if !error.is_retriable() {
                            return Err(OffsetCommitError::Rejected(error));
                        }
                    }
                    Err(RequestFailure::MaybeSent(error)) if routing_error(&error) => {
                        *sent = false;
                    }
                    Err(RequestFailure::MaybeSent(error)) if definite_rejection(&error) => {
                        return Err(OffsetCommitError::Rejected(error));
                    }
                    Err(RequestFailure::MaybeSent(error)) => {
                        return Err(OffsetCommitError::Unknown(error));
                    }
                }
                self.invalidate_owner();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        operation.unwrap_or_else(|_| {
            Err(if *sent {
                OffsetCommitError::Unknown(timeout_error())
            } else {
                OffsetCommitError::Rejected(timeout_error())
            })
        })
    }

    pub(crate) async fn delete_offsets(
        &self,
        buckets: &[TableBucket],
    ) -> std::result::Result<OffsetDeleteResult, OffsetCommitError> {
        if buckets.is_empty() {
            return Ok(HashMap::new());
        }
        let mut tables: HashMap<i64, Vec<proto::PbDeleteConsumerOffsetsRequestBucket>> =
            HashMap::new();
        let expected: HashSet<_> = buckets.iter().cloned().collect();
        if expected.len() != buckets.len() {
            return Err(OffsetCommitError::Rejected(invalid(
                "Duplicate consumer offset bucket",
            )));
        }
        for bucket in buckets {
            validate_bucket(bucket).map_err(OffsetCommitError::Rejected)?;
            tables.entry(bucket.table_id()).or_default().push(
                proto::PbDeleteConsumerOffsetsRequestBucket {
                    partition_id: bucket.partition_id(),
                    bucket_id: bucket.bucket_id(),
                },
            );
        }
        let request = proto::DeleteConsumerOffsetsRequest {
            group_id: self.options.group_id.clone(),
            tables: tables
                .into_iter()
                .map(
                    |(table_id, buckets)| proto::PbDeleteConsumerOffsetsRequestTable {
                        table_id,
                        buckets,
                    },
                )
                .collect(),
        };
        let response = self
            .mutate(&mut false, || message::DeleteConsumerOffsetsRequest {
                inner_request: request.clone(),
            })
            .await?;
        let mut result = HashMap::new();
        for table in response.tables {
            for bucket in table.buckets {
                let key = TableBucket::new_with_partition(
                    table.table_id,
                    bucket.partition_id,
                    bucket.bucket_id,
                );
                if !expected.contains(&key) || result.contains_key(&key) {
                    return Err(OffsetCommitError::Unknown(invalid(
                        "Unexpected or duplicate delete response bucket",
                    )));
                }
                result.insert(
                    key,
                    deletion_result(bucket.error_code, bucket.error_message),
                );
            }
        }
        if result.len() != expected.len() {
            return Err(OffsetCommitError::Unknown(invalid(
                "Missing delete response bucket",
            )));
        }
        Ok(result)
    }

    pub(crate) async fn delete_group(&self) -> std::result::Result<(), OffsetCommitError> {
        let response = self
            .mutate(&mut false, || message::DeleteConsumerGroupsRequest {
                inner_request: proto::DeleteConsumerGroupsRequest {
                    groups: vec![self.options.group_id.clone()],
                },
            })
            .await?;
        if response.groups.len() != 1 || response.groups[0].group_id != self.options.group_id {
            return Err(OffsetCommitError::Unknown(invalid(
                "Invalid delete group response",
            )));
        }
        let group = response.groups.into_iter().next().unwrap();
        deletion_result(group.error_code, group.error_message)
    }
}

fn deletion_result(
    code: Option<i32>,
    message: Option<String>,
) -> std::result::Result<(), OffsetCommitError> {
    match code.filter(|code| *code != 0) {
        None => Ok(()),
        Some(code) => {
            let error = Error::FlussAPIError {
                api_error: api_error(code, message),
            };
            Err(if definite_rejection(&error) || routing_error(&error) {
                OffsetCommitError::Rejected(error)
            } else {
                OffsetCommitError::Unknown(error)
            })
        }
    }
}

#[cfg(test)]
mod tests;
