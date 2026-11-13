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
use crate::rpc::{ApiError, RpcClient, message};
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
    /// The request may have been applied. Further commits on this scanner are blocked.
    #[error("Offset commit outcome unknown: {0}")]
    Unknown(#[source] Error),
    /// A previous commit has an unknown outcome.
    /// Fetching offsets does not fence a late write. Settle the previous writer
    /// before resuming with a new scanner.
    #[error(
        "A previous offset commit has an unknown outcome; settle it before creating a new scanner"
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

pub(crate) struct ConsumerGroup {
    table_id: i64,
    options: ConsumerGroupOptions,
    connections: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    owner: Mutex<Option<ServerNode>>,
    // Held through the response, not just dispatch: the server may reorder concurrent commits.
    // Setting true before dispatch also preserves uncertainty if the caller drops the future.
    uncertain: tokio::sync::Mutex<bool>,
    tail: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub(crate) delivered: Mutex<HashMap<TableBucket, OffsetAndMetadata>>,
}

impl ConsumerGroup {
    pub(crate) fn new(
        table_id: i64,
        options: ConsumerGroupOptions,
        connections: Arc<RpcClient>,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self {
            table_id,
            options,
            connections,
            metadata,
            owner: Mutex::new(None),
            uncertain: tokio::sync::Mutex::new(false),
            tail: Mutex::new(None),
            delivered: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn enqueue(
        self: &Arc<Self>,
        offsets: HashMap<TableBucket, OffsetAndMetadata>,
    ) -> OffsetCommitHandle {
        let (done, next) = tokio::sync::oneshot::channel();
        let previous = self.tail.lock().replace(next);
        let group = self.clone();
        let operation = tokio::spawn(async move {
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            let result = group.commit(offsets).await;
            let _ = done.send(());
            result
        });
        // Keep the queue link alive when a caller cancels its waiting handle.
        tokio::spawn(async move { operation.await? })
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
            self.metadata.reinit_cluster().await?;
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
        let mut requested: HashMap<i64, Vec<proto::PbGetConsumerOffsetsRequestBucket>> =
            HashMap::new();
        let expected: HashSet<_> = buckets.iter().cloned().collect();
        if expected.len() != buckets.len() {
            return Err(invalid("Duplicate requested consumer offset bucket"));
        }
        for bucket in buckets {
            validate_bucket(bucket.table_id(), bucket)?;
            requested.entry(bucket.table_id()).or_default().push(
                proto::PbGetConsumerOffsetsRequestBucket {
                    partition_id: bucket.partition_id(),
                    bucket_id: bucket.bucket_id(),
                },
            );
        }
        let request = proto::GetConsumerOffsetsRequest {
            group_id: self.options.group_id.clone(),
            all_offsets: Some(false),
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
                match response.and_then(|response| decode_fetch(response, &expected)) {
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

    pub(crate) async fn commit(
        &self,
        offsets: HashMap<TableBucket, OffsetAndMetadata>,
    ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
        let mut requested = Vec::with_capacity(offsets.len());
        for (bucket, value) in &offsets {
            validate_bucket(self.table_id, bucket).map_err(OffsetCommitError::Rejected)?;
            if value.offset < 0 {
                return Err(OffsetCommitError::Rejected(invalid(
                    "Committed offset must be nonnegative",
                )));
            }
            requested.push(proto::PbCommitConsumerOffsetsRequestBucket {
                partition_id: bucket.partition_id(),
                bucket_id: bucket.bucket_id(),
                offset: value.offset,
                metadata: value.metadata.clone(),
            });
        }
        let request = proto::CommitConsumerOffsetsRequest {
            group_id: self.options.group_id.clone(),
            tables: if requested.is_empty() {
                Vec::new()
            } else {
                vec![proto::PbCommitConsumerOffsetsRequestTable {
                    table_id: self.table_id,
                    buckets: requested,
                }]
            },
        };
        let mut uncertain = self.uncertain.lock().await;
        if *uncertain {
            return Err(OffsetCommitError::NeedsRecovery);
        }
        let result = tokio::time::timeout(self.options.timeout, async {
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
                *uncertain = true;
                match connection
                    .request(message::CommitConsumerOffsetsRequest {
                        inner_request: request.clone(),
                    })
                    .await
                {
                    Ok(response) => {
                        let result = decode_commit(response, &offsets)
                            .map_err(OffsetCommitError::Unknown)?;
                        *uncertain = result
                            .values()
                            .any(|v| matches!(v, OffsetCommitOutcome::Unknown(_)));
                        return Ok(result);
                    }
                    Err(error) if routing_error(&error) => {
                        *uncertain = false;
                        self.invalidate_owner();
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) if definite_rejection(&error) => {
                        *uncertain = false;
                        return Err(OffsetCommitError::Rejected(error));
                    }
                    Err(error) => return Err(OffsetCommitError::Unknown(error)),
                }
            }
        })
        .await;
        match result {
            Ok(result) => result,
            Err(_) if *uncertain => Err(OffsetCommitError::Unknown(timeout_error())),
            Err(_) => Err(OffsetCommitError::Rejected(timeout_error())),
        }
    }
}

fn validate_bucket(table_id: i64, bucket: &TableBucket) -> Result<()> {
    if bucket.table_id() != table_id {
        return Err(invalid("Consumer offset bucket belongs to another table"));
    }
    if bucket.bucket_id() < 0 || bucket.partition_id().is_some_and(|id| id < 0) {
        return Err(invalid(
            "Bucket and explicit partition IDs must be nonnegative; omit partition ID for nonpartitioned tables",
        ));
    }
    Ok(())
}

fn decode_fetch(
    response: proto::GetConsumerOffsetsResponse,
    expected: &HashSet<TableBucket>,
) -> Result<HashMap<TableBucket, Option<OffsetAndMetadata>>> {
    let mut result = HashMap::new();
    for table in response.tables {
        for bucket in table.buckets {
            let key = TableBucket::new_with_partition(
                table.table_id,
                bucket.partition_id,
                bucket.bucket_id,
            );
            if !expected.contains(&key) || result.contains_key(&key) {
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
    if result.len() != expected.len() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn response_validation_keeps_missing_offsets_and_partial_failures_distinct() {
        assert!(definite_rejection(&Error::FlussAPIError {
            api_error: api_error(48, None)
        }));
        assert!(!definite_rejection(&Error::FlussAPIError {
            api_error: api_error(999, None)
        }));
        let bucket = TableBucket::new(1, 0);
        let expected = HashSet::from([bucket.clone()]);
        let mut response = proto::GetConsumerOffsetsResponse {
            tables: vec![proto::PbGetConsumerOffsetsResponseTable {
                table_id: 1,
                buckets: vec![proto::PbGetConsumerOffsetsResponseBucket {
                    bucket_id: 0,
                    ..Default::default()
                }],
            }],
        };
        assert_eq!(
            decode_fetch(response.clone(), &expected).unwrap()[&bucket],
            None
        );
        response.tables[0].buckets[0].offset = Some(0);
        response.tables[0].buckets[0].metadata = Some(String::new());
        assert_eq!(
            decode_fetch(response.clone(), &expected).unwrap()[&bucket],
            Some(OffsetAndMetadata {
                offset: 0,
                metadata: Some(String::new())
            })
        );
        response.tables[0].buckets.clear();
        assert!(decode_fetch(response, &expected).is_err());

        let values = HashMap::from([
            (bucket.clone(), OffsetAndMetadata::new(2)),
            (TableBucket::new(1, 1), OffsetAndMetadata::new(3)),
        ]);
        let result = decode_commit(
            proto::CommitConsumerOffsetsResponse {
                tables: vec![proto::PbCommitConsumerOffsetsResponseTable {
                    table_id: 1,
                    buckets: vec![
                        proto::PbCommitConsumerOffsetsResponseBucket {
                            bucket_id: 0,
                            ..Default::default()
                        },
                        proto::PbCommitConsumerOffsetsResponseBucket {
                            bucket_id: 1,
                            error_code: Some(80),
                            ..Default::default()
                        },
                    ],
                }],
            },
            &values,
        )
        .unwrap();
        assert!(matches!(result[&bucket], OffsetCommitOutcome::Committed));
        assert!(matches!(
            result[&TableBucket::new(1, 1)],
            OffsetCommitOutcome::Rejected(_)
        ));
    }

    // A real TCP peer exercises version negotiation, request envelopes, rediscovery and
    // cancelled/unknown writes without depending on a full server's fault timing.
    #[tokio::test]
    async fn routing_retries_preserve_order_but_lost_ack_blocks_later_commits() {
        exercise_uncertain_commit(false).await;
        exercise_uncertain_commit(true).await;
    }

    async fn exercise_uncertain_commit(cancel: bool) {
        let observed_write = Arc::new(tokio::sync::Notify::new());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let commits = Arc::new(AtomicUsize::new(0));
        let discoveries = Arc::new(AtomicUsize::new(0));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let peer = {
            let commits = commits.clone();
            let discoveries = discoveries.clone();
            let recorded = recorded.clone();
            let observed_write = observed_write.clone();
            tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                while let Ok(len) = socket.read_i32().await {
                    let mut request = vec![0; len as usize];
                    socket.read_exact(&mut request).await.unwrap();
                    let key = i16::from_be_bytes(request[..2].try_into().unwrap());
                    let mut error = false;
                    let body = match key {
                        1000 => proto::ApiVersionsResponse {
                            api_versions: [1000, 1068, 1069, 1070]
                                .iter()
                                .map(|key| proto::PbApiVersion {
                                    api_key: *key,
                                    min_version: 0,
                                    max_version: 0,
                                })
                                .collect(),
                            server_type: Some(ServerType::TabletServer.to_type_id()),
                        }
                        .encode_to_vec(),
                        1068 => {
                            discoveries.fetch_add(1, Ordering::SeqCst);
                            let request =
                                proto::FindConsumerGroupOwnerRequest::decode(&request[8..])
                                    .unwrap();
                            assert_eq!(request.group_id, "consumer-offsets");
                            proto::FindConsumerGroupOwnerResponse {
                                owner: proto::PbServerNode {
                                    node_id: 0,
                                    host: "127.0.0.1".into(),
                                    port: port as i32,
                                    ..Default::default()
                                },
                            }
                            .encode_to_vec()
                        }
                        1069 => {
                            let request =
                                proto::CommitConsumerOffsetsRequest::decode(&request[8..]).unwrap();
                            let value = &request.tables[0].buckets[0];
                            assert_eq!(value.partition_id, None);
                            let attempt = commits.fetch_add(1, Ordering::SeqCst);
                            if attempt < 3 || matches!(value.offset, 21 | 22) {
                                error = true;
                                proto::ErrorResponse {
                                    error_code: match value.offset {
                                        21 => 81,
                                        22 => 48,
                                        _ => 76 + attempt as i32,
                                    },
                                    error_message: None,
                                }
                                .encode_to_vec()
                            } else if value.offset == 30 {
                                // Keep the socket alive but withhold the ACK after observing the write.
                                recorded.lock().push(value.offset);
                                observed_write.notify_one();
                                continue;
                            } else {
                                recorded.lock().push(value.offset);
                                proto::CommitConsumerOffsetsResponse {
                                    tables: vec![proto::PbCommitConsumerOffsetsResponseTable {
                                        table_id: 1,
                                        buckets: vec![
                                            proto::PbCommitConsumerOffsetsResponseBucket {
                                                bucket_id: 0,
                                                ..Default::default()
                                            },
                                        ],
                                    }],
                                }
                                .encode_to_vec()
                            }
                        }
                        _ => panic!("unexpected API {key}"),
                    };
                    let mut response = vec![u8::from(error)];
                    response.extend_from_slice(&request[4..8]);
                    response.extend_from_slice(&body);
                    socket.write_i32(response.len() as i32).await.unwrap();
                    socket.write_all(&response).await.unwrap();
                }
            })
        };
        let node = ServerNode::new(
            0,
            "127.0.0.1".into(),
            u32::from(port),
            ServerType::TabletServer,
        );
        let cluster = crate::cluster::Cluster::new(
            None,
            HashMap::from([(0, node)]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        let group = Arc::new(ConsumerGroup::new(
            1,
            ConsumerGroupOptions {
                group_id: "consumer-offsets".into(),
                timeout: Duration::from_secs(2),
            },
            Arc::new(RpcClient::new()),
            Arc::new(Metadata::new_for_test(Arc::new(cluster))),
        ));
        let offsets =
            |offset| HashMap::from([(TableBucket::new(1, 0), OffsetAndMetadata::new(offset))]);
        let first = group.enqueue(offsets(10));
        let cancelled = group.enqueue(offsets(15));
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        let second = group.enqueue(offsets(20));
        // Dropping a waiting handle must not cancel or reorder the first write.
        drop(first);
        assert!(matches!(
            second.await.unwrap().unwrap()[&TableBucket::new(1, 0)],
            OffsetCommitOutcome::Committed
        ));
        assert_eq!(*recorded.lock(), vec![10, 15, 20]);
        assert_eq!(discoveries.load(Ordering::SeqCst), 4);
        for (offset, code) in [(21, 81), (22, 48)] {
            assert!(matches!(
                group.enqueue(offsets(offset)).await.unwrap(),
                Err(OffsetCommitError::Rejected(Error::FlussAPIError { api_error }))
                    if api_error.code == code
            ));
        }
        assert!(group.enqueue(offsets(25)).await.unwrap().is_ok());
        let pending = group.enqueue(offsets(30));
        if cancel {
            tokio::time::timeout(Duration::from_secs(2), observed_write.notified())
                .await
                .unwrap();
            pending.abort();
            assert!(pending.await.unwrap_err().is_cancelled());
        } else {
            assert!(matches!(
                pending.await.unwrap(),
                Err(OffsetCommitError::Unknown(_))
            ));
        }
        assert!(matches!(
            group.enqueue(offsets(40)).await.unwrap(),
            Err(OffsetCommitError::NeedsRecovery)
        ));
        assert_eq!(*recorded.lock(), vec![10, 15, 20, 25, 30]);
        assert_eq!(commits.load(Ordering::SeqCst), 10);
        peer.abort();
    }
}
