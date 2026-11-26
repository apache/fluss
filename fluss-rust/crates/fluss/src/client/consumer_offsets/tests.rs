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

use super::*;
use prost::Message;
use std::collections::VecDeque;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn response_validation_keeps_missing_offsets_and_partial_failures_distinct() {
    assert!(definite_rejection(&Error::FlussAPIError {
        api_error: api_error(81, None)
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

enum Reply {
    Ack,
    Reject(i32),
    Disconnect,
    Withhold,
}

struct CommitPeer {
    group: Arc<ConsumerGroup>,
    received: Arc<Mutex<Vec<i64>>>,
    worker: tokio::task::JoinHandle<()>,
}

impl Drop for CommitPeer {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl CommitPeer {
    // Each action applies to the next commit; remaining requests are acknowledged.
    async fn start(script: impl IntoIterator<Item = Reply>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let script = Arc::new(Mutex::new(VecDeque::from_iter(script)));
        let received = Arc::new(Mutex::new(Vec::new()));
        let log = received.clone();
        let worker = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let script = script.clone();
                let log = log.clone();
                connections.spawn(async move {
                    while let Ok(len) = socket.read_i32().await {
                        let mut frame = vec![0; len as usize];
                        socket.read_exact(&mut frame).await.unwrap();
                        let key = i16::from_be_bytes(frame[..2].try_into().unwrap());
                        let mut error = false;
                        let body = match key {
                            1000 => proto::ApiVersionsResponse {
                                api_versions: [1000, 1068, 1069]
                                    .into_iter()
                                    .map(|api_key| proto::PbApiVersion {
                                        api_key,
                                        min_version: 0,
                                        max_version: 0,
                                    })
                                    .collect(),
                                server_type: Some(ServerType::TabletServer.to_type_id()),
                            }
                            .encode_to_vec(),
                            1068 => proto::FindConsumerGroupOwnerResponse {
                                owner: proto::PbServerNode {
                                    node_id: 1,
                                    host: "127.0.0.1".into(),
                                    port: port.into(),
                                    ..Default::default()
                                },
                            }
                            .encode_to_vec(),
                            1069 => {
                                let request =
                                    proto::CommitConsumerOffsetsRequest::decode(&frame[8..])
                                        .unwrap();
                                let bucket = &request.tables[0].buckets[0];
                                log.lock().push(bucket.offset);
                                match script.lock().pop_front().unwrap_or(Reply::Ack) {
                                    Reply::Disconnect => break,
                                    Reply::Withhold => continue,
                                    Reply::Reject(code) => {
                                        error = true;
                                        proto::ErrorResponse {
                                            error_code: code,
                                            error_message: None,
                                        }
                                        .encode_to_vec()
                                    }
                                    Reply::Ack => proto::CommitConsumerOffsetsResponse {
                                        tables: vec![proto::PbCommitConsumerOffsetsResponseTable {
                                            table_id: request.tables[0].table_id,
                                            buckets: vec![
                                                proto::PbCommitConsumerOffsetsResponseBucket {
                                                    bucket_id: bucket.bucket_id,
                                                    ..Default::default()
                                                },
                                            ],
                                        }],
                                    }
                                    .encode_to_vec(),
                                }
                            }
                            _ => panic!("unexpected API {key}"),
                        };
                        socket.write_i32((5 + body.len()) as i32).await.unwrap();
                        socket.write_u8(error.into()).await.unwrap();
                        socket.write_all(&frame[4..8]).await.unwrap();
                        socket.write_all(&body).await.unwrap();
                    }
                });
            }
        });
        let cluster = crate::test_utils::build_cluster_with_tables(&[], port.into());
        let group = Arc::new(ConsumerGroup::new(
            1,
            ConsumerGroupOptions {
                group_id: "test".into(),
                timeout: Duration::from_secs(2),
            },
            Arc::new(RpcClient::new()),
            Arc::new(Metadata::new_for_test(Arc::new(cluster))),
        ));
        Self {
            group,
            received,
            worker,
        }
    }

    async fn connection(&self) -> crate::rpc::ServerConnection {
        let owner = self.group.client.owner().await.unwrap();
        self.group
            .client
            .connections
            .get_connection(&owner)
            .await
            .unwrap()
    }

    fn enqueue(&self, offset: i64) -> Result<PendingCommit> {
        self.group.enqueue(HashMap::from([(
            TableBucket::new(1, 0),
            OffsetAndMetadata::new(offset),
        )]))
    }

    async fn commit(
        &self,
        offset: i64,
    ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
        self.enqueue(offset).unwrap().await
    }
}

#[tokio::test]
async fn commit_rediscovers_the_owner_after_it_moves() {
    let old = CommitPeer::start([Reply::Reject(78)]).await;
    let new = CommitPeer::start([]).await;
    *new.group.client.owner.lock() = Some(old.group.client.owner().await.unwrap());
    new.commit(10).await.unwrap();
    assert_eq!(*old.received.lock(), [10]);
    assert_eq!(*new.received.lock(), [10]);
}

#[tokio::test]
async fn disconnect_before_sending_allows_retry_and_rewind() {
    let peer = CommitPeer::start([]).await;
    let connection = peer.connection().await;
    connection
        .close_before_write_for_test(peer.commit(10))
        .await
        .unwrap();
    peer.commit(5).await.unwrap();
    assert_eq!(*peer.received.lock(), [10, 5]);
}

#[tokio::test]
async fn retry_after_lost_ack_cannot_fence_the_first_write() {
    let peer = CommitPeer::start([Reply::Disconnect]).await;
    peer.commit(10).await.unwrap();
    assert_eq!(*peer.received.lock(), [10, 10]);
    peer.commit(11).await.unwrap();
    assert!(matches!(
        peer.commit(9).await,
        Err(OffsetCommitError::NeedsRecovery)
    ));
    let mut changed = OffsetAndMetadata::new(10);
    changed.metadata = Some("new".into());
    assert!(matches!(
        peer.group
            .enqueue(HashMap::from([(TableBucket::new(1, 0), changed)]))
            .unwrap()
            .await,
        Err(OffsetCommitError::NeedsRecovery)
    ));
    let other = TableBucket::new(1, 1);
    peer.group
        .enqueue(HashMap::from([(other, OffsetAndMetadata::new(1))]))
        .unwrap()
        .await
        .unwrap();
}

#[tokio::test]
async fn rejection_after_lost_ack_leaves_outcome_unknown() {
    let peer = CommitPeer::start([Reply::Disconnect, Reply::Reject(48)]).await;
    assert!(matches!(
        peer.commit(10).await,
        Err(OffsetCommitError::Unknown(_))
    ));
}

#[tokio::test]
async fn timeout_before_sending_leaves_no_late_write() {
    let peer = CommitPeer::start([]).await;
    let result = peer
        .connection()
        .await
        .with_write_lock_for_test(peer.commit(10))
        .await;
    assert!(matches!(result, Err(OffsetCommitError::Rejected(_))));
    peer.commit(9).await.unwrap();
    assert_eq!(*peer.received.lock(), [9]);
}

#[tokio::test]
async fn timeout_after_sending_guards_a_rewind() {
    let peer = CommitPeer::start([Reply::Withhold]).await;
    assert!(matches!(
        peer.commit(10).await,
        Err(OffsetCommitError::Unknown(_))
    ));
    peer.commit(11).await.unwrap();
    assert!(matches!(
        peer.commit(9).await,
        Err(OffsetCommitError::NeedsRecovery)
    ));
}

#[tokio::test]
async fn callbacks_are_delivered_in_order_by_the_next_sync_commit() {
    let peer = CommitPeer::start([]).await;
    let received = Arc::new(Mutex::new(Vec::new()));
    let offsets = |id| HashMap::from([(TableBucket::new(1, 0), OffsetAndMetadata::new(id))]);
    for id in [1, 2] {
        let received = received.clone();
        peer.group
            .enqueue_callback(offsets(id), move |result| {
                assert!(matches!(
                    result.unwrap()[&TableBucket::new(1, 0)],
                    OffsetCommitOutcome::Committed
                ));
                received.lock().push(id);
            })
            .unwrap();
    }
    peer.enqueue(3).unwrap().await.unwrap();
    assert!(received.lock().is_empty());
    peer.group.commit_sync(offsets(4)).await.unwrap();
    assert_eq!(*received.lock(), [1, 2]);
    peer.group.await_pending_commits().await;
    assert_eq!(*received.lock(), [1, 2]);
}

#[tokio::test]
async fn dropping_handle_preserves_queue_order_and_capacity() {
    let peer = CommitPeer::start([]).await;
    let connection = peer.connection().await;
    let mut last = None;
    connection
        .with_write_lock_for_test(async {
            drop(peer.enqueue(0).unwrap());
            for offset in 1..MAX_PENDING_COMMITS {
                last = Some(peer.enqueue(offset as i64).unwrap());
            }
            assert!(matches!(
                peer.enqueue(100),
                Err(Error::BufferExhausted { .. })
            ));
        })
        .await;
    last.unwrap().await.unwrap();
    assert_eq!(
        *peer.received.lock(),
        (0..MAX_PENDING_COMMITS as i64).collect::<Vec<_>>()
    );
    peer.commit(100).await.unwrap();
}

#[tokio::test]
async fn handshake_timeout_discards_stale_owner_for_fetch_and_commit() {
    let peer = CommitPeer::start([]).await;
    // TCP connects, but the old owner never answers ApiVersions.
    let old = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stale = ServerNode::new(
        2,
        "127.0.0.1".into(),
        old.local_addr().unwrap().port().into(),
        ServerType::TabletServer,
    );
    *peer.group.client.owner.lock() = Some(stale.clone());
    assert!(matches!(
        peer.commit(10).await,
        Err(OffsetCommitError::Rejected(_))
    ));
    peer.commit(8).await.unwrap();
    *peer.group.client.owner.lock() = Some(stale);
    assert!(
        peer.group
            .fetch_committed_offsets(&[TableBucket::new(1, 0)])
            .await
            .is_err()
    );
    peer.commit(9).await.unwrap();
    assert_eq!(*peer.received.lock(), [8, 9]);
}

#[tokio::test]
async fn enqueue_without_runtime_returns_an_error() {
    let peer = CommitPeer::start([]).await;
    let group = peer.group.clone();
    let result = std::thread::spawn(move || group.enqueue(HashMap::new()))
        .join()
        .unwrap();
    assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    peer.commit(10).await.unwrap();
}
