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
use crate::client::lookup::{LookupQueue, PrefixLookupQuery, PrimaryLookupQuery};
use crate::cluster::{Cluster, ServerType};
use crate::proto::{
    ApiVersionsResponse, PbApiVersion, PbLookupRespForBucket, PbPrefixLookupRespForBucket, PbValue,
    PbValueList,
};
use crate::rpc::ApiKey;
use crate::test_utils::build_cluster_arc_with_port;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

mod failover;

#[cfg(feature = "integration_tests")]
mod integration;

const TABLE_ID: TableId = 1;
const MAX_WAITING_PER_BUCKET: usize = 1024;

type PrimaryResult = Result<Option<Vec<u8>>>;

fn table_path() -> TablePath {
    TablePath::new("db", "tbl")
}

/// A sender whose only tablet server listens on `port`, with the channels that feed and
/// stop it.
fn sender_on_port(
    port: u16,
    max_retries: i32,
) -> (
    LookupSender,
    mpsc::Sender<QueuedLookup>,
    watch::Sender<bool>,
) {
    sender_with_backoff(port, max_retries, 100, 0, 0)
}

fn sender_with_backoff(
    port: u16,
    max_retries: i32,
    batch_timeout_ms: u64,
    retry_backoff_ms: u64,
    retry_max_backoff_ms: u64,
) -> (
    LookupSender,
    mpsc::Sender<QueuedLookup>,
    watch::Sender<bool>,
) {
    let cluster = build_cluster_arc_with_port(&table_path(), TABLE_ID, 1, u32::from(port));
    sender_with_cluster(
        cluster,
        max_retries,
        batch_timeout_ms,
        retry_backoff_ms,
        retry_max_backoff_ms,
    )
}

fn sender_with_cluster(
    cluster: Arc<Cluster>,
    max_retries: i32,
    batch_timeout_ms: u64,
    retry_backoff_ms: u64,
    retry_max_backoff_ms: u64,
) -> (
    LookupSender,
    mpsc::Sender<QueuedLookup>,
    watch::Sender<bool>,
) {
    sender_with_metadata(
        Metadata::new_for_test(cluster),
        max_retries,
        batch_timeout_ms,
        retry_backoff_ms,
        retry_max_backoff_ms,
        MAX_WAITING_PER_BUCKET,
    )
}

fn sender_with_metadata(
    metadata: Metadata,
    max_retries: i32,
    batch_timeout_ms: u64,
    retry_backoff_ms: u64,
    retry_max_backoff_ms: u64,
    max_waiting_per_bucket: usize,
) -> (
    LookupSender,
    mpsc::Sender<QueuedLookup>,
    watch::Sender<bool>,
) {
    let metadata = Arc::new(metadata);
    let (queue, lookup_tx, re_enqueue_tx) =
        LookupQueue::new(8, 8, batch_timeout_ms, metadata.subscribe_cluster_changes());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sender = LookupSender::new(
        metadata,
        queue,
        re_enqueue_tx,
        1,
        max_waiting_per_bucket,
        max_retries,
        retry_backoff_ms,
        retry_max_backoff_ms,
        shutdown_rx,
    );
    (sender, lookup_tx, shutdown_tx)
}

fn sender(max_retries: i32) -> LookupSender {
    sender_on_port(9092, max_retries).0
}

fn lookup(bucket_id: BucketId) -> (PrimaryLookupQuery, oneshot::Receiver<PrimaryResult>) {
    lookup_to(TableBucket::new(TABLE_ID, bucket_id), 1)
}

fn lookup_to(
    table_bucket: TableBucket,
    bucket_count: i32,
) -> (PrimaryLookupQuery, oneshot::Receiver<PrimaryResult>) {
    let (result_tx, result_rx) = oneshot::channel();
    let query = LookupQuery::new(
        table_path(),
        table_bucket,
        bucket_count,
        Bytes::from_static(b"key"),
        result_tx,
    );
    (query, result_rx)
}

fn fail_with_bucket_error(sender: &LookupSender, error_code: i32) -> Error {
    respond_with_bucket_error(sender, error_code)
        .try_recv()
        .expect("lookup completed")
        .expect_err("lookup failed")
}

fn respond_with_bucket_error(
    sender: &LookupSender,
    error_code: i32,
) -> oneshot::Receiver<PrimaryResult> {
    let table_bucket = TableBucket::new(TABLE_ID, 0);
    let (result_tx, result_rx) = oneshot::channel();
    let mut batch = LookupBatch::new(table_bucket.clone());
    batch.add_lookup(LookupQuery::new(
        table_path(),
        table_bucket,
        1,
        Bytes::from_static(b"key"),
        result_tx,
    ));
    let response = LookupResponse {
        buckets_resp: vec![PbLookupRespForBucket {
            bucket_id: 0,
            error_code: Some(error_code),
            error_message: Some("from the server".to_string()),
            ..Default::default()
        }],
    };
    sender.dispatcher.handle_response::<Primary>(
        TABLE_ID,
        1,
        response,
        std::slice::from_mut(&mut batch),
    );
    result_rx
}

#[test]
fn a_bucket_error_fails_its_lookups_with_the_api_error() {
    let error = fail_with_bucket_error(&sender(3), FlussError::InvalidBucketRouting.code());
    assert_eq!(error.api_error(), Some(FlussError::InvalidBucketRouting));
    assert!(error.to_string().contains("from the server"), "{error}");
}

#[test]
fn a_retriable_bucket_error_keeps_its_code_once_retries_run_out() {
    let error = fail_with_bucket_error(&sender(0), FlussError::NotLeaderOrFollower.code());
    assert_eq!(error.api_error(), Some(FlussError::NotLeaderOrFollower));
}

#[test]
fn a_routing_error_drops_the_cached_leaders() {
    let sender = sender(3);
    let table_bucket = TableBucket::new(TABLE_ID, 0);
    assert!(
        sender
            .dispatcher
            .metadata
            .get_cluster()
            .leader_for(&table_bucket)
            .is_some()
    );

    fail_with_bucket_error(&sender, FlussError::InvalidBucketRouting.code());

    assert!(
        sender
            .dispatcher
            .metadata
            .get_cluster()
            .leader_for(&table_bucket)
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn a_not_leader_error_drops_the_cached_leaders_and_retries_after_the_backoff() {
    let (mut sender, _lookup_tx, _shutdown_tx) = sender_with_backoff(9092, 3, 100, 100, 1000);
    let table_bucket = TableBucket::new(TABLE_ID, 0);

    let mut result_rx = respond_with_bucket_error(&sender, FlussError::NotLeaderOrFollower.code());

    assert!(
        sender
            .dispatcher
            .metadata
            .get_cluster()
            .leader_for(&table_bucket)
            .is_none()
    );
    assert!(matches!(
        result_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let Some(QueuedLookup::Primary(retried)) = sender.queue.drain_all().pop() else {
        panic!("the lookup was not retried");
    };
    assert_eq!(retried.retries(), 1);
    let backoff = retried.retry_at().expect("retry deadline") - Instant::now();
    assert!(
        (Duration::from_millis(80)..=Duration::from_millis(120)).contains(&backoff),
        "should back off 80..=120ms, got {backoff:?}"
    );
}

fn api_error(error: FlussError) -> Error {
    Error::FlussAPIError {
        api_error: ApiError {
            code: error.code(),
            message: String::new(),
        },
    }
}

fn closed_response_channel() -> Error {
    let (result_tx, result_rx) = oneshot::channel::<()>();
    drop(result_tx);
    Error::UnexpectedError {
        message: "Receive error: response channel closed".to_string(),
        source: Some(Box::new(
            futures::executor::block_on(result_rx).unwrap_err(),
        )),
    }
}

#[test]
fn a_failed_request_drops_the_cached_leader_and_retries_unless_resending_cannot_help() {
    for (error, retried) in [
        (closed_response_channel(), true),
        (
            Error::invalid_bucket_routing("the bucket count is stale"),
            false,
        ),
    ] {
        let mut sender = sender(3);
        let (query, mut result_rx) = lookup(0);
        let table_bucket = query.table_bucket().clone();
        let mut batch = LookupBatch::new(table_bucket.clone());
        batch.add_lookup(query);
        let context = error.to_string();

        sender.dispatcher.handle_request_error(
            1,
            std::slice::from_mut(&mut batch),
            error,
            "Lookup request failed".to_string(),
        );

        assert!(
            sender
                .dispatcher
                .metadata
                .get_cluster()
                .leader_for(&table_bucket)
                .is_none(),
            "{context}"
        );
        let retries = sender.queue.drain_all();
        if retried {
            assert_eq!(retries.len(), 1, "{context}");
            assert!(
                matches!(
                    result_rx.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ),
                "{context}"
            );
        } else {
            assert!(retries.is_empty(), "{context}");
            let error = result_rx.try_recv().unwrap().unwrap_err();
            assert_eq!(error.api_error(), Some(FlussError::InvalidBucketRouting));
        }
    }
}

#[test]
fn only_a_request_without_an_answer_or_with_stale_metadata_may_have_lost_its_leader() {
    assert!(leader_may_have_moved(&closed_response_channel()));
    assert!(leader_may_have_moved(&api_error(
        FlussError::NotLeaderOrFollower
    )));
    assert!(!leader_may_have_moved(&api_error(
        FlussError::KvStorageException
    )));
    assert!(!leader_may_have_moved(&api_error(
        FlussError::AuthenticateException
    )));
    assert!(!leader_may_have_moved(&Error::UnsupportedVersion {
        message: "The server does not support Lookup".to_string(),
    }));
    assert!(!leader_may_have_moved(&Error::UnexpectedError {
        message: "Invalid API version advertisement".to_string(),
        source: None,
    }));
}

#[test]
fn only_a_refusal_fails_the_lookups_waiting_for_a_leader() {
    for refusal in [
        api_error(FlussError::AuthenticateException),
        api_error(FlussError::UnsupportedVersion),
        Error::UnsupportedVersion {
            message: "The server does not support Authenticate".to_string(),
        },
        Error::InvalidServerType {
            message: "Expected a tablet server".to_string(),
        },
        Error::UnexpectedError {
            message: "Invalid API version advertisement".to_string(),
            source: None,
        },
    ] {
        assert!(is_refusal(&refusal), "{refusal}");
    }
    for error in [
        api_error(FlussError::UnknownServerError),
        api_error(FlussError::RequestTimeOut),
        api_error(FlussError::RetriableAuthenticateException),
        api_error(FlussError::NotLeaderOrFollower),
        Error::IllegalArgument {
            message: "Failed to resolve the bootstrap server".to_string(),
        },
    ] {
        assert!(!is_refusal(&error), "{error}");
    }
}

#[test]
fn a_lookup_whose_caller_stopped_waiting_is_not_retried() {
    let mut sender = sender(3);
    let (query, result_rx) = lookup(0);
    drop(result_rx);
    let mut batch = LookupBatch::new(query.table_bucket().clone());
    batch.add_lookup(query);
    let error = Error::UnexpectedError {
        message: "connection reset".to_string(),
        source: None,
    };

    sender
        .dispatcher
        .handle_batch_error(&error, true, &mut batch);

    assert!(sender.queue.drain_all().is_empty());
}

fn retry_backoffs(
    retry_backoff_ms: u64,
    retry_max_backoff_ms: u64,
    retries: usize,
) -> Vec<Duration> {
    let (mut sender, _lookup_tx, _shutdown_tx) =
        sender_with_backoff(9092, i32::MAX, 100, retry_backoff_ms, retry_max_backoff_ms);
    let (mut query, _result_rx) = lookup(0);
    let error = Error::UnexpectedError {
        message: "connection reset".to_string(),
        source: None,
    };
    let mut backoffs = Vec::with_capacity(retries);
    for _ in 0..retries {
        let mut batch = LookupBatch::new(query.table_bucket().clone());
        batch.add_lookup(query);
        sender
            .dispatcher
            .handle_batch_error(&error, true, &mut batch);

        let Some(QueuedLookup::Primary(retried)) = sender.queue.drain_all().pop() else {
            panic!("the lookup was not retried");
        };
        backoffs.push(retried.retry_at().expect("retry deadline") - Instant::now());
        query = retried;
    }
    backoffs
}

#[tokio::test(start_paused = true)]
async fn a_retried_lookup_backs_off_exponentially() {
    let expected_ms = [(80, 120), (160, 240), (320, 480), (640, 960), (800, 1200)];
    let backoffs = retry_backoffs(100, 1000, expected_ms.len());
    for (retry, (min_ms, max_ms)) in expected_ms.into_iter().enumerate() {
        let backoff = backoffs[retry];
        assert!(
            (Duration::from_millis(min_ms)..=Duration::from_millis(max_ms)).contains(&backoff),
            "retry {retry} should back off {min_ms}..={max_ms}ms, got {backoff:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_zero_retry_backoff_resends_immediately() {
    assert_eq!(retry_backoffs(0, 1000, 3), [Duration::ZERO; 3]);
}

async fn fail_then_answer_lookups(listener: TcpListener) -> Duration {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut failed_at: Option<Instant> = None;
    loop {
        let len = stream.read_i32().await.unwrap();
        let mut frame = vec![0u8; len as usize];
        stream.read_exact(&mut frame).await.unwrap();
        let api_key = ApiKey::from(i16::from_be_bytes([frame[0], frame[1]]));
        let error_code = failed_at
            .is_none()
            .then(|| FlussError::KvStorageException.code());
        let body = match api_key {
            ApiKey::ApiVersion => ApiVersionsResponse {
                api_versions: [ApiKey::ApiVersion, ApiKey::Lookup, ApiKey::PrefixLookup]
                    .into_iter()
                    .map(|api_key| PbApiVersion {
                        api_key: i32::from(i16::from(api_key)),
                        min_version: 0,
                        max_version: 0,
                    })
                    .collect(),
                server_type: Some(ServerType::TabletServer.to_type_id()),
            }
            .encode_to_vec(),
            ApiKey::Lookup => LookupResponse {
                buckets_resp: vec![PbLookupRespForBucket {
                    bucket_id: 0,
                    error_code,
                    values: vec![PbValue {
                        values: Some(b"value".to_vec()),
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec(),
            ApiKey::PrefixLookup => PrefixLookupResponse {
                buckets_resp: vec![PbPrefixLookupRespForBucket {
                    bucket_id: 0,
                    error_code,
                    value_lists: vec![PbValueList {
                        values: vec![b"value".to_vec()],
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec(),
            other => panic!("unexpected api key {other:?}"),
        };
        stream.write_i32(5 + body.len() as i32).await.unwrap();
        stream.write_u8(0).await.unwrap();
        stream.write_all(&frame[4..8]).await.unwrap();
        stream.write_all(&body).await.unwrap();

        if api_key != ApiKey::ApiVersion {
            match failed_at {
                None => failed_at = Some(Instant::now()),
                Some(failed_at) => return failed_at.elapsed(),
            }
        }
    }
}

#[tokio::test]
async fn a_failed_lookup_succeeds_once_resent_after_the_backoff() {
    let timeout = Duration::from_secs(10);
    for prefix in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (mut sender, lookup_tx, shutdown_tx) =
            sender_with_backoff(port, i32::MAX, 0, 100, 1000);
        let server = tokio::spawn(fail_then_answer_lookups(listener));
        let run = tokio::spawn(async move { sender.run().await });

        if prefix {
            let (result_tx, result_rx) = oneshot::channel();
            let query = PrefixLookupQuery::new(
                table_path(),
                TableBucket::new(TABLE_ID, 0),
                1,
                Bytes::from_static(b"key"),
                result_tx,
            );
            lookup_tx.send(query.into()).await.unwrap();
            let rows = tokio::time::timeout(timeout, result_rx)
                .await
                .expect("the prefix lookup completed")
                .unwrap()
                .unwrap();
            assert_eq!(rows, [b"value".to_vec()]);
        } else {
            let (query, result_rx) = lookup(0);
            lookup_tx.send(query.into()).await.unwrap();
            let value = tokio::time::timeout(timeout, result_rx)
                .await
                .expect("the lookup completed")
                .unwrap()
                .unwrap();
            assert_eq!(value.as_deref(), Some(b"value".as_slice()));
        }

        let resent_after = tokio::time::timeout(timeout, server)
            .await
            .expect("the lookup was resent")
            .unwrap();
        assert!(
            resent_after >= Duration::from_millis(80),
            "resent after {resent_after:?}"
        );
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(timeout, run)
            .await
            .expect("the sender stopped")
            .unwrap();
    }
}

#[tokio::test]
async fn a_lookup_whose_caller_stopped_waiting_is_not_sent() {
    let mut sender = sender(3);
    // The test cluster has no bucket 1, so this lookup would wait for a leader.
    let (query, result_rx) = lookup(1);
    drop(result_rx);

    sender
        .dispatcher
        .send_lookups(vec![query.into()], &mut sender.tasks)
        .await;

    assert!(sender.queue.drain_all().is_empty());
    assert_eq!(waiting_lookups(&sender.dispatcher), 0);
    assert!(sender.tasks.leader_refresher.is_empty());
}

#[tokio::test]
async fn sending_does_not_wait_for_the_server_to_answer() {
    // Accepts connections and never answers.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut sender, _lookup_tx, _shutdown_tx) =
        sender_on_port(listener.local_addr().unwrap().port(), 3);
    let (query, mut result_rx) = lookup(0);

    tokio::time::timeout(
        Duration::from_secs(10),
        sender
            .dispatcher
            .send_lookups(vec![query.into()], &mut sender.tasks),
    )
    .await
    .expect("sent without waiting for the server");

    let _connection = listener.accept().await.unwrap();
    assert!(matches!(
        result_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(sender.dispatcher.inflight_semaphore.available_permits(), 0);
}

#[tokio::test]
async fn closing_waits_for_the_requests_in_flight_and_sends_those_waiting_for_a_permit() {
    let timeout = Duration::from_secs(10);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut sender, lookup_tx, shutdown_tx) =
        sender_on_port(listener.local_addr().unwrap().port(), i32::MAX);
    let dispatcher = Arc::clone(&sender.dispatcher);
    let (primary, primary_rx) = lookup(0);
    let (prefix_tx, mut prefix_rx) = oneshot::channel();
    let prefix = PrefixLookupQuery::new(
        table_path(),
        TableBucket::new(TABLE_ID, 0),
        1,
        Bytes::from_static(b"key"),
        prefix_tx,
    );
    lookup_tx.send(primary.into()).await.unwrap();
    lookup_tx.send(prefix.into()).await.unwrap();
    let run = tokio::spawn(async move { sender.run().await });
    let (first_connection, _) = listener.accept().await.unwrap();

    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(timeout, async {
        while !dispatcher.closing.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the sender started closing");
    assert!(!run.is_finished());
    assert!(matches!(
        prefix_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    // The server goes away, so the request fails and returns its permit.
    drop(first_connection);
    let (second_connection, _) = tokio::time::timeout(timeout, listener.accept())
        .await
        .expect("the lookup waiting for a permit was sent")
        .unwrap();
    drop(second_connection);
    tokio::time::timeout(timeout, run)
        .await
        .expect("the sender stopped")
        .unwrap();
    for error in [
        primary_rx
            .await
            .expect("the primary lookup completed")
            .unwrap_err(),
        prefix_rx
            .await
            .expect("the prefix lookup completed")
            .unwrap_err(),
    ] {
        assert!(
            error
                .to_string()
                .contains("Failed to get connection to server 1"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn dropping_the_sender_aborts_its_requests_in_flight() {
    // Accepts connections and never answers.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut sender, _lookup_tx, _shutdown_tx) =
        sender_on_port(listener.local_addr().unwrap().port(), 3);
    let (query, _result_rx) = lookup(0);
    tokio::time::timeout(
        Duration::from_secs(10),
        sender
            .dispatcher
            .send_lookups(vec![query.into()], &mut sender.tasks),
    )
    .await
    .expect("sent without waiting for the server");
    let (mut connection, _) = listener.accept().await.unwrap();

    drop(sender);

    let mut received = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        connection.read_to_end(&mut received),
    )
    .await
    .expect("the client closed its connection")
    .unwrap();
}

fn waiting_lookups(dispatcher: &LookupDispatcher) -> usize {
    dispatcher.leader_waits.lock().waiting_lookups()
}
