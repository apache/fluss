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
use crate::cluster::ServerNode;
use crate::metadata::{DataTypes, JsonSerde, Schema, TableDescriptor};
use crate::proto::{
    AuthenticateResponse, ErrorResponse, MetadataResponse, PbBucketMetadata, PbPartitionMetadata,
    PbServerNode, PbTableMetadata, PbTablePath,
};
use crate::rpc::message::REQUEST_HEADER_LENGTH;
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU64, AtomicUsize};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;

const TIMEOUT: Duration = Duration::from_secs(10);
const PARTITION_ID: PartitionId = 7;
const BUCKET_COUNT: i32 = 3;

type PrefixResult = Result<Vec<Vec<u8>>>;
type LookupResults = (
    oneshot::Receiver<PrimaryResult>,
    oneshot::Receiver<PrefixResult>,
);

#[derive(Default)]
struct FakeClusterState {
    servers: Mutex<HashMap<i32, u16>>,
    leaders: Mutex<HashMap<BucketId, Option<i32>>>,
    partition: Mutex<Option<(PartitionId, String)>>,
    metadata_requests: AtomicUsize,
    requested_partition_ids: Mutex<HashSet<PartitionId>>,
    table_dropped: AtomicBool,
    table_recreations: AtomicI64,
}

impl FakeClusterState {
    fn set(&self, servers: &[(i32, u16)], leaders: &[(BucketId, Option<i32>)]) {
        *self.servers.lock() = servers.iter().copied().collect();
        *self.leaders.lock() = leaders.iter().copied().collect();
    }

    fn set_partition(&self, partition_id: PartitionId, partition_name: &str) {
        *self.partition.lock() = Some((partition_id, partition_name.to_string()));
    }

    fn metadata_requests(&self) -> usize {
        self.metadata_requests.load(Ordering::Relaxed)
    }

    fn table_id(&self) -> TableId {
        TABLE_ID + self.table_recreations.load(Ordering::Acquire)
    }

    fn drop_table(&self) {
        self.table_dropped.store(true, Ordering::Release);
    }

    fn recreate_table(&self) {
        self.table_recreations.fetch_add(1, Ordering::AcqRel);
        self.table_dropped.store(false, Ordering::Release);
    }

    fn routing_error(
        &self,
        partition_id: Option<PartitionId>,
        bucket_id: BucketId,
        routing_bucket_count: Option<i32>,
    ) -> Option<FlussError> {
        let cached_partition_id = self.partition.lock().as_ref().map(|(id, _)| *id);
        if partition_id != cached_partition_id || !(0..BUCKET_COUNT).contains(&bucket_id) {
            Some(FlussError::UnknownTableOrBucketException)
        } else if routing_bucket_count != Some(BUCKET_COUNT) {
            Some(FlussError::InvalidBucketRouting)
        } else {
            None
        }
    }

    fn record_metadata_request(&self, request: &[u8]) -> Option<FlussError> {
        self.metadata_requests.fetch_add(1, Ordering::Relaxed);
        let request = crate::proto::MetadataRequest::decode(request).unwrap();
        for path in &request.table_path {
            match path.table_name.as_str() {
                "dropped" => return Some(FlussError::TableNotExist),
                "denied" => return Some(FlussError::AuthorizationException),
                "missing_schema" => return Some(FlussError::SchemaNotExist),
                "tbl" if self.table_dropped.load(Ordering::Acquire) => {
                    return Some(FlussError::TableNotExist);
                }
                _ => {}
            }
        }
        let partition_id = self.partition.lock().as_ref().map(|(id, _)| *id);
        let asks_for_dropped_partition = request
            .partitions_id
            .iter()
            .any(|id| Some(*id) != partition_id);
        self.requested_partition_ids
            .lock()
            .extend(request.partitions_id);
        asks_for_dropped_partition.then_some(FlussError::PartitionNotExists)
    }

    fn metadata_response(&self) -> MetadataResponse {
        let table_id = self.table_id();
        let partition = self.partition.lock().clone();
        let mut schema = Schema::builder().column("id", DataTypes::int());
        if partition.is_some() {
            schema = schema.column("p", DataTypes::string());
        }
        let descriptor = TableDescriptor::builder()
            .schema(schema.build().unwrap())
            .partitioned_by(if partition.is_some() {
                vec!["p"]
            } else {
                vec![]
            })
            .distributed_by(Some(BUCKET_COUNT), vec![])
            .build()
            .unwrap();
        let bucket_metadata: Vec<PbBucketMetadata> = self
            .leaders
            .lock()
            .iter()
            .map(|(&bucket_id, &leader_id)| PbBucketMetadata {
                bucket_id,
                leader_id,
                ..Default::default()
            })
            .collect();
        let (table_bucket_metadata, partition_metadata) = match partition {
            None => (bucket_metadata, Vec::new()),
            Some((partition_id, partition_name)) => (
                Vec::new(),
                vec![PbPartitionMetadata {
                    table_id,
                    partition_name,
                    partition_id,
                    bucket_metadata,
                    bucket_count: Some(BUCKET_COUNT),
                }],
            ),
        };
        MetadataResponse {
            coordinator_server: None,
            tablet_servers: self
                .servers
                .lock()
                .iter()
                .map(|(&node_id, &port)| PbServerNode {
                    node_id,
                    host: "127.0.0.1".to_string(),
                    port: i32::from(port),
                    ..Default::default()
                })
                .collect(),
            table_metadata: vec![PbTableMetadata {
                table_path: PbTablePath {
                    database_name: "db".to_string(),
                    table_name: "tbl".to_string(),
                },
                table_id,
                schema_id: 1,
                table_json: serde_json::to_vec(&descriptor.serialize_json().unwrap()).unwrap(),
                bucket_metadata: table_bucket_metadata,
                ..Default::default()
            }],
            partition_metadata,
        }
    }

    fn cluster(&self) -> Arc<Cluster> {
        Arc::new(Cluster::from_metadata_response(self.metadata_response(), None).unwrap())
    }
}

struct FakeTabletServer {
    port: u16,
    down: AtomicBool,
    metadata_requests: AtomicU64,
    metadata_answers: watch::Sender<u64>,
    lookup_error: Mutex<Option<FlussError>>,
    metadata_error: Mutex<Option<FlussError>>,
    rejects_credentials: AtomicBool,
    advertises_authenticate: AtomicBool,
    server_type: AtomicI32,
    invalid_api_versions: AtomicBool,
}

impl FakeTabletServer {
    async fn start(state: &Arc<FakeClusterState>) -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = Arc::new(Self {
            port: listener.local_addr().unwrap().port(),
            down: AtomicBool::new(false),
            metadata_requests: AtomicU64::new(0),
            metadata_answers: watch::Sender::new(u64::MAX),
            lookup_error: Mutex::new(None),
            metadata_error: Mutex::new(None),
            rejects_credentials: AtomicBool::new(false),
            advertises_authenticate: AtomicBool::new(true),
            server_type: AtomicI32::new(ServerType::TabletServer.to_type_id()),
            invalid_api_versions: AtomicBool::new(false),
        });
        let serving = Arc::clone(&server);
        let state = Arc::clone(state);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(Arc::clone(&serving).serve(stream, Arc::clone(&state)));
            }
        });
        server
    }

    fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::Release);
    }

    fn metadata_requests(&self) -> u64 {
        self.metadata_requests.load(Ordering::Acquire)
    }

    fn hold_metadata(&self) {
        self.metadata_answers.send_replace(self.metadata_requests());
    }

    fn answer_metadata(&self, count: u64) {
        self.metadata_answers
            .send_modify(|answers| *answers = answers.saturating_add(count));
    }

    fn answer_all_metadata(&self) {
        self.metadata_answers.send_replace(u64::MAX);
    }

    fn reject_lookups(&self, error: FlussError) {
        *self.lookup_error.lock() = Some(error);
    }

    fn reject_metadata(&self, error: FlussError) {
        *self.metadata_error.lock() = Some(error);
    }

    fn accept_metadata(&self) {
        *self.metadata_error.lock() = None;
    }

    fn reject_credentials(&self) {
        self.rejects_credentials.store(true, Ordering::Release);
    }

    fn stop_advertising_authenticate(&self) {
        self.advertises_authenticate.store(false, Ordering::Release);
    }

    async fn serve(self: Arc<Self>, stream: TcpStream, state: Arc<FakeClusterState>) {
        let (mut reader, writer) = stream.into_split();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));
        loop {
            let Ok(len) = reader.read_i32().await else {
                return;
            };
            let mut frame = vec![0u8; len as usize];
            if reader.read_exact(&mut frame).await.is_err() || self.down.load(Ordering::Acquire) {
                let _ = writer.lock().await.shutdown().await;
                return;
            }
            let api_key = ApiKey::from(i16::from_be_bytes([frame[0], frame[1]]));
            let request_id: [u8; 4] = frame[4..8].try_into().unwrap();
            let request = &frame[REQUEST_HEADER_LENGTH..];
            let body = match api_key {
                ApiKey::ApiVersion => Ok(ApiVersionsResponse {
                    api_versions: [
                        ApiKey::ApiVersion,
                        ApiKey::Authenticate,
                        ApiKey::MetaData,
                        ApiKey::Lookup,
                        ApiKey::PrefixLookup,
                    ]
                    .into_iter()
                    .filter(|api_key| {
                        *api_key != ApiKey::Authenticate
                            || self.advertises_authenticate.load(Ordering::Acquire)
                    })
                    .map(|api_key| PbApiVersion {
                        api_key: if self.invalid_api_versions.load(Ordering::Acquire) {
                            i32::MAX
                        } else {
                            i32::from(i16::from(api_key))
                        },
                        min_version: 0,
                        max_version: 0,
                    })
                    .collect(),
                    server_type: Some(self.server_type.load(Ordering::Acquire)),
                }
                .encode_to_vec()),
                ApiKey::Authenticate => {
                    if self.rejects_credentials.load(Ordering::Acquire) {
                        Err(FlussError::AuthenticateException)
                    } else {
                        Ok(AuthenticateResponse::default().encode_to_vec())
                    }
                }
                ApiKey::MetaData => {
                    let request_error = state.record_metadata_request(request);
                    let error = (*self.metadata_error.lock()).or(request_error);
                    let request_number = self.metadata_requests.fetch_add(1, Ordering::AcqRel) + 1;
                    let request = crate::proto::MetadataRequest::decode(request).unwrap();
                    let mut answers = self.metadata_answers.subscribe();
                    let (state, writer) = (Arc::clone(&state), Arc::clone(&writer));
                    tokio::spawn(async move {
                        if answers
                            .wait_for(|answers| *answers >= request_number)
                            .await
                            .is_ok()
                        {
                            let body = match error {
                                Some(error) => Err(error),
                                None => {
                                    let mut response = state.metadata_response();
                                    response.table_metadata.retain(|table| {
                                        request.table_path.contains(&table.table_path)
                                    });
                                    response.partition_metadata.retain(|partition| {
                                        request.partitions_id.contains(&partition.partition_id)
                                    });
                                    Ok(response.encode_to_vec())
                                }
                            };
                            respond(&writer, request_id, body).await;
                        }
                    });
                    continue;
                }
                ApiKey::Lookup | ApiKey::PrefixLookup => match *self.lookup_error.lock() {
                    Some(error) => Err(error),
                    None => Ok(lookup_response(api_key, request, &state)),
                },
                other => panic!("unexpected api key {other:?}"),
            };
            if !respond(&writer, request_id, body).await {
                return;
            }
        }
    }
}

fn lookup_response(api_key: ApiKey, request: &[u8], state: &FakeClusterState) -> Vec<u8> {
    if api_key == ApiKey::Lookup {
        let request = crate::proto::LookupRequest::decode(request).unwrap();
        return LookupResponse {
            buckets_resp: request
                .buckets_req
                .into_iter()
                .map(|bucket| PbLookupRespForBucket {
                    partition_id: bucket.partition_id,
                    bucket_id: bucket.bucket_id,
                    error_code: state
                        .routing_error(
                            bucket.partition_id,
                            bucket.bucket_id,
                            bucket.routing_bucket_count,
                        )
                        .map(|error| error.code()),
                    values: bucket
                        .keys
                        .iter()
                        .map(|_| PbValue {
                            values: Some(b"value".to_vec()),
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
        }
        .encode_to_vec();
    }
    let request = crate::proto::PrefixLookupRequest::decode(request).unwrap();
    PrefixLookupResponse {
        buckets_resp: request
            .buckets_req
            .into_iter()
            .map(|bucket| PbPrefixLookupRespForBucket {
                partition_id: bucket.partition_id,
                bucket_id: bucket.bucket_id,
                error_code: state
                    .routing_error(
                        bucket.partition_id,
                        bucket.bucket_id,
                        bucket.routing_bucket_count,
                    )
                    .map(|error| error.code()),
                value_lists: bucket
                    .keys
                    .iter()
                    .map(|_| PbValueList {
                        values: vec![b"value".to_vec()],
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
    }
    .encode_to_vec()
}

async fn respond(
    writer: &tokio::sync::Mutex<OwnedWriteHalf>,
    request_id: [u8; 4],
    body: std::result::Result<Vec<u8>, FlussError>,
) -> bool {
    let (response_type, body) = match body {
        Ok(body) => (0, body),
        Err(error) => (
            1,
            ErrorResponse {
                error_code: error.code(),
                error_message: Some(error.message().to_string()),
            }
            .encode_to_vec(),
        ),
    };
    let mut response = Vec::with_capacity(9 + body.len());
    response.extend_from_slice(&(5 + body.len() as i32).to_be_bytes());
    response.push(response_type);
    response.extend_from_slice(&request_id);
    response.extend_from_slice(&body);
    writer.lock().await.write_all(&response).await.is_ok()
}

fn lookups_to(table_bucket: TableBucket) -> ([QueuedLookup; 2], LookupResults) {
    lookups_to_table(table_path(), table_bucket)
}

fn lookups_to_table(
    path: TablePath,
    table_bucket: TableBucket,
) -> ([QueuedLookup; 2], LookupResults) {
    let (primary_tx, primary_rx) = oneshot::channel();
    let primary = PrimaryLookupQuery::new(
        path.clone(),
        table_bucket.clone(),
        BUCKET_COUNT,
        Bytes::from_static(b"key"),
        primary_tx,
    );
    let (prefix_tx, prefix_rx) = oneshot::channel();
    let prefix = PrefixLookupQuery::new(
        path,
        table_bucket,
        BUCKET_COUNT,
        Bytes::from_static(b"key"),
        prefix_tx,
    );
    ([primary.into(), prefix.into()], (primary_rx, prefix_rx))
}

async fn send_lookups_to(
    lookup_tx: &mpsc::Sender<QueuedLookup>,
    table_bucket: TableBucket,
) -> LookupResults {
    let (lookups, results) = lookups_to(table_bucket);
    for lookup in lookups {
        lookup_tx.send(lookup).await.unwrap();
    }
    results
}

async fn send_lookups_for(
    lookup_tx: &mpsc::Sender<QueuedLookup>,
    bucket_id: BucketId,
) -> LookupResults {
    send_lookups_to(lookup_tx, TableBucket::new(TABLE_ID, bucket_id)).await
}

async fn assert_found((primary_rx, prefix_rx): LookupResults) {
    let value = tokio::time::timeout(TIMEOUT, primary_rx)
        .await
        .expect("the lookup completed")
        .unwrap()
        .unwrap();
    assert_eq!(value.as_deref(), Some(b"value".as_slice()));
    let rows = tokio::time::timeout(TIMEOUT, prefix_rx)
        .await
        .expect("the prefix lookup completed")
        .unwrap()
        .unwrap();
    assert_eq!(rows, [b"value".to_vec()]);
}

async fn errors_of((primary_rx, prefix_rx): LookupResults) -> [Error; 2] {
    [
        tokio::time::timeout(TIMEOUT, primary_rx)
            .await
            .expect("the lookup completed")
            .unwrap()
            .unwrap_err(),
        tokio::time::timeout(TIMEOUT, prefix_rx)
            .await
            .expect("the prefix lookup completed")
            .unwrap()
            .unwrap_err(),
    ]
}

fn assert_pending(results: &mut LookupResults) {
    assert!(matches!(
        results.0.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        results.1.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
}

async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(TIMEOUT, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

async fn stop(shutdown_tx: watch::Sender<bool>, run: tokio::task::JoinHandle<()>) {
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(TIMEOUT, run)
        .await
        .expect("the sender stopped")
        .unwrap();
}

async fn send_and_wait(sender: &mut LookupSender, lookups: Vec<QueuedLookup>) {
    sender
        .dispatcher
        .send_lookups(lookups, &mut sender.tasks)
        .await;
    tokio::time::timeout(TIMEOUT, async {
        while sender.tasks.requests.join_next().await.is_some() {}
    })
    .await
    .expect("the requests completed");
}

fn cached_leader(dispatcher: &LookupDispatcher, bucket_id: BucketId) -> Option<i32> {
    dispatcher
        .metadata
        .get_cluster()
        .leader_for(&TableBucket::new(TABLE_ID, bucket_id))
        .map(ServerNode::id)
}

fn failed_refreshes(dispatcher: &LookupDispatcher, bucket_id: BucketId) -> Option<i32> {
    dispatcher
        .leader_waits
        .lock()
        .failed_refreshes(&TableBucket::new(TABLE_ID, bucket_id))
}

#[tokio::test]
async fn a_late_not_leader_response_keeps_the_replacement_and_other_buckets_leaders() {
    let state = FakeClusterState::default();
    state.set(&[(1, 9092), (2, 9093)], &[(0, Some(1)), (1, Some(1))]);
    let (sender, _lookup_tx, _shutdown_tx) = sender_with_cluster(state.cluster(), 3, 0, 0, 0);
    state.set(&[(1, 9092), (2, 9093)], &[(0, Some(2)), (1, Some(1))]);
    sender
        .dispatcher
        .metadata
        .update(state.metadata_response())
        .await
        .unwrap();

    let _result_rx = respond_with_bucket_error(&sender, FlussError::NotLeaderOrFollower.code());

    assert_eq!(cached_leader(&sender.dispatcher, 0), Some(2));
    assert_eq!(cached_leader(&sender.dispatcher, 1), Some(1));
}

#[tokio::test]
async fn a_lookup_to_a_dropped_or_inaccessible_table_fails_with_its_refresh_error() {
    for (name, expected_error) in [
        ("dropped", FlussError::TableNotExist),
        ("denied", FlussError::AuthorizationException),
        ("hidden", FlussError::AuthorizationException),
    ] {
        let state = Arc::new(FakeClusterState::default());
        let server = FakeTabletServer::start(&state).await;
        state.set(&[(1, server.port)], &[(0, Some(1))]);
        let (mut sender, _lookup_tx, shutdown_tx) =
            sender_with_cluster(state.cluster(), 3, 0, 100, 1000);
        let (lookups, results) = lookups_to_table(
            TablePath::new("db", name),
            TableBucket::new(TABLE_ID + 1, 0),
        );
        sender
            .dispatcher
            .send_lookups(lookups.into(), &mut sender.tasks)
            .await;
        let run = tokio::spawn(async move { sender.run().await });

        for error in errors_of(results).await {
            assert_eq!(error.api_error(), Some(expected_error), "{name}");
        }
        stop(shutdown_tx, run).await;
    }
}

#[tokio::test]
async fn a_lookup_to_a_table_or_partition_dropped_after_its_leader_was_cached_fails() {
    for partitioned in [false, true] {
        let state = Arc::new(FakeClusterState::default());
        let server = FakeTabletServer::start(&state).await;
        if partitioned {
            state.set_partition(PARTITION_ID, "p1");
        }
        state.set(&[(1, server.port)], &[(0, Some(1))]);
        let table_bucket =
            TableBucket::new_with_partition(TABLE_ID, partitioned.then_some(PARTITION_ID), 0);
        let (mut sender, lookup_tx, shutdown_tx) =
            sender_with_cluster(state.cluster(), i32::MAX, 0, 100, 1000);
        let dispatcher = Arc::clone(&sender.dispatcher);
        let run = tokio::spawn(async move { sender.run().await });
        assert_found(send_lookups_to(&lookup_tx, table_bucket.clone()).await).await;

        let expected_error = if partitioned {
            state.set_partition(PARTITION_ID + 1, "p2");
            FlussError::PartitionNotExists
        } else {
            state.drop_table();
            server.reject_lookups(FlussError::UnknownTableOrBucketException);
            FlussError::TableNotExist
        };

        for error in errors_of(send_lookups_to(&lookup_tx, table_bucket.clone()).await).await {
            assert_eq!(error.api_error(), Some(expected_error), "{partitioned}");
        }
        let cluster = dispatcher.metadata.get_cluster();
        assert!(cluster.leader_for(&table_bucket).is_none(), "{partitioned}");
        assert_eq!(waiting_lookups(&dispatcher), 0, "{partitioned}");
        stop(shutdown_tx, run).await;
    }
}

#[tokio::test]
async fn lookups_to_dropped_or_inaccessible_tables_fail_while_the_others_of_their_refresh_go_on() {
    for partitioned in [false, true] {
        let state = Arc::new(FakeClusterState::default());
        let server = FakeTabletServer::start(&state).await;
        if partitioned {
            state.set_partition(PARTITION_ID, "p1");
        }
        state.set(&[(1, server.port)], &[(0, None)]);
        let (mut sender, _lookup_tx, shutdown_tx) =
            sender_with_cluster(state.cluster(), 0, 0, 60_000, 60_000);
        state.set(&[(1, server.port)], &[(0, Some(1))]);
        let mut inaccessible_tables = vec![("hidden", TABLE_ID + 1)];
        if !partitioned {
            inaccessible_tables.push(("denied", TABLE_ID + 3));
        }
        let mut lookups = Vec::new();
        let mut inaccessible_results = Vec::new();
        for (name, table_id) in inaccessible_tables {
            let (inaccessible, results) =
                lookups_to_table(TablePath::new("db", name), TableBucket::new(table_id, 0));
            lookups.extend(inaccessible);
            inaccessible_results.push(results);
        }
        let (dropped, dropped_results) = if partitioned {
            lookups_to(TableBucket::new_with_partition(
                TABLE_ID,
                Some(PARTITION_ID + 1),
                0,
            ))
        } else {
            lookups_to_table(
                TablePath::new("db", "dropped"),
                TableBucket::new(TABLE_ID + 2, 0),
            )
        };
        let (healthy, results) = lookups_to(TableBucket::new_with_partition(
            TABLE_ID,
            partitioned.then_some(PARTITION_ID),
            0,
        ));
        lookups.extend(dropped.into_iter().chain(healthy));
        sender
            .dispatcher
            .send_lookups(lookups, &mut sender.tasks)
            .await;
        let run = tokio::spawn(async move { sender.run().await });

        for results in inaccessible_results {
            for error in errors_of(results).await {
                assert_eq!(error.api_error(), Some(FlussError::AuthorizationException));
            }
        }
        for error in errors_of(dropped_results).await {
            assert_eq!(
                error.api_error(),
                Some(if partitioned {
                    FlussError::PartitionNotExists
                } else {
                    FlussError::TableNotExist
                })
            );
        }
        assert_found(results).await;
        stop(shutdown_tx, run).await;
    }
}

#[tokio::test]
async fn a_retriable_table_metadata_error_does_not_hold_up_other_tables() {
    let state = Arc::new(FakeClusterState::default());
    let server = FakeTabletServer::start(&state).await;
    state.set(&[(1, server.port)], &[(0, None)]);
    let (mut sender, _lookup_tx, shutdown_tx) =
        sender_with_cluster(state.cluster(), 0, 0, 100, 1000);
    let dispatcher = Arc::clone(&sender.dispatcher);
    state.set(&[(1, server.port)], &[(0, Some(1))]);
    let failed_bucket = TableBucket::new(TABLE_ID + 1, 0);
    let (waiting, mut pending) = lookups_to_table(
        TablePath::new("db", "missing_schema"),
        failed_bucket.clone(),
    );
    let (healthy, results) = lookups_to(TableBucket::new(TABLE_ID, 0));
    sender
        .dispatcher
        .send_lookups(
            waiting.into_iter().chain(healthy).collect(),
            &mut sender.tasks,
        )
        .await;
    let run = tokio::spawn(async move { sender.run().await });

    assert_found(results).await;
    wait_until("the schema failure backs off", || {
        dispatcher
            .leader_waits
            .lock()
            .failed_refreshes(&failed_bucket)
            .is_some_and(|refreshes| refreshes > 0)
    })
    .await;
    assert_pending(&mut pending);
    stop(shutdown_tx, run).await;
    for error in errors_of(pending).await {
        assert!(matches!(error, Error::LookupClosed { .. }), "{error}");
    }
}

#[tokio::test]
async fn stale_table_metadata_does_not_fail_lookups_to_a_recreated_table() {
    let state = Arc::new(FakeClusterState::default());
    let server = FakeTabletServer::start(&state).await;
    state.set(&[(1, server.port)], &[(0, None)]);
    let stale_metadata = state.metadata_response();
    state.recreate_table();
    let (mut sender, lookup_tx, shutdown_tx) =
        sender_with_cluster(state.cluster(), 0, 0, 100, 1000);

    sender
        .dispatcher
        .metadata
        .update(stale_metadata)
        .await
        .unwrap();
    state.set(&[(1, server.port)], &[(0, Some(1))]);
    let run = tokio::spawn(async move { sender.run().await });

    assert_found(send_lookups_to(&lookup_tx, TableBucket::new(state.table_id(), 0)).await).await;
    assert!(state.metadata_requests() > 0);
    stop(shutdown_tx, run).await;
}

#[tokio::test]
async fn a_late_failure_of_a_dropped_table_keeps_the_lookups_of_the_table_recreated_in_its_place() {
    let state = Arc::new(FakeClusterState::default());
    let server = FakeTabletServer::start(&state).await;
    state.set(&[(1, server.port)], &[(0, None)]);
    let (mut sender, lookup_tx, shutdown_tx) =
        sender_with_cluster(state.cluster(), i32::MAX, 0, 100, 1000);
    let dispatcher = Arc::clone(&sender.dispatcher);
    state.drop_table();
    server.hold_metadata();
    let run = tokio::spawn(async move { sender.run().await });
    let dropped = send_lookups_for(&lookup_tx, 0).await;
    wait_until("the dropped table is refreshed", || {
        state.metadata_requests() == 1
    })
    .await;

    state.recreate_table();
    dispatcher
        .metadata
        .update(state.metadata_response())
        .await
        .unwrap();
    for error in errors_of(dropped).await {
        assert_eq!(error.api_error(), Some(FlussError::TableNotExist));
    }
    let mut recreated = send_lookups_to(&lookup_tx, TableBucket::new(state.table_id(), 0)).await;
    wait_until(
        "the lookups wait for the leader of the recreated table",
        || waiting_lookups(&dispatcher) == 2,
    )
    .await;
    server.answer_metadata(1);
    wait_until("the refresh of the dropped table ends", || {
        state.metadata_requests() == 2 || waiting_lookups(&dispatcher) == 0
    })
    .await;
    assert_pending(&mut recreated);

    state.set(&[(1, server.port)], &[(0, Some(1))]);
    server.answer_all_metadata();
    assert_found(recreated).await;
    stop(shutdown_tx, run).await;
}

#[tokio::test]
async fn an_unmapped_server_error_on_a_leader_refresh_keeps_the_lookups_waiting() {
    let state = Arc::new(FakeClusterState::default());
    let server = FakeTabletServer::start(&state).await;
    state.set(&[(1, server.port)], &[(0, None), (1, None)]);
    let (mut sender, lookup_tx, shutdown_tx) =
        sender_with_cluster(state.cluster(), i32::MAX, 0, 100, 1000);
    let dispatcher = Arc::clone(&sender.dispatcher);
    server.reject_metadata(FlussError::UnknownServerError);
    let run = tokio::spawn(async move { sender.run().await });

    let mut waiting = [
        send_lookups_for(&lookup_tx, 0).await,
        send_lookups_for(&lookup_tx, 1).await,
    ];
    wait_until("the failed refreshes back off both buckets", || {
        [0, 1].into_iter().all(|bucket_id| {
            failed_refreshes(&dispatcher, bucket_id).is_some_and(|refreshes| refreshes > 0)
        })
    })
    .await;
    for results in &mut waiting {
        assert_pending(results);
    }

    server.accept_metadata();
    state.set(&[(1, server.port)], &[(0, Some(1)), (1, Some(1))]);
    for results in waiting {
        assert_found(results).await;
    }
    stop(shutdown_tx, run).await;
}

#[tokio::test]
async fn a_lookup_failing_without_an_answer_or_for_stale_metadata_drops_its_cached_leader() {
    for failure in [
        "server not cached",
        "connection refused",
        "connection closed on handshake",
        "connection dropped",
        "request rejected as not leader",
    ] {
        let state = Arc::new(FakeClusterState::default());
        let server = FakeTabletServer::start(&state).await;
        let port = match failure {
            "connection refused" => {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                listener.local_addr().unwrap().port()
            }
            _ => server.port,
        };
        state.set(&[(1, port)], &[(0, Some(1)), (1, Some(1))]);
        let (mut sender, _lookup_tx, _shutdown_tx) =
            sender_with_cluster(state.cluster(), 3, 0, 100, 1000);
        match failure {
            "server not cached" => sender.dispatcher.metadata.invalidate_server(&1, vec![]),
            "connection refused" => {}
            "connection closed on handshake" => server.set_down(true),
            "request rejected as not leader" => {
                server.reject_lookups(FlussError::NotLeaderOrFollower)
            }
            _ => {
                let (query, result_rx) = lookup_to(TableBucket::new(TABLE_ID, 0), BUCKET_COUNT);
                send_and_wait(&mut sender, vec![query.into()]).await;
                assert!(result_rx.await.unwrap().is_ok());
                server.set_down(true);
            }
        }

        let (query, mut result_rx) = lookup_to(TableBucket::new(TABLE_ID, 0), BUCKET_COUNT);
        send_and_wait(&mut sender, vec![query.into()]).await;

        assert_eq!(cached_leader(&sender.dispatcher, 0), None, "{failure}");
        assert_eq!(cached_leader(&sender.dispatcher, 1), Some(1), "{failure}");
        let Some(QueuedLookup::Primary(retried)) = sender.queue.drain_all().pop() else {
            panic!("the lookup was not retried after: {failure}");
        };
        assert_eq!(retried.retries(), 1, "{failure}");
        assert!(
            matches!(
                result_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ),
            "{failure}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_steady_flow_of_lookups_does_not_put_off_the_refresh_of_their_bucket() {
    let (mut sender, lookup_tx, shutdown_tx) = sender_with_backoff(9092, 0, 0, 100, 1000);
    sender
        .dispatcher
        .metadata
        .invalidate_server(&1, vec![TABLE_ID]);
    let dispatcher = Arc::clone(&sender.dispatcher);
    let run = tokio::spawn(async move { sender.run().await });

    let mut results = Vec::new();
    for _ in 0..40 {
        results.push(send_lookups_for(&lookup_tx, 0).await);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let refreshes = failed_refreshes(&dispatcher, 0);
    assert!(matches!(refreshes, Some(5..=6)), "{refreshes:?}");
    assert_eq!(waiting_lookups(&dispatcher), 2 * results.len());
    stop(shutdown_tx, run).await;
}

#[tokio::test]
async fn refreshes_failing_at_once_without_a_backoff_do_not_starve_the_sender() {
    let (mut sender, lookup_tx, shutdown_tx) = sender_with_backoff(9092, 0, 0, 0, 0);
    sender
        .dispatcher
        .metadata
        .invalidate_server(&1, vec![TABLE_ID]);
    let dispatcher = Arc::clone(&sender.dispatcher);
    let run = tokio::spawn(async move { sender.run().await });

    let results = send_lookups_for(&lookup_tx, 0).await;
    wait_until("the bucket is refreshed again at once", || {
        failed_refreshes(&dispatcher, 0).is_some_and(|refreshes| refreshes > 1)
    })
    .await;

    stop(shutdown_tx, run).await;
    for error in errors_of(results).await {
        assert!(matches!(error, Error::LookupClosed { .. }), "{error}");
    }
}

#[tokio::test]
async fn lookups_without_a_leader_share_one_refresh_without_holding_up_the_others() {
    let state = Arc::new(FakeClusterState::default());
    let server = FakeTabletServer::start(&state).await;
    state.set(&[(1, server.port)], &[(0, None), (1, None), (2, Some(1))]);
    let (mut sender, lookup_tx, shutdown_tx) =
        sender_with_cluster(state.cluster(), i32::MAX, 0, 100, 1000);
    server.hold_metadata();
    let dispatcher = Arc::clone(&sender.dispatcher);
    let run = tokio::spawn(async move { sender.run().await });

    let mut waiting = Vec::new();
    for (sent, bucket_id) in (1..).zip([0, 1, 0, 1, 0, 1, 0, 1]) {
        waiting.push(send_lookups_for(&lookup_tx, bucket_id).await);
        wait_until("the lookups wait for a leader", || {
            waiting_lookups(&dispatcher) == 2 * sent
        })
        .await;
    }
    wait_until("the server got the refresh", || {
        state.metadata_requests() > 0
    })
    .await;
    assert_found(send_lookups_for(&lookup_tx, 2).await).await;

    state.set(
        &[(1, server.port)],
        &[(0, Some(1)), (1, Some(1)), (2, Some(1))],
    );
    server.answer_all_metadata();
    for results in waiting {
        assert_found(results).await;
    }
    assert_eq!(state.metadata_requests(), 1);
    stop(shutdown_tx, run).await;
}

#[tokio::test]
async fn lookups_waiting_for_a_leader_go_out_once_another_refresh_caches_it() {
    for refresh_in_flight in [false, true] {
        let state = Arc::new(FakeClusterState::default());
        let server = FakeTabletServer::start(&state).await;
        state.set(&[(1, server.port)], &[(0, None)]);
        let (mut sender, lookup_tx, shutdown_tx) =
            sender_with_cluster(state.cluster(), i32::MAX, 0, 60_000, 60_000);
        if refresh_in_flight {
            server.hold_metadata();
        }
        let dispatcher = Arc::clone(&sender.dispatcher);
        let run = tokio::spawn(async move { sender.run().await });
        let waiting = send_lookups_for(&lookup_tx, 0).await;
        wait_until("the lookups wait for a leader", || {
            waiting_lookups(&dispatcher) == 2
                && if refresh_in_flight {
                    state.metadata_requests() > 0
                } else {
                    failed_refreshes(&dispatcher, 0).is_some_and(|refreshes| refreshes > 0)
                }
        })
        .await;

        state.set(&[(1, server.port)], &[(0, Some(1))]);
        dispatcher
            .metadata
            .update(state.metadata_response())
            .await
            .unwrap();

        assert_found(waiting).await;
        assert_eq!(state.metadata_requests(), 1, "{refresh_in_flight}");
        server.answer_all_metadata();
        stop(shutdown_tx, run).await;
    }
}

#[tokio::test]
async fn a_leader_refresh_without_an_answer_moves_on_to_another_server() {
    let state = Arc::new(FakeClusterState::default());
    let silent = FakeTabletServer::start(&state).await;
    let unresponsive_bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent_bootstrap = FakeTabletServer::start(&state).await;
    let bootstrap = FakeTabletServer::start(&state).await;
    state.set(&[(1, silent.port)], &[(0, None)]);
    silent.hold_metadata();
    silent_bootstrap.hold_metadata();
    let metadata = Metadata::new_for_test(state.cluster())
        .with_bootstrap(&format!(
            "{},127.0.0.1:{},127.0.0.1:{}",
            unresponsive_bootstrap.local_addr().unwrap(),
            silent_bootstrap.port,
            bootstrap.port
        ))
        .with_connections(crate::rpc::RpcClient::new().with_timeout(Duration::from_millis(300)))
        .with_request_timeout(Duration::from_millis(300));
    let (mut sender, lookup_tx, shutdown_tx) =
        sender_with_metadata(metadata, i32::MAX, 0, 100, 1000, MAX_WAITING_PER_BUCKET);
    state.set(&[(2, bootstrap.port)], &[(0, Some(2))]);
    let run = tokio::spawn(async move { sender.run().await });

    assert_found(send_lookups_for(&lookup_tx, 0).await).await;
    assert_eq!(silent.metadata_requests(), 1);
    assert!(silent_bootstrap.metadata_requests() > 0);
    assert!(bootstrap.metadata_requests() > 0);
    stop(shutdown_tx, run).await;
}

#[tokio::test]
async fn lookups_fail_when_the_connection_cannot_be_used() {
    for failure in [
        "credentials rejected",
        "authorization failed",
        "no shared authenticate api version",
        "wrong server type",
        "invalid api version advertisement",
        "unsupported metadata api version",
    ] {
        let state = Arc::new(FakeClusterState::default());
        let server = FakeTabletServer::start(&state).await;
        state.set(&[(1, server.port)], &[(0, Some(1)), (1, None)]);
        let metadata = Metadata::new_for_test(state.cluster());
        let (metadata, is_failure): (Metadata, fn(&Error) -> bool) = match failure {
            "credentials rejected" => {
                server.reject_credentials();
                (metadata.with_sasl("user", "password"), |error| {
                    error.api_error() == Some(FlussError::AuthenticateException)
                })
            }
            "authorization failed" => {
                server.reject_lookups(FlussError::AuthorizationException);
                server.reject_metadata(FlussError::AuthorizationException);
                (metadata, |error| {
                    error.api_error() == Some(FlussError::AuthorizationException)
                })
            }
            "no shared authenticate api version" => {
                server.stop_advertising_authenticate();
                (metadata.with_sasl("user", "password"), |error| {
                    matches!(error, Error::UnsupportedVersion { .. })
                })
            }
            "wrong server type" => {
                server.server_type.store(
                    ServerType::CoordinatorServer.to_type_id(),
                    Ordering::Release,
                );
                (metadata, |error| {
                    matches!(error, Error::InvalidServerType { .. })
                })
            }
            "invalid api version advertisement" => {
                server.invalid_api_versions.store(true, Ordering::Release);
                (metadata, |error| {
                    matches!(error, Error::UnexpectedError { .. })
                        && error
                            .to_string()
                            .contains("outside the supported i16 range")
                })
            }
            _ => {
                server.reject_lookups(FlussError::UnsupportedVersion);
                server.reject_metadata(FlussError::UnsupportedVersion);
                (metadata, |error| {
                    error.api_error() == Some(FlussError::UnsupportedVersion)
                })
            }
        };
        let (mut sender, lookup_tx, shutdown_tx) =
            sender_with_metadata(metadata, i32::MAX, 0, 100, 1000, MAX_WAITING_PER_BUCKET);
        let dispatcher = Arc::clone(&sender.dispatcher);
        let run = tokio::spawn(async move { sender.run().await });

        for bucket_id in [0, 1] {
            for error in errors_of(send_lookups_for(&lookup_tx, bucket_id).await).await {
                assert!(is_failure(&error), "{failure}, bucket {bucket_id}: {error}");
            }
        }
        assert_eq!(cached_leader(&dispatcher, 0), Some(1), "{failure}");
        assert_eq!(waiting_lookups(&dispatcher), 0, "{failure}");
        stop(shutdown_tx, run).await;
    }
}

#[tokio::test]
async fn lookups_beyond_the_leader_wait_capacity_of_their_bucket_fail_at_once() {
    let state = Arc::new(FakeClusterState::default());
    let server = FakeTabletServer::start(&state).await;
    state.set(&[(1, server.port)], &[(0, None), (1, Some(1)), (2, None)]);
    server.hold_metadata();
    let (mut sender, lookup_tx, shutdown_tx) = sender_with_metadata(
        Metadata::new_for_test(state.cluster()),
        i32::MAX,
        0,
        100,
        1000,
        4,
    );
    let dispatcher = Arc::clone(&sender.dispatcher);
    let run = tokio::spawn(async move { sender.run().await });
    let abandoned = [
        send_lookups_for(&lookup_tx, 0).await,
        send_lookups_for(&lookup_tx, 0).await,
    ];
    wait_until("the lookups wait for a leader", || {
        waiting_lookups(&dispatcher) == 4
    })
    .await;

    let (query, refused_rx) = lookup_to(TableBucket::new(TABLE_ID, 0), BUCKET_COUNT);
    lookup_tx.send(query.into()).await.unwrap();
    let error = tokio::time::timeout(TIMEOUT, refused_rx)
        .await
        .expect("the lookup completed")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, Error::BufferExhausted { .. }), "{error}");
    let other_bucket = send_lookups_for(&lookup_tx, 2).await;
    wait_until("the lookups to another bucket wait as well", || {
        waiting_lookups(&dispatcher) == 6
    })
    .await;

    drop(abandoned);
    let waiting = send_lookups_for(&lookup_tx, 0).await;
    wait_until("the abandoned lookups make room", || {
        waiting_lookups(&dispatcher) == 4
    })
    .await;
    assert_found(send_lookups_for(&lookup_tx, 1).await).await;

    state.set(
        &[(1, server.port)],
        &[(0, Some(1)), (1, Some(1)), (2, Some(1))],
    );
    server.answer_all_metadata();
    assert_found(waiting).await;
    assert_found(other_bucket).await;
    stop(shutdown_tx, run).await;
}

#[tokio::test]
async fn closing_sends_the_lookups_whose_leader_is_found_and_fails_the_others() {
    let state = Arc::new(FakeClusterState::default());
    let server = FakeTabletServer::start(&state).await;
    state.set(&[(1, server.port)], &[(0, None), (1, None)]);
    let (mut sender, lookup_tx, shutdown_tx) =
        sender_with_cluster(state.cluster(), i32::MAX, 0, 60_000, 60_000);
    server.hold_metadata();
    let dispatcher = Arc::clone(&sender.dispatcher);
    let run = tokio::spawn(async move { sender.run().await });
    let not_found = send_lookups_for(&lookup_tx, 1).await;
    wait_until("the lookups to bucket 1 wait for a refresh", || {
        waiting_lookups(&dispatcher) == 2 && state.metadata_requests() == 1
    })
    .await;
    server.answer_metadata(1);
    wait_until("bucket 1 backs off", || {
        failed_refreshes(&dispatcher, 1) == Some(1)
    })
    .await;
    let found = send_lookups_for(&lookup_tx, 0).await;
    wait_until(
        "the lookups to bucket 0 wait for the refresh in flight",
        || waiting_lookups(&dispatcher) == 4 && state.metadata_requests() == 2,
    )
    .await;

    shutdown_tx.send(true).unwrap();
    wait_until("the sender is closing", || {
        dispatcher.closing.load(Ordering::Acquire)
    })
    .await;
    state.set(&[(1, server.port)], &[(0, Some(1)), (1, None)]);
    server.answer_metadata(1);

    assert_found(found).await;
    server.answer_all_metadata();
    for error in errors_of(not_found).await {
        assert!(matches!(error, Error::LookupClosed { .. }), "{error}");
    }
    tokio::time::timeout(TIMEOUT, run)
        .await
        .expect("the sender stopped")
        .unwrap();
    assert_eq!(state.metadata_requests(), 3);
}

#[tokio::test]
async fn lookups_fail_over_once_the_leaders_server_is_down() {
    for partition_id in [None, Some(PARTITION_ID)] {
        let state = Arc::new(FakeClusterState::default());
        let server_1 = FakeTabletServer::start(&state).await;
        let server_2 = FakeTabletServer::start(&state).await;
        let servers = [(1, server_1.port), (2, server_2.port)];
        if let Some(partition_id) = partition_id {
            state.set_partition(partition_id, "p1");
        }
        state.set(&servers, &[(0, Some(2)), (1, Some(1))]);
        let (mut sender, lookup_tx, shutdown_tx) =
            sender_with_cluster(state.cluster(), i32::MAX, 0, 100, 1000);

        server_2.set_down(true);
        state.set(&servers[..1], &[(0, Some(1)), (1, Some(1))]);
        let run = tokio::spawn(async move { sender.run().await });

        for bucket_id in [0, 1] {
            let table_bucket = TableBucket::new_with_partition(TABLE_ID, partition_id, bucket_id);
            assert_found(send_lookups_to(&lookup_tx, table_bucket).await).await;
        }
        assert_eq!(
            *state.requested_partition_ids.lock(),
            HashSet::from_iter(partition_id)
        );
        stop(shutdown_tx, run).await;
    }
}

#[tokio::test]
async fn lookups_wait_for_a_stopped_single_replica_while_other_buckets_go_on() {
    let state = Arc::new(FakeClusterState::default());
    let server_1 = FakeTabletServer::start(&state).await;
    let server_2 = FakeTabletServer::start(&state).await;
    let servers = [(1, server_1.port), (2, server_2.port)];
    let leaders = [(0, Some(2)), (1, Some(1))];
    state.set(&servers, &leaders);
    let (mut sender, lookup_tx, shutdown_tx) = sender_with_cluster(state.cluster(), 1, 0, 50, 200);

    server_2.set_down(true);
    state.set(&servers[..1], &[(0, None), (1, Some(1))]);
    let run = tokio::spawn(async move { sender.run().await });

    let mut waiting = send_lookups_for(&lookup_tx, 0).await;
    let refreshes = state.metadata_requests();
    wait_until(
        "the client looked for a leader of bucket 0 a few times",
        || state.metadata_requests() >= refreshes + 5,
    )
    .await;
    assert_found(send_lookups_for(&lookup_tx, 1).await).await;
    assert_pending(&mut waiting);

    server_2.set_down(false);
    state.set(&servers, &leaders);
    assert_found(waiting).await;
    stop(shutdown_tx, run).await;
}
