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
use crate::rpc::ApiKey;
use futures::future::join_all;
use std::sync::atomic::AtomicI64;
use tokio::sync::Notify;

const PLAIN_TABLE_ID: TableId = 2;
const LOST_RESPONSE_TIMEOUT_MS: i32 = 500;

#[derive(Clone, Copy)]
enum CreateReply {
    Create,
    Fail(FlussError),
    NoResponse,
}

struct PartitionServer {
    port: u16,
    parts_table_id: AtomicI64,
    re_creates_parts_table_on_create: AtomicBool,
    parts_table_dropped: AtomicBool,
    partitions: Mutex<HashMap<String, PartitionId>>,
    create_reply: CreateReply,
    create_requests: Mutex<Vec<String>>,
    supports_create_partition: AtomicBool,
    answers_table_refreshes: AtomicBool,
    holds_missing_partition: AtomicBool,
    missing_partition_held: Notify,
    release_missing_partition: Notify,
    unanswered_partition_refreshes: Mutex<HashSet<String>>,
    lost_partition_refreshes: Mutex<Vec<String>>,
    dropped_partition_ids: Mutex<HashSet<PartitionId>>,
    written_partition_ids: Mutex<Vec<PartitionId>>,
}

impl PartitionServer {
    async fn start(
        partitions: &[(&str, PartitionId)],
        create_reply: CreateReply,
    ) -> (Arc<Self>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let server = Arc::new(Self {
            port: listener.local_addr().expect("address").port(),
            parts_table_id: AtomicI64::new(1),
            re_creates_parts_table_on_create: AtomicBool::new(false),
            parts_table_dropped: AtomicBool::new(false),
            partitions: Mutex::new(
                partitions
                    .iter()
                    .map(|(name, id)| (name.to_string(), *id))
                    .collect(),
            ),
            create_reply,
            create_requests: Mutex::default(),
            supports_create_partition: AtomicBool::new(true),
            answers_table_refreshes: AtomicBool::new(true),
            holds_missing_partition: AtomicBool::new(false),
            missing_partition_held: Notify::new(),
            release_missing_partition: Notify::new(),
            unanswered_partition_refreshes: Mutex::default(),
            lost_partition_refreshes: Mutex::default(),
            dropped_partition_ids: Mutex::default(),
            written_partition_ids: Mutex::default(),
        });
        let serving = Arc::clone(&server);
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(Arc::clone(&serving).serve(stream));
            }
        });
        (server, task)
    }

    async fn serve(self: Arc<Self>, mut stream: TcpStream) {
        loop {
            let Ok(len) = stream.read_i32().await else {
                return;
            };
            let mut payload = vec![0u8; len as usize];
            if stream.read_exact(&mut payload).await.is_err() {
                return;
            }
            let api_key = i16::from_be_bytes([payload[0], payload[1]]);
            let request_id = i32::from_be_bytes(payload[4..8].try_into().unwrap());
            let reply = self.answer(api_key, &payload[REQUEST_HEADER_LENGTH..]);
            if matches!(reply, Some(Err(FlussError::PartitionNotExists)))
                && self.holds_missing_partition.swap(false, Ordering::Relaxed)
            {
                self.missing_partition_held.notify_one();
                self.release_missing_partition.notified().await;
            }
            let (response_type, body) = match reply {
                Some(Ok(body)) => (0u8, body),
                Some(Err(error)) => (
                    1u8,
                    crate::proto::ErrorResponse {
                        error_code: error.code(),
                        error_message: Some(error.message().to_string()),
                    }
                    .encode_to_vec(),
                ),
                None => continue,
            };
            let mut response = Vec::with_capacity(9 + body.len());
            response.extend_from_slice(&((5 + body.len()) as i32).to_be_bytes());
            response.push(response_type);
            response.extend_from_slice(&request_id.to_be_bytes());
            response.extend_from_slice(&body);
            if stream.write_all(&response).await.is_err() {
                return;
            }
        }
    }

    fn answer(
        &self,
        api_key: i16,
        body: &[u8],
    ) -> Option<std::result::Result<Vec<u8>, FlussError>> {
        match ApiKey::from(api_key) {
            ApiKey::ApiVersion => Some(Ok(ApiVersionsResponse {
                api_versions: [
                    ApiKey::ApiVersion,
                    ApiKey::MetaData,
                    ApiKey::CreatePartition,
                    ApiKey::ProduceLog,
                ]
                .into_iter()
                .filter(|api_key| {
                    *api_key != ApiKey::CreatePartition
                        || self.supports_create_partition.load(Ordering::Relaxed)
                })
                .map(|api_key| PbApiVersion {
                    api_key: i32::from(i16::from(api_key)),
                    min_version: 0,
                    max_version: 0,
                })
                .collect(),
                server_type: None,
            }
            .encode_to_vec())),
            ApiKey::MetaData => {
                let request =
                    crate::proto::MetadataRequest::decode(body).expect("metadata request");
                if self.parts_table_dropped.load(Ordering::Relaxed)
                    && request
                        .table_path
                        .iter()
                        .any(|path| path.table_name == "parts")
                {
                    return Some(Err(FlussError::UnknownServerError));
                }
                if request.partitions_path.is_empty()
                    && !self.answers_table_refreshes.load(Ordering::Relaxed)
                {
                    return None;
                }
                if let [path] = request.partitions_path.as_slice() {
                    if let Some(partition_name) = path
                        .partition_name
                        .as_ref()
                        .filter(|name| self.unanswered_partition_refreshes.lock().contains(*name))
                    {
                        self.lost_partition_refreshes
                            .lock()
                            .push(partition_name.clone());
                        return None;
                    }
                }
                let parts_table_id = self.parts_table_id.load(Ordering::Relaxed);
                let partitions = self.partitions.lock();
                Some(
                    request
                        .partitions_path
                        .into_iter()
                        .map(|path| {
                            let partition_name = path.partition_name.expect("partition name");
                            let partition_id = *partitions
                                .get(&partition_name)
                                .ok_or(FlussError::PartitionNotExists)?;
                            let mut partition =
                                partition_metadata(partition_name, partition_id, Some(1));
                            partition.table_id = parts_table_id;
                            Ok(partition)
                        })
                        .collect::<std::result::Result<Vec<_>, FlussError>>()
                        .map(|partition_metadata| {
                            let mut response = metadata_response(self.port, partition_metadata);
                            response.table_metadata = request
                                .table_path
                                .iter()
                                .map(|path| match path.table_name.as_str() {
                                    "plain" => plain_table_metadata(),
                                    _ => parts_table_metadata(parts_table_id),
                                })
                                .collect();
                            response.encode_to_vec()
                        }),
                )
            }
            ApiKey::CreatePartition => {
                let request = crate::proto::CreatePartitionRequest::decode(body)
                    .expect("create partition request");
                let partition_name = request.partition_spec.partition_key_values[0].value.clone();
                self.create_requests.lock().push(partition_name.clone());
                if self
                    .re_creates_parts_table_on_create
                    .swap(false, Ordering::Relaxed)
                {
                    self.parts_table_id.fetch_add(1, Ordering::Relaxed);
                    self.partitions.lock().clear();
                }
                match self.create_reply {
                    CreateReply::Create => {
                        let mut partitions = self.partitions.lock();
                        let partition_id = partitions.values().max().map_or(10, |id| id + 1);
                        partitions.entry(partition_name).or_insert(partition_id);
                        Some(Ok(crate::proto::CreatePartitionResponse {}.encode_to_vec()))
                    }
                    CreateReply::Fail(error) => Some(Err(error)),
                    CreateReply::NoResponse => None,
                }
            }
            ApiKey::ProduceLog => {
                let request =
                    crate::proto::ProduceLogRequest::decode(body).expect("produce log request");
                Some(Ok(ProduceLogResponse {
                    buckets_resp: request
                        .buckets_req
                        .into_iter()
                        .map(|bucket| {
                            let dropped = bucket
                                .partition_id
                                .is_some_and(|id| self.dropped_partition_ids.lock().contains(&id));
                            if !dropped {
                                self.written_partition_ids
                                    .lock()
                                    .extend(bucket.partition_id);
                            }
                            PbProduceLogRespForBucket {
                                partition_id: bucket.partition_id,
                                bucket_id: bucket.bucket_id,
                                base_offset: (!dropped).then_some(0),
                                error_code: dropped
                                    .then(|| FlussError::UnknownTableOrBucketException.code()),
                                ..Default::default()
                            }
                        })
                        .collect(),
                }
                .encode_to_vec()))
            }
            _ => Some(Err(FlussError::UnknownServerError)),
        }
    }
}

fn partition_metadata(
    partition_name: String,
    partition_id: PartitionId,
    leader_id: Option<i32>,
) -> crate::proto::PbPartitionMetadata {
    crate::proto::PbPartitionMetadata {
        table_id: 1,
        partition_name,
        partition_id,
        bucket_metadata: vec![crate::proto::PbBucketMetadata {
            bucket_id: 0,
            leader_id,
            ..Default::default()
        }],
        bucket_count: Some(1),
    }
}

fn table_metadata(
    table_name: &str,
    table_id: TableId,
    descriptor: TableDescriptor,
) -> crate::proto::PbTableMetadata {
    crate::proto::PbTableMetadata {
        table_path: PbTablePath {
            database_name: "db".to_string(),
            table_name: table_name.to_string(),
        },
        table_id,
        schema_id: 1,
        table_json: serde_json::to_vec(&descriptor.serialize_json().expect("table json"))
            .expect("table json"),
        ..Default::default()
    }
}

fn plain_table_metadata() -> crate::proto::PbTableMetadata {
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .build()
                .expect("schema"),
        )
        .distributed_by(Some(1), vec![])
        .build()
        .expect("descriptor");
    crate::proto::PbTableMetadata {
        bucket_metadata: vec![crate::proto::PbBucketMetadata {
            bucket_id: 0,
            leader_id: Some(1),
            ..Default::default()
        }],
        ..table_metadata("plain", PLAIN_TABLE_ID, descriptor)
    }
}

fn parts_table_descriptor() -> TableDescriptor {
    TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("p", DataTypes::string())
                .build()
                .expect("schema"),
        )
        .partitioned_by(vec!["p"])
        .distributed_by(Some(1), vec![])
        .build()
        .expect("descriptor")
}

fn parts_table_metadata(table_id: TableId) -> crate::proto::PbTableMetadata {
    table_metadata("parts", table_id, parts_table_descriptor())
}

fn metadata_response(
    port: u16,
    partition_metadata: Vec<crate::proto::PbPartitionMetadata>,
) -> crate::proto::MetadataResponse {
    let node = |node_id| crate::proto::PbServerNode {
        node_id,
        host: "127.0.0.1".to_string(),
        port: i32::from(port),
        ..Default::default()
    };
    crate::proto::MetadataResponse {
        coordinator_server: Some(node(0)),
        tablet_servers: vec![node(1)],
        partition_metadata,
        ..Default::default()
    }
}

fn idempotence_with_writer_id() -> Arc<IdempotenceManager> {
    let idempotence = enabled_idempotence();
    idempotence.set_writer_id(42);
    idempotence
}

fn partition_write_stack(
    port: u16,
    create_partitions: bool,
) -> (Sender, Arc<Metadata>, Arc<TableInfo>) {
    partition_write_stack_with_idempotence(port, create_partitions, idempotence_with_writer_id())
}

fn partition_write_stack_with_idempotence(
    port: u16,
    create_partitions: bool,
    idempotence: Arc<IdempotenceManager>,
) -> (Sender, Arc<Metadata>, Arc<TableInfo>) {
    let table_path = TablePath::new("db", "parts");
    let table_info = TableInfo::of(table_path.clone(), 1, 1, parts_table_descriptor(), 0, 0);
    let node = |id, server_type| {
        ServerNode::new(id, "127.0.0.1".to_string(), u32::from(port), server_type)
    };
    let cluster = Cluster::new(
        Some(node(0, ServerType::CoordinatorServer)),
        HashMap::from([(1, node(1, ServerType::TabletServer))]),
        HashMap::new(),
        HashMap::new(),
        HashMap::from([(table_path.clone(), 1)]),
        HashMap::from([(table_path, table_info.clone())]),
        HashMap::new(),
        HashMap::new(),
    );
    let metadata = Arc::new(Metadata::new_for_test(Arc::new(cluster)));
    let accumulator = Arc::new(RecordAccumulator::new(
        Config::default(),
        Arc::clone(&idempotence),
    ));
    let mut sender = Sender::new(
        Arc::clone(&metadata),
        accumulator,
        1024 * 1024,
        30_000,
        -1,
        i32::MAX,
        0,
        0,
        idempotence,
        Arc::new(crate::metrics::WriterMetrics::new()),
    );
    if create_partitions {
        sender = sender.with_partition_creator(Arc::new(FlussAdmin::new(
            Arc::new(crate::rpc::RpcClient::new()),
            Arc::clone(&metadata),
        )));
    }
    (sender, metadata, Arc::new(table_info))
}

fn append_to_partition(
    sender: &Sender,
    table_info: &Arc<TableInfo>,
    partition_name: &str,
) -> (Arc<PhysicalTablePath>, ResultHandle) {
    let path = Arc::new(PhysicalTablePath::of_partitioned(
        Arc::new(table_info.table_path.clone()),
        Some(partition_name.to_string()),
    ));
    let row = GenericRow {
        values: vec![Datum::Int32(1), Datum::String(partition_name.into())],
    };
    let record = WriteRecord::for_append(Arc::clone(table_info), Arc::clone(&path), 1, &row);
    let handle = sender
        .accumulator
        .append(&record, 0, &sender.metadata.get_cluster(), false)
        .expect("append")
        .result_handle
        .expect("result handle");
    (path, handle)
}

async fn refresh_and_create(sender: &Sender, unknown_leaders: &HashSet<Arc<PhysicalTablePath>>) {
    if sender.refresh_unknown_leaders(unknown_leaders).await != RefreshOutcome::RefreshEach {
        return;
    }
    for (paths, outcome) in join_all(sender.refresh_each(unknown_leaders.clone())).await {
        if outcome == RefreshOutcome::Create {
            for path in paths {
                sender.create_missing_partition(&path).await;
            }
        }
    }
}

async fn write_error_code(handle: &ResultHandle) -> i32 {
    let result = tokio::time::timeout(Duration::from_secs(10), handle.wait())
        .await
        .expect("the write must complete")
        .expect("write result");
    match result {
        Err(broadcast::Error::WriteFailed { code, .. }) => code,
        other => panic!("expected a failed write, got {other:?}"),
    }
}

fn partition_bucket(partition_id: PartitionId) -> TableBucket {
    TableBucket::new_with_partition(1, Some(partition_id), 0)
}

#[tokio::test]
async fn missing_partition_fails_only_its_writes() -> Result<()> {
    for (create_partitions, create_reply, expected_error) in [
        (false, CreateReply::Create, FlussError::PartitionNotExists),
        (
            true,
            CreateReply::Fail(FlussError::PartitionSpecInvalidException),
            FlussError::PartitionSpecInvalidException,
        ),
    ] {
        let (server, server_task) = PartitionServer::start(&[("p1", 10)], create_reply).await;
        server
            .unanswered_partition_refreshes
            .lock()
            .insert("p3".to_string());
        let (mut sender, metadata, table_info) =
            partition_write_stack(server.port, create_partitions);
        sender.max_request_timeout_ms = LOST_RESPONSE_TIMEOUT_MS;
        let (existing, existing_handle) = append_to_partition(&sender, &table_info, "p1");
        let (missing, missing_handle) = append_to_partition(&sender, &table_info, "p2");
        let (lost, lost_handle) = append_to_partition(&sender, &table_info, "p3");
        let plain = Arc::new(PhysicalTablePath::of(Arc::new(TablePath::new(
            "db", "plain",
        ))));

        refresh_and_create(
            &sender,
            &HashSet::from([Arc::clone(&existing), Arc::clone(&missing), lost, plain]),
        )
        .await;

        assert_eq!(
            write_error_code(&missing_handle).await,
            expected_error.code()
        );
        assert_eq!(
            server.create_requests.lock().len(),
            usize::from(create_partitions)
        );
        assert_eq!(metadata.get_cluster().get_partition_id(&existing), Some(10));
        assert!(
            metadata
                .get_cluster()
                .leader_for(&TableBucket::new(PLAIN_TABLE_ID, 0))
                .is_some()
        );
        assert!(existing_handle.wait().now_or_never().is_none());
        assert_eq!(
            *server.lost_partition_refreshes.lock(),
            vec!["p3".to_string()]
        );
        assert!(lost_handle.wait().now_or_never().is_none());
        assert!(sender.accumulator.has_undrained());
        let ready = sender.accumulator.ready(&metadata.get_cluster())?;
        assert!(!ready.unknown_leader_tables.contains(&missing));

        server_task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn failed_partition_creation_keeps_the_writes_of_a_re_created_table() {
    let (server, server_task) =
        PartitionServer::start(&[], CreateReply::Fail(FlussError::TableNotExist)).await;
    let (sender, _, table_info) = partition_write_stack(server.port, true);
    let (missing, dropped_table_handle) = append_to_partition(&sender, &table_info, "p1");
    let mut re_created = (*table_info).clone();
    re_created.table_id = 2;
    let (_, re_created_table_handle) = append_to_partition(&sender, &Arc::new(re_created), "p1");

    refresh_and_create(&sender, &HashSet::from([missing])).await;

    assert_eq!(
        write_error_code(&dropped_table_handle).await,
        FlussError::TableNotExist.code()
    );
    assert!(re_created_table_handle.wait().now_or_never().is_none());

    server_task.abort();
}

#[tokio::test]
async fn write_to_a_dropped_table_fails_without_blocking_other_tables() {
    for (create_partitions, expected_error) in [
        (true, FlussError::TableNotExist),
        (false, FlussError::PartitionNotExists),
    ] {
        let (server, server_task) =
            PartitionServer::start(&[], CreateReply::Fail(FlussError::TableNotExist)).await;
        server.parts_table_dropped.store(true, Ordering::Relaxed);
        let (sender, metadata, table_info) = partition_write_stack(server.port, create_partitions);
        let (missing, missing_handle) = append_to_partition(&sender, &table_info, "p1");
        let plain = Arc::new(PhysicalTablePath::of(Arc::new(TablePath::new(
            "db", "plain",
        ))));

        refresh_and_create(&sender, &HashSet::from([missing, plain])).await;

        assert_eq!(
            write_error_code(&missing_handle).await,
            expected_error.code()
        );
        assert!(
            metadata
                .get_cluster()
                .leader_for(&TableBucket::new(PLAIN_TABLE_ID, 0))
                .is_some()
        );

        server_task.abort();
    }
}

#[tokio::test]
async fn partition_creation_failing_in_the_client_fails_the_writes() -> Result<()> {
    for evicts_table in [true, false] {
        let (server, server_task) = PartitionServer::start(&[], CreateReply::Create).await;
        server
            .supports_create_partition
            .store(evicts_table, Ordering::Relaxed);
        let (sender, metadata, table_info) = partition_write_stack(server.port, true);
        let (missing, handle) = append_to_partition(&sender, &table_info, "p1");
        if evicts_table {
            metadata.evict_table_metadata(&table_info.table_path);
        }

        sender.create_missing_partition(&missing).await;

        let result = tokio::time::timeout(Duration::from_secs(10), handle.wait())
            .await
            .expect("the write must complete")?;
        if evicts_table {
            assert!(
                matches!(&result, Err(broadcast::Error::WriteFailed { code, .. })
                    if *code == FlussError::InvalidTableException.code()),
                "{result:?}"
            );
        } else {
            assert!(
                matches!(&result, Err(broadcast::Error::Client { message })
                    if message.contains("CreatePartition")),
                "{result:?}"
            );
        }
        assert!(server.create_requests.lock().is_empty());

        server_task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn timed_out_refresh_is_left_to_the_next_one() {
    let (server, server_task) = PartitionServer::start(&[], CreateReply::Create).await;
    server
        .answers_table_refreshes
        .store(false, Ordering::Relaxed);
    let (mut sender, _, _) = partition_write_stack(server.port, true);
    sender.max_request_timeout_ms = LOST_RESPONSE_TIMEOUT_MS;
    let plain = Arc::new(PhysicalTablePath::of(Arc::new(TablePath::new(
        "db", "plain",
    ))));

    let outcome = sender
        .refresh_unknown_leaders(&HashSet::from([plain]))
        .await;

    assert_eq!(outcome, RefreshOutcome::Done);

    server_task.abort();
}

#[tokio::test]
async fn retriable_partition_creation_failure_keeps_the_writes() {
    for (create_reply, answers_table_refreshes) in [
        (CreateReply::Fail(FlussError::RequestTimeOut), true),
        (CreateReply::Fail(FlussError::RequestTimeOut), false),
        (CreateReply::NoResponse, true),
    ] {
        let (server, server_task) = PartitionServer::start(&[], create_reply).await;
        server
            .answers_table_refreshes
            .store(answers_table_refreshes, Ordering::Relaxed);
        let (mut sender, _, table_info) = partition_write_stack(server.port, true);
        sender.max_request_timeout_ms = LOST_RESPONSE_TIMEOUT_MS;
        let (missing, missing_handle) = append_to_partition(&sender, &table_info, "p1");
        let unknown_leaders = HashSet::from([missing]);

        bounded(async {
            for _ in 0..2 {
                refresh_and_create(&sender, &unknown_leaders).await;
            }
        })
        .await;

        assert_eq!(server.create_requests.lock().len(), 2);
        assert!(missing_handle.wait().now_or_never().is_none());
        assert!(sender.accumulator.has_undrained());

        server_task.abort();
    }
}

#[tokio::test]
async fn partition_creation_finds_a_moved_or_unknown_coordinator() -> Result<()> {
    let (old_coordinator, old_coordinator_task) = PartitionServer::start(
        &[],
        CreateReply::Fail(FlussError::NotCoordinatorLeaderException),
    )
    .await;
    for old_coordinator_port in [Some(old_coordinator.port), None] {
        let (server, server_task) = PartitionServer::start(&[], CreateReply::Create).await;
        let (mut sender, metadata, table_info) = partition_write_stack(server.port, true);
        sender.max_request_timeout_ms = LOST_RESPONSE_TIMEOUT_MS;
        let mut stale = metadata_response(server.port, Vec::new());
        match old_coordinator_port {
            Some(port) => {
                let coordinator = stale.coordinator_server.as_mut().expect("coordinator");
                coordinator.node_id = 2;
                coordinator.port = i32::from(port);
            }
            None => stale.coordinator_server = None,
        }
        metadata.update(stale).await?;
        let (missing, missing_handle) = append_to_partition(&sender, &table_info, "p1");
        let unknown_leaders = HashSet::from([missing]);

        server
            .answers_table_refreshes
            .store(false, Ordering::Relaxed);
        refresh_and_create(&sender, &unknown_leaders).await;
        assert!(server.create_requests.lock().is_empty());
        server
            .answers_table_refreshes
            .store(true, Ordering::Relaxed);
        sender.max_request_timeout_ms = 30_000;
        refresh_and_create(&sender, &unknown_leaders).await;

        assert_eq!(*server.create_requests.lock(), vec!["p1".to_string()]);
        assert!(missing_handle.wait().now_or_never().is_none());

        server_task.abort();
    }
    assert_eq!(
        *old_coordinator.create_requests.lock(),
        vec!["p1".to_string()]
    );

    old_coordinator_task.abort();
    Ok(())
}

#[tokio::test]
async fn cached_partitions_are_refreshed_from_the_server() -> Result<()> {
    let (server, server_task) = PartitionServer::start(&[("p1", 10)], CreateReply::Create).await;
    let (sender, metadata, table_info) = partition_write_stack(server.port, false);
    metadata
        .update(metadata_response(
            server.port,
            vec![
                partition_metadata("p1".to_string(), 10, None),
                partition_metadata("p2".to_string(), 11, Some(1)),
            ],
        ))
        .await?;
    let (leaderless, leaderless_handle) = append_to_partition(&sender, &table_info, "p1");
    let (dropped, dropped_handle) = append_to_partition(&sender, &table_info, "p2");

    refresh_and_create(&sender, &HashSet::from([leaderless, dropped])).await;

    let cluster = metadata.get_cluster();
    assert!(cluster.leader_for(&partition_bucket(10)).is_some());
    assert!(leaderless_handle.wait().now_or_never().is_none());
    assert_eq!(
        write_error_code(&dropped_handle).await,
        FlussError::PartitionNotExists.code()
    );
    assert!(cluster.leader_for(&partition_bucket(11)).is_none());

    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn write_to_a_dropped_partition_fails_with_a_sequence_and_moves_without_one() -> Result<()> {
    for idempotence in [idempotence_with_writer_id(), disabled_idempotence()] {
        let idempotent = idempotence.is_enabled();
        let (server, server_task) =
            PartitionServer::start(&[("p1", 10), ("p2", 20)], CreateReply::Create).await;
        let (sender, metadata, table_info) =
            partition_write_stack_with_idempotence(server.port, true, idempotence);
        metadata
            .update(metadata_response(
                server.port,
                vec![partition_metadata("p1".to_string(), 10, Some(1))],
            ))
            .await?;
        server.partitions.lock().remove("p1");
        server.dropped_partition_ids.lock().insert(10);

        let sender = Arc::new(sender);
        let (tx, sender_task) = run_sender(&sender);
        let (_, handle) = append_to_partition(&sender, &table_info, "p1");
        sender.accumulator.wakeup_sender();

        if idempotent {
            assert_eq!(
                write_error_code(&handle).await,
                FlussError::PartitionNotExists.code()
            );
            assert!(server.create_requests.lock().is_empty());
            assert!(server.written_partition_ids.lock().is_empty());
        } else {
            let result = tokio::time::timeout(Duration::from_secs(10), handle.wait())
                .await
                .expect("the write must complete")?;
            assert!(result.is_ok());
            assert_eq!(*server.create_requests.lock(), vec!["p1".to_string()]);
            assert_eq!(*server.written_partition_ids.lock(), vec![21]);
        }
        assert!(
            metadata
                .get_cluster()
                .leader_for(&partition_bucket(10))
                .is_none()
        );
        bounded(finish_sender(&sender, tx, sender_task)).await;

        server_task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn dropped_partition_is_created_only_for_writes_without_a_sequence() -> Result<()> {
    let (server, server_task) = PartitionServer::start(&[], CreateReply::Create).await;
    let (sender, metadata, table_info) = partition_write_stack(server.port, true);
    metadata
        .update(metadata_response(
            server.port,
            vec![partition_metadata("p1".to_string(), 10, Some(1))],
        ))
        .await?;
    let (dropped, retried_handle) = append_to_partition(&sender, &table_info, "p1");
    let cluster = metadata.get_cluster();
    let nodes = HashSet::from([cluster.get_tablet_server(1).expect("server").clone()]);
    let mut sent = sender.accumulator.drain(cluster, &nodes, i32::MAX)?;
    sender
        .accumulator
        .re_enqueue(sent.remove(&1).expect("drained").pop().expect("batch"));
    let (_, new_handle) = append_to_partition(&sender, &table_info, "p1");

    let outcome = sender.refresh_partition(&dropped).await;

    assert_eq!(outcome, RefreshOutcome::Create);
    assert_eq!(
        write_error_code(&retried_handle).await,
        FlussError::PartitionNotExists.code()
    );
    assert!(new_handle.wait().now_or_never().is_none());

    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn partition_creation_does_not_block_other_partitions_or_shutdown() -> Result<()> {
    let (server, server_task) =
        PartitionServer::start(&[("p1", 10)], CreateReply::NoResponse).await;
    let (sender, _, table_info) = partition_write_stack(server.port, true);
    let sender = Arc::new(sender);
    let (tx, sender_task) = run_sender(&sender);
    let (_, missing_handle) = append_to_partition(&sender, &table_info, "p2");
    sender.accumulator.wakeup_sender();
    bounded(async {
        while server.create_requests.lock().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    let (_, existing_handle) = append_to_partition(&sender, &table_info, "p1");
    sender.accumulator.wakeup_sender();

    let result = tokio::time::timeout(Duration::from_secs(10), existing_handle.wait())
        .await
        .expect("the write must not wait for another partition's creation")?;
    assert!(result.is_ok());
    assert_eq!(server.create_requests.lock().len(), 1);

    server.partitions.lock().insert("p2".to_string(), 11);
    let result = tokio::time::timeout(Duration::from_secs(10), missing_handle.wait())
        .await
        .expect("a later refresh must find the created partition")?;
    assert!(result.is_ok());
    bounded(finish_sender(&sender, tx, sender_task)).await;

    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn lost_partition_refresh_does_not_block_other_partitions() -> Result<()> {
    let (server, server_task) = PartitionServer::start(&[("p4", 10)], CreateReply::Create).await;
    server
        .unanswered_partition_refreshes
        .lock()
        .insert("p2".to_string());
    let (sender, _, table_info) = partition_write_stack(server.port, true);
    let sender = Arc::new(sender);
    let (_, lost_handle) = append_to_partition(&sender, &table_info, "p2");
    let (_, missing_handle) = append_to_partition(&sender, &table_info, "p3");
    let (_tx, sender_task) = run_sender(&sender);

    let result = tokio::time::timeout(Duration::from_secs(10), missing_handle.wait())
        .await
        .expect("the missing partition must be created without waiting for another refresh")?;
    assert!(result.is_ok());
    assert_eq!(*server.create_requests.lock(), vec!["p3".to_string()]);

    let (_, later_handle) = append_to_partition(&sender, &table_info, "p4");
    sender.accumulator.wakeup_sender();
    let result = tokio::time::timeout(Duration::from_secs(10), later_handle.wait())
        .await
        .expect("a later partition must be refreshed without waiting for another refresh")?;
    assert!(result.is_ok());
    assert_eq!(
        *server.lost_partition_refreshes.lock(),
        vec!["p2".to_string()]
    );
    assert!(lost_handle.wait().now_or_never().is_none());

    sender_task.abort();
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn write_to_a_re_created_table_fails_whether_or_not_its_partition_was_created() -> Result<()>
{
    for re_created_on_create in [false, true] {
        let (server, server_task) = PartitionServer::start(&[], CreateReply::Create).await;
        if re_created_on_create {
            server
                .re_creates_parts_table_on_create
                .store(true, Ordering::Relaxed);
        } else {
            server.parts_table_id.store(2, Ordering::Relaxed);
        }
        let (sender, metadata, table_info) = partition_write_stack(server.port, true);
        let sender = Arc::new(sender);
        let (tx, sender_task) = run_sender(&sender);
        let (_, dropped_table_handle) = append_to_partition(&sender, &table_info, "p1");
        sender.accumulator.wakeup_sender();

        assert_eq!(
            write_error_code(&dropped_table_handle).await,
            FlussError::TableNotExist.code()
        );
        assert_eq!(
            server.create_requests.lock().len(),
            usize::from(re_created_on_create)
        );
        assert!(server.written_partition_ids.lock().is_empty());
        assert_eq!(
            metadata.get_cluster().get_table_id(&table_info.table_path),
            Some(2)
        );

        let mut re_created = (*table_info).clone();
        re_created.table_id = 2;
        let (_, re_created_table_handle) =
            append_to_partition(&sender, &Arc::new(re_created), "p1");
        sender.accumulator.wakeup_sender();
        let result = tokio::time::timeout(Duration::from_secs(10), re_created_table_handle.wait())
            .await
            .expect("the write to the re-created table must complete")?;
        assert!(result.is_ok());
        assert_eq!(*server.written_partition_ids.lock(), vec![10]);
        bounded(finish_sender(&sender, tx, sender_task)).await;

        server_task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn late_reply_that_a_partition_is_missing_keeps_the_partition_that_replaced_it() -> Result<()>
{
    let (server, server_task) = PartitionServer::start(&[], CreateReply::Create).await;
    let (sender, metadata, table_info) = partition_write_stack(server.port, true);
    metadata
        .update(metadata_response(
            server.port,
            vec![partition_metadata("p1".to_string(), 10, Some(1))],
        ))
        .await?;
    let (path, handle) = append_to_partition(&sender, &table_info, "p1");
    let sender = Arc::new(sender);
    server
        .holds_missing_partition
        .store(true, Ordering::Relaxed);
    let refresh = tokio::spawn({
        let sender = Arc::clone(&sender);
        let path = Arc::clone(&path);
        async move { sender.refresh_partition(&path).await }
    });
    bounded(server.missing_partition_held.notified()).await;

    server.partitions.lock().insert("p1".to_string(), 20);
    metadata
        .update(metadata_response(
            server.port,
            vec![partition_metadata("p1".to_string(), 20, Some(1))],
        ))
        .await?;
    let cluster = metadata.get_cluster();
    let nodes = HashSet::from([cluster.get_tablet_server(1).expect("server").clone()]);
    let mut sent = sender.accumulator.drain(cluster, &nodes, i32::MAX)?;
    let batch = sent.remove(&1).expect("drained").pop().expect("batch");
    assert_eq!(batch.write_batch.sequence_partition_id(), Some(20));
    sender.accumulator.re_enqueue(batch);
    server.release_missing_partition.notify_one();

    let outcome = tokio::time::timeout(Duration::from_secs(10), refresh)
        .await
        .expect("the refresh must complete")
        .expect("refresh task");

    assert_eq!(outcome, RefreshOutcome::Done);
    assert!(handle.wait().now_or_never().is_none());
    assert!(
        sender
            .accumulator
            .has_queued_batches_for_partition(&path, None)
    );
    assert!(
        metadata
            .get_cluster()
            .leader_for(&partition_bucket(20))
            .is_some()
    );
    assert!(server.create_requests.lock().is_empty());

    server_task.abort();
    Ok(())
}
