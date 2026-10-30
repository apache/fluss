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

const PLAIN_TABLE_ID: TableId = 2;

#[derive(Clone, Copy)]
enum CreateReply {
    Create,
    Fail(FlussError),
    NoResponse,
}

struct PartitionServer {
    port: u16,
    partitions: Mutex<HashMap<String, PartitionId>>,
    create_reply: CreateReply,
    create_requests: Mutex<Vec<String>>,
    answers_cluster_refresh: AtomicBool,
    unanswered_partition_refreshes: Mutex<HashSet<String>>,
    lost_partition_refreshes: Mutex<Vec<String>>,
}

impl PartitionServer {
    async fn start(
        partitions: &[(&str, PartitionId)],
        create_reply: CreateReply,
    ) -> (Arc<Self>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let server = Arc::new(Self {
            port: listener.local_addr().expect("address").port(),
            partitions: Mutex::new(
                partitions
                    .iter()
                    .map(|(name, id)| (name.to_string(), *id))
                    .collect(),
            ),
            create_reply,
            create_requests: Mutex::default(),
            answers_cluster_refresh: AtomicBool::new(true),
            unanswered_partition_refreshes: Mutex::default(),
            lost_partition_refreshes: Mutex::default(),
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
            let (response_type, body) =
                match self.answer(api_key, &payload[REQUEST_HEADER_LENGTH..]) {
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
                .map(|api_key| PbApiVersion {
                    api_key: i32::from(i16::from(api_key)),
                    min_version: 0,
                    max_version: 0,
                })
                .to_vec(),
                server_type: None,
            }
            .encode_to_vec())),
            ApiKey::MetaData => {
                let request =
                    crate::proto::MetadataRequest::decode(body).expect("metadata request");
                if request.table_path.is_empty()
                    && request.partitions_path.is_empty()
                    && !self.answers_cluster_refresh.load(Ordering::Relaxed)
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
                            Ok(partition_metadata(partition_name, partition_id, Some(1)))
                        })
                        .collect::<std::result::Result<Vec<_>, FlussError>>()
                        .map(|partition_metadata| {
                            let mut response = metadata_response(self.port, partition_metadata);
                            response.table_metadata = request
                                .table_path
                                .iter()
                                .filter(|path| path.table_name == "plain")
                                .map(|_| plain_table_metadata())
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
                        .map(|bucket| PbProduceLogRespForBucket {
                            partition_id: bucket.partition_id,
                            bucket_id: bucket.bucket_id,
                            base_offset: Some(0),
                            ..Default::default()
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
        table_path: PbTablePath {
            database_name: "db".to_string(),
            table_name: "plain".to_string(),
        },
        table_id: PLAIN_TABLE_ID,
        schema_id: 1,
        table_json: serde_json::to_vec(&descriptor.serialize_json().expect("table json"))
            .expect("table json"),
        bucket_metadata: vec![crate::proto::PbBucketMetadata {
            bucket_id: 0,
            leader_id: Some(1),
            ..Default::default()
        }],
        ..Default::default()
    }
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

fn partition_write_stack(
    port: u16,
    create_partitions: bool,
) -> (Sender, Arc<Metadata>, Arc<TableInfo>) {
    let table_path = TablePath::new("db", "parts");
    let descriptor = TableDescriptor::builder()
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
        .expect("descriptor");
    let table_info = TableInfo::of(table_path.clone(), 1, 1, descriptor, 0, 0);
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
    let idempotence = enabled_idempotence();
    idempotence.set_writer_id(42);
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

async fn refresh_and_create(
    sender: &Sender,
    unknown_leaders: &HashSet<Arc<PhysicalTablePath>>,
) -> Result<()> {
    if sender.refresh_unknown_leaders(unknown_leaders).await? != RefreshOutcome::RefreshEach {
        return Ok(());
    }
    for (paths, outcome) in join_all(sender.refresh_each(unknown_leaders.clone())).await {
        if outcome == RefreshOutcome::Create {
            for path in paths {
                sender.create_missing_partition(&path).await;
            }
        }
    }
    Ok(())
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
        sender.max_request_timeout_ms = 100;
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
        .await?;

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
async fn failed_partition_creation_keeps_the_writes_of_a_re_created_table() -> Result<()> {
    let (server, server_task) =
        PartitionServer::start(&[], CreateReply::Fail(FlussError::TableNotExist)).await;
    let (sender, _, table_info) = partition_write_stack(server.port, true);
    let (missing, dropped_table_handle) = append_to_partition(&sender, &table_info, "p1");
    let mut re_created = (*table_info).clone();
    re_created.table_id = 2;
    let (_, re_created_table_handle) = append_to_partition(&sender, &Arc::new(re_created), "p1");

    refresh_and_create(&sender, &HashSet::from([missing])).await?;

    assert_eq!(
        write_error_code(&dropped_table_handle).await,
        FlussError::TableNotExist.code()
    );
    assert!(re_created_table_handle.wait().now_or_never().is_none());

    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn retriable_partition_creation_failure_keeps_the_writes() -> Result<()> {
    for (create_reply, answers_cluster_refresh) in [
        (CreateReply::Fail(FlussError::RequestTimeOut), true),
        (CreateReply::Fail(FlussError::RequestTimeOut), false),
        (CreateReply::NoResponse, true),
    ] {
        let (server, server_task) = PartitionServer::start(&[], create_reply).await;
        server
            .answers_cluster_refresh
            .store(answers_cluster_refresh, Ordering::Relaxed);
        let (mut sender, _, table_info) = partition_write_stack(server.port, true);
        sender.max_request_timeout_ms = 100;
        let (missing, missing_handle) = append_to_partition(&sender, &table_info, "p1");
        let unknown_leaders = HashSet::from([missing]);

        bounded(async {
            for _ in 0..2 {
                refresh_and_create(&sender, &unknown_leaders)
                    .await
                    .expect("refresh");
            }
        })
        .await;

        assert_eq!(server.create_requests.lock().len(), 2);
        assert!(missing_handle.wait().now_or_never().is_none());
        assert!(sender.accumulator.has_undrained());

        server_task.abort();
    }
    Ok(())
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
        let (sender, metadata, table_info) = partition_write_stack(server.port, true);
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

        refresh_and_create(&sender, &unknown_leaders).await?;
        assert!(server.create_requests.lock().is_empty());
        refresh_and_create(&sender, &unknown_leaders).await?;

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

    refresh_and_create(&sender, &HashSet::from([leaderless, dropped])).await?;

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
async fn write_error_on_a_dropped_partition_drops_its_cached_leaders() -> Result<()> {
    let (server, server_task) = PartitionServer::start(&[], CreateReply::Create).await;
    let (sender, metadata, table_info) = partition_write_stack(server.port, false);
    metadata
        .update(metadata_response(
            server.port,
            vec![partition_metadata("p1".to_string(), 10, Some(1))],
        ))
        .await?;
    let dropped = Arc::new(PhysicalTablePath::of_partitioned(
        Arc::new(table_info.table_path.clone()),
        Some("p1".to_string()),
    ));

    sender
        .update_metadata_if_needed(
            HashSet::from([table_info.table_path.clone()]),
            HashSet::from([dropped]),
        )
        .await;

    assert!(
        metadata
            .get_cluster()
            .leader_for(&partition_bucket(10))
            .is_none()
    );

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
