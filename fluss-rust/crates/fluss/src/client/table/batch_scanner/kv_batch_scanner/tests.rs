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
use crate::client::admin::FlussAdmin;
use crate::client::metadata::Metadata;
use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
use crate::metadata::{DataField, DataTypes, PhysicalTablePath, SchemaInfo, TableInfo, TablePath};
use crate::record::kv::{SCHEMA_ID_LENGTH, ValueRecordBatch};
use crate::row::binary::BinaryWriter;
use crate::row::compacted::CompactedRowWriter;
use crate::rpc::test_utils::{
    FramedRequest, install_duplex_connection, read_framed_request, write_error_response,
    write_success_response,
};
use crate::rpc::{ApiKey, RpcClient};
use crate::test_utils::build_table_info_with_columns;
use bytes::Bytes;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, DuplexStream};
use tokio::sync::oneshot;

fn build_two_col_table_info() -> TableInfo {
    build_table_info_with_columns(
        TablePath::new("db".to_string(), "tbl".to_string()),
        42,
        1,
        vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("name", DataTypes::string(), None),
        ],
    )
}

fn protocol_test_cluster(table_info: &TableInfo, bucket: &TableBucket) -> Arc<Cluster> {
    let server = ServerNode::new(
        1,
        "scan-kv-test".to_string(),
        9124,
        ServerType::TabletServer,
    );
    let table_path = Arc::new(table_info.table_path.clone());
    let physical_table_path = Arc::new(match bucket.partition_id() {
        Some(partition_id) => PhysicalTablePath::of_partitioned(
            Arc::clone(&table_path),
            Some(format!("partition-{partition_id}")),
        ),
        None => PhysicalTablePath::of(Arc::clone(&table_path)),
    });
    let location = BucketLocation::new(
        bucket.clone(),
        Some(server.clone()),
        Arc::clone(&physical_table_path),
    );
    let partition_ids = bucket
        .partition_id()
        .map(|partition_id| HashMap::from([(Arc::clone(&physical_table_path), partition_id)]))
        .unwrap_or_default();
    Arc::new(Cluster::new(
        None,
        HashMap::from([(server.id(), server)]),
        HashMap::from([(Arc::clone(&physical_table_path), vec![location.clone()])]),
        HashMap::from([(bucket.clone(), location)]),
        HashMap::from([(table_info.table_path.clone(), table_info.table_id)]),
        HashMap::from([(table_info.table_path.clone(), table_info.clone())]),
        partition_ids,
    ))
}

fn protocol_test_scanner(
    table_info: TableInfo,
    bucket: TableBucket,
) -> (KvBatchScanner, DuplexStream) {
    let cluster = protocol_test_cluster(&table_info, &bucket);
    let leader = cluster
        .leader_for(&bucket)
        .expect("test bucket leader")
        .clone();
    let rpc_client = Arc::new(RpcClient::new());
    let metadata = Arc::new(Metadata::new_for_test_with_connections(
        cluster,
        Arc::clone(&rpc_client),
    ));
    let server_stream = install_duplex_connection(&rpc_client, &leader);
    let schema_getter = Arc::new(ClientSchemaGetter::new(
        table_info.table_path.clone(),
        Arc::new(FlussAdmin::new(
            Arc::clone(&rpc_client),
            Arc::clone(&metadata),
        )),
        SchemaInfo::new(table_info.get_schema().clone(), table_info.get_schema_id()),
    ));
    (
        KvBatchScanner::new(
            rpc_client,
            metadata,
            table_info,
            schema_getter,
            None,
            bucket,
            1024,
        ),
        server_stream,
    )
}

fn decode_scan_request(request: &FramedRequest) -> crate::proto::ScanKvRequest {
    assert_eq!(request.api_key, i16::from(ApiKey::ScanKv));
    assert_eq!(request.api_version, 0);
    crate::proto::ScanKvRequest::decode(request.body.as_slice()).expect("decode ScanKV request")
}

fn kv_record_bytes(schema_id: i16, id: i32, name: &str) -> Bytes {
    let row = compacted(2, |writer| {
        writer.write_int(id);
        writer.write_string(name);
    });
    Bytes::copy_from_slice(value_batch(&[(schema_id, row)]).data())
}

fn value_batch(records: &[(i16, Vec<u8>)]) -> ValueRecordBatch {
    let mut body = Vec::new();
    for (schema_id, row) in records {
        let record_len = (SCHEMA_ID_LENGTH + row.len()) as i32;
        body.extend_from_slice(&record_len.to_le_bytes());
        body.extend_from_slice(&schema_id.to_le_bytes());
        body.extend_from_slice(row);
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&((1 + 4 + body.len()) as i32).to_le_bytes());
    bytes.push(0);
    bytes.extend_from_slice(&(records.len() as i32).to_le_bytes());
    bytes.extend_from_slice(&body);
    ValueRecordBatch::new(Bytes::from(bytes))
}

fn compacted(field_count: usize, write: impl FnOnce(&mut CompactedRowWriter)) -> Vec<u8> {
    let mut writer = CompactedRowWriter::new(field_count);
    write(&mut writer);
    writer.to_bytes().as_ref().to_vec()
}

#[tokio::test]
async fn kv_scanner_sequences_and_pipelines_protocol_requests() {
    let table_info = build_two_col_table_info();
    let schema_id = table_info.get_schema_id() as i16;
    let bucket = TableBucket::new(table_info.table_id, 0);
    let (mut scanner, mut server_stream) = protocol_test_scanner(table_info, bucket.clone());
    let scanner_id = vec![1, 2, 3, 4];
    let expected_scanner_id = scanner_id.clone();
    let (open_seen_tx, open_seen_rx) = oneshot::channel();
    let (release_open_tx, release_open_rx) = oneshot::channel();
    let (continuation_seen_tx, continuation_seen_rx) = oneshot::channel();
    let (release_continuation_tx, release_continuation_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let open_frame = read_framed_request(&mut server_stream).await;
        let open = decode_scan_request(&open_frame);
        assert_eq!(open.scanner_id, None);
        assert_eq!(open.call_seq_id, Some(0));
        assert_eq!(open.batch_size_bytes, Some(1024));
        assert_eq!(open.close_scanner, None);
        let open_bucket = open.bucket_scan_req.expect("open bucket");
        assert_eq!(open_bucket.table_id, bucket.table_id());
        assert_eq!(open_bucket.partition_id, bucket.partition_id());
        assert_eq!(open_bucket.bucket_id, bucket.bucket_id());
        open_seen_tx.send(()).expect("report open request");
        release_open_rx.await.expect("release open response");

        write_success_response(
            &mut server_stream,
            open_frame.request_id,
            &ScanKvResponse {
                scanner_id: Some(scanner_id.clone()),
                has_more_results: Some(true),
                records: Some(kv_record_bytes(schema_id, 1, "one")),
                log_offset: Some(99),
                ..Default::default()
            },
        )
        .await;

        let continuation_1_frame = read_framed_request(&mut server_stream).await;
        let continuation_1 = decode_scan_request(&continuation_1_frame);
        assert_eq!(continuation_1.scanner_id, Some(scanner_id.clone()));
        assert_eq!(continuation_1.bucket_scan_req, None);
        assert_eq!(continuation_1.call_seq_id, Some(1));
        continuation_seen_tx
            .send(())
            .expect("report pipelined continuation");
        release_continuation_rx
            .await
            .expect("release first continuation");

        write_success_response(
            &mut server_stream,
            continuation_1_frame.request_id,
            &ScanKvResponse {
                scanner_id: Some(scanner_id.clone()),
                has_more_results: Some(true),
                records: Some(kv_record_bytes(schema_id, 2, "two")),
                ..Default::default()
            },
        )
        .await;

        let continuation_2_frame = read_framed_request(&mut server_stream).await;
        let continuation_2 = decode_scan_request(&continuation_2_frame);
        assert_eq!(continuation_2.scanner_id, Some(scanner_id));
        assert_eq!(continuation_2.bucket_scan_req, None);
        assert_eq!(continuation_2.call_seq_id, Some(2));
        write_success_response(
            &mut server_stream,
            continuation_2_frame.request_id,
            &ScanKvResponse {
                has_more_results: Some(false),
                ..Default::default()
            },
        )
        .await;
    });

    assert!(matches!(
        scanner
            .next_batch_with_timeout(Duration::from_millis(20))
            .await
            .expect("open timeout"),
        KvBatchReadOutcome::TimedOut
    ));
    open_seen_rx.await.expect("server observed open");
    release_open_tx.send(()).expect("release open response");

    let first = scanner
        .next_batch_with_timeout(Duration::from_secs(1))
        .await
        .expect("first batch");
    let KvBatchReadOutcome::Batch(first) = first else {
        panic!("expected first batch");
    };
    assert_eq!(first.batch().num_rows(), 1);
    assert_eq!(scanner.snapshot_log_offset(), Some(99));
    continuation_seen_rx
        .await
        .expect("continuation must be sent before returning the first batch");
    release_continuation_tx
        .send(())
        .expect("release continuation response");

    let second = scanner
        .next_batch_with_timeout(Duration::from_secs(1))
        .await
        .expect("second batch");
    let KvBatchReadOutcome::Batch(second) = second else {
        panic!("expected second batch");
    };
    assert_eq!(second.batch().num_rows(), 1);
    assert_eq!(
        scanner.scanner_id.as_deref(),
        Some(expected_scanner_id.as_slice())
    );
    assert!(matches!(
        scanner
            .next_batch_with_timeout(Duration::from_secs(1))
            .await
            .expect("terminal response"),
        KvBatchReadOutcome::Finished
    ));
    server.await.expect("protocol server");
}

#[tokio::test(start_paused = true)]
async fn kv_scanner_retries_too_many_scanners_as_a_fresh_open() {
    let table_info = build_two_col_table_info();
    let bucket = TableBucket::new(table_info.table_id, 0);
    let (mut scanner, mut server_stream) = protocol_test_scanner(table_info, bucket);

    let server = tokio::spawn(async move {
        let first_frame = read_framed_request(&mut server_stream).await;
        let first = decode_scan_request(&first_frame);
        assert_eq!(first.scanner_id, None);
        assert_eq!(first.call_seq_id, Some(0));
        write_success_response(
            &mut server_stream,
            first_frame.request_id,
            &ScanKvResponse {
                error_code: Some(FlussError::TooManyScanners.code()),
                error_message: Some("busy".to_string()),
                ..Default::default()
            },
        )
        .await;

        let retry_frame = read_framed_request(&mut server_stream).await;
        let retry = decode_scan_request(&retry_frame);
        assert_eq!(retry.scanner_id, None);
        assert_eq!(retry.call_seq_id, Some(0));
        assert_eq!(retry.bucket_scan_req, first.bucket_scan_req);
        write_success_response(
            &mut server_stream,
            retry_frame.request_id,
            &ScanKvResponse {
                has_more_results: Some(false),
                ..Default::default()
            },
        )
        .await;
    });

    assert!(matches!(
        scanner
            .next_batch_with_timeout(Duration::from_secs(1))
            .await
            .expect("retry open scanner"),
        KvBatchReadOutcome::Finished
    ));
    assert_eq!(scanner.open_retries, 1);
    server.await.expect("retry protocol server");
}

#[tokio::test]
async fn kv_scanner_close_sends_close_for_the_open_session() {
    let table_info = build_two_col_table_info();
    let schema_id = table_info.get_schema_id() as i16;
    let bucket = TableBucket::new(table_info.table_id, 0);
    let (mut scanner, mut server_stream) = protocol_test_scanner(table_info, bucket);
    let scanner_id = vec![9, 8, 7];
    let expected_scanner_id = scanner_id.clone();

    let server = tokio::spawn(async move {
        let open_frame = read_framed_request(&mut server_stream).await;
        write_success_response(
            &mut server_stream,
            open_frame.request_id,
            &ScanKvResponse {
                scanner_id: Some(scanner_id.clone()),
                has_more_results: Some(true),
                records: Some(kv_record_bytes(schema_id, 1, "one")),
                ..Default::default()
            },
        )
        .await;

        let next_frame = read_framed_request(&mut server_stream).await;
        let next = decode_scan_request(&next_frame);
        let (close_frame, close) = if next.close_scanner == Some(true) {
            (next_frame, next)
        } else {
            assert_eq!(next.call_seq_id, Some(1));
            let close_frame = read_framed_request(&mut server_stream).await;
            let close = decode_scan_request(&close_frame);
            (close_frame, close)
        };
        assert_eq!(close.scanner_id, Some(scanner_id));
        assert_eq!(close.bucket_scan_req, None);
        assert_eq!(close.call_seq_id, None);
        assert_eq!(close.close_scanner, Some(true));
        write_success_response(
            &mut server_stream,
            close_frame.request_id,
            &ScanKvResponse::default(),
        )
        .await;
    });

    assert!(matches!(
        scanner
            .next_batch_with_timeout(Duration::from_secs(1))
            .await
            .expect("first batch"),
        KvBatchReadOutcome::Batch(_)
    ));
    assert_eq!(
        scanner.scanner_id.as_deref(),
        Some(expected_scanner_id.as_slice())
    );
    scanner.close().await.expect("close scanner");
    let error = scanner
        .next_batch_with_timeout(Duration::from_secs(1))
        .await
        .expect_err("an explicitly closed scanner must not report completion");
    assert!(
        error
            .to_string()
            .contains("closed before it was fully drained")
    );
    server.await.expect("close protocol server");

    let table_info = build_two_col_table_info();
    let bucket = TableBucket::new(table_info.table_id, 0);
    let (bucket_scanner, _server_stream) = protocol_test_scanner(table_info, bucket);
    let mut snapshot_scanner = KvSnapshotScanner::new(vec![bucket_scanner]);
    snapshot_scanner.close().await.expect("close snapshot");
    let error = snapshot_scanner
        .next_batch_with_timeout(Duration::from_secs(1))
        .await
        .expect_err("an explicitly closed snapshot must not report completion");
    assert!(error.to_string().contains("closed before every bucket"));
}

#[tokio::test]
async fn not_leader_refresh_is_partition_aware_and_synchronous() {
    let table_info = build_two_col_table_info();
    let partition_id = 77;
    let bucket = TableBucket::new_with_partition(table_info.table_id, Some(partition_id), 0);
    let (mut scanner, mut server_stream) = protocol_test_scanner(table_info, bucket);
    let (refresh_seen_tx, refresh_seen_rx) = oneshot::channel();
    let (release_refresh_tx, release_refresh_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let scan_frame = read_framed_request(&mut server_stream).await;
        write_success_response(
            &mut server_stream,
            scan_frame.request_id,
            &ScanKvResponse {
                error_code: Some(FlussError::NotLeaderOrFollower.code()),
                error_message: Some("moved".to_string()),
                ..Default::default()
            },
        )
        .await;

        let refresh_frame = read_framed_request(&mut server_stream).await;
        assert_eq!(refresh_frame.api_key, i16::from(ApiKey::MetaData));
        assert_eq!(refresh_frame.api_version, 0);
        let refresh = crate::proto::MetadataRequest::decode(refresh_frame.body.as_slice())
            .expect("decode metadata refresh");
        assert_eq!(refresh.partitions_id, vec![partition_id]);
        assert_eq!(refresh.table_path.len(), 1);
        assert_eq!(refresh.table_path[0].database_name, "db");
        assert_eq!(refresh.table_path[0].table_name, "tbl");
        refresh_seen_tx.send(()).expect("report metadata refresh");
        release_refresh_rx.await.expect("release metadata refresh");
        write_error_response(
            &mut server_stream,
            refresh_frame.request_id,
            1,
            "refresh failed",
        )
        .await;
    });

    let mut scan = tokio::spawn(async move {
        let result = scanner
            .next_batch_with_timeout(Duration::from_secs(5))
            .await;
        (scanner, result)
    });
    tokio::select! {
        refresh = refresh_seen_rx => {
            refresh.expect("partition metadata refresh request");
        }
        completed = &mut scan => {
            let (_, result) = completed.expect("scanner task");
            panic!("scanner returned before sending metadata refresh: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_secs(1)) => {
            panic!("partition metadata refresh must be sent");
        }
    }
    assert!(
        !scan.is_finished(),
        "NotLeader must not return before metadata refresh completes"
    );
    release_refresh_tx
        .send(())
        .expect("complete metadata refresh");
    let (scanner, result) = tokio::time::timeout(Duration::from_secs(1), &mut scan)
        .await
        .expect("scanner must return after refresh completes")
        .expect("scanner task");
    let error = result.expect_err("NotLeader remains the authoritative error");
    assert_eq!(error.api_error(), Some(FlussError::NotLeaderOrFollower));
    assert!(scanner.closed);
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("NotLeader protocol server must complete")
        .expect("NotLeader protocol server");
}

#[tokio::test]
async fn kv_snapshot_scanner_stays_terminal_after_bucket_failure() {
    let table_info = build_two_col_table_info();
    let (first, mut first_server_stream) =
        protocol_test_scanner(table_info.clone(), TableBucket::new(table_info.table_id, 0));
    let (second, mut second_server_stream) =
        protocol_test_scanner(table_info.clone(), TableBucket::new(table_info.table_id, 1));
    let first_server = tokio::spawn(async move {
        let open_frame = read_framed_request(&mut first_server_stream).await;
        write_success_response(
            &mut first_server_stream,
            open_frame.request_id,
            &ScanKvResponse {
                scanner_id: Some(vec![1, 2, 3]),
                ..Default::default()
            },
        )
        .await;
    });
    let second_server = tokio::spawn(async move {
        let mut byte = [0];
        if let Ok(Ok(_)) = tokio::time::timeout(
            Duration::from_millis(100),
            second_server_stream.read_exact(&mut byte),
        )
        .await
        {
            panic!("whole-table scanner advanced to the next bucket");
        }
    });

    let mut scanner = KvSnapshotScanner::new(vec![first, second]);
    let original_error = scanner
        .next_batch_with_timeout(Duration::from_secs(1))
        .await
        .expect_err("first bucket must fail");
    assert!(
        original_error
            .to_string()
            .contains("did not include has_more_results")
    );

    let error = scanner
        .next_batch_with_timeout(Duration::from_secs(1))
        .await
        .expect_err("a failed whole-table scanner must remain terminal");
    assert!(
        error
            .to_string()
            .contains("KvSnapshotScanner cannot be resumed")
    );

    scanner.close().await.expect("close terminal scanner");
    let error = scanner
        .next_batch_with_timeout(Duration::from_secs(1))
        .await
        .expect_err("close must not turn a failed scan into normal completion");
    assert!(
        error
            .to_string()
            .contains("KvSnapshotScanner cannot be resumed")
    );
    first_server.await.expect("first bucket protocol server");
    second_server.await.expect("second bucket protocol server");
}
