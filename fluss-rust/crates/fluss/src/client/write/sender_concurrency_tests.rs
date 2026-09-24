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
use crate::client::{ResultHandle, WriteRecord};
use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
use crate::config::Config;
use crate::proto::{
    ApiVersionsResponse, PbApiVersion, PbProduceLogRespForBucket, ProduceLogResponse,
};
use crate::row::{Datum, GenericRow};
use crate::test_utils::build_table_info;
use futures::FutureExt;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

fn fixture(port: u32, max_inflight: usize) -> (Sender, Arc<Cluster>) {
    let server = ServerNode::new(1, "127.0.0.1".into(), port, ServerType::TabletServer);
    let mut paths = HashMap::new();
    let mut buckets = HashMap::new();
    let mut ids = HashMap::new();
    let mut infos = HashMap::new();
    for table_id in [1, 2] {
        let path = TablePath::new("db", format!("table_{table_id}"));
        let physical = Arc::new(PhysicalTablePath::of(Arc::new(path.clone())));
        let bucket = TableBucket::new(table_id, 0);
        let location = BucketLocation::new(bucket.clone(), Some(server.clone()), physical.clone());
        paths.insert(physical, vec![location.clone()]);
        buckets.insert(bucket, location);
        ids.insert(path.clone(), table_id);
        infos.insert(path.clone(), build_table_info(path, table_id, 1));
    }
    let cluster = Arc::new(Cluster::new(
        None,
        HashMap::from([(1, server)]),
        paths,
        buckets,
        ids,
        infos,
        HashMap::new(),
    ));
    let idempotence = Arc::new(IdempotenceManager::new(true, max_inflight));
    idempotence.set_writer_id(99);
    let accumulator = Arc::new(RecordAccumulator::new(
        Config {
            writer_batch_timeout_ms: 0,
            ..Default::default()
        },
        idempotence.clone(),
    ));
    let sender = Sender::new(
        Arc::new(Metadata::new_for_test(cluster.clone())),
        accumulator,
        1024 * 1024,
        1000,
        -1,
        0,
        100,
        1000,
        idempotence,
        Arc::new(crate::metrics::WriterMetrics::new()),
    );
    (sender, cluster)
}

fn append(sender: &Sender, cluster: &Cluster, table_id: TableId) -> Result<ResultHandle> {
    let path = TablePath::new("db", format!("table_{table_id}"));
    let info = Arc::new(build_table_info(path.clone(), table_id, 1));
    let physical = Arc::new(PhysicalTablePath::of(Arc::new(path)));
    let row = GenericRow {
        values: vec![Datum::Int32(42)],
    };
    let record = WriteRecord::for_append(info, physical, 1, &row);
    Ok(sender
        .accumulator
        .append(&record, 0, cluster, false)?
        .result_handle
        .expect("result handle"))
}

async fn independent_completions(fast_error: FlussError) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sender, cluster) = fixture(listener.local_addr().unwrap().port() as u32, 1);
    let handles = [append(&sender, &cluster, 1)?, append(&sender, &cluster, 2)?];
    let (release, released) = oneshot::channel();
    let (close, closed) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let first = read_produce_request(&mut stream).await;
        // Neither response is sent until BOTH requests arrive on the same connection.
        let second = read_produce_request(&mut stream).await;
        // In the error case, fail one table while the other is still in flight.
        respond_produce(&mut stream, second, fast_error).await;
        released.await.unwrap();
        respond_produce(&mut stream, first, FlussError::None).await;
        let _ = closed.await;
    });
    // Reuse an established connection, as the benchmark does after warmup.
    // Cold connection deduplication is outside the per-table scheduling contract.
    sender
        .metadata
        .get_connection(cluster.get_tablet_server(1).unwrap())
        .await?;
    let (sends, _, _) = sender.drain_ready_sends()?;
    assert_eq!(
        sends.len(),
        2,
        "one independently scheduled future per table"
    );
    let mut pending: FuturesUnordered<_> = sends.into_iter().collect();
    pending.next().await.expect("second request completes")?;
    assert_eq!(pending.len(), 1, "first request is still pending");
    assert_eq!(
        handles
            .iter()
            .filter(|h| h.wait().now_or_never().is_some())
            .count(),
        1,
        "one table completes without waiting for the other table"
    );
    assert!(sender.accumulator.has_incomplete());
    release.send(()).unwrap();
    pending.next().await.expect("first request completes")?;
    assert!(pending.next().await.is_none());
    let mut failures = 0;
    for handle in handles {
        if let Err(error) = handle.wait().await? {
            assert!(matches!(error, broadcast::Error::WriteFailed { code, .. }
                if code == fast_error.code()));
            failures += 1;
        }
    }
    assert_eq!(failures, usize::from(fast_error != FlussError::None));
    assert!(!sender.accumulator.has_incomplete());
    assert_eq!(
        sender.accumulator.buffer_available_bytes(),
        sender.accumulator.buffer_total_bytes()
    );
    for table_id in [1, 2] {
        assert_eq!(
            sender
                .idempotence_manager
                .in_flight_count(&TableBucket::new(table_id, 0)),
            0
        );
    }
    close.send(()).unwrap();
    server.await.unwrap();
    Ok(())
}

#[tokio::test]
async fn slow_table_does_not_block_another_tables_dispatch_or_completion() -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(10),
        independent_completions(FlussError::None),
    )
    .await
    .expect("cross-table sends must not await the first response")
}

#[tokio::test]
async fn terminal_error_is_scoped_to_its_table_and_releases_all_batches() -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(10),
        independent_completions(FlussError::RecordTooLargeException),
    )
    .await
    .expect("a table error must not strand other requests")
}

#[tokio::test]
async fn splitting_tables_keeps_bucket_inflight_and_sequence_constraints() -> Result<()> {
    let (sender, cluster) = fixture(9092, 1);
    let first_handle = append(&sender, &cluster, 1)?;
    let nodes = HashSet::from([cluster.get_tablet_server(1).unwrap().clone()]);
    let mut first = sender
        .accumulator
        .drain(cluster.clone(), &nodes, 1024 * 1024)?;
    sender.add_to_inflight_batches(&first);
    let batch = first.remove(&1).unwrap().pop().unwrap();
    assert_eq!(batch.write_batch.batch_sequence(), 0);
    let next_handle = append(&sender, &cluster, 1)?;
    let other_handle = append(&sender, &cluster, 2)?;
    let mut drained = sender
        .accumulator
        .drain(cluster.clone(), &nodes, 1024 * 1024)?;
    sender.add_to_inflight_batches(&drained);
    let other = drained.remove(&1).unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(
        other[0].table_bucket.table_id(),
        2,
        "table 1 must stay blocked"
    );
    assert_eq!(other[0].write_batch.batch_sequence(), 0);
    sender.complete_batch(other.into_iter().next().unwrap());
    assert!(other_handle.wait().await?.is_ok());
    assert!(next_handle.wait().now_or_never().is_none());
    sender.complete_batch(batch);
    assert!(first_handle.wait().await?.is_ok());
    let mut next = sender.accumulator.drain(cluster, &nodes, 1024 * 1024)?;
    sender.add_to_inflight_batches(&next);
    let batches = next.remove(&1).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].table_bucket.table_id(), 1);
    assert_eq!(batches[0].write_batch.batch_sequence(), 1);
    sender.complete_batch(batches.into_iter().next().unwrap());
    assert!(next_handle.wait().await?.is_ok());
    assert!(!sender.accumulator.has_incomplete());
    Ok(())
}

#[tokio::test]
async fn retry_backoff_keeps_sequence_and_does_not_block_another_table() -> Result<()> {
    let (mut sender, cluster) = fixture(9092, 1);
    sender.retries = 2;
    // Make the backoff long enough that this assertion does not depend on CPU speed.
    sender.retry_backoff_ms = 60_000;
    sender.retry_max_backoff_ms = 60_000;
    let retried = append(&sender, &cluster, 1)?;
    let nodes = HashSet::from([cluster.get_tablet_server(1).unwrap().clone()]);
    let mut drained = sender
        .accumulator
        .drain(cluster.clone(), &nodes, 1024 * 1024)?;
    sender.add_to_inflight_batches(&drained);
    let batch = drained.remove(&1).unwrap().pop().unwrap();
    assert_eq!(batch.write_batch.batch_sequence(), 0);
    sender.handle_write_batch_error(
        batch,
        FlussError::RequestTimeOut,
        "controlled retry".into(),
        &mut Vec::new(),
    )?;
    let other = append(&sender, &cluster, 2)?;
    let mut drained = sender
        .accumulator
        .drain(cluster.clone(), &nodes, 1024 * 1024)?;
    sender.add_to_inflight_batches(&drained);
    let batches = drained.remove(&1).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].table_bucket.table_id(), 2);
    sender.complete_batch(batches.into_iter().next().unwrap());
    assert!(other.wait().await?.is_ok());
    assert!(retried.wait().now_or_never().is_none());
    sender.accumulator.clear_retry_backoff();
    let mut drained = sender.accumulator.drain(cluster, &nodes, 1024 * 1024)?;
    sender.add_to_inflight_batches(&drained);
    let batches = drained.remove(&1).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].table_bucket.table_id(), 1);
    assert_eq!(
        batches[0].write_batch.batch_sequence(),
        0,
        "retry must reuse its sequence"
    );
    assert_eq!(batches[0].write_batch.attempts(), 1);
    sender.complete_batch(batches.into_iter().next().unwrap());
    assert!(retried.wait().await?.is_ok());
    assert!(!sender.accumulator.has_incomplete());
    Ok(())
}

/// Handle the handshake, then let the test choose when to respond to each produce request.
pub(super) async fn read_produce_request(stream: &mut TcpStream) -> i32 {
    loop {
        let len = stream.read_i32().await.expect("request length");
        let mut payload = vec![0u8; len as usize];
        stream.read_exact(&mut payload).await.expect("request");
        let api_key = i16::from_be_bytes(payload[..2].try_into().unwrap());
        let request_id = i32::from_be_bytes(payload[4..8].try_into().unwrap());
        if api_key == 1014 {
            return request_id;
        }
        assert_eq!(api_key, 1000, "expected ApiVersions or ProduceLog");
        let response = ApiVersionsResponse {
            api_versions: vec![
                PbApiVersion {
                    api_key: 1000, // ApiVersions
                    min_version: 0,
                    max_version: 0,
                },
                PbApiVersion {
                    api_key: 1014, // ProduceLog
                    min_version: 0,
                    max_version: 0,
                },
            ],
            server_type: Some(ServerType::TabletServer.to_type_id()),
        };
        write_controlled_response(stream, request_id, response).await;
    }
}

pub(super) async fn write_controlled_response(
    stream: &mut TcpStream,
    request_id: i32,
    response: impl Message,
) {
    let body = response.encode_to_vec();
    stream
        .write_i32((5 + body.len()) as i32)
        .await
        .expect("response length");
    stream.write_u8(0).await.expect("success response type");
    stream.write_i32(request_id).await.expect("request id");
    stream.write_all(&body).await.expect("response body");
}

pub(super) async fn respond_produce(stream: &mut TcpStream, request_id: i32, error: FlussError) {
    let response = ProduceLogResponse {
        buckets_resp: vec![PbProduceLogRespForBucket {
            bucket_id: 0,
            error_code: Some(error.code()),
            ..Default::default()
        }],
    };
    write_controlled_response(stream, request_id, response).await;
}
