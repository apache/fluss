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
use crate::client::FlussAdmin;
use crate::proto::{DatabaseExistsResponse, DropDatabaseResponse, ListPartitionInfosResponse};
use ApiKey::{DatabaseExists, DropDatabase, ListDatabases, ListPartitionInfos, MetaData};
use ServerType::{CoordinatorServer, TabletServer};

async fn listen() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}

fn node(listener: &TcpListener) -> PbServerNode {
    let addr = listener.local_addr().unwrap();
    PbServerNode {
        host: addr.ip().to_string(),
        port: addr.port().into(),
        ..Default::default()
    }
}

fn admin(nodes: MetadataResponse, bootstrap: &str) -> FlussAdmin {
    let cluster = Cluster::from_metadata_response(nodes, None).unwrap();
    let metadata = Arc::new(Metadata::new_for_test(Arc::new(cluster)).with_bootstrap(bootstrap));
    FlussAdmin::new(metadata.connections.clone(), metadata)
}

async fn exchange<T>(server: impl Future<Output = ()>, client: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(server, client).1
    })
    .await
    .expect("Admin RPC exchange timed out")
}

async fn reply_error(stream: &mut TcpStream, mut reply: Vec<u8>, error: FlussError) {
    reply[0] = 1;
    ErrorResponse {
        error_code: error.code(),
        error_message: None,
    }
    .encode(&mut reply)
    .unwrap();
    write_reply(stream, &reply).await;
}

#[tokio::test]
async fn partition_listing_recovers_a_moved_tablet() {
    let empty = listen().await;
    let bootstrap = listen().await;
    let tablet = listen().await;
    let addresses = format!(
        "{},{}",
        empty.local_addr().unwrap(),
        bootstrap.local_addr().unwrap()
    );
    let admin = admin(server_metadata(&[1], 0), &addresses);
    let table = TablePath::new("db", "parts");
    let server = async {
        // Only the second bootstrap knows the tablet's new address.
        let nodes = MetadataResponse {
            coordinator_server: Some(node(&bootstrap)),
            ..Default::default()
        };
        let moved = MetadataResponse {
            tablet_servers: server_metadata(&[1], tablet.local_addr().unwrap().port())
                .tablet_servers,
            ..nodes.clone()
        };
        serve_metadata_as(empty, CoordinatorServer, Ok(nodes)).await;
        serve_metadata_as(bootstrap, CoordinatorServer, Ok(moved)).await;
        let mut ts = accept_handshake(tablet, TabletServer, &[ListPartitionInfos]).await;
        serve_success(
            &mut ts,
            ListPartitionInfos,
            &ListPartitionInfosResponse::default(),
        )
        .await;
    };
    let partitions = exchange(server, admin.list_partition_infos(&table))
        .await
        .unwrap();
    assert!(partitions.is_empty());
}

#[tokio::test]
async fn coordinator_only_bootstrap_allows_discovery_but_not_admin_reads() {
    let listener = listen().await;
    let admin = admin(
        MetadataResponse::default(),
        &listener.local_addr().unwrap().to_string(),
    );
    let nodes = MetadataResponse {
        coordinator_server: Some(node(&listener)),
        ..Default::default()
    };
    let (started, pending) = oneshot::channel();
    let server = async {
        let mut cs = accept_handshake(listener, CoordinatorServer, &[MetaData]).await;
        // The overlapping inspection may accept this node list, but the read still needs a TS.
        let inspection = read_request(&mut cs, MetaData).await;
        started.send(()).unwrap();
        let read = read_request(&mut cs, MetaData).await;
        for mut reply in [inspection, read] {
            nodes.encode(&mut reply).unwrap();
            write_reply(&mut cs, &reply).await;
        }
        for _ in 0..MAX_BOOTSTRAP_RETRIES {
            serve_success(&mut cs, MetaData, &nodes).await;
        }
    };
    let (discovered, read) = exchange(server, async {
        tokio::join!(admin.get_server_nodes(), async {
            pending.await.unwrap();
            let start = tokio::time::Instant::now();
            let result = admin.list_databases().await;
            assert!(start.elapsed() >= Duration::from_millis(700));
            result
        })
    })
    .await;
    assert!(matches!(&discovered.unwrap()[..], [cs] if cs.server_type() == &CoordinatorServer));
    assert!(
        matches!(read, Err(Error::RpcError { source: crate::rpc::RpcError::ConnectionError(message), .. }) if message.contains("No available tablet server"))
    );
}

#[tokio::test]
async fn concurrent_admin_reads_refresh_once_and_retry_once() {
    let listener = listen().await;
    let nodes = server_metadata(&[1], listener.local_addr().unwrap().port());
    let admin = admin(nodes.clone(), "");
    let found = DatabaseExistsResponse { exists: true };
    let server = async {
        let apis = [DatabaseExists, ListDatabases, MetaData];
        let mut ts = accept_handshake(listener, TabletServer, &apis).await;
        // Establish one socket before racing the two requests on it.
        serve_success(&mut ts, DatabaseExists, &found).await;
        let first = read_request(&mut ts, DatabaseExists).await;
        let second = read_request(&mut ts, DatabaseExists).await;
        for reply in [first, second] {
            reply_error(&mut ts, reply, FlussError::NetworkException).await;
        }
        serve_success(&mut ts, MetaData, &nodes).await;
        serve_success(&mut ts, DatabaseExists, &found).await;
        let retry = read_request(&mut ts, DatabaseExists).await;
        reply_error(&mut ts, retry, FlussError::NetworkException).await;
        let denied = read_request(&mut ts, ListDatabases).await;
        reply_error(&mut ts, denied, FlussError::AuthorizationException).await;
        serve_success(&mut ts, DatabaseExists, &found).await;
    };
    exchange(server, async {
        let read = || admin.database_exists("db");
        read().await.unwrap();
        let (first, second) = tokio::join!(read(), read());
        let (ok, err) = if first.is_ok() {
            (first, second)
        } else {
            (second, first)
        };
        assert!(ok.unwrap());
        assert_eq!(
            err.unwrap_err().api_error(),
            Some(FlussError::NetworkException)
        );
        assert_eq!(
            admin.list_databases().await.unwrap_err().api_error(),
            Some(FlussError::AuthorizationException)
        );
        assert!(read().await.unwrap());
    })
    .await;
}

#[tokio::test]
async fn coordinator_mutations_refresh_but_only_replay_a_rejected_request() {
    let tablet = listen().await;
    let old = listen().await;
    let new = listen().await;
    let mut nodes = server_metadata(&[1], tablet.local_addr().unwrap().port());
    nodes.coordinator_server = Some(node(&old));
    let admin = admin(nodes.clone(), "");
    nodes.coordinator_server = Some(node(&new));
    let server = async {
        // The old coordinator takes the mutation, then loses its response.
        let mut first = accept_handshake(old, CoordinatorServer, &[DropDatabase]).await;
        read_request(&mut first, DropDatabase).await;
        drop(first);
        let mut ts = accept_handshake(tablet, TabletServer, &[MetaData]).await;
        serve_success(&mut ts, MetaData, &nodes).await;
        let mut cs = accept_handshake(new, CoordinatorServer, &[DropDatabase]).await;
        let rejected = read_request(&mut cs, DropDatabase).await;
        reply_error(&mut cs, rejected, FlussError::NotCoordinatorLeaderException).await;
        serve_success(&mut ts, MetaData, &nodes).await;
        serve_success(&mut cs, DropDatabase, &DropDatabaseResponse {}).await;
    };
    exchange(server, async {
        let lost = admin.drop_database("ambiguous", false, false).await;
        assert!(matches!(lost, Err(Error::RpcError { .. })));
        admin.drop_database("rejected", false, false).await.unwrap();
    })
    .await;
}
