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
    use crate::proto::ListPartitionInfosResponse;

    let bootstrap = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tablet = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut servers = server_metadata(&[1], 0);
    servers.coordinator_server = Some(PbServerNode {
        host: "127.0.0.1".into(),
        port: i32::from(bootstrap.local_addr().unwrap().port()),
        ..Default::default()
    });
    let stale = Cluster::from_metadata_response(servers.clone(), None).unwrap();
    let empty = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metadata = Metadata::new_for_test(Arc::new(stale)).with_bootstrap(&format!(
        "{},{}",
        empty.local_addr().unwrap(),
        bootstrap.local_addr().unwrap()
    ));
    let admin = FlussAdmin::new(metadata.connections.clone(), Arc::new(metadata));

    servers.tablet_servers[0].port = i32::from(tablet.local_addr().unwrap().port());
    let empty_task = tokio::spawn(serve_metadata_as(
        empty,
        ServerType::CoordinatorServer,
        Ok(MetadataResponse {
            coordinator_server: servers.coordinator_server.clone(),
            ..Default::default()
        }),
    ));
    let bootstrap_task = tokio::spawn(serve_metadata_as(
        bootstrap,
        ServerType::CoordinatorServer,
        Ok(servers),
    ));
    let tablet_task = tokio::spawn(async move {
        let mut stream = accept_handshake(
            tablet,
            ServerType::TabletServer,
            &[ApiKey::ListPartitionInfos],
        )
        .await;
        serve_success(
            &mut stream,
            ApiKey::ListPartitionInfos,
            &ListPartitionInfosResponse::default(),
        )
        .await;
    });

    let partitions = tokio::time::timeout(
        Duration::from_secs(5),
        admin.list_partition_infos(&TablePath::new("db", "parts")),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(partitions.is_empty());
    empty_task.await.unwrap();
    bootstrap_task.await.unwrap();
    tablet_task.await.unwrap();
}

#[tokio::test]
async fn coordinator_only_bootstrap_allows_discovery_but_not_admin_reads() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let nodes = MetadataResponse {
        coordinator_server: Some(PbServerNode {
            host: "127.0.0.1".into(),
            port: i32::from(address.port()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let metadata =
        Arc::new(Metadata::new_for_test(Arc::default()).with_bootstrap(&address.to_string()));
    let admin = FlussAdmin::new(metadata.connections.clone(), metadata);
    let (inspection_started, inspection_pending) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut stream =
            accept_handshake(listener, ServerType::CoordinatorServer, &[ApiKey::MetaData]).await;
        // Hold the inspection open: its coordinator-only result must not finish the read's refresh.
        let inspection = read_request(&mut stream, ApiKey::MetaData).await;
        inspection_started.send(()).unwrap();
        let read = read_request(&mut stream, ApiKey::MetaData).await;
        for mut reply in [inspection, read] {
            nodes.encode(&mut reply).unwrap();
            write_reply(&mut stream, &reply).await;
        }
        for _ in 0..MAX_BOOTSTRAP_RETRIES {
            serve_success(&mut stream, ApiKey::MetaData, &nodes).await;
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        let (discovered, read) = tokio::join!(admin.get_server_nodes(), async {
            inspection_pending.await.unwrap();
            let start = tokio::time::Instant::now();
            let result = admin.list_databases().await;
            assert!(start.elapsed() >= Duration::from_millis(700));
            result
        });
        let discovered = discovered.unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].server_type(), &ServerType::CoordinatorServer);
        assert!(matches!(read, Err(Error::RpcError {
            source: crate::rpc::RpcError::ConnectionError(message), ..
        }) if message.contains("No available tablet server")));
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_admin_reads_refresh_once_and_retry_once() {
    use crate::proto::ListDatabasesResponse;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nodes = server_metadata(&[1], listener.local_addr().unwrap().port());
    let cluster = Cluster::from_metadata_response(nodes.clone(), None).unwrap();
    let metadata = Arc::new(Metadata::new_for_test(Arc::new(cluster)));
    let admin = FlussAdmin::new(metadata.connections.clone(), metadata.clone());
    let server = tokio::spawn(async move {
        let mut stream = accept_handshake(
            listener,
            ServerType::TabletServer,
            &[
                ApiKey::ListDatabases,
                ApiKey::DatabaseExists,
                ApiKey::MetaData,
            ],
        )
        .await;
        let first = read_request(&mut stream, ApiKey::ListDatabases).await;
        let second = read_request(&mut stream, ApiKey::ListDatabases).await;
        for reply in [first, second] {
            reply_error(&mut stream, reply, FlussError::NetworkException).await;
        }
        serve_success(&mut stream, ApiKey::MetaData, &nodes).await;
        serve_success(
            &mut stream,
            ApiKey::ListDatabases,
            &ListDatabasesResponse {
                database_name: vec!["db".into()],
                ..Default::default()
            },
        )
        .await;
        let err = read_request(&mut stream, ApiKey::ListDatabases).await;
        reply_error(&mut stream, err, FlussError::NetworkException).await;
        // A failed replay and a subsequent authorization error must not start new refreshes.
        let denied = read_request(&mut stream, ApiKey::DatabaseExists).await;
        reply_error(&mut stream, denied, FlussError::AuthorizationException).await;
        serve_success(
            &mut stream,
            ApiKey::ListDatabases,
            &ListDatabasesResponse::default(),
        )
        .await;
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        // Both requests must share the established socket used by the simulated TS.
        metadata
            .get_connection(&metadata.admin_read_server().unwrap())
            .await
            .unwrap();
        let (first, second) = tokio::join!(admin.list_databases(), admin.list_databases());
        let (ok, err) = if first.is_ok() {
            (first, second)
        } else {
            (second, first)
        };
        assert_eq!(ok.unwrap(), ["db"]);
        assert_eq!(
            err.unwrap_err().api_error(),
            Some(FlussError::NetworkException)
        );
        assert_eq!(
            admin
                .database_exists("private")
                .await
                .unwrap_err()
                .api_error(),
            Some(FlussError::AuthorizationException)
        );
        assert!(admin.list_databases().await.unwrap().is_empty());
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn coordinator_mutations_refresh_but_only_replay_a_rejected_request() {
    use crate::proto::DropDatabaseResponse;

    let tablet = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let new = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut nodes = server_metadata(&[1], tablet.local_addr().unwrap().port());
    nodes.coordinator_server = Some(PbServerNode {
        host: "127.0.0.1".into(),
        port: i32::from(old.local_addr().unwrap().port()),
        ..Default::default()
    });
    let cluster = Cluster::from_metadata_response(nodes.clone(), None).unwrap();
    let metadata = Arc::new(Metadata::new_for_test(Arc::new(cluster)));
    let admin = FlussAdmin::new(metadata.connections.clone(), metadata);
    nodes.coordinator_server.as_mut().unwrap().port = i32::from(new.local_addr().unwrap().port());

    let server = tokio::spawn(async move {
        // The old coordinator takes the mutation, then loses its response.
        let mut first =
            accept_handshake(old, ServerType::CoordinatorServer, &[ApiKey::DropDatabase]).await;
        read_request(&mut first, ApiKey::DropDatabase).await;
        drop(first);
        let mut ts = accept_handshake(tablet, ServerType::TabletServer, &[ApiKey::MetaData]).await;
        serve_success(&mut ts, ApiKey::MetaData, &nodes).await;
        let mut cs =
            accept_handshake(new, ServerType::CoordinatorServer, &[ApiKey::DropDatabase]).await;
        let rejected = read_request(&mut cs, ApiKey::DropDatabase).await;
        reply_error(&mut cs, rejected, FlussError::NotCoordinatorLeaderException).await;
        serve_success(&mut ts, ApiKey::MetaData, &nodes).await;
        serve_success(&mut cs, ApiKey::DropDatabase, &DropDatabaseResponse {}).await;
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        let err = admin
            .drop_database("ambiguous", false, false)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::RpcError { .. }));
        admin.drop_database("rejected", false, false).await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
