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

use crate::cluster::{Cluster, ServerNode, ServerType};
use crate::error::{Error, FlussError, Result};
use crate::metadata::{PhysicalTablePath, TableBucket, TablePath};
use crate::proto::MetadataResponse;
use crate::rpc::from_pb_table_path;
use crate::rpc::message::{GetTableRequest, UpdateMetadataRequest};
use crate::rpc::{RpcClient, ServerConnection};
use crate::{PartitionId, TableId};
use arc_swap::{ArcSwap, Guard};
use log::{info, warn};
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::collections::HashSet;
use std::future::Future;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

const MAX_BOOTSTRAP_RETRIES: usize = 3;
const BOOTSTRAP_RETRY_INTERVAL_MS: u64 = 100;

pub struct Metadata {
    cluster: ArcSwap<Cluster>,
    /// Build the next cluster under it, otherwise concurrent changes get lost.
    changing: Mutex<()>,
    /// Keyed by endpoint, so a failure at an old address does not exclude the server at a new one.
    unavailable_tablet_servers: RwLock<HashSet<ServerNode>>,
    connections: Arc<RpcClient>,
    bootstrap: Arc<str>,
    cluster_version_tx: watch::Sender<u64>,
}

#[derive(Debug)]
struct BootstrapServer {
    raw: String,
    address: Result<SocketAddr>,
}

impl Metadata {
    pub async fn new(bootstrap: &str, connections: Arc<RpcClient>) -> Result<Self> {
        let cluster = Self::init_cluster(bootstrap, connections.clone()).await?;
        let (cluster_version_tx, _) = watch::channel(0);
        Ok(Metadata {
            cluster: ArcSwap::from_pointee(cluster),
            changing: Mutex::new(()),
            unavailable_tablet_servers: RwLock::new(HashSet::new()),
            connections,
            bootstrap: bootstrap.into(),
            cluster_version_tx,
        })
    }

    pub fn subscribe_cluster_changes(&self) -> watch::Receiver<u64> {
        self.cluster_version_tx.subscribe()
    }

    fn notify_cluster_changed(&self) {
        self.cluster_version_tx
            .send_modify(|v| *v = v.wrapping_add(1));
    }

    fn parse_bootstrap(bootstrap: &str) -> Result<SocketAddr> {
        // Resolve all socket addresses and deterministically choose one.
        let addrs = bootstrap
            .to_socket_addrs()
            .map_err(|e| Error::IllegalArgument {
                message: format!("Invalid bootstrap address '{bootstrap}': {e}"),
            })?;

        // Prefer IPv4 addresses; if none are available, fall back to the first IPv6.
        let mut ipv6_candidate: Option<SocketAddr> = None;
        for addr in addrs {
            if addr.is_ipv4() {
                return Ok(addr);
            }
            if ipv6_candidate.is_none() {
                ipv6_candidate = Some(addr);
            }
        }

        let addr = ipv6_candidate.ok_or_else(|| Error::IllegalArgument {
            message: format!("Unable to resolve bootstrap address '{bootstrap}'"),
        })?;
        Ok(addr)
    }

    fn parse_bootstrap_servers(bootstrap_servers: &str) -> Result<Vec<BootstrapServer>> {
        let mut bootstraps = Vec::new();

        for bootstrap in bootstrap_servers.split(',') {
            let bootstrap = bootstrap.trim();
            if bootstrap.is_empty() {
                continue;
            }
            bootstraps.push(BootstrapServer {
                raw: bootstrap.to_string(),
                address: Self::parse_bootstrap(bootstrap),
            });
        }

        if bootstraps.is_empty() {
            return Err(Error::IllegalArgument {
                message: "No bootstrap servers configured".to_string(),
            });
        }

        Ok(bootstraps)
    }

    async fn retry_bootstrap<T, Operation, OperationFuture, Disconnect>(
        bootstrap: &str,
        mut operation: Operation,
        mut disconnect: Disconnect,
    ) -> Result<T>
    where
        Operation: FnMut() -> OperationFuture,
        OperationFuture: Future<Output = Result<T>>,
        Disconnect: FnMut(),
    {
        let mut retry_count = 0;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(err) if err.is_retriable() && retry_count < MAX_BOOTSTRAP_RETRIES => {
                    disconnect();
                    let delay_ms = BOOTSTRAP_RETRY_INTERVAL_MS * (1_u64 << retry_count);
                    retry_count += 1;
                    warn!(
                        "Failed to initialize cluster from bootstrap server '{bootstrap}' \
                         (retry {retry_count}/{MAX_BOOTSTRAP_RETRIES}). Retrying in \
                         {delay_ms} ms: {err}"
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn init_cluster(bootstrap_servers: &str, connections: Arc<RpcClient>) -> Result<Cluster> {
        let bootstraps = Self::parse_bootstrap_servers(bootstrap_servers)?;
        let bootstrap_count = bootstraps.len();
        let mut errors = Vec::new();
        let mut last_connection_error = None;

        for (index, bootstrap) in bootstraps.into_iter().enumerate() {
            let socket_address = match bootstrap.address {
                Ok(socket_address) => socket_address,
                Err(err) => {
                    if index + 1 < bootstrap_count {
                        warn!(
                            "Failed to resolve bootstrap server '{}', trying the next server: {err}",
                            bootstrap.raw
                        );
                    }
                    errors.push(format!("{}: {err}", bootstrap.raw));
                    continue;
                }
            };
            let server_node = ServerNode::new(
                -1,
                socket_address.ip().to_string(),
                socket_address.port() as u32,
                ServerType::Unknown,
            );

            let cluster_result = if bootstrap_count == 1 {
                Self::retry_bootstrap(
                    &bootstrap.raw,
                    || Self::fetch_cluster_from_bootstrap(&server_node, connections.clone()),
                    || connections.disconnect_endpoint(&server_node),
                )
                .await
            } else {
                Self::fetch_cluster_from_bootstrap(&server_node, connections.clone()).await
            };

            match cluster_result {
                Ok(cluster) => return Ok(cluster),
                Err(err) => {
                    if index + 1 < bootstrap_count {
                        warn!(
                            "Failed to initialize cluster from bootstrap server '{}', trying the next server: {err}",
                            bootstrap.raw
                        );
                    }
                    errors.push(format!("{}: {err}", bootstrap.raw));
                    last_connection_error = Some(err);
                }
            }
        }

        Err(Self::bootstrap_initialization_error(
            bootstrap_servers,
            errors,
            last_connection_error,
        ))
    }

    fn bootstrap_initialization_error(
        bootstrap_servers: &str,
        errors: Vec<String>,
        last_connection_error: Option<Error>,
    ) -> Error {
        if let Some(error) = last_connection_error {
            warn!(
                "Unable to initialize cluster from bootstrap servers '{bootstrap_servers}': {}",
                errors.join("; ")
            );
            return error;
        }

        let message = format!(
            "Unable to initialize cluster from bootstrap servers '{bootstrap_servers}': {}",
            errors.join("; ")
        );
        Error::IllegalArgument { message }
    }

    async fn fetch_cluster_from_bootstrap(
        server_node: &ServerNode,
        connections: Arc<RpcClient>,
    ) -> Result<Cluster> {
        let con = connections.get_connection(server_node).await?;

        let response = con
            .request(UpdateMetadataRequest::new(
                &HashSet::default(),
                &HashSet::new(),
                vec![],
            ))
            .await?;
        Cluster::from_metadata_response(response, None)
    }

    pub(crate) async fn reinit_cluster(&self) -> Result<()> {
        let cluster = Self::init_cluster(&self.bootstrap, self.connections.clone()).await?;
        let changing = self.changing.lock();
        self.replace_cluster(&changing, cluster);
        Ok(())
    }

    fn replace_cluster(&self, changing: &MutexGuard<'_, ()>, cluster: Cluster) {
        let cluster = self.swap_cluster(changing, cluster);
        // A successful refresh may advertise new addresses for previously failed servers.
        self.unavailable_tablet_servers
            .write()
            .retain(|server| cluster.get_tablet_server(server.id()).is_none());
        self.connections
            .disconnect_superseded_endpoints(&cluster.get_server_nodes());
        self.notify_cluster_changed();
    }

    /// The cluster a change builds on, readable only under `changing`.
    fn current_cluster(&self, _changing: &MutexGuard<'_, ()>) -> Guard<Arc<Cluster>> {
        self.cluster.load()
    }

    fn swap_cluster(&self, _changing: &MutexGuard<'_, ()>, cluster: Cluster) -> Arc<Cluster> {
        let cluster = Arc::new(cluster);
        self.cluster.store(Arc::clone(&cluster));
        cluster
    }

    fn invalidate(&self, build: impl FnOnce(&Cluster) -> Option<Cluster>) -> bool {
        let changing = self.changing.lock();
        let Some(cluster) = build(&self.current_cluster(&changing)) else {
            return false;
        };
        self.swap_cluster(&changing, cluster);
        self.notify_cluster_changed();
        true
    }

    pub fn invalidate_server(&self, server_id: &i32, table_ids: Vec<i64>) {
        self.invalidate(|cluster| Some(cluster.invalidate_server(server_id, table_ids)));
    }

    pub fn invalidate_physical_table_meta(
        &self,
        physical_tables_to_invalid: &HashSet<PhysicalTablePath>,
    ) {
        self.invalidate(|cluster| {
            Some(cluster.invalidate_physical_table_meta(physical_tables_to_invalid))
        });
    }

    /// Drops the cached leaders of those of `table_buckets` that `server_id` still leads, so that a
    /// leader already replaced by a metadata refresh is kept. Leaves the cache as it is, without
    /// notifying a change, if `server_id` leads none of them.
    pub fn invalidate_bucket_leaders(&self, table_buckets: &HashSet<TableBucket>, server_id: i32) {
        self.invalidate(|cluster| cluster.invalidate_bucket_leaders(table_buckets, server_id));
    }

    /// Drops the cached leaders of `partition_path` if it still resolves to `partition_id` of
    /// table `table_id`, and returns whether it does. A partition found missing by a request sent
    /// before the cache learned a newer table or partition under the path says nothing about it.
    pub fn invalidate_partition_if_unchanged(
        &self,
        partition_path: &PhysicalTablePath,
        table_id: Option<TableId>,
        partition_id: Option<PartitionId>,
    ) -> bool {
        self.invalidate(|cluster| {
            if cluster.get_table_id(partition_path.get_table_path()) != table_id
                || cluster.get_partition_id(partition_path) != partition_id
            {
                return None;
            }
            Some(cluster.invalidate_physical_table_meta(&HashSet::from([partition_path.clone()])))
        })
    }

    /// Fetches the table id from the cluster, bypassing the local cache.
    /// Returns `Ok(None)` when the table does not exist. Unlike the metadata
    /// refresh, which wraps a missing table in a generic server error, this
    /// reports `TableNotExist` so callers can tell a dropped table apart from
    /// a transient failure.
    pub async fn fetch_table_id(&self, table_path: &TablePath) -> Result<Option<TableId>> {
        let maybe_server = self.cluster.load().get_one_available_server().cloned();
        let server = maybe_server.ok_or_else(|| Error::UnexpectedError {
            message: "No available server to fetch table metadata".to_string(),
            source: None,
        })?;

        let conn = self.connections.get_connection(&server).await?;
        match conn.request(GetTableRequest::new(table_path)).await {
            Ok(response) => Ok(Some(response.table_id)),
            Err(e) if e.api_error() == Some(FlussError::TableNotExist) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Drops the cached id, info, partitions and bucket locations of `table_path`.
    pub fn evict_table_metadata(&self, table_path: &TablePath) {
        self.invalidate(|cluster| Some(cluster.evict_table(table_path)));
    }

    pub async fn update(&self, metadata_response: MetadataResponse) -> Result<()> {
        let changing = self.changing.lock();
        let cluster = Cluster::from_metadata_response(
            metadata_response,
            Some(&self.current_cluster(&changing)),
        )?;
        self.replace_cluster(&changing, cluster);
        Ok(())
    }

    fn server_for_refresh(&self) -> Option<ServerNode> {
        let cluster = self.cluster.load();
        let excluded: HashSet<i32> = self
            .unavailable_tablet_servers
            .read()
            .iter()
            .filter(|server| cluster.get_tablet_server(server.id()) == Some(*server))
            .map(ServerNode::id)
            .collect();
        cluster
            .get_one_available_server_excluding(&excluded)
            .cloned()
    }

    pub async fn update_tables_metadata(
        &self,
        table_paths: &HashSet<&TablePath>,
        physical_table_paths: &HashSet<&Arc<PhysicalTablePath>>,
        partition_ids: Vec<i64>,
    ) -> Result<()> {
        if let Some(response) = self
            .fetch_tables_metadata(table_paths, physical_table_paths, partition_ids)
            .await?
        {
            self.update(response).await?;
        }
        Ok(())
    }

    /// Updates lookup routing and returns the tables omitted from a successful response, which the
    /// server does for tables the client may not describe, even if they are cached. A refresh
    /// through the bootstrap servers only discovers servers, so it reports no table as omitted.
    pub(crate) async fn update_lookup_metadata(
        &self,
        table_paths: &HashSet<&TablePath>,
        partition_ids: Vec<PartitionId>,
    ) -> Result<HashSet<TablePath>> {
        let Some(response) = self
            .fetch_tables_metadata(table_paths, &HashSet::new(), partition_ids)
            .await?
        else {
            return Ok(HashSet::new());
        };
        let returned_tables: HashSet<TablePath> = response
            .table_metadata
            .iter()
            .map(|table| from_pb_table_path(&table.table_path))
            .collect();
        let missing_tables = table_paths
            .iter()
            .filter(|path| !returned_tables.contains(*path))
            .map(|path| (*path).clone())
            .collect();
        self.update(response).await?;
        Ok(missing_tables)
    }

    async fn fetch_tables_metadata(
        &self,
        table_paths: &HashSet<&TablePath>,
        physical_table_paths: &HashSet<&Arc<PhysicalTablePath>>,
        partition_ids: Vec<PartitionId>,
    ) -> Result<Option<MetadataResponse>> {
        let server = match self.server_for_refresh() {
            Some(s) => s,
            None => {
                info!(
                    "No available tablet server to update metadata, attempting to re-initialize cluster using bootstrap server."
                );
                self.reinit_cluster().await?;
                return Ok(None);
            }
        };

        let result = async {
            let conn = self.connections.get_connection(&server).await?;
            conn.request(UpdateMetadataRequest::new(
                table_paths,
                physical_table_paths,
                partition_ids,
            ))
            .await
            .map(Some)
        }
        .await;
        if let Err(err) = &result {
            // A schema that is not visible yet does not make the server unavailable for other
            // tables, or for the individual requests used to isolate a failed combined refresh.
            if err.is_retriable() && err.api_error() != Some(FlussError::SchemaNotExist) {
                self.unavailable_tablet_servers
                    .write()
                    .insert(server.clone());
                warn!("Tablet server {server:?} is unavailable for updating metadata: {err}");
            }
        }
        result
    }

    pub async fn update_table_metadata(&self, table_path: &TablePath) -> Result<()> {
        self.update_tables_metadata(&HashSet::from([table_path]), &HashSet::new(), vec![])
            .await
    }

    pub async fn update_physical_table_metadata(
        &self,
        physical_table_paths: &[Arc<PhysicalTablePath>],
    ) -> Result<()> {
        let mut update_table_paths = HashSet::new();
        let mut update_partition_paths = HashSet::new();
        for physical_table_path in physical_table_paths {
            match physical_table_path.get_partition_name() {
                Some(_) => {
                    update_partition_paths.insert(physical_table_path);
                }
                None => {
                    update_table_paths.insert(physical_table_path.get_table_path());
                }
            }
        }

        self.update_tables_metadata(&update_table_paths, &update_partition_paths, vec![])
            .await
    }

    pub async fn check_and_update_table_metadata(&self, table_paths: &[TablePath]) -> Result<()> {
        let cluster_binding = self.cluster.load_full();
        let need_update_table_paths: HashSet<&TablePath> = table_paths
            .iter()
            .filter(|table_path| cluster_binding.opt_get_table(table_path).is_none())
            .collect();

        if !need_update_table_paths.is_empty() {
            self.update_tables_metadata(&need_update_table_paths, &HashSet::new(), vec![])
                .await?;
        }
        Ok(())
    }

    /// Checks that the partition ids exist, refreshing metadata for uncached ids.
    pub async fn check_and_update_partition_metadata_by_ids(
        &self,
        table_path: &TablePath,
        partition_ids: &[PartitionId],
    ) -> Result<()> {
        let cluster_binding = self.cluster.load_full();
        let need_update_partition_ids: Vec<PartitionId> = partition_ids
            .iter()
            .filter(|partition_id| cluster_binding.get_partition_name(**partition_id).is_none())
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if !need_update_partition_ids.is_empty() {
            self.update_tables_metadata(
                &HashSet::from([table_path]),
                &HashSet::new(),
                need_update_partition_ids,
            )
            .await?;
        }
        Ok(())
    }

    /// Resolves the partition id, refreshing metadata once if not cached.
    /// Returns `None` when the partition does not exist — `PartitionNotExists`
    /// server errors are swallowed so callers can short-circuit to an empty result.
    pub async fn check_and_update_partition_metadata(
        &self,
        physical_table_path: &PhysicalTablePath,
    ) -> Result<Option<PartitionId>> {
        if let Some(id) = self.get_cluster().get_partition_id(physical_table_path) {
            return Ok(Some(id));
        }
        let path = Arc::new(physical_table_path.clone());
        match self.update_physical_table_metadata(&[path]).await {
            Ok(()) => {}
            Err(e) if matches!(e.api_error(), Some(FlussError::PartitionNotExists)) => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        }
        Ok(self.get_cluster().get_partition_id(physical_table_path))
    }

    pub(crate) async fn get_connection(
        &self,
        server_node: &ServerNode,
    ) -> Result<ServerConnection> {
        let result = self.connections.get_connection(server_node).await?;
        Ok(result)
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        self.cluster.load_full()
    }

    /// For short reads on hot paths, unlike `get_cluster` does not clone the `Arc`.
    /// Do not hold it for long, e.g. across `.await`, use `get_cluster` there.
    pub(crate) fn load_cluster(&self) -> Guard<Arc<Cluster>> {
        self.cluster.load()
    }

    const MAX_RETRY_TIMES: u8 = 3;

    pub async fn leader_for(
        &self,
        table_path: &TablePath,
        table_bucket: &TableBucket,
    ) -> Result<Option<ServerNode>> {
        let leader = self.get_leader_for(table_bucket);

        if leader.is_some() {
            Ok(leader)
        } else {
            for _ in 0..Self::MAX_RETRY_TIMES {
                if let Some(partition_id) = table_bucket.partition_id() {
                    self.update_tables_metadata(
                        &HashSet::from([table_path]),
                        &HashSet::new(),
                        vec![partition_id],
                    )
                    .await?;
                } else {
                    self.update_tables_metadata(
                        &HashSet::from([table_path]),
                        &HashSet::new(),
                        vec![],
                    )
                    .await?;
                }

                let cluster = self.cluster.load();
                let leader = cluster.leader_for(table_bucket);

                if leader.is_some() {
                    return Ok(leader.cloned());
                }
            }

            Ok(None)
        }
    }

    fn get_leader_for(&self, table_bucket: &TableBucket) -> Option<ServerNode> {
        self.cluster.load().leader_for(table_bucket).cloned()
    }
}

#[cfg(test)]
impl Metadata {
    pub(crate) fn new_for_test(cluster: Arc<Cluster>) -> Self {
        let (cluster_version_tx, _) = watch::channel(0);
        Metadata {
            cluster: ArcSwap::new(cluster),
            changing: Mutex::new(()),
            unavailable_tablet_servers: RwLock::new(HashSet::new()),
            connections: Arc::new(RpcClient::new()),
            bootstrap: Arc::from(""),
            cluster_version_tx,
        }
    }

    pub(crate) fn with_connections(mut self, connections: RpcClient) -> Self {
        self.connections = Arc::new(connections);
        self
    }

    pub(crate) fn with_bootstrap(mut self, bootstrap: &str) -> Self {
        self.bootstrap = bootstrap.into();
        self
    }

    pub(crate) fn with_sasl(mut self, username: &str, password: &str) -> Self {
        self.connections =
            Arc::new(RpcClient::new().with_sasl(username.to_string(), password.to_string()));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ApiError;
    use crate::metadata::{TableBucket, TablePath};
    use crate::proto::{ApiVersionsResponse, ErrorResponse, PbApiVersion, PbServerNode};
    use crate::rpc::ApiKey;
    use crate::test_utils::{
        build_cluster_arc, pb_bucket, pb_metadata_response, pb_table_metadata,
    };
    use prost::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    fn server_metadata(ids: &[i32], port: u16) -> MetadataResponse {
        MetadataResponse {
            tablet_servers: ids
                .iter()
                .map(|id| PbServerNode {
                    node_id: *id,
                    host: "127.0.0.1".to_string(),
                    port: i32::from(port),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    // Serve the API-version handshake followed by one metadata response.
    async fn serve_metadata(
        listener: TcpListener,
        response: std::result::Result<MetadataResponse, FlussError>,
    ) {
        serve_metadata_as(listener, ServerType::TabletServer, response).await;
    }

    async fn serve_metadata_as(
        listener: TcpListener,
        server_type: ServerType,
        response: std::result::Result<MetadataResponse, FlussError>,
    ) -> TcpStream {
        let mut stream = accept_handshake(listener, server_type).await;
        let mut reply = read_request(&mut stream, ApiKey::MetaData).await;
        match &response {
            Ok(metadata) => metadata.encode(&mut reply).unwrap(),
            Err(error) => {
                reply[0] = 1;
                ErrorResponse {
                    error_code: error.code(),
                    error_message: Some("metadata request failed".to_string()),
                }
                .encode(&mut reply)
                .unwrap();
            }
        }
        write_reply(&mut stream, &reply).await;
        stream
    }

    // Accept a connection and serve the API-version handshake.
    async fn accept_handshake(listener: TcpListener, server_type: ServerType) -> TcpStream {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut reply = read_request(&mut stream, ApiKey::ApiVersion).await;
        ApiVersionsResponse {
            api_versions: vec![PbApiVersion {
                api_key: i32::from(i16::from(ApiKey::MetaData)),
                min_version: 0,
                max_version: 0,
            }],
            server_type: Some(server_type.to_type_id()),
        }
        .encode(&mut reply)
        .unwrap();
        write_reply(&mut stream, &reply).await;
        stream
    }

    // Read a request of `api_key` and return the header of its successful reply.
    async fn read_request(stream: &mut TcpStream, api_key: ApiKey) -> Vec<u8> {
        let len = stream.read_u32().await.unwrap() as usize;
        assert!((8..=1024 * 1024).contains(&len));
        let mut request = vec![0; len];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(
            i16::from_be_bytes([request[0], request[1]]),
            i16::from(api_key)
        );
        let mut reply = vec![0];
        reply.extend_from_slice(&request[4..8]);
        reply
    }

    async fn write_reply(stream: &mut TcpStream, reply: &[u8]) {
        stream.write_u32(reply.len() as u32).await.unwrap();
        stream.write_all(reply).await.unwrap();
    }

    fn unavailable_ids(metadata: &Metadata) -> HashSet<i32> {
        metadata
            .unavailable_tablet_servers
            .read()
            .iter()
            .map(ServerNode::id)
            .collect()
    }

    async fn request_coordinator(metadata: &Metadata, listener: TcpListener) -> TcpStream {
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_metadata_as(
            listener,
            ServerType::CoordinatorServer,
            Ok(MetadataResponse::default()),
        ));
        tokio::time::timeout(Duration::from_secs(10), async {
            let coordinator = metadata.get_cluster().get_coordinator_server().cloned();
            let connection = metadata
                .get_connection(&coordinator.unwrap())
                .await
                .unwrap();
            connection
                .request(UpdateMetadataRequest::new(
                    &HashSet::new(),
                    &HashSet::new(),
                    vec![],
                ))
                .await
                .unwrap();
            server.await.unwrap()
        })
        .await
        .unwrap_or_else(|_| panic!("the coordinator request did not reach {address}"))
    }

    #[tokio::test]
    async fn metadata_refresh_recovers_when_all_cached_servers_are_unavailable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Port zero cannot accept connections, so all cached addresses are stale.
        let stale_cluster =
            Cluster::from_metadata_response(server_metadata(&[1, 2, 3], 0), None).unwrap();
        let mut metadata = Metadata::new_for_test(Arc::new(stale_cluster));
        metadata.bootstrap = address.to_string().into();
        let mut changes = metadata.subscribe_cluster_changes();

        for failed_count in 1..=3 {
            let error = metadata
                .update_tables_metadata(&HashSet::new(), &HashSet::new(), vec![])
                .await
                .unwrap_err();
            assert!(error.is_retriable());
            assert_eq!(
                metadata.unavailable_tablet_servers.read().len(),
                failed_count
            );
            assert!(!changes.has_changed().unwrap());
        }

        let server = tokio::spawn(serve_metadata(
            listener,
            Ok(server_metadata(&[1, 2, 3], address.port())),
        ));
        metadata
            .update_tables_metadata(&HashSet::new(), &HashSet::new(), vec![])
            .await
            .unwrap();
        server.await.unwrap();
        assert!(metadata.unavailable_tablet_servers.read().is_empty());
        for id in 1..=3 {
            assert_eq!(
                metadata.get_cluster().get_tablet_server(id).unwrap().port(),
                u32::from(address.port())
            );
        }
        assert!(changes.has_changed().unwrap());
        assert_eq!(*changes.borrow_and_update(), 1);
    }

    #[tokio::test]
    async fn metadata_refresh_only_excludes_servers_on_retriable_request_errors() {
        for error in [FlussError::RequestTimeOut, FlussError::PartitionNotExists] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let cluster =
                Cluster::from_metadata_response(server_metadata(&[1], port), None).unwrap();
            let metadata = Metadata::new_for_test(Arc::new(cluster));
            let server = tokio::spawn(serve_metadata(listener, Err(error)));
            let result = metadata
                .update_tables_metadata(&HashSet::new(), &HashSet::new(), vec![])
                .await
                .unwrap_err();
            server.await.unwrap();
            assert_eq!(result.api_error(), Some(error));
            assert_eq!(
                unavailable_ids(&metadata).contains(&1),
                result.is_retriable()
            );
        }
    }

    #[tokio::test]
    async fn metadata_refresh_uses_remaining_server_and_clears_recovered_ids() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut response = server_metadata(&[1, 2], 0);
        response.tablet_servers[1].port = i32::from(port);
        let cluster = Cluster::from_metadata_response(response, None).unwrap();
        let failed = cluster.get_tablet_server(1).unwrap().clone();
        let metadata = Metadata::new_for_test(Arc::new(cluster));
        let gone = ServerNode::new(3, "127.0.0.1".to_string(), 0, ServerType::TabletServer);
        metadata
            .unavailable_tablet_servers
            .write()
            .extend([failed, gone]);
        let server = tokio::spawn(serve_metadata(listener, Ok(server_metadata(&[1, 2], port))));

        metadata
            .update_tables_metadata(&HashSet::new(), &HashSet::new(), vec![])
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(unavailable_ids(&metadata), HashSet::from([3]));
        assert_eq!(
            metadata.get_cluster().get_tablet_server(1).unwrap().port(),
            u32::from(port)
        );
    }

    #[tokio::test]
    async fn metadata_refresh_drops_connections_to_moved_servers() {
        let server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let old_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let new_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_port = server_listener.local_addr().unwrap().port();
        let coordinator_at = |listener: &TcpListener| PbServerNode {
            node_id: 0,
            host: "127.0.0.1".to_string(),
            port: i32::from(listener.local_addr().unwrap().port()),
            ..Default::default()
        };
        let mut response = server_metadata(&[1], server_port);
        response.coordinator_server = Some(coordinator_at(&old_listener));
        let cluster = Cluster::from_metadata_response(response, None).unwrap();
        let metadata = Metadata::new_for_test(Arc::new(cluster));
        let _old_stream = request_coordinator(&metadata, old_listener).await;

        let mut response = server_metadata(&[1, 2], server_port);
        response.tablet_servers[0].port = 0;
        response.coordinator_server = Some(coordinator_at(&new_listener));
        let server = tokio::spawn(serve_metadata(server_listener, Ok(response)));
        metadata
            .update_tables_metadata(&HashSet::new(), &HashSet::new(), vec![])
            .await
            .unwrap();
        server.await.unwrap();
        assert!(metadata.connections.cached_endpoints().is_empty());
        request_coordinator(&metadata, new_listener).await;
    }

    #[tokio::test]
    async fn failed_bootstrap_preserves_unavailable_servers_and_cached_metadata() {
        let cluster =
            Arc::new(Cluster::from_metadata_response(server_metadata(&[1], 0), None).unwrap());
        let metadata = Metadata::new_for_test(cluster.clone());
        metadata
            .unavailable_tablet_servers
            .write()
            .insert(cluster.get_tablet_server(1).unwrap().clone());
        let changes = metadata.subscribe_cluster_changes();
        let error = metadata
            .update_tables_metadata(&HashSet::new(), &HashSet::new(), vec![])
            .await
            .unwrap_err();
        assert!(matches!(error, Error::IllegalArgument { .. }));
        assert!(Arc::ptr_eq(&metadata.get_cluster(), &cluster));
        assert_eq!(unavailable_ids(&metadata), HashSet::from([1]));
        assert!(!changes.has_changed().unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn bootstrap_retry_succeeds_after_retriable_failures() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let disconnects = Arc::new(AtomicUsize::new(0));
        let started_at = tokio::time::Instant::now();

        let result = Metadata::retry_bootstrap(
            "localhost:9123",
            {
                let attempts = attempts.clone();
                move || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt < 2 {
                            Err(crate::rpc::RpcError::ConnectionError(
                                "bootstrap is recovering".to_string(),
                            )
                            .into())
                        } else {
                            Ok(Cluster::default())
                        }
                    }
                }
            },
            {
                let disconnects = disconnects.clone();
                move || {
                    disconnects.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(disconnects.load(Ordering::SeqCst), 2);
        assert_eq!(
            tokio::time::Instant::now().duration_since(started_at),
            std::time::Duration::from_millis(300)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bootstrap_retry_returns_last_error_after_exhaustion() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let disconnects = Arc::new(AtomicUsize::new(0));
        let started_at = tokio::time::Instant::now();

        let result = Metadata::retry_bootstrap(
            "localhost:9123",
            {
                let attempts = attempts.clone();
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async {
                        Err::<Cluster, Error>(
                            FlussError::RequestTimeOut
                                .to_api_error(Some("bootstrap is recovering".to_string()))
                                .into(),
                        )
                    }
                }
            },
            {
                let disconnects = disconnects.clone();
                move || {
                    disconnects.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await;

        let error = match result {
            Ok(_) => panic!("retries should be exhausted"),
            Err(error) => error,
        };
        assert_eq!(error.api_error(), Some(FlussError::RequestTimeOut));
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        assert_eq!(disconnects.load(Ordering::SeqCst), 3);
        assert_eq!(
            tokio::time::Instant::now().duration_since(started_at),
            std::time::Duration::from_millis(700)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bootstrap_retry_does_not_retry_non_retriable_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let disconnects = Arc::new(AtomicUsize::new(0));

        let result = Metadata::retry_bootstrap(
            "localhost:9123",
            {
                let attempts = attempts.clone();
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async {
                        Err::<Cluster, Error>(Error::IllegalArgument {
                            message: "invalid bootstrap response".to_string(),
                        })
                    }
                }
            },
            {
                let disconnects = disconnects.clone();
                move || {
                    disconnects.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await;

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(disconnects.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn leader_for_returns_server() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Metadata::new_for_test(cluster);
        let leader = metadata
            .leader_for(&table_path, &TableBucket::new(1, 0))
            .await
            .unwrap()
            .expect("leader");
        assert_eq!(leader.id(), 1);
    }

    #[test]
    fn invalidate_server_removes_leader() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Metadata::new_for_test(cluster);
        metadata.invalidate_server(&1, vec![1]);
        let cluster = metadata.get_cluster();
        assert!(cluster.get_tablet_server(1).is_none());
    }

    #[test]
    fn evict_table_metadata_purges_cluster() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let cluster = build_cluster_arc(&table_path, 1, 1);
        let metadata = Metadata::new_for_test(cluster);
        metadata.evict_table_metadata(&table_path);
        let cluster = metadata.get_cluster();
        assert!(cluster.get_table_id(&table_path).is_none());
        assert!(cluster.opt_get_table(&table_path).is_none());
        assert!(cluster.leader_for(&TableBucket::new(1, 0)).is_none());
    }

    fn moved_to_server_2(table: &str, table_id: TableId) -> MetadataResponse {
        let mut bucket = pb_bucket(0);
        bucket.leader_id = Some(2);
        bucket.replica_id = vec![2];
        bucket.isr = vec![2];
        let table = pb_table_metadata(table, table_id, false, vec![bucket]);
        let mut response = pb_metadata_response(vec![table], vec![]);
        response.tablet_servers.push(PbServerNode {
            node_id: 2,
            host: "ts2".to_string(),
            ..response.tablet_servers[0].clone()
        });
        response
    }

    fn leader(cluster: &Cluster, table_id: TableId) -> Option<i32> {
        cluster
            .leader_for(&TableBucket::new(table_id, 0))
            .map(ServerNode::id)
    }

    #[test]
    fn concurrent_changes_are_not_lost() {
        let tables = ["a", "b", "c"]
            .into_iter()
            .zip(1..)
            .map(|(name, id)| pb_table_metadata(name, id, false, vec![pb_bucket(0)]))
            .collect();
        let cluster = Cluster::from_metadata_response(pb_metadata_response(tables, vec![]), None);
        let metadata = Metadata::new_for_test(Arc::new(cluster.unwrap()));
        let metadata = &metadata;
        let table_c = PhysicalTablePath::of(Arc::new(TablePath::new("db", "c")));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        std::thread::scope(|s| {
            // An invalidation that holds the lock until released.
            let invalidation = s.spawn(move || {
                metadata.invalidate(|cluster| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Some(cluster.invalidate_physical_table_meta(&HashSet::from([table_c])))
                })
            });
            entered_rx.recv().unwrap();
            let updates = [("a", 1), ("b", 2)].map(|(table, id)| {
                s.spawn(move || {
                    futures::executor::block_on(metadata.update(moved_to_server_2(table, id)))
                })
            });
            // Time for a change that reads outside the lock to read the old cluster.
            std::thread::sleep(Duration::from_millis(50));
            // Readers do not wait for a running change.
            assert_eq!(leader(&metadata.load_cluster(), 3), Some(1));
            release_tx.send(()).unwrap();
            assert!(invalidation.join().unwrap());
            for update in updates {
                update.join().unwrap().unwrap();
            }
        });
        let cluster = metadata.get_cluster();
        assert_eq!(leader(&cluster, 1), Some(2));
        assert_eq!(leader(&cluster, 2), Some(2));
        assert_eq!(leader(&cluster, 3), None);
    }

    #[tokio::test]
    async fn a_failure_at_an_old_address_does_not_exclude_the_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let old_port = listener.local_addr().unwrap().port();
        let cluster = Cluster::from_metadata_response(server_metadata(&[1], old_port), None);
        let metadata = Arc::new(Metadata::new_for_test(Arc::new(cluster.unwrap())));
        let (received_tx, received_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        // The old address holds the refresh until released, then drops it.
        let server = tokio::spawn(async move {
            let mut stream = accept_handshake(listener, ServerType::TabletServer).await;
            read_request(&mut stream, ApiKey::MetaData).await;
            received_tx.send(()).unwrap();
            release_rx.await.unwrap();
        });
        let refresh = tokio::spawn({
            let metadata = Arc::clone(&metadata);
            async move {
                metadata
                    .update_tables_metadata(&HashSet::new(), &HashSet::new(), vec![])
                    .await
            }
        });
        received_rx.await.unwrap();
        let new_port = 9123;
        metadata
            .update(server_metadata(&[1], new_port))
            .await
            .unwrap();
        release_tx.send(()).unwrap();
        server.await.unwrap();
        assert!(refresh.await.unwrap().unwrap_err().is_retriable());
        assert_eq!(
            metadata.server_for_refresh().map(|server| server.port()),
            Some(u32::from(new_port))
        );
    }

    #[test]
    fn invalidate_partition_if_unchanged_skips_a_newer_table() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let metadata = Metadata::new_for_test(build_cluster_arc(&table_path, 2, 1));
        let partition_path =
            PhysicalTablePath::of_partitioned(Arc::new(table_path), Some("p1".to_string()));

        assert!(!metadata.invalidate_partition_if_unchanged(&partition_path, Some(1), None));
        assert!(metadata.invalidate_partition_if_unchanged(&partition_path, Some(2), None));
    }

    #[test]
    fn bootstrap_failure_preserves_last_connection_error() {
        let authentication_error = Error::FlussAPIError {
            api_error: ApiError {
                code: FlussError::AuthenticateException.code(),
                message: "Authentication failed".to_string(),
            },
        };

        let error = Metadata::bootstrap_initialization_error(
            "127.0.0.1:9123",
            vec!["127.0.0.1:9123: Authentication failed".to_string()],
            Some(authentication_error),
        );

        assert_eq!(error.api_error(), Some(FlussError::AuthenticateException));
    }

    #[test]
    fn parse_bootstrap_variants() {
        // valid IP
        let bootstraps = Metadata::parse_bootstrap_servers("127.0.0.1:8080").unwrap();
        let addr = bootstraps[0].address.as_ref().unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_eq!(addr.port(), 8080);

        // valid hostname
        let bootstraps = Metadata::parse_bootstrap_servers("localhost:9090").unwrap();
        let addr = bootstraps[0].address.as_ref().unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_eq!(addr.port(), 9090);

        // valid IPv6 address
        let bootstraps = Metadata::parse_bootstrap_servers("[::1]:8080").unwrap();
        let addr = bootstraps[0].address.as_ref().unwrap();
        assert_eq!(addr.ip().to_string(), "::1");
        assert_eq!(addr.port(), 8080);

        // invalid input: missing port is preserved as an entry-level parse error
        let bootstraps = Metadata::parse_bootstrap_servers("localhost").unwrap();
        assert!(bootstraps[0].address.is_err());

        // invalid input: out-of-range port is preserved as an entry-level parse error
        let bootstraps = Metadata::parse_bootstrap_servers("localhost:99999").unwrap();
        assert!(bootstraps[0].address.is_err());

        // invalid input: empty string
        assert!(Metadata::parse_bootstrap_servers("").is_err());

        // invalid input: nonsensical address is preserved as an entry-level parse error
        let bootstraps = Metadata::parse_bootstrap_servers("invalid_address").unwrap();
        assert!(bootstraps[0].address.is_err());
    }

    #[test]
    fn parse_bootstrap_accepts_comma_separated_servers() {
        let bootstraps =
            Metadata::parse_bootstrap_servers("127.0.0.1:8080, localhost:9090, [::1]:7070")
                .unwrap();

        assert_eq!(bootstraps.len(), 3);
        let first = bootstraps[0].address.as_ref().unwrap();
        assert_eq!(first.ip().to_string(), "127.0.0.1");
        assert_eq!(first.port(), 8080);
        let second = bootstraps[1].address.as_ref().unwrap();
        assert_eq!(second.ip().to_string(), "127.0.0.1");
        assert_eq!(second.port(), 9090);
        let third = bootstraps[2].address.as_ref().unwrap();
        assert_eq!(third.ip().to_string(), "::1");
        assert_eq!(third.port(), 7070);
    }

    #[test]
    fn parse_bootstrap_preserves_later_entries_when_one_entry_is_unresolvable() {
        let bootstraps =
            Metadata::parse_bootstrap_servers("invalid.invalid:8080, 127.0.0.1:9090").unwrap();

        assert_eq!(bootstraps.len(), 2);
        assert!(bootstraps[0].address.is_err());
        let addr = bootstraps[1].address.as_ref().unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_eq!(addr.port(), 9090);
    }

    #[test]
    fn parse_bootstrap_ignores_empty_comma_separated_entries() {
        let bootstraps =
            Metadata::parse_bootstrap_servers("127.0.0.1:8080, , localhost:9090,").unwrap();

        assert_eq!(bootstraps.len(), 2);
        assert_eq!(bootstraps[0].raw, "127.0.0.1:8080");
        assert_eq!(bootstraps[1].raw, "localhost:9090");
    }

    #[test]
    fn parse_bootstrap_rejects_config_without_servers() {
        assert!(Metadata::parse_bootstrap_servers(" , ").is_err());
    }
}
