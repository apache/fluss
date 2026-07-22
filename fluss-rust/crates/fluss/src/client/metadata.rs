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

use crate::PartitionId;
use crate::cluster::{Cluster, ServerNode, ServerType};
use crate::error::{Error, FlussError, Result};
use crate::metadata::{PhysicalTablePath, TableBucket, TablePath};
use crate::proto::MetadataResponse;
use crate::rpc::message::UpdateMetadataRequest;
use crate::rpc::{RpcClient, ServerConnection};
use log::{info, warn};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use tokio::sync::watch;

pub struct Metadata {
    cluster: RwLock<Arc<Cluster>>,
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
            cluster: RwLock::new(Arc::new(cluster)),
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

            match Self::fetch_cluster_from_bootstrap(&server_node, connections.clone()).await {
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
        *self.cluster.write() = cluster.into();
        self.notify_cluster_changed();
        Ok(())
    }

    pub fn invalidate_server(&self, server_id: &i32, table_ids: Vec<i64>) {
        {
            let mut cluster_guard = self.cluster.write();
            let updated_cluster = cluster_guard.invalidate_server(server_id, table_ids);
            *cluster_guard = Arc::new(updated_cluster);
        }
        self.notify_cluster_changed();
    }

    pub fn invalidate_physical_table_meta(
        &self,
        physical_tables_to_invalid: &HashSet<PhysicalTablePath>,
    ) {
        {
            let mut cluster_guard = self.cluster.write();
            let updated_cluster =
                cluster_guard.invalidate_physical_table_meta(physical_tables_to_invalid);
            *cluster_guard = Arc::new(updated_cluster);
        }
        self.notify_cluster_changed();
    }

    pub async fn update(&self, metadata_response: MetadataResponse) -> Result<()> {
        let origin_cluster = self.cluster.read().clone();
        let new_cluster =
            Cluster::from_metadata_response(metadata_response, Some(&origin_cluster))?;
        {
            let mut cluster = self.cluster.write();
            *cluster = Arc::new(new_cluster);
        }
        self.notify_cluster_changed();
        Ok(())
    }

    pub async fn update_tables_metadata(
        &self,
        table_paths: &HashSet<&TablePath>,
        physical_table_paths: &HashSet<&Arc<PhysicalTablePath>>,
        partition_ids: Vec<i64>,
    ) -> Result<()> {
        let maybe_server = {
            let guard = self.cluster.read();
            guard.get_one_available_server().cloned()
        };

        let server = match maybe_server {
            Some(s) => s,
            None => {
                info!(
                    "No available tablet server to update metadata, attempting to re-initialize cluster using bootstrap server."
                );
                self.reinit_cluster().await?;
                return Ok(());
            }
        };

        let conn = self.connections.get_connection(&server).await?;

        let response = conn
            .request(UpdateMetadataRequest::new(
                table_paths,
                physical_table_paths,
                partition_ids,
            ))
            .await?;
        self.update(response).await?;
        Ok(())
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
        let cluster_binding = self.cluster.read().clone();
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
        let guard = self.cluster.read();
        guard.clone()
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

                let cluster = self.cluster.read();
                let leader = cluster.leader_for(table_bucket);

                if leader.is_some() {
                    return Ok(leader.cloned());
                }
            }

            Ok(None)
        }
    }

    fn get_leader_for(&self, table_bucket: &TableBucket) -> Option<ServerNode> {
        let cluster = self.cluster.read();
        cluster.leader_for(table_bucket).cloned()
    }
}

#[cfg(test)]
impl Metadata {
    pub(crate) fn new_for_test(cluster: Arc<Cluster>) -> Self {
        let (cluster_version_tx, _) = watch::channel(0);
        Metadata {
            cluster: RwLock::new(cluster),
            connections: Arc::new(RpcClient::new()),
            bootstrap: Arc::from(""),
            cluster_version_tx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ApiError;
    use crate::metadata::{TableBucket, TablePath};
    use crate::test_utils::build_cluster_arc;

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
            Metadata::parse_bootstrap_servers("invalid_address:8080, 127.0.0.1:9090").unwrap();

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
