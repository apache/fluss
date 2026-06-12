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
use log::info;
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

    fn parse_bootstrap(boot_strap: &str) -> Result<SocketAddr> {
        // Resolve all socket addresses and deterministically choose one.
        let addrs = boot_strap
            .to_socket_addrs()
            .map_err(|e| Error::IllegalArgument {
                message: format!("Invalid bootstrap address '{boot_strap}': {e}"),
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
            message: format!("Unable to resolve bootstrap address '{boot_strap}'"),
        })?;
        Ok(addr)
    }

    async fn init_cluster(boot_strap: &str, connections: Arc<RpcClient>) -> Result<Cluster> {
        let socket_address = Self::parse_bootstrap(boot_strap)?;
        let server_node = ServerNode::new(
            -1,
            socket_address.ip().to_string(),
            socket_address.port() as u32,
            ServerType::CoordinatorServer,
        );
        let con = connections.get_connection(&server_node).await?;

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

    pub async fn get_connection(&self, server_node: &ServerNode) -> Result<ServerConnection> {
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
    fn parse_bootstrap_variants() {
        // valid IP
        let addr = Metadata::parse_bootstrap("127.0.0.1:8080").unwrap();
        assert_eq!(addr.port(), 8080);

        // valid hostname
        let addr = Metadata::parse_bootstrap("localhost:9090").unwrap();
        assert_eq!(addr.port(), 9090);

        // valid IPv6 address
        let addr = Metadata::parse_bootstrap("[::1]:8080").unwrap();
        assert_eq!(addr.port(), 8080);

        // invalid input: missing port
        assert!(Metadata::parse_bootstrap("localhost").is_err());

        // invalid input: out-of-range port
        assert!(Metadata::parse_bootstrap("localhost:99999").is_err());

        // invalid input: empty string
        assert!(Metadata::parse_bootstrap("").is_err());

        // invalid input: nonsensical address
        assert!(Metadata::parse_bootstrap("invalid_address").is_err());
    }
}
