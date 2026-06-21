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

use crate::cluster::{BucketLocation, ServerNode, ServerType};
use crate::error::{Error, Result};
use crate::metadata::{
    JsonSerde, PhysicalTablePath, TableBucket, TableDescriptor, TableInfo, TablePath,
};
use crate::proto::{MetadataResponse, PbBucketMetadata};
use crate::rpc::{from_pb_server_node, from_pb_table_path};
use crate::{BucketId, PartitionId, TableId};
use rand::random_range;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

static EMPTY: Vec<BucketLocation> = Vec::new();

#[derive(Default)]
pub struct Cluster {
    coordinator_server: Option<ServerNode>,
    alive_tablet_servers_by_id: HashMap<i32, ServerNode>,
    alive_tablet_servers: Vec<ServerNode>,
    available_locations_by_path: HashMap<Arc<PhysicalTablePath>, Vec<BucketLocation>>,
    available_locations_by_bucket: HashMap<TableBucket, BucketLocation>,
    table_id_by_path: HashMap<TablePath, TableId>,
    table_path_by_id: HashMap<TableId, TablePath>,
    table_info_by_path: HashMap<TablePath, TableInfo>,
    partitions_id_by_path: HashMap<Arc<PhysicalTablePath>, PartitionId>,
    partition_name_by_id: HashMap<PartitionId, String>,
}

impl Cluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        coordinator_server: Option<ServerNode>,
        alive_tablet_servers_by_id: HashMap<i32, ServerNode>,
        available_locations_by_path: HashMap<Arc<PhysicalTablePath>, Vec<BucketLocation>>,
        available_locations_by_bucket: HashMap<TableBucket, BucketLocation>,
        table_id_by_path: HashMap<TablePath, TableId>,
        table_info_by_path: HashMap<TablePath, TableInfo>,
        partitions_id_by_path: HashMap<Arc<PhysicalTablePath>, PartitionId>,
    ) -> Self {
        let alive_tablet_servers = alive_tablet_servers_by_id.values().cloned().collect();
        let table_path_by_id = table_id_by_path
            .iter()
            .map(|(path, table_id)| (*table_id, path.clone()))
            .collect();
        let partition_name_by_id = partitions_id_by_path
            .iter()
            .filter_map(|(path, id)| path.get_partition_name().map(|name| (*id, name.clone())))
            .collect();
        Cluster {
            coordinator_server,
            alive_tablet_servers_by_id,
            alive_tablet_servers,
            available_locations_by_path,
            available_locations_by_bucket,
            table_id_by_path,
            table_path_by_id,
            table_info_by_path,
            partitions_id_by_path,
            partition_name_by_id,
        }
    }

    pub fn invalidate_server(&self, server_id: &i32, table_ids: Vec<TableId>) -> Self {
        let alive_tablet_servers_by_id = self
            .alive_tablet_servers_by_id
            .iter()
            .filter(|&(id, _)| id != server_id)
            .map(|(id, ts)| (*id, ts.clone()))
            .collect();

        let table_paths: HashSet<&TablePath> = table_ids
            .iter()
            .filter_map(|id| self.table_path_by_id.get(id))
            .collect();

        let (available_locations_by_path, available_locations_by_bucket) =
            self.filter_bucket_locations_by_path(&table_paths);

        Cluster::new(
            self.coordinator_server.clone(),
            alive_tablet_servers_by_id,
            available_locations_by_path,
            available_locations_by_bucket,
            self.table_id_by_path.clone(),
            self.table_info_by_path.clone(),
            self.partitions_id_by_path.clone(),
        )
    }

    pub fn invalidate_physical_table_meta(
        &self,
        physical_tables_to_invalid: &HashSet<PhysicalTablePath>,
    ) -> Self {
        let table_paths: HashSet<&TablePath> = physical_tables_to_invalid
            .iter()
            .map(|path| path.get_table_path())
            .collect();
        let (available_locations_by_path, available_locations_by_bucket) =
            self.filter_bucket_locations_by_path(&table_paths);

        Cluster::new(
            self.coordinator_server.clone(),
            self.alive_tablet_servers_by_id.clone(),
            available_locations_by_path,
            available_locations_by_bucket,
            self.table_id_by_path.clone(),
            self.table_info_by_path.clone(),
            self.partitions_id_by_path.clone(),
        )
    }

    pub fn update(&mut self, cluster: Cluster) {
        let Cluster {
            coordinator_server,
            alive_tablet_servers_by_id,
            alive_tablet_servers,
            available_locations_by_path,
            available_locations_by_bucket,
            table_id_by_path,
            table_path_by_id,
            table_info_by_path,
            partitions_id_by_path,
            partition_name_by_id,
        } = cluster;
        self.coordinator_server = coordinator_server;
        self.alive_tablet_servers_by_id = alive_tablet_servers_by_id;
        self.alive_tablet_servers = alive_tablet_servers;
        self.available_locations_by_path = available_locations_by_path;
        self.available_locations_by_bucket = available_locations_by_bucket;
        self.table_id_by_path = table_id_by_path;
        self.table_path_by_id = table_path_by_id;
        self.table_info_by_path = table_info_by_path;
        self.partitions_id_by_path = partitions_id_by_path;
        self.partition_name_by_id = partition_name_by_id;
    }

    fn filter_bucket_locations_by_path(
        &self,
        table_paths: &HashSet<&TablePath>,
    ) -> (
        HashMap<Arc<PhysicalTablePath>, Vec<BucketLocation>>,
        HashMap<TableBucket, BucketLocation>,
    ) {
        let available_locations_by_path = self
            .available_locations_by_path
            .iter()
            .filter(|&(path, _)| !table_paths.contains(path.get_table_path()))
            .map(|(path, locations)| (path.clone(), locations.clone()))
            .collect();

        let available_locations_by_bucket = self
            .available_locations_by_bucket
            .iter()
            .filter(|&(_bucket, location)| {
                !table_paths.contains(&location.physical_table_path.get_table_path())
            })
            .map(|(bucket, location)| (bucket.clone(), location.clone()))
            .collect();

        (available_locations_by_path, available_locations_by_bucket)
    }

    pub fn from_metadata_response(
        metadata_response: MetadataResponse,
        origin_cluster: Option<&Cluster>,
    ) -> Result<Cluster> {
        let mut servers = HashMap::with_capacity(metadata_response.tablet_servers.len());
        for pb_server in metadata_response.tablet_servers {
            let server_id = pb_server.node_id;
            let server_node = from_pb_server_node(pb_server, ServerType::TabletServer);
            servers.insert(server_id, server_node);
        }

        let coordinator_server = metadata_response
            .coordinator_server
            .map(|node| from_pb_server_node(node, ServerType::CoordinatorServer));

        let mut table_id_by_path = HashMap::new();
        let mut table_info_by_path = HashMap::new();
        let mut partitions_id_by_path = HashMap::new();
        let mut tmp_available_locations_by_path = HashMap::new();
        let mut tmp_available_location_by_bucket = HashMap::new();

        if let Some(origin) = origin_cluster {
            table_info_by_path.extend(origin.get_table_info_by_path().clone());
            table_id_by_path.extend(origin.get_table_id_by_path().clone());
            partitions_id_by_path.extend(origin.partitions_id_by_path.clone());
            tmp_available_locations_by_path.extend(origin.available_locations_by_path.clone());
            tmp_available_location_by_bucket.extend(origin.available_locations_by_bucket.clone());
        }

        // iterate all table metadata
        for table_metadata in metadata_response.table_metadata {
            let table_id = table_metadata.table_id;
            let table_path = from_pb_table_path(&table_metadata.table_path);
            let table_descriptor = TableDescriptor::deserialize_json(
                &serde_json::from_slice(table_metadata.table_json.as_slice()).map_err(|e| {
                    Error::JsonSerdeError {
                        message: format!(
                            "Error deserializing table_json into TableDescriptor for table_id {table_id} and table_path {table_path}: {e}"
                        )
                    }
                })?,
            )?;
            let table_info = TableInfo::of(
                table_path.clone(),
                table_id,
                table_metadata.schema_id,
                table_descriptor,
                table_metadata.created_time,
                table_metadata.modified_time,
            );
            table_info_by_path.insert(table_path.clone(), table_info);
            table_id_by_path.insert(table_path.clone(), table_id);

            let bucket_metadata = table_metadata.bucket_metadata;
            let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));

            let bucket_locations = get_bucket_locations(
                &mut servers,
                bucket_metadata.as_slice(),
                table_id,
                None,
                &physical_table_path,
            );
            tmp_available_locations_by_path.insert(physical_table_path, bucket_locations);
        }

        // iterate all partition metadata
        for partition_metadata in metadata_response.partition_metadata {
            let table_id = partition_metadata.table_id;

            if let Some(cluster) = origin_cluster {
                let partition_name = partition_metadata.partition_name;
                let table_path = cluster.get_table_path_by_id(table_id).unwrap();
                let partition_id = partition_metadata.partition_id;

                let physical_table_path = Arc::new(PhysicalTablePath::of_partitioned(
                    Arc::new(table_path.clone()),
                    Some(partition_name),
                ));

                partitions_id_by_path.insert(Arc::clone(&physical_table_path), partition_id);

                let bucket_locations = get_bucket_locations(
                    &mut servers,
                    partition_metadata.bucket_metadata.as_slice(),
                    table_id,
                    Some(partition_id),
                    &physical_table_path,
                );

                tmp_available_locations_by_path.insert(physical_table_path, bucket_locations);
            }
        }

        for bucket_locations in &mut tmp_available_locations_by_path.values() {
            for location in bucket_locations {
                if location.leader().is_some() {
                    tmp_available_location_by_bucket
                        .insert(location.table_bucket.clone(), location.clone());
                }
            }
        }

        Ok(Cluster::new(
            coordinator_server,
            servers,
            tmp_available_locations_by_path,
            tmp_available_location_by_bucket,
            table_id_by_path,
            table_info_by_path,
            partitions_id_by_path,
        ))
    }

    pub fn get_coordinator_server(&self) -> Option<&ServerNode> {
        self.coordinator_server.as_ref()
    }

    pub fn leader_for(&self, table_bucket: &TableBucket) -> Option<&ServerNode> {
        let location = self.available_locations_by_bucket.get(table_bucket);
        if let Some(location) = location {
            location.leader().as_ref()
        } else {
            None
        }
    }

    pub fn get_tablet_server(&self, id: i32) -> Option<&ServerNode> {
        self.alive_tablet_servers_by_id.get(&id)
    }

    pub fn get_table_bucket(
        &self,
        physical_table_path: &PhysicalTablePath,
        bucket_id: BucketId,
    ) -> Result<TableBucket> {
        let table_info = self.get_table(physical_table_path.get_table_path())?;
        let partition_id = self.get_partition_id(physical_table_path);

        if physical_table_path.get_partition_name().is_some() && partition_id.is_none() {
            return Err(Error::partition_not_exist(format!(
                "The partition {} is not found in cluster",
                physical_table_path.get_partition_name().unwrap()
            )));
        }

        Ok(TableBucket::new_with_partition(
            table_info.table_id,
            partition_id,
            bucket_id,
        ))
    }

    pub fn get_partition_id(&self, physical_table_path: &PhysicalTablePath) -> Option<PartitionId> {
        self.partitions_id_by_path.get(physical_table_path).copied()
    }

    pub fn get_partition_name(&self, partition_id: PartitionId) -> Option<&String> {
        self.partition_name_by_id.get(&partition_id)
    }

    pub fn get_table_id(&self, table_path: &TablePath) -> Option<i64> {
        self.table_id_by_path.get(table_path).copied()
    }

    pub fn get_bucket_locations_by_path(
        &self,
    ) -> &HashMap<Arc<PhysicalTablePath>, Vec<BucketLocation>> {
        &self.available_locations_by_path
    }

    pub fn get_table_info_by_path(&self) -> &HashMap<TablePath, TableInfo> {
        &self.table_info_by_path
    }

    pub fn get_table_id_by_path(&self) -> &HashMap<TablePath, i64> {
        &self.table_id_by_path
    }

    pub fn get_table_path_by_id(&self, table_id: TableId) -> Option<&TablePath> {
        self.table_path_by_id.get(&table_id)
    }

    pub fn get_available_buckets_for_table_path(
        &self,
        table_path: &PhysicalTablePath,
    ) -> &Vec<BucketLocation> {
        self.available_locations_by_path
            .get(table_path)
            .unwrap_or(&EMPTY)
    }

    pub fn get_server_nodes(&self) -> Vec<ServerNode> {
        let mut nodes = Vec::new();
        if let Some(coordinator) = &self.coordinator_server {
            nodes.push(coordinator.clone());
        }
        nodes.extend(self.alive_tablet_servers.iter().cloned());
        nodes
    }

    pub fn get_one_available_server(&self) -> Option<&ServerNode> {
        if self.alive_tablet_servers.is_empty() {
            return None;
        }
        let offset = random_range(0..self.alive_tablet_servers.len());
        self.alive_tablet_servers.get(offset)
    }

    pub fn get_bucket_count(&self, table_path: &TablePath) -> i32 {
        self.table_info_by_path
            .get(table_path)
            .unwrap_or_else(|| panic!("can't not table info by path {table_path}"))
            .num_buckets
    }

    pub fn get_table(&self, table_path: &TablePath) -> Result<&TableInfo> {
        self.table_info_by_path
            .get(table_path)
            .ok_or_else(|| Error::invalid_table(format!("Table info not found for {table_path}")))
    }

    pub fn opt_get_table(&self, table_path: &TablePath) -> Option<&TableInfo> {
        self.table_info_by_path.get(table_path)
    }

    pub fn get_partition_id_by_path(&self) -> &HashMap<Arc<PhysicalTablePath>, PartitionId> {
        &self.partitions_id_by_path
    }
}

fn get_bucket_locations(
    servers: &mut HashMap<i32, ServerNode>,
    bucket_metadata: &[PbBucketMetadata],
    table_id: i64,
    partition_id: Option<PartitionId>,
    physical_table_path: &Arc<PhysicalTablePath>,
) -> Vec<BucketLocation> {
    let mut bucket_locations = Vec::new();
    for metadata in bucket_metadata {
        let bucket_id = metadata.bucket_id;
        let bucket = TableBucket::new_with_partition(table_id, partition_id, bucket_id);

        let server = if let Some(leader_id) = metadata.leader_id
            && let Some(server_node) = servers.get(&leader_id)
        {
            Some(server_node.clone())
        } else {
            None
        };

        bucket_locations.push(BucketLocation::new(
            bucket.clone(),
            server,
            Arc::clone(physical_table_path),
        ));
    }
    bucket_locations
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_coordinator() -> ServerNode {
        ServerNode::new(
            0,
            "coord-host".to_string(),
            9123,
            ServerType::CoordinatorServer,
        )
    }

    fn make_tablet_servers() -> HashMap<i32, ServerNode> {
        let mut servers = HashMap::new();
        servers.insert(
            1,
            ServerNode::new(1, "ts1-host".to_string(), 9124, ServerType::TabletServer),
        );
        servers.insert(
            2,
            ServerNode::new(2, "ts2-host".to_string(), 9125, ServerType::TabletServer),
        );
        servers
    }

    #[test]
    fn test_server_node_getters() {
        let node = ServerNode::new(5, "myhost".to_string(), 8080, ServerType::TabletServer);
        assert_eq!(node.id(), 5);
        assert_eq!(node.host(), "myhost");
        assert_eq!(node.port(), 8080);
        assert_eq!(node.server_type(), &ServerType::TabletServer);
        assert_eq!(node.uid(), "ts-5");
        assert_eq!(node.url(), "myhost:8080");
    }

    #[test]
    fn test_server_type_display() {
        assert_eq!(ServerType::TabletServer.to_string(), "TabletServer");
        assert_eq!(
            ServerType::CoordinatorServer.to_string(),
            "CoordinatorServer"
        );
    }

    #[test]
    fn test_get_server_nodes_with_coordinator_and_tablets() {
        let cluster = Cluster::new(
            Some(make_coordinator()),
            make_tablet_servers(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        let nodes = cluster.get_server_nodes();
        assert_eq!(nodes.len(), 3);

        let coordinator_count = nodes
            .iter()
            .filter(|n| *n.server_type() == ServerType::CoordinatorServer)
            .count();
        assert_eq!(coordinator_count, 1);

        let tablet_count = nodes
            .iter()
            .filter(|n| *n.server_type() == ServerType::TabletServer)
            .count();
        assert_eq!(tablet_count, 2);
    }

    #[test]
    fn test_get_server_nodes_no_coordinator() {
        let cluster = Cluster::new(
            None,
            make_tablet_servers(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        let nodes = cluster.get_server_nodes();
        assert_eq!(nodes.len(), 2);
        assert!(
            nodes
                .iter()
                .all(|n| *n.server_type() == ServerType::TabletServer)
        );
    }

    #[test]
    fn test_get_server_nodes_empty_cluster() {
        let cluster = Cluster::default();
        let nodes = cluster.get_server_nodes();
        assert!(nodes.is_empty());
    }
}
