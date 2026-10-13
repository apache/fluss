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
    JsonSerde, PhysicalTablePath, TableBucket, TableDescriptor, TableInfo, TableOrPartition,
    TablePath,
};
use crate::proto::{MetadataResponse, PbBucketMetadata};
use crate::rpc::{from_pb_server_node, from_pb_table_path};
use crate::{BucketId, PartitionId, TableId};
use rand::random_range;
use rand::seq::IteratorRandom;
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
    bucket_count_by_table_or_partition: HashMap<TableOrPartition, i32>,
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
        bucket_count_by_table_or_partition: HashMap<TableOrPartition, i32>,
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
            bucket_count_by_table_or_partition,
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
            self.bucket_count_by_table_or_partition.clone(),
        )
    }

    pub fn invalidate_physical_table_meta(
        &self,
        physical_tables_to_invalid: &HashSet<PhysicalTablePath>,
    ) -> Self {
        let (available_locations_by_path, available_locations_by_bucket) =
            self.filter_bucket_locations_by_physical_path(physical_tables_to_invalid);

        // A table or partition id keeps one bucket count for life, so the counts stay cached.
        Cluster::new(
            self.coordinator_server.clone(),
            self.alive_tablet_servers_by_id.clone(),
            available_locations_by_path,
            available_locations_by_bucket,
            self.table_id_by_path.clone(),
            self.table_info_by_path.clone(),
            self.partitions_id_by_path.clone(),
            self.bucket_count_by_table_or_partition.clone(),
        )
    }

    /// Returns a copy without `table_path`'s id, info, partitions and bucket
    /// locations. Used once the table is known to be dropped or recreated.
    pub fn evict_table(&self, table_path: &TablePath) -> Self {
        let table_paths = HashSet::from([table_path]);
        let (available_locations_by_path, available_locations_by_bucket) =
            self.filter_bucket_locations_by_path(&table_paths);

        let table_id_by_path = self
            .table_id_by_path
            .iter()
            .filter(|&(path, _)| path != table_path)
            .map(|(path, table_id)| (path.clone(), *table_id))
            .collect();

        let table_info_by_path = self
            .table_info_by_path
            .iter()
            .filter(|&(path, _)| path != table_path)
            .map(|(path, table_info)| (path.clone(), table_info.clone()))
            .collect();

        let partitions_id_by_path = self
            .partitions_id_by_path
            .iter()
            .filter(|&(path, _)| path.get_table_path() != table_path)
            .map(|(path, partition_id)| (Arc::clone(path), *partition_id))
            .collect();

        let mut evicted: HashSet<TableOrPartition> = self
            .partitions_id_by_path
            .iter()
            .filter(|&(path, _)| path.get_table_path() == table_path)
            .map(|(_, partition_id)| TableOrPartition::Partition(*partition_id))
            .collect();
        if let Some(table_id) = self.get_table_id(table_path) {
            evicted.insert(TableOrPartition::Table(table_id));
        }
        let bucket_count_by_table_or_partition = self
            .bucket_count_by_table_or_partition
            .iter()
            .filter(|(key, _)| !evicted.contains(key))
            .map(|(key, count)| (*key, *count))
            .collect();

        Cluster::new(
            self.coordinator_server.clone(),
            self.alive_tablet_servers_by_id.clone(),
            available_locations_by_path,
            available_locations_by_bucket,
            table_id_by_path,
            table_info_by_path,
            partitions_id_by_path,
            bucket_count_by_table_or_partition,
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
            bucket_count_by_table_or_partition,
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
        self.bucket_count_by_table_or_partition = bucket_count_by_table_or_partition;
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

    fn filter_bucket_locations_by_physical_path(
        &self,
        physical_table_paths: &HashSet<PhysicalTablePath>,
    ) -> (
        HashMap<Arc<PhysicalTablePath>, Vec<BucketLocation>>,
        HashMap<TableBucket, BucketLocation>,
    ) {
        let available_locations_by_path = self
            .available_locations_by_path
            .iter()
            .filter(|&(path, _)| !physical_table_paths.contains(path.as_ref()))
            .map(|(path, locations)| (path.clone(), locations.clone()))
            .collect();

        let available_locations_by_bucket = self
            .available_locations_by_bucket
            .iter()
            .filter(|&(_bucket, location)| {
                !physical_table_paths.contains(location.physical_table_path.as_ref())
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
        let mut bucket_count_by_table_or_partition = HashMap::new();

        // A table's metadata never lists its partitions, so a table recreated under the same
        // path would otherwise keep the dropped table's partitions and their bucket counts.
        let without_dropped_tables = origin_cluster.and_then(|origin| {
            metadata_response
                .table_metadata
                .iter()
                .map(|table_metadata| {
                    (
                        from_pb_table_path(&table_metadata.table_path),
                        table_metadata.table_id,
                    )
                })
                .filter(|(table_path, table_id)| {
                    origin
                        .get_table_id(table_path)
                        .is_some_and(|cached_id| cached_id < *table_id)
                })
                .fold(None, |evicted: Option<Cluster>, (table_path, _)| {
                    Some(evicted.as_ref().unwrap_or(origin).evict_table(&table_path))
                })
        });
        if let Some(origin) = without_dropped_tables.as_ref().or(origin_cluster) {
            table_info_by_path.extend(origin.get_table_info_by_path().clone());
            table_id_by_path.extend(origin.get_table_id_by_path().clone());
            partitions_id_by_path.extend(origin.partitions_id_by_path.clone());
            tmp_available_locations_by_path.extend(origin.available_locations_by_path.clone());
            tmp_available_location_by_bucket.extend(origin.available_locations_by_bucket.clone());
            bucket_count_by_table_or_partition
                .extend(origin.bucket_count_by_table_or_partition.clone());
        }

        let mut response_table_paths: HashMap<TableId, TablePath> = HashMap::new();
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
            )
            .with_bucket_count_epoch(table_metadata.bucket_count_epoch.unwrap_or(0));
            // A bucket is listed only once it has had a leader, but a non-partitioned table
            // never changes `bucket.num`.
            if !table_info.is_partitioned() && table_info.num_buckets > 0 {
                bucket_count_by_table_or_partition
                    .insert(TableOrPartition::Table(table_id), table_info.num_buckets);
            }
            table_info_by_path.insert(table_path.clone(), table_info);
            table_id_by_path.insert(table_path.clone(), table_id);
            response_table_paths.insert(table_id, table_path.clone());

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
            // The response may carry the partition's table too, e.g. after the table was recreated.
            let table_path = response_table_paths
                .get(&table_id)
                .or_else(|| origin_cluster.and_then(|cluster| cluster.get_table_path_by_id(table_id)))
                .cloned()
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!(
                        "Partition {} belongs to table id {table_id}, which the metadata does not know.",
                        partition_metadata.partition_id
                    ),
                    source: None,
                })?;
            let partition_name = partition_metadata.partition_name;
            let partition_id = partition_metadata.partition_id;
            // Servers before per-partition bucket counts send none. Their assignment lists only
            // buckets that have had a leader, while every partition has the table's count until
            // `bucket.num` changes.
            let bucket_count = partition_metadata.bucket_count.unwrap_or_else(|| {
                table_info_by_path
                    .get(&table_path)
                    .filter(|table_info| table_info.get_bucket_count_epoch() == 0)
                    .map_or(
                        partition_metadata.bucket_metadata.len() as i32,
                        |table_info| table_info.num_buckets,
                    )
            });

            let physical_table_path = Arc::new(PhysicalTablePath::of_partitioned(
                Arc::new(table_path),
                Some(partition_name),
            ));

            partitions_id_by_path.insert(Arc::clone(&physical_table_path), partition_id);

            if bucket_count > 0 {
                bucket_count_by_table_or_partition
                    .insert(TableOrPartition::Partition(partition_id), bucket_count);
            }

            let bucket_locations = get_bucket_locations(
                &mut servers,
                partition_metadata.bucket_metadata.as_slice(),
                table_id,
                Some(partition_id),
                &physical_table_path,
            );

            tmp_available_locations_by_path.insert(physical_table_path, bucket_locations);
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
            bucket_count_by_table_or_partition,
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

    pub fn get_table_id(&self, table_path: &TablePath) -> Option<TableId> {
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

    pub fn get_table_id_by_path(&self) -> &HashMap<TablePath, TableId> {
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

    pub(crate) fn get_one_available_server_excluding(
        &self,
        unavailable_server_ids: &HashSet<i32>,
    ) -> Option<&ServerNode> {
        self.alive_tablet_servers
            .iter()
            .filter(|server| !unavailable_server_ids.contains(&server.id()))
            .choose(&mut rand::rng())
    }

    pub fn bucket_count(&self, table_or_partition: TableOrPartition) -> Option<i32> {
        self.bucket_count_by_table_or_partition
            .get(&table_or_partition)
            .copied()
    }

    /// Falls back to `num_buckets` only while the table has never changed `bucket.num`.
    pub fn bucket_count_or_fallback(
        &self,
        table_info: &TableInfo,
        partition_id: Option<PartitionId>,
    ) -> Result<i32> {
        let table_or_partition = TableOrPartition::of(table_info.table_id, partition_id);
        if let Some(count) = self.bucket_count(table_or_partition) {
            return Ok(count);
        }
        let epoch = table_info.get_bucket_count_epoch();
        if epoch > 0 {
            return Err(Error::invalid_bucket_routing(format!(
                "Routing bucket count is unavailable for {table_or_partition:?} at bucket count \
                 epoch {epoch}; refusing to fall back to the table-level count."
            )));
        }
        Ok(table_info.num_buckets)
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
    table_id: TableId,
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
    use crate::metadata::{DataTypes, Schema};
    use crate::proto::{PbPartitionMetadata, PbServerNode, PbTableMetadata, PbTablePath};

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
    fn test_server_type_from_type_id() {
        assert_eq!(ServerType::from_type_id(1), ServerType::CoordinatorServer);
        assert_eq!(ServerType::from_type_id(2), ServerType::TabletServer);
        assert_eq!(ServerType::from_type_id(-1), ServerType::Unknown);
        assert_eq!(ServerType::from_type_id(99), ServerType::Unknown);
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
    fn test_invalidate_physical_table_meta_only_invalidates_exact_partition() {
        let table_path = Arc::new(TablePath::new("db", "table"));
        let partition_1 = Arc::new(PhysicalTablePath::of_partitioned(
            Arc::clone(&table_path),
            Some("p1".to_string()),
        ));
        let partition_2 = Arc::new(PhysicalTablePath::of_partitioned(
            Arc::clone(&table_path),
            Some("p2".to_string()),
        ));
        let bucket_1 = TableBucket::new_with_partition(1, Some(10), 0);
        let bucket_2 = TableBucket::new_with_partition(1, Some(20), 0);
        let leader = ServerNode::new(1, "ts1-host".to_string(), 9124, ServerType::TabletServer);
        let location_1 = BucketLocation::new(
            bucket_1.clone(),
            Some(leader.clone()),
            Arc::clone(&partition_1),
        );
        let location_2 = BucketLocation::new(
            bucket_2.clone(),
            Some(leader.clone()),
            Arc::clone(&partition_2),
        );
        let cluster = Cluster::new(
            None,
            HashMap::from([(leader.id(), leader)]),
            HashMap::from([
                (Arc::clone(&partition_1), vec![location_1.clone()]),
                (Arc::clone(&partition_2), vec![location_2.clone()]),
            ]),
            HashMap::from([
                (bucket_1.clone(), location_1),
                (bucket_2.clone(), location_2),
            ]),
            HashMap::new(),
            HashMap::new(),
            HashMap::from([
                (Arc::clone(&partition_1), 10),
                (Arc::clone(&partition_2), 20),
            ]),
            HashMap::from([(TableOrPartition::Partition(10), 4)]),
        );

        let updated_cluster =
            cluster.invalidate_physical_table_meta(&HashSet::from([partition_1.as_ref().clone()]));

        assert_eq!(
            updated_cluster.bucket_count(TableOrPartition::Partition(10)),
            Some(4)
        );

        assert!(updated_cluster.leader_for(&bucket_1).is_none());
        assert!(updated_cluster.leader_for(&bucket_2).is_some());
        assert!(
            updated_cluster
                .get_available_buckets_for_table_path(partition_1.as_ref())
                .is_empty()
        );
        assert_eq!(
            updated_cluster
                .get_available_buckets_for_table_path(partition_2.as_ref())
                .len(),
            1
        );
    }

    #[test]
    fn test_evict_table_removes_all_table_state() {
        let table_path_a = Arc::new(TablePath::new("db", "table_a"));
        let table_path_b = Arc::new(TablePath::new("db", "table_b"));
        let partition_a = Arc::new(PhysicalTablePath::of_partitioned(
            Arc::clone(&table_path_a),
            Some("p1".to_string()),
        ));
        let physical_b = Arc::new(PhysicalTablePath::of(Arc::clone(&table_path_b)));
        let bucket_a = TableBucket::new_with_partition(1, Some(10), 0);
        let bucket_b = TableBucket::new(2, 0);
        let leader = ServerNode::new(1, "ts1-host".to_string(), 9124, ServerType::TabletServer);
        let location_a = BucketLocation::new(
            bucket_a.clone(),
            Some(leader.clone()),
            Arc::clone(&partition_a),
        );
        let location_b = BucketLocation::new(
            bucket_b.clone(),
            Some(leader.clone()),
            Arc::clone(&physical_b),
        );
        let cluster = Cluster::new(
            None,
            HashMap::from([(leader.id(), leader)]),
            HashMap::from([
                (Arc::clone(&partition_a), vec![location_a.clone()]),
                (Arc::clone(&physical_b), vec![location_b.clone()]),
            ]),
            HashMap::from([
                (bucket_a.clone(), location_a),
                (bucket_b.clone(), location_b),
            ]),
            HashMap::from([
                (table_path_a.as_ref().clone(), 1),
                (table_path_b.as_ref().clone(), 2),
            ]),
            HashMap::from([
                (
                    table_path_a.as_ref().clone(),
                    crate::test_utils::build_table_info(table_path_a.as_ref().clone(), 1, 1),
                ),
                (
                    table_path_b.as_ref().clone(),
                    crate::test_utils::build_table_info(table_path_b.as_ref().clone(), 2, 1),
                ),
            ]),
            HashMap::from([(Arc::clone(&partition_a), 10)]),
            HashMap::from([
                (TableOrPartition::Partition(10), 1),
                (TableOrPartition::Table(2), 1),
            ]),
        );

        let updated_cluster = cluster.evict_table(table_path_a.as_ref());

        assert_eq!(
            updated_cluster.bucket_count(TableOrPartition::Partition(10)),
            None
        );
        assert_eq!(
            updated_cluster.bucket_count(TableOrPartition::Table(2)),
            Some(1)
        );

        assert!(
            updated_cluster
                .get_table_id(table_path_a.as_ref())
                .is_none()
        );
        assert!(
            updated_cluster
                .opt_get_table(table_path_a.as_ref())
                .is_none()
        );
        assert!(updated_cluster.get_table_path_by_id(1).is_none());
        assert!(
            updated_cluster
                .get_partition_id(partition_a.as_ref())
                .is_none()
        );
        assert!(updated_cluster.get_partition_name(10).is_none());
        assert!(updated_cluster.leader_for(&bucket_a).is_none());
        assert!(
            updated_cluster
                .get_available_buckets_for_table_path(partition_a.as_ref())
                .is_empty()
        );

        assert_eq!(updated_cluster.get_table_id(table_path_b.as_ref()), Some(2));
        assert!(
            updated_cluster
                .opt_get_table(table_path_b.as_ref())
                .is_some()
        );
        assert!(updated_cluster.leader_for(&bucket_b).is_some());
        assert_eq!(
            updated_cluster
                .get_available_buckets_for_table_path(physical_b.as_ref())
                .len(),
            1
        );
    }

    #[test]
    fn test_get_server_nodes_empty_cluster() {
        let cluster = Cluster::default();
        let nodes = cluster.get_server_nodes();
        assert!(nodes.is_empty());
    }

    fn partitioned_table_info(num_buckets: i32, bucket_count_epoch: i64) -> TableInfo {
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("p", DataTypes::string())
                    .build()
                    .unwrap(),
            )
            .partitioned_by(vec!["p"])
            .distributed_by(Some(num_buckets), vec!["id".to_string()])
            .build()
            .unwrap();
        TableInfo::of(TablePath::new("db", "parts"), 1, 1, descriptor, 0, 0)
            .with_bucket_count_epoch(bucket_count_epoch)
    }

    #[test]
    fn bucket_count_or_fallback_refuses_the_table_count_after_a_rescale() {
        let cluster = Cluster::new(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::from([(TableOrPartition::Partition(10), 4)]),
        );

        let never_rescaled = partitioned_table_info(8, 0);
        assert_eq!(
            cluster
                .bucket_count_or_fallback(&never_rescaled, Some(10))
                .unwrap(),
            4
        );
        assert_eq!(
            cluster
                .bucket_count_or_fallback(&never_rescaled, Some(20))
                .unwrap(),
            8
        );

        let rescaled = partitioned_table_info(8, 1);
        assert_eq!(
            cluster
                .bucket_count_or_fallback(&rescaled, Some(10))
                .unwrap(),
            4
        );
        let err = cluster
            .bucket_count_or_fallback(&rescaled, Some(20))
            .unwrap_err();
        assert_eq!(
            err.api_error(),
            Some(crate::rpc::FlussError::InvalidBucketRouting)
        );
    }

    #[test]
    fn from_metadata_response_reads_partitions_of_a_table_in_the_same_response() {
        let table_info = partitioned_table_info(4, 0);
        let table_json = serde_json::to_vec(
            &TableDescriptor::builder()
                .schema(table_info.schema.clone())
                .partitioned_by(vec!["p"])
                .distributed_by(Some(4), vec!["id".to_string()])
                .build()
                .unwrap()
                .serialize_json()
                .unwrap(),
        )
        .unwrap();
        let response = |table_id| MetadataResponse {
            coordinator_server: None,
            tablet_servers: vec![PbServerNode {
                node_id: 1,
                host: "ts1".to_string(),
                port: 9123,
                listeners: None,
                rack: None,
            }],
            table_metadata: vec![PbTableMetadata {
                table_path: PbTablePath {
                    database_name: "db".to_string(),
                    table_name: "parts".to_string(),
                },
                table_id,
                schema_id: 1,
                table_json: table_json.clone(),
                bucket_metadata: vec![],
                created_time: 0,
                modified_time: 0,
                remote_data_dir: None,
                bucket_count_epoch: None,
            }],
            partition_metadata: vec![PbPartitionMetadata {
                table_id,
                partition_name: "p1".to_string(),
                partition_id: table_id * 10,
                bucket_metadata: vec![],
                bucket_count: Some(4),
            }],
        };
        let partition_path = PhysicalTablePath::of_partitioned(
            Arc::new(TablePath::new("db", "parts")),
            Some("p1".to_string()),
        );

        let fresh = Cluster::from_metadata_response(response(1), None).unwrap();
        assert_eq!(fresh.get_partition_id(&partition_path), Some(10));
        assert_eq!(fresh.bucket_count(TableOrPartition::Partition(10)), Some(4));

        // The cached cluster only knows the dropped table.
        let recreated = Cluster::from_metadata_response(response(2), Some(&fresh)).unwrap();
        assert_eq!(recreated.get_partition_id(&partition_path), Some(20));
        assert_eq!(
            recreated.bucket_count(TableOrPartition::Partition(20)),
            Some(4)
        );
    }

    #[test]
    fn from_metadata_response_reads_bucket_counts_and_epoch() {
        let table_info = partitioned_table_info(8, 0);
        let table_json = serde_json::to_vec(
            &TableDescriptor::builder()
                .schema(table_info.schema.clone())
                .partitioned_by(vec!["p"])
                .distributed_by(Some(8), vec!["id".to_string()])
                .build()
                .unwrap()
                .serialize_json()
                .unwrap(),
        )
        .unwrap();
        let bucket = |bucket_id| PbBucketMetadata {
            bucket_id,
            leader_id: Some(1),
            replica_id: vec![1],
            leader_epoch: None,
            bucket_epoch: None,
            isr: vec![1],
        };
        let server = PbServerNode {
            node_id: 1,
            host: "ts1".to_string(),
            port: 9123,
            listeners: None,
            rack: None,
        };
        let table_response = MetadataResponse {
            coordinator_server: None,
            tablet_servers: vec![server.clone()],
            table_metadata: vec![PbTableMetadata {
                table_path: PbTablePath {
                    database_name: "db".to_string(),
                    table_name: "parts".to_string(),
                },
                table_id: 1,
                schema_id: 1,
                table_json,
                bucket_metadata: vec![],
                created_time: 0,
                modified_time: 0,
                remote_data_dir: None,
                bucket_count_epoch: Some(2),
            }],
            partition_metadata: vec![],
        };
        let table_cluster = Cluster::from_metadata_response(table_response, None).unwrap();
        assert_eq!(
            table_cluster
                .get_table(&TablePath::new("db", "parts"))
                .unwrap()
                .get_bucket_count_epoch(),
            2
        );
        // A partitioned table has no table-level assignment.
        assert_eq!(table_cluster.bucket_count(TableOrPartition::Table(1)), None);

        let partition_response = MetadataResponse {
            coordinator_server: None,
            tablet_servers: vec![server],
            table_metadata: vec![],
            partition_metadata: vec![
                PbPartitionMetadata {
                    table_id: 1,
                    partition_name: "p1".to_string(),
                    partition_id: 10,
                    bucket_metadata: vec![bucket(0)],
                    bucket_count: Some(4),
                },
                // An older server sends only the assignment.
                PbPartitionMetadata {
                    table_id: 1,
                    partition_name: "p2".to_string(),
                    partition_id: 20,
                    bucket_metadata: vec![bucket(0), bucket(1), bucket(2)],
                    bucket_count: None,
                },
            ],
        };
        let cluster =
            Cluster::from_metadata_response(partition_response, Some(&table_cluster)).unwrap();
        assert_eq!(
            cluster.bucket_count(TableOrPartition::Partition(10)),
            Some(4)
        );
        assert_eq!(
            cluster.bucket_count(TableOrPartition::Partition(20)),
            Some(3)
        );
    }

    fn pb_table_metadata(
        table_name: &str,
        table_id: TableId,
        partitioned: bool,
        bucket_metadata: Vec<PbBucketMetadata>,
    ) -> PbTableMetadata {
        let mut descriptor = TableDescriptor::builder().schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("p", DataTypes::string())
                .build()
                .unwrap(),
        );
        if partitioned {
            descriptor = descriptor.partitioned_by(vec!["p"]);
        }
        let descriptor = descriptor
            .distributed_by(Some(4), vec!["id".to_string()])
            .build()
            .unwrap();
        PbTableMetadata {
            table_path: PbTablePath {
                database_name: "db".to_string(),
                table_name: table_name.to_string(),
            },
            table_id,
            schema_id: 1,
            table_json: serde_json::to_vec(&descriptor.serialize_json().unwrap()).unwrap(),
            bucket_metadata,
            created_time: 0,
            modified_time: 0,
            remote_data_dir: None,
            bucket_count_epoch: None,
        }
    }

    fn pb_bucket(bucket_id: i32) -> PbBucketMetadata {
        PbBucketMetadata {
            bucket_id,
            leader_id: Some(1),
            replica_id: vec![1],
            leader_epoch: None,
            bucket_epoch: None,
            isr: vec![1],
        }
    }

    fn pb_metadata_response(
        table_metadata: Vec<PbTableMetadata>,
        partition_metadata: Vec<PbPartitionMetadata>,
    ) -> MetadataResponse {
        MetadataResponse {
            coordinator_server: None,
            tablet_servers: vec![PbServerNode {
                node_id: 1,
                host: "ts1".to_string(),
                port: 9123,
                listeners: None,
                rack: None,
            }],
            table_metadata,
            partition_metadata,
        }
    }

    #[test]
    fn from_metadata_response_counts_buckets_that_never_had_a_leader() {
        let listed = vec![pb_bucket(0), pb_bucket(1), pb_bucket(3)];
        // An older server, where bucket 2 has never had a leader.
        let response = pb_metadata_response(
            vec![
                pb_table_metadata("plain", 1, false, listed.clone()),
                pb_table_metadata("parts", 2, true, vec![]),
            ],
            vec![PbPartitionMetadata {
                table_id: 2,
                partition_name: "p1".to_string(),
                partition_id: 10,
                bucket_metadata: listed,
                bucket_count: None,
            }],
        );

        let cluster = Cluster::from_metadata_response(response, None).unwrap();

        assert_eq!(cluster.bucket_count(TableOrPartition::Table(1)), Some(4));
        assert_eq!(
            cluster.bucket_count(TableOrPartition::Partition(10)),
            Some(4)
        );
    }

    #[test]
    fn from_metadata_response_drops_the_partitions_of_a_recreated_table() {
        let dropped = pb_metadata_response(
            vec![pb_table_metadata("parts", 1, true, vec![])],
            vec![PbPartitionMetadata {
                table_id: 1,
                partition_name: "p1".to_string(),
                partition_id: 10,
                bucket_metadata: vec![pb_bucket(0)],
                bucket_count: Some(2),
            }],
        );
        let dropped = Cluster::from_metadata_response(dropped, None).unwrap();
        let partition_path = PhysicalTablePath::of_partitioned(
            Arc::new(TablePath::new("db", "parts")),
            Some("p1".to_string()),
        );
        assert_eq!(dropped.get_partition_id(&partition_path), Some(10));

        let recreated =
            pb_metadata_response(vec![pb_table_metadata("parts", 2, true, vec![])], vec![]);
        let cluster = Cluster::from_metadata_response(recreated, Some(&dropped)).unwrap();

        assert_eq!(
            cluster.get_table_id(&TablePath::new("db", "parts")),
            Some(2)
        );
        assert_eq!(cluster.get_partition_id(&partition_path), None);
        assert_eq!(cluster.bucket_count(TableOrPartition::Partition(10)), None);
    }
}
