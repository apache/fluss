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

use crate::client::metadata::Metadata;
use crate::cluster::{Cluster, ServerNode};
use crate::metadata::{
    AclFilter, AclInfo, AcquireKvSnapshotLeaseResult, ActiveKvSnapshots, AlterConfig,
    AlterTableChanges, BucketStatsRequest, ClusterHealth, CreateAclResult, DatabaseDescriptor,
    DatabaseInfo, DatabaseSummary, DescribeConfig, DropAclsFilterResult, GoalType, JsonSerde,
    KvSnapshotLeaseForTable, KvSnapshotMetadata, LakeSnapshot, LakeSnapshotInfo, LatestKvSnapshots,
    PartitionInfo, PartitionSpec, PhysicalTablePath, ProducerOffsets, ProducerTableOffsets,
    RebalanceProgress, RegisterProducerResult, RemoteLogManifestEntry, Schema, SchemaInfo,
    ServerTag, TableBucket, TableDescriptor, TableInfo, TableOrPartition, TablePath, TableStats,
};
use crate::rpc::message::{
    AcquireKvSnapshotLeaseRequest, AddServerTagRequest, AlterClusterConfigsRequest,
    AlterDatabaseRequest, AlterTableRequest, CancelRebalanceRequest, CreateAclsRequest,
    CreateDatabaseRequest, CreatePartitionRequest, CreateTableRequest, DatabaseExistsRequest,
    DeleteProducerOffsetsRequest, DescribeClusterConfigsRequest, DropAclsRequest,
    DropDatabaseRequest, DropKvSnapshotLeaseRequest, DropPartitionRequest, DropTableRequest,
    GetClusterHealthRequest, GetDatabaseInfoRequest, GetKvSnapshotMetadataRequest,
    GetLakeSnapshotRequest, GetLatestKvSnapshotsRequest, GetLatestLakeSnapshotRequest,
    GetProducerOffsetsRequest, GetTableRequest, GetTableSchemaRequestMsg, GetTableStatsRequest,
    ListAclsRequest, ListDatabaseSummariesRequest, ListDatabasesRequest, ListKvSnapshotsRequest,
    ListPartitionInfosRequest, ListRebalanceProgressRequest, ListRemoteLogManifestsRequest,
    ListTablesRequest, RebalanceRequest, RegisterProducerOffsetsRequest,
    ReleaseKvSnapshotLeaseRequest, RemoveServerTagRequest, TableExistsRequest,
};
use crate::rpc::message::{ListOffsetsRequest, OffsetSpec};
use crate::rpc::{RpcClient, ServerConnection};

use crate::error::{Error, Result};
use crate::proto::GetTableInfoResponse;
use crate::{BucketId, PartitionId, SnapshotId, TableId};
use futures::future::try_join_all;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::task::JoinHandle;

fn parse_table_descriptor(bytes: &[u8]) -> Result<TableDescriptor> {
    let node = serde_json::from_slice(bytes).map_err(|e| Error::JsonSerdeError {
        message: format!("Failed to parse table_json: {e}"),
    })?;
    TableDescriptor::deserialize_json(&node)
}

pub struct FlussAdmin {
    metadata: Arc<Metadata>,
    rpc_client: Arc<RpcClient>,
}

impl FlussAdmin {
    pub fn new(connections: Arc<RpcClient>, metadata: Arc<Metadata>) -> Self {
        FlussAdmin {
            metadata,
            rpc_client: connections,
        }
    }

    async fn admin_gateway(&self) -> Result<ServerConnection> {
        let cluster = self.metadata.get_cluster();
        let coordinator =
            cluster
                .get_coordinator_server()
                .ok_or_else(|| Error::UnexpectedError {
                    message: "Coordinator server not found in cluster metadata".to_string(),
                    source: None,
                })?;
        self.rpc_client.get_connection(coordinator).await
    }

    pub async fn create_database(
        &self,
        database_name: &str,
        database_descriptor: Option<&DatabaseDescriptor>,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(CreateDatabaseRequest::new(
                database_name,
                database_descriptor,
                ignore_if_exists,
            )?)
            .await?;
        Ok(())
    }

    pub async fn create_table(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(CreateTableRequest::new(
                table_path,
                table_descriptor,
                ignore_if_exists,
            )?)
            .await?;
        Ok(())
    }

    pub async fn drop_table(
        &self,
        table_path: &TablePath,
        ignore_if_not_exists: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(DropTableRequest::new(table_path, ignore_if_not_exists))
            .await?;
        Ok(())
    }

    /// Fetch the schema for `table_path` at the given `schema_id`. Pass
    /// `None` to request the latest.
    pub async fn get_table_schema(
        &self,
        table_path: &TablePath,
        schema_id: Option<i32>,
    ) -> Result<SchemaInfo> {
        let response = self
            .admin_gateway()
            .await?
            .request(GetTableSchemaRequestMsg::new(table_path, schema_id))
            .await?;

        let schema_node: serde_json::Value = serde_json::from_slice(&response.schema_json)
            .map_err(|e| Error::JsonSerdeError {
                message: format!("Failed to parse schema_json: {e}"),
            })?;
        let schema = Schema::deserialize_json(&schema_node)?;
        Ok(SchemaInfo::new(schema, response.schema_id))
    }

    pub async fn get_table_info(&self, table_path: &TablePath) -> Result<TableInfo> {
        let response = self
            .admin_gateway()
            .await?
            .request(GetTableRequest::new(table_path))
            .await?;

        // force update to avoid stale data in cache
        self.metadata
            .update_tables_metadata(&HashSet::from([table_path]), &HashSet::new(), vec![])
            .await?;

        let GetTableInfoResponse {
            table_id,
            schema_id,
            table_json,
            created_time,
            modified_time,
            bucket_count_epoch,
            ..
        } = response;
        let table_descriptor = parse_table_descriptor(&table_json)?;
        Ok(TableInfo::of(
            table_path.clone(),
            table_id,
            schema_id,
            table_descriptor,
            created_time,
            modified_time,
        )
        .with_bucket_count_epoch(bucket_count_epoch.unwrap_or(0)))
    }

    /// List all tables in the given database
    pub async fn list_tables(&self, database_name: &str) -> Result<Vec<String>> {
        let response = self
            .admin_gateway()
            .await?
            .request(ListTablesRequest::new(database_name))
            .await?;
        Ok(response.table_name)
    }

    /// List all partitions in the given table.
    pub async fn list_partition_infos(&self, table_path: &TablePath) -> Result<Vec<PartitionInfo>> {
        self.list_partition_infos_with_spec(table_path, None).await
    }

    /// List partitions in the given table that match the partial partition spec.
    pub async fn list_partition_infos_with_spec(
        &self,
        table_path: &TablePath,
        partial_partition_spec: Option<&PartitionSpec>,
    ) -> Result<Vec<PartitionInfo>> {
        let response = self
            .admin_gateway()
            .await?
            .request(ListPartitionInfosRequest::new(
                table_path,
                partial_partition_spec,
            ))
            .await?;
        let partitions = response.get_partitions_info();
        if partitions
            .iter()
            .all(|partition| partition.get_bucket_count() > 0)
        {
            return Ok(partitions);
        }
        let table_info = self.get_table_info(table_path).await?;
        fill_bucket_counts(partitions, &table_info)
    }

    /// Create a new partition for a partitioned table.
    pub async fn create_partition(
        &self,
        table_path: &TablePath,
        partition_spec: &PartitionSpec,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(CreatePartitionRequest::new(
                table_path,
                partition_spec,
                ignore_if_exists,
            ))
            .await?;
        Ok(())
    }

    /// Drop a partition from a partitioned table.
    pub async fn drop_partition(
        &self,
        table_path: &TablePath,
        partition_spec: &PartitionSpec,
        ignore_if_not_exists: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(DropPartitionRequest::new(
                table_path,
                partition_spec,
                ignore_if_not_exists,
            ))
            .await?;
        Ok(())
    }

    /// Check if a table exists
    pub async fn table_exists(&self, table_path: &TablePath) -> Result<bool> {
        let response = self
            .admin_gateway()
            .await?
            .request(TableExistsRequest::new(table_path))
            .await?;
        Ok(response.exists)
    }

    /// Drop a database
    pub async fn drop_database(
        &self,
        database_name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(DropDatabaseRequest::new(
                database_name,
                ignore_if_not_exists,
                cascade,
            ))
            .await?;
        Ok(())
    }

    /// List all databases
    pub async fn list_databases(&self) -> Result<Vec<String>> {
        let response = self
            .admin_gateway()
            .await?
            .request(ListDatabasesRequest::new())
            .await?;
        Ok(response.database_name)
    }

    /// Check if a database exists
    pub async fn database_exists(&self, database_name: &str) -> Result<bool> {
        let response = self
            .admin_gateway()
            .await?
            .request(DatabaseExistsRequest::new(database_name))
            .await?;
        Ok(response.exists)
    }

    /// Get database information
    pub async fn get_database_info(&self, database_name: &str) -> Result<DatabaseInfo> {
        let request = GetDatabaseInfoRequest::new(database_name);
        let response = self.admin_gateway().await?.request(request).await?;

        // Convert proto response to DatabaseInfo
        let database_descriptor = DatabaseDescriptor::from_json_bytes(&response.database_json)?;

        Ok(DatabaseInfo::new(
            database_name.to_string(),
            database_descriptor,
            response.created_time,
            response.modified_time,
        ))
    }

    /// Get all alive server nodes in the cluster, including the coordinator
    /// and all tablet servers. Refreshes cluster metadata before returning.
    pub async fn get_server_nodes(&self) -> Result<Vec<ServerNode>> {
        self.metadata.reinit_cluster().await?;
        Ok(self.metadata.get_cluster().get_server_nodes())
    }

    /// Get the latest lake snapshot for a table
    pub async fn get_latest_lake_snapshot(&self, table_path: &TablePath) -> Result<LakeSnapshot> {
        let response = self
            .admin_gateway()
            .await?
            .request(GetLatestLakeSnapshotRequest::new(table_path))
            .await?;

        // Convert proto response to LakeSnapshot
        let mut table_buckets_offset = HashMap::new();
        for bucket_snapshot in response.bucket_snapshots {
            let table_bucket = TableBucket::new_with_partition(
                response.table_id,
                bucket_snapshot.partition_id,
                bucket_snapshot.bucket_id,
            );
            if let Some(log_offset) = bucket_snapshot.log_offset {
                table_buckets_offset.insert(table_bucket, log_offset);
            }
        }

        Ok(LakeSnapshot::new(
            response.snapshot_id,
            table_buckets_offset,
        ))
    }

    /// List offset for the specified buckets. This operation enables to find the beginning offset,
    /// end offset as well as the offset matching a timestamp in buckets.
    pub async fn list_offsets(
        &self,
        table_path: &TablePath,
        buckets_id: &[BucketId],
        offset_spec: OffsetSpec,
    ) -> Result<HashMap<i32, i64>> {
        self.do_list_offsets(table_path, None, buckets_id, offset_spec)
            .await
    }

    /// List offset for the specified buckets in a partition. This operation enables to find
    /// the beginning offset, end offset as well as the offset matching a timestamp in buckets.
    pub async fn list_partition_offsets(
        &self,
        table_path: &TablePath,
        partition_name: &str,
        buckets_id: &[BucketId],
        offset_spec: OffsetSpec,
    ) -> Result<HashMap<i32, i64>> {
        self.do_list_offsets(table_path, Some(partition_name), buckets_id, offset_spec)
            .await
    }

    async fn do_list_offsets(
        &self,
        table_path: &TablePath,
        partition_name: Option<&str>,
        buckets_id: &[BucketId],
        offset_spec: OffsetSpec,
    ) -> Result<HashMap<i32, i64>> {
        if buckets_id.is_empty() {
            return Err(Error::IllegalArgument {
                message: "Buckets are empty.".to_string(),
            });
        }

        // force to update table metadata like java side
        self.metadata.update_table_metadata(table_path).await?;

        let cluster = self.metadata.get_cluster();
        let table_id = cluster.get_table(table_path)?.table_id;

        // Resolve partition_id from partition_name if provided
        let partition_id = if let Some(name) = partition_name {
            let physical_table_path = Arc::new(PhysicalTablePath::of_partitioned(
                Arc::new(table_path.clone()),
                Some(name.to_string()),
            ));

            // Update partition metadata like java side
            self.metadata
                .update_physical_table_metadata(std::slice::from_ref(&physical_table_path))
                .await?;

            let cluster = self.metadata.get_cluster();
            Some(
                cluster
                    .get_partition_id(&physical_table_path)
                    .ok_or_else(|| {
                        Error::partition_not_exist(format!(
                            "Partition '{name}' not found for table '{table_path}'"
                        ))
                    })?,
            )
        } else {
            None
        };

        self.list_offsets_by_id(table_id, partition_id, buckets_id, offset_spec)
            .await
    }

    async fn list_offsets_by_id(
        &self,
        table_id: TableId,
        partition_id: Option<PartitionId>,
        buckets_id: &[BucketId],
        offset_spec: OffsetSpec,
    ) -> Result<HashMap<i32, i64>> {
        // Prepare requests
        let requests_by_server =
            self.prepare_list_offsets_requests(table_id, partition_id, buckets_id, offset_spec)?;

        // Send Requests
        let response_futures = self.send_list_offsets_request(requests_by_server).await?;

        let mut results = HashMap::new();

        for response_future in response_futures {
            let offsets = response_future.await.map_err(|e| Error::UnexpectedError {
                message: "Fail to get result for list offsets.".to_string(),
                source: Some(Box::new(e)),
            })?;
            results.extend(offsets?);
        }
        Ok(results)
    }

    fn prepare_list_offsets_requests(
        &self,
        table_id: TableId,
        partition_id: Option<PartitionId>,
        buckets: &[BucketId],
        offset_spec: OffsetSpec,
    ) -> Result<HashMap<i32, ListOffsetsRequest>> {
        let cluster = self.metadata.get_cluster();
        let mut node_for_bucket_list: HashMap<i32, Vec<BucketId>> = HashMap::new();

        for bucket_id in buckets {
            let table_bucket = TableBucket::new_with_partition(table_id, partition_id, *bucket_id);
            let leader = cluster.leader_for(&table_bucket).ok_or_else(|| {
                // todo: consider retry?
                Error::UnexpectedError {
                    message: format!("No leader found for table bucket: {table_bucket}."),
                    source: None,
                }
            })?;

            node_for_bucket_list
                .entry(leader.id())
                .or_default()
                .push(*bucket_id);
        }

        let routing_bucket_count =
            cluster.bucket_count(TableOrPartition::of(table_id, partition_id));
        let mut list_offsets_requests = HashMap::new();
        for (leader_id, bucket_ids) in node_for_bucket_list {
            let request =
                ListOffsetsRequest::new(table_id, partition_id, bucket_ids, offset_spec.clone())
                    .with_routing_bucket_count(routing_bucket_count);
            list_offsets_requests.insert(leader_id, request);
        }
        Ok(list_offsets_requests)
    }

    async fn send_list_offsets_request(
        &self,
        request_map: HashMap<i32, ListOffsetsRequest>,
    ) -> Result<Vec<JoinHandle<Result<HashMap<i32, i64>>>>> {
        let mut tasks = Vec::new();

        for (leader_id, request) in request_map {
            let rpc_client = self.rpc_client.clone();
            let metadata = self.metadata.clone();

            let task = tokio::spawn(async move {
                let cluster = metadata.get_cluster();
                let tablet_server = cluster.get_tablet_server(leader_id).ok_or_else(|| {
                    Error::leader_not_available(format!(
                        "Tablet server {leader_id} is not found in metadata cache."
                    ))
                })?;
                let connection = rpc_client.get_connection(tablet_server).await?;
                let list_offsets_response = connection.request(request).await?;
                list_offsets_response.offsets()
            });
            tasks.push(task);
        }
        Ok(tasks)
    }

    /// List database summaries (name, created_time, table_count).
    pub async fn list_database_summaries(&self) -> Result<Vec<DatabaseSummary>> {
        let response = self
            .admin_gateway()
            .await?
            .request(ListDatabaseSummariesRequest::new())
            .await?;
        Ok(response
            .database_summary
            .iter()
            .map(DatabaseSummary::from_pb)
            .collect())
    }

    /// Alter a database: config changes and/or an updated comment.
    pub async fn alter_database(
        &self,
        name: &str,
        config_changes: Vec<AlterConfig>,
        comment: Option<&str>,
        ignore_if_not_exists: bool,
    ) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(AlterDatabaseRequest::new(
                name,
                ignore_if_not_exists,
                config_changes,
                comment,
            ))
            .await?;
        Ok(())
    }

    /// Alter a table: config changes plus any combination of add/drop/rename/modify columns.
    /// Bundle the column-level edits in [`AlterTableChanges`].
    pub async fn alter_table(
        &self,
        table_path: &TablePath,
        ignore_if_not_exists: bool,
        changes: AlterTableChanges,
    ) -> Result<()> {
        changes.validate()?;
        let _response = self
            .admin_gateway()
            .await?
            .request(AlterTableRequest::new(
                table_path,
                ignore_if_not_exists,
                changes,
            ))
            .await?;
        Ok(())
    }

    /// Get table statistics for buckets. Pass empty `target_columns` to request stats for all columns.
    ///
    /// The table must be in the metadata cache, as it is after [`Self::get_table_info`].
    pub async fn get_table_stats(
        &self,
        table_id: TableId,
        buckets_req: Vec<BucketStatsRequest>,
        target_columns: Vec<i32>,
    ) -> Result<TableStats> {
        if buckets_req.is_empty() {
            return Ok(TableStats {
                buckets: Vec::new(),
            });
        }
        let table_path = self
            .metadata
            .get_cluster()
            .get_table_path_by_id(table_id)
            .cloned()
            .ok_or_else(|| Error::IllegalArgument {
                message: format!("Table id {table_id} is not in the metadata cache."),
            })?;
        // Nothing else refreshes a cached partition's leaders, so refresh them with the table.
        let partition_ids: Vec<PartitionId> = buckets_req
            .iter()
            .filter_map(|bucket| bucket.partition_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        self.metadata
            .update_tables_metadata(
                &HashSet::from([&table_path]),
                &HashSet::new(),
                partition_ids,
            )
            .await?;

        let cluster = self.metadata.get_cluster();
        let mut requests = Vec::new();
        for (leader_id, buckets) in group_stats_requests_by_leader(&cluster, table_id, buckets_req)?
        {
            let tablet_server = cluster
                .get_tablet_server(leader_id)
                .cloned()
                .ok_or_else(|| {
                    Error::leader_not_available(format!(
                        "Tablet server {leader_id} is not found in metadata cache."
                    ))
                })?;
            let request = GetTableStatsRequest::new(table_id, buckets, target_columns.clone())
                .with_routing_bucket_counts(|partition_id| {
                    cluster.bucket_count(TableOrPartition::of(table_id, partition_id))
                });
            requests.push((tablet_server, request));
        }
        let responses = try_join_all(requests.into_iter().map(
            |(tablet_server, request)| async move {
                self.rpc_client
                    .get_connection(&tablet_server)
                    .await?
                    .request(request)
                    .await
            },
        ))
        .await?;
        Ok(TableStats {
            buckets: responses
                .iter()
                .flat_map(|response| TableStats::from_pb(response).buckets)
                .collect(),
        })
    }

    /// Get the latest KV snapshots for a table (optionally scoped to one partition).
    pub async fn get_latest_kv_snapshots(
        &self,
        table_path: &TablePath,
        partition_name: Option<&str>,
    ) -> Result<LatestKvSnapshots> {
        let response = self
            .admin_gateway()
            .await?
            .request(GetLatestKvSnapshotsRequest::new(table_path, partition_name))
            .await?;
        let snapshots = LatestKvSnapshots::from_pb(&response);
        if !response.has_sync_indexes.unwrap_or(false) {
            return Ok(snapshots);
        }
        self.metadata.update_table_metadata(table_path).await?;
        if let Some(partition_id) = response.partition_id {
            self.metadata
                .check_and_update_partition_metadata_by_ids(table_path, &[partition_id])
                .await?;
        }
        let buckets: Vec<_> = snapshots
            .latest_snapshots
            .iter()
            .map(|s| s.bucket_id)
            .collect();
        let (visible, start) = futures::try_join!(
            self.list_offsets_by_id(
                response.table_id,
                response.partition_id,
                &buckets,
                OffsetSpec::Latest
            ),
            self.list_offsets_by_id(
                response.table_id,
                response.partition_id,
                &buckets,
                OffsetSpec::Earliest
            ),
        )?;
        if snapshots.latest_snapshots.iter().all(|snapshot| {
            let bucket = snapshot.bucket_id;
            if snapshot.snapshot_id.is_none() {
                start[&bucket] == 0
            } else {
                snapshot_offset_is_readable(snapshot.log_offset, visible[&bucket], start[&bucket])
            }
        }) {
            return Ok(snapshots);
        }
        let retained = self
            .admin_gateway()
            .await?
            .request(
                ListKvSnapshotsRequest::new(response.table_id, response.partition_id)
                    .with_log_offsets(),
            )
            .await?;
        select_readable_snapshots(snapshots, &retained.active_snapshots, &visible, &start)
    }

    /// Get KV snapshot metadata (manifest file list).
    pub async fn get_kv_snapshot_metadata(
        &self,
        table_id: TableId,
        partition_id: Option<PartitionId>,
        bucket_id: BucketId,
        snapshot_id: SnapshotId,
    ) -> Result<KvSnapshotMetadata> {
        let response = self
            .admin_gateway()
            .await?
            .request(GetKvSnapshotMetadataRequest::new(
                table_id,
                partition_id,
                bucket_id,
                snapshot_id,
            ))
            .await?;
        Ok(KvSnapshotMetadata::from_pb(&response))
    }

    /// Acquire a KV snapshot lease. Returns the snapshots the server could not lease.
    pub async fn create_kv_snapshot_lease(
        &self,
        lease_id: &str,
        lease_duration_ms: i64,
        snapshots_to_lease: Vec<KvSnapshotLeaseForTable>,
    ) -> Result<AcquireKvSnapshotLeaseResult> {
        let response = self
            .admin_gateway()
            .await?
            .request(AcquireKvSnapshotLeaseRequest::new(
                lease_id,
                lease_duration_ms,
                snapshots_to_lease,
            ))
            .await?;
        Ok(AcquireKvSnapshotLeaseResult::from_pb(&response))
    }

    /// Get a specific lake snapshot for a table.
    pub async fn get_lake_snapshot(
        &self,
        table_path: &TablePath,
        snapshot_id: Option<SnapshotId>,
        readable: Option<bool>,
    ) -> Result<LakeSnapshotInfo> {
        let response = self
            .admin_gateway()
            .await?
            .request(GetLakeSnapshotRequest::new(
                table_path,
                snapshot_id,
                readable,
            ))
            .await?;
        Ok(LakeSnapshotInfo::from_pb(&response))
    }

    /// Create ACLs. Returns one result per submitted ACL (success or per-ACL error).
    pub async fn create_acls(&self, acls: Vec<AclInfo>) -> Result<Vec<CreateAclResult>> {
        let response = self
            .admin_gateway()
            .await?
            .request(CreateAclsRequest::new(acls))
            .await?;
        response
            .acl_res
            .iter()
            .map(CreateAclResult::from_pb)
            .collect()
    }

    /// List ACLs matching a filter.
    pub async fn list_acls(&self, acl_filter: AclFilter) -> Result<Vec<AclInfo>> {
        let response = self
            .admin_gateway()
            .await?
            .request(ListAclsRequest::new(acl_filter))
            .await?;
        response.acl.iter().map(AclInfo::from_pb).collect()
    }

    /// Drop ACLs matching filters. Returns one result per submitted filter.
    pub async fn drop_acls(
        &self,
        acl_filters: Vec<AclFilter>,
    ) -> Result<Vec<DropAclsFilterResult>> {
        let response = self
            .admin_gateway()
            .await?
            .request(DropAclsRequest::new(acl_filters))
            .await?;
        response
            .filter_results
            .iter()
            .map(DropAclsFilterResult::from_pb)
            .collect()
    }

    /// Describe cluster configuration.
    pub async fn describe_cluster_configs(&self) -> Result<Vec<DescribeConfig>> {
        let response = self
            .admin_gateway()
            .await?
            .request(DescribeClusterConfigsRequest::new())
            .await?;
        Ok(response
            .configs
            .iter()
            .map(DescribeConfig::from_pb)
            .collect())
    }

    /// Alter cluster configuration.
    pub async fn alter_cluster_configs(&self, alter_configs: Vec<AlterConfig>) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(AlterClusterConfigsRequest::new(alter_configs))
            .await?;
        Ok(())
    }

    /// Add a tag to servers.
    pub async fn add_server_tag(&self, server_ids: Vec<i32>, server_tag: ServerTag) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(AddServerTagRequest::new(server_ids, server_tag))
            .await?;
        Ok(())
    }

    /// Remove a tag from servers.
    pub async fn remove_server_tag(
        &self,
        server_ids: Vec<i32>,
        server_tag: ServerTag,
    ) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(RemoveServerTagRequest::new(server_ids, server_tag))
            .await?;
        Ok(())
    }

    /// Trigger a rebalance. Returns the rebalance id assigned by the server.
    pub async fn rebalance(&self, goals: Vec<GoalType>) -> Result<String> {
        let response = self
            .admin_gateway()
            .await?
            .request(RebalanceRequest::new(goals))
            .await?;
        Ok(response.rebalance_id)
    }

    /// List rebalance progress (for a specific rebalance id, or all in-flight ones if `None`).
    pub async fn list_rebalance_progress(
        &self,
        rebalance_id: Option<&str>,
    ) -> Result<RebalanceProgress> {
        let response = self
            .admin_gateway()
            .await?
            .request(ListRebalanceProgressRequest::new(rebalance_id))
            .await?;
        RebalanceProgress::from_pb(&response)
    }

    /// Cancel a rebalance.
    pub async fn cancel_rebalance(&self, rebalance_id: Option<&str>) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(CancelRebalanceRequest::new(rebalance_id))
            .await?;
        Ok(())
    }

    /// Register producer offsets. Returns the server-side registration outcome (if any).
    pub async fn register_producer_offsets(
        &self,
        producer_id: &str,
        table_offsets: Vec<ProducerTableOffsets>,
        ttl_ms: Option<i64>,
    ) -> Result<Option<RegisterProducerResult>> {
        let response = self
            .admin_gateway()
            .await?
            .request(RegisterProducerOffsetsRequest::new(
                producer_id,
                table_offsets,
                ttl_ms,
            ))
            .await?;
        response
            .result
            .map(RegisterProducerResult::try_from_i32)
            .transpose()
    }

    /// Get producer offsets.
    pub async fn get_producer_offsets(&self, producer_id: &str) -> Result<ProducerOffsets> {
        let response = self
            .admin_gateway()
            .await?
            .request(GetProducerOffsetsRequest::new(producer_id))
            .await?;
        Ok(ProducerOffsets::from_pb(&response))
    }

    /// Delete producer offsets.
    pub async fn delete_producer_offsets(&self, producer_id: &str) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(DeleteProducerOffsetsRequest::new(producer_id))
            .await?;
        Ok(())
    }

    /// Get cluster health status.
    pub async fn get_cluster_health(&self) -> Result<ClusterHealth> {
        let response = self
            .admin_gateway()
            .await?
            .request(GetClusterHealthRequest::new())
            .await?;
        ClusterHealth::from_pb(&response)
    }

    /// List remote log manifests for a table (optionally scoped to one partition).
    pub async fn list_remote_log_manifests(
        &self,
        table_id: TableId,
        partition_id: Option<PartitionId>,
    ) -> Result<Vec<RemoteLogManifestEntry>> {
        let response = self
            .admin_gateway()
            .await?
            .request(ListRemoteLogManifestsRequest::new(table_id, partition_id))
            .await?;
        Ok(response
            .manifests
            .iter()
            .map(RemoteLogManifestEntry::from_pb)
            .collect())
    }

    /// List active KV snapshots for a table (optionally scoped to one partition).
    pub async fn list_kv_snapshots(
        &self,
        table_id: TableId,
        partition_id: Option<PartitionId>,
    ) -> Result<ActiveKvSnapshots> {
        let response = self
            .admin_gateway()
            .await?
            .request(ListKvSnapshotsRequest::new(table_id, partition_id))
            .await?;
        Ok(ActiveKvSnapshots::from_pb(&response))
    }

    /// Release specific bucket snapshots from a KV snapshot lease.
    pub async fn release_kv_snapshot_lease(
        &self,
        lease_id: &str,
        buckets_to_release: Vec<TableBucket>,
    ) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(ReleaseKvSnapshotLeaseRequest::new(
                lease_id,
                buckets_to_release,
            ))
            .await?;
        Ok(())
    }

    /// Drop an entire KV snapshot lease.
    pub async fn drop_kv_snapshot_lease(&self, lease_id: &str) -> Result<()> {
        let _response = self
            .admin_gateway()
            .await?
            .request(DropKvSnapshotLeaseRequest::new(lease_id))
            .await?;
        Ok(())
    }
}

fn snapshot_offset_is_readable(offset: Option<i64>, visible: i64, start: i64) -> bool {
    offset.is_some_and(|offset| offset <= visible && offset >= start)
}

fn select_readable_snapshots(
    mut snapshots: LatestKvSnapshots,
    candidates: &[crate::proto::PbKvSnapshot],
    visible: &HashMap<BucketId, i64>,
    start: &HashMap<BucketId, i64>,
) -> Result<LatestKvSnapshots> {
    let mut selected = HashMap::new();
    for candidate in candidates {
        let bucket = candidate.bucket_id;
        if snapshot_offset_is_readable(candidate.log_offset, visible[&bucket], start[&bucket]) {
            let previous = selected.entry(bucket).or_insert(candidate);
            if candidate.snapshot_id > previous.snapshot_id {
                *previous = candidate;
            }
        }
    }
    for bucket in &mut snapshots.latest_snapshots {
        let snapshot = selected.get(&bucket.bucket_id);
        if snapshot.is_none() && start[&bucket.bucket_id] > 0 {
            // TODO: Improve this rare case. More retained snapshots and a longer log TTL
            // can preserve the history needed to start a full read.
            return Err(Error::UnexpectedError {
                message: format!("No readable KV snapshot and complete changelog history for bucket {}. Increase kv.snapshot.num-retained and log TTL.", bucket.bucket_id),
                source: None,
            });
        }
        bucket.snapshot_id = snapshot.and_then(|snapshot| snapshot.snapshot_id);
        bucket.log_offset = snapshot.and_then(|snapshot| snapshot.log_offset);
    }
    Ok(snapshots)
}

fn group_stats_requests_by_leader(
    cluster: &Cluster,
    table_id: TableId,
    buckets: Vec<BucketStatsRequest>,
) -> Result<HashMap<i32, Vec<BucketStatsRequest>>> {
    let mut buckets_by_leader: HashMap<i32, Vec<BucketStatsRequest>> = HashMap::new();
    for bucket in buckets {
        let table_bucket =
            TableBucket::new_with_partition(table_id, bucket.partition_id, bucket.bucket_id);
        let leader = cluster.leader_for(&table_bucket).ok_or_else(|| {
            Error::leader_not_available(format!(
                "No leader found for table bucket: {table_bucket}."
            ))
        })?;
        buckets_by_leader
            .entry(leader.id())
            .or_default()
            .push(bucket);
    }
    Ok(buckets_by_leader)
}

/// Gives the table's bucket count to partitions from a server that sent none, which is only
/// correct while the table has never changed `bucket.num`.
fn fill_bucket_counts(
    partitions: Vec<PartitionInfo>,
    table_info: &TableInfo,
) -> Result<Vec<PartitionInfo>> {
    if table_info.get_bucket_count_epoch() > 0 {
        return Err(Error::UnexpectedError {
            message: format!(
                "The server sent no partition bucket counts for {}, whose bucket count changed.",
                table_info.table_path
            ),
            source: None,
        });
    }
    Ok(partitions
        .into_iter()
        .map(|partition| match partition.get_bucket_count() {
            0 => partition.with_bucket_count(table_info.num_buckets),
            _ => partition,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::{fill_bucket_counts, group_stats_requests_by_leader, parse_table_descriptor};
    use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
    use crate::error::{Error, FlussError};
    use crate::metadata::{
        BucketStatsRequest, PartitionInfo, PhysicalTablePath, ResolvedPartitionSpec, TableBucket,
        TablePath,
    };
    use crate::test_utils::build_table_info;
    use std::collections::HashMap;
    use std::sync::Arc;

    const TABLE_ID: i64 = 1;

    #[tokio::test]
    async fn test_snapshot_discovery_public_entry() {
        use crate::client::metadata::Metadata;
        use crate::rpc::RpcClient;
        use crate::test_utils::build_cluster_with_port;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;
        use tokio::net::TcpListener;
        for (sync, visible, expected, history_queries) in [
            (Some(true), 100, 2, 1),
            (Some(true), 120, 3, 0),
            (Some(false), 0, 3, 0),
            (None, 0, 3, 0),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let node = crate::proto::PbServerNode {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: port.into(),
                ..Default::default()
            };
            let metadata = crate::proto::MetadataResponse {
                coordinator_server: Some(crate::proto::PbServerNode {
                    node_id: 0,
                    ..node.clone()
                }),
                tablet_servers: vec![node],
                ..Default::default()
            };
            let path = TablePath::new("db", "snapshot_table");
            let cached = build_cluster_with_port(&path, TABLE_ID, 1, port.into());
            let cluster = Cluster::from_metadata_response(metadata.clone(), Some(&cached)).unwrap();
            let history = Arc::new(AtomicUsize::new(0));
            let count = history.clone();
            let server = tokio::spawn(async move {
                let mut connections = tokio::task::JoinSet::new();
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    connections.spawn(serve_snapshot_requests(
                        stream,
                        metadata.clone(),
                        sync,
                        visible,
                        count.clone(),
                    ));
                }
            });
            let admin = super::FlussAdmin::new(
                Arc::new(RpcClient::new().with_timeout(Duration::from_secs(2))),
                Arc::new(Metadata::new_for_test(Arc::new(cluster))),
            );
            let selected = tokio::time::timeout(
                Duration::from_secs(10),
                admin.get_latest_kv_snapshots(&path, None),
            )
            .await;
            server.abort();
            let _ = server.await;
            let selected = selected.unwrap().unwrap();
            assert_eq!(selected.latest_snapshots[0].snapshot_id, Some(expected));
            assert_eq!(history.load(Ordering::SeqCst), history_queries);
        }
    }

    async fn serve_snapshot_requests(
        mut stream: tokio::net::TcpStream,
        metadata: crate::proto::MetadataResponse,
        sync: Option<bool>,
        visible: i64,
        history: Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use crate::proto::{
            ApiVersionsResponse, GetLatestKvSnapshotsResponse, ListKvSnapshotsRequest,
            ListKvSnapshotsResponse, ListOffsetsRequest, ListOffsetsResponse, PbApiVersion,
            PbKvSnapshot, PbListOffsetsRespForBucket,
        };
        use prost::Message;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let Ok(size) = stream.read_i32().await else {
                return;
            };
            let mut request = vec![0; size as usize];
            stream.read_exact(&mut request).await.unwrap();
            let api = i16::from_be_bytes(request[..2].try_into().unwrap());
            let id = i32::from_be_bytes(request[4..8].try_into().unwrap());
            let payload = &request[crate::rpc::message::REQUEST_HEADER_LENGTH..];
            let body = match api {
                1000 => ApiVersionsResponse {
                    api_versions: [1000, 1012, 1021, 1023, 1064]
                        .into_iter()
                        .map(|api_key| PbApiVersion {
                            api_key,
                            min_version: 0,
                            max_version: 0,
                        })
                        .collect(),
                    server_type: None,
                }
                .encode_to_vec(),
                1012 => metadata.encode_to_vec(),
                1023 => GetLatestKvSnapshotsResponse {
                    table_id: TABLE_ID,
                    partition_id: None,
                    has_sync_indexes: sync,
                    latest_snapshots: vec![PbKvSnapshot {
                        bucket_id: 0,
                        snapshot_id: Some(3),
                        log_offset: Some(120),
                    }],
                }
                .encode_to_vec(),
                1021 => {
                    let request = ListOffsetsRequest::decode(payload).unwrap();
                    assert_eq!(request.table_id, TABLE_ID);
                    assert_eq!(request.follower_server_id, -1);
                    ListOffsetsResponse {
                        buckets_resp: vec![PbListOffsetsRespForBucket {
                            bucket_id: 0,
                            offset: Some(if request.offset_type == 1 { visible } else { 0 }),
                            ..Default::default()
                        }],
                    }
                    .encode_to_vec()
                }
                1064 => {
                    let request = ListKvSnapshotsRequest::decode(payload).unwrap();
                    assert_eq!(request.include_log_offsets, Some(true));
                    history.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    ListKvSnapshotsResponse {
                        table_id: TABLE_ID,
                        partition_id: None,
                        active_snapshots: vec![PbKvSnapshot {
                            bucket_id: 0,
                            snapshot_id: Some(2),
                            log_offset: Some(100),
                        }],
                    }
                    .encode_to_vec()
                }
                other => panic!("Unexpected snapshot discovery API {other}"),
            };
            stream.write_i32((5 + body.len()) as i32).await.unwrap();
            stream.write_u8(0).await.unwrap();
            stream.write_i32(id).await.unwrap();
            stream.write_all(&body).await.unwrap();
        }
    }

    #[test]
    fn test_snapshot_selection_requires_visibility_and_complete_wal() {
        use crate::metadata::{KvSnapshot, LatestKvSnapshots};
        use crate::proto::PbKvSnapshot;
        let candidates: Vec<_> = (1..=3)
            .map(|id| PbKvSnapshot {
                bucket_id: 0,
                snapshot_id: Some(id),
                log_offset: Some(60 + 20 * id),
            })
            .collect();
        for (visible, start, expected) in [
            (100, 0, Some(2)),
            (90, 0, Some(1)),
            (70, 0, None),
            (100, 90, Some(2)),
        ] {
            let snapshots = LatestKvSnapshots {
                table_id: TABLE_ID,
                partition_id: None,
                latest_snapshots: vec![KvSnapshot {
                    bucket_id: 0,
                    snapshot_id: Some(3),
                    log_offset: Some(120),
                }],
            };
            let selected = super::select_readable_snapshots(
                snapshots.clone(),
                &candidates,
                &HashMap::from([(0, visible)]),
                &HashMap::from([(0, start)]),
            )
            .unwrap();
            assert_eq!(selected.latest_snapshots[0].snapshot_id, expected);
            assert!(super::select_readable_snapshots(
                snapshots.clone(),
                &candidates,
                &HashMap::from([(0, 90)]),
                &HashMap::from([(0, 81)]),
            )
            .is_err());
            if expected.is_none() {
                assert!(super::select_readable_snapshots(
                    snapshots,
                    &candidates,
                    &HashMap::from([(0, visible)]),
                    &HashMap::from([(0, 1)])
                )
                .is_err());
            }
        }
    }

    fn cluster(leaders: [Option<i32>; 4]) -> Cluster {
        let table_path = TablePath::new("db", "tbl");
        let physical_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let servers: HashMap<i32, ServerNode> = (1..=2)
            .map(|id| {
                let node =
                    ServerNode::new(id, "127.0.0.1".to_string(), 9000, ServerType::TabletServer);
                (id, node)
            })
            .collect();
        let locations: Vec<BucketLocation> = leaders
            .iter()
            .enumerate()
            .map(|(bucket, leader)| {
                BucketLocation::new(
                    TableBucket::new(TABLE_ID, bucket as i32),
                    leader.map(|id| servers[&id].clone()),
                    Arc::clone(&physical_path),
                )
            })
            .collect();
        Cluster::new(
            None,
            servers,
            HashMap::from([(physical_path, locations)]),
            HashMap::from([(table_path.clone(), TABLE_ID)]),
            HashMap::from([(
                table_path.clone(),
                build_table_info(table_path, TABLE_ID, 4),
            )]),
            HashMap::new(),
            HashMap::new(),
        )
    }

    fn all_buckets() -> Vec<BucketStatsRequest> {
        (0..4)
            .map(|bucket| BucketStatsRequest::new(None, bucket))
            .collect()
    }

    #[test]
    fn table_stats_requests_are_grouped_by_bucket_leader() {
        let cluster = cluster([Some(1), Some(2), Some(1), Some(2)]);
        let grouped = group_stats_requests_by_leader(&cluster, TABLE_ID, all_buckets()).unwrap();
        let bucket_ids = |leader: i32| -> Vec<i32> {
            grouped[&leader]
                .iter()
                .map(|bucket| bucket.bucket_id)
                .collect()
        };
        assert_eq!(grouped.len(), 2);
        assert_eq!(bucket_ids(1), vec![0, 2]);
        assert_eq!(bucket_ids(2), vec![1, 3]);
    }

    #[test]
    fn a_bucket_without_a_leader_fails_the_table_stats_request() {
        let cluster = cluster([Some(1), None, Some(1), Some(1)]);
        let error = group_stats_requests_by_leader(&cluster, TABLE_ID, all_buckets()).unwrap_err();
        assert_eq!(
            error.api_error(),
            Some(FlussError::LeaderNotAvailableException)
        );
    }

    fn partition(id: i64, bucket_count: i32) -> PartitionInfo {
        let spec = ResolvedPartitionSpec::new(Arc::from(["p".to_string()]), vec![id.to_string()])
            .expect("spec");
        PartitionInfo::new(id, spec).with_bucket_count(bucket_count)
    }

    #[test]
    fn partitions_without_a_count_get_the_table_count_while_never_rescaled() {
        let table_info = build_table_info(TablePath::new("db", "tbl"), 1, 4);
        let filled = fill_bucket_counts(vec![partition(1, 0), partition(2, 8)], &table_info)
            .expect("filled");
        let counts: Vec<i32> = filled.iter().map(|p| p.get_bucket_count()).collect();
        assert_eq!(counts, vec![4, 8]);

        let rescaled = table_info.with_bucket_count_epoch(1);
        assert!(fill_bucket_counts(vec![partition(1, 0)], &rescaled).is_err());
    }

    #[test]
    fn test_parse_table_descriptor_rejects_malformed_json() {
        let error = parse_table_descriptor(b"{not valid json").unwrap_err();

        assert!(matches!(
            error,
            Error::JsonSerdeError { message }
                if message.starts_with("Failed to parse table_json:")
        ));
    }
}
