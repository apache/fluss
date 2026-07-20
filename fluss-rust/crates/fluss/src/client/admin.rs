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
use crate::cluster::ServerNode;
use crate::metadata::{
    AclFilter, AclInfo, AcquireKvSnapshotLeaseResult, ActiveKvSnapshots, AlterConfig,
    AlterTableChanges, BucketStatsRequest, ClusterHealth, CreateAclResult, DatabaseDescriptor,
    DatabaseInfo, DatabaseSummary, DescribeConfig, DropAclsFilterResult, GoalType, JsonSerde,
    KvSnapshotLeaseForTable, KvSnapshotMetadata, LakeSnapshot, LakeSnapshotInfo, LatestKvSnapshots,
    PartitionInfo, PartitionSpec, PhysicalTablePath, ProducerOffsets, ProducerTableOffsets,
    RebalanceProgress, RegisterProducerResult, RemoteLogManifestEntry, Schema, SchemaInfo,
    ServerTag, TableBucket, TableDescriptor, TableInfo, TablePath, TableStats,
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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::task::JoinHandle;

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
            ..
        } = response;
        let v: &[u8] = &table_json[..];
        let table_descriptor =
            TableDescriptor::deserialize_json(&serde_json::from_slice(v).unwrap())?;
        Ok(TableInfo::of(
            table_path.clone(),
            table_id,
            schema_id,
            table_descriptor,
            created_time,
            modified_time,
        ))
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
        Ok(response.get_partitions_info())
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

        let mut list_offsets_requests = HashMap::new();
        for (leader_id, bucket_ids) in node_for_bucket_list {
            let request =
                ListOffsetsRequest::new(table_id, partition_id, bucket_ids, offset_spec.clone());
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
        let _response = self
            .admin_gateway()
            .await?
            .request(AlterTableRequest::new(
                table_path,
                ignore_if_not_exists,
                changes.config_changes,
                changes.add_columns,
                changes.drop_columns,
                changes.rename_columns,
                changes.modify_columns,
            ))
            .await?;
        Ok(())
    }

    /// Get table statistics for buckets. Pass empty `target_columns` to request stats for all columns.
    pub async fn get_table_stats(
        &self,
        table_id: TableId,
        buckets_req: Vec<BucketStatsRequest>,
        target_columns: Vec<i32>,
    ) -> Result<TableStats> {
        let response = self
            .admin_gateway()
            .await?
            .request(GetTableStatsRequest::new(
                table_id,
                buckets_req,
                target_columns,
            ))
            .await?;
        Ok(TableStats::from_pb(&response))
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
        Ok(LatestKvSnapshots::from_pb(&response))
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
