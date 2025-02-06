/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server;

import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.LakeStorageNotConfiguredException;
import com.alibaba.fluss.exception.LakeTableSnapshotNotExistException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.SecurityTokenException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.exception.TableNotPartitionedException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.lakehouse.LakeStorageInfo;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.RpcGatewayService;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsResponse;
import com.alibaba.fluss.rpc.messages.DescribeLakeStorageRequest;
import com.alibaba.fluss.rpc.messages.DescribeLakeStorageResponse;
import com.alibaba.fluss.rpc.messages.GetDatabaseRequest;
import com.alibaba.fluss.rpc.messages.GetDatabaseResponse;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenRequest;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotResponse;
import com.alibaba.fluss.rpc.messages.GetLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetLakeTableSnapshotResponse;
import com.alibaba.fluss.rpc.messages.GetPartitionSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetPartitionSnapshotResponse;
import com.alibaba.fluss.rpc.messages.GetTableRequest;
import com.alibaba.fluss.rpc.messages.GetTableResponse;
import com.alibaba.fluss.rpc.messages.GetTableSchemaRequest;
import com.alibaba.fluss.rpc.messages.GetTableSchemaResponse;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesResponse;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosRequest;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosResponse;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.ListTablesResponse;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.PbApiVersion;
import com.alibaba.fluss.rpc.messages.PbPhysicalTablePath;
import com.alibaba.fluss.rpc.messages.PbTablePath;
import com.alibaba.fluss.rpc.messages.TableExistsRequest;
import com.alibaba.fluss.rpc.messages.TableExistsResponse;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.rpc.messages.UpdateMetadataResponse;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.server.coordinator.CoordinatorService;
import com.alibaba.fluss.server.coordinator.MetaDataManager;
import com.alibaba.fluss.server.metadata.ClusterMetadataInfo;
import com.alibaba.fluss.server.metadata.PartitionMetadataInfo;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metadata.TableMetadataInfo;
import com.alibaba.fluss.server.tablet.TabletService;
import com.alibaba.fluss.server.utils.LakeStorageUtils;
import com.alibaba.fluss.server.utils.RpcMessageUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.BucketSnapshot;
import com.alibaba.fluss.server.zk.data.LakeTableSnapshot;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.server.utils.RpcMessageUtils.toTablePath;

/**
 * An RPC service basic implementation that implements the common RPC methods of {@link
 * CoordinatorService} and {@link TabletService}.
 */
public abstract class RpcServiceBase extends RpcGatewayService implements AdminReadOnlyGateway {
    private static final Logger LOG = LoggerFactory.getLogger(RpcServiceBase.class);

    private static final long TOKEN_EXPIRATION_TIME_MS = 60 * 1000;

    private final FileSystem remoteFileSystem;
    private final ServerType provider;
    private final ApiManager apiManager;
    protected final ZooKeeperClient zkClient;
    protected final ServerMetadataCache metadataCache;
    protected final MetaDataManager metadataManager;

    private long tokenLastUpdateTimeMs = 0;
    private ObtainedSecurityToken securityToken = null;

    private @Nullable final LakeStorageInfo lakeStorageInfo;

    public RpcServiceBase(
            Configuration config,
            FileSystem remoteFileSystem,
            ServerType provider,
            ZooKeeperClient zkClient,
            ServerMetadataCache metadataCache) {
        this.remoteFileSystem = remoteFileSystem;
        this.provider = provider;
        this.apiManager = new ApiManager(provider);
        this.zkClient = zkClient;
        this.metadataCache = metadataCache;
        this.metadataManager = new MetaDataManager(zkClient);
        this.lakeStorageInfo =
                config.get(ConfigOptions.LAKEHOUSE_STORAGE) != null
                        ? LakeStorageUtils.getLakeStorageInfo(config)
                        : null;
    }

    @Override
    public ServerType providerType() {
        return provider;
    }

    @Override
    public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
        Set<ApiKeys> apiKeys = apiManager.enabledApis();
        List<PbApiVersion> apiVersions = new ArrayList<>();
        for (ApiKeys api : apiKeys) {
            apiVersions.add(
                    new PbApiVersion()
                            .setApiKey(api.id)
                            .setMinVersion(api.lowestSupportedVersion)
                            .setMaxVersion(api.highestSupportedVersion));
        }
        ApiVersionsResponse response = new ApiVersionsResponse();
        response.addAllApiVersions(apiVersions);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<ListDatabasesResponse> listDatabases(ListDatabasesRequest request) {
        ListDatabasesResponse response = new ListDatabasesResponse();
        List<String> databaseNames = metadataManager.listDatabases();
        response.addAllDatabaseNames(databaseNames);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetDatabaseResponse> getDatabase(GetDatabaseRequest request) {
        GetDatabaseResponse response = new GetDatabaseResponse();
        DatabaseInfo databaseInfo = metadataManager.getDatabase(request.getDatabaseName());
        response.setDatabaseJson(databaseInfo.getDatabaseDescriptor().toJsonBytes())
                .setCreatedTime(databaseInfo.getCreatedTime())
                .setModifiedTime(databaseInfo.getModifiedTime());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<DatabaseExistsResponse> databaseExists(DatabaseExistsRequest request) {
        DatabaseExistsResponse response = new DatabaseExistsResponse();
        boolean exists = metadataManager.databaseExists(request.getDatabaseName());
        response.setExists(exists);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
        ListTablesResponse response = new ListTablesResponse();
        List<String> tableNames = metadataManager.listTables(request.getDatabaseName());
        response.addAllTableNames(tableNames);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetTableResponse> getTable(GetTableRequest request) {
        GetTableResponse response = new GetTableResponse();
        TablePath tablePath = toTablePath(request.getTablePath());
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        response.setTableJson(tableInfo.getTableDescriptor().toJsonBytes())
                .setSchemaId(tableInfo.getSchemaId())
                .setTableId(tableInfo.getTableId())
                .setCreatedTime(tableInfo.getCreatedTime())
                .setModifiedTime(tableInfo.getModifiedTime());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetTableSchemaResponse> getTableSchema(GetTableSchemaRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        final SchemaInfo schemaInfo;
        if (request.hasSchemaId()) {
            schemaInfo = metadataManager.getSchemaById(tablePath, request.getSchemaId());
        } else {
            schemaInfo = metadataManager.getLatestSchema(tablePath);
        }
        GetTableSchemaResponse response = new GetTableSchemaResponse();
        response.setSchemaId(schemaInfo.getSchemaId());
        response.setSchemaJson(schemaInfo.getSchema().toJsonBytes());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<TableExistsResponse> tableExists(TableExistsRequest request) {
        TableExistsResponse response = new TableExistsResponse();
        boolean exists = metadataManager.tableExists(toTablePath(request.getTablePath()));
        response.setExists(exists);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        Set<ServerNode> aliveTableServers = getAllTabletServerNodes();
        List<PbTablePath> pbTablePaths = request.getTablePathsList();
        List<TablePath> tablePaths = new ArrayList<>(pbTablePaths.size());

        List<PbPhysicalTablePath> partitions = request.getPartitionsPathsList();
        long[] partitionIds = request.getPartitionsIds();

        List<TableMetadataInfo> tableMetadataInfos = new ArrayList<>();
        List<PartitionMetadataInfo> partitionMetadataInfos = new ArrayList<>();

        for (PbTablePath pbTablePath : pbTablePaths) {
            TablePath tablePath = toTablePath(pbTablePath);
            tablePaths.add(tablePath);
            tableMetadataInfos.add(getTableMetadata(tablePath));
        }

        for (PbPhysicalTablePath partitionPath : partitions) {
            partitionMetadataInfos.add(
                    getPartitionMetadata(RpcMessageUtils.toPhysicalTablePath(partitionPath)));
        }

        // get partition info from partition ids
        partitionMetadataInfos.addAll(getPartitionMetadata(tablePaths, partitionIds));

        return CompletableFuture.completedFuture(
                new ClusterMetadataInfo(
                                metadataCache.getCoordinatorServer() == null
                                        ? Optional.empty()
                                        : Optional.of(metadataCache.getCoordinatorServer()),
                                aliveTableServers,
                                tableMetadataInfos,
                                partitionMetadataInfos)
                        .toMetadataResponse());
    }

    @Override
    public CompletableFuture<UpdateMetadataResponse> updateMetadata(UpdateMetadataRequest request) {
        UpdateMetadataResponse updateMetadataResponse = new UpdateMetadataResponse();
        metadataCache.updateMetadata(ClusterMetadataInfo.fromUpdateMetadataRequest(request));
        return CompletableFuture.completedFuture(updateMetadataResponse);
    }

    @Override
    public CompletableFuture<GetKvSnapshotResponse> getKvSnapshot(GetKvSnapshotRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());

        // get table info
        TableInfo tableInfo = metadataManager.getTable(tablePath);

        // get table id
        long tableId = tableInfo.getTableId();
        try {
            // get latest snapshots
            Optional<Map<Integer, Optional<BucketSnapshot>>> optBucketSnapshots =
                    zkClient.getTableLatestBucketSnapshot(tableId);
            if (!optBucketSnapshots.isPresent()) {
                // if bucket snapshot info is not present, it should be the table assignment has
                // been deleted from zk, throw TableNotExistException
                throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
            }
            return CompletableFuture.completedFuture(
                    RpcMessageUtils.makeGetKvSnapshotResponse(tableId, optBucketSnapshots.get()));
        } catch (TableNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to get table snapshot for table id %s", tableId), e);
        }
    }

    @Override
    public CompletableFuture<GetFileSystemSecurityTokenResponse> getFileSystemSecurityToken(
            GetFileSystemSecurityTokenRequest request) {
        try {
            // In order to avoid repeatedly obtaining security token, cache it for a while.
            long currentTimeMs = System.currentTimeMillis();
            if (securityToken == null
                    || currentTimeMs - tokenLastUpdateTimeMs > TOKEN_EXPIRATION_TIME_MS) {
                securityToken = remoteFileSystem.obtainSecurityToken();
                tokenLastUpdateTimeMs = currentTimeMs;
            }

            return CompletableFuture.completedFuture(
                    RpcMessageUtils.toGetFileSystemSecurityTokenResponse(
                            remoteFileSystem.getUri().getScheme(), securityToken));
        } catch (Exception e) {
            throw new SecurityTokenException(
                    "Failed to get file access security token: " + e.getMessage());
        }
    }

    @Override
    public CompletableFuture<ListPartitionInfosResponse> listPartitionInfos(
            ListPartitionInfosRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        Map<String, Long> partitionNameAndIds = metadataManager.listPartitions(tablePath);
        return CompletableFuture.completedFuture(
                RpcMessageUtils.toListPartitionInfosResponse(partitionNameAndIds));
    }

    @Override
    public CompletableFuture<GetPartitionSnapshotResponse> getPartitionSnapshot(
            GetPartitionSnapshotRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());

        // get table info
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        if (!tableInfo.getTableDescriptor().isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Table '" + tablePath + "' is not a partitioned table.");
        }
        long tableId = tableInfo.getTableId();
        String partitionName = request.getPartitionName();
        Map<Long, Map<Integer, Optional<BucketSnapshot>>> partitionBucketsSnapshots =
                new HashMap<>();
        try {
            Optional<TablePartition> optTablePartition =
                    zkClient.getPartition(tablePath, partitionName);
            if (!optTablePartition.isPresent()) {
                // if bucket snapshot info is not present, it should be the partition assignment
                // has been deleted from zk, throw PartitionNotExistException
                throw new PartitionNotExistException(
                        String.format(
                                "The partition '%s' of table %s does not exist.",
                                partitionName, tablePath));
            }

            long partitionId = optTablePartition.get().getPartitionId();
            Optional<Map<Integer, Optional<BucketSnapshot>>> optBucketSnapshots =
                    zkClient.getPartitionLatestBucketSnapshot(partitionId);
            if (!optBucketSnapshots.isPresent()) {
                // if bucket snapshot info is not present, it should be the partition assignment
                // has been deleted from zk, throw PartitionNotExistException
                throw new PartitionNotExistException(
                        String.format(
                                "The partition '%s' of table %s does not exist. Can't find assignment for the partition id %d in zk.",
                                partitionName, tablePath, partitionId));
            }
            return CompletableFuture.completedFuture(
                    RpcMessageUtils.makeGetTablePartitionSnapshotResponse(
                            tableId, partitionId, optBucketSnapshots.get()));
        } catch (PartitionNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to get table partition snapshot for partition %s of table %s",
                            partitionName, tablePath),
                    e);
        }
    }

    @Override
    public CompletableFuture<DescribeLakeStorageResponse> describeLakeStorage(
            DescribeLakeStorageRequest request) {
        if (lakeStorageInfo == null) {
            throw new LakeStorageNotConfiguredException("Lake storage is not configured.");
        }

        return CompletableFuture.completedFuture(
                RpcMessageUtils.makeDescribeLakeStorageResponse(lakeStorageInfo));
    }

    @Override
    public CompletableFuture<GetLakeTableSnapshotResponse> getLakeTableSnapshot(
            GetLakeTableSnapshotRequest request) {
        if (lakeStorageInfo == null) {
            throw new LakeStorageNotConfiguredException("Lake storage is not configured.");
        }

        // get table info
        TablePath tablePath = toTablePath(request.getTablePath());
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        // get table id
        long tableId = tableInfo.getTableId();

        Optional<LakeTableSnapshot> optLakeTableSnapshot;
        try {
            optLakeTableSnapshot = zkClient.getLakeTableSnapshot(tableId);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to get lake table snapshot for table: %s, table id: %d",
                            tablePath, tableId),
                    e);
        }

        if (!optLakeTableSnapshot.isPresent()) {
            throw new LakeTableSnapshotNotExistException(
                    String.format(
                            "Lake table snapshot not exist for table: %s, table id: %d",
                            tablePath, tableId));
        }

        LakeTableSnapshot lakeTableSnapshot = optLakeTableSnapshot.get();
        return CompletableFuture.completedFuture(
                RpcMessageUtils.makeGetLakeTableSnapshotResponse(
                        tableId, lakeStorageInfo, lakeTableSnapshot));
    }

    private Set<ServerNode> getAllTabletServerNodes() {
        return new HashSet<>(metadataCache.getAllAliveTabletServers().values());
    }

    /**
     * Returned a {@link TableMetadataInfo} contains the table info for {@code tablePath} and the
     * bucket locations for {@code physicalTablePaths}.
     */
    private TableMetadataInfo getTableMetadata(TablePath tablePath) {
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        long tableId = tableInfo.getTableId();
        try {
            AssignmentInfo assignmentInfo =
                    getAssignmentInfo(tableId, PhysicalTablePath.of(tablePath));
            List<BucketLocation> bucketLocations = new ArrayList<>();
            if (assignmentInfo.tableAssignment != null) {
                TableAssignment tableAssignment = assignmentInfo.tableAssignment;
                bucketLocations =
                        toBucketLocations(
                                PhysicalTablePath.of(tablePath), tableId, null, tableAssignment);
            } else {
                if (!tableInfo.getTableDescriptor().isPartitioned()) {
                    LOG.warn("No table assignment node found for table {}", tableId);
                }
            }
            return new TableMetadataInfo(tableInfo, bucketLocations);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to get metadata for %s", tablePath), e);
        }
    }

    private PartitionMetadataInfo getPartitionMetadata(PhysicalTablePath partitionPath) {
        try {
            Preconditions.checkNotNull(
                    partitionPath.getPartitionName(),
                    "partitionName must be not null, but get: " + partitionPath);
            AssignmentInfo assignmentInfo = getAssignmentInfo(null, partitionPath);
            List<BucketLocation> bucketLocations = new ArrayList<>();
            Preconditions.checkNotNull(
                    assignmentInfo.partitionId,
                    "partition id must be not null for " + partitionPath);
            if (assignmentInfo.tableAssignment != null) {
                TableAssignment tableAssignment = assignmentInfo.tableAssignment;
                bucketLocations =
                        toBucketLocations(
                                partitionPath,
                                assignmentInfo.tableId,
                                assignmentInfo.partitionId,
                                tableAssignment);
            } else {
                LOG.warn("No partition assignment node found for partition {}", partitionPath);
            }
            return new PartitionMetadataInfo(
                    assignmentInfo.tableId,
                    partitionPath.getPartitionName(),
                    assignmentInfo.partitionId,
                    bucketLocations);
        } catch (PartitionNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to get metadata for partition %s", partitionPath), e);
        }
    }

    private List<PartitionMetadataInfo> getPartitionMetadata(
            Collection<TablePath> tablePaths, long[] partitionIds) {
        // todo: hack logic; currently, we can't get partition metadata by partition ids directly,
        // in here, we always assume the partition ids must belong to the first argument tablePaths;
        // at least, in current client metadata request design, the assumption is true.
        // but the assumption is fragile; we should use metadata cache to help to get partition by
        // partition ids

        List<PartitionMetadataInfo> partitionMetadataInfos = new ArrayList<>();
        Set<Long> partitionIdSet = new HashSet<>();
        for (long partitionId : partitionIds) {
            partitionIdSet.add(partitionId);
        }
        try {
            for (TablePath tablePath : tablePaths) {
                if (partitionIdSet.isEmpty()) {
                    break;
                }
                Set<Long> hitPartitionIds = new HashSet<>();
                // TODO: this is a heavy operation, should be optimized when we have metadata cache
                Map<Long, String> partitionNameById = zkClient.getPartitionIdAndNames(tablePath);
                for (Long partitionId : partitionIdSet) {
                    // the partition is under the table, get the metadata
                    String partitionName = partitionNameById.get(partitionId);
                    if (partitionName != null) {
                        partitionMetadataInfos.add(
                                getPartitionMetadata(
                                        PhysicalTablePath.of(tablePath, partitionName)));
                        hitPartitionIds.add(partitionId);
                    }
                }
                partitionIdSet.removeAll(hitPartitionIds);
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Failed to get metadata for partition ids: " + Arrays.toString(partitionIds),
                    e);
        }
        if (!partitionIdSet.isEmpty()) {
            throw new PartitionNotExistException(
                    "Partition not exist for partition ids: " + partitionIdSet);
        }
        return partitionMetadataInfos;
    }

    private List<BucketLocation> toBucketLocations(
            PhysicalTablePath physicalTablePath,
            long tableId,
            @Nullable Long partitionId,
            TableAssignment tableAssignment)
            throws Exception {
        List<BucketLocation> bucketLocations = new ArrayList<>();
        // iterate each bucket assignment
        for (Map.Entry<Integer, BucketAssignment> assignment :
                tableAssignment.getBucketAssignments().entrySet()) {
            int bucketId = assignment.getKey();
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

            List<Integer> replicas = assignment.getValue().getReplicas();
            ServerNode[] replicaNode = new ServerNode[replicas.size()];
            Map<Integer, ServerNode> nodes = metadataCache.getAllAliveTabletServers();
            for (int i = 0; i < replicas.size(); i++) {
                replicaNode[i] =
                        nodes.getOrDefault(
                                replicas.get(i),
                                // if not in alive node, we set host as ""
                                // and port as -1 just like kafka
                                // TODO: client will not use this node to connect,
                                //  should be removed in the future.
                                new ServerNode(replicas.get(i), "", -1, ServerType.TABLET_SERVER));
            }

            // now get the leader
            Optional<LeaderAndIsr> optLeaderAndIsr = zkClient.getLeaderAndIsr(tableBucket);
            ServerNode leader;
            leader =
                    optLeaderAndIsr
                            .map(
                                    leaderAndIsr ->
                                            metadataCache
                                                    .getAllAliveTabletServers()
                                                    .get(leaderAndIsr.leader()))
                            .orElse(null);
            bucketLocations.add(
                    new BucketLocation(physicalTablePath, tableBucket, leader, replicaNode));
        }
        return bucketLocations;
    }

    private AssignmentInfo getAssignmentInfo(
            @Nullable Long tableId, PhysicalTablePath physicalTablePath) throws Exception {
        // it's a partition, get the partition assignment
        if (physicalTablePath.getPartitionName() != null) {
            Optional<TablePartition> tablePartition =
                    zkClient.getPartition(
                            physicalTablePath.getTablePath(), physicalTablePath.getPartitionName());
            if (!tablePartition.isPresent()) {
                throw new PartitionNotExistException(
                        "Table partition '" + physicalTablePath + "' does not exist.");
            }
            long partitionId = tablePartition.get().getPartitionId();

            return new AssignmentInfo(
                    tablePartition.get().getTableId(),
                    zkClient.getPartitionAssignment(partitionId).orElse(null),
                    partitionId);
        } else {
            Preconditions.checkNotNull(tableId, "tableId must be not null");
            return new AssignmentInfo(
                    tableId, zkClient.getTableAssignment(tableId).orElse(null), null);
        }
    }

    private static class AssignmentInfo {
        private final long tableId;
        // null then the bucket doesn't belong to a partition. Otherwise, not null
        private final @Nullable Long partitionId;
        private final @Nullable TableAssignment tableAssignment;

        private AssignmentInfo(
                long tableId, TableAssignment tableAssignment, @Nullable Long partitionId) {
            this.tableId = tableId;
            this.tableAssignment = tableAssignment;
            this.partitionId = partitionId;
        }
    }
}
