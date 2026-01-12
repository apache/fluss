/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.KvSnapshotNotExistException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.NonPrimaryKeyTableException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.SecurityDisabledException;
import org.apache.fluss.exception.SecurityTokenException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.DatabaseExistsRequest;
import org.apache.fluss.rpc.messages.DatabaseExistsResponse;
import org.apache.fluss.rpc.messages.DescribeClusterConfigsRequest;
import org.apache.fluss.rpc.messages.DescribeClusterConfigsResponse;
import org.apache.fluss.rpc.messages.GetDatabaseInfoRequest;
import org.apache.fluss.rpc.messages.GetDatabaseInfoResponse;
import org.apache.fluss.rpc.messages.GetFileSystemSecurityTokenRequest;
import org.apache.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import org.apache.fluss.rpc.messages.GetKvSnapshotMetadataRequest;
import org.apache.fluss.rpc.messages.GetKvSnapshotMetadataResponse;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsRequest;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.GetLatestLakeSnapshotRequest;
import org.apache.fluss.rpc.messages.GetLatestLakeSnapshotResponse;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.GetTableSchemaRequest;
import org.apache.fluss.rpc.messages.GetTableSchemaResponse;
import org.apache.fluss.rpc.messages.ListAclsRequest;
import org.apache.fluss.rpc.messages.ListAclsResponse;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.ListDatabasesResponse;
import org.apache.fluss.rpc.messages.ListPartitionInfosRequest;
import org.apache.fluss.rpc.messages.ListPartitionInfosResponse;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.messages.TableExistsRequest;
import org.apache.fluss.rpc.messages.TableExistsResponse;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.coordinator.CoordinatorService;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.metadata.MetadataProvider;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.tablet.TabletService;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toAclFilter;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toResolvedPartitionSpec;
import static org.apache.fluss.security.acl.Resource.TABLE_SPLITTER;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.buildMetadataResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeGetLatestKvSnapshotsResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeGetLatestLakeSnapshotResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeKvSnapshotMetadataResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeListAclsResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toGetFileSystemSecurityTokenResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toListPartitionInfosResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toPbConfigEntries;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTablePath;
import static org.apache.fluss.utils.Preconditions.checkState;

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
    protected final MetadataManager metadataManager;
    protected final @Nullable Authorizer authorizer;
    protected final DynamicConfigManager dynamicConfigManager;

    private long tokenLastUpdateTimeMs = 0;
    private ObtainedSecurityToken securityToken = null;

    private final ExecutorService ioExecutor;

    public RpcServiceBase(
            FileSystem remoteFileSystem,
            ServerType provider,
            ZooKeeperClient zkClient,
            MetadataManager metadataManager,
            @Nullable Authorizer authorizer,
            DynamicConfigManager dynamicConfigManager,
            ExecutorService ioExecutor) {
        this.remoteFileSystem = remoteFileSystem;
        this.provider = provider;
        this.apiManager = new ApiManager(provider);
        this.zkClient = zkClient;
        this.metadataManager = metadataManager;
        this.authorizer = authorizer;
        this.dynamicConfigManager = dynamicConfigManager;
        this.ioExecutor = ioExecutor;
    }

    @Override
    public ServerType providerType() {
        return provider;
    }

    public abstract void authorizeTable(OperationType operationType, long tableId);

    public void authorizeDatabase(OperationType operationType, String databaseName) {
        if (authorizer != null) {
            authorizer.authorize(currentSession(), operationType, Resource.database(databaseName));
        }
    }

    public void authorizeTable(OperationType operationType, TablePath tablePath) {
        if (authorizer != null) {
            authorizer.authorize(currentSession(), operationType, Resource.table(tablePath));
        }
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
        Collection<String> databaseNames = metadataManager.listDatabases();

        if (authorizer != null) {
            Collection<Resource> authorizedDatabase =
                    authorizer.filterByAuthorized(
                            currentSession(),
                            OperationType.DESCRIBE,
                            databaseNames.stream()
                                    .map(Resource::database)
                                    .collect(Collectors.toList()));
            databaseNames =
                    authorizedDatabase.stream().map(Resource::getName).collect(Collectors.toList());
        }

        response.addAllDatabaseNames(databaseNames);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetDatabaseInfoResponse> getDatabaseInfo(
            GetDatabaseInfoRequest request) {
        String databaseName = request.getDatabaseName();
        authorizeDatabase(OperationType.DESCRIBE, databaseName);

        GetDatabaseInfoResponse response = new GetDatabaseInfoResponse();
        DatabaseInfo databaseInfo = metadataManager.getDatabase(databaseName);
        response.setDatabaseJson(databaseInfo.getDatabaseDescriptor().toJsonBytes())
                .setCreatedTime(databaseInfo.getCreatedTime())
                .setModifiedTime(databaseInfo.getModifiedTime());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<DatabaseExistsResponse> databaseExists(DatabaseExistsRequest request) {
        // By design: database exists not need to check database authorization.
        DatabaseExistsResponse response = new DatabaseExistsResponse();
        boolean exists = metadataManager.databaseExists(request.getDatabaseName());
        response.setExists(exists);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
        ListTablesResponse response = new ListTablesResponse();
        List<String> tableNames = metadataManager.listTables(request.getDatabaseName());
        if (authorizer != null) {
            List<Resource> resources =
                    tableNames.stream()
                            .map(t -> Resource.table(request.getDatabaseName(), t))
                            .collect(Collectors.toList());
            Collection<Resource> authorizedTable =
                    authorizer.filterByAuthorized(
                            currentSession(), OperationType.DESCRIBE, resources);
            tableNames =
                    authorizedTable.stream()
                            .map(resource -> resource.getName().split(TABLE_SPLITTER)[1])
                            .collect(Collectors.toList());
        }

        response.addAllTableNames(tableNames);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        authorizeTable(OperationType.DESCRIBE, tablePath);

        GetTableInfoResponse response = new GetTableInfoResponse();
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        response.setTableJson(tableInfo.toTableDescriptor().toJsonBytes())
                .setSchemaId(tableInfo.getSchemaId())
                .setTableId(tableInfo.getTableId())
                .setCreatedTime(tableInfo.getCreatedTime())
                .setModifiedTime(tableInfo.getModifiedTime());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetTableSchemaResponse> getTableSchema(GetTableSchemaRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        authorizeTable(OperationType.DESCRIBE, tablePath);

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
        // By design: table exists not need to check table authorization.
        TableExistsResponse response = new TableExistsResponse();
        boolean exists = metadataManager.tableExists(toTablePath(request.getTablePath()));
        response.setExists(exists);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetLatestKvSnapshotsResponse> getLatestKvSnapshots(
            GetLatestKvSnapshotsRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        authorizeTable(OperationType.DESCRIBE, tablePath);

        // get table info
        TableInfo tableInfo = metadataManager.getTable(tablePath);

        boolean hasPrimaryKey = tableInfo.hasPrimaryKey();
        boolean hasPartitionName = request.hasPartitionName();
        boolean isPartitioned = tableInfo.isPartitioned();
        if (!hasPrimaryKey) {
            throw new NonPrimaryKeyTableException(
                    "Table '"
                            + tablePath
                            + "' is not a primary key table, so it doesn't have any kv snapshots.");
        } else if (hasPartitionName && !isPartitioned) {
            throw new TableNotPartitionedException(
                    "Table '" + tablePath + "' is not a partitioned table.");
        } else if (!hasPartitionName && isPartitioned) {
            throw new PartitionNotExistException(
                    "Table '"
                            + tablePath
                            + "' is a partitioned table, but partition name is not provided.");
        }

        try {
            // get table id
            long tableId = tableInfo.getTableId();
            int numBuckets = tableInfo.getNumBuckets();
            Long partitionId =
                    hasPartitionName ? getPartitionId(tablePath, request.getPartitionName()) : null;
            Map<Integer, Optional<BucketSnapshot>> snapshots;
            if (partitionId != null) {
                snapshots = zkClient.getPartitionLatestBucketSnapshot(partitionId);
            } else {
                snapshots = zkClient.getTableLatestBucketSnapshot(tableId);
            }
            return CompletableFuture.completedFuture(
                    makeGetLatestKvSnapshotsResponse(tableId, partitionId, snapshots, numBuckets));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long getPartitionId(TablePath tablePath, String partitionName) {
        Optional<TablePartition> optTablePartition;
        try {
            optTablePartition = zkClient.getPartition(tablePath, partitionName);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to get latest kv snapshots for table '%s'", tablePath),
                    e);
        }
        if (!optTablePartition.isPresent()) {
            throw new PartitionNotExistException(
                    String.format(
                            "The partition '%s' of table '%s' does not exist.",
                            partitionName, tablePath));
        }
        return optTablePartition.get().getPartitionId();
    }

    @Override
    public CompletableFuture<GetKvSnapshotMetadataResponse> getKvSnapshotMetadata(
            GetKvSnapshotMetadataRequest request) {
        long tableId = request.getTableId();
        authorizeTable(OperationType.DESCRIBE, tableId);

        TableBucket tableBucket =
                new TableBucket(
                        tableId,
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId());
        long snapshotId = request.getSnapshotId();

        Optional<BucketSnapshot> snapshot;
        try {
            snapshot = zkClient.getTableBucketSnapshot(tableBucket, snapshotId);
            checkState(snapshot.isPresent(), "Kv snapshot not found");
            CompletedSnapshot completedSnapshot =
                    snapshot.get().toCompletedSnapshotHandle().retrieveCompleteSnapshot();
            return CompletableFuture.completedFuture(
                    makeKvSnapshotMetadataResponse(completedSnapshot));
        } catch (Exception e) {
            throw new KvSnapshotNotExistException(
                    String.format(
                            "Failed to get kv snapshot metadata for table bucket %s and snapshot id %s. Error: %s",
                            tableBucket, snapshotId, e.getMessage()));
        }
    }

    @Override
    public CompletableFuture<GetFileSystemSecurityTokenResponse> getFileSystemSecurityToken(
            GetFileSystemSecurityTokenRequest request) {
        // TODO: add ACL for per-table in https://github.com/apache/fluss/issues/752
        try {
            // In order to avoid repeatedly obtaining security token, cache it for a while.
            long currentTimeMs = System.currentTimeMillis();
            if (securityToken == null
                    || currentTimeMs - tokenLastUpdateTimeMs > TOKEN_EXPIRATION_TIME_MS) {
                securityToken = remoteFileSystem.obtainSecurityToken();
                tokenLastUpdateTimeMs = currentTimeMs;
            }

            return CompletableFuture.completedFuture(
                    toGetFileSystemSecurityTokenResponse(
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
        authorizeTable(OperationType.DESCRIBE, tablePath);

        Map<String, Long> partitionNameAndIds;
        if (request.hasPartialPartitionSpec()) {
            ResolvedPartitionSpec partitionSpecFromRequest =
                    toResolvedPartitionSpec(request.getPartialPartitionSpec());
            partitionNameAndIds =
                    metadataManager.listPartitions(tablePath, partitionSpecFromRequest);
        } else {
            partitionNameAndIds = metadataManager.listPartitions(tablePath);
        }
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        return CompletableFuture.completedFuture(
                toListPartitionInfosResponse(partitionKeys, partitionNameAndIds));
    }

    @Override
    public CompletableFuture<GetLatestLakeSnapshotResponse> getLatestLakeSnapshot(
            GetLatestLakeSnapshotRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        authorizeTable(OperationType.DESCRIBE, tablePath);

        // get table info
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        // get table id
        long tableId = tableInfo.getTableId();
        CompletableFuture<GetLatestLakeSnapshotResponse> resultFuture = new CompletableFuture<>();
        ioExecutor.execute(
                () -> {
                    Optional<LakeTableSnapshot> optLakeTableSnapshot;
                    try {
                        optLakeTableSnapshot = zkClient.getLakeTableSnapshot(tableId);
                        if (!optLakeTableSnapshot.isPresent()) {
                            resultFuture.completeExceptionally(
                                    new LakeTableSnapshotNotExistException(
                                            String.format(
                                                    "Lake table snapshot not exist for table: %s, table id: %d",
                                                    tablePath, tableId)));
                        } else {
                            LakeTableSnapshot lakeTableSnapshot = optLakeTableSnapshot.get();
                            resultFuture.complete(
                                    makeGetLatestLakeSnapshotResponse(tableId, lakeTableSnapshot));
                        }
                    } catch (Exception e) {
                        resultFuture.completeExceptionally(
                                new FlussRuntimeException(
                                        String.format(
                                                "Failed to get lake table snapshot for table: %s, table id: %d",
                                                tablePath, tableId),
                                        e));
                    }
                });
        return resultFuture;
    }

    @Override
    public CompletableFuture<ListAclsResponse> listAcls(ListAclsRequest request) {
        if (authorizer == null) {
            throw new SecurityDisabledException("No Authorizer is configured.");
        }
        AclBindingFilter aclBindingFilter = toAclFilter(request.getAclFilter());
        try {
            Collection<AclBinding> acls = authorizer.listAcls(currentSession(), aclBindingFilter);
            return CompletableFuture.completedFuture(makeListAclsResponse(acls));
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to list acls for resource: %s", aclBindingFilter), e);
        }
    }

    @Override
    public CompletableFuture<DescribeClusterConfigsResponse> describeClusterConfigs(
            DescribeClusterConfigsRequest request) {
        if (authorizer != null) {
            authorizer.authorize(currentSession(), OperationType.DESCRIBE, Resource.cluster());
        }

        List<ConfigEntry> configs = dynamicConfigManager.describeConfigs();
        return CompletableFuture.completedFuture(
                new DescribeClusterConfigsResponse().addAllConfigs(toPbConfigEntries(configs)));
    }

    protected MetadataResponse processMetadataRequest(
            MetadataRequest request,
            String listenerName,
            Session session,
            Authorizer authorizer,
            ServerMetadataCache metadataCache,
            MetadataProvider metadataProvider) {
        List<TablePath> authorizedTables = new ArrayList<>();
        for (PbTablePath pbTablePath : request.getTablePathsList()) {
            if (authorizer == null
                    || authorizer.isAuthorized(
                            session,
                            OperationType.DESCRIBE,
                            Resource.table(
                                    pbTablePath.getDatabaseName(), pbTablePath.getTableName()))) {
                authorizedTables.add(ServerRpcMessageUtils.toTablePath(pbTablePath));
            }
        }
        List<TableMetadata> tablesMetadata = new ArrayList<>();
        List<TablePath> unknownTables = new ArrayList<>();
        for (TablePath tablePath : authorizedTables) {
            Optional<TableMetadata> metadataFromCache =
                    metadataProvider.getTableMetadataFromCache(tablePath);
            if (metadataFromCache.isPresent()) {
                tablesMetadata.add(metadataFromCache.get());
            } else {
                unknownTables.add(tablePath);
            }
        }
        // fetch unknown table metadata from ZK
        tablesMetadata.addAll(metadataProvider.getTablesMetadataFromZK(unknownTables));

        // handle partition ids request
        Set<PhysicalTablePath> partitionPaths =
                request.getPartitionsPathsList().stream()
                        .map(ServerRpcMessageUtils::toPhysicalTablePath)
                        .collect(Collectors.toSet());
        long[] partitionIds = request.getPartitionsIds();
        List<Long> partitionIdsNotExistsInCache = new ArrayList<>();
        for (long partitionId : partitionIds) {
            Optional<PhysicalTablePath> physicalTablePath =
                    metadataProvider.getPhysicalTablePathFromCache(partitionId);
            if (physicalTablePath.isPresent()) {
                partitionPaths.add(physicalTablePath.get());
            } else {
                partitionIdsNotExistsInCache.add(partitionId);
            }
        }

        if (!partitionIdsNotExistsInCache.isEmpty()) {
            Map<Long, PhysicalTablePath> partitionIdAndPaths;
            try {
                partitionIdAndPaths = zkClient.getPartitionIdAndPaths(authorizedTables);
            } catch (Exception e) {
                throw new FlussRuntimeException("Failed to get partition paths from ZK.", e);
            }
            for (long partitionId : partitionIdsNotExistsInCache) {
                if (partitionIdAndPaths.containsKey(partitionId)) {
                    partitionPaths.add(partitionIdAndPaths.get(partitionId));
                } else {
                    throw new PartitionNotExistException(
                            String.format(
                                    "The partition id '%d' does not exist or you don't have permission to access it.",
                                    partitionId));
                }
            }
        }

        // collect partition metadata
        List<PhysicalTablePath> authorizedPartitions = new ArrayList<>();
        for (PhysicalTablePath path : partitionPaths) {
            if (authorizer == null
                    || authorizer.isAuthorized(
                            session,
                            OperationType.DESCRIBE,
                            Resource.table(path.getDatabaseName(), path.getTableName()))) {
                authorizedPartitions.add(path);
            }
        }
        List<PartitionMetadata> partitionsMetadata = new ArrayList<>();
        List<PhysicalTablePath> unknownPartitions = new ArrayList<>();
        for (PhysicalTablePath partitionPath : authorizedPartitions) {
            Optional<PartitionMetadata> metadataFromCache =
                    metadataProvider.getPartitionMetadataFromCache(partitionPath);
            if (metadataFromCache.isPresent()) {
                partitionsMetadata.add(metadataFromCache.get());
            } else {
                unknownPartitions.add(partitionPath);
            }
        }
        // fetch unknown partition metadata from ZK
        partitionsMetadata.addAll(metadataProvider.getPartitionsMetadataFromZK(unknownPartitions));

        // build response
        ServerNode coordinatorServer = metadataCache.getCoordinatorServer(listenerName);
        Set<ServerNode> aliveTabletServers =
                new HashSet<>(metadataCache.getAllAliveTabletServers(listenerName).values());
        return buildMetadataResponse(
                coordinatorServer, aliveTabletServers, tablesMetadata, partitionsMetadata);
    }
}
