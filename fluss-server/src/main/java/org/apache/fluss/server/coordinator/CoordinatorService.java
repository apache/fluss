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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidCoordinatorException;
import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.LakeTableAlreadyExistException;
import org.apache.fluss.exception.SecurityDisabledException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AdjustIsrRequest;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.rpc.messages.CommitKvSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitKvSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import org.apache.fluss.rpc.messages.ControlledShutdownRequest;
import org.apache.fluss.rpc.messages.ControlledShutdownResponse;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.CreateAclsResponse;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.messages.CreateDatabaseResponse;
import org.apache.fluss.rpc.messages.CreatePartitionRequest;
import org.apache.fluss.rpc.messages.CreatePartitionResponse;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.CreateTableResponse;
import org.apache.fluss.rpc.messages.DropAclsRequest;
import org.apache.fluss.rpc.messages.DropAclsResponse;
import org.apache.fluss.rpc.messages.DropDatabaseRequest;
import org.apache.fluss.rpc.messages.DropDatabaseResponse;
import org.apache.fluss.rpc.messages.DropPartitionRequest;
import org.apache.fluss.rpc.messages.DropPartitionResponse;
import org.apache.fluss.rpc.messages.DropTableRequest;
import org.apache.fluss.rpc.messages.DropTableResponse;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbHeartbeatReqForTable;
import org.apache.fluss.rpc.messages.PbHeartbeatRespForTable;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.RpcServiceBase;
import org.apache.fluss.server.authorizer.AclCreateResult;
import org.apache.fluss.server.authorizer.AclDeleteResult;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import org.apache.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import org.apache.fluss.server.coordinator.event.CommitLakeTableSnapshotEvent;
import org.apache.fluss.server.coordinator.event.CommitRemoteLogManifestEvent;
import org.apache.fluss.server.coordinator.event.ControlledShutdownEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.entity.CommitKvSnapshotData;
import org.apache.fluss.server.entity.LakeTieringTableInfo;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toAclBindingFilters;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toAclBindings;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.fromTablePath;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getAdjustIsrData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getCommitLakeTableSnapshotData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getCommitRemoteLogManifestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getPartitionSpec;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeCreateAclsResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeDropAclsResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTablePath;
import static org.apache.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static org.apache.fluss.utils.PartitionUtils.validatePartitionSpec;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** An RPC Gateway service for coordinator server. */
public final class CoordinatorService extends RpcServiceBase implements CoordinatorGateway {

    private final int defaultBucketNumber;
    private final int defaultReplicationFactor;
    private final boolean logTableAllowCreation;
    private final boolean kvTableAllowCreation;
    private final Supplier<EventManager> eventManagerSupplier;
    private final Supplier<Integer> coordinatorEpochSupplier;
    private final ServerMetadataCache metadataCache;

    // null if the cluster hasn't configured datalake format
    private final @Nullable DataLakeFormat dataLakeFormat;
    private final @Nullable LakeCatalog lakeCatalog;
    private final LakeTableTieringManager lakeTableTieringManager;

    public CoordinatorService(
            Configuration conf,
            FileSystem remoteFileSystem,
            ZooKeeperClient zkClient,
            Supplier<CoordinatorEventProcessor> coordinatorEventProcessorSupplier,
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            @Nullable Authorizer authorizer,
            @Nullable LakeCatalog lakeCatalog,
            LakeTableTieringManager lakeTableTieringManager) {
        super(remoteFileSystem, ServerType.COORDINATOR, zkClient, metadataManager, authorizer);
        this.defaultBucketNumber = conf.getInt(ConfigOptions.DEFAULT_BUCKET_NUMBER);
        this.defaultReplicationFactor = conf.getInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR);
        this.logTableAllowCreation = conf.getBoolean(ConfigOptions.LOG_TABLE_ALLOW_CREATION);
        this.kvTableAllowCreation = conf.getBoolean(ConfigOptions.KV_TABLE_ALLOW_CREATION);
        this.eventManagerSupplier =
                () -> coordinatorEventProcessorSupplier.get().getCoordinatorEventManager();
        this.coordinatorEpochSupplier =
                () -> coordinatorEventProcessorSupplier.get().getCoordinatorEpoch();
        this.dataLakeFormat = conf.getOptional(ConfigOptions.DATALAKE_FORMAT).orElse(null);
        this.lakeCatalog = lakeCatalog;
        this.lakeTableTieringManager = lakeTableTieringManager;
        this.metadataCache = metadataCache;
        checkState(
                (dataLakeFormat == null) == (lakeCatalog == null),
                "dataLakeFormat and lakeCatalog must both be null or both non-null, but dataLakeFormat is %s, lakeCatalog is %s.",
                dataLakeFormat,
                lakeCatalog);
    }

    @Override
    public String name() {
        return "coordinator";
    }

    @Override
    public void shutdown() {
        IOUtils.closeQuietly(lakeCatalog, "lake catalog");
    }

    @Override
    public CompletableFuture<CreateDatabaseResponse> createDatabase(CreateDatabaseRequest request) {
        if (authorizer != null) {
            authorizer.authorize(currentSession(), OperationType.CREATE, Resource.cluster());
        }

        CreateDatabaseResponse response = new CreateDatabaseResponse();
        try {
            TablePath.validateDatabaseName(request.getDatabaseName());
        } catch (InvalidDatabaseException e) {
            return FutureUtils.completedExceptionally(e);
        }

        DatabaseDescriptor databaseDescriptor;
        if (request.getDatabaseJson() != null) {
            databaseDescriptor = DatabaseDescriptor.fromJsonBytes(request.getDatabaseJson());
        } else {
            databaseDescriptor = DatabaseDescriptor.builder().build();
        }
        metadataManager.createDatabase(
                request.getDatabaseName(), databaseDescriptor, request.isIgnoreIfExists());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<DropDatabaseResponse> dropDatabase(DropDatabaseRequest request) {
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.DROP,
                    Resource.database(request.getDatabaseName()));
        }

        DropDatabaseResponse response = new DropDatabaseResponse();
        metadataManager.dropDatabase(
                request.getDatabaseName(), request.isIgnoreIfNotExists(), request.isCascade());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<CreateTableResponse> createTable(CreateTableRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        tablePath.validate();
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.CREATE,
                    Resource.database(tablePath.getDatabaseName()));
        }

        TableDescriptor tableDescriptor;
        try {
            tableDescriptor = TableDescriptor.fromJsonBytes(request.getTableJson());
        } catch (Exception e) {
            if (e instanceof UncheckedIOException) {
                throw new InvalidTableException(
                        "Failed to parse table descriptor: " + e.getMessage());
            } else {
                // wrap the validate message to InvalidTableException
                throw new InvalidTableException(e.getMessage());
            }
        }

        // Check table creation permissions based on table type
        validateTableCreationPermission(tableDescriptor, tablePath);

        // apply system defaults if the config is not set
        tableDescriptor = applySystemDefaults(tableDescriptor);

        // the distribution and bucket count must be set now
        //noinspection OptionalGetWithoutIsPresent
        int bucketCount = tableDescriptor.getTableDistribution().get().getBucketCount().get();

        // first, generate the assignment
        TableAssignment tableAssignment = null;
        // only when it's no partitioned table do we generate the assignment for it
        if (!tableDescriptor.isPartitioned()) {
            // the replication factor must be set now
            int replicaFactor = tableDescriptor.getReplicationFactor();
            TabletServerInfo[] servers = metadataCache.getLiveServers();
            tableAssignment = generateAssignment(bucketCount, replicaFactor, servers);
        }

        // TODO: should tolerate if the lake exist but matches our schema. This ensures eventually
        //  consistent by idempotently creating the table multiple times. See #846
        // before create table in fluss, we may create in lake
        if (isDataLakeEnabled(tableDescriptor)) {
            try {
                checkNotNull(lakeCatalog).createTable(tablePath, tableDescriptor);
            } catch (TableAlreadyExistException e) {
                throw new LakeTableAlreadyExistException(
                        String.format(
                                "The table %s already exists in %s catalog, please "
                                        + "first drop the table in %s catalog or use a new table name.",
                                tablePath, dataLakeFormat, dataLakeFormat));
            }
        }

        // then create table;
        metadataManager.createTable(
                tablePath, tableDescriptor, tableAssignment, request.isIgnoreIfExists());

        return CompletableFuture.completedFuture(new CreateTableResponse());
    }

    private TableDescriptor applySystemDefaults(TableDescriptor tableDescriptor) {
        TableDescriptor newDescriptor = tableDescriptor;

        // not set bucket num
        if (!newDescriptor.getTableDistribution().isPresent()
                || !newDescriptor.getTableDistribution().get().getBucketCount().isPresent()) {
            newDescriptor = newDescriptor.withBucketCount(defaultBucketNumber);
        }

        // not set replication factor
        Map<String, String> properties = newDescriptor.getProperties();
        if (!properties.containsKey(ConfigOptions.TABLE_REPLICATION_FACTOR.key())) {
            newDescriptor = newDescriptor.withReplicationFactor(defaultReplicationFactor);
        }

        // override set num-precreate for auto-partition table with multi-level partition keys
        if (newDescriptor.getPartitionKeys().size() > 1
                && "true".equals(properties.get(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key()))
                && !properties.containsKey(
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key())) {
            Map<String, String> newProperties = new HashMap<>(newDescriptor.getProperties());
            // disable precreate partitions for multi-level partitions.
            newProperties.put(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(), "0");
            newDescriptor = newDescriptor.withProperties(newProperties);
        }

        // override the datalake format if the table hasn't set it and the cluster configured
        if (dataLakeFormat != null
                && !properties.containsKey(ConfigOptions.TABLE_DATALAKE_FORMAT.key())) {
            newDescriptor = newDescriptor.withDataLakeFormat(dataLakeFormat);
        }

        // lake table can only be enabled when the cluster configures datalake format
        boolean dataLakeEnabled = isDataLakeEnabled(tableDescriptor);
        if (dataLakeEnabled && dataLakeFormat == null) {
            throw new InvalidTableException(
                    String.format(
                            "'%s' is enabled for the table, but the Fluss cluster doesn't enable datalake tables.",
                            ConfigOptions.TABLE_DATALAKE_ENABLED.key()));
        }

        return newDescriptor;
    }

    private boolean isDataLakeEnabled(TableDescriptor tableDescriptor) {
        String dataLakeEnabledValue =
                tableDescriptor.getProperties().get(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
        return Boolean.parseBoolean(dataLakeEnabledValue);
    }

    @Override
    public CompletableFuture<DropTableResponse> dropTable(DropTableRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.DROP,
                    Resource.table(tablePath.getDatabaseName(), tablePath.getTableName()));
        }

        DropTableResponse response = new DropTableResponse();
        metadataManager.dropTable(tablePath, request.isIgnoreIfNotExists());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<CreatePartitionResponse> createPartition(
            CreatePartitionRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.WRITE,
                    Resource.table(tablePath.getDatabaseName(), tablePath.getTableName()));
        }

        CreatePartitionResponse response = new CreatePartitionResponse();
        TableRegistration table = metadataManager.getTableRegistration(tablePath);
        if (!table.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Only partitioned table support create partition.");
        }

        // first, validate the partition spec, and get resolved partition spec.
        PartitionSpec partitionSpec = getPartitionSpec(request.getPartitionSpec());
        validatePartitionSpec(tablePath, table.partitionKeys, partitionSpec);
        ResolvedPartitionSpec partitionToCreate =
                ResolvedPartitionSpec.fromPartitionSpec(table.partitionKeys, partitionSpec);

        // second, generate the PartitionAssignment.
        int replicaFactor = table.getTableConfig().getReplicationFactor();
        TabletServerInfo[] servers = metadataCache.getLiveServers();
        Map<Integer, BucketAssignment> bucketAssignments =
                generateAssignment(table.bucketCount, replicaFactor, servers)
                        .getBucketAssignments();
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(table.tableId, bucketAssignments);

        metadataManager.createPartition(
                tablePath,
                table.tableId,
                partitionAssignment,
                partitionToCreate,
                request.isIgnoreIfNotExists());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<DropPartitionResponse> dropPartition(DropPartitionRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.WRITE,
                    Resource.table(tablePath.getDatabaseName(), tablePath.getTableName()));
        }

        DropPartitionResponse response = new DropPartitionResponse();
        TableRegistration table = metadataManager.getTableRegistration(tablePath);
        if (!table.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Only partitioned table support drop partition.");
        }

        // first, validate the partition spec.
        PartitionSpec partitionSpec = getPartitionSpec(request.getPartitionSpec());
        validatePartitionSpec(tablePath, table.partitionKeys, partitionSpec);
        ResolvedPartitionSpec partitionToDrop =
                ResolvedPartitionSpec.fromPartitionSpec(table.partitionKeys, partitionSpec);

        metadataManager.dropPartition(tablePath, partitionToDrop, request.isIgnoreIfNotExists());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        String listenerName = currentListenerName();
        Session session = currentSession();

        AccessContextEvent<MetadataResponse> metadataResponseAccessContextEvent =
                new AccessContextEvent<>(
                        ctx ->
                                makeMetadataResponse(
                                        request,
                                        listenerName,
                                        session,
                                        authorizer,
                                        metadataCache,
                                        (tablePath) -> getTableMetadata(ctx, tablePath),
                                        ctx::getPhysicalTablePath,
                                        (physicalTablePath) ->
                                                getPartitionMetadata(ctx, physicalTablePath)));
        eventManagerSupplier.get().put(metadataResponseAccessContextEvent);
        return metadataResponseAccessContextEvent.getResultFuture();
    }

    public CompletableFuture<AdjustIsrResponse> adjustIsr(AdjustIsrRequest request) {
        CompletableFuture<AdjustIsrResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(new AdjustIsrReceivedEvent(getAdjustIsrData(request), response));
        return response;
    }

    @Override
    public CompletableFuture<CommitKvSnapshotResponse> commitKvSnapshot(
            CommitKvSnapshotRequest request) {
        CompletableFuture<CommitKvSnapshotResponse> response = new CompletableFuture<>();
        // parse completed snapshot from request
        byte[] completedSnapshotBytes = request.getCompletedSnapshot();
        CompletedSnapshot completedSnapshot =
                CompletedSnapshotJsonSerde.fromJson(completedSnapshotBytes);
        CommitKvSnapshotData commitKvSnapshotData =
                new CommitKvSnapshotData(
                        completedSnapshot,
                        request.getCoordinatorEpoch(),
                        request.getBucketLeaderEpoch());
        eventManagerSupplier.get().put(new CommitKvSnapshotEvent(commitKvSnapshotData, response));
        return response;
    }

    @Override
    public CompletableFuture<CommitRemoteLogManifestResponse> commitRemoteLogManifest(
            CommitRemoteLogManifestRequest request) {
        CompletableFuture<CommitRemoteLogManifestResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(
                        new CommitRemoteLogManifestEvent(
                                getCommitRemoteLogManifestData(request), response));
        return response;
    }

    @Override
    public CompletableFuture<CreateAclsResponse> createAcls(CreateAclsRequest request) {
        if (authorizer == null) {
            throw new SecurityDisabledException("No Authorizer is configured.");
        }
        List<AclBinding> aclBindings = toAclBindings(request.getAclsList());
        List<AclCreateResult> aclCreateResults = authorizer.addAcls(currentSession(), aclBindings);
        return CompletableFuture.completedFuture(makeCreateAclsResponse(aclCreateResults));
    }

    @Override
    public CompletableFuture<DropAclsResponse> dropAcls(DropAclsRequest request) {
        if (authorizer == null) {
            throw new SecurityDisabledException("No Authorizer is configured.");
        }
        List<AclBindingFilter> filters = toAclBindingFilters(request.getAclFiltersList());
        List<AclDeleteResult> aclDeleteResults = authorizer.dropAcls(currentSession(), filters);
        return CompletableFuture.completedFuture(makeDropAclsResponse(aclDeleteResults));
    }

    @Override
    public CompletableFuture<CommitLakeTableSnapshotResponse> commitLakeTableSnapshot(
            CommitLakeTableSnapshotRequest request) {
        CompletableFuture<CommitLakeTableSnapshotResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(
                        new CommitLakeTableSnapshotEvent(
                                getCommitLakeTableSnapshotData(request), response));
        return response;
    }

    @Override
    public CompletableFuture<LakeTieringHeartbeatResponse> lakeTieringHeartbeat(
            LakeTieringHeartbeatRequest request) {
        LakeTieringHeartbeatResponse heartbeatResponse = new LakeTieringHeartbeatResponse();
        int currentCoordinatorEpoch = coordinatorEpochSupplier.get();
        heartbeatResponse.setCoordinatorEpoch(currentCoordinatorEpoch);

        // process failed tables
        for (PbHeartbeatReqForTable failedTable : request.getFailedTablesList()) {
            PbHeartbeatRespForTable pbHeartbeatRespForTable =
                    heartbeatResponse.addFailedTableResp().setTableId(failedTable.getTableId());
            try {
                validateHeartbeatRequest(failedTable, currentCoordinatorEpoch);
                lakeTableTieringManager.reportTieringFail(
                        failedTable.getTableId(), failedTable.getTieringEpoch());
            } catch (Throwable e) {
                pbHeartbeatRespForTable.setError(ApiError.fromThrowable(e).toErrorResponse());
            }
        }

        // process tiering tables
        for (PbHeartbeatReqForTable tieringTable : request.getTieringTablesList()) {
            PbHeartbeatRespForTable pbHeartbeatRespForTable =
                    heartbeatResponse.addTieringTableResp().setTableId(tieringTable.getTableId());
            try {
                validateHeartbeatRequest(tieringTable, currentCoordinatorEpoch);
                lakeTableTieringManager.renewTieringHeartbeat(
                        tieringTable.getTableId(), tieringTable.getTieringEpoch());
            } catch (Throwable t) {
                pbHeartbeatRespForTable.setError(ApiError.fromThrowable(t).toErrorResponse());
            }
        }

        // process finished tables
        for (PbHeartbeatReqForTable finishTable : request.getFinishedTablesList()) {
            PbHeartbeatRespForTable pbHeartbeatRespForTable =
                    heartbeatResponse.addFinishedTableResp().setTableId(finishTable.getTableId());
            try {
                validateHeartbeatRequest(finishTable, currentCoordinatorEpoch);
                lakeTableTieringManager.finishTableTiering(
                        finishTable.getTableId(), finishTable.getTieringEpoch());
            } catch (Throwable e) {
                pbHeartbeatRespForTable.setError(ApiError.fromThrowable(e).toErrorResponse());
            }
        }

        if (request.hasRequestTable() && request.isRequestTable()) {
            LakeTieringTableInfo lakeTieringTableInfo = lakeTableTieringManager.requestTable();
            if (lakeTieringTableInfo != null) {
                heartbeatResponse
                        .setTieringTable()
                        .setTableId(lakeTieringTableInfo.tableId())
                        .setTablePath(fromTablePath(lakeTieringTableInfo.tablePath()))
                        .setTieringEpoch(lakeTieringTableInfo.tieringEpoch());
            }
        }
        return CompletableFuture.completedFuture(heartbeatResponse);
    }

    @Override
    public CompletableFuture<ControlledShutdownResponse> controlledShutdown(
            ControlledShutdownRequest request) {
        CompletableFuture<ControlledShutdownResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(
                        new ControlledShutdownEvent(
                                request.getTabletServerId(),
                                request.getTabletServerEpoch(),
                                response));
        return response;
    }

    private void validateHeartbeatRequest(
            PbHeartbeatReqForTable heartbeatReqForTable, int currentEpoch) {
        if (heartbeatReqForTable.getCoordinatorEpoch() != currentEpoch) {
            throw new InvalidCoordinatorException(
                    String.format(
                            "The coordinator epoch %s in request is not match current coordinator epoch %d for table %d.",
                            heartbeatReqForTable.getCoordinatorEpoch(),
                            currentEpoch,
                            heartbeatReqForTable.getTableId()));
        }
    }

    private TableMetadata getTableMetadata(CoordinatorContext ctx, TablePath tablePath) {
        // always get table info from zk.
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        long tableId = ctx.getTableIdByPath(tablePath);
        List<BucketMetadata> bucketMetadataList;
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            // TODO no need to get assignment from zk if refactor client metadata cache. Trace by
            // https://github.com/apache/fluss/issues/483
            // get table assignment from zk.
            bucketMetadataList =
                    getTableMetadataFromZk(
                            zkClient, tablePath, tableInfo.getTableId(), tableInfo.isPartitioned());
        } else {
            // get table assignment from coordinatorContext.
            bucketMetadataList =
                    getBucketMetadataFromContext(
                            ctx, tableId, null, ctx.getTableAssignment(tableId));
        }
        return new TableMetadata(tableInfo, bucketMetadataList);
    }

    private PartitionMetadata getPartitionMetadata(
            CoordinatorContext ctx, PhysicalTablePath partitionPath) {
        TablePath tablePath =
                new TablePath(partitionPath.getDatabaseName(), partitionPath.getTableName());
        String partitionName = partitionPath.getPartitionName();
        long tableId = ctx.getTableIdByPath(tablePath);
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            // TODO no need to get assignment from zk if refactor client metadata cache. Trace by
            // https://github.com/apache/fluss/issues/483
            return getPartitionMetadataFromZk(partitionPath, zkClient);
        } else {
            Optional<Long> partitionIdOpt = ctx.getPartitionId(partitionPath);
            if (partitionIdOpt.isPresent()) {
                long partitionId = partitionIdOpt.get();
                List<BucketMetadata> bucketMetadataList =
                        getBucketMetadataFromContext(
                                ctx,
                                tableId,
                                partitionId,
                                ctx.getPartitionAssignment(
                                        new TablePartition(tableId, partitionId)));
                return new PartitionMetadata(
                        tableId, partitionName, partitionId, bucketMetadataList);
            } else {
                return getPartitionMetadataFromZk(partitionPath, zkClient);
            }
        }
    }

    private static List<BucketMetadata> getBucketMetadataFromContext(
            CoordinatorContext ctx,
            long tableId,
            @Nullable Long partitionId,
            Map<Integer, List<Integer>> tableAssigment) {
        List<BucketMetadata> bucketMetadataList = new ArrayList<>();
        tableAssigment.forEach(
                (bucketId, serverIds) -> {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                    Optional<LeaderAndIsr> optLeaderAndIsr = ctx.getBucketLeaderAndIsr(tableBucket);
                    Integer leader = optLeaderAndIsr.map(LeaderAndIsr::leader).orElse(null);
                    BucketMetadata bucketMetadata =
                            new BucketMetadata(
                                    bucketId,
                                    leader,
                                    ctx.getBucketLeaderEpoch(tableBucket),
                                    serverIds);
                    bucketMetadataList.add(bucketMetadata);
                });
        return bucketMetadataList;
    }

    /**
     * Validates whether the table creation is allowed based on the table type and configuration.
     *
     * @param tableDescriptor the table descriptor to validate
     * @param tablePath the table path for error reporting
     * @throws InvalidTableException if table creation is not allowed
     */
    private void validateTableCreationPermission(
            TableDescriptor tableDescriptor, TablePath tablePath) {
        boolean hasPrimaryKey = tableDescriptor.hasPrimaryKey();

        if (hasPrimaryKey) {
            // This is a KV table (Primary Key Table)
            if (!kvTableAllowCreation) {
                throw new InvalidTableException(
                        "Creation of Primary Key Tables is disallowed in the cluster.");
            }
        } else {
            // This is a Log table
            if (!logTableAllowCreation) {
                throw new InvalidTableException(
                        "Creation of Log Tables is disallowed in the cluster.");
            }
        }
    }
}
