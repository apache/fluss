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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidCoordinatorException;
import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.LakeTableAlreadyExistException;
import org.apache.fluss.exception.SecurityDisabledException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AddServerTagRequest;
import org.apache.fluss.rpc.messages.AddServerTagResponse;
import org.apache.fluss.rpc.messages.AdjustIsrRequest;
import org.apache.fluss.rpc.messages.AdjustIsrResponse;
import org.apache.fluss.rpc.messages.AlterClusterConfigsRequest;
import org.apache.fluss.rpc.messages.AlterClusterConfigsResponse;
import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.AlterTableResponse;
import org.apache.fluss.rpc.messages.CancelRebalanceRequest;
import org.apache.fluss.rpc.messages.CancelRebalanceResponse;
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
import org.apache.fluss.rpc.messages.ListRebalanceProgressRequest;
import org.apache.fluss.rpc.messages.ListRebalanceProgressResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbAlterConfig;
import org.apache.fluss.rpc.messages.PbHeartbeatReqForTable;
import org.apache.fluss.rpc.messages.PbHeartbeatRespForTable;
import org.apache.fluss.rpc.messages.PbPrepareLakeTableRespForTable;
import org.apache.fluss.rpc.messages.PbTableOffsets;
import org.apache.fluss.rpc.messages.PrepareLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PrepareLakeTableSnapshotResponse;
import org.apache.fluss.rpc.messages.RebalanceRequest;
import org.apache.fluss.rpc.messages.RebalanceResponse;
import org.apache.fluss.rpc.messages.RemoveServerTagRequest;
import org.apache.fluss.rpc.messages.RemoveServerTagResponse;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.DynamicConfigManager;
import org.apache.fluss.server.RpcServiceBase;
import org.apache.fluss.server.authorizer.AclCreateResult;
import org.apache.fluss.server.authorizer.AclDeleteResult;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.AddServerTagEvent;
import org.apache.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import org.apache.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import org.apache.fluss.server.coordinator.event.CommitLakeTableSnapshotEvent;
import org.apache.fluss.server.coordinator.event.CommitRemoteLogManifestEvent;
import org.apache.fluss.server.coordinator.event.ControlledShutdownEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RemoveServerTagEvent;
import org.apache.fluss.server.entity.CommitKvSnapshotData;
import org.apache.fluss.server.entity.LakeTieringTableInfo;
import org.apache.fluss.server.entity.TablePropertyChanges;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.CoordinatorMetadataProvider;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableHelper;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.json.TableBucketOffsets;

import javax.annotation.Nullable;

import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.fluss.config.FlussConfigUtils.isTableStorageConfig;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toAclBindingFilters;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toAclBindings;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.fromTablePath;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getAdjustIsrData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getCommitLakeTableSnapshotData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getCommitRemoteLogManifestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getPartitionSpec;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeCreateAclsResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeDropAclsResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toAlterTableConfigChanges;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toAlterTableSchemaChanges;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucketOffsets;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTablePath;
import static org.apache.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static org.apache.fluss.utils.PartitionUtils.validatePartitionSpec;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An RPC Gateway service for coordinator server. */
public final class CoordinatorService extends RpcServiceBase implements CoordinatorGateway {

    private final int defaultBucketNumber;
    private final int defaultReplicationFactor;
    private final boolean logTableAllowCreation;
    private final boolean kvTableAllowCreation;
    private final Supplier<EventManager> eventManagerSupplier;
    private final Supplier<Integer> coordinatorEpochSupplier;
    private final CoordinatorMetadataCache metadataCache;

    private final LakeTableTieringManager lakeTableTieringManager;
    private final LakeCatalogDynamicLoader lakeCatalogDynamicLoader;
    private final ExecutorService ioExecutor;
    private final LakeTableHelper lakeTableHelper;

    public CoordinatorService(
            Configuration conf,
            FileSystem remoteFileSystem,
            ZooKeeperClient zkClient,
            Supplier<CoordinatorEventProcessor> coordinatorEventProcessorSupplier,
            CoordinatorMetadataCache metadataCache,
            MetadataManager metadataManager,
            @Nullable Authorizer authorizer,
            LakeCatalogDynamicLoader lakeCatalogDynamicLoader,
            LakeTableTieringManager lakeTableTieringManager,
            DynamicConfigManager dynamicConfigManager,
            ExecutorService ioExecutor) {
        super(
                remoteFileSystem,
                ServerType.COORDINATOR,
                zkClient,
                metadataManager,
                authorizer,
                dynamicConfigManager,
                ioExecutor);
        this.defaultBucketNumber = conf.getInt(ConfigOptions.DEFAULT_BUCKET_NUMBER);
        this.defaultReplicationFactor = conf.getInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR);
        this.logTableAllowCreation = conf.getBoolean(ConfigOptions.LOG_TABLE_ALLOW_CREATION);
        this.kvTableAllowCreation = conf.getBoolean(ConfigOptions.KV_TABLE_ALLOW_CREATION);
        this.eventManagerSupplier =
                () -> coordinatorEventProcessorSupplier.get().getCoordinatorEventManager();
        this.coordinatorEpochSupplier =
                () -> coordinatorEventProcessorSupplier.get().getCoordinatorEpoch();
        this.lakeTableTieringManager = lakeTableTieringManager;
        this.metadataCache = metadataCache;
        this.lakeCatalogDynamicLoader = lakeCatalogDynamicLoader;
        this.ioExecutor = ioExecutor;
        this.lakeTableHelper =
                new LakeTableHelper(zkClient, conf.getString(ConfigOptions.REMOTE_DATA_DIR));
    }

    @Override
    public String name() {
        return "coordinator";
    }

    @Override
    public void shutdown() {
        IOUtils.closeQuietly(lakeCatalogDynamicLoader, "lake catalog");
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

        LakeCatalogDynamicLoader.LakeCatalogContainer lakeCatalogContainer =
                lakeCatalogDynamicLoader.getLakeCatalogContainer();

        // Check table creation permissions based on table type
        validateTableCreationPermission(tableDescriptor, tablePath);

        // apply system defaults if the config is not set
        tableDescriptor =
                applySystemDefaults(tableDescriptor, lakeCatalogContainer.getDataLakeFormat());

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

        // before create table in fluss, we may create in lake
        if (isDataLakeEnabled(tableDescriptor)) {
            try {
                checkNotNull(lakeCatalogContainer.getLakeCatalog())
                        .createTable(
                                tablePath,
                                tableDescriptor,
                                new DefaultLakeCatalogContext(
                                        true, currentSession().getPrincipal()));
            } catch (TableAlreadyExistException e) {
                throw new LakeTableAlreadyExistException(e.getMessage(), e);
            }
        }

        // then create table;
        metadataManager.createTable(
                tablePath, tableDescriptor, tableAssignment, request.isIgnoreIfExists());

        return CompletableFuture.completedFuture(new CreateTableResponse());
    }

    @Override
    public CompletableFuture<AlterTableResponse> alterTable(AlterTableRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        tablePath.validate();
        if (authorizer != null) {
            authorizer.authorize(currentSession(), OperationType.ALTER, Resource.table(tablePath));
        }

        List<TableChange> alterTableConfigChanges =
                toAlterTableConfigChanges(request.getConfigChangesList());
        TablePropertyChanges tablePropertyChanges = toTablePropertyChanges(alterTableConfigChanges);
        List<TableChange> alterSchemaChanges = toAlterTableSchemaChanges(request);

        if (!alterSchemaChanges.isEmpty() && !alterTableConfigChanges.isEmpty()) {
            // Only support one of alterTableConfigChanges and alterSchemaChanges for atomic change.
            throw new InvalidAlterTableException(
                    "Table alteration can only be applied to one of the following: "
                            + "table properties or table schema.");
        }

        LakeCatalogDynamicLoader.LakeCatalogContainer lakeCatalogContainer =
                lakeCatalogDynamicLoader.getLakeCatalogContainer();
        LakeCatalog.Context lakeCatalogContext =
                new DefaultLakeCatalogContext(false, currentSession().getPrincipal());

        if (!alterSchemaChanges.isEmpty()) {
            metadataManager.alterTableSchema(
                    tablePath,
                    alterSchemaChanges,
                    request.isIgnoreIfNotExists(),
                    lakeCatalogContainer.getLakeCatalog(),
                    lakeCatalogContext);
        }

        if (!alterTableConfigChanges.isEmpty()) {
            metadataManager.alterTableProperties(
                    tablePath,
                    alterTableConfigChanges,
                    tablePropertyChanges,
                    request.isIgnoreIfNotExists(),
                    lakeCatalogContainer.getLakeCatalog(),
                    lakeTableTieringManager,
                    lakeCatalogContext);
        }

        return CompletableFuture.completedFuture(new AlterTableResponse());
    }

    public static TablePropertyChanges toTablePropertyChanges(List<TableChange> tableChanges) {
        TablePropertyChanges.Builder builder = TablePropertyChanges.builder();
        if (tableChanges.isEmpty()) {
            return builder.build();
        }

        for (TableChange tableChange : tableChanges) {
            if (tableChange instanceof TableChange.SetOption) {
                TableChange.SetOption setOption = (TableChange.SetOption) tableChange;
                String optionKey = setOption.getKey();
                if (isTableStorageConfig(optionKey)) {
                    builder.setTableProperty(optionKey, setOption.getValue());
                } else {
                    // otherwise, it's considered as custom property
                    builder.setCustomProperty(optionKey, setOption.getValue());
                }
            } else if (tableChange instanceof TableChange.ResetOption) {
                TableChange.ResetOption resetOption = (TableChange.ResetOption) tableChange;
                String optionKey = resetOption.getKey();
                if (isTableStorageConfig(optionKey)) {
                    builder.resetTableProperty(optionKey);
                } else {
                    // otherwise, it's considered as custom property
                    builder.resetCustomProperty(optionKey);
                }
            } else {
                throw new InvalidAlterTableException(
                        "Unsupported alter table change: " + tableChange);
            }
        }
        return builder.build();
    }

    private TableDescriptor applySystemDefaults(
            TableDescriptor tableDescriptor, DataLakeFormat dataLakeFormat) {
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

        // For tables with first_row, versioned or aggregation merge engines, automatically set to
        // IGNORE if delete behavior is not set
        Configuration tableConf = Configuration.fromMap(tableDescriptor.getProperties());
        MergeEngineType mergeEngine =
                tableConf.getOptional(ConfigOptions.TABLE_MERGE_ENGINE).orElse(null);
        if (mergeEngine == MergeEngineType.FIRST_ROW
                || mergeEngine == MergeEngineType.VERSIONED
                || mergeEngine == MergeEngineType.AGGREGATION) {
            if (tableDescriptor.hasPrimaryKey()
                    && !tableConf.getOptional(ConfigOptions.TABLE_DELETE_BEHAVIOR).isPresent()) {
                Map<String, String> newProperties = new HashMap<>(newDescriptor.getProperties());
                newProperties.put(
                        ConfigOptions.TABLE_DELETE_BEHAVIOR.key(), DeleteBehavior.IGNORE.name());
                newDescriptor = newDescriptor.withProperties(newProperties);
            }
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
        validatePartitionSpec(tablePath, table.partitionKeys, partitionSpec, true);
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
        validatePartitionSpec(tablePath, table.partitionKeys, partitionSpec, false);
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
                        ctx -> {
                            return processMetadataRequest(
                                    request,
                                    listenerName,
                                    session,
                                    authorizer,
                                    metadataCache,
                                    new CoordinatorMetadataProvider(
                                            zkClient, metadataManager, ctx));
                        });
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
    public CompletableFuture<PrepareLakeTableSnapshotResponse> prepareLakeTableSnapshot(
            PrepareLakeTableSnapshotRequest request) {
        CompletableFuture<PrepareLakeTableSnapshotResponse> future = new CompletableFuture<>();
        ioExecutor.submit(
                () -> {
                    PrepareLakeTableSnapshotResponse response =
                            new PrepareLakeTableSnapshotResponse();
                    try {
                        for (PbTableOffsets bucketOffsets : request.getBucketOffsetsList()) {
                            PbPrepareLakeTableRespForTable pbPrepareLakeTableRespForTable =
                                    response.addPrepareLakeTableResp();
                            try {
                                long tableId = bucketOffsets.getTableId();
                                TableBucketOffsets tableBucketOffsets =
                                        toTableBucketOffsets(bucketOffsets);
                                // get previous lake tables
                                Optional<LakeTable> optPreviousLakeTable =
                                        zkClient.getLakeTable(tableId);
                                if (optPreviousLakeTable.isPresent()) {
                                    // need to merge with previous lake table
                                    tableBucketOffsets =
                                            lakeTableHelper.mergeTableBucketOffsets(
                                                    optPreviousLakeTable.get(), tableBucketOffsets);
                                }
                                TablePath tablePath = toTablePath(bucketOffsets.getTablePath());
                                FsPath fsPath =
                                        lakeTableHelper.storeLakeTableOffsetsFile(
                                                tablePath, tableBucketOffsets);
                                pbPrepareLakeTableRespForTable.setTableId(tableId);
                                pbPrepareLakeTableRespForTable.setLakeTableOffsetsPath(
                                        fsPath.toString());
                            } catch (Exception e) {
                                Errors error = ApiError.fromThrowable(e).error();
                                pbPrepareLakeTableRespForTable.setError(
                                        error.code(), error.message());
                            }
                        }
                        future.complete(response);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
        return future;
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

    @Override
    public CompletableFuture<AlterClusterConfigsResponse> alterClusterConfigs(
            AlterClusterConfigsRequest request) {
        CompletableFuture<AlterClusterConfigsResponse> future = new CompletableFuture<>();
        List<PbAlterConfig> infos = request.getAlterConfigsList();
        if (infos.isEmpty()) {
            return CompletableFuture.completedFuture(new AlterClusterConfigsResponse());
        }

        if (authorizer != null) {
            authorizer.authorize(currentSession(), OperationType.ALTER, Resource.cluster());
        }

        List<AlterConfig> serverConfigChanges =
                infos.stream()
                        .map(
                                info ->
                                        new AlterConfig(
                                                info.getConfigKey(),
                                                info.hasConfigValue()
                                                        ? info.getConfigValue()
                                                        : null,
                                                AlterConfigOpType.from((byte) info.getOpType())))
                        .collect(Collectors.toList());
        AccessContextEvent<Void> accessContextEvent =
                new AccessContextEvent<>(
                        (context) -> {
                            try {
                                dynamicConfigManager.alterConfigs(serverConfigChanges);
                                future.complete(new AlterClusterConfigsResponse());
                            } catch (Exception e) {
                                future.completeExceptionally(e);
                            }
                            return null;
                        });
        eventManagerSupplier.get().put(accessContextEvent);
        return future;
    }

    @Override
    public CompletableFuture<AddServerTagResponse> addServerTag(AddServerTagRequest request) {
        CompletableFuture<AddServerTagResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(
                        new AddServerTagEvent(
                                Arrays.stream(request.getServerIds())
                                        .boxed()
                                        .collect(Collectors.toList()),
                                ServerTag.valueOf(request.getServerTag()),
                                response));
        return response;
    }

    @Override
    public CompletableFuture<RemoveServerTagResponse> removeServerTag(
            RemoveServerTagRequest request) {
        CompletableFuture<RemoveServerTagResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(
                        new RemoveServerTagEvent(
                                Arrays.stream(request.getServerIds())
                                        .boxed()
                                        .collect(Collectors.toList()),
                                ServerTag.valueOf(request.getServerTag()),
                                response));
        return response;
    }

    @Override
    public CompletableFuture<RebalanceResponse> rebalance(RebalanceRequest request) {
        throw new UnsupportedOperationException("Support soon!");
    }

    @Override
    public CompletableFuture<ListRebalanceProgressResponse> listRebalanceProgress(
            ListRebalanceProgressRequest request) {
        throw new UnsupportedOperationException("Support soon!");
    }

    @Override
    public CompletableFuture<CancelRebalanceResponse> cancelRebalance(
            CancelRebalanceRequest request) {
        throw new UnsupportedOperationException("Support soon!");
    }

    @VisibleForTesting
    public DataLakeFormat getDataLakeFormat() {
        return lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat();
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

    static class DefaultLakeCatalogContext implements LakeCatalog.Context {

        private final boolean isCreatingFlussTable;
        private final FlussPrincipal flussPrincipal;

        public DefaultLakeCatalogContext(
                boolean isCreatingFlussTable, FlussPrincipal flussPrincipal) {
            this.isCreatingFlussTable = isCreatingFlussTable;
            this.flussPrincipal = flussPrincipal;
        }

        @Override
        public boolean isCreatingFlussTable() {
            return isCreatingFlussTable;
        }

        @Override
        public FlussPrincipal getFlussPrincipal() {
            return flussPrincipal;
        }
    }
}
