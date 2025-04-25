/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.InvalidDatabaseException;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.exception.SecurityDisabledException;
import com.alibaba.fluss.exception.TableNotPartitionedException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.AdjustIsrRequest;
import com.alibaba.fluss.rpc.messages.AdjustIsrResponse;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotRequest;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotResponse;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import com.alibaba.fluss.rpc.messages.CreateAclsRequest;
import com.alibaba.fluss.rpc.messages.CreateAclsResponse;
import com.alibaba.fluss.rpc.messages.CreateDatabaseRequest;
import com.alibaba.fluss.rpc.messages.CreateDatabaseResponse;
import com.alibaba.fluss.rpc.messages.CreatePartitionRequest;
import com.alibaba.fluss.rpc.messages.CreatePartitionResponse;
import com.alibaba.fluss.rpc.messages.CreateTableRequest;
import com.alibaba.fluss.rpc.messages.CreateTableResponse;
import com.alibaba.fluss.rpc.messages.DropAclsRequest;
import com.alibaba.fluss.rpc.messages.DropAclsResponse;
import com.alibaba.fluss.rpc.messages.DropDatabaseRequest;
import com.alibaba.fluss.rpc.messages.DropDatabaseResponse;
import com.alibaba.fluss.rpc.messages.DropPartitionRequest;
import com.alibaba.fluss.rpc.messages.DropPartitionResponse;
import com.alibaba.fluss.rpc.messages.DropTableRequest;
import com.alibaba.fluss.rpc.messages.DropTableResponse;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.server.RpcServiceBase;
import com.alibaba.fluss.server.authorizer.AclCreateResult;
import com.alibaba.fluss.server.authorizer.AclDeleteResult;
import com.alibaba.fluss.server.authorizer.Authorizer;
import com.alibaba.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import com.alibaba.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import com.alibaba.fluss.server.coordinator.event.CommitLakeTableSnapshotEvent;
import com.alibaba.fluss.server.coordinator.event.CommitRemoteLogManifestEvent;
import com.alibaba.fluss.server.coordinator.event.EventManager;
import com.alibaba.fluss.server.entity.CommitKvSnapshotData;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.utils.TableAssignmentUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toAclBindingFilters;
import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toAclBindings;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getAdjustIsrData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getCommitLakeTableSnapshotData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getCommitRemoteLogManifestData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getPartitionSpec;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeCreateAclsResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeDropAclsResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toTablePath;
import static com.alibaba.fluss.utils.PartitionUtils.validatePartitionSpec;

/** An RPC Gateway service for coordinator server. */
public final class CoordinatorService extends RpcServiceBase implements CoordinatorGateway {

    private final int defaultBucketNumber;
    private final int defaultReplicationFactor;
    private final Supplier<EventManager> eventManagerSupplier;

    // null if the cluster hasn't configured datalake format
    private final @Nullable DataLakeFormat dataLakeFormat;

    public CoordinatorService(
            Configuration conf,
            FileSystem remoteFileSystem,
            ZooKeeperClient zkClient,
            Supplier<EventManager> eventManagerSupplier,
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            @Nullable Authorizer authorizer) {
        super(
                remoteFileSystem,
                ServerType.COORDINATOR,
                zkClient,
                metadataCache,
                metadataManager,
                authorizer);
        this.defaultBucketNumber = conf.getInt(ConfigOptions.DEFAULT_BUCKET_NUMBER);
        this.defaultReplicationFactor = conf.getInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR);
        this.eventManagerSupplier = eventManagerSupplier;
        this.dataLakeFormat = conf.getOptional(ConfigOptions.DATALAKE_FORMAT).orElse(null);
    }

    @Override
    public String name() {
        return "coordinator";
    }

    @Override
    public void shutdown() {
        // no resources hold by coordinator service by now, nothing to do
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
            return FutureUtils.failedFuture(e);
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
            int[] servers = metadataCache.getLiveServerIds();
            tableAssignment =
                    TableAssignmentUtils.generateAssignment(bucketCount, replicaFactor, servers);
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

        // override the datalake format if the table hasn't set it and the cluster configured
        if (dataLakeFormat != null
                && !properties.containsKey(ConfigOptions.TABLE_DATALAKE_FORMAT.key())) {
            newDescriptor = newDescriptor.withDataLakeFormat(dataLakeFormat);
        }

        // lake table can only be enabled when the cluster configures datalake format
        String dataLakeEnabledValue =
                newDescriptor.getProperties().get(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
        boolean dataLakeEnabled = Boolean.parseBoolean(dataLakeEnabledValue);
        if (dataLakeEnabled && dataLakeFormat == null) {
            throw new InvalidTableException(
                    String.format(
                            "'%s' is enabled for the table, but the Fluss cluster doesn't enable datalake tables.",
                            ConfigOptions.TABLE_DATALAKE_ENABLED.key()));
        }

        return newDescriptor;
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
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        if (!tableInfo.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Only partitioned table support create partition.");
        }

        // first, validate the partition spec, and get resolved partition spec.
        PartitionSpec partitionSpec = getPartitionSpec(request.getPartitionSpec());
        validatePartitionSpec(tableInfo, partitionSpec);
        ResolvedPartitionSpec partitionToCreate =
                ResolvedPartitionSpec.fromPartitionSpec(
                        tableInfo.getPartitionKeys(), partitionSpec);

        // second, generate the PartitionAssignment.
        int replicaFactor = tableInfo.getTableConfig().getReplicationFactor();
        int[] servers = metadataCache.getLiveServerIds();
        Map<Integer, BucketAssignment> bucketAssignments =
                TableAssignmentUtils.generateAssignment(
                                tableInfo.getNumBuckets(), replicaFactor, servers)
                        .getBucketAssignments();
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableInfo.getTableId(), bucketAssignments);

        metadataManager.createPartition(
                tableInfo.getTablePath(),
                tableInfo.getTableId(),
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
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        if (!tableInfo.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Only partitioned table support drop partition.");
        }

        // first, validate the partition spec.
        PartitionSpec partitionSpec = getPartitionSpec(request.getPartitionSpec());
        validatePartitionSpec(tableInfo, partitionSpec);
        ResolvedPartitionSpec partitionToDrop =
                ResolvedPartitionSpec.fromPartitionSpec(
                        tableInfo.getPartitionKeys(), partitionSpec);

        metadataManager.dropPartition(tablePath, partitionToDrop, request.isIgnoreIfNotExists());
        return CompletableFuture.completedFuture(response);
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
}
