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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.InvalidConfigException;
import com.alibaba.fluss.exception.InvalidDatabaseException;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.MergeEngineType;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
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
import com.alibaba.fluss.rpc.messages.CreateDatabaseRequest;
import com.alibaba.fluss.rpc.messages.CreateDatabaseResponse;
import com.alibaba.fluss.rpc.messages.CreateTableRequest;
import com.alibaba.fluss.rpc.messages.CreateTableResponse;
import com.alibaba.fluss.rpc.messages.DropDatabaseRequest;
import com.alibaba.fluss.rpc.messages.DropDatabaseResponse;
import com.alibaba.fluss.rpc.messages.DropTableRequest;
import com.alibaba.fluss.rpc.messages.DropTableResponse;
import com.alibaba.fluss.server.RpcServiceBase;
import com.alibaba.fluss.server.coordinator.event.AdjustIsrReceivedEvent;
import com.alibaba.fluss.server.coordinator.event.CommitKvSnapshotEvent;
import com.alibaba.fluss.server.coordinator.event.CommitLakeTableSnapshotEvent;
import com.alibaba.fluss.server.coordinator.event.CommitRemoteLogManifestEvent;
import com.alibaba.fluss.server.coordinator.event.EventManager;
import com.alibaba.fluss.server.entity.CommitKvSnapshotData;
import com.alibaba.fluss.server.kv.rowmerger.VersionedRowMerger;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.utils.RpcMessageUtils;
import com.alibaba.fluss.server.utils.TableAssignmentUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.utils.AutoPartitionStrategy;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.alibaba.fluss.server.utils.RpcMessageUtils.getCommitLakeTableSnapshotData;
import static com.alibaba.fluss.server.utils.RpcMessageUtils.toTablePath;

/** An RPC Gateway service for coordinator server. */
public final class CoordinatorService extends RpcServiceBase implements CoordinatorGateway {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorService.class);

    private final int defaultBucketNumber;
    private final int defaultReplicationFactor;
    private final Supplier<EventManager> eventManagerSupplier;

    public CoordinatorService(
            Configuration conf,
            FileSystem remoteFileSystem,
            ZooKeeperClient zkClient,
            Supplier<EventManager> eventManagerSupplier,
            ServerMetadataCache metadataCache) {
        super(conf, remoteFileSystem, ServerType.COORDINATOR, zkClient, metadataCache);
        this.defaultBucketNumber = conf.getInt(ConfigOptions.DEFAULT_BUCKET_NUMBER);
        this.defaultReplicationFactor = conf.getInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR);
        this.eventManagerSupplier = eventManagerSupplier;
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
        CreateDatabaseResponse response = new CreateDatabaseResponse();
        try {
            TablePath.validateDatabaseName(request.getDatabaseName());
        } catch (InvalidDatabaseException e) {
            return FutureUtils.failedFuture(e);
        }

        DatabaseDescriptor databaseDescriptor = null;
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
        DropDatabaseResponse response = new DropDatabaseResponse();
        metadataManager.dropDatabase(
                request.getDatabaseName(), request.isIgnoreIfNotExists(), request.isCascade());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<CreateTableResponse> createTable(CreateTableRequest request) {
        CreateTableResponse response = new CreateTableResponse();
        TablePath tablePath = toTablePath(request.getTablePath());
        try {
            tablePath.validate();
        } catch (InvalidTableException | InvalidDatabaseException e) {
            return FutureUtils.failedFuture(e);
        }

        TableDescriptor tableDescriptor = null;
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

        sanityCheckMergeEngine(tableDescriptor);

        int bucketCount = defaultBucketNumber;
        // not set distribution
        if (!tableDescriptor.getTableDistribution().isPresent()) {
            tableDescriptor = tableDescriptor.copy(defaultBucketNumber);
        } else {
            Optional<Integer> optBucketCount =
                    tableDescriptor.getTableDistribution().get().getBucketCount();
            // not set bucket number
            if (!optBucketCount.isPresent()) {
                tableDescriptor = tableDescriptor.copy(defaultBucketNumber);
            } else {
                bucketCount = optBucketCount.get();
            }
        }

        // first, generate the assignment
        TableAssignment tableAssignment = null;
        // only when it's no partitioned table do we generate the assignment for it
        if (!tableDescriptor.isPartitioned()) {
            int replicaFactor = tableDescriptor.getReplicationFactor(defaultReplicationFactor);
            int[] servers = metadataCache.getLiveServerIds();
            tableAssignment =
                    TableAssignmentUtils.generateAssignment(bucketCount, replicaFactor, servers);
        } else {
            sanityCheckPartitionedTable(tableDescriptor);
        }

        // then create table;
        metadataManager.createTable(
                tablePath, tableDescriptor, tableAssignment, request.isIgnoreIfExists());

        return CompletableFuture.completedFuture(response);
    }

    private void sanityCheckPartitionedTable(TableDescriptor tableDescriptor) {
        AutoPartitionStrategy autoPartitionStrategy = tableDescriptor.getAutoPartitionStrategy();
        if (!autoPartitionStrategy.isAutoPartitionEnabled()) {
            throw new InvalidTableException(
                    String.format(
                            "Currently, partitioned table must enable auto partition, please set table property '%s' to true.",
                            ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key()));
        } else if (autoPartitionStrategy.timeUnit() == null) {
            throw new InvalidTableException(
                    String.format(
                            "Currently, partitioned table must set auto partition time unit when auto partition is enabled, please set table property '%s'.",
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key()));
        }

        if (tableDescriptor.getPartitionKeys().size() > 1) {
            throw new InvalidTableException(
                    String.format(
                            "Currently, partitioned table only supports one partition key, but got partition keys %s.",
                            tableDescriptor.getPartitionKeys()));
        }
        String partitionKey = tableDescriptor.getPartitionKeys().get(0);
        Schema schema = tableDescriptor.getSchema();
        int partitionIndex = schema.getColumnNames().indexOf(partitionKey);
        DataType partitionDataType = schema.getColumns().get(partitionIndex).getDataType();
        if (partitionDataType.getTypeRoot() != DataTypeRoot.STRING) {
            throw new InvalidTableException(
                    String.format(
                            "Currently, partitioned table only supports STRING type partition key, but got partition key '%s' with data type %s.",
                            partitionKey, partitionDataType));
        }
    }

    private void sanityCheckMergeEngine(TableDescriptor tableDescriptor) {
        Configuration config = Configuration.fromMap(tableDescriptor.getProperties());
        MergeEngineType mergeEngine = config.get(ConfigOptions.TABLE_MERGE_ENGINE);
        if (mergeEngine != null) {
            if (!tableDescriptor.hasPrimaryKey()) {
                throw new InvalidConfigException(
                        "Merge engine is only supported in primary key table.");
            }
            if (mergeEngine == MergeEngineType.VERSIONED) {
                Optional<String> versionColumn =
                        config.getOptional(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN);
                if (!versionColumn.isPresent()) {
                    throw new InvalidConfigException(
                            String.format(
                                    "'%s' must be set for versioned merge engine.",
                                    ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key()));
                }
                try {
                    VersionedRowMerger.createVersionComparator(
                            tableDescriptor.getSchema().toRowType(), versionColumn.get());
                } catch (IllegalArgumentException e) {
                    throw new InvalidConfigException(
                            "Failed to create versioned merge engine: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public CompletableFuture<DropTableResponse> dropTable(DropTableRequest request) {
        DropTableResponse response = new DropTableResponse();
        metadataManager.dropTable(
                toTablePath(request.getTablePath()), request.isIgnoreIfNotExists());
        return CompletableFuture.completedFuture(response);
    }

    public CompletableFuture<AdjustIsrResponse> adjustIsr(AdjustIsrRequest request) {
        CompletableFuture<AdjustIsrResponse> response = new CompletableFuture<>();
        eventManagerSupplier
                .get()
                .put(
                        new AdjustIsrReceivedEvent(
                                RpcMessageUtils.getAdjustIsrData(request), response));
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
                                RpcMessageUtils.getCommitRemoteLogManifestData(request), response));
        return response;
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
