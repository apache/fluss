/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.ConsumerGroupOwnerLoadingException;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.exception.NotConsumerGroupOwnerException;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.replica.ReplicaManager.HostedReplica;
import org.apache.fluss.server.replica.ReplicaManager.OnlineReplica;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE;
import static org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetTableConfig.OFFSET_COMMIT_REQUIRED_ACKS;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.NO_LEADER;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Discovers consumer group owners and reads and commits their offsets on local leaders. */
@ThreadSafe
@Internal
public final class ConsumerGroupManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupManager.class);
    private static final int OFFSET_COMMIT_PAGE_SIZE = 4 * 1024;

    private final int serverId;
    private final ReplicaManager replicaManager;
    private final ConsumerOffsetTableConfig offsetsConfig;
    private final ConsumerOffsetSchema offsetSchema = new ConsumerOffsetSchema();
    private final ZooKeeperClient zkClient;
    private final MetadataManager metadataManager;
    private final TabletServerMetadataCache metadataCache;
    private final TabletServerMetadataProvider metadataProvider;
    private final CoordinatorGateway coordinatorGateway;
    private final Executor ioExecutor;
    private final AtomicReference<CompletableFuture<TableInfo>> tableInitFuture =
            new AtomicReference<>();
    @Nullable private volatile CachedValueDecoder cachedValueDecoder;

    /** Creates a manager for consumer groups on this tablet server. */
    public ConsumerGroupManager(
            int serverId,
            ReplicaManager replicaManager,
            ConsumerOffsetTableConfig offsetsConfig,
            ZooKeeperClient zkClient,
            MetadataManager metadataManager,
            TabletServerMetadataCache metadataCache,
            TabletServerMetadataProvider metadataProvider,
            CoordinatorGateway coordinatorGateway,
            Executor ioExecutor) {
        this.serverId = serverId;
        this.replicaManager = checkNotNull(replicaManager, "replicaManager must not be null");
        this.offsetsConfig = checkNotNull(offsetsConfig, "offsetsConfig must not be null");
        this.zkClient = checkNotNull(zkClient, "zkClient must not be null");
        this.metadataManager = checkNotNull(metadataManager, "metadataManager must not be null");
        this.metadataCache = checkNotNull(metadataCache, "metadataCache must not be null");
        this.metadataProvider = checkNotNull(metadataProvider, "metadataProvider must not be null");
        this.coordinatorGateway =
                checkNotNull(coordinatorGateway, "coordinatorGateway must not be null");
        this.ioExecutor = checkNotNull(ioExecutor, "ioExecutor must not be null");
    }

    /** Finds the owner of a group, initializing the offsets table on first discovery. */
    public CompletableFuture<ServerNode> findOwner(String groupId, String listenerName) {
        checkGroupId(groupId);
        return ensureTableAsync()
                .thenCompose(
                        info ->
                                resolveOwnerAsync(
                                        offsetSchema.bucketFor(groupId, info.getNumBuckets()),
                                        listenerName));
    }

    /** Updates local offsets table metadata even when another tablet served the find request. */
    public void onTableMetadataUpdate(List<TableMetadata> tables) {
        for (TableMetadata table : tables) {
            TableInfo info = table.getTableInfo();
            if (SystemTables.CONSUMER_OFFSETS.equals(info.getTablePath())) {
                if (info.getTableId() == DELETED_TABLE_ID) {
                    tableInitFuture.set(null);
                } else if (metadataCache.getBucketCountEpoch(info.getTableId()).orElse(-1L)
                        == info.getBucketCountEpoch()) {
                    // Use only a bucket layout accepted by the metadata cache.
                    tableInitFuture.set(CompletableFuture.completedFuture(info));
                }
            } else if (DELETED_TABLE_PATH.equals(info.getTablePath())) {
                CompletableFuture<TableInfo> current = tableInitFuture.get();
                TableInfo cached = getInitializedTable(current);
                if (cached != null && cached.getTableId() == info.getTableId()) {
                    tableInitFuture.compareAndSet(current, null);
                }
            }
        }
    }

    /** Returns the local leader replica or throws a request-level group error. */
    public Replica validateGroupId(String groupId) {
        checkGroupId(groupId);
        TableInfo info = getInitializedTable(tableInitFuture.get());
        if (info == null) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "Table metadata for "
                            + SystemTables.CONSUMER_OFFSETS
                            + " is not available yet.");
        }
        return checkOwnership(
                new TableBucket(
                        info.getTableId(), offsetSchema.bucketFor(groupId, info.getNumBuckets())));
    }

    @Nullable
    private static TableInfo getInitializedTable(@Nullable CompletableFuture<TableInfo> current) {
        return current != null && current.isDone() && !current.isCompletedExceptionally()
                ? current.getNow(null)
                : null;
    }

    private static void checkGroupId(String groupId) {
        if (groupId == null || groupId.isEmpty()) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must not be null or empty.");
        }
    }

    /**
     * Resolves the owner of the requested bucket from the local metadata snapshot, falling back to
     * ZooKeeper when the bucket, leader, or advertised server endpoint is not yet cached. A
     * successful bucket fallback also refreshes the local metadata cache.
     */
    CompletableFuture<ServerNode> resolveOwnerAsync(int bucketId, String listenerName) {
        Optional<BucketMetadata> cachedBucket =
                metadataCache.getCachedBucketMetadata(SystemTables.CONSUMER_OFFSETS, bucketId);
        if (cachedBucket.isPresent() && cachedBucket.get().getLeaderId().isPresent()) {
            Optional<ServerNode> cachedOwner =
                    metadataCache.getTabletServer(
                            cachedBucket.get().getLeaderId().getAsInt(), listenerName);
            if (cachedOwner.isPresent()) {
                return CompletableFuture.completedFuture(cachedOwner.get());
            }
        }

        return CompletableFuture.supplyAsync(
                () -> resolveOwnerFromZooKeeper(bucketId, listenerName), ioExecutor);
    }

    private ServerNode resolveOwnerFromZooKeeper(int bucketId, String listenerName) {
        try {
            List<TableMetadata> tableMetadataList =
                    metadataProvider.getTablesMetadataFromZK(
                            Collections.singleton(SystemTables.CONSUMER_OFFSETS));
            BucketMetadata bucketMetadata =
                    tableMetadataList.get(0).getBucketMetadataList().stream()
                            .filter(metadata -> metadata.getBucketId() == bucketId)
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new ConsumerGroupOwnerNotAvailableException(
                                                    "Bucket "
                                                            + bucketId
                                                            + " is not available yet in "
                                                            + SystemTables.CONSUMER_OFFSETS));
            OptionalInt leaderIdOpt = bucketMetadata.getLeaderId();
            if (!leaderIdOpt.isPresent()) {
                throw new ConsumerGroupOwnerLoadingException(
                        "No leader yet for bucket "
                                + bucketId
                                + " in "
                                + SystemTables.CONSUMER_OFFSETS);
            }

            int leaderId = leaderIdOpt.getAsInt();
            Optional<ServerNode> cachedOwner =
                    metadataCache.getTabletServer(leaderId, listenerName);
            if (cachedOwner.isPresent()) {
                return cachedOwner.get();
            }

            TabletServerRegistration registration =
                    zkClient.getTabletServer(leaderId)
                            .orElseThrow(
                                    () ->
                                            new ConsumerGroupOwnerNotAvailableException(
                                                    "Group owner node "
                                                            + leaderId
                                                            + " is not registered in ZooKeeper"));
            ServerNode owner =
                    new ServerInfo(
                                    leaderId,
                                    registration.getRack(),
                                    registration.getEndpoints(),
                                    ServerType.TABLET_SERVER,
                                    registration.getResource())
                            .node(listenerName);
            if (owner == null) {
                throw new ConsumerGroupOwnerNotAvailableException(
                        "Group owner node "
                                + leaderId
                                + " is not advertised on listener "
                                + listenerName);
            }
            return owner;
        } catch (ConsumerGroupOwnerLoadingException | ConsumerGroupOwnerNotAvailableException e) {
            throw e;
        } catch (Exception e) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "Failed to resolve the owner of "
                            + SystemTables.CONSUMER_OFFSETS
                            + " bucket "
                            + bucketId,
                    e);
        }
    }

    /**
     * Ensures that the consumer-offset table exists without blocking an RPC worker thread.
     * Concurrent requests share the same initialization future. The completed future contains the
     * table id, schema id and actual bucket count of the existing or newly created table.
     */
    CompletableFuture<TableInfo> ensureTableAsync() {
        // Retry only when another thread wins the CAS and its initialization is concurrently
        // cleared after a fast failure. This loop never waits for the initialization itself.
        while (true) {
            CompletableFuture<TableInfo> current = tableInitFuture.get();
            if (current != null) {
                return current;
            }

            CompletableFuture<TableInfo> created = new CompletableFuture<>();
            if (tableInitFuture.compareAndSet(null, created)) {
                submitTableInitialization(created);
                return created;
            }
        }
    }

    private void submitTableInitialization(CompletableFuture<TableInfo> initializationFuture) {
        try {
            ioExecutor.execute(
                    () -> {
                        try {
                            initializationFuture.complete(createOrResolveTableInfo());
                        } catch (Exception e) {
                            completeInitializationExceptionally(initializationFuture, e);
                        } catch (Error error) {
                            abortInitializationAfterFatalError(initializationFuture, error);
                            throw error;
                        }
                    });
        } catch (Exception e) {
            completeInitializationExceptionally(initializationFuture, e);
        } catch (Error error) {
            abortInitializationAfterFatalError(initializationFuture, error);
            throw error;
        }
    }

    private void completeInitializationExceptionally(
            CompletableFuture<TableInfo> initializationFuture, Exception cause) {
        tableInitFuture.compareAndSet(initializationFuture, null);
        ConsumerGroupOwnerNotAvailableException exception =
                cause instanceof ConsumerGroupOwnerNotAvailableException
                        ? (ConsumerGroupOwnerNotAvailableException) cause
                        : new ConsumerGroupOwnerNotAvailableException(
                                "Failed to initialize " + SystemTables.CONSUMER_OFFSETS, cause);
        LOG.warn("Failed to initialize {}.", SystemTables.CONSUMER_OFFSETS, cause);
        initializationFuture.completeExceptionally(exception);
    }

    private void abortInitializationAfterFatalError(
            CompletableFuture<TableInfo> initializationFuture, Error error) {
        tableInitFuture.compareAndSet(initializationFuture, null);
        initializationFuture.completeExceptionally(error);
    }

    private TableInfo createOrResolveTableInfo() {
        try {
            if (zkClient.tableExist(SystemTables.CONSUMER_OFFSETS)) {
                LOG.debug("System table {} already exists.", SystemTables.CONSUMER_OFFSETS);
                return metadataManager.getTable(SystemTables.CONSUMER_OFFSETS);
            }

            String databaseName = SystemTables.CONSUMER_OFFSETS.getDatabaseName();
            if (!zkClient.databaseExists(databaseName)) {
                CreateDatabaseRequest createDatabaseRequest =
                        new CreateDatabaseRequest()
                                .setDatabaseName(databaseName)
                                .setDatabaseJson(DatabaseDescriptor.builder().build().toJsonBytes())
                                .setIgnoreIfExists(true);
                coordinatorGateway.createDatabase(createDatabaseRequest).get();
            }

            int bucketCount = offsetsConfig.getOffsetsTableNumBuckets();
            int replicationFactor = offsetsConfig.getOffsetsTableReplicationFactor();
            TableDescriptor tableDescriptor =
                    TableDescriptor.builder()
                            .schema(ConsumerOffsetSchema.getTableSchema())
                            .logFormat(LogFormat.COMPACTED)
                            .distributedBy(bucketCount, ConsumerOffsetSchema.getBucketKeyColumn())
                            .property(
                                    ConfigOptions.TABLE_LOG_TTL,
                                    offsetsConfig.getOffsetsRetention())
                            .property(
                                    ConfigOptions.TABLE_LOG_LOCAL_TTL,
                                    ConsumerOffsetTableConfig.OFFSETS_TABLE_LOCAL_LOG_TTL)
                            .property(
                                    ConfigOptions.TABLE_KV_TTL, offsetsConfig.getOffsetsRetention())
                            .property(ConfigOptions.TABLE_REPLICATION_FACTOR, replicationFactor)
                            .build();

            CreateTableRequest createTableRequest =
                    new CreateTableRequest()
                            .setTableJson(tableDescriptor.toJsonBytes())
                            .setIgnoreIfExists(true);
            createTableRequest
                    .setTablePath()
                    .setDatabaseName(SystemTables.CONSUMER_OFFSETS.getDatabaseName())
                    .setTableName(SystemTables.CONSUMER_OFFSETS.getTableName());
            coordinatorGateway.createTable(createTableRequest).get();
            return metadataManager.getTable(SystemTables.CONSUMER_OFFSETS);
        } catch (Exception e) {
            throw new ConsumerGroupOwnerNotAvailableException(
                    "Failed to get or create system table: " + SystemTables.CONSUMER_OFFSETS, e);
        }
    }

    /** Appends on the calling worker and completes the result when the write is acknowledged. */
    public CompletableFuture<Map<TableBucket, Errors>> handleCommitConsumerOffsets(
            String groupId,
            Replica offsetsReplica,
            Map<TableBucket, ConsumerOffsetAndMetadata> offsets) {
        long maxMetadataBytes = offsetsConfig.getMaxMetadataSize().getBytes();
        Map<TableBucket, ConsumerOffsetAndMetadata> validOffsets = new HashMap<>();
        Map<TableBucket, Errors> results = new HashMap<>();
        for (Map.Entry<TableBucket, ConsumerOffsetAndMetadata> entry : offsets.entrySet()) {
            TableBucket bucket = entry.getKey();
            if (entry.getValue().isMetadataSizeValid(maxMetadataBytes)) {
                validOffsets.put(bucket, entry.getValue());
                results.put(bucket, Errors.NONE);
            } else {
                results.put(bucket, Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE);
            }
        }
        if (validOffsets.isEmpty()) {
            return CompletableFuture.completedFuture(results);
        }

        CompletableFuture<List<PutKvResultForBucket>> write = new CompletableFuture<>();
        try {
            KvRecordBatch batch =
                    buildUpsertBatch(
                            groupId, validOffsets, offsetsReplica.getTableInfo().getSchemaId());
            Map<TableBucket, KvRecordBatch> batchByBucket =
                    Collections.singletonMap(offsetsReplica.getTableBucket(), batch);
            replicaManager.putRecordsToKv(
                    offsetsConfig.getOffsetCommitTimeoutMs(),
                    OFFSET_COMMIT_REQUIRED_ACKS,
                    batchByBucket,
                    null,
                    MergeMode.DEFAULT,
                    ApiKeys.PUT_KV.highestSupportedVersion,
                    write::complete);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(handleCommitFailure(groupId, results, e));
        }
        return write.thenApply(
                writeResults -> handleWriteCompletion(groupId, results, writeResults));
    }

    /**
     * Reads all or selected committed offsets from the local leader replica on the calling thread.
     */
    public CompletableFuture<Map<TableBucket, ConsumerOffsetAndMetadata>> handleGetConsumerOffsets(
            String groupId, Replica offsetsReplica, List<TableBucket> requestedBuckets) {
        Map<TableBucket, ConsumerOffsetAndMetadata> offsets = new HashMap<>();
        try {
            ValueDecoder decoder = getValueDecoder(offsetsReplica.getSchemaGetter());
            if (requestedBuckets == null) {
                populateAllOffsets(
                        offsetsReplica.prefixLookup(offsetSchema.encodeOffsetCommitPrefix(groupId)),
                        decoder,
                        offsets);
            } else {
                List<byte[]> keys = new ArrayList<>(requestedBuckets.size());
                for (TableBucket tableBucket : requestedBuckets) {
                    keys.add(
                            offsetSchema.encodeOffsetKey(
                                    groupId,
                                    tableBucket.getTableId(),
                                    tableBucket.getPartitionId(),
                                    tableBucket.getBucket()));
                }
                populateSelectedOffsets(
                        requestedBuckets, offsetsReplica.lookups(keys), decoder, offsets);
            }
            return CompletableFuture.completedFuture(offsets);
        } catch (Exception e) {
            LOG.warn("Failed to read offsets for group {}.", groupId, e);
            ApiException exception =
                    e instanceof ApiException
                            ? (ApiException) e
                            : Errors.forException(e).exception(e.getMessage());
            return FutureUtils.completedExceptionally(exception);
        }
    }

    @VisibleForTesting
    ValueDecoder getValueDecoder(SchemaGetter schemaGetter) {
        CachedValueDecoder cached = cachedValueDecoder;
        if (cached == null || cached.schemaGetter != schemaGetter) {
            // Concurrent misses may create two decoders. Each caller keeps its matching pair.
            cached = new CachedValueDecoder(schemaGetter);
            cachedValueDecoder = cached;
        }
        return cached.decoder;
    }

    @VisibleForTesting
    static void populateAllOffsets(
            List<ByteArraySlice> values,
            ValueDecoder valueDecoder,
            Map<TableBucket, ConsumerOffsetAndMetadata> offsets) {
        for (ByteArraySlice value : values) {
            InternalRow row = valueDecoder.decodeValue(value.toByteArray()).row;
            offsets.put(
                    ConsumerOffsetSchema.toTableBucket(row),
                    ConsumerOffsetSchema.toOffsetAndMetadata(row));
        }
    }

    @VisibleForTesting
    static void populateSelectedOffsets(
            List<TableBucket> requestedBuckets,
            List<ByteArraySlice> values,
            ValueDecoder valueDecoder,
            Map<TableBucket, ConsumerOffsetAndMetadata> offsets) {
        for (int i = 0; i < requestedBuckets.size(); i++) {
            ByteArraySlice value = values.get(i);
            if (value == null) {
                offsets.put(requestedBuckets.get(i), null);
                continue;
            }
            InternalRow row = valueDecoder.decodeValue(value.toByteArray()).row;
            offsets.put(requestedBuckets.get(i), ConsumerOffsetSchema.toOffsetAndMetadata(row));
        }
    }

    @VisibleForTesting
    static Map<TableBucket, Errors> handleWriteCompletion(
            String groupId,
            Map<TableBucket, Errors> results,
            List<PutKvResultForBucket> writeResults) {
        try {
            for (PutKvResultForBucket writeResult : writeResults) {
                Errors error = writeResult.getError().error();
                if (error != Errors.NONE) {
                    return applyWriteError(results, mapCommitError(error));
                }
            }
            return results;
        } catch (Exception e) {
            return handleCommitFailure(groupId, results, e);
        }
    }

    private static Map<TableBucket, Errors> handleCommitFailure(
            String groupId, Map<TableBucket, Errors> results, Exception exception) {
        LOG.error("Failed to commit offsets for group {}.", groupId, exception);
        return applyWriteError(results, mapCommitError(Errors.forException(exception)));
    }

    private KvRecordBatch buildUpsertBatch(
            String groupId, Map<TableBucket, ConsumerOffsetAndMetadata> offsets, int schemaId)
            throws Exception {
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(OFFSET_COMMIT_PAGE_SIZE),
                        KvFormat.COMPACTED);
        for (Map.Entry<TableBucket, ConsumerOffsetAndMetadata> entry : offsets.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            CompactedRow row =
                    offsetSchema.encodeOffsetRow(
                            groupId,
                            OFFSET_COMMIT_RECORD_TYPE,
                            tableBucket.getTableId(),
                            tableBucket.getPartitionId(),
                            tableBucket.getBucket(),
                            entry.getValue());
            byte[] key = offsetSchema.encodeOffsetKey(row);
            builder.append(key, row);
        }
        return DefaultKvRecordBatch.pointToBytesView(builder.build());
    }

    /**
     * Returns the group's owner replica, the local online leader for its offsets-table bucket. If
     * the local replica is missing, offline, or a follower, the cached leader determines the
     * exception.
     *
     * @throws ConsumerGroupOwnerNotAvailableException if no leader is known
     * @throws ConsumerGroupOwnerLoadingException if this server is the expected leader
     * @throws NotConsumerGroupOwnerException if another server is the expected leader
     */
    private Replica checkOwnership(TableBucket offsetTableBucket) {
        HostedReplica hostedReplica = replicaManager.getReplica(offsetTableBucket);
        if (hostedReplica instanceof OnlineReplica
                && ((OnlineReplica) hostedReplica).getReplica().isLeader()) {
            return ((OnlineReplica) hostedReplica).getReplica();
        }

        int leaderId =
                metadataCache
                        .getCachedBucketMetadata(
                                SystemTables.CONSUMER_OFFSETS, offsetTableBucket.getBucket())
                        .map(bucket -> bucket.getLeaderId().orElse(NO_LEADER))
                        .orElse(NO_LEADER);
        if (leaderId == NO_LEADER) {
            throw Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE.exception();
        }
        if (leaderId == serverId) {
            throw Errors.CONSUMER_GROUP_OWNER_LOADING.exception();
        }
        throw Errors.NOT_CONSUMER_GROUP_OWNER.exception();
    }

    private static Map<TableBucket, Errors> applyWriteError(
            Map<TableBucket, Errors> results, Errors writeError) {
        results.replaceAll((bucket, error) -> error == Errors.NONE ? writeError : error);
        return results;
    }

    static Errors mapCommitError(Errors error) {
        switch (error) {
            case UNKNOWN_TABLE_OR_BUCKET_EXCEPTION:
                return Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE;
            case NOT_LEADER_OR_FOLLOWER:
                return Errors.NOT_CONSUMER_GROUP_OWNER;
            default:
                return error;
        }
    }

    private static final class CachedValueDecoder {
        private final SchemaGetter schemaGetter;
        private final ValueDecoder decoder;

        private CachedValueDecoder(SchemaGetter schemaGetter) {
            this.schemaGetter = schemaGetter;
            this.decoder = ConsumerOffsetSchema.createValueDecoder(schemaGetter);
        }
    }
}
