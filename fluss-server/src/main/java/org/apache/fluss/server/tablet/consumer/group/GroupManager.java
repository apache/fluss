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
import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConsumerGroupOwnerLoadingException;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.DiskWriteLockedException;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.exception.NotConsumerGroupOwnerException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.StorageBackpressureException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.replica.ReplicaManager.HostedReplica;
import org.apache.fluss.server.replica.ReplicaManager.OnlineReplica;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetSchema.OFFSET_COMMIT_RECORD_TYPE;
import static org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetTableConfig.OFFSET_COMMIT_REQUIRED_ACKS;
import static org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetTableConfig.UNRESOLVED_SCHEMA_ID;
import static org.apache.fluss.server.tablet.consumer.group.ConsumerOffsetTableConfig.UNRESOLVED_TABLE_ID;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Validates, routes, and commits consumer-group offsets. */
@Internal
public final class GroupManager {

    private static final int OFFSET_COMMIT_PAGE_SIZE = 4 * 1024;

    private static final Logger LOG = LoggerFactory.getLogger(GroupManager.class);

    private final int serverId;
    private final ConsumerOffsetTableConfig consumerOffsetTableConfig;
    private final ReplicaManager replicaManager;
    private final MetadataManager metadataManager;
    private final ZooKeeperClient zkClient;

    private volatile long consumerOffsetTableId = UNRESOLVED_TABLE_ID;
    private volatile int consumerOffsetSchemaId = UNRESOLVED_SCHEMA_ID;

    public GroupManager(
            int serverId,
            ConsumerOffsetTableConfig consumerOffsetTableConfig,
            ReplicaManager replicaManager,
            MetadataManager metadataManager,
            ZooKeeperClient zkClient) {
        this.serverId = serverId;
        this.consumerOffsetTableConfig =
                checkNotNull(
                        consumerOffsetTableConfig, "consumerOffsetTableConfig must not be null");
        this.replicaManager = checkNotNull(replicaManager, "replicaManager must not be null");
        this.metadataManager = checkNotNull(metadataManager, "metadataManager must not be null");
        this.zkClient = checkNotNull(zkClient, "zkClient must not be null");
    }

    public static GroupManager from(
            Configuration configuration,
            int serverId,
            ReplicaManager replicaManager,
            MetadataManager metadataManager,
            ZooKeeperClient zkClient) {
        return new GroupManager(
                serverId,
                ConsumerOffsetTableConfig.from(configuration),
                replicaManager,
                metadataManager,
                zkClient);
    }

    public ConsumerOffsetTableConfig consumerOffsetTableConfig() {
        return consumerOffsetTableConfig;
    }

    /** Maps a group id to a bucket using the actual system-table bucket count. */
    public int bucketFor(String groupId, int bucketCount) {
        validateGroupId(groupId);
        byte[] bucketKey = ConsumerOffsetSchema.encodeGroupPrefix(groupId);
        return FlussBucketingFunction.bucketForRowKey(bucketKey, bucketCount);
    }

    /** Validates a consumer group id before routing or accessing its offsets. */
    public void validateGroupId(String groupId) {
        if (groupId == null || groupId.isEmpty()) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must not be null or empty.");
        }
    }

    /** Commits offsets and completes the callback exactly once with per-bucket results. */
    public void handleCommitConsumerOffsets(
            String groupId,
            Map<TableBucket, ConsumerOffsetAndMetadata> requestedOffsets,
            int bucketCount,
            Consumer<Map<TableBucket, Errors>> callback) {
        Map<TableBucket, ConsumerOffsetAndMetadata> offsets =
                new LinkedHashMap<>(
                        checkNotNull(requestedOffsets, "requestedOffsets must not be null"));
        try {
            doCommitConsumerOffsets(groupId, offsets, bucketCount, callback);
        } catch (Exception e) {
            logCommitFailure(groupId, e);
            completeCallback(
                    groupId,
                    callback,
                    buildCommitResult(offsets, Collections.emptyMap(), Errors.forException(e)));
        }
    }

    private void doCommitConsumerOffsets(
            String groupId,
            Map<TableBucket, ConsumerOffsetAndMetadata> offsets,
            int bucketCount,
            Consumer<Map<TableBucket, Errors>> callback)
            throws Exception {
        int bucketId = bucketFor(groupId, bucketCount);
        TableBucket offsetTableBucket = new TableBucket(consumerOffsetTableId(), bucketId);

        Errors ownershipError = checkOwnership(offsetTableBucket);
        if (ownershipError != Errors.NONE) {
            completeCallback(
                    groupId,
                    callback,
                    buildCommitResult(offsets, Collections.emptyMap(), ownershipError));
            return;
        }

        Map<TableBucket, ConsumerOffsetAndMetadata> validOffsets = new LinkedHashMap<>();
        Map<TableBucket, Errors> validationErrors = new LinkedHashMap<>();
        for (Map.Entry<TableBucket, ConsumerOffsetAndMetadata> entry : offsets.entrySet()) {
            if (isOffsetMetadataSizeValid(
                    entry.getValue().getMetadata(),
                    consumerOffsetTableConfig.getMaxMetadataSize().getBytes())) {
                validOffsets.put(entry.getKey(), entry.getValue());
            } else {
                validationErrors.put(entry.getKey(), Errors.CONSUMER_OFFSET_METADATA_TOO_LARGE);
            }
        }
        if (validOffsets.isEmpty()) {
            completeCallback(
                    groupId, callback, buildCommitResult(offsets, validationErrors, Errors.NONE));
            return;
        }

        try {
            KvRecordBatch batch = buildUpsertBatch(groupId, validOffsets);
            Map<TableBucket, KvRecordBatch> batchByBucket =
                    Collections.singletonMap(offsetTableBucket, batch);
            replicaManager.putRecordsToKv(
                    (int) consumerOffsetTableConfig.getOffsetCommitTimeout().toMillis(),
                    OFFSET_COMMIT_REQUIRED_ACKS,
                    batchByBucket,
                    null,
                    MergeMode.DEFAULT,
                    (short) 1,
                    writeResults ->
                            handleWriteCompletion(
                                    groupId, offsets, validationErrors, writeResults, callback));
        } catch (Exception e) {
            logCommitFailure(groupId, e);
            completeCallback(
                    groupId,
                    callback,
                    buildCommitResult(
                            offsets, validationErrors, mapCommitError(Errors.forException(e))));
        }
    }

    private void logCommitFailure(String groupId, Exception exception) {
        if (isExpectedCommitFailure(exception)) {
            LOG.debug(
                    "Offset commit rejected for group {}: {} ({}).",
                    groupId,
                    Errors.forException(exception),
                    exception.getMessage());
        } else {
            LOG.error("Unexpected error committing offsets for group {}.", groupId, exception);
        }
    }

    private boolean isExpectedCommitFailure(Exception exception) {
        Throwable cause = exception.getCause();
        if (cause != null) {
            // Metadata lookup wraps all failures as owner unavailable. Only a missing table or
            // schema is expected; keep infrastructure failures and programming errors visible.
            return exception instanceof ConsumerGroupOwnerNotAvailableException
                    && cause.getCause() == null
                    && (cause instanceof TableNotExistException
                            || cause instanceof SchemaNotExistException);
        }
        return exception instanceof InvalidConsumerGroupIdException
                || exception instanceof ConsumerGroupOwnerNotAvailableException
                || exception instanceof ConsumerGroupOwnerLoadingException
                || exception instanceof NotConsumerGroupOwnerException
                || exception instanceof UnknownTableOrBucketException
                || exception instanceof NotLeaderOrFollowerException
                || exception instanceof DiskWriteLockedException
                || exception instanceof StorageBackpressureException;
    }

    private void handleWriteCompletion(
            String groupId,
            Map<TableBucket, ConsumerOffsetAndMetadata> offsets,
            Map<TableBucket, Errors> validationErrors,
            List<PutKvResultForBucket> writeResults,
            Consumer<Map<TableBucket, Errors>> callback) {
        Map<TableBucket, Errors> result;
        try {
            Errors writeError = Errors.NONE;
            for (PutKvResultForBucket writeResult : writeResults) {
                if (writeResult.getError().error() != Errors.NONE) {
                    writeError = mapCommitError(writeResult.getError().error());
                    break;
                }
            }
            result = buildCommitResult(offsets, validationErrors, writeError);
        } catch (Exception e) {
            LOG.error("Failed to process offset commit result for group {}.", groupId, e);
            result = buildCommitResult(offsets, validationErrors, Errors.UNKNOWN_SERVER_ERROR);
        }
        completeCallback(groupId, callback, result);
    }

    private void completeCallback(
            String groupId,
            Consumer<Map<TableBucket, Errors>> callback,
            Map<TableBucket, Errors> result) {
        try {
            callback.accept(result);
        } catch (Exception e) {
            LOG.error("Consumer offset commit callback failed for group {}.", groupId, e);
        }
    }

    private KvRecordBatch buildUpsertBatch(
            String groupId, Map<TableBucket, ConsumerOffsetAndMetadata> offsets) throws Exception {
        KeyEncoder keyEncoder = ConsumerOffsetSchema.createOffsetKeyEncoder();
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        consumerOffsetSchemaId(),
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(OFFSET_COMMIT_PAGE_SIZE),
                        KvFormat.COMPACTED);
        for (Map.Entry<TableBucket, ConsumerOffsetAndMetadata> entry : offsets.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            CompactedRow row =
                    ConsumerOffsetSchema.encodeOffsetRow(
                            groupId,
                            OFFSET_COMMIT_RECORD_TYPE,
                            tableBucket.getTableId(),
                            tableBucket.getPartitionId(),
                            tableBucket.getBucket(),
                            entry.getValue());
            byte[] key = ConsumerOffsetSchema.encodeOffsetKey(keyEncoder, row);
            builder.append(key, row);
        }
        return DefaultKvRecordBatch.pointToBytesView(builder.build());
    }

    private Errors checkOwnership(TableBucket offsetTableBucket) {
        HostedReplica hostedReplica = replicaManager.getReplica(offsetTableBucket);
        if (hostedReplica instanceof OnlineReplica
                && ((OnlineReplica) hostedReplica).getReplica().isLeader()) {
            return Errors.NONE;
        }

        try {
            Optional<LeaderAndIsr> leaderAndIsr = zkClient.getLeaderAndIsr(offsetTableBucket);
            if (!leaderAndIsr.isPresent()) {
                return Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE;
            }
            return leaderAndIsr.get().leader() == serverId
                    ? Errors.CONSUMER_GROUP_OWNER_LOADING
                    : Errors.NOT_CONSUMER_GROUP_OWNER;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to resolve consumer group owner for bucket {}.",
                    offsetTableBucket.getBucket(),
                    e);
            return Errors.CONSUMER_GROUP_OWNER_NOT_AVAILABLE;
        }
    }

    private long consumerOffsetTableId() {
        if (consumerOffsetTableId == UNRESOLVED_TABLE_ID) {
            synchronized (this) {
                if (consumerOffsetTableId == UNRESOLVED_TABLE_ID) {
                    try {
                        consumerOffsetTableId =
                                metadataManager
                                        .getTable(SystemTables.CONSUMER_OFFSETS)
                                        .getTableId();
                    } catch (Exception e) {
                        throw new ConsumerGroupOwnerNotAvailableException(
                                "Consumer offset table id is not available.", e);
                    }
                }
            }
        }
        return consumerOffsetTableId;
    }

    private int consumerOffsetSchemaId() {
        if (consumerOffsetSchemaId == UNRESOLVED_SCHEMA_ID) {
            synchronized (this) {
                if (consumerOffsetSchemaId == UNRESOLVED_SCHEMA_ID) {
                    try {
                        consumerOffsetSchemaId =
                                metadataManager
                                        .getLatestSchema(SystemTables.CONSUMER_OFFSETS)
                                        .getSchemaId();
                    } catch (Exception e) {
                        throw new ConsumerGroupOwnerNotAvailableException(
                                "Consumer offset table schema is not available.", e);
                    }
                }
            }
        }
        return consumerOffsetSchemaId;
    }

    static boolean isOffsetMetadataSizeValid(String metadata, long maxMetadataSizeBytes) {
        return metadata == null
                || metadata.getBytes(StandardCharsets.UTF_8).length <= maxMetadataSizeBytes;
    }

    static Map<TableBucket, Errors> buildCommitResult(
            Map<TableBucket, ConsumerOffsetAndMetadata> offsets,
            Map<TableBucket, Errors> bucketErrors,
            Errors defaultError) {
        Map<TableBucket, Errors> result = new LinkedHashMap<>();
        offsets.keySet()
                .forEach(
                        tableBucket ->
                                result.put(
                                        tableBucket,
                                        bucketErrors.containsKey(tableBucket)
                                                ? bucketErrors.get(tableBucket)
                                                : defaultError));
        return result;
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
}
