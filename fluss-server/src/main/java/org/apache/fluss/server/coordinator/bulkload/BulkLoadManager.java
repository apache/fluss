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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.BulkLoadStateException;
import org.apache.fluss.exception.BulkLoadTargetWithdrawnException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.GetBulkLoadInfoResponse;
import org.apache.fluss.rpc.messages.ListInProgressBulkLoadsResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetProgress;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.event.CommitBulkLoadBucketEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.ReclaimTableBulkLoadMetadataEvent;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.zk.BulkLoadTransactionStore;
import org.apache.fluss.server.zk.BulkLoadTransactionStore.BucketProgress;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.concurrent.FutureUtils.runAsync;

/**
 * Coordinates BulkLoad transactions and bucket snapshot imports. Transaction mutations run on the
 * Coordinator event thread. Reading transaction progress and preparing bucket operations run on I/O
 * threads.
 */
public final class BulkLoadManager {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadManager.class);

    private final ZooKeeperClient zooKeeperClient;
    private final BulkLoadTransactionStore transactionStore;
    private final int coordinatorEpochZkVersion;
    private final Executor ioExecutor;
    private @Nullable ScheduledFuture<?> metadataReclamationTask;

    public BulkLoadManager(
            ZooKeeperClient zooKeeperClient, int coordinatorEpochZkVersion, Executor ioExecutor) {
        this.zooKeeperClient = zooKeeperClient;
        this.transactionStore = new BulkLoadTransactionStore(zooKeeperClient);
        this.coordinatorEpochZkVersion = coordinatorEpochZkVersion;
        this.ioExecutor = ioExecutor;
    }

    /** Establishes ownership of the selected physical targets and fixes their build context. */
    public BeginBulkLoadResponse begin(
            TableInfo table,
            @Nullable String bulkLoadId,
            List<String> partitionNames,
            int maxTargets,
            Function<String, BulkLoadTarget> resolvePartitionTarget)
            throws Exception {
        requireDistinctPartitions(partitionNames);
        String id = bulkLoadId == null ? UUID.randomUUID().toString() : bulkLoadId;
        long tableId = table.getTableId();
        if (bulkLoadId != null) {
            Optional<VersionedData<BulkLoadTransaction>> previous =
                    zooKeeperClient.getBulkLoadTransaction(tableId, id);
            if (previous.isPresent()) {
                return replayBegin(previous.get().data(), partitionNames, table);
            }
        }

        if (!table.hasPrimaryKey()) {
            throw new InvalidRequestException("BulkLoad requires a primary key table.");
        }
        if (!table.getSchema().getAutoIncrementColumnNames().isEmpty()) {
            throw new InvalidRequestException("BulkLoad does not support auto-increment columns.");
        }
        if (table.getTableConfig().getMergeEngineType().isPresent()) {
            throw new InvalidRequestException(
                    "BulkLoad does not support tables with a merge engine.");
        }
        if (table.isPartitioned() == partitionNames.isEmpty()) {
            throw new InvalidRequestException("Invalid partition selection for this table.");
        }
        if (Math.max(1, partitionNames.size()) > maxTargets) {
            throw new InvalidRequestException("BulkLoad exceeds max-targets.");
        }

        if (partitionNames.contains(HISTORICAL_PARTITION_VALUE)) {
            throw new InvalidRequestException("Historical partitions are unsupported.");
        }
        List<BulkLoadTarget> targets =
                resolveBeginTargets(table, partitionNames, resolvePartitionTarget);
        BulkLoadTransaction transaction =
                new BulkLoadTransaction(
                        tableId,
                        id,
                        table.getTablePath(),
                        table.getSchemaId(),
                        table.getRemoteDataDir(),
                        BulkLoadTransaction.RUNNING,
                        System.currentTimeMillis(),
                        null,
                        null);
        try {
            transactionStore.begin(transaction, targets, coordinatorEpochZkVersion);
        } catch (KeeperException.NodeExistsException e) {
            // Curator may retry a Begin whose first write succeeded without a response.
            Optional<VersionedData<BulkLoadTransaction>> current =
                    zooKeeperClient.getBulkLoadTransaction(tableId, id);
            if (current.isPresent()) {
                BulkLoadTransaction existing = current.get().data();
                return replayBegin(existing, partitionNames, table);
            }
            throw e;
        }
        return beginResponse(transaction, targets, table, table.toTableDescriptor());
    }

    /** Returns progress for the original targets, including withdrawn partitions. */
    public GetBulkLoadInfoResponse getBulkLoadInfo(long tableId, String bulkLoadId)
            throws Exception {
        BulkLoadTransaction transaction = getTransaction(tableId, bulkLoadId).data();
        TableRegistration table = getOriginalTableWithVersion(transaction).data();
        List<BulkLoadTarget> targets =
                transactionStore.getTargets(transaction, table.isPartitioned());
        if (targets.isEmpty()) {
            // Reclamation may finish between the transaction and target reads.
            throw new BulkLoadNotFoundException("BulkLoad transaction does not exist.");
        }
        boolean running = transaction.state == BulkLoadTransaction.RUNNING;
        Map<String, PartitionRegistration> currentPartitions =
                running && table.isPartitioned()
                        ? getCurrentPartitions(transaction, targets)
                        : Collections.emptyMap();
        GetBulkLoadInfoResponse response =
                new GetBulkLoadInfoResponse().setStatus(status(transaction));
        for (BulkLoadTarget target : targets) {
            BucketProgress buckets =
                    transactionStore.getBucketProgress(transaction, target.partitionId);
            PbBulkLoadTargetProgress progress =
                    response.addTarget()
                            .setBucketCount(target.bucketCount)
                            .setAllocatedBucketCount(buckets.allocated)
                            .setCommittedBucketCount(buckets.committed)
                            .setWithdrawn(target.withdrawn);
            if (target.partitionId != null) {
                progress.setPartitionId(target.partitionId);
                if (running && !target.withdrawn) {
                    PartitionRegistration partition = currentPartitions.get(target.partitionName);
                    progress.setWithdrawn(
                            partition == null || partition.getPartitionId() != target.partitionId);
                }
            }
        }
        return response;
    }

    /** Lists the running transactions owning any of the selected current physical targets. */
    public ListInProgressBulkLoadsResponse listInProgressBulkLoads(
            TablePath path, List<String> partitionNames) throws Exception {
        requireDistinctPartitions(partitionNames);
        TableRegistration table =
                zooKeeperClient
                        .getTable(path)
                        .orElseThrow(
                                () ->
                                        new TableNotExistException(
                                                "Table " + path + " does not exist."));
        if (!table.isPartitioned() && !partitionNames.isEmpty()) {
            throw new InvalidRequestException("Non-partitioned target has partition names.");
        }

        List<Long> ids = new ArrayList<>();
        if (table.isPartitioned()) {
            Collection<String> selected =
                    partitionNames.isEmpty() ? zooKeeperClient.getPartitions(path) : partitionNames;
            Map<String, PartitionRegistration> registrations =
                    zooKeeperClient.getPartitionRegistrations(path, selected);
            for (String name : selected) {
                PartitionRegistration partition = registrations.get(name);
                if (partition != null && partition.getTableId() == table.tableId) {
                    ids.add(partition.getPartitionId());
                } else if (!partitionNames.isEmpty()) {
                    throw partitionNotFound(name);
                }
            }
        } else {
            ids.add(null);
        }

        ListInProgressBulkLoadsResponse response = new ListInProgressBulkLoadsResponse();
        for (BulkLoadTransaction transaction :
                transactionStore.listRunningTransactions(table.tableId, ids)) {
            response.addTransaction().copyFrom(status(transaction));
        }
        return response;
    }

    /** Ends the transaction, or returns its existing matching terminal result. */
    public PbBulkLoadStatus finishTransaction(
            long tableId,
            String bulkLoadId,
            int terminalState,
            @Nullable String abortMessage,
            @Nullable TableInfo table,
            Predicate<TablePartition> partitionExists)
            throws Exception {
        VersionedData<BulkLoadTransaction> transaction = getTransaction(tableId, bulkLoadId);
        BulkLoadTransaction original = transaction.data();
        if (table == null) {
            throw originalTableNotFound();
        }
        if (original.state == terminalState) {
            return status(original);
        }
        requireRunning(original);
        List<BulkLoadTarget> targets = transactionStore.getTargets(original, table.isPartitioned());
        List<BulkLoadTarget> newlyWithdrawnTargets = new ArrayList<>();
        for (int i = 0; i < targets.size(); i++) {
            BulkLoadTarget target = targets.get(i);
            if (!target.withdrawn
                    && target.partitionId != null
                    && !partitionExists.test(new TablePartition(tableId, target.partitionId))) {
                BulkLoadTarget withdrawn = target.withdraw();
                targets.set(i, withdrawn);
                newlyWithdrawnTargets.add(withdrawn);
            }
        }
        if (!newlyWithdrawnTargets.isEmpty()) {
            transactionStore.persistWithdrawnTargets(
                    original, newlyWithdrawnTargets, coordinatorEpochZkVersion);
        }
        try {
            return status(
                    transactionStore.finishTransaction(
                            transaction,
                            targets,
                            terminalState,
                            abortMessage,
                            coordinatorEpochZkVersion));
        } catch (KeeperException.BadVersionException e) {
            BulkLoadTransaction current =
                    getTransaction(original.tableId, original.bulkLoadId).data();
            if (current.state == terminalState) {
                return status(current);
            }
            throw e;
        }
    }

    /** Schedules periodic metadata reclamation for the Coordinator leadership term. */
    public void startMetadataReclamation(
            Scheduler scheduler, EventManager eventManager, long retentionMillis) {
        AtomicBoolean sweepRunning = new AtomicBoolean();
        metadataReclamationTask =
                scheduler.schedule(
                        "expire-bulk-load-results",
                        () ->
                                scheduleMetadataReclamation(
                                        eventManager, retentionMillis, sweepRunning),
                        0,
                        Duration.ofHours(1).toMillis());
    }

    /** Cancels the periodic trigger before the Coordinator event queue closes. */
    public void stopMetadataReclamation() {
        if (metadataReclamationTask != null) {
            metadataReclamationTask.cancel(false);
        }
    }

    /** Enqueues reclamation for a dropped physical table without waiting for the next sweep. */
    public void onTableDropped(long tableId, long retentionMillis, EventManager eventManager) {
        enqueueTableMetadataReclamation(
                tableId, System.currentTimeMillis() - retentionMillis, eventManager);
    }

    /** Discovers tables and enqueues one metadata reclamation event at a time. */
    public CompletableFuture<Void> enqueueMetadataReclamation(
            long retentionCutoff, EventManager eventManager) throws Exception {
        CompletableFuture<Void> sweep = CompletableFuture.completedFuture(null);
        // Other Coordinator events can run between tables.
        for (long tableId : transactionStore.listTableIdsWithBulkLoadMetadata()) {
            sweep =
                    sweep.thenCompose(
                            ignored ->
                                    enqueueTableMetadataReclamation(
                                            tableId, retentionCutoff, eventManager));
        }
        return sweep;
    }

    /** Reclaims eligible BulkLoad metadata for one physical table. */
    public void reclaimTableBulkLoadMetadata(
            long tableId, long retentionCutoff, boolean tableDropped, boolean hasPendingPublication)
            throws Exception {
        boolean hasRetainedResults = false;
        for (VersionedData<BulkLoadTransaction> storedTransaction :
                transactionStore.getTransactionsForTable(tableId)) {
            BulkLoadTransaction transaction = storedTransaction.data();
            boolean expired =
                    transaction.finishedTime != null && transaction.finishedTime <= retentionCutoff;
            if (tableDropped || (expired && !hasPendingPublication)) {
                transactionStore.reclaimTransaction(storedTransaction, coordinatorEpochZkVersion);
            } else {
                hasRetainedResults = true;
            }
        }
        if (!hasRetainedResults) {
            transactionStore.pruneTableDirectories(tableId, coordinatorEpochZkVersion);
        }
    }

    /**
     * Allocates a snapshot at the bucket's current recovery offset, or returns its existing
     * allocation.
     */
    public BulkLoadBucket allocateBucketSnapshot(String bulkLoadId, TableBucket bucket)
            throws Exception {
        VersionedData<BulkLoadTransaction> transaction =
                getTransaction(bucket.getTableId(), bulkLoadId);
        BulkLoadTransaction original = transaction.data();
        VersionedData<TableRegistration> table = getOriginalTableWithVersion(original);
        BulkLoadBucket existing = zooKeeperClient.getBulkLoadBucket(original, bucket).orElse(null);
        if (existing != null && existing.state == BulkLoadBucket.COMMITTED) {
            return existing;
        }

        VersionedData<BulkLoadTarget> target = resolveTarget(original, bucket);
        requireRunning(original);
        Integer partitionVersion = validateBuildContext(original, target.data(), table.data());
        if (existing != null) {
            return existing;
        }
        long snapshotId = zooKeeperClient.allocateBucketSnapshotId(bucket);
        long logOffset =
                zooKeeperClient
                        .getTableBucketLatestSnapshot(bucket)
                        .map(BucketSnapshot::getLogOffset)
                        .orElse(0L);
        BulkLoadBucket allocation =
                new BulkLoadBucket(snapshotId, logOffset, BulkLoadBucket.ALLOCATED);
        try {
            zooKeeperClient.allocateBulkLoadBucket(
                    transaction,
                    target,
                    bucket,
                    allocation,
                    table.zkVersion(),
                    partitionVersion,
                    coordinatorEpochZkVersion);
            return allocation;
        } catch (KeeperException.NodeExistsException e) {
            // The preceding checks succeeded; only creating the bucket record can report
            // NodeExists.
            Optional<BulkLoadBucket> winner = zooKeeperClient.getBulkLoadBucket(original, bucket);
            if (winner.isPresent()) {
                return winner.get();
            }
            checkMutationTarget(original, target.data());
            throw e;
        } catch (KeeperException.NoNodeException | KeeperException.BadVersionException e) {
            checkMutationTarget(original, target.data());
            throw e;
        }
    }

    /** Publishes an allocated snapshot, or acknowledges an already committed bucket. */
    public CompletableFuture<Void> commitBucketSnapshot(
            String bulkLoadId,
            TableBucket bucket,
            CompletedSnapshotStoreManager snapshotStores,
            EventManager eventManager)
            throws Exception {
        VersionedData<BulkLoadTransaction> transaction =
                getTransaction(bucket.getTableId(), bulkLoadId);
        BulkLoadTransaction bulkLoadTransaction = transaction.data();
        VersionedData<TableRegistration> currentTable =
                getOriginalTableWithVersion(bulkLoadTransaction);
        Optional<BulkLoadBucket> record =
                zooKeeperClient.getBulkLoadBucket(bulkLoadTransaction, bucket);
        if (record.isPresent() && record.get().state == BulkLoadBucket.COMMITTED) {
            return CompletableFuture.completedFuture(null);
        }
        VersionedData<BulkLoadTarget> target = resolveTarget(bulkLoadTransaction, bucket);
        BulkLoadBucket allocation =
                record.orElseThrow(
                        () -> new InvalidRequestException("Bucket has not been allocated."));
        TablePath tablePath = bulkLoadTransaction.tablePath;
        requireRunning(bulkLoadTransaction);
        Integer partitionVersion =
                validateBuildContext(bulkLoadTransaction, target.data(), currentTable.data());
        CompletedSnapshot snapshot =
                BulkLoadSnapshotValidator.validate(tablePath, target.data(), bucket, allocation);
        CommitBulkLoadBucketEvent event =
                new CommitBulkLoadBucketEvent(
                        snapshot,
                        transaction,
                        target,
                        currentTable.zkVersion(),
                        partitionVersion,
                        coordinatorEpochZkVersion,
                        snapshotStores.getOrCreateCompletedSnapshotStore(tablePath, bucket));
        eventManager.put(event);
        return event.getResultFuture()
                .handle(
                        (ignored, failure) -> {
                            if (failure instanceof KeeperException.NoNodeException
                                    || failure instanceof KeeperException.BadVersionException) {
                                return runAsync(
                                        () -> {
                                            checkMutationTarget(bulkLoadTransaction, target.data());
                                            throw (KeeperException) failure;
                                        },
                                        ioExecutor);
                            }
                            return event.getResultFuture();
                        })
                .thenCompose(result -> result);
    }

    private void scheduleMetadataReclamation(
            EventManager eventManager, long retentionMillis, AtomicBoolean sweepRunning) {
        if (!sweepRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            ioExecutor.execute(
                    () -> enqueuePeriodicReclamation(eventManager, retentionMillis, sweepRunning));
        } catch (RejectedExecutionException ignored) {
            sweepRunning.set(false);
        }
    }

    private void enqueuePeriodicReclamation(
            EventManager eventManager, long retentionMillis, AtomicBoolean sweepRunning) {
        try {
            enqueueMetadataReclamation(System.currentTimeMillis() - retentionMillis, eventManager)
                    .whenComplete((ignored, failure) -> sweepRunning.set(false));
        } catch (Exception e) {
            sweepRunning.set(false);
            LOG.warn("Failed to discover BulkLoad tables for maintenance.", e);
        }
    }

    private CompletableFuture<Void> enqueueTableMetadataReclamation(
            long tableId, long retentionCutoff, EventManager eventManager) {
        ReclaimTableBulkLoadMetadataEvent event =
                new ReclaimTableBulkLoadMetadataEvent(tableId, retentionCutoff);
        eventManager.put(event);
        return event.getResultFuture()
                .exceptionally(failure -> handleReclamationFailure(tableId, failure));
    }

    private Void handleReclamationFailure(long tableId, Throwable failure) {
        Throwable cause = ExceptionUtils.stripCompletionException(failure);
        if (cause instanceof NotCoordinatorLeaderException) {
            throw new CompletionException(cause);
        }
        LOG.warn("Failed to maintain BulkLoad metadata for table {}.", tableId, cause);
        return null;
    }

    private Map<String, PartitionRegistration> getCurrentPartitions(
            BulkLoadTransaction transaction, List<BulkLoadTarget> targets) throws Exception {
        List<String> partitionNames = new ArrayList<>();
        for (BulkLoadTarget target : targets) {
            if (!target.withdrawn) {
                partitionNames.add(target.partitionName);
            }
        }
        return zooKeeperClient.getPartitionRegistrations(transaction.tablePath, partitionNames);
    }

    private BeginBulkLoadResponse replayBegin(
            BulkLoadTransaction transaction, List<String> partitionNames, TableInfo table)
            throws Exception {
        List<BulkLoadTarget> targets =
                transactionStore.getTargets(transaction, table.isPartitioned());
        Set<String> original = new HashSet<>();
        for (BulkLoadTarget target : targets) {
            if (target.partitionName != null) {
                original.add(target.partitionName);
            }
        }
        if (!original.equals(new HashSet<>(partitionNames))) {
            throw new InvalidRequestException("bulk_load_id has different targets.");
        }
        TableDescriptor descriptor = table.toTableDescriptor();
        if (table.getSchemaId() != transaction.schemaId) {
            SchemaInfo schema =
                    zooKeeperClient
                            .getSchemaById(transaction.tablePath, transaction.schemaId)
                            .orElseThrow(
                                    () ->
                                            new SchemaNotExistException(
                                                    String.format(
                                                            "Schema %d of table %s does not exist.",
                                                            transaction.schemaId,
                                                            transaction.tablePath)));
            descriptor = TableDescriptor.builder(descriptor).schema(schema.getSchema()).build();
        }
        return beginResponse(transaction, targets, table, descriptor);
    }

    private static List<BulkLoadTarget> resolveBeginTargets(
            TableInfo table,
            List<String> partitionNames,
            Function<String, BulkLoadTarget> resolvePartitionTarget) {
        if (!table.isPartitioned()) {
            return Collections.singletonList(
                    new BulkLoadTarget(
                            null, null, table.getNumBuckets(), table.getRemoteDataDir(), false));
        }
        List<BulkLoadTarget> targets = new ArrayList<>();
        for (String name : partitionNames) {
            BulkLoadTarget target = resolvePartitionTarget.apply(name);
            if (target == null) {
                throw partitionNotFound(name);
            }
            targets.add(target);
        }
        targets.sort(Comparator.comparing(target -> target.partitionId));
        return targets;
    }

    private static BeginBulkLoadResponse beginResponse(
            BulkLoadTransaction transaction,
            List<BulkLoadTarget> targets,
            TableInfo table,
            TableDescriptor descriptor) {
        BeginBulkLoadResponse response = new BeginBulkLoadResponse();
        response.setStatus(status(transaction));
        response.setTableInfo()
                .setTableId(table.getTableId())
                .setSchemaId(transaction.schemaId)
                .setTableJson(descriptor.toJsonBytes())
                .setCreatedTime(table.getCreatedTime())
                .setModifiedTime(table.getModifiedTime())
                .setBucketCountEpoch(table.getBucketCountEpoch())
                .setRemoteDataDir(transaction.remoteDataDir);
        for (BulkLoadTarget target : targets) {
            PbBulkLoadTargetInfo info =
                    response.addTarget()
                            .setBucketCount(target.bucketCount)
                            .setRemoteDataDir(target.remoteDataDir);
            if (target.partitionId != null) {
                info.setPartitionId(target.partitionId).setPartitionName(target.partitionName);
            }
        }
        return response;
    }

    private static PbBulkLoadStatus status(BulkLoadTransaction transaction) {
        PbBulkLoadStatus status =
                new PbBulkLoadStatus()
                        .setState(transaction.state)
                        .setCreatedTime(transaction.createdTime);
        status.setHandle().setTableId(transaction.tableId).setBulkLoadId(transaction.bulkLoadId);
        if (transaction.finishedTime != null) {
            status.setFinishedTime(transaction.finishedTime);
        }
        if (transaction.abortMessage != null) {
            status.setAbortMessage(transaction.abortMessage);
        }
        return status;
    }

    private static void requireDistinctPartitions(List<String> partitionNames) {
        if (new HashSet<>(partitionNames).size() != partitionNames.size()) {
            throw new InvalidRequestException("Duplicate partition names.");
        }
    }

    private static PartitionNotExistException partitionNotFound(String name) {
        return new PartitionNotExistException("Partition " + name + " does not exist.");
    }

    private VersionedData<BulkLoadTransaction> getTransaction(long tableId, String bulkLoadId)
            throws Exception {
        return zooKeeperClient
                .getBulkLoadTransaction(tableId, bulkLoadId)
                .orElseThrow(
                        () ->
                                new BulkLoadNotFoundException(
                                        "BulkLoad transaction does not exist."));
    }

    private VersionedData<TableRegistration> getOriginalTableWithVersion(
            BulkLoadTransaction transaction) throws Exception {
        return zooKeeperClient
                .getTableWithVersion(transaction.tablePath)
                .filter(table -> table.data().tableId == transaction.tableId)
                .orElseThrow(BulkLoadManager::originalTableNotFound);
    }

    private static BulkLoadNotFoundException originalTableNotFound() {
        return new BulkLoadNotFoundException("The original BulkLoad table no longer exists.");
    }

    private VersionedData<BulkLoadTarget> resolveTarget(
            BulkLoadTransaction transaction, TableBucket bucket) throws Exception {
        VersionedData<BulkLoadTarget> target =
                zooKeeperClient
                        .getBulkLoadTarget(transaction, bucket.getPartitionId())
                        .orElseThrow(
                                () ->
                                        new InvalidRequestException(
                                                "Target is not part of this transaction."));
        if (bucket.getBucket() < 0 || bucket.getBucket() >= target.data().bucketCount) {
            throw new InvalidRequestException("Invalid bucket ID.");
        }
        return target;
    }

    // Schema is compared at read time. The partition registration version fences publication.
    private @Nullable Integer validateBuildContext(
            BulkLoadTransaction transaction, BulkLoadTarget target, TableRegistration table)
            throws Exception {
        if (target.withdrawn) {
            throw new BulkLoadTargetWithdrawnException("The original physical target was deleted.");
        }
        VersionedData<PartitionRegistration> partition =
                target.partitionId == null
                        ? null
                        : getOriginalPartition(transaction.tablePath, target);
        // KV encoding, row TTL, and changelog image are immutable through ALTER TABLE.
        if (zooKeeperClient.getCurrentSchemaId(transaction.tablePath) != transaction.schemaId
                || !Objects.equals(table.remoteDataDir, transaction.remoteDataDir)) {
            throw new InvalidRequestException("BulkLoad build context changed.");
        }
        if (partition == null) {
            if (table.bucketCount != target.bucketCount) {
                throw new InvalidRequestException("BulkLoad target bucket count changed.");
            }
            return null;
        }

        PartitionRegistration registration = partition.data();
        int bucketCount =
                registration.getBucketCountOrDefault(table.bucketCount, table.bucketCountEpoch);
        if (bucketCount != target.bucketCount
                || !Objects.equals(registration.getRemoteDataDir(), target.remoteDataDir)) {
            throw new InvalidRequestException("BulkLoad target build context changed.");
        }
        return partition.zkVersion();
    }

    private VersionedData<PartitionRegistration> getOriginalPartition(
            TablePath tablePath, BulkLoadTarget target) throws Exception {
        return zooKeeperClient
                .getPartitionWithVersion(tablePath, target.partitionName)
                .filter(partition -> partition.data().getPartitionId() == target.partitionId)
                .orElseThrow(
                        () ->
                                new BulkLoadTargetWithdrawnException(
                                        "The original partition was deleted."));
    }

    private void checkMutationTarget(BulkLoadTransaction original, BulkLoadTarget target)
            throws Exception {
        getOriginalTableWithVersion(original);
        requireRunning(getTransaction(original.tableId, original.bulkLoadId).data());
        if (target.partitionId != null) {
            getOriginalPartition(original.tablePath, target);
        }
    }

    private static void requireRunning(BulkLoadTransaction transaction) {
        if (transaction.state != BulkLoadTransaction.RUNNING) {
            throw new BulkLoadStateException("BulkLoad transaction has ended.");
        }
    }
}
