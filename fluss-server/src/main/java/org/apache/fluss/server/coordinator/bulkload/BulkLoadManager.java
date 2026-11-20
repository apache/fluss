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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadConflictException;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.BulkLoadStateException;
import org.apache.fluss.exception.BulkLoadTargetWithdrawnException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.GetBulkLoadInfoResponse;
import org.apache.fluss.rpc.messages.ListInProgressBulkLoadsResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetProgress;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.function.FunctionWithException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntSupplier;

import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;

/** Executes per-table BulkLoad operations on Coordinator workers using versioned metadata. */
public final class BulkLoadManager {
    private final ZooKeeperClient client;
    private final MetadataManager metadata;
    private final BulkLoadMetadataStore store;
    private final int maxTargets;
    private final IntSupplier epochVersion;

    public BulkLoadManager(
            Configuration conf,
            ZooKeeperClient client,
            MetadataManager metadata,
            IntSupplier epochVersion) {
        this.client = client;
        this.metadata = metadata;
        this.store = new BulkLoadMetadataStore(client);
        this.maxTargets = conf.get(ConfigOptions.COORDINATOR_BULKLOAD_MAX_TARGETS);
        if (maxTargets <= 0) {
            throw new IllegalArgumentException("BulkLoad max-targets must be positive.");
        }
        this.epochVersion = epochVersion;
    }

    /** Fixes the original build context and atomically owns all selected targets. */
    public BeginBulkLoadResponse begin(TablePath path, BeginBulkLoadRequest request)
            throws Exception {
        List<String> names = request.getPartitionNamesList();
        require(new HashSet<>(names).size() == names.size(), "Duplicate partition names.");
        String id =
                request.hasBulkLoadId() ? request.getBulkLoadId() : UUID.randomUUID().toString();
        BulkLoadMetadataStore.validateId(id);
        VersionedData<TableRegistration> registration =
                client.getTableWithVersion(path)
                        .orElseThrow(
                                () ->
                                        new TableNotExistException(
                                                "Table " + path + " does not exist."));
        long tableId = registration.data().tableId;
        Optional<Versioned<BulkLoadTransaction>> previous = store.transaction(tableId, id);
        if (previous.isPresent()) {
            return existingBegin(previous.get().value, names);
        }
        TableInfo table = registration.data().toTableInfo(path, metadata.getLatestSchema(path));
        require(table.hasPrimaryKey(), "BulkLoad requires a primary key table.");
        require(
                table.getSchema().getAutoIncrementColumnNames().isEmpty(),
                "BulkLoad does not support auto-increment columns.");
        require(
                !table.getTableConfig().getMergeEngineType().isPresent(),
                "BulkLoad does not support tables with a merge engine.");
        require(
                table.isPartitioned() ? !names.isEmpty() : names.isEmpty(),
                "Invalid partition selection for this table.");
        require(Math.max(1, names.size()) <= maxTargets, "BulkLoad exceeds max-targets.");
        List<CuratorOp> checks = new ArrayList<>();
        checks.add(store.check(ZkData.TableZNode.path(path), registration.zkVersion()));
        List<BulkLoadTarget> targets = resolveTargets(table, names, checks);
        BulkLoadTransaction transaction =
                new BulkLoadTransaction(
                        tableId,
                        id,
                        table,
                        BulkLoadTransaction.RUNNING,
                        System.currentTimeMillis(),
                        null,
                        null);
        try {
            store.begin(transaction, targets, checks, epochVersion.getAsInt());
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException e) {
            // The next RPC resolves concurrent Begin or a changed physical registration.
            Optional<Versioned<BulkLoadTransaction>> current = store.transaction(tableId, id);
            if (current.isPresent()) {
                return existingBegin(current.get().value, names);
            }
            throw e;
        }
        return beginResponse(transaction, store.targets(transaction));
    }

    private BeginBulkLoadResponse existingBegin(BulkLoadTransaction transaction, List<String> names)
            throws Exception {
        List<Versioned<BulkLoadTarget>> targets = store.targets(transaction);
        Set<String> original = new HashSet<>();
        for (Versioned<BulkLoadTarget> target : targets) {
            if (target.value.partitionName != null) {
                original.add(target.value.partitionName);
            }
        }
        require(original.equals(new HashSet<>(names)), "bulk_load_id has different targets.");
        return beginResponse(transaction, targets);
    }

    private List<BulkLoadTarget> resolveTargets(
            TableInfo table, List<String> names, List<CuratorOp> checks) throws Exception {
        if (!table.isPartitioned()) {
            return Collections.singletonList(
                    new BulkLoadTarget(
                            null, null, table.getNumBuckets(), table.getRemoteDataDir(), false));
        }
        List<BulkLoadTarget> result = new ArrayList<>();
        for (String name : names) {
            require(
                    !HISTORICAL_PARTITION_VALUE.equals(name),
                    "Historical partitions are unsupported.");
            VersionedData<PartitionRegistration> partition =
                    client.getPartitionWithVersion(table.getTablePath(), name)
                            .orElseThrow(
                                    () ->
                                            new PartitionNotExistException(
                                                    "Partition " + name + " does not exist."));
            require(
                    partition.data().getTableId() == table.getTableId(),
                    "Partition belongs to another table.");
            checks.add(
                    store.check(
                            ZkData.PartitionZNode.path(table.getTablePath(), name),
                            partition.zkVersion()));
            result.add(
                    new BulkLoadTarget(
                            partition.data().getPartitionId(),
                            name,
                            partition
                                    .data()
                                    .getBucketCountOrDefault(
                                            table.getNumBuckets(), table.getBucketCountEpoch()),
                            partition.data().getRemoteDataDir(),
                            false));
        }
        result.sort(Comparator.comparing(t -> t.partitionId));
        return result;
    }

    /** Resolves the original table ID; a same-name replacement is never accepted. */
    public Versioned<BulkLoadTransaction> transaction(PbBulkLoadHandle handle) throws Exception {
        return transaction(handle.getTableId(), handle.getBulkLoadId());
    }

    private Versioned<BulkLoadTransaction> transaction(long tableId, String id) throws Exception {
        Versioned<BulkLoadTransaction> transaction =
                store.transaction(tableId, id)
                        .orElseThrow(
                                () ->
                                        new BulkLoadNotFoundException(
                                                "BulkLoad transaction does not exist."));
        Optional<TableRegistration> table =
                client.getTable(transaction.value.tableInfo.getTablePath());
        if (!table.isPresent() || table.get().tableId != tableId) {
            throw new BulkLoadNotFoundException("The original BulkLoad table no longer exists.");
        }
        return transaction;
    }

    /** Allocation retries return the original result before reading the current leader again. */
    public BulkLoadBucket allocate(
            Versioned<BulkLoadTransaction> transaction,
            @Nullable Long partition,
            int bucketId,
            FunctionWithException<TableBucket, Long, Exception> leaderOffset)
            throws Exception {
        BulkLoadTransaction value = transaction.value;
        TableBucket bucket = new TableBucket(value.tableId, partition, bucketId);
        Versioned<BulkLoadTarget> target = target(transaction, partition, bucketId);
        Optional<Versioned<BulkLoadBucket>> previous = store.bucket(transaction.value, bucket);
        if (previous.isPresent() && previous.get().value.state == BulkLoadBucket.COMMITTED) {
            return previous.get().value;
        }
        requireRunning(transaction);
        List<CuratorOp> checks = buildContextChecks(transaction, target);
        if (previous.isPresent()) {
            return previous.get().value;
        }
        long sid =
                new ZkSequenceIDCounter(
                                client.getCuratorClient(),
                                ZkData.BucketSnapshotSequenceIdZNode.path(bucket))
                        .getAndIncrement();
        long offset = leaderOffset.apply(bucket);
        BulkLoadBucket allocation = new BulkLoadBucket(sid, offset, BulkLoadBucket.ALLOCATED);
        try {
            store.allocate(
                    transaction, target, bucket, allocation, checks, epochVersion.getAsInt());
        } catch (KeeperException.NodeExistsException e) {
            return allocate(
                    transaction(value.tableId, value.bulkLoadId),
                    partition,
                    bucketId,
                    leaderOffset);
        } catch (KeeperException.BadVersionException e) {
            requireRunning(transaction(value.tableId, value.bulkLoadId));
            throw e;
        }
        return allocation;
    }

    private Versioned<BulkLoadTarget> target(
            Versioned<BulkLoadTransaction> transaction, @Nullable Long partition, int bucketId)
            throws Exception {
        Versioned<BulkLoadTarget> target =
                store.target(transaction.value, partition)
                        .orElseThrow(
                                () ->
                                        new InvalidBulkLoadRequestException(
                                                "Target is not part of this transaction."));
        require(bucketId >= 0 && bucketId < target.value.bucketCount, "Invalid bucket ID.");
        return target;
    }

    /** Checks construction context without adding DDL serialization. */
    private List<CuratorOp> buildContextChecks(
            Versioned<BulkLoadTransaction> transaction, Versioned<BulkLoadTarget> target)
            throws Exception {
        if (target.value.withdrawn) {
            throw new BulkLoadTargetWithdrawnException("The original physical target was deleted.");
        }
        TableInfo original = transaction.value.tableInfo;
        VersionedData<TableRegistration> current =
                client.getTableWithVersion(original.getTablePath())
                        .orElseThrow(
                                () ->
                                        new BulkLoadNotFoundException(
                                                "The original table was deleted."));
        require(
                current.data().tableId == original.getTableId(),
                "Table physical identity changed.");
        TableInfo table =
                current.data()
                        .toTableInfo(
                                original.getTablePath(),
                                metadata.getLatestSchema(original.getTablePath()));
        require(
                table.toTableDescriptor().equals(original.toTableDescriptor())
                        && Objects.equals(table.getRemoteDataDir(), original.getRemoteDataDir()),
                "BulkLoad build context changed.");
        List<CuratorOp> checks = new ArrayList<>();
        checks.add(
                store.check(ZkData.TableZNode.path(original.getTablePath()), current.zkVersion()));
        if (target.value.partitionId != null) {
            Optional<VersionedData<PartitionRegistration>> partition =
                    client.getPartitionWithVersion(
                            original.getTablePath(), target.value.partitionName);
            if (!partition.isPresent()
                    || partition.get().data().getPartitionId() != target.value.partitionId) {
                throw new BulkLoadTargetWithdrawnException("The original partition was deleted.");
            }
            require(
                    partition
                                            .get()
                                            .data()
                                            .getBucketCountOrDefault(
                                                    table.getNumBuckets(),
                                                    table.getBucketCountEpoch())
                                    == target.value.bucketCount
                            && Objects.equals(
                                    partition.get().data().getRemoteDataDir(),
                                    target.value.remoteDataDir),
                    "BulkLoad target build context changed.");
            checks.add(
                    store.check(
                            ZkData.PartitionZNode.path(
                                    original.getTablePath(), target.value.partitionName),
                            partition.get().zkVersion()));
        }
        return checks;
    }

    /** Persists a bucket once; retries do not reread input or republish an overwrite. */
    public boolean commitBucket(
            Versioned<BulkLoadTransaction> transaction,
            @Nullable Long partition,
            int bucketId,
            FunctionWithException<TableBucket, Long, Exception> leaderOffset,
            CompletedSnapshotStoreManager snapshots)
            throws Exception {
        BulkLoadTransaction value = transaction.value;
        TableBucket bucket = new TableBucket(value.tableId, partition, bucketId);
        Versioned<BulkLoadTarget> target = target(transaction, partition, bucketId);
        Versioned<BulkLoadBucket> allocation =
                store.bucket(transaction.value, bucket)
                        .orElseThrow(
                                () ->
                                        new InvalidBulkLoadRequestException(
                                                "Bucket has not been allocated."));
        if (allocation.value.state == BulkLoadBucket.COMMITTED) {
            return false;
        }
        requireRunning(transaction);
        List<CuratorOp> checks = buildContextChecks(transaction, target);
        CompletedSnapshot snapshot =
                BulkLoadSnapshotValidator.validate(
                        transaction.value.tableInfo, target.value, bucket, allocation.value);
        CompletedSnapshotStore snapshotStore =
                snapshots.getOrCreateCompletedSnapshotStore(
                        transaction.value.tableInfo.getTablePath(), bucket);
        // The upstream job must pause writes through commit. An offset check detects a
        // changed WAL but cannot prevent appends after the check.
        long currentOffset = leaderOffset.apply(bucket);
        if (currentOffset != allocation.value.logOffset) {
            throw new BulkLoadConflictException(
                    String.format(
                            "Cannot commit BulkLoad for %s: the leader WAL end offset changed "
                                    + "from %d at allocation to %d. Resolve concurrent writes "
                                    + "and retry with a fresh allocation.",
                            bucket, allocation.value.logOffset, currentOffset));
        }
        try {
            return snapshotStore.commitImportedSnapshot(
                    snapshot,
                    () ->
                            store.commitBucket(
                                    transaction,
                                    target,
                                    bucket,
                                    allocation,
                                    new BucketSnapshot(
                                            snapshot.getSnapshotID(),
                                            snapshot.getLogOffset(),
                                            snapshot.getMetadataFilePath().toString()),
                                    checks,
                                    epochVersion.getAsInt()));
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException e) {
            Versioned<BulkLoadTransaction> current = transaction(value.tableId, value.bulkLoadId);
            Optional<Versioned<BulkLoadBucket>> result = store.bucket(current.value, bucket);
            if (!result.isPresent() || result.get().value.state != BulkLoadBucket.COMMITTED) {
                requireRunning(current);
                throw e;
            }
            return false;
        }
    }

    /** Queries fixed target summaries one bucket at a time. */
    public GetBulkLoadInfoResponse info(Versioned<BulkLoadTransaction> resolved) throws Exception {
        Versioned<BulkLoadTransaction> transaction =
                store.withdrawDroppedTargets(resolved, epochVersion.getAsInt());
        GetBulkLoadInfoResponse response = new GetBulkLoadInfoResponse();
        response.setStatus(status(transaction.value));
        for (Versioned<BulkLoadTarget> target : store.targets(transaction.value)) {
            long allocated = 0;
            long committed = 0;
            for (int id = 0; id < target.value.bucketCount; id++) {
                Optional<Versioned<BulkLoadBucket>> record =
                        store.bucket(
                                transaction.value,
                                new TableBucket(
                                        transaction.value.tableId, target.value.partitionId, id));
                if (record.isPresent()) {
                    allocated++;
                    if (record.get().value.state == BulkLoadBucket.COMMITTED) {
                        committed++;
                    }
                }
            }
            PbBulkLoadTargetProgress progress =
                    response.addTarget()
                            .setBucketCount(target.value.bucketCount)
                            .setAllocatedBucketCount(allocated)
                            .setCommittedBucketCount(committed)
                            .setWithdrawn(target.value.withdrawn);
            if (target.value.partitionId != null) {
                progress.setPartitionId(target.value.partitionId);
            }
        }
        return response;
    }

    public BulkLoadTransaction finish(
            Versioned<BulkLoadTransaction> resolved, boolean abort, @Nullable String message)
            throws Exception {
        Versioned<BulkLoadTransaction> transaction =
                store.withdrawDroppedTargets(resolved, epochVersion.getAsInt());
        int expected = abort ? BulkLoadTransaction.ABORTED : BulkLoadTransaction.COMMITTED;
        if (transaction.value.state == expected) {
            return transaction.value;
        }
        requireRunning(transaction);
        try {
            if (abort) {
                return store.abort(
                        transaction, System.currentTimeMillis(), message, epochVersion.getAsInt());
            }
            return store.commit(transaction, epochVersion.getAsInt(), System.currentTimeMillis());
        } catch (KeeperException.BadVersionException e) {
            BulkLoadTransaction value = transaction.value;
            Versioned<BulkLoadTransaction> current = transaction(value.tableId, value.bulkLoadId);
            if (current.version == transaction.version) {
                throw e;
            }
            return finish(current, abort, message);
        }
    }

    public ListInProgressBulkLoadsResponse list(TablePath path, List<String> names)
            throws Exception {
        require(new HashSet<>(names).size() == names.size(), "Duplicate partition names.");
        TableInfo table = metadata.getTable(path);
        require(
                table.isPartitioned() || names.isEmpty(),
                "Non-partitioned target has partition names.");
        List<Long> ids = new ArrayList<>();
        if (table.isPartitioned()) {
            List<String> selected =
                    names.isEmpty() ? new ArrayList<>(client.getPartitions(path)) : names;
            for (String name : selected) {
                Optional<PartitionRegistration> partition = client.getPartition(path, name);
                if (!partition.isPresent()) {
                    if (!names.isEmpty()) {
                        throw new PartitionNotExistException(
                                "Partition " + name + " does not exist.");
                    }
                    continue;
                }
                ids.add(partition.get().getPartitionId());
            }
        } else {
            ids.add(null);
        }
        ListInProgressBulkLoadsResponse response = new ListInProgressBulkLoadsResponse();
        for (BulkLoadTransaction transaction : store.list(table.getTableId(), ids)) {
            response.addTransaction().copyFrom(status(transaction));
        }
        return response;
    }

    public static PbBulkLoadStatus status(BulkLoadTransaction transaction) {
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

    private static BeginBulkLoadResponse beginResponse(
            BulkLoadTransaction transaction, List<Versioned<BulkLoadTarget>> targets) {
        BeginBulkLoadResponse response = new BeginBulkLoadResponse();
        response.setStatus(status(transaction));
        TableInfo table = transaction.tableInfo;
        response.setTableInfo()
                .setTableId(table.getTableId())
                .setSchemaId(table.getSchemaId())
                .setTableJson(table.toTableDescriptor().toJsonBytes())
                .setCreatedTime(table.getCreatedTime())
                .setModifiedTime(table.getModifiedTime())
                .setBucketCountEpoch(table.getBucketCountEpoch())
                .setRemoteDataDir(table.getRemoteDataDir());
        for (Versioned<BulkLoadTarget> target : targets) {
            PbBulkLoadTargetInfo info =
                    response.addTarget()
                            .setBucketCount(target.value.bucketCount)
                            .setRemoteDataDir(target.value.remoteDataDir);
            if (target.value.partitionId != null) {
                info.setPartitionId(target.value.partitionId)
                        .setPartitionName(target.value.partitionName);
            }
        }
        return response;
    }

    private static void requireRunning(Versioned<BulkLoadTransaction> transaction) {
        if (transaction.value.state != BulkLoadTransaction.RUNNING) {
            throw new BulkLoadStateException(
                    transaction.value.state == BulkLoadTransaction.COMMITTED
                            ? "BulkLoad transaction is committed."
                            : "BulkLoad transaction is aborted.");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new InvalidBulkLoadRequestException(message);
        }
    }
}
