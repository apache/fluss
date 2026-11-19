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

import org.apache.fluss.exception.BulkLoadConflictException;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.BulkLoadStateException;
import org.apache.fluss.exception.BulkLoadTargetWithdrawnException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Validates and atomically publishes an allocated BulkLoad bucket snapshot. */
public final class BulkLoadManager {
    private final ZooKeeperClient client;
    private final MetadataManager metadata;
    private final BulkLoadMetadataStore store;
    private final int epochVersion;

    public BulkLoadManager(ZooKeeperClient client, MetadataManager metadata, int epochVersion) {
        this.client = client;
        this.metadata = metadata;
        this.store = new BulkLoadMetadataStore(client);
        this.epochVersion = epochVersion;
    }

    /** Resolves the transaction and checks that its original physical table still exists. */
    public Versioned<BulkLoadTransaction> transaction(long tableId, String id) throws Exception {
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
        List<CuratorOp> checks = validateBuildContext(transaction, target, !previous.isPresent());
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
            store.allocate(transaction, target, bucket, allocation, checks, epochVersion);
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
    private List<CuratorOp> validateBuildContext(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<BulkLoadTarget> target,
            boolean willPersist)
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
        List<CuratorOp> checks = willPersist ? new ArrayList<>(2) : Collections.emptyList();
        if (willPersist) {
            checks.add(
                    store.check(
                            ZkData.TableZNode.path(original.getTablePath()), current.zkVersion()));
        }
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
            if (willPersist) {
                checks.add(
                        store.check(
                                ZkData.PartitionZNode.path(
                                        original.getTablePath(), target.value.partitionName),
                                partition.get().zkVersion()));
            }
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
            snapshots.reconcileUnconfirmedImport(bucket);
            return false;
        }
        requireRunning(transaction);
        List<CuratorOp> checks = validateBuildContext(transaction, target, true);
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
                                    epochVersion));
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException e) {
            Versioned<BulkLoadTransaction> current = transaction(value.tableId, value.bulkLoadId);
            Optional<Versioned<BulkLoadBucket>> result = store.bucket(current.value, bucket);
            if (!result.isPresent() || result.get().value.state != BulkLoadBucket.COMMITTED) {
                requireRunning(current);
                throw e;
            }
            snapshots.reconcileUnconfirmedImport(bucket);
            return false;
        }
    }

    private static void requireRunning(Versioned<BulkLoadTransaction> transaction) {
        if (transaction.value.state != BulkLoadTransaction.RUNNING) {
            throw new BulkLoadStateException("BulkLoad transaction has ended.");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new InvalidBulkLoadRequestException(message);
        }
    }
}
