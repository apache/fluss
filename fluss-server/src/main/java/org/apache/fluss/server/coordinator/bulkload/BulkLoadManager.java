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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.event.CommitBulkLoadBucketEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Allocates and commits snapshots for individual BulkLoad buckets. */
public final class BulkLoadManager {
    private final ZooKeeperClient zooKeeperClient;
    private final int epochVersion;

    public BulkLoadManager(ZooKeeperClient zooKeeperClient, int epochVersion) {
        this.zooKeeperClient = zooKeeperClient;
        this.epochVersion = epochVersion;
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
        VersionedData<TableRegistration> table = getOriginalTable(original);
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
                    epochVersion);
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
        VersionedData<TableRegistration> currentTable = getOriginalTable(bulkLoadTransaction);
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
                        epochVersion,
                        snapshotStores.getOrCreateCompletedSnapshotStore(tablePath, bucket));
        eventManager.put(event);
        return event.result.exceptionally(
                failure -> {
                    try {
                        if (failure instanceof KeeperException.NoNodeException
                                || failure instanceof KeeperException.BadVersionException) {
                            checkMutationTarget(bulkLoadTransaction, target.data());
                        }
                    } catch (Exception classified) {
                        failure = classified;
                    }
                    throw new CompletionException(failure);
                });
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

    private VersionedData<TableRegistration> getOriginalTable(BulkLoadTransaction transaction)
            throws Exception {
        return zooKeeperClient
                .getTableWithVersion(transaction.tablePath)
                .filter(table -> table.data().tableId == transaction.tableId)
                .orElseThrow(
                        () ->
                                new BulkLoadNotFoundException(
                                        "The original BulkLoad table no longer exists."));
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
        // KV encoding, row TTL, and changelog image are immutable through ALTER TABLE.
        if (zooKeeperClient.getCurrentSchemaId(transaction.tablePath) != transaction.schemaId
                || !Objects.equals(table.remoteDataDir, transaction.remoteDataDir)) {
            throw new InvalidRequestException("BulkLoad build context changed.");
        }
        if (target.partitionId == null) {
            if (table.bucketCount != target.bucketCount) {
                throw new InvalidRequestException("BulkLoad target bucket count changed.");
            }
            return null;
        }

        VersionedData<PartitionRegistration> partition =
                getOriginalPartition(transaction.tablePath, target);
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
        getOriginalTable(original);
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
