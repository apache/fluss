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
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.BulkLoadPublicationUnknownException;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.function.FunctionWithException;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/** Allocates and commits snapshots for individual BulkLoad buckets. */
public final class BulkLoadManager {
    private final ZooKeeperClient client;
    private final MetadataManager metadata;
    private final int epochVersion;

    public BulkLoadManager(ZooKeeperClient client, MetadataManager metadata, int epochVersion) {
        this.client = client;
        this.metadata = metadata;
        this.epochVersion = epochVersion;
    }

    /** Resolves the transaction and checks that its original physical table still exists. */
    public VersionedData<BulkLoadTransaction> resolveTransaction(long tableId, String bulkLoadId)
            throws Exception {
        VersionedData<BulkLoadTransaction> transaction =
                client.getBulkLoadTransaction(tableId, bulkLoadId)
                        .orElseThrow(
                                () ->
                                        new BulkLoadNotFoundException(
                                                "BulkLoad transaction does not exist."));
        Optional<TableRegistration> table =
                client.getTable(transaction.data().tableInfo.getTablePath());
        if (!table.isPresent() || table.get().tableId != tableId) {
            throw new BulkLoadNotFoundException("The original BulkLoad table no longer exists.");
        }
        return transaction;
    }

    /** Allocates a snapshot ID and WAL offset, or returns the previously allocated coordinates. */
    public BulkLoadBucket allocateBucketSnapshot(
            VersionedData<BulkLoadTransaction> transaction,
            @Nullable Long partitionId,
            int bucketId,
            FunctionWithException<TableBucket, Long, Exception> leaderLogEndOffsetFetcher)
            throws Exception {
        BulkLoadTransaction original = transaction.data();
        TableBucket bucket = new TableBucket(original.tableId, partitionId, bucketId);
        VersionedData<BulkLoadTarget> target = resolveTarget(original, bucket);
        BulkLoadBucket existing =
                client.getBulkLoadBucket(original, bucket).map(VersionedData::data).orElse(null);
        if (existing != null) {
            return reuseBucketAllocation(original, target.data(), existing);
        }

        requireRunning(original);
        TableAndPartitionZkVersions registrationVersions =
                validateBuildContext(original, target.data());
        long snapshotId = client.allocateBucketSnapshotId(bucket);
        long logOffset = leaderLogEndOffsetFetcher.apply(bucket);
        BulkLoadBucket allocation =
                new BulkLoadBucket(snapshotId, logOffset, BulkLoadBucket.ALLOCATED);
        try {
            client.allocateBulkLoadBucket(
                    transaction,
                    target,
                    bucket,
                    allocation,
                    registrationVersions.tableZkVersion,
                    registrationVersions.partitionZkVersion,
                    epochVersion);
            return allocation;
        } catch (KeeperException.NodeExistsException e) {
            BulkLoadTransaction current =
                    resolveTransaction(original.tableId, original.bulkLoadId).data();
            BulkLoadTarget currentTarget = resolveTarget(current, bucket).data();
            BulkLoadBucket winner =
                    client.getBulkLoadBucket(current, bucket)
                            .map(VersionedData::data)
                            .orElseThrow(() -> e);
            return reuseBucketAllocation(current, currentTarget, winner);
        } catch (KeeperException.BadVersionException e) {
            requireRunning(resolveTransaction(original.tableId, original.bulkLoadId).data());
            throw e;
        }
    }

    private BulkLoadBucket reuseBucketAllocation(
            BulkLoadTransaction transaction, BulkLoadTarget target, BulkLoadBucket allocation)
            throws Exception {
        if (allocation.state != BulkLoadBucket.COMMITTED) {
            requireRunning(transaction);
            validateBuildContext(transaction, target);
        }
        return allocation;
    }

    private VersionedData<BulkLoadTarget> resolveTarget(
            BulkLoadTransaction transaction, TableBucket bucket) throws Exception {
        VersionedData<BulkLoadTarget> target =
                client.getBulkLoadTarget(transaction, bucket.getPartitionId())
                        .orElseThrow(
                                () ->
                                        new InvalidBulkLoadRequestException(
                                                "Target is not part of this transaction."));
        if (bucket.getBucket() < 0 || bucket.getBucket() >= target.data().bucketCount) {
            throw new InvalidBulkLoadRequestException("Invalid bucket ID.");
        }
        return target;
    }

    // Schema is compared at read time. Callers coordinate DDL through publication.
    private TableAndPartitionZkVersions validateBuildContext(
            BulkLoadTransaction transaction, BulkLoadTarget target) throws Exception {
        if (target.withdrawn) {
            throw new BulkLoadTargetWithdrawnException("The original physical target was deleted.");
        }
        TableInfo original = transaction.tableInfo;
        VersionedData<TableRegistration> current =
                client.getTableWithVersion(original.getTablePath())
                        .orElseThrow(
                                () ->
                                        new BulkLoadNotFoundException(
                                                "The original table was deleted."));
        if (current.data().tableId != original.getTableId()) {
            throw new InvalidBulkLoadRequestException("Table physical identity changed.");
        }
        TableInfo table =
                current.data()
                        .toTableInfo(
                                original.getTablePath(),
                                metadata.getLatestSchema(original.getTablePath()));
        // KV encoding, row TTL, and changelog image are immutable through ALTER TABLE.
        if (!table.getSchema().equals(original.getSchema())
                || !table.getPartitionKeys().equals(original.getPartitionKeys())
                || !table.getBucketKeys().equals(original.getBucketKeys())
                || !Objects.equals(table.getRemoteDataDir(), original.getRemoteDataDir())) {
            throw new InvalidBulkLoadRequestException("BulkLoad build context changed.");
        }
        if (target.partitionId == null) {
            if (table.getNumBuckets() != target.bucketCount) {
                throw new InvalidBulkLoadRequestException("BulkLoad target bucket count changed.");
            }
            return new TableAndPartitionZkVersions(current.zkVersion(), null);
        }

        VersionedData<PartitionRegistration> partition =
                client.getPartitionWithVersion(original.getTablePath(), target.partitionName)
                        .orElseThrow(
                                () ->
                                        new BulkLoadTargetWithdrawnException(
                                                "The original partition was deleted."));
        PartitionRegistration registration = partition.data();
        if (registration.getPartitionId() != target.partitionId) {
            throw new BulkLoadTargetWithdrawnException("The original partition was deleted.");
        }
        int bucketCount =
                registration.getBucketCountOrDefault(
                        table.getNumBuckets(), table.getBucketCountEpoch());
        if (bucketCount != target.bucketCount
                || !Objects.equals(registration.getRemoteDataDir(), target.remoteDataDir)) {
            throw new InvalidBulkLoadRequestException("BulkLoad target build context changed.");
        }
        return new TableAndPartitionZkVersions(current.zkVersion(), partition.zkVersion());
    }

    /**
     * Durably publishes an allocated bucket snapshot. Signals recovery when a publication is new or
     * its result is unknown. A successful return does not imply the leader has applied it yet.
     */
    public void commitBucketSnapshot(
            VersionedData<BulkLoadTransaction> transaction,
            TableBucket bucket,
            FunctionWithException<TableBucket, Long, Exception> leaderLogEndOffsetFetcher,
            CompletedSnapshotStoreManager snapshots,
            Runnable requestRecovery)
            throws Exception {
        BulkLoadTransaction bulkLoadTransaction = transaction.data();
        VersionedData<BulkLoadTarget> target = resolveTarget(bulkLoadTransaction, bucket);
        VersionedData<BulkLoadBucket> allocation =
                client.getBulkLoadBucket(bulkLoadTransaction, bucket)
                        .orElseThrow(
                                () ->
                                        new InvalidBulkLoadRequestException(
                                                "Bucket has not been allocated."));
        if (allocation.data().state == BulkLoadBucket.COMMITTED) {
            snapshots.reconcileUnconfirmedImport(bucket);
            return;
        }

        requireRunning(bulkLoadTransaction);
        TableAndPartitionZkVersions registrationVersions =
                validateBuildContext(bulkLoadTransaction, target.data());
        TableInfo table = bulkLoadTransaction.tableInfo;
        CompletedSnapshot snapshot =
                BulkLoadSnapshotValidator.validate(table, target.data(), bucket, allocation.data());
        CompletedSnapshotStore snapshotStore =
                snapshots.getOrCreateCompletedSnapshotStore(table.getTablePath(), bucket);
        validateLogEndOffset(
                bucket, allocation.data().logOffset, leaderLogEndOffsetFetcher.apply(bucket));

        BucketSnapshot snapshotReference =
                new BucketSnapshot(
                        snapshot.getSnapshotID(),
                        snapshot.getLogOffset(),
                        snapshot.getMetadataFilePath().toString());
        boolean published;
        try {
            published =
                    snapshotStore.commitImportedSnapshot(
                            snapshot,
                            () ->
                                    client.commitBulkLoadBucket(
                                            transaction,
                                            target,
                                            bucket,
                                            allocation,
                                            snapshotReference,
                                            registrationVersions.tableZkVersion,
                                            registrationVersions.partitionZkVersion,
                                            epochVersion));
        } catch (BulkLoadPublicationUnknownException e) {
            requestRecovery.run();
            throw e.getCause();
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException e) {
            BulkLoadTransaction current =
                    resolveTransaction(bulkLoadTransaction.tableId, bulkLoadTransaction.bulkLoadId)
                            .data();
            Optional<VersionedData<BulkLoadBucket>> result =
                    client.getBulkLoadBucket(current, bucket);
            if (!result.isPresent() || result.get().data().state != BulkLoadBucket.COMMITTED) {
                requireRunning(current);
                throw e;
            }
            snapshots.reconcileUnconfirmedImport(bucket);
            return;
        }
        if (published) {
            requestRecovery.run();
        }
    }

    private static void validateLogEndOffset(
            TableBucket bucket, long allocatedOffset, long currentLogEndOffset) {
        // The caller pauses writes through publication. The offset check detects earlier appends.
        if (currentLogEndOffset != allocatedOffset) {
            throw new BulkLoadConflictException(
                    String.format(
                            "Cannot commit BulkLoad for %s: the leader WAL end offset changed "
                                    + "from %d at allocation to %d. Resolve concurrent writes "
                                    + "and retry with a fresh allocation.",
                            bucket, allocatedOffset, currentLogEndOffset));
        }
    }

    private static void requireRunning(BulkLoadTransaction transaction) {
        if (transaction.state != BulkLoadTransaction.RUNNING) {
            throw new BulkLoadStateException("BulkLoad transaction has ended.");
        }
    }

    private static final class TableAndPartitionZkVersions {
        private final int tableZkVersion;
        private final @Nullable Integer partitionZkVersion;

        private TableAndPartitionZkVersions(
                int tableZkVersion, @Nullable Integer partitionZkVersion) {
            this.tableZkVersion = tableZkVersion;
            this.partitionZkVersion = partitionZkVersion;
        }
    }
}
