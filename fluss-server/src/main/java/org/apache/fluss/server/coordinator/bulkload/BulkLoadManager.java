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
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.exception.NotEnoughReplicasException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.ListOffsetsResultForBucket;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.function.CheckedSupplier;
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

    private VersionedData<BulkLoadTransaction> getTransaction(long tableId, String bulkLoadId)
            throws Exception {
        return client.getBulkLoadTransaction(tableId, bulkLoadId)
                .orElseThrow(
                        () ->
                                new BulkLoadNotFoundException(
                                        "BulkLoad transaction does not exist."));
    }

    private VersionedData<TableRegistration> getOriginalTable(BulkLoadTransaction transaction)
            throws Exception {
        TableInfo original = transaction.tableInfo;
        Optional<VersionedData<TableRegistration>> table =
                client.getTableWithVersion(original.getTablePath());
        if (!table.isPresent() || table.get().data().tableId != original.getTableId()) {
            throw new BulkLoadNotFoundException("The original BulkLoad table no longer exists.");
        }
        return table.get();
    }

    /** Allocates a snapshot ID and WAL offset, or returns the previously allocated coordinates. */
    public BulkLoadBucket allocateBucketSnapshot(
            String bulkLoadId,
            TableBucket bucket,
            FunctionWithException<TableBucket, Long, Exception> leaderLogEndOffsetFetcher)
            throws Exception {
        VersionedData<BulkLoadTransaction> transaction =
                getTransaction(bucket.getTableId(), bulkLoadId);
        BulkLoadTransaction original = transaction.data();
        VersionedData<TableRegistration> table = getOriginalTable(original);
        VersionedData<BulkLoadTarget> target = resolveTarget(original, bucket);
        BulkLoadBucket existing =
                client.getBulkLoadBucket(original, bucket).map(VersionedData::data).orElse(null);
        if (existing != null) {
            return reuseBucketAllocation(original, target.data(), existing, table);
        }

        requireRunning(original);
        TableAndPartitionZkVersions registrationVersions =
                validateBuildContext(original, target.data(), table);
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
            BulkLoadTransaction current = getTransaction(bucket.getTableId(), bulkLoadId).data();
            VersionedData<TableRegistration> currentTable = getOriginalTable(current);
            BulkLoadTarget currentTarget = resolveTarget(current, bucket).data();
            BulkLoadBucket winner =
                    client.getBulkLoadBucket(current, bucket)
                            .map(VersionedData::data)
                            .orElseThrow(() -> e);
            return reuseBucketAllocation(current, currentTarget, winner, currentTable);
        } catch (KeeperException.BadVersionException e) {
            BulkLoadTransaction current = getTransaction(bucket.getTableId(), bulkLoadId).data();
            getOriginalTable(current);
            requireRunning(current);
            throw e;
        }
    }

    private BulkLoadBucket reuseBucketAllocation(
            BulkLoadTransaction transaction,
            BulkLoadTarget target,
            BulkLoadBucket allocation,
            VersionedData<TableRegistration> table)
            throws Exception {
        if (allocation.state != BulkLoadBucket.COMMITTED) {
            requireRunning(transaction);
            validateBuildContext(transaction, target, table);
        }
        return allocation;
    }

    private VersionedData<BulkLoadTarget> resolveTarget(
            BulkLoadTransaction transaction, TableBucket bucket) throws Exception {
        VersionedData<BulkLoadTarget> target =
                client.getBulkLoadTarget(transaction, bucket.getPartitionId())
                        .orElseThrow(
                                () ->
                                        new InvalidRequestException(
                                                "Target is not part of this transaction."));
        if (bucket.getBucket() < 0 || bucket.getBucket() >= target.data().bucketCount) {
            throw new InvalidRequestException("Invalid bucket ID.");
        }
        return target;
    }

    // Schema is compared at read time. Callers coordinate DDL through publication.
    private TableAndPartitionZkVersions validateBuildContext(
            BulkLoadTransaction transaction,
            BulkLoadTarget target,
            VersionedData<TableRegistration> current)
            throws Exception {
        if (target.withdrawn) {
            throw new BulkLoadTargetWithdrawnException("The original physical target was deleted.");
        }
        TableInfo original = transaction.tableInfo;
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
            throw new InvalidRequestException("BulkLoad build context changed.");
        }
        if (target.partitionId == null) {
            if (table.getNumBuckets() != target.bucketCount) {
                throw new InvalidRequestException("BulkLoad target bucket count changed.");
            }
            return new TableAndPartitionZkVersions(current.zkVersion(), null);
        }

        VersionedData<PartitionRegistration> partition = getOriginalPartition(table, target);
        PartitionRegistration registration = partition.data();
        int bucketCount =
                registration.getBucketCountOrDefault(
                        table.getNumBuckets(), table.getBucketCountEpoch());
        if (bucketCount != target.bucketCount
                || !Objects.equals(registration.getRemoteDataDir(), target.remoteDataDir)) {
            throw new InvalidRequestException("BulkLoad target build context changed.");
        }
        return new TableAndPartitionZkVersions(current.zkVersion(), partition.zkVersion());
    }

    private VersionedData<PartitionRegistration> getOriginalPartition(
            TableInfo table, BulkLoadTarget target) throws Exception {
        VersionedData<PartitionRegistration> partition =
                client.getPartitionWithVersion(table.getTablePath(), target.partitionName)
                        .orElseThrow(
                                () ->
                                        new BulkLoadTargetWithdrawnException(
                                                "The original partition was deleted."));
        PartitionRegistration registration = partition.data();
        if (registration.getPartitionId() != target.partitionId) {
            throw new BulkLoadTargetWithdrawnException("The original partition was deleted.");
        }
        return partition;
    }

    /** Prepares an allocated bucket snapshot for publication, or returns empty on replay. */
    public Optional<PreparedCommit> prepareBucketCommit(
            String bulkLoadId,
            TableBucket bucket,
            FunctionWithException<Integer, ListOffsetsResultForBucket, Exception>
                    fetchLeaderOffsets)
            throws Exception {
        VersionedData<BulkLoadTransaction> transaction =
                getTransaction(bucket.getTableId(), bulkLoadId);
        BulkLoadTransaction bulkLoadTransaction = transaction.data();
        VersionedData<TableRegistration> currentTable = getOriginalTable(bulkLoadTransaction);
        VersionedData<BulkLoadTarget> target = resolveTarget(bulkLoadTransaction, bucket);
        VersionedData<BulkLoadBucket> allocation =
                client.getBulkLoadBucket(bulkLoadTransaction, bucket)
                        .orElseThrow(
                                () ->
                                        new InvalidRequestException(
                                                "Bucket has not been allocated."));
        if (allocation.data().state == BulkLoadBucket.COMMITTED) {
            return Optional.empty();
        }

        requireRunning(bulkLoadTransaction);
        TableAndPartitionZkVersions registrationVersions =
                validateBuildContext(bulkLoadTransaction, target.data(), currentTable);
        TableInfo table = bulkLoadTransaction.tableInfo;
        CompletedSnapshot snapshot =
                BulkLoadSnapshotValidator.validate(table, target.data(), bucket, allocation.data());
        LeaderAndIsr role =
                client.getLeaderAndIsr(bucket)
                        .filter(current -> current.leader() != LeaderAndIsr.NO_LEADER)
                        .orElseThrow(
                                () ->
                                        new NotLeaderOrFollowerException(
                                                "BulkLoad bucket has no leader: " + bucket));
        validateCommitOffsets(
                bucket, allocation.data().logOffset, fetchLeaderOffsets.apply(role.leader()));

        BucketSnapshot reference =
                new BucketSnapshot(
                        snapshot.getSnapshotID(),
                        snapshot.getLogOffset(),
                        snapshot.getMetadataFilePath().toString());
        return Optional.of(
                new PreparedCommit(
                        table.getTablePath(),
                        snapshot,
                        () ->
                                publishValidatedSnapshot(
                                        transaction,
                                        target,
                                        bucket,
                                        allocation,
                                        reference,
                                        role,
                                        registrationVersions),
                        () ->
                                client.confirmBulkLoadBucketCommit(
                                        bulkLoadTransaction,
                                        bucket,
                                        snapshot.getSnapshotID(),
                                        epochVersion)));
    }

    private Optional<LeaderAndIsr> publishValidatedSnapshot(
            VersionedData<BulkLoadTransaction> transaction,
            VersionedData<BulkLoadTarget> target,
            TableBucket bucket,
            VersionedData<BulkLoadBucket> allocation,
            BucketSnapshot reference,
            LeaderAndIsr role,
            TableAndPartitionZkVersions registrationVersions)
            throws Exception {
        try {
            return client.commitBulkLoadBucket(
                    transaction,
                    target,
                    bucket,
                    allocation,
                    reference,
                    role,
                    registrationVersions.tableZkVersion,
                    registrationVersions.partitionZkVersion,
                    epochVersion);
        } catch (KeeperException.NodeExistsException
                | KeeperException.BadVersionException
                | KeeperException.NoNodeException e) {
            BulkLoadTransaction current =
                    getTransaction(bucket.getTableId(), transaction.data().bulkLoadId).data();
            getOriginalTable(current);
            requireRunning(current);
            if (e instanceof KeeperException.NoNodeException && bucket.getPartitionId() != null) {
                BulkLoadTarget originalTarget = target.data();
                if (client.getBulkLoadTarget(current, bucket.getPartitionId())
                        .map(currentTarget -> currentTarget.data().withdrawn)
                        .orElse(false)) {
                    throw new BulkLoadTargetWithdrawnException(
                            "The original physical target was deleted.");
                }
                getOriginalPartition(current.tableInfo, originalTarget);
            }
            if (e instanceof KeeperException.BadVersionException
                    && !client.getLeaderAndIsr(bucket)
                            .filter(latest -> latest.zkVersion() == role.zkVersion())
                            .isPresent()) {
                throw new NotLeaderOrFollowerException(
                        "BulkLoad bucket role changed during commit: " + bucket);
            }
            throw e;
        }
    }

    /** A validated snapshot and its versioned publication operation. */
    public static final class PreparedCommit {
        private final TablePath tablePath;
        private final CompletedSnapshot snapshot;
        private final CheckedSupplier<Optional<LeaderAndIsr>> publication;
        private final CheckedSupplier<Boolean> publicationConfirmed;

        private PreparedCommit(
                TablePath tablePath,
                CompletedSnapshot snapshot,
                CheckedSupplier<Optional<LeaderAndIsr>> publication,
                CheckedSupplier<Boolean> publicationConfirmed) {
            this.tablePath = tablePath;
            this.snapshot = snapshot;
            this.publication = publication;
            this.publicationConfirmed = publicationConfirmed;
        }

        public TablePath tablePath() {
            return tablePath;
        }

        public CompletedSnapshot snapshot() {
            return snapshot;
        }

        public Optional<LeaderAndIsr> publish() throws Exception {
            return publication.get();
        }

        public boolean confirmPublication() throws Exception {
            return publicationConfirmed.get();
        }
    }

    private static void validateCommitOffsets(
            TableBucket bucket, long allocatedOffset, ListOffsetsResultForBucket offsets) {
        long currentLogEndOffset = offsets.getOffset();
        // The caller pauses writes through publication. The offset check detects earlier appends.
        if (currentLogEndOffset != allocatedOffset) {
            throw new BulkLoadConflictException(
                    String.format(
                            "Cannot commit BulkLoad for %s: the leader WAL end offset changed "
                                    + "from %d at allocation to %d. Resolve concurrent writes "
                                    + "and allocate the bucket in a new BulkLoad transaction.",
                            bucket, allocatedOffset, currentLogEndOffset));
        }
        long highWatermark = offsets.getHighWatermark();
        if (highWatermark < allocatedOffset) {
            throw new NotEnoughReplicasException(
                    String.format(
                            "Cannot commit BulkLoad for %s: the leader WAL high watermark %d "
                                    + "has not reached %d. Retry with the same allocation.",
                            bucket, highWatermark, allocatedOffset));
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
