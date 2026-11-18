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

import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Versioned BulkLoad persistence. Callers resolve physical registrations before constructing a
 * mutation; their version checks are included in the same multi as the durable result.
 */
final class BulkLoadMetadataStore {
    private static final Pattern BULK_LOAD_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}");
    private static final String ROOT = "/bulk_load/tables";
    private final ZooKeeperClient client;
    private final CuratorFramework curator;

    BulkLoadMetadataStore(ZooKeeperClient client) {
        this.client = client;
        this.curator = client.getCuratorClient();
    }

    private static String tablePath(long tableId) {
        return ROOT + "/" + tableId;
    }

    private static void validateId(String id) {
        if (!BULK_LOAD_ID.matcher(id).matches()) {
            throw new InvalidBulkLoadRequestException("Invalid bulk_load_id.");
        }
    }

    private static String transactionPath(long tableId, String id) {
        validateId(id);
        return tablePath(tableId) + "/transactions/" + id;
    }

    private static String targetSuffix(@Nullable Long partitionId) {
        return partitionId == null ? "/table" : "/partitions/" + partitionId;
    }

    private static String targetPath(long tableId, String id, @Nullable Long partitionId) {
        return transactionPath(tableId, id) + "/targets" + targetSuffix(partitionId);
    }

    private static String bucketPath(long tableId, String id, TableBucket bucket) {
        return targetPath(tableId, id, bucket.getPartitionId()) + "/buckets/" + bucket.getBucket();
    }

    Optional<Versioned<BulkLoadTransaction>> transaction(long tableId, String id) throws Exception {
        return read(
                transactionPath(tableId, id),
                bytes -> BulkLoadTransaction.fromJson(tableId, id, bytes));
    }

    Optional<Versioned<BulkLoadBucket>> bucket(BulkLoadTransaction transaction, TableBucket bucket)
            throws Exception {
        return read(
                bucketPath(transaction.tableId, transaction.bulkLoadId, bucket),
                BulkLoadBucket::fromJson);
    }

    Optional<Versioned<BulkLoadTarget>> target(
            BulkLoadTransaction transaction, @Nullable Long partitionId) throws Exception {
        return read(
                targetPath(transaction.tableId, transaction.bulkLoadId, partitionId),
                bytes -> BulkLoadTarget.fromJson(partitionId, bytes));
    }

    /** Persists the bucket result, snapshot reference, and recovery selection atomically. */
    void commitBucket(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<BulkLoadTarget> target,
            TableBucket bucket,
            Versioned<BulkLoadBucket> allocation,
            BucketSnapshot snapshot,
            List<CuratorOp> physicalChecks,
            int epochVersion)
            throws Exception {
        String snapshotsPath = ZkData.BucketSnapshotsZNode.path(bucket);
        // Never recreate physical bucket parents after deletion.
        try {
            curator.create().forPath(snapshotsPath);
        } catch (KeeperException.NodeExistsException ignored) {
            // Ordinary snapshots may already have created this directory.
        }
        List<CuratorOp> ops = mutationChecks(transaction, target, physicalChecks);
        ops.add(
                update(
                        allocation,
                        new BulkLoadBucket(
                                        allocation.value.snapshotId,
                                        allocation.value.logOffset,
                                        BulkLoadBucket.COMMITTED)
                                .toJson()));
        ops.add(
                create(
                        ZkData.BucketSnapshotIdZNode.path(bucket, snapshot.getSnapshotId()),
                        ZkData.BucketSnapshotIdZNode.encode(snapshot)));
        String recoveryPath = ZkData.RecoverySnapshotZNode.path(bucket);
        byte[] data = ZkData.RecoverySnapshotZNode.encode(snapshot.getSnapshotId());
        while (true) {
            Optional<VersionedData<Long>> previous = client.getRecoverySnapshotSelection(bucket);
            List<CuratorOp> attempt = new ArrayList<>(ops);
            attempt.add(
                    previous.isPresent()
                            ? curator.transactionOp()
                                    .setData()
                                    .withVersion(previous.get().zkVersion())
                                    .forPath(recoveryPath, data)
                            : create(recoveryPath, data));
            try {
                submit(attempt, epochVersion);
                return;
            } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
                Optional<VersionedData<Long>> current = client.getRecoverySnapshotSelection(bucket);
                if (current.isPresent() && current.get().data() == snapshot.getSnapshotId()) {
                    // Curator may have retried the multi after its first response was lost.
                    return;
                }
                if (previous.map(VersionedData::zkVersion)
                        .equals(current.map(VersionedData::zkVersion))) {
                    throw e;
                }
            }
        }
    }

    private List<CuratorOp> mutationChecks(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<BulkLoadTarget> target,
            List<CuratorOp> physicalChecks)
            throws Exception {
        List<CuratorOp> ops = new ArrayList<>(physicalChecks);
        ops.add(check(transaction));
        ops.add(check(target));
        return ops;
    }

    CuratorOp check(String path, int version) throws Exception {
        return curator.transactionOp().check().withVersion(version).forPath(path);
    }

    private CuratorOp check(Versioned<?> value) throws Exception {
        return check(value.path, value.version);
    }

    private CuratorOp create(String path, byte[] data) throws Exception {
        return curator.transactionOp().create().forPath(path, data);
    }

    private CuratorOp update(Versioned<?> value, byte[] data) throws Exception {
        return curator.transactionOp()
                .setData()
                .withVersion(value.version)
                .forPath(value.path, data);
    }

    private void submit(List<CuratorOp> ops, int epochVersion) throws Exception {
        curator.transaction().forOperations(client.wrapRequestsWithEpochCheck(ops, epochVersion));
    }

    private <T> Optional<Versioned<T>> read(String path, Function<byte[], T> decoder)
            throws Exception {
        Stat stat = new Stat();
        try {
            byte[] data = curator.getData().storingStatIn(stat).forPath(path);
            return Optional.of(new Versioned<>(path, stat.getVersion(), decoder.apply(data)));
        } catch (KeeperException.NoNodeException ignored) {
            return Optional.empty();
        }
    }

    /** A value and its version are read together for subsequent CAS operations. */
    static final class Versioned<T> {
        private final String path;
        private final int version;
        final T value;

        private Versioned(String path, int version, T value) {
            this.path = path;
            this.version = version;
            this.value = value;
        }
    }
}
