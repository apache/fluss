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
import org.apache.fluss.exception.BulkLoadIncompleteException;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.BulkLoadStateException;
import org.apache.fluss.exception.BulkLoadTargetWithdrawnException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Versioned BulkLoad persistence. Callers resolve physical registrations before constructing a
 * mutation; their version checks are included in the same multi as the durable result.
 */
public final class BulkLoadMetadataStore {
    private static final Pattern BULK_LOAD_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}");
    private static final String ROOT = "/bulk_load/tables";
    private final ZooKeeperClient client;
    private final CuratorFramework curator;

    public BulkLoadMetadataStore(ZooKeeperClient client) {
        this.client = client;
        this.curator = client.getCuratorClient();
    }

    static String tablePath(long tableId) {
        return ROOT + "/" + tableId;
    }

    static void validateId(String id) {
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

    private static String ownerPath(long tableId, @Nullable Long partitionId) {
        return tablePath(tableId) + "/owners" + targetSuffix(partitionId);
    }

    private static String bucketPath(long tableId, String id, TableBucket bucket) {
        return targetPath(tableId, id, bucket.getPartitionId()) + "/buckets/" + bucket.getBucket();
    }

    /** Atomically establishes all ownership, without allocating any bucket. */
    public void begin(
            BulkLoadTransaction transaction,
            List<BulkLoadTarget> targets,
            List<CuratorOp> physicalChecks,
            int epochVersion)
            throws Exception {
        String table = tablePath(transaction.tableId);
        ensurePath(table + "/transactions");
        ensurePath(table + "/owners/partitions");
        List<CuratorOp> ops = new ArrayList<>(physicalChecks);
        String path = transactionPath(transaction.tableId, transaction.bulkLoadId);
        ops.add(create(path, transaction.toJson()));
        ops.add(create(path + "/targets", new byte[0]));
        if (targets.get(0).partitionId != null) {
            ops.add(create(path + "/targets/partitions", new byte[0]));
        }
        byte[] ownerBytes =
                JsonSerdeUtils.writeValueAsBytes(
                        transaction.bulkLoadId,
                        (id, out) -> {
                            out.writeStartObject();
                            out.writeNumberField("version", 1);
                            out.writeStringField("bulk_load_id", id);
                            out.writeEndObject();
                        });
        for (BulkLoadTarget target : targets) {
            String ownerPath = ownerPath(transaction.tableId, target.partitionId);
            Optional<Versioned<String>> owner = readOwner(ownerPath);
            if (owner.isPresent()) {
                Optional<Versioned<BulkLoadTransaction>> old =
                        transaction(transaction.tableId, owner.get().value);
                if (old.isPresent()) {
                    if (old.get().value.state == BulkLoadTransaction.RUNNING) {
                        throw new BulkLoadConflictException(
                                "BulkLoad target is owned by " + old.get().value.bulkLoadId + ".");
                    }
                    ops.add(check(old.get()));
                }
                ops.add(update(owner.get(), ownerBytes));
            } else {
                ops.add(create(ownerPath, ownerBytes));
            }
            String targetPath =
                    targetPath(transaction.tableId, transaction.bulkLoadId, target.partitionId);
            ops.add(create(targetPath, target.toJson()));
            ops.add(create(targetPath + "/buckets", new byte[0]));
        }
        submit(ops, epochVersion);
    }

    public Optional<Versioned<BulkLoadTransaction>> transaction(long tableId, String id)
            throws Exception {
        return read(
                transactionPath(tableId, id),
                bytes -> BulkLoadTransaction.fromJson(tableId, id, bytes));
    }

    public List<Versioned<BulkLoadTarget>> targets(BulkLoadTransaction transaction)
            throws Exception {
        String path = transactionPath(transaction.tableId, transaction.bulkLoadId) + "/targets";
        Optional<Versioned<BulkLoadTarget>> table = target(transaction, null);
        if (table.isPresent()) {
            return Collections.singletonList(table.get());
        }
        List<Versioned<BulkLoadTarget>> result = new ArrayList<>();
        List<Long> ids = new ArrayList<>();
        for (String id : client.getChildren(path + "/partitions")) {
            ids.add(Long.parseLong(id));
        }
        Collections.sort(ids);
        for (long id : ids) {
            read(path + "/partitions/" + id, bytes -> BulkLoadTarget.fromJson(id, bytes))
                    .ifPresent(result::add);
        }
        return result;
    }

    public Optional<Versioned<BulkLoadBucket>> bucket(
            BulkLoadTransaction transaction, TableBucket bucket) throws Exception {
        return read(
                bucketPath(transaction.tableId, transaction.bulkLoadId, bucket),
                BulkLoadBucket::fromJson);
    }

    public Optional<Versioned<BulkLoadTarget>> target(
            BulkLoadTransaction transaction, @Nullable Long partitionId) throws Exception {
        return read(
                targetPath(transaction.tableId, transaction.bulkLoadId, partitionId),
                bytes -> BulkLoadTarget.fromJson(partitionId, bytes));
    }

    /** Persists one allocation; the transaction check serializes it against termination. */
    public void allocate(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<BulkLoadTarget> target,
            TableBucket bucket,
            BulkLoadBucket allocation,
            List<CuratorOp> physicalChecks,
            int epochVersion)
            throws Exception {
        requireRunning(transaction, target);
        List<CuratorOp> ops = mutationChecks(transaction, target, physicalChecks);
        ops.add(
                create(
                        bucketPath(transaction.value.tableId, transaction.value.bulkLoadId, bucket),
                        allocation.toJson()));
        submit(ops, epochVersion);
    }

    /** Persists the immutable commit result and both independent recovery references together. */
    public void commitBucket(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<BulkLoadTarget> target,
            TableBucket bucket,
            Versioned<BulkLoadBucket> allocation,
            BucketSnapshot snapshot,
            List<CuratorOp> physicalChecks,
            int epochVersion)
            throws Exception {
        requireRunning(transaction, target);
        checkArgument(
                allocation.value.state == BulkLoadBucket.ALLOCATED,
                "Bucket has already committed.");
        checkArgument(
                snapshot.getSnapshotId() == allocation.value.snapshotId
                        && snapshot.getLogOffset() == allocation.value.logOffset,
                "Snapshot does not match its allocation.");
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

    /**
     * Completion is independent of replica recovery. Counts buckets without retaining their data.
     */
    public BulkLoadTransaction commit(
            Versioned<BulkLoadTransaction> transaction, int epochVersion, long now)
            throws Exception {
        requireRunning(transaction);
        for (Versioned<BulkLoadTarget> target : targets(transaction.value)) {
            if (target.value.withdrawn) {
                continue;
            }
            for (int id = 0; id < target.value.bucketCount; id++) {
                TableBucket bucket =
                        new TableBucket(transaction.value.tableId, target.value.partitionId, id);
                Optional<Versioned<BulkLoadBucket>> record = bucket(transaction.value, bucket);
                if (!record.isPresent() || record.get().value.state != BulkLoadBucket.COMMITTED) {
                    throw new BulkLoadIncompleteException("BulkLoad transaction is incomplete.");
                }
            }
        }
        // Target withdrawals and bucket commits are monotonic while RUNNING.
        return finish(transaction, BulkLoadTransaction.COMMITTED, now, null, epochVersion);
    }

    /** First successful terminal write fixes both the status and abort explanation. */
    public BulkLoadTransaction abort(
            Versioned<BulkLoadTransaction> transaction,
            long now,
            @Nullable String message,
            int epochVersion)
            throws Exception {
        requireRunning(transaction);
        return finish(transaction, BulkLoadTransaction.ABORTED, now, message, epochVersion);
    }

    private BulkLoadTransaction finish(
            Versioned<BulkLoadTransaction> transaction,
            int state,
            long now,
            @Nullable String message,
            int epochVersion)
            throws Exception {
        BulkLoadTransaction result = transaction.value.finish(state, now, message);
        submit(Collections.singletonList(update(transaction, result.toJson())), epochVersion);
        return result;
    }

    /** Withdrawal cannot modify a retained terminal result. */
    public void withdraw(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<BulkLoadTarget> target,
            int epochVersion)
            throws Exception {
        requireRunning(transaction);
        List<CuratorOp> ops = new ArrayList<>();
        ops.add(check(transaction));
        ops.add(update(target, target.value.withdraw().toJson()));
        submit(ops, epochVersion);
    }

    Versioned<BulkLoadTransaction> withdrawDroppedTargets(
            Versioned<BulkLoadTransaction> transaction, int epochVersion) throws Exception {
        if (transaction.value.state != BulkLoadTransaction.RUNNING) {
            return transaction;
        }
        for (Versioned<BulkLoadTarget> target : targets(transaction.value)) {
            if (target.value.partitionId == null || target.value.withdrawn) {
                continue;
            }
            Optional<PartitionRegistration> partition =
                    client.getPartition(transaction.value.tablePath, target.value.partitionName);
            if (partition.isPresent()
                    && partition.get().getPartitionId() == target.value.partitionId) {
                continue;
            }
            try {
                withdraw(transaction, target, epochVersion);
            } catch (KeeperException.BadVersionException e) {
                BulkLoadTransaction value = transaction.value;
                Versioned<BulkLoadTransaction> current =
                        transaction(value.tableId, value.bulkLoadId)
                                .orElseThrow(
                                        () ->
                                                new BulkLoadNotFoundException(
                                                        "BulkLoad result was reclaimed."));
                if (current.value.state != BulkLoadTransaction.RUNNING) {
                    return current;
                }
                if (!target(current.value, target.value.partitionId)
                        .map(updated -> updated.value.withdrawn)
                        .orElse(false)) {
                    throw e;
                }
            }
        }
        return transaction;
    }

    /** Enumerates physical ownership only; terminal history is never listed. */
    public List<BulkLoadTransaction> list(long tableId, List<Long> partitions) throws Exception {
        Map<String, BulkLoadTransaction> result = new LinkedHashMap<>();
        for (Long partition : partitions) {
            Optional<Versioned<String>> owner = readOwner(ownerPath(tableId, partition));
            if (owner.isPresent()) {
                Optional<Versioned<BulkLoadTransaction>> transaction =
                        transaction(tableId, owner.get().value);
                if (transaction.isPresent()
                        && transaction.get().value.state == BulkLoadTransaction.RUNNING) {
                    result.put(transaction.get().value.bulkLoadId, transaction.get().value);
                }
            }
        }
        return new ArrayList<>(result.values());
    }

    /** Reclaims expired results; selected recovery snapshot records live outside this subtree. */
    public void maintain(long now, long retentionMillis, int epochVersion) throws Exception {
        checkArgument(retentionMillis > 0, "Result retention must be positive.");
        Set<Long> liveTableIds = null;
        for (String table : children(ROOT)) {
            long tableId = Long.parseLong(table);
            List<String> transactions = children(tablePath(tableId) + "/transactions");
            boolean deletedTable = false;
            if (transactions.isEmpty()
                    && curator.checkExists().forPath(ZkData.TableIdZNode.path(tableId)) == null) {
                // Partitioned tables may have no table assignment. Read registrations after
                // enumerating the existing BulkLoad roots so concurrent Begin cannot be pruned.
                if (liveTableIds == null) {
                    liveTableIds = readLiveTableIds();
                }
                deletedTable = !liveTableIds.contains(tableId);
            }
            for (String id : transactions) {
                Optional<Versioned<BulkLoadTransaction>> transaction = transaction(tableId, id);
                if (!transaction.isPresent()) {
                    continue;
                }
                BulkLoadTransaction value = transaction.get().value;
                Optional<TableRegistration> registration = client.getTable(value.tablePath);
                boolean deleted =
                        !registration.isPresent() || registration.get().tableId != tableId;
                deletedTable |= deleted;
                if (!deleted && value.state == BulkLoadTransaction.RUNNING) {
                    try {
                        withdrawDroppedTargets(transaction.get(), epochVersion);
                    } catch (KeeperException.BadVersionException
                            | BulkLoadNotFoundException ignored) {
                        // A concurrent RPC changed the transaction or target. Revisit on the next
                        // pass.
                    }
                }
                if (deleted
                        || (value.state != BulkLoadTransaction.RUNNING
                                && value.finishedTime != null
                                && value.finishedTime <= now - retentionMillis)) {
                    removeOwners(transaction.get(), epochVersion);
                    deleteTree(transaction.get().path, transaction.get(), epochVersion);
                }
            }
            // Begin creates the organizing paths before publishing its transaction.
            if (deletedTable) {
                pruneEmpty(tablePath(tableId) + "/owners/partitions", epochVersion);
                pruneEmpty(tablePath(tableId) + "/owners", epochVersion);
                pruneEmpty(tablePath(tableId) + "/transactions", epochVersion);
                pruneEmpty(tablePath(tableId), epochVersion);
            }
        }
    }

    private Set<Long> readLiveTableIds() throws Exception {
        List<TablePath> paths = new ArrayList<>();
        for (String database : client.listDatabases()) {
            for (String table : client.listTables(database)) {
                paths.add(TablePath.of(database, table));
            }
        }
        return client.getRawTableRegistrations(paths).values().stream()
                .map(registration -> registration.tableId)
                .collect(Collectors.toSet());
    }

    private void pruneEmpty(String path, int epochVersion) throws Exception {
        try {
            submit(
                    Collections.singletonList(curator.transactionOp().delete().forPath(path)),
                    epochVersion);
        } catch (KeeperException.NoNodeException | KeeperException.NotEmptyException ignored) {
            // A concurrent Begin may have populated the organizing node.
        }
    }

    private void removeOwners(Versioned<BulkLoadTransaction> transaction, int epochVersion)
            throws Exception {
        for (Versioned<BulkLoadTarget> target : targets(transaction.value)) {
            String path = ownerPath(transaction.value.tableId, target.value.partitionId);
            Optional<Versioned<String>> owner = readOwner(path);
            if (owner.isPresent() && owner.get().value.equals(transaction.value.bulkLoadId)) {
                List<CuratorOp> ops = new ArrayList<>();
                ops.add(check(transaction));
                ops.add(
                        curator.transactionOp()
                                .delete()
                                .withVersion(owner.get().version)
                                .forPath(path));
                try {
                    submit(ops, epochVersion);
                } catch (KeeperException.BadVersionException e) {
                    // A later Begin may have replaced an expired ownership record.
                    Optional<Versioned<String>> current = readOwner(path);
                    if (current.isPresent()
                            && current.get().value.equals(transaction.value.bulkLoadId)) {
                        throw e;
                    }
                } catch (KeeperException.NoNodeException ignored) {
                    // Another maintenance attempt already removed the stale owner.
                }
            }
        }
    }

    private Optional<Versioned<String>> readOwner(String path) throws Exception {
        return read(
                path,
                bytes ->
                        JsonSerdeUtils.readValue(bytes, node -> node.get("bulk_load_id").asText()));
    }

    private void deleteTree(
            String path, Versioned<BulkLoadTransaction> transaction, int epochVersion)
            throws Exception {
        for (String child : children(path)) {
            deleteTree(path + "/" + child, transaction, epochVersion);
        }
        List<CuratorOp> ops = new ArrayList<>();
        ops.add(check(transaction));
        ops.add(curator.transactionOp().delete().forPath(path));
        try {
            submit(ops, epochVersion);
        } catch (KeeperException.NoNodeException ignored) {
            // A previous attempt may have completed this subtree before its response was lost.
        }
    }

    private static void requireRunning(
            Versioned<BulkLoadTransaction> transaction, Versioned<BulkLoadTarget> target) {
        requireRunning(transaction);
        if (target.value.withdrawn) {
            throw new BulkLoadTargetWithdrawnException("BulkLoad target has been withdrawn.");
        }
    }

    private static void requireRunning(Versioned<BulkLoadTransaction> transaction) {
        if (transaction.value.state != BulkLoadTransaction.RUNNING) {
            throw new BulkLoadStateException("BulkLoad transaction has ended.");
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

    private void ensurePath(String path) throws Exception {
        try {
            curator.create().creatingParentsIfNeeded().forPath(path);
        } catch (KeeperException.NodeExistsException ignored) {
            // Concurrent Begin can establish shared organizing nodes.
        }
    }

    private List<String> children(String path) throws Exception {
        try {
            return curator.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException ignored) {
            return Collections.emptyList();
        }
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
    public static final class Versioned<T> {
        private final String path;
        final int version;
        public final T value;

        private Versioned(String path, int version, T value) {
            this.path = path;
            this.version = version;
            this.value = value;
        }
    }
}
