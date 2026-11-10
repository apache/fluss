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

import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadReplicaConvergence.ReplicaConvergenceContext;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.DataWithStat;
import org.apache.fluss.server.zk.ZooKeeperOp;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Versioned persistence for a shared BulkLoad transaction and its complete target set. */
final class BulkLoadMetadataStore {
    private final ZooKeeperClient zkClient;
    private final ZooKeeperOp zkOp;

    BulkLoadMetadataStore(ZooKeeperClient zkClient) {
        this.zkClient = checkNotNull(zkClient);
        this.zkOp = new ZooKeeperOp(zkClient.getCuratorClient());
    }

    Versioned<BulkLoadTransaction> createTransactionAndOwn(
            List<Versioned<?>> registrations,
            List<Versioned<? extends TableAssignment>> assignments,
            Versioned<TableRegistration> table,
            BulkLoadTransaction transaction,
            int epoch)
            throws Exception {
        List<CuratorOp> operations = beginOperations(epoch);
        operations.add(zkOp.checkOp(table.getPath(), table.getVersion()));
        for (Versioned<? extends TableAssignment> assignment : assignments) {
            operations.add(zkOp.checkOp(assignment.getPath(), assignment.getVersion()));
        }
        if (!zkClient.pathExists(transactionParentPath(transaction.getHandle()))) {
            operations.add(
                    zkOp.createOp(transactionParentPath(transaction.getHandle()), new byte[0]));
        }
        operations.add(
                zkOp.createOp(
                        transactionPath(transaction.getHandle()), encodeTransaction(transaction)));
        for (Versioned<?> registration : registrations) {
            checkArgument(
                    owner(registration.getValue()) == null, "BulkLoad target is already owned.");
            operations.add(
                    zkOp.updateOp(
                            registration.getPath(),
                            encodeRegistration(
                                    registration.getValue(), transaction.getBulkLoadId()),
                            registration.getVersion()));
        }
        // Admit only target sets whose largest terminal record and confirmation multi also fit.
        List<CuratorOp> terminal = beginOperations(epoch);
        terminal.add(zkOp.checkOp(transactionPath(transaction.getHandle()), 0));
        terminal.add(
                zkOp.updateOp(
                        transactionPath(transaction.getHandle()),
                        encodeTransaction(
                                transaction
                                        .withInput(
                                                String.join(
                                                        "", java.util.Collections.nCopies(64, "0")))
                                        .aborted(
                                                BulkLoadAbortReason.ABORTED_BY_CALLER,
                                                String.join(
                                                        "",
                                                        java.util.Collections.nCopies(
                                                                4096, "\u0001")),
                                                Long.MAX_VALUE)),
                        0));
        for (Versioned<?> registration : registrations) {
            terminal.add(zkOp.checkOp(registration.getPath(), registration.getVersion() + 1));
            terminal.add(
                    zkOp.updateOp(
                            registration.getPath(),
                            encodeRegistration(registration.getValue(), null),
                            registration.getVersion() + 1));
        }
        Map<String, BulkLoadTarget> targetsByPath = new java.util.HashMap<>();
        for (BulkLoadTarget target : transaction.getTargets()) {
            targetsByPath.put(target.getMetadataPath(), target);
        }
        for (int index = 0; index < assignments.size(); index++) {
            Versioned<? extends TableAssignment> assignment = assignments.get(index);
            terminal.add(zkOp.checkOp(assignment.getPath(), assignment.getVersion()));
            BulkLoadTarget target =
                    checkNotNull(targetsByPath.get(registrations.get(index).getPath()));
            for (int bucket : assignment.getValue().getBuckets()) {
                terminal.add(
                        zkOp.checkOp(
                                ZkData.LeaderAndIsrZNode.path(
                                        new TableBucket(
                                                transaction.getTableId(),
                                                target.getPartitionId(),
                                                bucket)),
                                0));
                terminal.add(zkOp.checkOp(ZkData.ServerIdZNode.path(Integer.MAX_VALUE), 0));
                terminal.add(
                        zkOp.checkOp(
                                ZkData.TabletServerSessionFenceZNode.path(
                                        Integer.MAX_VALUE, Long.MIN_VALUE),
                                0));
            }
        }
        zkClient.validateCheckedMultiSize(terminal);
        zkClient.submitCheckedMulti(operations);
        return new Versioned<>(transaction, transactionPath(transaction.getHandle()), 0, 0L);
    }

    void adoptBucketMetadata(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<?> registration,
            TableBucket bucket,
            BucketSnapshot snapshot,
            int epoch)
            throws Exception {
        checkArgument(
                transaction.getValue().getState()
                        == org.apache.fluss.metadata.BulkLoadState.COMMITTING,
                "Snapshot adoption requires the durable commit decision.");
        for (long snapshotId : zkClient.listBucketSnapshotIds(bucket)) {
            checkArgument(
                    snapshotId <= snapshot.getSnapshotId(),
                    "A newer unrelated Snapshot already exists.");
        }
        List<CuratorOp> operations =
                controlChecks(
                        epoch, transaction, java.util.Collections.singletonList(registration));
        String bucketPath = ZkData.BucketIdZNode.path(bucket);
        DataWithStat root = zkClient.getDataWithStat(bucketPath);
        operations.add(zkOp.checkOp(bucketPath, root.getStat().getVersion()));
        String path = ZkData.BucketSnapshotIdZNode.path(bucket, snapshot.getSnapshotId());
        Optional<Versioned<BucketSnapshot>> existing =
                readOptional(path, ZkData.BucketSnapshotIdZNode::decode);
        if (existing.isPresent()) {
            checkArgument(
                    existing.get().getValue().equals(snapshot),
                    "Conflicting ordinary Snapshot metadata.");
            operations.add(zkOp.checkOp(path, existing.get().getVersion()));
        } else {
            String parent = ZkData.BucketSnapshotsZNode.path(bucket);
            if (!zkClient.pathExists(parent)) {
                operations.add(zkOp.createOp(parent, new byte[0]));
            }
            operations.add(zkOp.createOp(path, ZkData.BucketSnapshotIdZNode.encode(snapshot)));
        }
        zkClient.submitCheckedMulti(operations);
    }

    Versioned<BulkLoadTransaction> update(
            Versioned<BulkLoadTransaction> current,
            BulkLoadTransaction replacement,
            List<Versioned<?>> registrations,
            @Nullable ReplicaConvergenceContext confirmed,
            int epoch)
            throws Exception {
        List<CuratorOp> operations = controlChecks(epoch, current, registrations);
        if (confirmed != null) {
            for (Versioned<? extends TableAssignment> assignment : confirmed.assignments.values()) {
                operations.add(zkOp.checkOp(assignment.getPath(), assignment.getVersion()));
            }
            for (Versioned<LeaderAndIsr> leader : confirmed.leaders.values()) {
                operations.add(zkOp.checkOp(leader.getPath(), leader.getVersion()));
            }
            for (Map.Entry<Integer, Versioned<TabletServerRegistration>> holder :
                    confirmed.holders.entrySet()) {
                Versioned<TabletServerRegistration> server = holder.getValue();
                operations.add(zkOp.checkOp(server.getPath(), server.getVersion()));
                operations.add(
                        zkOp.checkOp(
                                ZkData.TabletServerSessionFenceZNode.path(
                                        holder.getKey(), server.getEphemeralOwner()),
                                0));
            }
        }
        if (replacement.getState().isTerminal()) {
            for (Versioned<?> registration : registrations) {
                operations.add(
                        zkOp.updateOp(
                                registration.getPath(),
                                encodeRegistration(registration.getValue(), null),
                                registration.getVersion()));
            }
        }
        operations.add(
                zkOp.updateOp(
                        current.getPath(), encodeTransaction(replacement), current.getVersion()));
        Map<String, Stat> result = zkClient.submitCheckedMulti(operations);
        Stat stat = checkNotNull(result.get(current.getPath()));
        return new Versioned<>(
                replacement, current.getPath(), stat.getVersion(), stat.getEphemeralOwner());
    }

    private List<CuratorOp> beginOperations(int epoch) throws Exception {
        checkArgument(epoch >= 0, "Invalid coordinator epoch version.");
        List<CuratorOp> operations = new ArrayList<>();
        operations.add(zkOp.checkOp(ZkData.CoordinatorEpochZNode.path(), epoch));
        return operations;
    }

    private List<CuratorOp> controlChecks(
            int epoch, Versioned<BulkLoadTransaction> transaction, List<Versioned<?>> registrations)
            throws Exception {
        List<CuratorOp> operations = beginOperations(epoch);
        operations.add(zkOp.checkOp(transaction.getPath(), transaction.getVersion()));
        for (Versioned<?> registration : registrations) {
            operations.add(zkOp.checkOp(registration.getPath(), registration.getVersion()));
        }
        return operations;
    }

    <T> Optional<Versioned<T>> readOptional(String path, Function<byte[], T> decoder)
            throws Exception {
        Optional<DataWithStat> data = zkClient.getDataWithStatIfExists(path);
        if (!data.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(
                new Versioned<>(
                        decoder.apply(data.get().getData()),
                        path,
                        data.get().getStat().getVersion(),
                        data.get().getStat().getEphemeralOwner()));
    }

    static String transactionParentPath(BulkLoadHandle handle) {
        return ZkData.BulkLoadTableTransactionsZNode.path(handle.getTableId());
    }

    static String transactionPath(BulkLoadHandle handle) {
        return ZkData.BulkLoadTableTransactionZNode.path(
                handle.getTableId(), handle.getBulkLoadId());
    }

    static @Nullable String owner(Object registration) {
        return registration instanceof TableRegistration
                ? ((TableRegistration) registration).bulkLoadId
                : ((PartitionRegistration) registration).getBulkLoadId();
    }

    private static byte[] encodeTransaction(BulkLoadTransaction transaction) {
        return ZkData.BulkLoadTransactionsZNode.encode(transaction);
    }

    private static byte[] encodeRegistration(Object registration, @Nullable String owner) {
        return registration instanceof TableRegistration
                ? ZkData.TableZNode.encode(((TableRegistration) registration).withBulkLoadId(owner))
                : ZkData.PartitionZNode.encode(
                        ((PartitionRegistration) registration).withBulkLoadId(owner));
    }

    /** Immutable value plus the ZooKeeper observation that authorized it. */
    public static final class Versioned<T> {
        private final T value;
        private final String path;
        private final int version;
        private final long ephemeralOwner;

        public Versioned(T value, String path, int version, long ephemeralOwner) {
            this.value = checkNotNull(value);
            this.path = checkNotNull(path);
            this.version = version;
            this.ephemeralOwner = ephemeralOwner;
        }

        public T getValue() {
            return value;
        }

        public String getPath() {
            return path;
        }

        public int getVersion() {
            return version;
        }

        public long getEphemeralOwner() {
            return ephemeralOwner;
        }
    }
}
