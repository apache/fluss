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

import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadReplicaConvergence.ReplicaRound;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.DataWithStat;
import org.apache.fluss.server.zk.ZooKeeperOp;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Package-local persistence for transitions decided by {@link BulkLoadManager}. Every write checks
 * the observed ZooKeeper versions and coordinator epoch atomically.
 */
final class BulkLoadMetadataStore {

    private final ZooKeeperClient zkClient;
    private final ZooKeeperOp zkOp;

    BulkLoadMetadataStore(ZooKeeperClient zkClient) {
        this.zkClient = checkNotNull(zkClient);
        this.zkOp = new ZooKeeperOp(zkClient.getCuratorClient());
    }

    Versioned<BulkLoadTransaction> createTransactionAndFence(
            Versioned<?> registration,
            Versioned<? extends TableAssignment> assignment,
            BulkLoadTransaction transaction,
            int coordinatorEpochVersion)
            throws Exception {
        BulkLoadHandle handle = transaction.getHandle();
        List<CuratorOp> operations = beginOperations(coordinatorEpochVersion);
        operations.add(zkOp.checkOp(registration.getPath(), registration.getVersion()));
        operations.add(zkOp.checkOp(assignment.getPath(), assignment.getVersion()));
        if (!zkClient.pathExists(transactionParentPath(handle))) {
            operations.add(zkOp.createOp(transactionParentPath(handle), new byte[0]));
        }
        operations.add(zkOp.createOp(transactionPath(handle), encodeTransaction(transaction)));
        operations.add(
                zkOp.updateOp(
                        registration.getPath(),
                        encodeRegistration(
                                registration.getValue(),
                                BulkLoadDataState.LOADING,
                                handle.getBulkLoadId()),
                        registration.getVersion()));
        zkClient.submitCheckedMulti(operations);
        return new Versioned<>(transaction, transactionPath(handle), 0, 0L);
    }

    void adoptBucketMetadata(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<?> registration,
            int bucketId,
            BucketSnapshot snapshot,
            int coordinatorEpochVersion)
            throws Exception {
        TableBucket tableBucket =
                new TableBucket(
                        transaction.getValue().getTableId(),
                        transaction.getValue().getPartitionId(),
                        bucketId);
        String bucketPath = ZkData.BucketIdZNode.path(tableBucket);
        String snapshotPath =
                ZkData.BucketSnapshotIdZNode.path(tableBucket, snapshot.getSnapshotId());
        DataWithStat bucketRoot = zkClient.getDataWithStat(bucketPath);
        Optional<Versioned<BucketSnapshot>> existingSnapshot =
                readOptional(snapshotPath, ZkData.BucketSnapshotIdZNode::decode);
        List<CuratorOp> operations =
                controlChecks(coordinatorEpochVersion, transaction, registration);
        operations.add(zkOp.checkOp(bucketPath, bucketRoot.getStat().getVersion()));
        if (existingSnapshot.isPresent()) {
            checkArgument(
                    existingSnapshot.get().getValue().equals(snapshot),
                    "BulkLoad adoption found conflicting ordinary Snapshot metadata.");
            operations.add(zkOp.checkOp(snapshotPath, existingSnapshot.get().getVersion()));
        } else {
            String snapshotParent = ZkData.BucketSnapshotsZNode.path(tableBucket);
            if (!zkClient.pathExists(snapshotParent)) {
                operations.add(zkOp.createOp(snapshotParent, new byte[0]));
            }
            operations.add(
                    zkOp.createOp(snapshotPath, ZkData.BucketSnapshotIdZNode.encode(snapshot)));
        }
        zkClient.submitCheckedMulti(operations);
    }

    /** Atomically publishes a transaction transition with the observations that authorized it. */
    Versioned<BulkLoadTransaction> update(
            Versioned<BulkLoadTransaction> current,
            BulkLoadTransaction replacement,
            Versioned<?> registration,
            @Nullable ReplicaRound confirmed,
            int coordinatorEpochVersion)
            throws Exception {
        List<CuratorOp> operations = controlChecks(coordinatorEpochVersion, current, registration);
        if (confirmed != null) {
            operations.add(
                    zkOp.checkOp(
                            confirmed.assignment.getPath(), confirmed.assignment.getVersion()));
            addRegistrationChecks(operations, confirmed);
        }
        if (replacement.getMetadataVersion() != current.getValue().getMetadataVersion()) {
            operations.add(
                    zkOp.updateOp(
                            registration.getPath(),
                            encodeRegistration(
                                    registration.getValue(),
                                    BulkLoadDataState.ACTIVE,
                                    replacement.getState().isTerminal()
                                            ? null
                                            : replacement.getBulkLoadId()),
                            registration.getVersion()));
        }
        operations.add(
                zkOp.updateOp(
                        current.getPath(), encodeTransaction(replacement), current.getVersion()));
        return transactionResult(replacement, zkClient.submitCheckedMulti(operations));
    }

    private List<CuratorOp> beginOperations(int coordinatorEpochVersion) throws Exception {
        checkArgument(
                coordinatorEpochVersion >= 0, "Coordinator epoch version must be non-negative.");
        return new ArrayList<>(
                Collections.singletonList(
                        zkOp.checkOp(
                                ZkData.CoordinatorEpochZNode.path(), coordinatorEpochVersion)));
    }

    private List<CuratorOp> controlChecks(
            int coordinatorEpochVersion,
            Versioned<BulkLoadTransaction> transaction,
            Versioned<?> registration)
            throws Exception {
        List<CuratorOp> operations = beginOperations(coordinatorEpochVersion);
        operations.add(zkOp.checkOp(transaction.getPath(), transaction.getVersion()));
        operations.add(zkOp.checkOp(registration.getPath(), registration.getVersion()));
        return operations;
    }

    private void addRegistrationChecks(List<CuratorOp> operations, ReplicaRound assignment)
            throws Exception {
        for (Map.Entry<Integer, RegisteredServer> entry : assignment.holders.entrySet()) {
            RegisteredServer holder = entry.getValue();
            if (holder == null) {
                zkOp.assertAbsent(operations, ZkData.ServerIdZNode.path(entry.getKey()));
            } else {
                Versioned<TabletServerRegistration> registration = holder.registration;
                operations.add(zkOp.checkOp(registration.getPath(), registration.getVersion()));
                operations.add(
                        zkOp.checkOp(
                                ZkData.TabletServerSessionFenceZNode.path(
                                        holder.serverId, registration.getEphemeralOwner()),
                                0));
            }
        }
    }

    private Versioned<BulkLoadTransaction> transactionResult(
            BulkLoadTransaction value, Map<String, Stat> result) throws Exception {
        String path = transactionPath(value.getHandle());
        Stat stat = checkNotNull(result.get(path), "Missing transaction write result.");
        return new Versioned<>(value, path, stat.getVersion(), stat.getEphemeralOwner());
    }

    <T> Optional<Versioned<T>> readOptional(String path, Function<byte[], T> decoder)
            throws Exception {
        Optional<DataWithStat> data = zkClient.getDataWithStatIfExists(path);
        if (!data.isPresent()) {
            return Optional.empty();
        }
        DataWithStat current = data.get();
        return Optional.of(
                new Versioned<>(
                        decoder.apply(current.getData()),
                        path,
                        current.getStat().getVersion(),
                        current.getStat().getEphemeralOwner()));
    }

    static String transactionParentPath(BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? ZkData.BulkLoadTableTransactionsZNode.path(handle.getTableId())
                : ZkData.BulkLoadPartitionTransactionsZNode.path(handle.getPartitionId());
    }

    static String transactionPath(BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? ZkData.BulkLoadTableTransactionZNode.path(
                        handle.getTableId(), handle.getBulkLoadId())
                : ZkData.BulkLoadPartitionTransactionZNode.path(
                        handle.getPartitionId(), handle.getBulkLoadId());
    }

    private static byte[] encodeTransaction(BulkLoadTransaction value) {
        return ZkData.BulkLoadTransactionsZNode.encode(value);
    }

    private static byte[] encodeRegistration(
            Object registration, BulkLoadDataState state, @Nullable String bulkLoadId) {
        return registration instanceof TableRegistration
                ? ZkData.TableZNode.encode(
                        ((TableRegistration) registration).withDataState(state, bulkLoadId))
                : ZkData.PartitionZNode.encode(
                        ((PartitionRegistration) registration).withDataState(state, bulkLoadId));
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

    static final class RegisteredServer {
        final int serverId;
        final Versioned<TabletServerRegistration> registration;

        RegisteredServer(int serverId, Versioned<TabletServerRegistration> registration) {
            this.serverId = serverId;
            this.registration = registration;
        }
    }
}
