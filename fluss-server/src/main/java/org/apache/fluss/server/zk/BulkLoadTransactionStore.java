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

package org.apache.fluss.server.zk;

import org.apache.fluss.exception.BulkLoadConflictException;
import org.apache.fluss.exception.BulkLoadIncompleteException;
import org.apache.fluss.server.zk.ZkAsyncResponse.ZkGetDataResponse;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadBucketZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadOwnerZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTableZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTargetZNode;
import org.apache.fluss.server.zk.data.ZkData.BulkLoadTransactionZNode;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.server.zk.ZooKeeperClient.processGetDataResponsesOrThrow;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Persists BulkLoad transaction lifecycle metadata in ZooKeeper. */
public final class BulkLoadTransactionStore {
    private static final int BUCKET_DELETE_BATCH_SIZE = 128;

    private final ZooKeeperClient zooKeeperClient;
    private final ZooKeeperOp zkOp;

    /** Creates a store over the existing ZooKeeper connection. */
    public BulkLoadTransactionStore(ZooKeeperClient zooKeeperClient) {
        this.zooKeeperClient = zooKeeperClient;
        this.zkOp = new ZooKeeperOp(zooKeeperClient.getCuratorClient());
    }

    /** Atomically fixes the targets and acquires all ownership for a new transaction. */
    public void begin(
            BulkLoadTransaction transaction,
            List<BulkLoadTarget> targets,
            int coordinatorEpochZkVersion)
            throws Exception {
        long tableId = transaction.tableId;
        String bulkLoadId = transaction.bulkLoadId;
        boolean partitioned = targets.get(0).partitionId != null;
        ensureBulkLoadDirectory(BulkLoadTableZNode.transactions(tableId));
        ensureBulkLoadDirectory(
                partitioned
                        ? BulkLoadOwnerZNode.partitions(tableId)
                        : BulkLoadOwnerZNode.root(tableId));
        List<CuratorOp> operations = new ArrayList<>();
        operations.add(
                createPersistent(
                        BulkLoadTransactionZNode.path(tableId, bulkLoadId),
                        BulkLoadTransactionZNode.encode(transaction)));
        byte[] empty = new byte[0];
        operations.add(createPersistent(BulkLoadTargetZNode.root(tableId, bulkLoadId), empty));
        if (partitioned) {
            operations.add(
                    createPersistent(BulkLoadTargetZNode.partitions(tableId, bulkLoadId), empty));
        }
        byte[] ownerBytes = BulkLoadOwnerZNode.encode(bulkLoadId);
        for (BulkLoadTarget target : targets) {
            Long partitionId = target.partitionId;
            operations.add(
                    createPersistent(BulkLoadOwnerZNode.path(tableId, partitionId), ownerBytes));
            operations.add(
                    createPersistent(
                            BulkLoadTargetZNode.path(tableId, bulkLoadId, partitionId),
                            BulkLoadTargetZNode.encode(target)));
            operations.add(
                    createPersistent(BulkLoadBucketZNode.root(transaction, partitionId), empty));
        }
        List<CuratorOp> fencedOperations =
                zooKeeperClient.wrapRequestsWithEpochCheck(operations, coordinatorEpochZkVersion);
        try {
            zooKeeperClient.writeBulkLoadMetadata(fencedOperations, coordinatorEpochZkVersion);
        } catch (KeeperException.NodeExistsException e) {
            String ownerRoot = BulkLoadOwnerZNode.root(tableId) + "/";
            for (int i = 0; i < fencedOperations.size(); i++) {
                if (ZooKeeperClient.failedMultiOperation(e, i)
                        && fencedOperations
                                .get(i)
                                .getTypeAndPath()
                                .getForPath()
                                .startsWith(ownerRoot)) {
                    throw new BulkLoadConflictException(
                            "BulkLoad target is already owned. "
                                    + "Use ListInProgressBulkLoads to find the current owner.");
                }
            }
            throw e;
        }
    }

    /**
     * Lists running transactions that own at least one of the selected physical targets. A {@code
     * null} entry in {@code partitionIds} selects the whole-table target.
     */
    public List<BulkLoadTransaction> listRunningTransactions(long tableId, List<Long> partitionIds)
            throws Exception {
        List<String> ownerPaths = new ArrayList<>(partitionIds.size());
        for (Long partitionId : partitionIds) {
            ownerPaths.add(BulkLoadOwnerZNode.path(tableId, partitionId));
        }
        Map<String, String> owners =
                processGetDataResponsesOrThrow(
                        zooKeeperClient.getDataInBackground(ownerPaths),
                        ZkGetDataResponse::getPath,
                        BulkLoadOwnerZNode::decode);
        Set<String> transactionPaths = new LinkedHashSet<>();
        for (String ownerPath : ownerPaths) {
            String owner = owners.get(ownerPath);
            if (owner != null) {
                transactionPaths.add(BulkLoadTransactionZNode.path(tableId, owner));
            }
        }
        List<BulkLoadTransaction> runningTransactions = new ArrayList<>();
        for (VersionedData<BulkLoadTransaction> storedTransaction :
                readTransactions(transactionPaths)) {
            BulkLoadTransaction transaction = storedTransaction.data();
            if (transaction.state == BulkLoadTransaction.RUNNING) {
                runningTransactions.add(transaction);
            }
        }
        return runningTransactions;
    }

    /** Reads the fixed targets in physical partition ID order. */
    public List<BulkLoadTarget> getTargets(BulkLoadTransaction transaction, boolean partitioned)
            throws Exception {
        return partitioned ? getPartitionTargets(transaction) : getTableTarget(transaction);
    }

    /** Counts existing allocation records and completed bucket commits for the target. */
    public BucketProgress getBucketProgress(
            BulkLoadTransaction transaction, @Nullable Long partitionId) throws Exception {
        String bucketRoot = BulkLoadBucketZNode.root(transaction, partitionId);
        return countBucketProgress(bucketRoot, zooKeeperClient.getChildren(bucketRoot));
    }

    /** Persists withdrawn targets so prepared bucket writes fail their target version check. */
    public void persistWithdrawnTargets(
            BulkLoadTransaction transaction,
            List<BulkLoadTarget> withdrawnTargets,
            int coordinatorEpochZkVersion)
            throws Exception {
        List<CuratorOp> updates = new ArrayList<>();
        for (BulkLoadTarget target : withdrawnTargets) {
            updates.add(
                    zkOp.updateOp(
                            BulkLoadTargetZNode.path(
                                    transaction.tableId,
                                    transaction.bulkLoadId,
                                    target.partitionId),
                            BulkLoadTargetZNode.encode(target)));
        }
        writeWithCoordinatorEpochCheck(updates, coordinatorEpochZkVersion);
    }

    /** Ends the transaction and atomically releases its owners. */
    public BulkLoadTransaction finishTransaction(
            VersionedData<BulkLoadTransaction> transaction,
            List<BulkLoadTarget> targets,
            int state,
            @Nullable String abortMessage,
            int coordinatorEpochZkVersion)
            throws Exception {
        BulkLoadTransaction current = transaction.data();
        checkState(current.state == BulkLoadTransaction.RUNNING, "BulkLoad is already terminal.");
        if (state == BulkLoadTransaction.COMMITTED) {
            for (BulkLoadTarget target : targets) {
                if (target.withdrawn) {
                    continue;
                }
                String bucketRoot = BulkLoadBucketZNode.root(current, target.partitionId);
                List<String> bucketNodes = zooKeeperClient.getChildren(bucketRoot);
                // Allocate creates at most one record for each bucket in the target's fixed range.
                if (bucketNodes.size() != target.bucketCount
                        || countBucketProgress(bucketRoot, bucketNodes).committed
                                != target.bucketCount) {
                    throw new BulkLoadIncompleteException("BulkLoad transaction is incomplete.");
                }
            }
        }
        BulkLoadTransaction result =
                current.withTerminalState(state, System.currentTimeMillis(), abortMessage);
        List<CuratorOp> operations = new ArrayList<>();
        // Advancing the root version also fences in-flight bucket mutations.
        operations.add(
                zkOp.updateOp(
                        BulkLoadTransactionZNode.path(current.tableId, current.bulkLoadId),
                        BulkLoadTransactionZNode.encode(result),
                        transaction.zkVersion()));
        for (BulkLoadTarget target : targets) {
            operations.add(
                    zkOp.deleteOp(BulkLoadOwnerZNode.path(current.tableId, target.partitionId)));
        }
        writeWithCoordinatorEpochCheck(operations, coordinatorEpochZkVersion);
        return result;
    }

    /** Lists physical table IDs with BulkLoad metadata. */
    public List<Long> listTableIdsWithBulkLoadMetadata() throws Exception {
        return zooKeeperClient.getChildren(BulkLoadTableZNode.root()).stream()
                .map(Long::parseLong)
                .collect(Collectors.toList());
    }

    /**
     * Reads running and retained terminal transactions and their versions for one physical table.
     */
    public List<VersionedData<BulkLoadTransaction>> getTransactionsForTable(long tableId)
            throws Exception {
        return readTransactions(childPaths(BulkLoadTableZNode.transactions(tableId)));
    }

    /** Ends a running transaction before removing its bucket records. */
    public void reclaimTransaction(
            VersionedData<BulkLoadTransaction> storedTransaction, int coordinatorEpochZkVersion)
            throws Exception {
        BulkLoadTransaction transaction = storedTransaction.data();
        List<BulkLoadTarget> targets = discoverTargets(transaction);
        if (transaction.state == BulkLoadTransaction.RUNNING) {
            finishTransaction(
                    storedTransaction,
                    targets,
                    BulkLoadTransaction.ABORTED,
                    null,
                    coordinatorEpochZkVersion);
        }
        deleteTransactionRecords(transaction, targets, coordinatorEpochZkVersion);
    }

    /** Removes organizing directories once the event thread has reclaimed the last result. */
    public void pruneTableDirectories(long tableId, int coordinatorEpochZkVersion)
            throws Exception {
        String tableRoot = BulkLoadTableZNode.path(tableId);
        if (!zooKeeperClient.getStat(tableRoot).isPresent()) {
            return;
        }
        for (String path :
                new String[] {
                    BulkLoadOwnerZNode.partitions(tableId),
                    BulkLoadOwnerZNode.root(tableId),
                    BulkLoadTableZNode.transactions(tableId),
                    tableRoot
                }) {
            try {
                writeWithCoordinatorEpochCheck(
                        Collections.singletonList(zkOp.deleteOp(path)), coordinatorEpochZkVersion);
            } catch (KeeperException.NoNodeException | KeeperException.NotEmptyException ignored) {
                // A directory may be absent or still contain records pending the next pass.
            }
        }
    }

    private List<VersionedData<BulkLoadTransaction>> readTransactions(Collection<String> paths)
            throws Exception {
        Map<String, VersionedData<BulkLoadTransaction>> transactions = new HashMap<>();
        for (ZkGetDataResponse response : zooKeeperClient.getDataInBackground(paths)) {
            if (response.getResultCode() == KeeperException.Code.NONODE) {
                continue;
            }
            response.maybeThrow();
            int version = checkNotNull(response.getStat()).getVersion();
            transactions.put(
                    response.getPath(),
                    new VersionedData<>(
                            BulkLoadTransactionZNode.decode(response.getData()), version));
        }
        List<VersionedData<BulkLoadTransaction>> ordered = new ArrayList<>(transactions.size());
        for (String path : paths) {
            VersionedData<BulkLoadTransaction> transaction = transactions.get(path);
            if (transaction != null) {
                ordered.add(transaction);
            }
        }
        return ordered;
    }

    private List<BulkLoadTarget> discoverTargets(BulkLoadTransaction transaction) throws Exception {
        List<BulkLoadTarget> tableTarget = getTableTarget(transaction);
        return tableTarget.isEmpty() ? getPartitionTargets(transaction) : tableTarget;
    }

    private List<BulkLoadTarget> getTableTarget(BulkLoadTransaction transaction) throws Exception {
        Optional<VersionedData<BulkLoadTarget>> target =
                zooKeeperClient.getBulkLoadTarget(transaction, null);
        return target.isPresent()
                ? Collections.singletonList(target.get().data())
                : Collections.emptyList();
    }

    private List<BulkLoadTarget> getPartitionTargets(BulkLoadTransaction transaction)
            throws Exception {
        String partitionTargetsRoot =
                BulkLoadTargetZNode.partitions(transaction.tableId, transaction.bulkLoadId);
        List<BulkLoadTarget> targets = new ArrayList<>();
        for (ZkGetDataResponse response :
                zooKeeperClient.getDataInBackground(childPaths(partitionTargetsRoot))) {
            if (response.getResultCode() != KeeperException.Code.NONODE) {
                response.maybeThrow();
                targets.add(BulkLoadTargetZNode.decode(response.getData()));
            }
        }
        targets.sort(Comparator.comparing(target -> target.partitionId));
        return targets;
    }

    private BucketProgress countBucketProgress(String bucketRoot, List<String> bucketNodes)
            throws Exception {
        long allocated = 0;
        long committed = 0;
        for (ZkGetDataResponse response :
                zooKeeperClient.getDataInBackground(childPaths(bucketRoot, bucketNodes))) {
            if (response.getResultCode() == KeeperException.Code.NONODE) {
                continue;
            }
            response.maybeThrow();
            allocated++;
            if (BulkLoadBucketZNode.decode(response.getData()).state == BulkLoadBucket.COMMITTED) {
                committed++;
            }
        }
        return new BucketProgress(allocated, committed);
    }

    private CuratorOp createPersistent(String path, byte[] data) throws Exception {
        return zkOp.createOp(path, data, CreateMode.PERSISTENT);
    }

    private void ensureBulkLoadDirectory(String path) throws Exception {
        try {
            zooKeeperClient
                    .getCuratorClient()
                    .create()
                    .creatingParentsIfNeeded()
                    .forPath(path, new byte[0]);
        } catch (KeeperException.NodeExistsException ignored) {
            // Another transaction already created the organizing directory.
        }
    }

    private void deleteTransactionRecords(
            BulkLoadTransaction transaction,
            List<BulkLoadTarget> targets,
            int coordinatorEpochZkVersion)
            throws Exception {
        long tableId = transaction.tableId;
        String bulkLoadId = transaction.bulkLoadId;
        List<CuratorOp> removals = new ArrayList<>();
        for (BulkLoadTarget target : targets) {
            Long partitionId = target.partitionId;
            String bucketRecordsRoot = BulkLoadBucketZNode.root(transaction, partitionId);
            deleteBucketRecordsInBatches(bucketRecordsRoot, coordinatorEpochZkVersion);
            removals.add(zkOp.deleteOp(bucketRecordsRoot));
            removals.add(zkOp.deleteOp(BulkLoadTargetZNode.path(tableId, bulkLoadId, partitionId)));
        }
        if (!targets.isEmpty() && targets.get(0).partitionId != null) {
            removals.add(zkOp.deleteOp(BulkLoadTargetZNode.partitions(tableId, bulkLoadId)));
        }
        removals.add(zkOp.deleteOp(BulkLoadTargetZNode.root(tableId, bulkLoadId)));
        removals.add(zkOp.deleteOp(BulkLoadTransactionZNode.path(tableId, bulkLoadId)));
        writeWithCoordinatorEpochCheck(removals, coordinatorEpochZkVersion);
    }

    private void deleteBucketRecordsInBatches(
            String bucketRecordsRoot, int coordinatorEpochZkVersion) throws Exception {
        List<String> children = zooKeeperClient.getChildren(bucketRecordsRoot);
        for (int start = 0; start < children.size(); start += BUCKET_DELETE_BATCH_SIZE) {
            int end = Math.min(start + BUCKET_DELETE_BATCH_SIZE, children.size());
            List<CuratorOp> operations = new ArrayList<>(end - start);
            for (int i = start; i < end; i++) {
                operations.add(zkOp.deleteOp(bucketRecordsRoot + "/" + children.get(i)));
            }
            writeWithCoordinatorEpochCheck(operations, coordinatorEpochZkVersion);
        }
    }

    private List<String> childPaths(String root) throws Exception {
        return childPaths(root, zooKeeperClient.getChildren(root));
    }

    private static List<String> childPaths(String root, List<String> children) {
        List<String> paths = new ArrayList<>(children.size());
        for (String child : children) {
            paths.add(root + "/" + child);
        }
        return paths;
    }

    private void writeWithCoordinatorEpochCheck(
            List<CuratorOp> operations, int coordinatorEpochZkVersion) throws Exception {
        zooKeeperClient.writeBulkLoadMetadata(
                zooKeeperClient.wrapRequestsWithEpochCheck(operations, coordinatorEpochZkVersion),
                coordinatorEpochZkVersion);
    }

    /** The persisted bucket progress for a single physical target. */
    public static final class BucketProgress {
        public final long allocated;
        public final long committed;

        private BucketProgress(long allocated, long committed) {
            this.allocated = allocated;
            this.committed = committed;
        }
    }
}
