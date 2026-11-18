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
import org.apache.fluss.exception.BulkLoadStateException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for atomic ownership, commit decisions and independent recovery metadata. */
class BulkLoadMetadataStoreTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZooKeeperClient client;
    private BulkLoadMetadataStore store;
    private static final int EPOCH = -1;

    @BeforeEach
    void setUp() {
        client = ZK.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        ZK.getCustomExtension().cleanupRoot();
        store = new BulkLoadMetadataStore(client);
    }

    @Test
    void testOverlappingBeginIsAtomicAndTerminalOwnerCanBeReplaced() throws Exception {
        begin("a", 10L);
        assertThatThrownBy(() -> begin("b", 11L, 10L))
                .isInstanceOf(BulkLoadConflictException.class)
                .hasMessageContaining("owned by a");
        assertThat(store.transaction(1, "b")).isEmpty();
        assertThat(store.list(1, Collections.singletonList(11L))).isEmpty();
        store.abort(transaction("a"), 20, null, EPOCH);
        begin("b", 10L, 11L);
        assertThat(store.list(1, Arrays.asList(10L, 11L)))
                .extracting(t -> t.bulkLoadId)
                .containsExactly("b");
    }

    @Test
    void testAbortFencesAllocatedBucketCommit() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        Versioned<BulkLoadTransaction> stale = transaction("a");
        Versioned<BulkLoadTarget> target = store.targets(stale.value).get(0);
        Versioned<BulkLoadBucket> allocation = store.bucket(stale.value, bucket).get();
        store.abort(stale, 20, "cancelled", EPOCH);
        assertThatThrownBy(
                        () ->
                                store.commitBucket(
                                        stale,
                                        target,
                                        bucket,
                                        allocation,
                                        new BucketSnapshot(7, 100, "file:/snapshot/_METADATA"),
                                        Collections.emptyList(),
                                        EPOCH))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(client.getRecoverySnapshotSelection(bucket)).isEmpty();
        assertThat(client.getTableBucketSnapshot(bucket, 7)).isEmpty();
        assertThat(store.bucket(transaction("a").value, bucket).get().value.state)
                .isEqualTo(BulkLoadBucket.ALLOCATED);
    }

    @Test
    void testTransactionCompletionDoesNotChangeRecoverySelection() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        commitBucket("a", bucket);
        store.commit(transaction("a"), EPOCH, 20);
        assertThat(transaction("a").value.state).isEqualTo(BulkLoadTransaction.COMMITTED);
        assertThat(client.getRecoverySnapshotSelection(bucket).get().zkVersion()).isZero();
        begin("b", 10L);
        allocate("b", bucket, 8, 110);
        commitBucket("b", bucket);
        assertThat(client.getRecoverySnapshotSelection(bucket).get().data()).isEqualTo(8);
        assertThat(client.getRecoverySnapshotSelection(bucket).get().zkVersion()).isEqualTo(1);
        assertThat(store.bucket(transaction("a").value, bucket).get().value.snapshotId)
                .isEqualTo(7);
    }

    @Test
    void testUnallocatedBucketsBlockCommitAndWithdrawalAllowsCompletion() throws Exception {
        begin("a", 10L);
        assertThatThrownBy(() -> store.commit(transaction("a"), EPOCH, 20))
                .isInstanceOf(BulkLoadIncompleteException.class)
                .hasMessageContaining("incomplete");
        Versioned<BulkLoadTransaction> transaction = transaction("a");
        Versioned<BulkLoadTarget> target = store.targets(transaction.value).get(0);
        store.withdraw(transaction, target, EPOCH);
        store.commit(transaction("a"), EPOCH, 20);
        assertThatThrownBy(
                        () ->
                                store.withdraw(
                                        transaction("a"),
                                        store.targets(transaction("a").value).get(0),
                                        EPOCH))
                .isInstanceOf(BulkLoadStateException.class);
    }

    @Test
    void testRetentionPreservesRunningOwnerAndPhysicalRecoveryRecord() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        commitBucket("a", bucket);
        store.abort(transaction("a"), 20, null, EPOCH);
        registerTable();
        begin("b", 10L);
        store.maintain(119, 100, EPOCH);
        assertThat(store.transaction(1, "a")).isPresent();
        store.maintain(120, 100, EPOCH);
        assertThat(store.transaction(1, "a")).isEmpty();
        assertThat(store.list(1, Collections.singletonList(10L)))
                .extracting(t -> t.bulkLoadId)
                .containsExactly("b");
        assertThat(client.getRecoverySnapshotSelection(bucket).get().data()).isEqualTo(7);
        assertThat(client.getTableBucketSnapshot(bucket, 7))
                .hasValueSatisfying(snapshot -> assertThat(snapshot.getLogOffset()).isEqualTo(100));
        client.deleteTable(TablePath.of("db", "t"));
        store.maintain(121, 100, EPOCH);
        assertThat(
                        client.getCuratorClient()
                                .checkExists()
                                .forPath(BulkLoadMetadataStore.tablePath(1)))
                .isNull();
        assertThat(client.getRecoverySnapshotSelection(bucket).get().data()).isEqualTo(7);
    }

    @Test
    void testDeposedCoordinatorCannotWriteTerminalState() throws Exception {
        begin("a", 10L);
        int epoch = client.fenceBecomeCoordinatorLeader("first").getCoordinatorEpochZkVersion();
        client.fenceBecomeCoordinatorLeader("second");
        assertThatThrownBy(() -> store.abort(transaction("a"), 20, null, epoch))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(transaction("a").value.state).isEqualTo(BulkLoadTransaction.RUNNING);
    }

    @Test
    void testDeposedCoordinatorCannotReclaimExpiredResults() throws Exception {
        begin("a", 10L);
        registerTable();
        store.abort(transaction("a"), 20, null, EPOCH);
        int stale = client.fenceBecomeCoordinatorLeader("first").getCoordinatorEpochZkVersion();
        int current = client.fenceBecomeCoordinatorLeader("second").getCoordinatorEpochZkVersion();
        assertThatThrownBy(() -> store.maintain(120, 100, stale))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(store.transaction(1, "a")).isPresent();
        store.maintain(120, 100, current);
        assertThat(store.transaction(1, "a")).isEmpty();
        assertThat(client.getChildren(BulkLoadMetadataStore.tablePath(1)))
                .containsExactlyInAnyOrder("owners", "transactions");
        store.maintain(121, 100, current);
        assertThat(client.getChildren(BulkLoadMetadataStore.tablePath(1)))
                .containsExactlyInAnyOrder("owners", "transactions");
    }

    @Test
    void testSnapshotReferenceConflictCannotPartiallyCommitBucket() throws Exception {
        begin("a", 10L);
        TableBucket bucket = new TableBucket(1, 10L, 0);
        allocate("a", bucket, 7, 100);
        client.registerTableBucketSnapshot(bucket, new BucketSnapshot(7, 100, "file:/existing"));
        assertThatThrownBy(() -> commitBucket("a", bucket))
                .isInstanceOf(KeeperException.NodeExistsException.class);
        assertThat(store.bucket(transaction("a").value, bucket).get().value.state)
                .isEqualTo(BulkLoadBucket.ALLOCATED);
        assertThat(client.getRecoverySnapshotSelection(bucket)).isEmpty();
    }

    private void registerTable() throws Exception {
        client.registerTable(
                TablePath.of("db", "t"),
                TableRegistration.newTable(
                        1, "file:/remote", transaction("a").value.tableInfo.toTableDescriptor()));
    }

    private Versioned<BulkLoadTransaction> transaction(String id) throws Exception {
        return store.transaction(1, id).get();
    }

    private void begin(String id, Long... partitions) throws Exception {
        TableInfo table =
                TableInfo.of(
                        TablePath.of("db", "t"),
                        1,
                        1,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("p", DataTypes.STRING())
                                                .column("k", DataTypes.INT())
                                                .primaryKey("p", "k")
                                                .build())
                                .partitionedBy("p")
                                .distributedBy(1, "k")
                                .build(),
                        "file:/remote",
                        1,
                        1);
        BulkLoadTransaction transaction =
                new BulkLoadTransaction(1, id, table, BulkLoadTransaction.RUNNING, 10, null, null);
        List<BulkLoadTarget> targets = new ArrayList<>();
        for (long partition : partitions) {
            targets.add(new BulkLoadTarget(partition, "p" + partition, 1, "file:/remote", false));
        }
        store.begin(transaction, targets, Collections.emptyList(), EPOCH);
    }

    private void allocate(String id, TableBucket bucket, long sid, long offset) throws Exception {
        String path = ZkData.BucketIdZNode.path(bucket);
        if (client.getCuratorClient().checkExists().forPath(path) == null) {
            client.getCuratorClient().create().creatingParentsIfNeeded().forPath(path);
        }
        store.allocate(
                transaction(id),
                store.targets(transaction(id).value).get(0),
                bucket,
                new BulkLoadBucket(sid, offset, BulkLoadBucket.ALLOCATED),
                Collections.emptyList(),
                EPOCH);
    }

    private void commitBucket(String id, TableBucket bucket) throws Exception {
        commitBucket(id, bucket, "file:/snapshot/_METADATA");
    }

    private void commitBucket(String id, TableBucket bucket, String metadataPath) throws Exception {
        Versioned<BulkLoadTransaction> transaction = transaction(id);
        Versioned<BulkLoadBucket> allocation = store.bucket(transaction.value, bucket).get();
        store.commitBucket(
                transaction,
                store.targets(transaction.value).get(0),
                bucket,
                allocation,
                new BucketSnapshot(
                        allocation.value.snapshotId, allocation.value.logOffset, metadataPath),
                Collections.emptyList(),
                EPOCH);
    }
}
