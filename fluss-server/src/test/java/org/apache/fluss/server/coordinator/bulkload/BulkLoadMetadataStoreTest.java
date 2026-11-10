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
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadReplicaConvergence.ReplicaConvergenceContext;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Atomic ownership and terminal CAS behavior for shared partition transactions. */
class BulkLoadMetadataStoreTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZooKeeperClient client;
    private BulkLoadMetadataStore store;
    private TablePath path;
    private int epoch;
    private long tableId;

    @BeforeEach
    void setUp() throws Exception {
        client = ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE);
        epoch =
                client.fenceBecomeCoordinatorLeader("shared-bulkload")
                        .getCoordinatorEpochZkVersion();
        store = new BulkLoadMetadataStore(client);
        client.ensureBulkLoadMetadataPaths();
        tableId = Math.abs(UUID.randomUUID().getMostSignificantBits());
        path = TablePath.of("shared", "t" + tableId);
        client.registerTable(
                path,
                TableRegistration.newTable(tableId, "file:///warehouse", DATA1_TABLE_DESCRIPTOR));
        for (long id = 1; id <= 3; id++) {
            create(
                    ZkData.PartitionZNode.path(path, "p" + id),
                    ZkData.PartitionZNode.encode(
                            new PartitionRegistration(
                                    tableId, tableId + id, "file:///warehouse", 1)));
            create(
                    ZkData.PartitionIdZNode.path(tableId + id),
                    ZkData.PartitionIdZNode.encode(
                            new PartitionAssignment(
                                    tableId, Collections.singletonMap(0, BucketAssignment.of(1)))));
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
    }

    @Test
    void testConflictingTargetLeavesWholeSetUnowned() throws Exception {
        Versioned<BulkLoadTransaction> first = begin(2L);
        assertThatThrownBy(() -> begin(1L, 2L)).isInstanceOf(IllegalArgumentException.class);
        assertThat(registration(1L).getValue().getBulkLoadId()).isNull();
        assertThat(registration(2L).getValue().getBulkLoadId())
                .isEqualTo(first.getValue().getBulkLoadId());
        assertThat(
                        client.getChildren(
                                BulkLoadMetadataStore.transactionParentPath(
                                        first.getValue().getHandle())))
                .hasSize(1);
    }

    @Test
    void testMissingTargetRollsBackEveryOwner() throws Exception {
        List<Versioned<?>> registrations = Arrays.asList(registration(1L), registration(2L));
        BulkLoadTransaction value = transaction(1L, 2L);
        client.getCuratorClient().delete().forPath(registration(2L).getPath());
        assertThatThrownBy(
                        () ->
                                store.createTransactionAndOwn(
                                        registrations, assignments(1L, 2L), table(), value, epoch))
                .isInstanceOf(KeeperException.NoNodeException.class);
        assertThat(registration(1L).getValue().getBulkLoadId()).isNull();
        assertThat(client.pathExists(BulkLoadMetadataStore.transactionPath(value.getHandle())))
                .isFalse();
    }

    @Test
    void testDisjointOwnersAndAtomicAbortRelease() throws Exception {
        Versioned<BulkLoadTransaction> first = begin(1L, 2L);
        Versioned<BulkLoadTransaction> disjoint = begin(3L);
        Versioned<BulkLoadTransaction> aborted =
                store.update(
                        first,
                        first.getValue()
                                .aborted(
                                        BulkLoadAbortReason.ABORTED_BY_CALLER,
                                        null,
                                        Long.MAX_VALUE),
                        Arrays.asList(registration(1L), registration(2L)),
                        null,
                        epoch);
        assertThat(aborted.getValue().getState()).isEqualTo(BulkLoadState.ABORTED);
        assertThat(registration(1L).getValue().getBulkLoadId()).isNull();
        assertThat(registration(2L).getValue().getBulkLoadId()).isNull();
        assertThat(registration(3L).getValue().getBulkLoadId())
                .isEqualTo(disjoint.getValue().getBulkLoadId());
    }

    @Test
    void testStaleLeaderObservationCannotCompleteSharedTransaction() throws Exception {
        Versioned<BulkLoadTransaction> begun = begin(1L, 2L);
        List<Versioned<?>> owned = Arrays.asList(registration(1L), registration(2L));
        Versioned<BulkLoadTransaction> decision =
                store.update(
                        begun, begun.getValue().withInput("a").committing(), owned, null, epoch);
        TableBucket bucket = new TableBucket(tableId, tableId + 1L, 0);
        String leaderPath = ZkData.LeaderAndIsrZNode.path(bucket);
        create(leaderPath, ZkData.LeaderAndIsrZNode.encode(new LeaderAndIsr(1, 0)));
        Versioned<LeaderAndIsr> leader = observed(leaderPath, ZkData.LeaderAndIsrZNode::decode);
        ReplicaConvergenceContext round =
                new ReplicaConvergenceContext(
                        assignments(1L, 2L),
                        Collections.singletonMap(bucket, leader),
                        Collections.emptyMap(),
                        0L);
        client.getCuratorClient()
                .setData()
                .forPath(leaderPath, ZkData.LeaderAndIsrZNode.encode(new LeaderAndIsr(2, 1)));
        assertThatThrownBy(
                        () ->
                                store.update(
                                        decision,
                                        decision.getValue().committed(Long.MAX_VALUE),
                                        owned,
                                        round,
                                        epoch))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(
                        store.readOptional(
                                        decision.getPath(),
                                        ZkData.BulkLoadTransactionsZNode::decode)
                                .get()
                                .getValue()
                                .getState())
                .isEqualTo(BulkLoadState.COMMITTING);
        assertThat(registration(1L).getValue().getBulkLoadId()).isNotNull();
        assertThat(registration(2L).getValue().getBulkLoadId()).isNotNull();
    }

    @Test
    void testPartialAdoptionIsIdempotentAndProtectedByDecision() throws Exception {
        Versioned<BulkLoadTransaction> begun = begin(1L, 2L);
        List<Versioned<?>> owned = Arrays.asList(registration(1L), registration(2L));
        Versioned<BulkLoadTransaction> decision =
                store.update(
                        begun, begun.getValue().withInput("a").committing(), owned, null, epoch);
        TableBucket first = new TableBucket(tableId, tableId + 1L, 0);
        create(ZkData.BucketIdZNode.path(first), new byte[0]);
        BucketSnapshot snapshot = new BucketSnapshot(7L, 0L, "file:///snapshot/_METADATA");
        store.adoptBucketMetadata(decision, registration(1L), first, snapshot, epoch);
        store.adoptBucketMetadata(decision, registration(1L), first, snapshot, epoch);
        assertThat(client.listBucketSnapshotIds(first)).containsExactly(7L);
        assertThat(registration(2L).getValue().getBulkLoadId())
                .isEqualTo(decision.getValue().getBulkLoadId());
        assertThatThrownBy(
                        () ->
                                store.adoptBucketMetadata(
                                        begun, registration(1L), first, snapshot, epoch))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("durable commit decision");
    }

    @Test
    void testBeginReservesTerminalMultiBudgetBeforeOwningTargets() throws Exception {
        org.apache.fluss.config.Configuration configuration =
                new org.apache.fluss.config.Configuration();
        configuration.set(org.apache.fluss.config.ConfigOptions.ZOOKEEPER_MAX_BUFFER_SIZE, 4096);
        configuration.setString(
                org.apache.fluss.config.ConfigOptions.REMOTE_DATA_DIR, "file:///warehouse");
        ZooKeeperClient bounded =
                new ZooKeeperClient(
                        new org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener(
                                client.getCuratorClient(), (message, failure) -> {}),
                        configuration);
        BulkLoadTransaction transaction = transaction(1L, 2L);
        assertThatThrownBy(
                        () ->
                                new BulkLoadMetadataStore(bounded)
                                        .createTransactionAndOwn(
                                                Arrays.asList(registration(1L), registration(2L)),
                                                assignments(1L, 2L),
                                                table(),
                                                transaction,
                                                epoch))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("serialized size");
        assertThat(registration(1L).getValue().getBulkLoadId()).isNull();
        assertThat(registration(2L).getValue().getBulkLoadId()).isNull();
        assertThat(
                        client.pathExists(
                                BulkLoadMetadataStore.transactionPath(transaction.getHandle())))
                .isFalse();
    }

    @Test
    void testUnrelatedTargetVersionDoesNotPreventOwnBucketAdoption() throws Exception {
        Versioned<BulkLoadTransaction> begun = begin(1L, 2L);
        Versioned<BulkLoadTransaction> decision =
                store.update(
                        begun,
                        begun.getValue().withInput("a").committing(),
                        Arrays.asList(registration(1L), registration(2L)),
                        null,
                        epoch);
        Versioned<PartitionRegistration> unrelated = registration(2L);
        client.getCuratorClient()
                .setData()
                .forPath(unrelated.getPath(), ZkData.PartitionZNode.encode(unrelated.getValue()));
        TableBucket bucket = new TableBucket(tableId, tableId + 1L, 0);
        create(ZkData.BucketIdZNode.path(bucket), new byte[0]);
        store.adoptBucketMetadata(
                decision,
                registration(1L),
                bucket,
                new BucketSnapshot(7L, 0L, "file:///snapshot/_METADATA"),
                epoch);
        assertThat(client.listBucketSnapshotIds(bucket)).containsExactly(7L);
    }

    @Test
    void testSameIdReplacementSessionCannotSatisfyAnOldRestorationAcknowledgement()
            throws Exception {
        Versioned<BulkLoadTransaction> begun = begin(1L, 2L);
        List<Versioned<?>> registrations = Arrays.asList(registration(1L), registration(2L));
        Versioned<BulkLoadTransaction> decision =
                store.update(
                        begun,
                        begun.getValue().withInput("a").committing(),
                        registrations,
                        null,
                        epoch);
        org.apache.fluss.server.zk.data.TabletServerRegistration server =
                new org.apache.fluss.server.zk.data.TabletServerRegistration(
                        "rack",
                        Collections.singletonList(
                                new org.apache.fluss.cluster.Endpoint(
                                        "localhost", 20001, "INTERNAL")),
                        1L,
                        org.apache.fluss.server.metadata.TabletServerResource.unknown(),
                        org.apache.fluss.server.zk.data.ServerProtocolVersion.CURRENT);
        Versioned<org.apache.fluss.server.zk.data.TabletServerRegistration> acknowledged;
        try (ZooKeeperClient original =
                ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE)) {
            original.getCuratorClient().blockUntilConnected();
            original.registerTabletServer(1, server);
            acknowledged = observed(ZkData.ServerIdZNode.path(1), ZkData.ServerIdZNode::decode);
        }
        org.apache.fluss.testutils.common.CommonTestUtils.waitUntil(
                () -> !client.pathExists(ZkData.ServerIdZNode.path(1)),
                java.time.Duration.ofSeconds(5),
                "old TabletServer session registration expires");
        try (ZooKeeperClient replacement =
                ZOOKEEPER.getCustomExtension().createZooKeeperClient(NOPErrorHandler.INSTANCE)) {
            replacement.getCuratorClient().blockUntilConnected();
            replacement.registerTabletServer(1, server);
            assertThat(
                            observed(ZkData.ServerIdZNode.path(1), ZkData.ServerIdZNode::decode)
                                    .getVersion())
                    .isEqualTo(acknowledged.getVersion());
            ReplicaConvergenceContext round =
                    new ReplicaConvergenceContext(
                            assignments(1L, 2L),
                            Collections.emptyMap(),
                            Collections.singletonMap(1, acknowledged),
                            0L);
            assertThatThrownBy(
                            () ->
                                    store.update(
                                            decision,
                                            decision.getValue().committed(Long.MAX_VALUE),
                                            registrations,
                                            round,
                                            epoch))
                    .isInstanceOf(KeeperException.NoNodeException.class);
            assertThat(registration(1L).getValue().getBulkLoadId())
                    .isEqualTo(decision.getValue().getBulkLoadId());
            assertThat(registration(2L).getValue().getBulkLoadId())
                    .isEqualTo(decision.getValue().getBulkLoadId());
        }
    }

    private Versioned<BulkLoadTransaction> begin(Long... ids) throws Exception {
        List<Versioned<?>> registrations = new ArrayList<>();
        for (long id : ids) {
            registrations.add(registration(id));
        }
        return store.createTransactionAndOwn(
                registrations, assignments(ids), table(), transaction(ids), epoch);
    }

    private BulkLoadTransaction transaction(Long... ids) throws Exception {
        List<BulkLoadTarget> targets = new ArrayList<>();
        for (long id : ids) {
            targets.add(
                    new BulkLoadTarget(
                            PhysicalTablePath.of(path, "p" + id),
                            tableId + id,
                            "file:///warehouse",
                            0,
                            registration(id).getVersion() + 1,
                            new long[] {7L}));
        }
        return new BulkLoadTransaction(
                new BulkLoadHandle(path, tableId, UUID.randomUUID().toString()),
                BulkLoadState.BEGUN,
                "alice",
                "USER",
                targets,
                Long.MAX_VALUE,
                null,
                null,
                null);
    }

    private List<Versioned<? extends TableAssignment>> assignments(Long... ids) throws Exception {
        List<Versioned<? extends TableAssignment>> result = new ArrayList<>();
        for (long id : ids) {
            result.add(
                    observed(
                            ZkData.PartitionIdZNode.path(tableId + id),
                            ZkData.PartitionIdZNode::decode));
        }
        return result;
    }

    private Versioned<PartitionRegistration> registration(long id) throws Exception {
        return observed(ZkData.PartitionZNode.path(path, "p" + id), ZkData.PartitionZNode::decode);
    }

    private Versioned<TableRegistration> table() throws Exception {
        return observed(ZkData.TableZNode.path(path), ZkData.TableZNode::decode);
    }

    private <T> Versioned<T> observed(String path, Function<byte[], T> decoder) throws Exception {
        ZooKeeperClient.DataWithStat data = client.getDataWithStat(path);
        return new Versioned<>(
                decoder.apply(data.getData()),
                path,
                data.getStat().getVersion(),
                data.getStat().getEphemeralOwner());
    }

    private void create(String path, byte[] data) throws Exception {
        client.getCuratorClient().create().creatingParentsIfNeeded().forPath(path, data);
    }
}
