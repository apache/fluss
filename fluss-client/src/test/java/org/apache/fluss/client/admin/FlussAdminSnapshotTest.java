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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.utils.TestingRpcClient;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.ListKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests snapshot selection against live SYNC visibility and retained WAL. */
class FlussAdminSnapshotTest {

    private static final TablePath TABLE = TablePath.of("db", "snapshot_table");
    private static final long TABLE_ID = 42L;
    private static final long PARTITION_ID = 7L;

    @ParameterizedTest
    @CsvSource({"100,0,2", "90,0,1", "70,0,0", "100,90,2", "100,100,2", "90,81,-1", "70,1,-1"})
    void testSelectsLatestVisibleSnapshotWithCompleteLogSuffix(
            long visibleOffset, long startOffset, int expectedSnapshot) throws Exception {
        try (FlussAdmin admin = newAdmin(candidates(), visibleOffset, startOffset, () -> {})) {
            if (expectedSnapshot < 0) {
                assertThatThrownBy(
                                () -> admin.getLatestKvSnapshots(TABLE).get(10, TimeUnit.SECONDS))
                        .rootCause()
                        .hasMessageContaining("No readable KV snapshot")
                        .hasMessageContaining("kv.snapshot.num-retained");
            } else {
                KvSnapshots selected = admin.getLatestKvSnapshots(TABLE).get(10, TimeUnit.SECONDS);
                assertThat(selected.getTableId()).isEqualTo(TABLE_ID);
                assertThat(selected.getPartitionId()).isEqualTo(PARTITION_ID);
                if (expectedSnapshot == 0) {
                    assertThat(selected.getSnapshotId(0)).isEmpty();
                    assertThat(selected.getLogOffset(0)).isEmpty();
                } else {
                    assertThat(selected.getSnapshotId(0)).hasValue(expectedSnapshot);
                    assertThat(selected.getLogOffset(0)).hasValue(60L + 20L * expectedSnapshot);
                }
            }
        }
    }

    @Test
    void testSelectsSnapshotsIndependentlyForEachBucket() throws Exception {
        GetLatestKvSnapshotsResponse latest = candidates();
        latest.getLatestSnapshotsList().get(0).setSnapshotId(5L).setLogOffset(80L);
        latest.addLatestSnapshot().setBucketId(1).setSnapshotId(3L).setLogOffset(120L);
        latest.addLatestSnapshot().setBucketId(2).setSnapshotId(1L).setLogOffset(80L);

        ListKvSnapshotsResponse retained = new ListKvSnapshotsResponse().setTableId(TABLE_ID);
        retained.addActiveSnapshot().setBucketId(1).setSnapshotId(2L).setLogOffset(100L);
        retained.addActiveSnapshot().setBucketId(0).setSnapshotId(4L).setLogOffset(60L);
        retained.addActiveSnapshot().setBucketId(2).setSnapshotId(1L).setLogOffset(80L);
        retained.addActiveSnapshot().setBucketId(1).setSnapshotId(1L).setLogOffset(80L);
        retained.addActiveSnapshot().setBucketId(0).setSnapshotId(5L).setLogOffset(80L);
        retained.addActiveSnapshot().setBucketId(1).setSnapshotId(3L).setLogOffset(120L);

        Map<Integer, Long> visible = new HashMap<>();
        visible.put(0, 100L);
        visible.put(1, 90L);
        visible.put(2, 70L);
        Map<Integer, Long> start = new HashMap<>();
        start.put(0, 60L);
        start.put(1, 80L);
        start.put(2, 0L);

        try (FlussAdmin admin = newAdmin(latest, retained, visible, start, () -> {}, () -> {})) {
            KvSnapshots selected = admin.getLatestKvSnapshots(TABLE).get(10, TimeUnit.SECONDS);
            assertThat(selected.getTableId()).isEqualTo(TABLE_ID);
            assertThat(selected.getPartitionId()).isEqualTo(PARTITION_ID);
            assertThat(selected.getSnapshotId(0)).hasValue(5L);
            assertThat(selected.getLogOffset(0)).hasValue(80L);
            assertThat(selected.getSnapshotId(1)).hasValue(1L);
            assertThat(selected.getLogOffset(1)).hasValue(80L);
            assertThat(selected.getSnapshotId(2)).isEmpty();
            assertThat(selected.getLogOffset(2)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 5L})
    void testNoSnapshotsRequiresCompleteWal(long startOffset) throws Exception {
        GetLatestKvSnapshotsResponse response =
                new GetLatestKvSnapshotsResponse().setTableId(TABLE_ID).setHasSyncIndexes(true);
        response.addLatestSnapshot().setBucketId(0);
        try (FlussAdmin admin = newAdmin(response, 100L, startOffset, () -> {})) {
            if (startOffset == 0L) {
                assertThat(
                                admin.getLatestKvSnapshots(TABLE)
                                        .get(10, TimeUnit.SECONDS)
                                        .getSnapshotId(0))
                        .isEmpty();
            } else {
                assertThatThrownBy(
                                () -> admin.getLatestKvSnapshots(TABLE).get(10, TimeUnit.SECONDS))
                        .rootCause()
                        .hasMessageContaining("complete changelog history");
            }
        }
    }

    @Test
    void testTablesWithoutSyncIndexesKeepLatestSnapshot() throws Exception {
        GetLatestKvSnapshotsResponse response = candidates();
        response.clearHasSyncIndexes();
        try (FlussAdmin admin =
                newAdmin(
                        response,
                        0L,
                        1L,
                        () -> {
                            throw new AssertionError(
                                    "Ordinary and ASYNC-only tables need no offset query.");
                        })) {
            assertThat(admin.getLatestKvSnapshots(TABLE).get(10, TimeUnit.SECONDS).getSnapshotId(0))
                    .hasValue(3L);
        }
    }

    @Test
    void testBlockingMetadataPreparationDoesNotBlockRpcCallback() throws Exception {
        CountDownLatch preparing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService callback = Executors.newSingleThreadExecutor();
        try (FlussAdmin admin =
                newAdmin(
                        candidates(),
                        100L,
                        0L,
                        () -> {
                            preparing.countDown();
                            try {
                                release.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        })) {
            CompletableFuture<KvSnapshots> result =
                    callback.submit(() -> admin.getLatestKvSnapshots(TABLE))
                            .get(10, TimeUnit.SECONDS);
            assertThat(preparing.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(result).isNotDone();
            release.countDown();
            assertThat(result.get(10, TimeUnit.SECONDS).getSnapshotId(0)).hasValue(2L);
        } finally {
            release.countDown();
            callback.shutdownNow();
        }
    }

    @Test
    void testReadableLatestSnapshotDoesNotEnumerateHistory() throws Exception {
        try (FlussAdmin admin =
                newAdmin(
                        candidates(),
                        120L,
                        0L,
                        () -> {},
                        () -> {
                            throw new AssertionError(
                                    "A readable latest snapshot needs no history query.");
                        })) {
            assertThat(admin.getLatestKvSnapshots(TABLE).get(10, TimeUnit.SECONDS).getSnapshotId(0))
                    .hasValue(3L);
        }
    }

    @Test
    void testCloseCompletesQueuedSnapshotQueries() throws Exception {
        CountDownLatch preparing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (FlussAdmin admin =
                newAdmin(
                        candidates(),
                        100L,
                        0L,
                        () -> {
                            preparing.countDown();
                            try {
                                release.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        })) {
            CompletableFuture<KvSnapshots> running = admin.getLatestKvSnapshots(TABLE);
            assertThat(preparing.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<KvSnapshots> queued = admin.getLatestKvSnapshots(TABLE);
            admin.close();
            assertThatThrownBy(() -> queued.get(1, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> running.get(1, TimeUnit.SECONDS))
                    .rootCause()
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            release.countDown();
        }
    }

    private static GetLatestKvSnapshotsResponse candidates() {
        GetLatestKvSnapshotsResponse response =
                new GetLatestKvSnapshotsResponse()
                        .setTableId(TABLE_ID)
                        .setPartitionId(PARTITION_ID)
                        .setHasSyncIndexes(true);
        response.addLatestSnapshot().setBucketId(0).setSnapshotId(3L).setLogOffset(120L);
        return response;
    }

    private static FlussAdmin newAdmin(
            GetLatestKvSnapshotsResponse snapshots,
            long visibleOffset,
            long startOffset,
            Runnable prepare) {
        return newAdmin(snapshots, visibleOffset, startOffset, prepare, () -> {});
    }

    private static FlussAdmin newAdmin(
            GetLatestKvSnapshotsResponse snapshots,
            long visibleOffset,
            long startOffset,
            Runnable prepare,
            Runnable readHistory) {
        ListKvSnapshotsResponse retained = new ListKvSnapshotsResponse().setTableId(TABLE_ID);
        if (snapshots.getLatestSnapshotsList().get(0).hasSnapshotId()) {
            for (int id = 3; id >= 1; id--) {
                retained.addActiveSnapshot()
                        .setBucketId(0)
                        .setSnapshotId(id)
                        .setLogOffset(60L + 20L * id);
            }
        }
        return newAdmin(
                snapshots,
                retained,
                Collections.singletonMap(0, visibleOffset),
                Collections.singletonMap(0, startOffset),
                prepare,
                readHistory);
    }

    private static FlussAdmin newAdmin(
            GetLatestKvSnapshotsResponse snapshots,
            ListKvSnapshotsResponse retained,
            Map<Integer, Long> visibleOffsets,
            Map<Integer, Long> startOffsets,
            Runnable prepare,
            Runnable readHistory) {
        ServerNode node = new ServerNode(1, "tablet", 9123, ServerType.TABLET_SERVER);
        TestingRpcClient client =
                new TestingRpcClient(
                        (server, api) -> {
                            if (api == ApiKeys.LIST_KV_SNAPSHOTS) {
                                readHistory.run();
                                return CompletableFuture.completedFuture(retained);
                            }
                            assertThat(api).isEqualTo(ApiKeys.GET_LATEST_KV_SNAPSHOTS);
                            return CompletableFuture.completedFuture(snapshots);
                        });
        MetadataUpdater updater =
                new MetadataUpdater(client, new Configuration(), Cluster.empty()) {
                    @Override
                    public ServerNode getCoordinatorServer() {
                        return new ServerNode(0, "coordinator", 9124, ServerType.COORDINATOR);
                    }

                    @Override
                    public ServerNode getRandomTabletServer() {
                        return node;
                    }

                    @Override
                    public int leaderFor(TablePath tablePath, TableBucket bucket) {
                        prepare.run();
                        assertThat(bucket.getTableId()).isEqualTo(TABLE_ID);
                        assertThat(bucket.getPartitionId())
                                .isEqualTo(snapshots.hasPartitionId() ? PARTITION_ID : null);
                        return node.id();
                    }

                    @Override
                    public TabletServerGateway newTabletServerClientForNode(int serverId) {
                        return new TestingTabletGatewayService() {
                            @Override
                            public CompletableFuture<ListOffsetsResponse> listOffsets(
                                    ListOffsetsRequest request) {
                                assertThat(request.getFollowerServerId()).isEqualTo(-1);
                                Map<Integer, Long> offsets =
                                        request.getOffsetType() == OffsetSpec.LIST_LATEST_OFFSET
                                                ? visibleOffsets
                                                : startOffsets;
                                ListOffsetsResponse response = new ListOffsetsResponse();
                                for (int bucket : request.getBucketIds()) {
                                    response.addBucketsResp()
                                            .setBucketId(bucket)
                                            .setOffset(offsets.get(bucket));
                                }
                                return CompletableFuture.completedFuture(response);
                            }
                        };
                    }
                };
        return new FlussAdmin(client, updater);
    }
}
