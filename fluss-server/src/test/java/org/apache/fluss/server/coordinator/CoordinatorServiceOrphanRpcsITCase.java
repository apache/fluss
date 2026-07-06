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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.ListKvSnapshotsRequest;
import org.apache.fluss.rpc.messages.ListKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.ListRemoteLogManifestsRequest;
import org.apache.fluss.rpc.messages.ListRemoteLogManifestsResponse;
import org.apache.fluss.rpc.messages.PbKvSnapshot;
import org.apache.fluss.rpc.messages.PbRemoteLogManifestEntry;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandleStore;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.kv.snapshot.SharedKvFileRegistry;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for orphan-cleanup-related coordinator RPCs: ListRemoteLogManifests, ListKvSnapshots. */
class CoordinatorServiceOrphanRpcsITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    private static final TablePath PK_TABLE_PATH = TablePath.of("orphan_rpc_test_db", "pk_table");
    private static final TableDescriptor PK_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .withComment("pk col")
                                    .column("val", DataTypes.STRING())
                                    .withComment("value col")
                                    .primaryKey("id")
                                    .build())
                    .distributedBy(3, "id")
                    .build();

    private static final int MAX_SNAPSHOTS_TO_RETAIN = 2;

    private static long pkTableId;
    @TempDir static Path tempDir;

    @BeforeAll
    static void setupTables() throws Exception {
        pkTableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, PK_TABLE_PATH, PK_TABLE_DESCRIPTOR);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(pkTableId);
    }

    // -------------------------------------------------------------------------
    // ListRemoteLogManifests
    // -------------------------------------------------------------------------

    @Test
    void listManifestsForUnpartitionedTable() throws Exception {
        long tableId = 999_111L;
        ZooKeeperClient zk = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        zk.upsertRemoteLogManifestHandle(
                new TableBucket(tableId, 0),
                new RemoteLogManifestHandle(new FsPath("oss://bucket-0/m.manifest"), 100L));
        zk.upsertRemoteLogManifestHandle(
                new TableBucket(tableId, 1),
                new RemoteLogManifestHandle(new FsPath("oss://bucket-1/m.manifest"), 200L));

        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        ListRemoteLogManifestsRequest request = new ListRemoteLogManifestsRequest();
        request.setTableId(tableId);
        ListRemoteLogManifestsResponse response = gateway.listRemoteLogManifests(request).get();

        assertThat(response.getManifestsList()).hasSize(2);
        assertThat(response.getManifestsList())
                .extracting(PbRemoteLogManifestEntry::getRemoteLogEndOffset)
                .containsExactlyInAnyOrder(100L, 200L);
    }

    @Test
    void listManifestsForUnknownTableReturnsEmpty() throws Exception {
        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        ListRemoteLogManifestsRequest request = new ListRemoteLogManifestsRequest();
        request.setTableId(999_999L);
        ListRemoteLogManifestsResponse response = gateway.listRemoteLogManifests(request).get();

        assertThat(response.getManifestsList()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // ListKvSnapshots
    // -------------------------------------------------------------------------

    @Test
    void listSnapshotsReturnsRetainedSubset() throws Exception {
        TableBucket tb = new TableBucket(pkTableId, 0);
        CompletedSnapshotStore store = createTestStore(snapshot -> false);
        for (long snapId = 1L; snapId <= 4L; snapId++) {
            store.add(createMinimalSnapshot(tb, snapId));
        }
        injectStore(tb, store);

        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        ListKvSnapshotsRequest request = new ListKvSnapshotsRequest();
        request.setTableId(pkTableId);
        ListKvSnapshotsResponse response = gateway.listKvSnapshots(request).get();

        assertThat(response.getTableId()).isEqualTo(pkTableId);
        assertThat(response.hasPartitionId()).isFalse();
        List<PbKvSnapshot> bucket0 =
                response.getActiveSnapshotsList().stream()
                        .filter(s -> s.getBucketId() == 0)
                        .collect(Collectors.toList());
        assertThat(bucket0)
                .extracting(PbKvSnapshot::getSnapshotId)
                .containsExactlyInAnyOrder(3L, 4L);
    }

    @Test
    void listSnapshotsReturnsStillInUseBeyondRetention() throws Exception {
        TableBucket tb = new TableBucket(pkTableId, 1);
        CompletedSnapshotStore store = createTestStore(snapshot -> snapshot.getSnapshotID() == 1L);
        for (long snapId = 1L; snapId <= 4L; snapId++) {
            store.add(createMinimalSnapshot(tb, snapId));
        }
        injectStore(tb, store);

        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        ListKvSnapshotsRequest request = new ListKvSnapshotsRequest();
        request.setTableId(pkTableId);
        ListKvSnapshotsResponse response = gateway.listKvSnapshots(request).get();

        List<PbKvSnapshot> bucket1Snapshots =
                response.getActiveSnapshotsList().stream()
                        .filter(s -> s.getBucketId() == 1)
                        .collect(Collectors.toList());
        // RETAINED = {3, 4}, STILL_IN_USE = {1}
        assertThat(bucket1Snapshots)
                .extracting(PbKvSnapshot::getSnapshotId)
                .containsExactlyInAnyOrder(1L, 3L, 4L);
    }

    @Test
    void listSnapshotsForUnknownTableFails() {
        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        ListKvSnapshotsRequest request = new ListKvSnapshotsRequest();
        request.setTableId(999_888L);

        assertThatThrownBy(() -> gateway.listKvSnapshots(request).get())
                .isInstanceOf(ExecutionException.class);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private CompletedSnapshotStore createTestStore(
            CompletedSnapshotStore.SnapshotInUseChecker inUseChecker) {
        return new CompletedSnapshotStore(
                MAX_SNAPSHOTS_TO_RETAIN,
                new SharedKvFileRegistry(),
                Collections.emptyList(),
                new NoOpSnapshotHandleStore(),
                directExecutor(),
                inUseChecker);
    }

    private void injectStore(TableBucket tb, CompletedSnapshotStore store) {
        CompletedSnapshotStoreManager storeManager =
                FLUSS_CLUSTER_EXTENSION
                        .getCoordinatorServer()
                        .getCoordinatorEventProcessor()
                        .completedSnapshotStoreManager();
        storeManager.getBucketCompletedSnapshotStores().put(tb, store);
    }

    private CompletedSnapshot createMinimalSnapshot(TableBucket tb, long snapshotId) {
        Path snapDir = tempDir.resolve("snap-" + tb.getBucket() + "-" + snapshotId);
        snapDir.toFile().mkdirs();
        return new CompletedSnapshot(
                tb,
                snapshotId,
                new FsPath(snapDir.resolve("_metadata").toUri().toString()),
                new KvSnapshotHandle(Collections.emptyList(), Collections.emptyList(), 0L),
                snapshotId,
                null,
                null);
    }

    /** No-op implementation that avoids ZK writes during test snapshot population. */
    private static class NoOpSnapshotHandleStore implements CompletedSnapshotHandleStore {

        @Override
        public void add(
                TableBucket tableBucket,
                long snapshotId,
                CompletedSnapshotHandle completedSnapshotHandle) {}

        @Override
        public void remove(TableBucket tableBucket, long snapshotId) {}

        @Override
        public Optional<CompletedSnapshotHandle> get(TableBucket tableBucket, long snapshotId) {
            return Optional.empty();
        }

        @Override
        public List<CompletedSnapshotHandle> getAllCompletedSnapshotHandles(
                TableBucket tableBucket) {
            return Collections.emptyList();
        }

        @Override
        public Optional<CompletedSnapshotHandle> getLatestCompletedSnapshotHandle(
                TableBucket tableBucket) {
            return Optional.empty();
        }
    }
}
