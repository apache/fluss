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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.ListKvSnapshotsRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies snapshot discovery while the main WAL commits ahead of its SYNC index. */
class SyncIndexSnapshotITCase {

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .build();

    @Test
    void testSelectsOlderSnapshotAndFallsBackToCompleteWal() throws Exception {
        TablePath path = TablePath.of("snapshot_visibility", "main_table");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            TableBucket bucket = createIndexedBucket(admin, path);
            long tableId = bucket.getTableId();
            Replica replica = CLUSTER.waitAndGetLeaderReplica(bucket);
            CLUSTER.newTabletServerClientForNode(CLUSTER.waitAndGetLeader(bucket))
                    .putKv(
                            newPutKvRequest(
                                    tableId, 0, 1, genKvRecordBatch(new Object[] {0, "baseline"})))
                    .get(30, TimeUnit.SECONDS);
            CompletedSnapshot older = CLUSTER.triggerAndWaitSnapshot(bucket);
            ListKvSnapshotsRequest snapshotIds = new ListKvSnapshotsRequest().setTableId(tableId);
            assertThat(
                            CLUSTER.newCoordinatorClient()
                                    .listKvSnapshots(snapshotIds)
                                    .get()
                                    .getActiveSnapshotsList())
                    .anySatisfy(
                            snapshot -> {
                                assertThat(snapshot.getSnapshotId())
                                        .isEqualTo(older.getSnapshotID());
                                assertThat(snapshot.hasLogOffset()).isFalse();
                            });
            int leader = CLUSTER.waitAndGetLeader(bucket);
            CLUSTER.getTabletServerById(leader)
                    .getReplicaManager()
                    .getIndexReplicationManager()
                    .close();

            CompletableFuture<PutKvResponse> write =
                    CLUSTER.newTabletServerClientForNode(leader)
                            .putKv(
                                    newPutKvRequest(
                                            tableId,
                                            0,
                                            1,
                                            genKvRecordBatch(new Object[] {1, "Alice"})));
            try {
                waitUntil(
                        () -> replica.getLogTablet().getHighWatermark() > older.getLogOffset(),
                        Duration.ofSeconds(30),
                        "Main WAL should commit while SYNC replication is paused.");
                CompletedSnapshot newer = CLUSTER.triggerAndWaitSnapshot(bucket);
                assertThat(newer.getLogOffset()).isGreaterThan(replica.getClientLogHighWatermark());

                KvSnapshots selected = admin.getLatestKvSnapshots(path).get(10, TimeUnit.SECONDS);
                assertThat(selected.getSnapshotId(0)).hasValue(older.getSnapshotID());
                assertThat(selected.getLogOffset(0)).hasValue(older.getLogOffset());

                CLUSTER.getZooKeeperClient()
                        .deleteTableBucketSnapshot(bucket, older.getSnapshotID());
                KvSnapshots fallback = admin.getLatestKvSnapshots(path).get(10, TimeUnit.SECONDS);
                assertThat(fallback.getSnapshotId(0)).isEmpty();
                assertThat(fallback.getLogOffset(0)).hasValue(0L);
            } finally {
                write.cancel(false);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDiscoveryUsesCommittedSyncProgress(boolean legacy) throws Exception {
        TablePath path = TablePath.of("snapshot_visibility", "stored_progress_" + legacy);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            TableBucket bucket = createIndexedBucket(admin, path);
            // Each committed snapshot advances the WAL by 20. The third snapshot must skip the
            // second one, and the final snapshot is itself readable. The live source stays empty.
            long[] syncProgress = {70, 80, 90, 100, 160};
            int[] expectedSnapshot = {0, 1, 1, 2, 5};
            for (int id = 1; id <= syncProgress.length; id++) {
                long progress = syncProgress[id - 1];
                long logOffset = 60 + 20L * id;
                if (legacy) {
                    registerLegacySnapshot(bucket, id, logOffset, progress);
                } else {
                    CLUSTER.getZooKeeperClient()
                            .registerTableBucketSnapshot(
                                    bucket, new BucketSnapshot(id, logOffset, "unused", progress));
                }
                KvSnapshots snapshots = admin.getLatestKvSnapshots(path).get(10, TimeUnit.SECONDS);
                int expected = expectedSnapshot[id - 1];
                if (expected == 0) {
                    assertThat(snapshots.getSnapshotId(0)).isEmpty();
                    assertThat(snapshots.getLogOffset(0)).hasValue(0);
                } else {
                    assertThat(snapshots.getSnapshotId(0))
                            .as("published snapshot %d", id)
                            .hasValue(expected);
                    assertThat(snapshots.getLogOffset(0)).hasValue(60 + 20L * expected);
                }
            }
        }
    }

    private static TableBucket createIndexedBucket(Admin admin, TablePath path) throws Exception {
        admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.INT())
                                                .column("name", DataTypes.STRING())
                                                .primaryKey("id")
                                                .index("idx_name", "name")
                                                .build())
                                .distributedBy(1, "id")
                                .build(),
                        false)
                .get();
        long tableId = admin.getTableInfo(path).get().getTableId();
        return new TableBucket(tableId, 0);
    }

    private void registerLegacySnapshot(
            TableBucket bucket, long snapshotId, long logOffset, long indexProgress)
            throws Exception {
        FsPath location =
                new FsPath(
                        CLUSTER.getRemoteDataDir()
                                + "/legacy-snapshots/"
                                + bucket.getTableId()
                                + "/"
                                + snapshotId);
        CompletedSnapshot snapshot =
                new CompletedSnapshot(
                        bucket,
                        snapshotId,
                        location,
                        KvSnapshotHandle.create(
                                Collections.emptyList(), Collections.emptyList(), 0L),
                        logOffset,
                        null,
                        indexProgress,
                        null,
                        null);
        FsPath metadataPath = snapshot.getMetadataFilePath();
        try (FSDataOutputStream out =
                metadataPath.getFileSystem().create(metadataPath, FileSystem.WriteMode.OVERWRITE)) {
            out.write(CompletedSnapshotJsonSerde.toJson(snapshot));
        }
        CLUSTER.getZooKeeperClient()
                .registerTableBucketSnapshot(
                        bucket,
                        new BucketSnapshot(
                                snapshotId, snapshot.getLogOffset(), metadataPath.toString()));
    }

    private static Configuration clusterConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        return conf;
    }
}
