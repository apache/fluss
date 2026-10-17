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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.KvTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1_KEY_TYPE;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatchWithWriterId;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests KV recovery to the remote durable boundary after losing every local replica. */
class KvFullDiskLossITCase {

    @ParameterizedTest
    @CsvSource({"true,false", "true,true", "false,false"})
    void testRecoveryAfterAllTabletServerDisksAreLost(
            boolean createSnapshot, boolean snapshotAheadOfRemoteLog) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofMillis(100));
        conf.set(ConfigOptions.LOG_RETENTION_ROLL_ACTIVE_SEGMENT_ENABLED, false);
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        conf.setInt(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES, 0);

        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(3)
                        .setClusterConf(conf)
                        .build();
        try {
            cluster.start();
            TablePath tablePath = TablePath.of("test_db", "full_disk_loss");
            long tableId =
                    createTable(
                            cluster,
                            tablePath,
                            TableDescriptor.builder()
                                    .schema(DATA1_SCHEMA_PK)
                                    .distributedBy(1, "a")
                                    .build());
            TableBucket tableBucket = new TableBucket(tableId, 0);
            cluster.waitUntilAllReplicaReady(tableBucket);
            Replica leader = cluster.waitAndGetLeaderReplica(tableBucket);
            TabletServerGateway gateway =
                    cluster.newTabletServerClientForNode(leader.getLeaderId());

            putRecords(
                    gateway,
                    tableBucket,
                    genKvRecordBatch(new Object[] {1, "snapshot"}, new Object[] {9, "delete-me"}));
            CompletedSnapshot snapshot =
                    createSnapshot ? cluster.triggerAndWaitSnapshot(tableBucket) : null;

            KvRecordBatch durableBatch =
                    genKvRecordBatchWithWriterId(
                            Arrays.asList(
                                    Tuple2.of(new Object[] {1}, new Object[] {1, "updated"}),
                                    Tuple2.of(new Object[] {2}, new Object[] {2, "remote"}),
                                    Tuple2.of(new Object[] {9}, null)),
                            DATA1_KEY_TYPE,
                            DATA1_ROW_TYPE,
                            123L,
                            0);
            putRecords(gateway, tableBucket, durableBatch);
            long remoteEndOffset = leader.getLocalLogEndOffset();
            // Only completed segments are uploaded. Explicitly roll the durable prefix.
            leader.getLogTablet().roll(Optional.empty());
            waitUntil(
                    () -> leader.getLogTablet().canFetchFromRemoteLog(remoteEndOffset - 1),
                    Duration.ofMinutes(1),
                    "The durable prefix must be committed to remote storage");

            if (snapshotAheadOfRemoteLog) {
                putRecords(
                        gateway, tableBucket, genKvRecordBatch(new Object[] {3, "snapshot-only"}));
                snapshot = cluster.triggerAndWaitSnapshot(tableBucket);
                assertThat(snapshot.getLogOffset()).isGreaterThan(remoteEndOffset);
            } else if (snapshot != null) {
                assertThat(snapshot.getLogOffset()).isPositive().isLessThan(remoteEndOffset);
            }
            long recoveredOffset =
                    Math.max(remoteEndOffset, snapshot == null ? 0 : snapshot.getLogOffset());

            // Acknowledged inserts, updates and deletes in the active segment are deliberately
            // left outside both the snapshot and the committed remote log.
            putRecords(
                    gateway,
                    tableBucket,
                    genKvRecordBatch(
                            Arrays.asList(
                                    Tuple2.of(new Object[] {1}, new Object[] {1, "lost-update"}),
                                    Tuple2.of(new Object[] {2}, null),
                                    Tuple2.of(new Object[] {4}, new Object[] {4, "lost-insert"}))));
            assertThat(leader.getLogHighWatermark()).isGreaterThan(recoveredOffset);
            assertThat(leader.getLogTablet().canFetchFromRemoteLog(remoteEndOffset)).isFalse();

            ZooKeeperClient zkClient = cluster.getZooKeeperClient();
            List<Integer> replicas =
                    zkClient.getTableAssignment(tableId).get().getBucketAssignment(0).getReplicas();
            assertThat(replicas).hasSize(3);
            List<File> dataDirs = new ArrayList<>();
            for (int serverId : replicas) {
                dataDirs.add(
                        cluster.getTabletServerById(serverId)
                                .getReplicaManager()
                                .getReplicaOrException(tableBucket)
                                .getLogTablet()
                                .getDataDir());
            }

            // Freeze elections before stopping servers. Closing releases native resources; deleting
            // every data directory removes WAL, KV, checkpoints and clean-shutdown markers.
            // This models the startup state after total disk loss, rather than a process kill.
            cluster.stopCoordinatorServer();
            for (int serverId : replicas) {
                cluster.stopTabletServer(serverId);
            }
            for (File dataDir : dataDirs) {
                FileUtils.deleteDirectory(dataDir);
                assertThat(dataDir).doesNotExist();
            }

            ZooKeeperCompletedSnapshotHandleStore snapshots =
                    new ZooKeeperCompletedSnapshotHandleStore(zkClient);
            if (snapshot != null) {
                CompletedSnapshot retained =
                        snapshots
                                .getLatestCompletedSnapshotHandle(tableBucket)
                                .get()
                                .retrieveCompleteSnapshot();
                List<Tuple2<byte[], byte[]>> snapshotValues =
                        snapshotAheadOfRemoteLog
                                ? getKeyValuePairs(
                                        genKvRecords(
                                                new Object[] {1, "updated"},
                                                new Object[] {2, "remote"},
                                                new Object[] {3, "snapshot-only"}))
                                : getKeyValuePairs(
                                        genKvRecords(
                                                new Object[] {1, "snapshot"},
                                                new Object[] {9, "delete-me"}));
                KvTestUtils.checkSnapshot(retained, snapshotValues, snapshot.getLogOffset());
            } else {
                assertThat(snapshots.getLatestCompletedSnapshotHandle(tableBucket)).isEmpty();
            }
            assertThat(zkClient.getRemoteLogManifestHandle(tableBucket)).isPresent();

            for (int serverId : replicas) {
                cluster.startTabletServer(serverId);
            }
            cluster.startCoordinatorServer();
            cluster.waitUntilAllReplicaReady(tableBucket);
            Replica restored = cluster.waitAndGetLeaderReplica(tableBucket);
            gateway = cluster.newTabletServerClientForNode(restored.getLeaderId());
            assertThat(restored.getLocalLogStartOffset()).isEqualTo(recoveredOffset);
            assertThat(restored.getLocalLogEndOffset()).isEqualTo(recoveredOffset);
            assertThat(restored.getLogHighWatermark()).isEqualTo(recoveredOffset);
            assertThat(restored.getLogTablet().getLeaderEndOffsetSnapshot())
                    .isEqualTo(recoveredOffset);
            assertThat(restored.getRowCount()).isEqualTo(snapshotAheadOfRemoteLog ? 3 : 2);
            assertValues(
                    gateway, tableBucket, new Object[] {1, "updated"}, new Object[] {2, "remote"});
            if (snapshotAheadOfRemoteLog) {
                assertValues(gateway, tableBucket, new Object[] {3, "snapshot-only"});
            }
            assertMissing(gateway, tableBucket, 4);
            assertMissing(gateway, tableBucket, 9);

            if (!snapshotAheadOfRemoteLog) {
                // The remote writer snapshot must prevent a retry of a surviving batch from
                // generating another changelog after the disk loss.
                putRecords(gateway, tableBucket, durableBatch);
                assertThat(restored.getLocalLogEndOffset()).isEqualTo(recoveredOffset);
            }
            putRecords(gateway, tableBucket, genKvRecordBatch(new Object[] {5, "after-recovery"}));
            assertThat(restored.getLocalLogEndOffset()).isEqualTo(recoveredOffset + 1);
            assertThat(restored.getRowCount()).isEqualTo(snapshotAheadOfRemoteLog ? 4 : 3);
            assertValues(gateway, tableBucket, new Object[] {5, "after-recovery"});
            waitUntil(
                    () -> {
                        if (!zkClient.getLeaderAndIsr(tableBucket)
                                .get()
                                .isr()
                                .containsAll(replicas)) {
                            return false;
                        }
                        for (int serverId : replicas) {
                            Replica replica =
                                    cluster.getTabletServerById(serverId)
                                            .getReplicaManager()
                                            .getReplicaOrException(tableBucket);
                            if (replica.getLocalLogEndOffset() != recoveredOffset + 1) {
                                return false;
                            }
                        }
                        return true;
                    },
                    Duration.ofMinutes(1),
                    "All replicas must catch up after disk recovery");

            // Tiering must continue even if the recovered snapshot was ahead of the old remote WAL.
            restored.getLogTablet().roll(Optional.empty());
            waitUntil(
                    () -> restored.getLogTablet().canFetchFromRemoteLog(recoveredOffset),
                    Duration.ofMinutes(1),
                    "New writes after disk recovery must still reach remote storage");
            CompletedSnapshot newSnapshot = cluster.triggerAndWaitSnapshot(tableBucket);
            assertThat(newSnapshot.getLogOffset()).isEqualTo(recoveredOffset + 1);
        } finally {
            cluster.close();
        }
    }

    private static void assertValues(
            TabletServerGateway gateway, TableBucket tableBucket, Object[]... rows)
            throws Exception {
        for (Tuple2<byte[], byte[]> entry : getKeyValuePairs(genKvRecords(rows))) {
            assertLookupResponse(
                    gateway.lookup(
                                    newLookupRequest(
                                            tableBucket.getTableId(),
                                            tableBucket.getBucket(),
                                            entry.f0))
                            .get(),
                    entry.f1);
        }
    }

    private static void assertMissing(TabletServerGateway gateway, TableBucket tableBucket, int key)
            throws Exception {
        byte[] keyBytes = getKeyValuePairs(genKvRecords(new Object[] {key, "unused"})).get(0).f0;
        assertLookupResponse(
                gateway.lookup(
                                newLookupRequest(
                                        tableBucket.getTableId(),
                                        tableBucket.getBucket(),
                                        keyBytes))
                        .get(),
                null);
    }

    private static void putRecords(
            TabletServerGateway gateway, TableBucket tableBucket, KvRecordBatch records)
            throws Exception {
        PutKvResponse response =
                gateway.putKv(
                                newPutKvRequest(
                                        tableBucket.getTableId(),
                                        tableBucket.getBucket(),
                                        -1,
                                        records))
                        .get();
        assertThat(response.getBucketsRespsList()).hasSize(1);
        assertThat(response.getBucketsRespAt(0).hasErrorCode())
                .as("PutKv response: %s", response.getBucketsRespAt(0))
                .isFalse();
    }
}
