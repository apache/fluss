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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.DataTestUtils.toKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case for recovering KV state from remote log when local log segments are insufficient.
 *
 * <p>This tests the scenario where a follower that has fallen behind becomes the new leader and
 * must fetch log data from remote storage to recover the KV state, because the local log does not
 * cover the offset range needed by the latest KV snapshot.
 *
 * <p>Test flow:
 *
 * <ol>
 *   <li>Stop both followers so they fall behind the leader.
 *   <li>Phase 1: Write first batch of data → wait for tiering → manually trigger snapshot (S1).
 *   <li>Phase 2: Write more data → wait for Phase 2 data to be tiered beyond S1's offset.
 *   <li>Restart follower → it goes through {@code processFetchResultFromRemoteStorage} path,
 *       setting {@code localLogStartOffset = remoteLogEndOffset >> snapshotLogOffset}.
 *   <li>Assert: {@code snapshotLogOffset < followerLocalLogStartOffset} (remote log recovery
 *       condition).
 *   <li>Promote follower to leader → new leader downloads S1 → recovers gap from remote log.
 *   <li>Verify all data via lookup.
 * </ol>
 */
class KvRecoverFromRemoteLogITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    @Test
    void testKvRecoverFromRemoteLogAfterLeaderTransfer() throws Exception {
        // Step 1: Create a PK table with 1 bucket and replication factor 3.
        TablePath tablePath = TablePath.of("test_db", "test_kv_recover_from_remote");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(1, "a").build();

        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tableBucket = new TableBucket(tableId, 0);

        // Wait until all replicas are ready.
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tableBucket);
        int originalLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);

        // Get the replicas assignment for later use.
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        Optional<TableAssignment> tableAssignmentOpt = zkClient.getTableAssignment(tableId);
        assertThat(tableAssignmentOpt).isPresent();
        List<Integer> replicas = tableAssignmentOpt.get().getBucketAssignment(0).getReplicas();

        // Pick a follower to become new leader later.
        int followerToPromote = -1;
        int otherFollower = -1;
        for (int replica : replicas) {
            if (replica != originalLeader) {
                if (followerToPromote == -1) {
                    followerToPromote = replica;
                } else {
                    otherFollower = replica;
                }
            }
        }
        assertThat(followerToPromote).isNotEqualTo(-1);
        assertThat(otherFollower).isNotEqualTo(-1);

        // Step 2: Stop both followers so they fall behind the leader.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(followerToPromote);
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(otherFollower);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(tableBucket, followerToPromote);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(tableBucket, otherFollower);

        // =====================================================================
        // Phase 1: Write first batch → wait for tiering → manually trigger snapshot
        // =====================================================================
        List<KvRecord> allRecords = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            List<KvRecord> batch = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                batch.addAll(genKvRecords(new Object[] {i, "value_" + i}));
            }
            allRecords.addAll(batch);
            putRecordBatch(tableBucket, originalLeader, toKvRecordBatch(batch)).join();
        }

        // Wait for Phase 1 log segments to be copied to remote storage.
        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(tableBucket);

        // Manually trigger a KV snapshot (auto-snapshot is disabled with 1-hour interval).
        // This snapshot (S1) captures Phase 1 data only.
        CompletedSnapshot snapshot = FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tableBucket);
        assertThat(snapshot).isNotNull();
        long snapshotLogOffset = snapshot.getLogOffset();

        // =====================================================================
        // Phase 2: Write more data → wait for tiering beyond snapshot offset
        // =====================================================================
        for (int i = 50; i < 100; i++) {
            List<KvRecord> batch = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                batch.addAll(genKvRecords(new Object[] {i, "value_" + i}));
            }
            allRecords.addAll(batch);
            putRecordBatch(tableBucket, originalLeader, toKvRecordBatch(batch)).join();
        }

        // Wait until the remote log covers beyond the snapshot offset.
        // This ensures that when the follower restarts and goes through the
        // processFetchResultFromRemoteStorage path, its localLogStartOffset
        // will be set to remoteLogEndOffset which is > snapshotLogOffset.
        Replica leaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
        LogTablet leaderLogTablet = leaderReplica.getLogTablet();
        waitUntil(
                () -> leaderLogTablet.canFetchFromRemoteLog(snapshotLogOffset),
                Duration.ofMinutes(2),
                "Fail to wait for remote log to cover beyond snapshot offset " + snapshotLogOffset);

        // =====================================================================
        // Step 3: Restart follower → catches up via remote fetch path
        // =====================================================================
        // When the follower restarts, its fetch offset (0) is out of range on the leader.
        // The leader detects canFetchFromRemoteLog(0) == true and returns RemoteLogFetchInfo.
        // The follower calls processFetchResultFromRemoteStorage which sets
        // localLogStartOffset = remoteLogEndOffset (>> snapshotLogOffset).
        FLUSS_CLUSTER_EXTENSION.startTabletServer(followerToPromote);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaExpandToIsr(tableBucket, followerToPromote);

        // =====================================================================
        // Step 4: Assert the remote log recovery precondition
        // =====================================================================
        // Verify: snapshotLogOffset < followerLocalLogStartOffset
        // This is the exact condition checked in Replica.recoverKvTablet():
        //   if (startRecoverLogOffset < logTablet.localLogStartOffset()) { ... }
        // When the follower becomes leader and downloads S1 (logOffset = snapshotLogOffset),
        // it will need to fetch remote log to fill the gap from snapshotLogOffset to
        // localLogStartOffset.
        Replica followerReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tableBucket, followerToPromote);
        long followerLocalLogStart = followerReplica.getLogTablet().localLogStartOffset();
        assertThat(snapshotLogOffset)
                .as(
                        "Snapshot log offset (%d) should be less than follower's "
                                + "localLogStartOffset (%d) to trigger remote log recovery",
                        snapshotLogOffset, followerLocalLogStart)
                .isLessThan(followerLocalLogStart);

        // =====================================================================
        // Step 5: Promote follower to new leader
        // =====================================================================
        LeaderAndIsr currentLeaderAndIsr =
                FLUSS_CLUSTER_EXTENSION.waitLeaderAndIsrReady(tableBucket);
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        followerToPromote,
                        currentLeaderAndIsr.leaderEpoch() + 1,
                        currentLeaderAndIsr.isr(),
                        Collections.emptyList(),
                        currentLeaderAndIsr.coordinatorEpoch(),
                        currentLeaderAndIsr.bucketEpoch() + 1);

        FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                followerToPromote, tablePath, tableBucket, newLeaderAndIsr, replicas);

        // =====================================================================
        // Step 6: Verify the new leader recovers all data correctly
        // =====================================================================
        // The new leader will:
        //   1. Download snapshot S1 (contains Phase 1 data, logOffset = snapshotLogOffset)
        //   2. Recover from remote log: snapshotLogOffset → localLogStartOffset (Phase 2 data)
        //   3. Recover from local log: localLogStartOffset → HW (remaining data)
        TabletServerGateway newLeaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(followerToPromote);

        List<Tuple2<byte[], byte[]>> expectedKeyValues = getKeyValuePairs(allRecords);

        // Wait until KV recovery is complete by checking the last key is available.
        waitUntil(
                () -> {
                    try {
                        PbLookupRespForBucket resp =
                                newLeaderGateway
                                        .lookup(
                                                newLookupRequest(
                                                        tableBucket.getTableId(),
                                                        tableBucket.getBucket(),
                                                        expectedKeyValues.get(
                                                                        expectedKeyValues.size()
                                                                                - 1)
                                                                .f0))
                                        .get()
                                        .getBucketsRespAt(0);
                        return resp.getValuesCount() > 0 && resp.getValueAt(0).hasValues();
                    } catch (Exception e) {
                        if (ExceptionUtils.stripExecutionException(e)
                                instanceof NotLeaderOrFollowerException) {
                            return false;
                        }
                        throw e;
                    }
                },
                Duration.ofMinutes(3),
                "Fail to wait for the KV recovery from remote log to complete.");

        // Verify all records are correctly recovered by looking up each key.
        for (Tuple2<byte[], byte[]> keyValue : expectedKeyValues) {
            assertLookupResponse(
                    newLeaderGateway
                            .lookup(
                                    newLookupRequest(
                                            tableBucket.getTableId(),
                                            tableBucket.getBucket(),
                                            keyValue.f0))
                            .get(),
                    keyValue.f1);
        }

        // Clean up: restart the other follower to restore cluster health.
        FLUSS_CLUSTER_EXTENSION.startTabletServer(otherFollower);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_BUCKET_NUMBER, 1);
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // Small segment size to produce multiple log segments quickly.
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("100b"));
        // Short remote log task interval for faster tiering.
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        // Keep only 1 local segment after tiering to push localLogStartOffset forward.
        conf.setInt(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS, 1);
        // Disable auto-snapshot by setting a very long interval.
        // Snapshot will be triggered manually in the test to control the exact timing.
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        // Tiny write buffer so KV data is flushed immediately.
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1b"));
        // Short max lag time to allow quick ISR shrink/expand.
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        return conf;
    }

    private CompletableFuture<?> putRecordBatch(
            TableBucket tableBucket, int leaderServer, KvRecordBatch kvRecordBatch) {
        PutKvRequest putKvRequest =
                newPutKvRequest(
                        tableBucket.getTableId(), tableBucket.getBucket(), -1, kvRecordBatch);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);
        return leaderGateway.putKv(putKvRequest);
    }
}
