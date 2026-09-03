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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.toKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for restoring a follower's KV log retention boundary after restart. */
class KvFollowerRetentionRestoreITCase {

    private static final Duration LOG_TTL = Duration.ofHours(1);
    private static final ManualClock MANUAL_CLOCK = new ManualClock(System.currentTimeMillis());

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(2)
                    .setClusterConf(initConfig())
                    .setClock(MANUAL_CLOCK)
                    .build();

    @Test
    void testCleanupLocalLogsAfterFollowerRestartWithoutNewWrites() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_follower_retention_restore");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA_PK)
                        .distributedBy(1, "a")
                        .property(ConfigOptions.TABLE_LOG_TTL, LOG_TTL)
                        .build();
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tableBucket);

        LeaderAndIsr leaderAndIsr = FLUSS_CLUSTER_EXTENSION.waitLeaderAndIsrReady(tableBucket);
        int leader = leaderAndIsr.leader();
        int follower =
                leaderAndIsr.isr().stream().filter(replica -> replica != leader).findFirst().get();
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        for (int batchId = 0; batchId < 4; batchId++) {
            List<KvRecord> records = new ArrayList<>();
            for (int recordId = 0; recordId < 10; recordId++) {
                int key = batchId * 10 + recordId;
                records.addAll(genKvRecords(new Object[] {key, "value_" + key}));
            }
            PutKvResponse response =
                    leaderGateway
                            .putKv(
                                    newPutKvRequest(
                                            tableId,
                                            tableBucket.getBucket(),
                                            -1,
                                            toKvRecordBatch(records)))
                            .get();
            assertThat(response.getBucketsRespAt(0).hasErrorCode()).isFalse();
        }

        Replica leaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
        Replica followerReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tableBucket, follower);
        long logEndOffset = leaderReplica.getLocalLogEndOffset();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(followerReplica.getLocalLogEndOffset()).isEqualTo(logEndOffset);
                    assertThat(followerReplica.getLogTablet().getHighWatermark())
                            .isEqualTo(logEndOffset);
                    assertThat(followerReplica.getLogTablet().getMinRetainOffset()).isZero();
                    assertThat(followerReplica.getLogTablet().getSegments().size()).isEqualTo(4);
                });

        // The stopped follower misses the snapshot notification that advances minRetainOffset.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(follower);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(tableBucket, follower);
        CompletedSnapshot snapshot = FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tableBucket);
        assertThat(snapshot.getLogOffset()).isEqualTo(logEndOffset);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(leaderReplica.getLogTablet().getMinRetainOffset())
                                .isEqualTo(logEndOffset));
        MANUAL_CLOCK.advanceTime(LOG_TTL.plusMillis(1));

        // Do not write after the snapshot. The restarted follower must recover the retention
        // boundary from a successful empty fetch response.
        FLUSS_CLUSTER_EXTENSION.startTabletServer(follower);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaExpandToIsr(tableBucket, follower);
        Replica restartedFollower =
                FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tableBucket, follower);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(restartedFollower.getLocalLogEndOffset()).isEqualTo(logEndOffset);
                    assertThat(restartedFollower.getLogTablet().getMinRetainOffset())
                            .isEqualTo(logEndOffset);
                    // since minRetainOffset is equal to logEndOffset, the follower should have
                    // cleaned up all segments except the last one.
                    assertThat(restartedFollower.getLogTablet().getSegments().size()).isEqualTo(1);
                    assertThat(restartedFollower.getLocalLogStartOffset()).isPositive();
                });
        assertThat(leaderReplica.getLocalLogEndOffset()).isEqualTo(logEndOffset);
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 2);
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("100b"));
        conf.set(ConfigOptions.LOG_RETENTION_CHECK_INTERVAL, Duration.ofMillis(100));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        return conf;
    }
}
