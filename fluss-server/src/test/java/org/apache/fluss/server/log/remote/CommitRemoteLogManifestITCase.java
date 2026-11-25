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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestData;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.testutils.TestTableNames;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for commit remote log manifest. */
class CommitRemoteLogManifestITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private final TestTableNames tableNames = new TestTableNames();
    private final TablePath data1TablePath = tableNames.tablePath(TestData.DATA1_TABLE_PATH);

    @Test
    void testDeleteOutOfSyncReplicaLogAfterCommit() throws Exception {
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, data1TablePath, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        Replica leaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
        int leader = Objects.requireNonNull(leaderReplica.getLeaderId());
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        List<Integer> followers =
                Stream.of(0, 1, 2).filter(i -> i != leader).collect(Collectors.toList());

        // Wait for each batch to replicate so followers roll the same segments as the leader.
        long followerEndOffset = 0L;
        for (int i = 0; i < 3; i++) {
            assertProduceLogResponse(
                    leaderGateway
                            .produceLog(
                                    newProduceLogRequest(
                                            tableId, 0, -1, genMemoryLogRecordsByObject(DATA1)))
                            .get(),
                    0,
                    followerEndOffset);
            followerEndOffset += DATA1.size();
            long expectedEndOffset = followerEndOffset;
            for (int follower : followers) {
                LogTablet followerLog =
                        FLUSS_CLUSTER_EXTENSION
                                .waitAndGetFollowerReplica(tb, follower)
                                .getLogTablet();
                retry(
                        Duration.ofMinutes(1),
                        () ->
                                assertThat(followerLog.localLogEndOffset())
                                        .as("Follower %s log end offset", follower)
                                        .isEqualTo(expectedEndOffset));
            }
        }

        for (int follower : followers) {
            FLUSS_CLUSTER_EXTENSION.stopReplica(follower, tb, leaderReplica.getLeaderEpoch());
        }
        // Roll the leader's third segment without appending the fourth batch to the followers.
        assertProduceLogResponse(
                leaderGateway
                        .produceLog(
                                newProduceLogRequest(
                                        tableId, 0, -1, genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                followerEndOffset);
        long batchSize = DATA1.size();
        for (int follower : followers) {
            FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(tb, follower);
            LogTablet followerLog =
                    FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tb, follower).getLogTablet();
            assertThat(followerLog.localLogEndOffset()).isEqualTo(followerEndOffset);
            assertThat(followerLog.logSegments())
                    .extracting(LogSegment::getBaseOffset)
                    .containsExactly(0L, batchSize, 2 * batchSize);
        }

        // restart the leader server with a small log tiering interval
        FLUSS_CLUSTER_EXTENSION.restartTabletServer(
                leader,
                new Configuration()
                        .set(
                                ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION,
                                Duration.ofMillis(1)));
        FLUSS_CLUSTER_EXTENSION.waitUntilRemoteLogEndOffset(tb, followerEndOffset);

        // The stopped followers should delete their oldest local segment after the commit.
        for (int follower : followers) {
            LogTablet followerLog =
                    FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tb, follower).getLogTablet();
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(followerLog.logSegments())
                                    .extracting(LogSegment::getBaseOffset)
                                    .containsExactly(batchSize, 2 * batchSize));
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a larger interval for testing purpose
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofDays(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1b"));

        // set a shorter max log time to allow replica shrink from isr
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(2));
        return conf;
    }
}
