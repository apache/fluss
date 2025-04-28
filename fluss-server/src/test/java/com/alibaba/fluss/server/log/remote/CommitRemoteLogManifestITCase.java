/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.log.remote;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.server.log.LogTablet;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for commit remote log manifest. */
class CommitRemoteLogManifestITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    @Test
    void testDeleteOutOfSyncReplicaLogAfterCommit() throws Exception {
        // then create a table with 3 buckets
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);

        // find the tb whose leader is the server with large log tiering interval.
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);
        int leader =
                Objects.requireNonNull(
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb).getLeaderId());
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        // produce many records to trigger remote log copy.
        for (int i = 0; i < 3; i++) {
            assertProduceLogResponse(
                    leaderGateWay
                            .produceLog(
                                    newProduceLogRequest(
                                            tableId, 0, -1, genMemoryLogRecordsByObject(DATA1)))
                            .get(),
                    0,
                    i * 10L);
        }

        // stop a replica to mock follower is out of sync
        int stopFollower = Stream.of(0, 1, 2).filter(i -> i != leader).findFirst().get();
        FLUSS_CLUSTER_EXTENSION.stopReplica(stopFollower, tb, 1);
        leaderGateWay
                .produceLog(
                        newProduceLogRequest(tableId, 0, -1, genMemoryLogRecordsByObject(DATA1)))
                .get();
        FLUSS_CLUSTER_EXTENSION.waitUtilReplicaShrinkFromIsr(tb, stopFollower);

        LogTablet stopfollowerLogTablet =
                FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tb, stopFollower).getLogTablet();
        assertThat(stopfollowerLogTablet.logSegments()).hasSize(3);

        // send notify leader to make remote log tier happen immediately
        FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                leader,
                DATA1_TABLE_PATH,
                tb,
                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getLeaderAndIsr(tb).get(),
                Arrays.asList(0, 1, 2));
        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(tb);

        // check has two remote log segments for the stopped replica
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(stopfollowerLogTablet.logSegments()).hasSize(2));
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
