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

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.testutils.RpcMessageTestUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for remove offline replica from ISR. */
class RemoveOfflineReplicaFromIsrITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);

        // set a long max lag time, so that the log replica can be removed from isr due to
        // fetch lag by leader to make sure it's removed by coordinator server due to offline
        // replica
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofDays(1));

        // set log replica min in sync replicas number to 2, if the isr set size less than 2,
        // the produce log request will be failed, and the leader HW will not increase.
        conf.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return conf;
    }

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testRemoveOfflineReplicaFromIsr() throws Exception {
        long tableId = createLogTable();
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr currentLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
                        Duration.ofSeconds(20),
                        "Leader and isr not found");
        List<Integer> isr = currentLeaderAndIsr.isr();
        assertThat(isr).containsExactlyInAnyOrder(0, 1, 2);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        Integer follower = isr.stream().filter(i -> i != leader).findFirst().get();

        // stop the follower
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(follower);

        // the follower should be removed from isr
        isr.remove(follower);

        // send one batch data to check the stop follower will become out of sync replica.
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // Wait the stop follower to be removed from ISR since the follower is offline
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .isPresent()
                                .hasValueSatisfying(
                                        leaderAndIsr ->
                                                assertThat(leaderAndIsr.isr())
                                                        .containsExactlyInAnyOrderElementsOf(isr)));

        // produce logs should be successful
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1,
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // make this tablet server re-start.
        FLUSS_CLUSTER_EXTENSION.startTabletServer(follower);
        isr.add(follower);

        // make sure the stopped follower can add back to isr after restart
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .isPresent()
                                .hasValueSatisfying(
                                        leaderAndIsr ->
                                                assertThat(leaderAndIsr.isr())
                                                        .containsExactlyInAnyOrderElementsOf(isr)));
    }

    private long createLogTable() throws Exception {
        // Set bucket to 1 to easy for debug.
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1, "a").build();
        return RpcMessageTestUtils.createTable(
                FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, tableDescriptor);
    }
}
