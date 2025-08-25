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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for notify leader and isr. */
public class NotifyLeaderAndIsrITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testNotifyLeaderAndIsr() throws Exception {
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr leaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
                        Duration.ofMinutes(1),
                        "Leader and isr not found");

        // test leader.
        int leader = leaderAndIsr.leader();
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader);
        ReplicaManager replicaManager = tabletServer.getReplicaManager();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(replicaManager.getReplica(tb))
                            .isInstanceOf(ReplicaManager.OnlineReplica.class);
                    Replica replica = replicaManager.getReplicaOrException(tb);
                    assertThat(replica.isLeader()).isTrue();
                    assertThat(replica.getLeaderEpoch()).isEqualTo(0);
                    assertThat(replica.getBucketEpoch()).isEqualTo(0);
                });

        // test follower.
        for (int followId :
                leaderAndIsr.isr().stream()
                        .filter(id -> id != leader)
                        .collect(Collectors.toList())) {
            TabletServer follower = FLUSS_CLUSTER_EXTENSION.getTabletServerById(followId);
            ReplicaManager replicaManager1 = follower.getReplicaManager();
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        assertThat(replicaManager1.getReplica(tb))
                                .isInstanceOf(ReplicaManager.OnlineReplica.class);
                        Replica replica = replicaManager1.getReplicaOrException(tb);
                        assertThat(replica.isLeader()).isFalse();
                        assertThat(replica.getLeaderEpoch()).isEqualTo(0);
                        assertThat(replica.getBucketEpoch()).isEqualTo(0);
                        assertThat(replica.getLeaderId()).isEqualTo(leader);
                    });
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }
}
