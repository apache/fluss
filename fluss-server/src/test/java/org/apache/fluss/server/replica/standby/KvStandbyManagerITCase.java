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

package org.apache.fluss.server.replica.standby;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link KvStandbyManager}. */
public class KvStandbyManagerITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperCompletedSnapshotHandleStore completedSnapshotHandleStore;
    private ZooKeeperClient zkClient;

    @BeforeEach
    void beforeEach() {
        completedSnapshotHandleStore =
                new ZooKeeperCompletedSnapshotHandleStore(
                        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient());
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testStartStandby() throws Exception {
        int bucketId = 0;
        TablePath tablePath = TablePath.of("test_db", "test_table_standby");
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(1, "a").build();
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, descriptor);
        TableBucket tb = new TableBucket(tableId, bucketId);

        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);

        produceKvRecordsAndWaitRemoteLogCopy(0, leaderGateway, tb, true);
        final long snapshot1Id = 0;
        waitValue(
                        () -> completedSnapshotHandleStore.get(tb, snapshot1Id),
                        Duration.ofMinutes(2),
                        "Fail to wait for the snapshot 0 for bucket " + tb)
                .retrieveCompleteSnapshot();

        produceKvRecordsAndWaitRemoteLogCopy(1, leaderGateway, tb, false);

        // get a follower.
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        List<Integer> replicas =
                zkClient.getTableAssignment(tableId).get().getBucketAssignment(0).getReplicas();
        Integer follower = replicas.stream().filter(i -> i != leaderId).findFirst().get();
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(follower);
        ReplicaManager replicaManager = tabletServer.getReplicaManager();

        Replica replica = replicaManager.getReplicaOrException(tb);
        assertThat(replica.getKvTablet()).isNull();

        // try to start standby.
        KvStandbyManager kvStandbyManager = replicaManager.getKvStandbyManager();
        kvStandbyManager.startStandby(replicaManager.getReplicaOrException(tb));

        // wait until kv become hotStandby.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    KvTablet kvTablet = replica.getKvTablet();
                    assertThat(kvTablet).isNotNull();
                    assertThat(kvTablet.getKvAppliedOffset())
                            .isEqualTo(replica.getLogTablet().localLogEndOffset());
                    assertThat(kvTablet.getFlushedLogOffset())
                            .isEqualTo(replica.getLogTablet().localLogEndOffset());
                });

        kvStandbyManager.cancelBecomeHotStandbyTask(tb);
        kvStandbyManager.stopStandby(tb);
    }

    private void produceKvRecordsAndWaitRemoteLogCopy(
            int keyStart,
            TabletServerGateway leaderGateway,
            TableBucket tb,
            boolean waitRemoteLogCopy)
            throws Exception {
        for (int i = keyStart * 10; i < keyStart * 10 + 10; i++) {
            KvRecordBatch kvRecordBatch =
                    genKvRecordBatch(
                            Tuple2.of("k1-" + i, new Object[] {1, "k1-" + i}),
                            Tuple2.of("k2-" + i, new Object[] {2, "k2-" + i}),
                            Tuple2.of("k3-" + i, new Object[] {3, "k3-" + i}));
            PutKvRequest putKvRequest =
                    newPutKvRequest(tb.getTableId(), tb.getBucket(), 1, kvRecordBatch);
            leaderGateway.putKv(putKvRequest).get();
        }

        if (waitRemoteLogCopy) {
            FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(tb);
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_BUCKET_NUMBER, 1);
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1kb"));

        // set a shorter interval for test
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(10));

        // set a shorter max log time to allow replica shrink from isr. Don't be too low, otherwise
        // normal follower synchronization will also be affected
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        return conf;
    }
}
