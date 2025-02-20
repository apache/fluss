/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.server.coordinator.CoordinatorService;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.testutils.KvTestUtils;
import com.alibaba.fluss.server.testutils.RpcMessageTestUtils;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecords;
import static com.alibaba.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for kv doing snapshot. */
class KvSnapshotITCase {

    private static final int BUCKET_NUM = 2;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperCompletedSnapshotHandleStore completedSnapshotHandleStore;
    private CoordinatorService coordinatorService;
    private String remoteDataDir;

    @BeforeEach
    void beforeEach() {
        completedSnapshotHandleStore =
                new ZooKeeperCompletedSnapshotHandleStore(
                        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient());
        this.coordinatorService =
                FLUSS_CLUSTER_EXTENSION.getCoordinatorServer().getCoordinatorService();
        remoteDataDir = FLUSS_CLUSTER_EXTENSION.getRemoteDataDir();
    }

    @Test
    void testKvSnapshotAndDelete() throws Exception {
        // test snapshot for multiple table
        int tableNum = 3;
        List<TableBucket> tableBuckets = new ArrayList<>();
        Map<Long, TablePath> tablePathMap = new HashMap<>();
        for (int i = 0; i < tableNum; i++) {
            TablePath tablePath = TablePath.of("test_db", "test_table_" + i);
            long tableId =
                    RpcMessageTestUtils.createTable(
                            FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR_PK);
            tablePathMap.put(tableId, tablePath);
            for (int bucket = 0; bucket < BUCKET_NUM; bucket++) {
                tableBuckets.add(new TableBucket(tableId, bucket));
            }
        }

        Set<File> bucketKvSnapshotDirs = new HashSet<>();
        for (TableBucket tableBucket : tableBuckets) {
            long tableId = tableBucket.getTableId();
            int bucket = tableBucket.getBucket();
            TableBucket tb = new TableBucket(tableId, bucket);
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
            // get the leader server
            int leaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);

            // put one kv batch
            KvRecordBatch kvRecordBatch =
                    genKvRecordBatch(
                            Tuple2.of("k1", new Object[] {1, "k1"}),
                            Tuple2.of("k2", new Object[] {2, "k2"}));

            PutKvRequest putKvRequest =
                    RpcMessageTestUtils.newPutKvRequest(tableId, bucket, 1, kvRecordBatch);

            TabletServerGateway leaderGateway =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);
            leaderGateway.putKv(putKvRequest).get();

            // wait for snapshot is available
            final long snapshot1Id = 0;
            CompletedSnapshot completedSnapshot =
                    waitValue(
                                    () -> completedSnapshotHandleStore.get(tb, snapshot1Id),
                                    Duration.ofMinutes(2),
                                    "Fail to wait for the snapshot 0 for bucket " + tb)
                            .retrieveCompleteSnapshot();

            // check snapshot
            List<Tuple2<byte[], byte[]>> expectedKeyValues =
                    getKeyValuePairs(
                            genKvRecords(
                                    Tuple2.of("k1", new Object[] {1, "k1"}),
                                    Tuple2.of("k2", new Object[] {2, "k2"})));
            KvTestUtils.checkSnapshot(completedSnapshot, expectedKeyValues, 2);
            bucketKvSnapshotDirs.add(
                    new File(completedSnapshot.getSnapshotLocation().getParent().getPath()));

            // put kv batch again
            kvRecordBatch =
                    genKvRecordBatch(
                            Tuple2.of("k1", new Object[] {1, "k11"}),
                            Tuple2.of("k2", null),
                            Tuple2.of("k3", new Object[] {3, "k3"}));
            putKvRequest = RpcMessageTestUtils.newPutKvRequest(tableId, bucket, 1, kvRecordBatch);
            leaderGateway.putKv(putKvRequest).get();

            // wait for next snapshot is available
            final long snapshot2Id = 1;
            completedSnapshot =
                    waitValue(
                                    () -> completedSnapshotHandleStore.get(tb, snapshot2Id),
                                    Duration.ofMinutes(2),
                                    "Fail to wait for the snapshot 0 for bucket " + tb)
                            .retrieveCompleteSnapshot();

            // check snapshot
            expectedKeyValues =
                    getKeyValuePairs(
                            genKvRecords(
                                    Tuple2.of("k1", new Object[] {1, "k11"}),
                                    Tuple2.of("k3", new Object[] {3, "k3"})));
            KvTestUtils.checkSnapshot(completedSnapshot, expectedKeyValues, 6);

            // check min retain offset
            for (TabletServer server : FLUSS_CLUSTER_EXTENSION.getTabletServers()) {
                Replica replica = server.getReplicaManager().getReplicaOrException(tb);
                // all replica min retain offset should equal to snapshot offset.
                // use retry here because the follower min retain offset is updated asynchronously
                retry(
                        Duration.ofMinutes(1),
                        () ->
                                assertThat(replica.getLogTablet().getMinRetainOffset())
                                        .as("Replica %s min retain offset", replica)
                                        .isEqualTo(6));
            }
        }
        for (TablePath tablePath : tablePathMap.values()) {
            coordinatorService.dropTable(
                    newDropTableRequest(
                            tablePath.getDatabaseName(), tablePath.getTableName(), false));
        }
        checkDirsDeleted(bucketKvSnapshotDirs, tablePathMap);
    }

    private void checkDirsDeleted(Set<File> bucketDirs, Map<Long, TablePath> tablePathMap) {
        for (File bucketDir : bucketDirs) {
            retry(Duration.ofMinutes(1), () -> assertThat(bucketDir.exists()).isFalse());
        }
        for (Map.Entry<Long, TablePath> tablePathEntry : tablePathMap.entrySet()) {
            FsPath fsPath =
                    FlussPaths.remoteTableDir(
                            FsPath.fromLocalFile(new File(remoteDataDir)),
                            tablePathEntry.getValue(),
                            tablePathEntry.getKey());
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(new File(fsPath.getPath()).exists()).isFalse());
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for test
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));

        return conf;
    }
}
