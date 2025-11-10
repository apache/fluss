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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.server.coordinator.CoordinatorService;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.KvSnapshotManager;
import org.apache.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

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

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.checkSnapshot;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
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
                    createTable(FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR_PK);
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

            PutKvRequest putKvRequest = newPutKvRequest(tableId, bucket, 1, kvRecordBatch);

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
            checkSnapshot(completedSnapshot, expectedKeyValues, 2);
            bucketKvSnapshotDirs.add(
                    new File(completedSnapshot.getSnapshotLocation().getParent().getPath()));

            // put kv batch again
            kvRecordBatch =
                    genKvRecordBatch(
                            Tuple2.of("k1", new Object[] {1, "k11"}),
                            Tuple2.of("k2", null),
                            Tuple2.of("k3", new Object[] {3, "k3"}));
            putKvRequest = newPutKvRequest(tableId, bucket, 1, kvRecordBatch);
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
            checkSnapshot(completedSnapshot, expectedKeyValues, 6);

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

    @Test
    void testStandbyReplicaDownloadLatestSnapshot() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_table_standby");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb0 = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb0);
        // get the leader server
        int leaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb0);

        // put one kv batch
        KvRecordBatch kvRecordBatch =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "k1"}),
                        Tuple2.of("k2", new Object[] {2, "k2"}));
        PutKvRequest putKvRequest = newPutKvRequest(tableId, 0, -1, kvRecordBatch);

        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);
        leaderGateway.putKv(putKvRequest).get();

        // wait for snapshot is available
        final long snapshot1Id = 0;
        CompletedSnapshot completedSnapshot =
                waitValue(
                                () -> completedSnapshotHandleStore.get(tb0, snapshot1Id),
                                Duration.ofMinutes(2),
                                "Fail to wait for the snapshot 0 for bucket " + tb0)
                        .retrieveCompleteSnapshot();
        // check snapshot
        List<Tuple2<byte[], byte[]>> expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {1, "k1"}),
                                Tuple2.of("k2", new Object[] {2, "k2"})));
        checkSnapshot(completedSnapshot, expectedKeyValues, 2);

        // check the standby replica contains the latest snapshot.
        int standbyServer = FLUSS_CLUSTER_EXTENSION.waitAndGetStandby(tb0);
        TabletServer standbyTs = FLUSS_CLUSTER_EXTENSION.getTabletServerById(standbyServer);
        Replica replica = standbyTs.getReplicaManager().getReplicaOrException(tb0);
        assertThat(replica.isStandby()).isTrue();

        KvSnapshotManager kvSnapshotManager = replica.getKvSnapshotManager();
        assertThat(kvSnapshotManager).isNotNull();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(kvSnapshotManager.getDownloadedSstFiles()).isNotEmpty();
                    assertThat(kvSnapshotManager.getDownloadedMiscFiles()).isNotEmpty();
                });

        // put kv batch again
        kvRecordBatch =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "k11"}),
                        Tuple2.of("k2", null),
                        Tuple2.of("k3", new Object[] {3, "k3"}));
        putKvRequest = newPutKvRequest(tableId, 0, 1, kvRecordBatch);
        leaderGateway.putKv(putKvRequest).get();

        // wait for next snapshot is available
        final long snapshot2Id = 1;
        completedSnapshot =
                waitValue(
                                () -> completedSnapshotHandleStore.get(tb0, snapshot2Id),
                                Duration.ofMinutes(2),
                                "Fail to wait for the snapshot 0 for bucket " + tb0)
                        .retrieveCompleteSnapshot();

        // check snapshot
        expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {1, "k11"}),
                                Tuple2.of("k3", new Object[] {3, "k3"})));
        checkSnapshot(completedSnapshot, expectedKeyValues, 6);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(kvSnapshotManager.getDownloadedSstFiles()).isNotEmpty();
                    assertThat(kvSnapshotManager.getDownloadedMiscFiles()).isNotEmpty();
                });
    }

    @Test
    void testStandbyPromotedToLeaderOnLeaderShutdown() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_table_standby_promote");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb0 = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb0);
        int leaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb0);

        // put some data
        KvRecordBatch kvRecordBatch =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "k1"}),
                        Tuple2.of("k2", new Object[] {2, "k2"}));
        PutKvRequest putKvRequest = newPutKvRequest(tableId, 0, -1, kvRecordBatch);

        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);
        leaderGateway.putKv(putKvRequest).get();

        // wait for snapshot to be available
        final long snapshot1Id = 0;
        CompletedSnapshot completedSnapshot =
                waitValue(
                                () -> completedSnapshotHandleStore.get(tb0, snapshot1Id),
                                Duration.ofMinutes(2),
                                "Fail to wait for the snapshot 0 for bucket " + tb0)
                        .retrieveCompleteSnapshot();

        // check snapshot
        List<Tuple2<byte[], byte[]>> expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {1, "k1"}),
                                Tuple2.of("k2", new Object[] {2, "k2"})));
        checkSnapshot(completedSnapshot, expectedKeyValues, 2);

        // wait for standby to be ready
        int standbyServer = FLUSS_CLUSTER_EXTENSION.waitAndGetStandby(tb0);
        TabletServer standbyTs = FLUSS_CLUSTER_EXTENSION.getTabletServerById(standbyServer);
        Replica standbyReplica = standbyTs.getReplicaManager().getReplicaOrException(tb0);
        assertThat(standbyReplica.isStandby()).isTrue();

        KvSnapshotManager standbySnapshotManager = standbyReplica.getKvSnapshotManager();
        assertThat(standbySnapshotManager).isNotNull();
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(standbySnapshotManager.getDownloadedSstFiles()).isNotEmpty());

        // shutdown leader tablet server to trigger standby -> leader promotion
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(leaderServer);

        // wait for the standby to become leader
        retry(
                Duration.ofMinutes(1),
                () -> {
                    int newLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb0);
                    assertThat(newLeader).isEqualTo(standbyServer);
                });

        // verify the new leader (former standby) can serve read/write requests
        TabletServerGateway newLeaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(standbyServer);

        // put more data
        kvRecordBatch =
                genKvRecordBatch(
                        Tuple2.of("k3", new Object[] {3, "k3"}),
                        Tuple2.of("k4", new Object[] {4, "k4"}));
        putKvRequest = newPutKvRequest(tableId, 0, 1, kvRecordBatch);
        newLeaderGateway.putKv(putKvRequest).get();

        // wait for new snapshot
        final long snapshot2Id = 1;
        completedSnapshot =
                waitValue(
                                () -> completedSnapshotHandleStore.get(tb0, snapshot2Id),
                                Duration.ofMinutes(2),
                                "Fail to wait for the snapshot 1 for bucket " + tb0)
                        .retrieveCompleteSnapshot();

        // check snapshot contains all data
        expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {1, "k1"}),
                                Tuple2.of("k2", new Object[] {2, "k2"}),
                                Tuple2.of("k3", new Object[] {3, "k3"}),
                                Tuple2.of("k4", new Object[] {4, "k4"})));
        checkSnapshot(completedSnapshot, expectedKeyValues, 4);

        // restart the shutdown server to restore cluster state
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leaderServer, true);
    }

    @Test
    void testStandbyIncrementalSnapshotDownload() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_table_incremental");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb0 = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb0);
        int leaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb0);

        // put first batch of data
        KvRecordBatch kvRecordBatch =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "k1"}),
                        Tuple2.of("k2", new Object[] {2, "k2"}));
        PutKvRequest putKvRequest = newPutKvRequest(tableId, 0, -1, kvRecordBatch);

        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);
        leaderGateway.putKv(putKvRequest).get();

        // wait for first snapshot
        waitValue(
                () -> completedSnapshotHandleStore.get(tb0, 0),
                Duration.ofMinutes(2),
                "Fail to wait for snapshot 0");

        // wait for standby to download first snapshot
        int standbyServer = FLUSS_CLUSTER_EXTENSION.waitAndGetStandby(tb0);
        TabletServer standbyTs = FLUSS_CLUSTER_EXTENSION.getTabletServerById(standbyServer);
        Replica standbyReplica = standbyTs.getReplicaManager().getReplicaOrException(tb0);
        KvSnapshotManager standbySnapshotManager = standbyReplica.getKvSnapshotManager();

        retry(
                Duration.ofMinutes(1),
                () -> assertThat(standbySnapshotManager.getDownloadedSstFiles()).isNotEmpty());

        // record the sst files after first snapshot
        int firstSnapshotSstCount = standbySnapshotManager.getDownloadedSstFiles().size();

        // put second batch of data (this should create new sst files)
        kvRecordBatch =
                genKvRecordBatch(
                        Tuple2.of("k3", new Object[] {3, "k3"}),
                        Tuple2.of("k4", new Object[] {4, "k4"}),
                        Tuple2.of("k5", new Object[] {5, "k5"}));
        putKvRequest = newPutKvRequest(tableId, 0, 1, kvRecordBatch);
        leaderGateway.putKv(putKvRequest).get();

        // wait for second snapshot
        waitValue(
                () -> completedSnapshotHandleStore.get(tb0, 1),
                Duration.ofMinutes(2),
                "Fail to wait for snapshot 1");

        // wait for standby to download incremental snapshot
        retry(
                Duration.ofMinutes(1),
                () -> {
                    // should have more or equal sst files after incremental download
                    assertThat(standbySnapshotManager.getDownloadedSstFiles().size())
                            .isGreaterThanOrEqualTo(firstSnapshotSstCount);
                });

        // verify standby snapshot size is updated
        assertThat(standbySnapshotManager.getStandbySnapshotSize()).isGreaterThan(0);
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
