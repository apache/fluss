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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ErrorMessage;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.FlushOptions;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.dropTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** The ITCase for tabletServer shutdown (controlled shutdown). */
public class TabletServerShutdownITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIOExceptionShouldStopTabletServer(boolean isLogTable) throws Exception {
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        Schema schema =
                isLogTable
                        ? Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.STRING())
                                .build()
                        : Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.STRING())
                                .primaryKey("a")
                                .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();

        TablePath tablePath =
                TablePath.of(
                        "test_failover", "test_ioexception_table_" + (isLogTable ? "log" : "pk"));
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // delete the active segment, which will cause IOException when append log/changelog
        LogSegment logSegment =
                FLUSS_CLUSTER_EXTENSION
                        .waitAndGetLeaderReplica(tb)
                        .getLogTablet()
                        .activeLogSegment();
        logSegment.deleteIfExists();

        ErrorMessage errorResponse = null;
        ExecutionException requestFailure = null;
        try {
            errorResponse = writeData(leaderGateWay, tableId, isLogTable);
        } catch (ExecutionException e) {
            requestFailure = e;
        }

        TabletServer leaderServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader);
        try {
            // The storage IOException should trigger the fatal error handler and stop the leader
            // server. The failed request may either receive the storage error response or fail when
            // the RPC connection is closed, depending on which completes first.
            FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(2);
        } finally {
            // Wait for the fatal shutdown to finish before reusing the same server ID and data dir.
            leaderServer.closeAsync().get();
            FLUSS_CLUSTER_EXTENSION.startTabletServer(leader, true);
        }

        if (requestFailure != null) {
            assertThat(requestFailure).cause().isInstanceOf(RetriableException.class);
        } else {
            assertThat(errorResponse).isNotNull();
            assertThat(errorResponse.hasErrorCode()).isTrue();
            assertThat(errorResponse.getErrorCode())
                    .isEqualTo(
                            isLogTable
                                    ? Errors.LOG_STORAGE_EXCEPTION.code()
                                    : Errors.KV_STORAGE_EXCEPTION.code());
        }
    }

    @Test
    void testControlledShutdownConfiguration() throws Exception {
        // Test that the controlled shutdown configuration options are properly loaded
        Configuration conf = new Configuration();

        // Verify default values are loaded correctly
        assertThat(conf.getInt(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES))
                .isEqualTo(3);
        assertThat(
                        conf.get(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_RETRY_INTERVAL)
                                .toMillis())
                .isEqualTo(1000L);

        // Test custom configuration values
        Configuration customConf = new Configuration();
        customConf.set(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES, 5);
        customConf.set(
                ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_RETRY_INTERVAL,
                Duration.ofMillis(2000));

        assertThat(customConf.getInt(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES))
                .isEqualTo(5);
        assertThat(
                        customConf
                                .get(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_RETRY_INTERVAL)
                                .toMillis())
                .isEqualTo(2000L);
    }

    @Test
    void testControlledShutdown() throws Exception {
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();
        TablePath tablePath = TablePath.of("test_shutdown", "test_controlled_shutdown");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr leaderAndIsr = FLUSS_CLUSTER_EXTENSION.waitLeaderAndIsrReady(tb);
        int leader = leaderAndIsr.leader();

        // test kill the tabletServers with leader on.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(leader);
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();

        // the leader should be removed from isr, and new leader should be elected.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .map(LeaderAndIsr::leader)
                                .isNotEqualTo(leader));

        // restart the shutdown server
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leader, true);
    }

    @Test
    void testControlledShutdownCleansKvWhenRestartedAsFollower() throws Exception {
        TablePath tablePath = TablePath.of("test_shutdown", "kv_orphan_cleanup");
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA_PK)
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                                .build());
        TableBucket tableBucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tableBucket);
        int oldLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(oldLeader);
        assertThat(writeData(gateway, tableId, false).hasErrorCode()).isFalse();
        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
        flushAndWait(replica.getKvTablet(), Long.MAX_VALUE);
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            replica.getKvTablet().getRocksDBKv().getDb().flush(flushOptions);
        }
        File kvDir = replica.getKvTablet().getKvTabletDir();
        File logDir = replica.getLogTablet().getLogDir();

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(oldLeader);
        try {
            // Controlled shutdown closes the old leader's RocksDB without deleting its files.
            assertThat(kvDir).isDirectory();
            assertThat(kvDir.toPath().resolve("db"))
                    .isDirectoryContaining(path -> path.toString().endsWith(".sst"));
        } finally {
            FLUSS_CLUSTER_EXTENSION.startTabletServer(oldLeader);
        }

        Replica follower =
                FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tableBucket, oldLeader);
        assertThat(follower.getKvTablet()).isNull();
        assertThat(kvDir).doesNotExist();
        assertThat(logDir).isDirectory();

        dropTable(FLUSS_CLUSTER_EXTENSION, tablePath);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(logDir).doesNotExist();
                    assertThat(kvDir.getParentFile()).doesNotExist();
                });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testKvRecoveryAfterStartupCleanup(boolean withSnapshot) throws Exception {
        TablePath tablePath = TablePath.of("test_shutdown", "kv_startup_recovery_" + withSnapshot);
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA_PK)
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                                .build());
        TableBucket tableBucket = new TableBucket(tableId, 0);
        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
        TabletServerGateway gateway = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        assertThat(writeData(gateway, tableId, false).hasErrorCode()).isFalse();
        flushAndWait(replica.getKvTablet(), Long.MAX_VALUE);
        if (withSnapshot) {
            FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tableBucket);
        }

        // This update is recovered from the log, including when an older snapshot exists.
        assertThat(
                        gateway.putKv(
                                        newPutKvRequest(
                                                tableId,
                                                0,
                                                1,
                                                genKvRecordBatch(new Object[] {1, "updated"})))
                                .get()
                                .getBucketsRespAt(0)
                                .hasErrorCode())
                .isFalse();
        File kvDir = replica.getKvTablet().getKvTabletDir();
        Path staleFile = kvDir.toPath().resolve("stale-file");
        Files.write(staleFile, new byte[] {1});

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(leader);
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leader);
        Replica recovered = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
        assertThat(staleFile).doesNotExist();
        assertThat(kvDir).isDirectory();
        assertThat(recovered.getRowCount()).isEqualTo(2L);
        TabletServerGateway recoveredGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        for (Tuple2<byte[], byte[]> keyValue :
                getKeyValuePairs(
                        genKvRecords(new Object[] {1, "updated"}, new Object[] {2, "b1"}))) {
            assertLookupResponse(
                    recoveredGateway.lookup(newLookupRequest(tableId, 0, keyValue.f0)).get(),
                    keyValue.f1);
        }
        dropTable(FLUSS_CLUSTER_EXTENSION, tablePath);
    }

    @Test
    void testControlledShutdownRetriesFailover() throws Exception {
        // This case is to test the scenario that the controlled shutdown request is retried and
        // failed by cannot elect any new leader. In this case the controlled shutdown will finally
        // go uncontrolled shutdown.
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 2)
                        .build();
        TablePath tablePath = TablePath.of("test_failover", "test_controlled_shutdown_failed");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr leaderAndIsr = FLUSS_CLUSTER_EXTENSION.waitLeaderAndIsrReady(tb);
        List<Integer> isr = new ArrayList<>(leaderAndIsr.isr());
        int leader = leaderAndIsr.leader();
        isr.remove(Integer.valueOf(leader));
        int follower = isr.get(0);

        // Let's kil follower. Will go controlled shutdown.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(follower);
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();

        // the follower should be removed from isr
        LeaderAndIsr expectedLeaderAndIsr1 =
                leaderAndIsr.newLeaderAndIsr(Collections.singletonList(leader));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb).get())
                                .isEqualTo(expectedLeaderAndIsr1));

        // kill the leader. As we only have 1 replica, no leader can be elected as we send the
        // controlled shutdown request to the leader. So the controlled shutdown will finally go
        // uncontrolled shutdown.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(leader);

        // should be no leader
        LeaderAndIsr expectedLeaderAndIsr2 =
                expectedLeaderAndIsr1.newLeaderAndIsr(
                        LeaderAndIsr.NO_LEADER,
                        Collections.singletonList(leader),
                        Collections.emptyList());
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb).get())
                                .isEqualTo(expectedLeaderAndIsr2));

        // start the follower
        // should still be no leader since the follower is out of isr, should be elected as leader
        FLUSS_CLUSTER_EXTENSION.startTabletServer(follower);

        // start the leader server, the leader should be the previous leader server
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leader);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zkClient.getLeaderAndIsr(tb).get().leader()).isEqualTo(leader));
    }

    private ErrorMessage writeData(
            TabletServerGateway tabletServerGateway, long tableId, boolean isLogTable)
            throws Exception {
        if (isLogTable) {
            return tabletServerGateway
                    .produceLog(
                            newProduceLogRequest(tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                    .get()
                    .getBucketsRespAt(0);
        } else {
            return tabletServerGateway
                    .putKv(
                            newPutKvRequest(
                                    tableId, 0, 1, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                    .get()
                    .getBucketsRespAt(0);
        }
    }
}
