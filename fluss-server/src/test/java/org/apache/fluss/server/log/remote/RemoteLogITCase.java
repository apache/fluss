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
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for remote log. */
public class RemoteLogITCase {

    private static final ManualClock MANUAL_CLOCK = new ManualClock(System.currentTimeMillis());

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .setClock(MANUAL_CLOCK)
                    .build();

    private TableBucket setupTableBucket() throws Exception {
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        return tb;
    }

    private void produceRecordsAndWaitRemoteLogCopy(
            TabletServerGateway leaderGateway, TableBucket tb) throws Exception {
        for (int i = 0; i < 10; i++) {
            assertProduceLogResponse(
                    leaderGateway
                            .produceLog(
                                    newProduceLogRequest(
                                            tb.getTableId(),
                                            0,
                                            1,
                                            genMemoryLogRecordsByObject(DATA1)))
                            .get(),
                    0,
                    i * 10L);
        }
        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(
                new TableBucket(tb.getTableId(), 0));
    }

    @Test
    public void remoteLogMiscTest() throws Exception {
        TableBucket tb = setupTableBucket();
        long tableId = tb.getTableId();

        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);

        // produce test records
        produceRecordsAndWaitRemoteLogCopy(leaderGateway, tb);

        // test metadata updated: verify manifest in metadata
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leaderId);
        RemoteLogManager remoteLogManager = tabletServer.getReplicaManager().getRemoteLogManager();
        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tb);

        RemoteLogManifest manifest = remoteLogTablet.currentManifest();
        assertThat(manifest.getPhysicalTablePath().getTablePath()).isEqualTo(DATA1_TABLE_PATH);
        assertThat(manifest.getTableBucket()).isEqualTo(tb);
        assertThat(manifest.getRemoteLogSegmentList().size()).isGreaterThan(0);

        // test create: verify remote log created
        FsPath fsPath =
                FlussPaths.remoteLogTabletDir(
                        tabletServer.getReplicaManager().getRemoteLogManager().remoteLogDir(),
                        PhysicalTablePath.of(DATA1_TABLE_PATH),
                        tb);
        FileSystem fileSystem = fsPath.getFileSystem();
        assertThat(fileSystem.exists(fsPath)).isTrue();
        assertThat(fileSystem.listStatus(fsPath).length).isGreaterThan(0);

        // test download remote log
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();

        tabletServer
                .getReplicaManager()
                .fetchLogRecords(
                        new FetchParams(-1, Integer.MAX_VALUE),
                        Collections.singletonMap(tb, new FetchReqInfo(tableId, 0, 10240)),
                        null,
                        future::complete);

        Map<TableBucket, FetchLogResultForBucket> result = future.get();
        assertThat(result).hasSize(1);

        FetchLogResultForBucket fetchLogResult = result.get(tb);
        assertThat(fetchLogResult.getError()).isEqualTo(ApiError.NONE);
        assertThat(fetchLogResult.fetchFromRemote()).isTrue();

        // test drop remote log
        CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        coordinatorGateway
                .dropTable(
                        newDropTableRequest(
                                DATA1_TABLE_PATH.getDatabaseName(),
                                DATA1_TABLE_PATH.getTableName(),
                                true))
                .get();
        retry(Duration.ofMinutes(2), () -> assertThat(fileSystem.exists(fsPath)).isFalse());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFollowerFetchAlreadyMoveToRemoteLog(boolean withWriterId) throws Exception {
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        int follower;
        for (int i = 0; true; i++) {
            if (i != leader) {
                follower = i;
                break;
            }
        }
        // kill follower, and restart after some segments in leader has been copied to remote.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(follower);

        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        // produce many records to trigger remote log copy.
        for (int i = 0; i < 10; i++) {
            assertProduceLogResponse(
                    leaderGateWay
                            .produceLog(
                                    newProduceLogRequest(
                                            tableId,
                                            0,
                                            1,
                                            withWriterId
                                                    ? genMemoryLogRecordsWithWriterId(
                                                            DATA1, 100, i, 0L)
                                                    : genMemoryLogRecordsByObject(DATA1)))
                            .get(),
                    0,
                    i * 10L);
        }

        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(tb, follower);
        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(tb);

        // restart follower
        FLUSS_CLUSTER_EXTENSION.startTabletServer(follower);
        FLUSS_CLUSTER_EXTENSION.waitUntilReplicaExpandToIsr(tb, follower);
    }

    @Test
    void testRemoteLogTTLWithLakeDisabled() throws Exception {
        // Create table without data lake enabled
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        // Set a short TTL for testing (2 hours)
                        .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofHours(2))
                        .build();

        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);

        // Produce records to create remote log segments
        produceRecordsAndWaitRemoteLogCopy(leaderGateway, tb);

        // Verify remote log segments exist
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leaderId);
        RemoteLogManager remoteLogManager = tabletServer.getReplicaManager().getRemoteLogManager();
        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tb);

        int initialSegmentCount = remoteLogTablet.allRemoteLogSegments().size();
        assertThat(initialSegmentCount).isGreaterThan(0);
        long initialRemoteLogStartOffset = remoteLogTablet.getRemoteLogStartOffset();
        assertThat(initialRemoteLogStartOffset).isEqualTo(0L);

        // Verify data lake is disabled
        Replica leaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
        LogTablet logTablet = leaderReplica.getLogTablet();
        assertThat(logTablet.isDataLakeEnabled()).isFalse();

        // Advance time past TTL (2 hours + buffer)
        MANUAL_CLOCK.advanceTime(Duration.ofHours(2).plusMinutes(30));

        // Wait for remote log segments to be cleaned up
        // Since lake is disabled, expired segments should be deleted directly
        retry(
                Duration.ofMinutes(2),
                () -> {
                    // Remote log segments should be deleted after TTL
                    assertThat(remoteLogTablet.allRemoteLogSegments()).isEmpty();
                    // Remote log start offset should be reset
                    assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(Long.MAX_VALUE);
                });
    }

    @Test
    void testRemoteLogTTLWithLakeEnabled() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_remote_log_ttl_lake");
        // Create table with data lake enabled
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        // Set a short TTL for testing (2 hours)
                        .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofHours(2))
                        // Enable data lake
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();

        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);

        // Produce records to create remote log segments
        produceRecordsAndWaitRemoteLogCopy(leaderGateway, tb);

        // Verify remote log segments exist
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leaderId);
        RemoteLogManager remoteLogManager = tabletServer.getReplicaManager().getRemoteLogManager();
        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tb);

        int initialSegmentCount = remoteLogTablet.allRemoteLogSegments().size();
        assertThat(initialSegmentCount).isGreaterThan(0);

        // Verify data lake is enabled
        Replica leaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
        LogTablet logTablet = leaderReplica.getLogTablet();
        assertThat(logTablet.isDataLakeEnabled()).isTrue();

        // Record the remote log end offset before advancing time
        long remoteLogEndOffset = remoteLogTablet.getRemoteLogEndOffset().orElse(-1L);
        assertThat(remoteLogEndOffset).isGreaterThan(0L);

        // Advance time past TTL (2 hours + buffer)
        MANUAL_CLOCK.advanceTime(Duration.ofHours(2).plusMinutes(30));

        // Since lake is enabled but no data has been tiered to lake (lakeLogEndOffset = -1),
        // the expired segments should NOT be deleted
        assertThat(remoteLogTablet.allRemoteLogSegments()).hasSize(initialSegmentCount);
        assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(0L);

        // Now simulate lake tiering by updating the lake log end offset
        // This simulates that some segments have been tiered to lake
        // Use a value that will cover at least one segment but not all
        // Segments are deleted when segment.remoteLogEndOffset() <= lakeLogEndOffset
        long partialLakeOffset = remoteLogEndOffset / 2;
        // Ensure we have at least some offset to tier
        if (partialLakeOffset < 10) {
            partialLakeOffset = 10;
        }
        logTablet.updateLakeLogEndOffset(partialLakeOffset);

        final long expectedMinStartOffset = partialLakeOffset;

        // Wait for partial cleanup - only segments that have been tiered should be deleted
        retry(
                Duration.ofMinutes(2),
                () -> {
                    // Some segments should be deleted (those that have been tiered)
                    int currentSegmentCount = remoteLogTablet.allRemoteLogSegments().size();
                    assertThat(currentSegmentCount).isLessThan(initialSegmentCount);
                    // Remote log start offset should be updated to at least the tiered offset
                    assertThat(remoteLogTablet.getRemoteLogStartOffset())
                            .isGreaterThanOrEqualTo(expectedMinStartOffset);
                    // Remaining segments should have remoteLogEndOffset > partialLakeOffset
                    assertThat(remoteLogTablet.allRemoteLogSegments())
                            .allSatisfy(
                                    segment ->
                                            assertThat(segment.remoteLogEndOffset())
                                                    .isGreaterThan(expectedMinStartOffset));
                });

        // Now update lake log end offset to include all segments
        logTablet.updateLakeLogEndOffset(remoteLogEndOffset);

        // Wait for all segments to be cleaned up
        retry(
                Duration.ofMinutes(2),
                () -> {
                    // All segments should now be deleted
                    assertThat(remoteLogTablet.allRemoteLogSegments()).isEmpty();
                    assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(Long.MAX_VALUE);
                });
    }

    @Test
    void testDynamicLakeEnableAffectsTTL() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_dynamic_lake_ttl");
        // Create table without data lake enabled initially
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        // Set a short TTL for testing (1 hour)
                        .property(ConfigOptions.TABLE_LOG_TTL, Duration.ofHours(1))
                        .build();

        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);

        // Produce records to create remote log segments
        produceRecordsAndWaitRemoteLogCopy(leaderGateway, tb);

        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leaderId);
        RemoteLogManager remoteLogManager = tabletServer.getReplicaManager().getRemoteLogManager();
        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tb);

        int initialSegmentCount = remoteLogTablet.allRemoteLogSegments().size();
        assertThat(initialSegmentCount).isGreaterThan(0);

        // Record the remote log end offset
        long remoteLogEndOffset = remoteLogTablet.getRemoteLogEndOffset().orElse(-1L);
        assertThat(remoteLogEndOffset).isGreaterThan(0L);

        Replica leaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
        LogTablet logTablet = leaderReplica.getLogTablet();
        assertThat(logTablet.isDataLakeEnabled()).isFalse();

        // Dynamically enable data lake using admin API
        CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        coordinatorGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                Collections.singletonMap(
                                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true"),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();
        retry(Duration.ofMinutes(1), () -> assertThat(logTablet.isDataLakeEnabled()).isTrue());

        // Advance time past TTL (1 hour + buffer)
        MANUAL_CLOCK.advanceTime(Duration.ofHours(1).plusMinutes(30));

        // Since lake is now enabled but no data has been tiered (lakeLogEndOffset = -1),
        // the expired segments should NOT be deleted
        assertThat(remoteLogTablet.allRemoteLogSegments()).hasSize(initialSegmentCount);

        // Now disable data lake using admin API
        coordinatorGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                Collections.singletonMap(
                                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false"),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();
        retry(Duration.ofMinutes(1), () -> assertThat(logTablet.isDataLakeEnabled()).isFalse());

        // Wait for cleanup - now segments should be deleted since lake is disabled
        retry(
                Duration.ofMinutes(2),
                () -> {
                    assertThat(remoteLogTablet.allRemoteLogSegments()).isEmpty();
                    assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(Long.MAX_VALUE);
                });
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_BUCKET_NUMBER, 1);
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1kb"));

        // set a shorter max log time to allow replica shrink from isr. Don't be too low, otherwise
        // normal follower synchronization will also be affected
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));

        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        return conf;
    }
}
