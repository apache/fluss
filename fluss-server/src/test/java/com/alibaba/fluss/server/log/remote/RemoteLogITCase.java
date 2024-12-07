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

package com.alibaba.fluss.server.log.remote;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.server.entity.FetchData;
import com.alibaba.fluss.server.log.FetchParams;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for remote log. */
public class RemoteLogITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private TableBucket setupTestEnvironment() throws Exception {
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        DATA1_TABLE_PATH,
                        DATA1_TABLE_INFO.getTableDescriptor());
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);
        return tb;
    }

    private void produceTestRecords(TabletServerGateway leaderGateWay, TableBucket tb, int count)
            throws Exception {
        for (int i = 0; i < count; i++) {
            assertProduceLogResponse(
                    leaderGateWay
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
    }

    @Test
    void testCreateAndUploadRemoteLog() throws Exception {
        TableBucket tb = setupTestEnvironment();
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // Produce records and wait for remote copy
        produceTestRecords(leaderGateWay, tb, 5);
        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(tb);

        // Verify remote files
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader);
        FsPath fsPath =
                FlussPaths.remoteLogTabletDir(
                        tabletServer.getReplicaManager().getRemoteLogManager().remoteLogDir(),
                        PhysicalTablePath.of(DATA1_TABLE_PATH),
                        tb);
        FileSystem fileSystem = fsPath.getFileSystem();

        assertThat(fileSystem.exists(fsPath)).isTrue();
        assertThat(fileSystem.listStatus(fsPath).length).isGreaterThan(0);
    }

    @Test
    void testDownloadRemoteLog() throws Exception {
        TableBucket tb = setupTestEnvironment();
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader);

        // Produce records and wait for remote copy
        produceTestRecords(leaderGateWay, tb, 10);
        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(tb);

        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();
        tabletServer
                .getReplicaManager()
                .fetchLogRecords(
                        new FetchParams(-1, Integer.MAX_VALUE),
                        Collections.singletonMap(
                                tb, new FetchData(tb.getTableId(), 0L, 1024 * 1024)),
                        future::complete);

        Map<TableBucket, FetchLogResultForBucket> result = future.get();
        assertThat(result).hasSize(1);

        FetchLogResultForBucket resultForBucket = result.get(tb);
        assertThat(resultForBucket.getError()).isEqualTo(ApiError.NONE);
        assertThat(resultForBucket.fetchFromRemote()).isTrue();
        assertThat(resultForBucket.getHighWatermark()).isGreaterThan(0L);
    }

    @Test
    void testDeleteRemoteLog() throws Exception {
        TableBucket tb = setupTestEnvironment();
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // Produce records and wait for remote copy
        produceTestRecords(leaderGateWay, tb, 10);
        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(tb);

        // Verify remote files exist
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader);
        FsPath fsPath =
                FlussPaths.remoteLogTabletDir(
                        tabletServer.getReplicaManager().getRemoteLogManager().remoteLogDir(),
                        PhysicalTablePath.of(DATA1_TABLE_PATH),
                        tb);
        FileSystem fileSystem = fsPath.getFileSystem();
        assertThat(fileSystem.exists(fsPath)).isTrue();
        assertThat(fileSystem.listStatus(fsPath).length).isGreaterThan(1);

        // Delete table and verify cleanup
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

    @Test
    void testFollowerFetchAlreadyMoveToRemoteLog() throws Exception {
        TableBucket tb = setupTestEnvironment();
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        int follower = leader == 0 ? 1 : 0;

        // Stop follower
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(follower);

        // Produce records and wait for remote copy
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        produceTestRecords(leaderGateWay, tb, 10);

        FLUSS_CLUSTER_EXTENSION.waitUtilReplicaShrinkFromIsr(tb, follower);
        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(tb);

        // Restart follower and verify recovery
        FLUSS_CLUSTER_EXTENSION.startTabletServer(follower);
        FLUSS_CLUSTER_EXTENSION.waitUtilReplicaExpandToIsr(tb, follower);
    }

    @Test
    void testRemoteLogMetadataUpdate() throws Exception {
        TableBucket tb = setupTestEnvironment();
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // Produce records and wait for remote copy
        produceTestRecords(leaderGateWay, tb, 5);
        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(tb);

        // Verify metadata
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leader);
        RemoteLogManager remoteLogManager = tabletServer.getReplicaManager().getRemoteLogManager();
        RemoteLogTablet remoteLog = remoteLogManager.remoteLogTablet(tb);

        assertThat(remoteLog.allRemoteLogSegments()).isNotEmpty();
        assertThat(remoteLog.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLog.getRemoteLogEndOffset()).isPresent();

        // Verify manifest
        RemoteLogManifest manifest = remoteLog.currentManifest();
        assertThat(manifest.getRemoteLogSegmentList()).isNotEmpty();
        assertThat(manifest.getRemoteLogSegmentList())
                .containsExactlyInAnyOrderElementsOf(remoteLog.allRemoteLogSegments());
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_BUCKET_NUMBER, 1);
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        return conf;
    }
}
