/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
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

    private TableBucket setupTableBucket() throws Exception {
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);
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
        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(
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
                        Collections.singletonMap(tb, new FetchData(tableId, 0, 10240)),
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

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);
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

        FLUSS_CLUSTER_EXTENSION.waitUtilReplicaShrinkFromIsr(tb, follower);
        FLUSS_CLUSTER_EXTENSION.waitUtilSomeLogSegmentsCopyToRemote(tb);

        // restart follower
        FLUSS_CLUSTER_EXTENSION.startTabletServer(follower);
        FLUSS_CLUSTER_EXTENSION.waitUtilReplicaExpandToIsr(tb, follower);
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
        return conf;
    }
}
