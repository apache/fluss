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
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
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
