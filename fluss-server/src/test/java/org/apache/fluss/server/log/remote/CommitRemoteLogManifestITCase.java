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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for commit remote log manifest. */
class CommitRemoteLogManifestITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    @Test
    void testConditionalCommitRejectsDelayedPredecessor(@TempDir Path tempDir) throws Exception {
        TablePath table = TablePath.of(DATA1_TABLE_PATH.getDatabaseName(), "conditional_manifest");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, table, DATA1_TABLE_DESCRIPTOR);
        TableBucket bucket = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);
        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(bucket);
        byte[] manifestBytes =
                new RemoteLogManifest(
                                replica.getPhysicalTablePath(), bucket, Collections.emptyList())
                        .toJsonBytes();
        Path first = tempDir.resolve("first.manifest");
        Path second = tempDir.resolve("second.manifest");
        Files.write(first, manifestBytes);
        Files.write(second, manifestBytes);
        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        CommitRemoteLogManifestRequest firstRequest =
                new CommitRemoteLogManifestRequest()
                        .setTableId(tableId)
                        .setBucketId(0)
                        .setCoordinatorEpoch(replica.getCoordinatorEpoch())
                        .setBucketLeaderEpoch(replica.getBucketEpoch())
                        .setRemoteLogStartOffset(Long.MAX_VALUE)
                        .setRemoteLogEndOffset(-1)
                        .setHighestCopiedEndOffset(-1)
                        .setRemoteLogManifestPath(first.toString())
                        .setExpectedManifestPath("");
        assertThat(gateway.commitRemoteLogManifest(firstRequest).get().isCommitSuccess()).isTrue();
        // Retrying a successful request must remain successful and keep the existing notify path.
        assertThat(gateway.commitRemoteLogManifest(firstRequest).get().isCommitSuccess()).isTrue();
        CommitRemoteLogManifestRequest successor =
                new CommitRemoteLogManifestRequest()
                        .setTableId(tableId)
                        .setBucketId(0)
                        .setCoordinatorEpoch(replica.getCoordinatorEpoch())
                        .setBucketLeaderEpoch(replica.getBucketEpoch())
                        .setRemoteLogStartOffset(Long.MAX_VALUE)
                        .setRemoteLogEndOffset(-1)
                        .setHighestCopiedEndOffset(-1)
                        .setRemoteLogManifestPath(second.toString())
                        .setExpectedManifestPath(first.toString());
        assertThat(gateway.commitRemoteLogManifest(successor).get().isCommitSuccess()).isTrue();
        assertThat(gateway.commitRemoteLogManifest(firstRequest).get().isCommitSuccess()).isFalse();
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getRemoteLogManifestHandle(bucket)
                                .get()
                                .getRemoteLogManifestPath())
                .isEqualTo(new FsPath(second.toString()));
    }

    @Test
    void testDeleteOutOfSyncReplicaLogAfterCommit() throws Exception {
        // then create a table with 3 buckets
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);

        // find the tb whose leader is the server with large log tiering interval.
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        int leader =
                Objects.requireNonNull(
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb).getLeaderId());
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        // produce many records to trigger remote log copy.
        for (int i = 0; i < 3; i++) {
            assertProduceLogResponse(
                    leaderGateWay
                            .produceLog(
                                    newProduceLogRequest(
                                            tableId, 0, -1, genMemoryLogRecordsByObject(DATA1)))
                            .get(),
                    0,
                    i * 10L);
        }

        // stop replicas to mock followers are out of sync
        List<Integer> stopFollowers =
                Stream.of(0, 1, 2).filter(i -> i != leader).collect(Collectors.toList());
        for (int stopFollower : stopFollowers) {
            FLUSS_CLUSTER_EXTENSION.stopReplica(stopFollower, tb, 1);
        }
        leaderGateWay
                .produceLog(
                        newProduceLogRequest(tableId, 0, -1, genMemoryLogRecordsByObject(DATA1)))
                .get();
        for (int stopFollower : stopFollowers) {
            FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(tb, stopFollower);
            LogTablet stopfollowerLogTablet =
                    FLUSS_CLUSTER_EXTENSION
                            .waitAndGetFollowerReplica(tb, stopFollower)
                            .getLogTablet();
            assertThat(stopfollowerLogTablet.logSegments()).hasSize(3);
        }

        // restart the leader server with a small log tiering interval
        FLUSS_CLUSTER_EXTENSION.restartTabletServer(
                leader,
                new Configuration()
                        .set(
                                ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION,
                                Duration.ofMillis(1)));
        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(tb);

        // check only has two remote log segments for the stopped replicas
        for (int stopFollower : stopFollowers) {
            LogTablet stopfollowerLogTablet =
                    FLUSS_CLUSTER_EXTENSION
                            .waitAndGetFollowerReplica(tb, stopFollower)
                            .getLogTablet();
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(stopfollowerLogTablet.logSegments()).hasSize(2));
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a larger interval for testing purpose
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofDays(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1b"));

        // set a shorter max log time to allow replica shrink from isr
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(2));
        return conf;
    }
}
