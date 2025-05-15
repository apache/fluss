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

package com.alibaba.fluss.server.replica.fetcher;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.entity.ProduceLogResultForBucket;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.server.coordinator.TestCoordinatorGateway;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import com.alibaba.fluss.server.kv.KvManager;
import com.alibaba.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import com.alibaba.fluss.server.log.LogManager;
import com.alibaba.fluss.server.metadata.TabletServerMetadataCache;
import com.alibaba.fluss.server.metrics.group.TabletServerMetricGroup;
import com.alibaba.fluss.server.metrics.group.TestingMetricGroups;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.utils.clock.Clock;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.FlussScheduler;
import com.alibaba.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static com.alibaba.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static com.alibaba.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ReplicaFetcherThread}. */
public class ReplicaFetcherThreadTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;
    private @TempDir File tempDir;
    private TableBucket tb;
    private final int leaderServerId = 1;
    private final int followerServerId = 2;
    private ReplicaManager leaderRM;
    private ServerNode leader;
    private ReplicaManager followerRM;
    private ReplicaFetcherThread followerFetcher;

    @BeforeAll
    static void baseBeforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    public void setup() throws Exception {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
        Configuration conf = new Configuration();
        tb = new TableBucket(DATA1_TABLE_ID, 0);
        leaderRM = createReplicaManager(leaderServerId);
        followerRM = createReplicaManager(followerServerId);
        // with local test leader end point.
        leader =
                new ServerNode(
                        leaderServerId, "localhost", 9099, ServerType.TABLET_SERVER, "rack1");
        ServerNode follower =
                new ServerNode(
                        followerServerId, "localhost", 10001, ServerType.TABLET_SERVER, "rack2");
        followerFetcher =
                new ReplicaFetcherThread(
                        "test-fetcher-thread",
                        followerRM,
                        new TestingLeaderEndpoint(conf, leaderRM, follower),
                        1000);

        registerTableInZkClient();
        // make the tb(table, 0) to be leader in leaderRM and to be follower in followerRM.
        makeLeaderAndFollower();
    }

    @Test
    void testSimpleFetch() throws Exception {
        // append records to leader.
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        leaderRM.appendRecordsToLog(
                1000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));

        followerFetcher.addBuckets(
                Collections.singletonMap(
                        tb,
                        new InitialFetchStatus(DATA1_TABLE_ID, DATA1_TABLE_PATH, leader.id(), 0L)));
        assertThat(followerRM.getReplicaOrException(tb).getLocalLogEndOffset()).isEqualTo(0L);

        // begin fetcher thread.
        followerFetcher.start();
        retry(
                Duration.ofSeconds(20),
                () ->
                        assertThat(followerRM.getReplicaOrException(tb).getLocalLogEndOffset())
                                .isEqualTo(10L));

        // append again.
        future = new CompletableFuture<>();
        leaderRM.appendRecordsToLog(
                1000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 10L, 20L));
        retry(
                Duration.ofSeconds(20),
                () ->
                        assertThat(followerRM.getReplicaOrException(tb).getLocalLogEndOffset())
                                .isEqualTo(20L));
    }

    @Test
    void testFollowerHighWatermarkHigherThanOrEqualToLeader() throws Exception {
        Replica leaderReplica = leaderRM.getReplicaOrException(tb);
        Replica followerReplica = followerRM.getReplicaOrException(tb);

        followerFetcher.addBuckets(
                Collections.singletonMap(
                        tb,
                        new InitialFetchStatus(DATA1_TABLE_ID, DATA1_TABLE_PATH, leader.id(), 0L)));
        assertThat(leaderReplica.getLocalLogEndOffset()).isEqualTo(0L);
        assertThat(leaderReplica.getLogHighWatermark()).isEqualTo(0L);
        assertThat(followerReplica.getLocalLogEndOffset()).isEqualTo(0L);
        assertThat(followerReplica.getLogHighWatermark()).isEqualTo(0L);
        // begin fetcher thread.
        followerFetcher.start();

        CompletableFuture<List<ProduceLogResultForBucket>> future;
        for (int i = 0; i < 1000; i++) {
            long baseOffset = i * 10L;
            future = new CompletableFuture<>();
            leaderRM.appendRecordsToLog(
                    1000,
                    1, // don't wait ack
                    Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                    future::complete);
            assertThat(future.get())
                    .containsOnly(new ProduceLogResultForBucket(tb, baseOffset, baseOffset + 10L));
            retry(
                    Duration.ofSeconds(20),
                    () ->
                            assertThat(followerReplica.getLocalLogEndOffset())
                                    .isEqualTo(baseOffset + 10L));
            assertThat(followerReplica.getLogHighWatermark())
                    .isGreaterThanOrEqualTo(leaderReplica.getLogHighWatermark());
        }
    }

    // TODO this test need to be removed after we introduce leader epoch cache. Trace by
    // https://github.com/alibaba/fluss/issues/673
    @Test
    void testAppendAsFollowerThrowDuplicatedBatchException() throws Exception {
        Replica leaderReplica = leaderRM.getReplicaOrException(tb);
        Replica followerReplica = followerRM.getReplicaOrException(tb);

        // 1. append same batches to leader and follower with different writer id.
        CompletableFuture<List<ProduceLogResultForBucket>> future;
        List<Long> writerIds = Arrays.asList(100L, 101L);
        long baseOffset = 0L;
        for (long writerId : writerIds) {
            for (int i = 0; i < 5; i++) {
                future = new CompletableFuture<>();
                leaderRM.appendRecordsToLog(
                        1000,
                        1,
                        Collections.singletonMap(
                                tb, genMemoryLogRecordsWithWriterId(DATA1, writerId, i, 0)),
                        future::complete);
                assertThat(future.get())
                        .containsOnly(
                                new ProduceLogResultForBucket(tb, baseOffset, baseOffset + 10L));

                followerReplica.appendRecordsToFollower(
                        genMemoryLogRecordsWithWriterId(DATA1, writerId, i, baseOffset));
                assertThat(followerReplica.getLocalLogEndOffset()).isEqualTo(baseOffset + 10L);
                baseOffset = baseOffset + 10L;
            }
        }

        // 2. append one batch to follower with (writerId=100L, batchSequence=5 offset=100L) to mock
        // follower have one batch ahead of leader.
        followerReplica.appendRecordsToFollower(
                genMemoryLogRecordsWithWriterId(DATA1, 100L, 5, 100L));
        assertThat(followerReplica.getLocalLogEndOffset()).isEqualTo(110L);

        // 3. mock becomeLeaderAndFollower as follower end.
        leaderReplica.updateLeaderEndOffsetSnapshot();
        followerFetcher.addBuckets(
                Collections.singletonMap(
                        tb,
                        new InitialFetchStatus(
                                DATA1_TABLE_ID, DATA1_TABLE_PATH, leader.id(), 110L)));
        followerFetcher.start();

        // 4. mock append to leader with different writer id (writerId=101L, batchSequence=5
        // offset=100L) to mock leader receive different batch from recovery follower.
        future = new CompletableFuture<>();
        leaderRM.appendRecordsToLog(
                1000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsWithWriterId(DATA1, 101L, 5, 100L)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 100L, 110L));

        // 5. mock append to leader with (writerId=100L, batchSequence=5 offset=110L) to mock
        // follower fetch duplicated batch from leader. In this case follower will truncate to
        // LeaderEndOffsetSnapshot and fetch again.
        future = new CompletableFuture<>();
        leaderRM.appendRecordsToLog(
                1000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsWithWriterId(DATA1, 100L, 5, 110L)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 110L, 120L));
        retry(
                Duration.ofSeconds(20),
                () -> assertThat(followerReplica.getLocalLogEndOffset()).isEqualTo(120L));
    }

    private void registerTableInZkClient() throws Exception {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
        zkClient.registerTable(
                DATA1_TABLE_PATH,
                TableRegistration.newTable(DATA1_TABLE_ID, DATA1_TABLE_DESCRIPTOR));
        zkClient.registerSchema(DATA1_TABLE_PATH, DATA1_SCHEMA);
    }

    private void makeLeaderAndFollower() {
        leaderRM.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                Arrays.asList(leaderServerId, followerServerId),
                                new LeaderAndIsr(
                                        leaderServerId,
                                        INITIAL_LEADER_EPOCH,
                                        Arrays.asList(leaderServerId, followerServerId),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH))),
                result -> {});
        followerRM.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                Arrays.asList(leaderServerId, followerServerId),
                                new LeaderAndIsr(
                                        leaderServerId,
                                        INITIAL_LEADER_EPOCH,
                                        Arrays.asList(leaderServerId, followerServerId),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH))),
                result -> {});
    }

    private ReplicaManager createReplicaManager(int serverId) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.DATA_DIR, tempDir.getAbsolutePath() + "/server-" + serverId);
        Scheduler scheduler = new FlussScheduler(2);
        scheduler.startup();

        LogManager logManager =
                LogManager.create(conf, zkClient, scheduler, SystemClock.getInstance());
        logManager.startup();
        ReplicaManager replicaManager =
                new TestingReplicaManager(
                        conf,
                        scheduler,
                        logManager,
                        null,
                        zkClient,
                        serverId,
                        new TabletServerMetadataCache(),
                        RpcClient.create(conf, TestingClientMetricGroup.newInstance()),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        SystemClock.getInstance());
        replicaManager.startup();
        return replicaManager;
    }

    /**
     * TestingReplicaManager only for ReplicaFetcherThreadTest. override becomeLeaderOrFollower to
     * make sure that no fetcher task will be added to the ReplicaFetcherThread.
     */
    private static class TestingReplicaManager extends ReplicaManager {
        public TestingReplicaManager(
                Configuration conf,
                Scheduler scheduler,
                LogManager logManager,
                KvManager kvManager,
                ZooKeeperClient zkClient,
                int serverId,
                TabletServerMetadataCache metadataCache,
                RpcClient rpcClient,
                TabletServerMetricGroup serverMetricGroup,
                Clock clock)
                throws IOException {
            super(
                    conf,
                    scheduler,
                    logManager,
                    kvManager,
                    zkClient,
                    serverId,
                    metadataCache,
                    rpcClient,
                    new TestCoordinatorGateway(),
                    new TestingCompletedKvSnapshotCommitter(),
                    NOPErrorHandler.INSTANCE,
                    serverMetricGroup,
                    clock);
        }

        @Override
        public void becomeLeaderOrFollower(
                int requestCoordinatorEpoch,
                List<NotifyLeaderAndIsrData> notifyLeaderAndIsrDataList,
                Consumer<List<NotifyLeaderAndIsrResultForBucket>> responseCallback) {
            for (NotifyLeaderAndIsrData data : notifyLeaderAndIsrDataList) {
                Optional<Replica> replicaOpt = maybeCreateReplica(data);
                if (replicaOpt.isPresent() && data.getReplicas().contains(serverId)) {
                    Replica replica = replicaOpt.get();
                    int leaderId = data.getLeader();
                    if (leaderId == serverId) {
                        try {
                            replica.makeLeader(data);
                        } catch (IOException e) {
                            throw new FlussRuntimeException(e);
                        }
                    } else {
                        replica.makeFollower(data);
                    }
                }
            }
        }
    }
}
