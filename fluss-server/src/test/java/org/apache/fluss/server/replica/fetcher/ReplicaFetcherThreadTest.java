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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

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

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ReplicaFetcherThread}. */
public class ReplicaFetcherThreadTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;
    private ManualClock manualClock;
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
        manualClock = new ManualClock(System.currentTimeMillis());
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
    // https://github.com/apache/fluss/issues/673
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

    @Test
    void testAppendAsFollowerThrowOutOfOrderSequenceException() throws Exception {
        Replica followerReplica = followerRM.getReplicaOrException(tb);

        long writerId = 101;
        CompletableFuture<List<ProduceLogResultForBucket>> future;

        // 1. append 2 batches to leader with (writerId=101L, batchSequence=0 offset=0L)
        future = new CompletableFuture<>();
        leaderRM.appendRecordsToLog(
                1000,
                1,
                Collections.singletonMap(
                        tb, genMemoryLogRecordsWithWriterId(DATA1, writerId, 0, 0)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 0L, 10L));

        // 2. append the first batch to follower with (writerId=101L, batchSequence=0 offset=0L) to
        // mock
        // follower have already fetched one batch.
        followerReplica.appendRecordsToFollower(
                genMemoryLogRecordsWithWriterId(DATA1, writerId, 0, 0));
        assertThat(followerReplica.getLocalLogEndOffset()).isEqualTo(10L);

        // advance time
        manualClock.advanceTime(Duration.ofHours(13));

        // 3. append the second batch to leader with (writerId=101L, batchSequence=1 offset=10L)
        future = new CompletableFuture<>();
        leaderRM.appendRecordsToLog(
                1000,
                1,
                Collections.singletonMap(
                        tb, genMemoryLogRecordsWithWriterId(DATA1, writerId, 1, 0)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 10L, 20L));

        // 3. mock remove expired writer, writerId=101 will be removed.
        assertThat(followerReplica.getLogTablet().writerStateManager().activeWriters().size())
                .isEqualTo(1);
        followerReplica.getLogTablet().removeExpiredWriter(manualClock.milliseconds());
        assertThat(followerReplica.getLogTablet().writerStateManager().activeWriters().size())
                .isEqualTo(0);

        // 4. begin fetcher thread.
        followerFetcher.addBuckets(
                Collections.singletonMap(
                        tb,
                        new InitialFetchStatus(
                                DATA1_TABLE_ID, DATA1_TABLE_PATH, leader.id(), 10L)));
        followerFetcher.start();
        // fetcher will force append the second batch to follower
        retry(
                Duration.ofSeconds(20),
                () -> assertThat(followerReplica.getLocalLogEndOffset()).isEqualTo(20L));

        // 5. mock new batch to leader with (writerId=101L, batchSequence=2 offset=20L)
        future = new CompletableFuture<>();
        leaderRM.appendRecordsToLog(
                1000,
                1,
                Collections.singletonMap(
                        tb, genMemoryLogRecordsWithWriterId(DATA1, writerId, 2, 0)),
                future::complete);
        assertThat(future.get()).containsOnly(new ProduceLogResultForBucket(tb, 20L, 30L));
        // now fetcher will work well since the state of writerId=101 is established
        retry(
                Duration.ofSeconds(20),
                () -> assertThat(followerReplica.getLocalLogEndOffset()).isEqualTo(30L));
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
        conf.set(ConfigOptions.WRITER_ID_EXPIRATION_TIME, Duration.ofHours(12));
        Scheduler scheduler = new FlussScheduler(2);
        scheduler.startup();

        LogManager logManager =
                LogManager.create(
                        conf,
                        zkClient,
                        scheduler,
                        manualClock,
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        logManager.startup();
        ReplicaManager replicaManager =
                new TestingReplicaManager(
                        conf,
                        scheduler,
                        logManager,
                        null,
                        zkClient,
                        serverId,
                        new TabletServerMetadataCache(
                                new MetadataManager(
                                        null,
                                        conf,
                                        new LakeCatalogDynamicLoader(conf, null, true))),
                        RpcClient.create(conf, TestingClientMetricGroup.newInstance(), false),
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        manualClock);
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
