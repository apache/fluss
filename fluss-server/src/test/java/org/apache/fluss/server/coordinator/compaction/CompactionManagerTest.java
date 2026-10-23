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

package org.apache.fluss.server.coordinator.compaction;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.compaction.CompactionInfo;
import org.apache.fluss.cluster.compaction.CompactionProgress;
import org.apache.fluss.cluster.compaction.CompactionStatus;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.CompactRequest;
import org.apache.fluss.rpc.messages.ListCompactionsRequest;
import org.apache.fluss.rpc.messages.PbBucketCompactionStatus;
import org.apache.fluss.rpc.messages.StartBucketCompactionRequest;
import org.apache.fluss.rpc.messages.StartBucketCompactionResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Admission and durable per-server ordering with real task storage. */
class CompactionManagerTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @TempDir Path directory;
    private ZooKeeperClient zk;
    private CompactionStore store;
    private CoordinatorContext context;
    private CompactionManager manager;
    private RpcClient rpc;
    private boolean failZkWrites;
    private final FailingFileSystem fs = new FailingFileSystem();
    private final Configuration config = new Configuration();
    private final ManualClock clock = new ManualClock();
    private final ManualExecutor events = new ManualExecutor();
    private final ManualExecutor io = new ManualExecutor();
    private final List<StartBucketCompactionRequest> starts = new ArrayList<>();
    private final List<CompletableFuture<ApiMessage>> replies = new ArrayList<>();

    @BeforeEach
    void setup() throws Exception {
        zk =
                org.mockito.Mockito.spy(
                        ZK.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE));
        org.mockito.Mockito.doAnswer(
                        call -> {
                            if (failZkWrites) {
                                throw new org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper
                                        .KeeperException.ConnectionLossException();
                            }
                            return call.callRealMethod();
                        })
                .when(zk)
                .wrapRequestsWithEpochCheck(
                        org.mockito.ArgumentMatchers.anyList(),
                        org.mockito.ArgumentMatchers.anyInt());
        context = new CoordinatorContext(zk.fenceBecomeCoordinatorLeader("test"));
        store =
                new CompactionStore(
                        zk, directory.toString(), fs, 64 * 1024, 256 * 1024, 512L * 1024 * 1024);
        for (int id : Arrays.asList(1, 2)) {
            context.addLiveTabletServer(
                    new ServerInfo(
                            id,
                            null,
                            Collections.singletonList(
                                    new Endpoint("localhost", 9000 + id, "INTERNAL")),
                            ServerType.TABLET_SERVER));
        }
        rpc = mock(RpcClient.class);
        when(rpc.sendRequest(any(), any(), any()))
                .thenAnswer(
                        call -> {
                            if (call.getArgument(1) == ApiKeys.START_BUCKET_COMPACTION) {
                                StartBucketCompactionRequest request = call.getArgument(2);
                                CompactionTask persisted = store.load(request.getOperationId());
                                assertThat(persisted.planHandle()).isNotNull();
                                assertThat(persisted.info().status())
                                        .isEqualTo(CompactionStatus.RUNNING);
                                starts.add(
                                        new StartBucketCompactionRequest()
                                                .copyFrom(call.getArgument(2)));
                                CompletableFuture<ApiMessage> result = new CompletableFuture<>();
                                replies.add(result);
                                return result;
                            }
                            CompletableFuture<ApiMessage> response = new CompletableFuture<>();
                            return response;
                        });
        manager =
                new CompactionManager(
                        context,
                        store,
                        zk,
                        rpc,
                        "INTERNAL",
                        () -> config,
                        events,
                        io,
                        mock(Scheduler.class),
                        clock,
                        86400000,
                        1000);
        drain();
    }

    @AfterEach
    void cleanup() {
        manager.close();
        events.runAll();
        ZK.getCustomExtension().cleanupRoot();
    }

    @Test
    void testFixedPlanAndExactActiveDedup() throws Exception {
        table(4, "orders", true, true);
        bucket(new TableBucket(4, 20L, 0), 1, "day2");
        bucket(new TableBucket(4, 10L, 1), 1, "day1");
        String id = submit(request("orders"));
        CompactionTask task = store.load(id);
        assertThat(store.readPlan(task.planHandle()).buckets())
                .containsExactly(new TableBucket(4, 10L, 1), new TableBucket(4, 20L, 0));
        assertThat(submit(request("orders"))).isEqualTo(id);
        bucket(new TableBucket(4, 30L, 0), 2, "day3");
        CompletableFuture<String> expanded = manager.compact(request("orders"));
        drain();
        assertThatThrownBy(expanded::join)
                .hasRootCauseMessage("Compaction already in progress: " + id);
        assertThat(store.readPlan(store.load(id).planHandle()).buckets()).hasSize(2);
        CompletableFuture<String> other = manager.compact(request("orders").setType("RECLAIM"));
        drain();
        assertThatThrownBy(other::join)
                .hasRootCauseMessage("Compaction already in progress: " + id);
        table(5, "orders", true, true);
        bucket(new TableBucket(5, 40L, 0), 2, "day4");
        CompletableFuture<String> recreated = manager.compact(request("orders"));
        drain();
        assertThatThrownBy(recreated::join)
                .hasRootCauseMessage("Compaction already in progress: " + id);
    }

    @Test
    void testPerServerOrderingWaitsForPublishedSnapshot() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 2), 1, null);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 2, null);
        String id = submit(request("orders"));
        assertThat(starts).hasSize(2);
        assertThat(starts.get(0).getTableBucket().getBucketId()).isZero();
        assertThat(starts.get(1).getTableBucket().getBucketId()).isEqualTo(1);
        assertThat(store.load(id).progressHandle()).isNull();
        replies.get(1).complete(success(starts.get(1), 11));
        drain();
        cycle();
        assertThat(progress(id).succeeded()).isEqualTo(1);
        assertThat(starts).hasSize(2);
        replies.get(0).complete(success(starts.get(0), 12));
        drain();
        assertThat(starts).hasSize(2);
        assertThat(progress(id).succeeded()).isEqualTo(1);
        clock.advanceTime(1000, TimeUnit.MILLISECONDS);
        manager.controlCycle();
        events.runAll();
        assertThat(starts).hasSize(2);
        assertThat(progress(id).currentExecutions())
                .singleElement()
                .satisfies(
                        execution -> {
                            assertThat(execution.snapshotId()).hasValue(12L);
                        });
        drain();
        assertThat(starts).hasSize(2);
        cycle();
        assertThat(starts).hasSize(3);
        assertThat(starts.get(2).getTableBucket().getBucketId()).isEqualTo(2);
        replies.get(2).complete(success(starts.get(2), 13));
        drain();
        cycle();
        CompactionProgress done = progress(id);
        assertThat(done.info().status()).isEqualTo(CompactionStatus.SUCCEEDED);
        assertThat(done.succeeded()).isEqualTo(done.total());
        assertCounts(done);
        assertThat(zk.getCompactionActive().data()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"Tablet server busy", "Disk protection active", "Write backpressure active"})
    void testFailureAndCancellationCountsIgnoreLateResults(String reason) throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 1, null);
        String id = submit(request("orders"));
        replies.get(0).completeExceptionally(new IllegalStateException(reason));
        drain();
        CompactionProgress failed = progress(id);
        assertThat(failed.info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(failed.firstError()).hasValue("java.lang.IllegalStateException: " + reason);
        assertThat(failed.failed()).isEqualTo(2);
        assertThat(failed.cancelled()).isZero();
        assertCounts(failed);
        String next = submit(request("orders"));
        CompletableFuture<Void> cancellation = manager.cancelCompaction(next);
        drain();
        cancellation.join();
        replies.get(1).complete(success(starts.get(1), 99));
        drain();
        manager.cancelCompaction(next);
        drain();
        CompactionProgress cancelled = progress(next);
        assertThat(cancelled.info().status()).isEqualTo(CompactionStatus.CANCELLED);
        assertThat(cancelled.cancelled()).isEqualTo(cancelled.total());
        assertCounts(cancelled);
    }

    @Test
    void testClusterSkipsLogTablesAndRejectsUnsupportedPrimaryKeyTables() throws Exception {
        table(1, "orders", true, false);
        table(2, "log", false, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(2, 0), 2, null);
        String id = submit(new CompactRequest().setType("OPTIMIZE"));
        assertThat(progress(id).total()).isEqualTo(1);
        manager.cancelCompaction(id);
        drain();
        table(3, "history", true, true);
        bucket(new TableBucket(3, 30L, 0), 2, "__historical__");
        CompletableFuture<String> rejected =
                manager.compact(new CompactRequest().setType("OPTIMIZE"));
        drain();
        assertThatThrownBy(rejected::join).hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(zk.getCompactionActive().data()).isEmpty();
    }

    @Test
    void testConfigurationChangeAndMissingLeaderFailWithoutWaiting() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        config.set(
                ConfigOptions.KV_PARTITIONED_INDEX_FILTERS,
                !config.get(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS));
        manager.controlCycle();
        drain();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.FAILED);
        context.putBucketLeaderAndIsr(new TableBucket(1, 0), new LeaderAndIsr(-1, 0));
        String next = submit(request("orders"));
        assertThat(progress(next).info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(progress(next).failed()).isEqualTo(progress(next).total());
        assertThat(progress(next).cancelled()).isZero();
        assertCounts(progress(next));
        assertThat(starts).hasSize(1);
    }

    @Test
    void testUnchangedCyclesDoNotPublishAndConcurrentResultsAreCoalesced() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 2, null);
        String id = submit(request("orders"));
        int version = store.load(id).version();
        cycle();
        assertThat(store.load(id).version()).isEqualTo(version);
        StartBucketCompactionResponse first = success(starts.get(0), 10);
        first.getStatus().setReason("Snapshot committed");
        replies.get(0).complete(first);
        drain();
        assertThat(store.load(id).version()).isEqualTo(version);
        assertThat(io.tasks).isEmpty();
        replies.get(1).complete(success(starts.get(1), 11));
        drain();
        assertThat(store.load(id).version()).isEqualTo(version);
        assertThat(progress(id).succeeded()).isZero();
        clock.advanceTime(1000, TimeUnit.MILLISECONDS);
        manager.controlCycle();
        events.runAll();
        assertThat(io.tasks).hasSize(1);
        assertThat(progress(id).currentExecutions()).hasSize(2);
        CompactionProgress.Execution execution = progress(id).currentExecutions().get(0);
        assertThat(execution.snapshotId()).hasValue(10L);
        assertThat(execution.reason()).hasValue("Snapshot committed");
        assertThat(progress(id).succeeded()).isZero();
        drain();
        assertThat(store.load(id).version()).isEqualTo(version + 1);
        assertThat(store.readProgress(store.load(id).progressHandle(), store.load(id).succeeded()))
                .hasSize(2);
        assertThat(progress(id).succeeded()).isEqualTo(2);
        assertThat(progress(id).currentExecutions()).isEmpty();
        assertCounts(progress(id));
    }

    @ParameterizedTest
    @ValueSource(strings = {"CANCELLED", "FAILED"})
    void testTerminationReachesWorkersOnlyAfterTerminalPublication(String state) throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 2, null);
        String id = submit(request("orders"));
        replies.get(0).complete(success(starts.get(0), 10));
        drain();
        CompletableFuture<Void> cancellation = null;
        if (state.equals("CANCELLED")) {
            cancellation = manager.cancelCompaction(id);
        } else {
            replies.get(1).completeExceptionally(new IllegalStateException("compaction failed"));
        }
        events.runAll();
        if (cancellation != null) {
            assertThat(cancellation).isNotDone();
        }
        assertThat(store.load(id).isTerminal()).isFalse();
        verify(rpc, times(0))
                .sendRequest(
                        any(),
                        org.mockito.ArgumentMatchers.eq(ApiKeys.CANCEL_BUCKET_COMPACTION),
                        any());
        drain();
        if (cancellation != null) {
            assertThat(cancellation).isCompleted();
        }
        assertThat(store.load(id).info().status()).isEqualTo(CompactionStatus.valueOf(state));
        assertThat(store.load(id).succeeded()).isZero();
        assertThat(store.load(id).failed()).isEqualTo(state.equals("FAILED") ? 2 : 0);
        assertThat(store.load(id).cancelled()).isEqualTo(state.equals("CANCELLED") ? 2 : 0);
        assertThat(zk.getCompactionActive().data()).isEmpty();
        verify(rpc, times(2))
                .sendRequest(
                        any(),
                        org.mockito.ArgumentMatchers.eq(ApiKeys.CANCEL_BUCKET_COMPACTION),
                        any());
    }

    @Test
    void testExplicitPartitionAndAdmissionValidation() throws Exception {
        table(1, "orders", true, true);
        bucket(new TableBucket(1, 10L, 0), 1, "day1");
        bucket(new TableBucket(1, 20L, 0), 2, "__historical__");
        String id = submit(request("orders").setPartitionName("day1"));
        assertThat(progress(id).total()).isEqualTo(1);
        CompletableFuture<List<CompactionInfo>> listed =
                manager.listCompactions(
                        new ListCompactionsRequest().setDatabaseName("db").setTableName("orders"));
        drain();
        assertThat(listed.join())
                .extracting(info -> info.partitionName().get())
                .containsExactly("day1");
        manager.cancelCompaction(id);
        drain();
        for (CompactRequest request :
                Arrays.asList(
                        request("orders").setPartitionName("missing"),
                        request("orders").setPartitionName("__historical__"),
                        request("missing"),
                        new CompactRequest().setTableName("orders").setType("OPTIMIZE"))) {
            CompletableFuture<String> rejected = manager.compact(request);
            drain();
            assertThatThrownBy(rejected::join)
                    .hasRootCauseInstanceOf(IllegalArgumentException.class);
        }
        config.set(ConfigOptions.KV_COMPACTION_STYLE, ConfigOptions.CompactionStyle.UNIVERSAL);
        CompletableFuture<String> rejected =
                manager.compact(request("orders").setPartitionName("day1"));
        drain();
        assertThatThrownBy(rejected::join)
                .hasRootCauseMessage("Manual compaction requires LEVEL compaction style");
        assertThat(zk.getCompactionActive().data()).isEmpty();
    }

    @Test
    void testDeadlineFailsAndLateSuccessCannotChangeTerminalCounts() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        clock.advanceTime(86400000, TimeUnit.MILLISECONDS);
        manager.controlCycle();
        drain();
        CompactionProgress failed = progress(id);
        assertThat(failed.info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(failed.firstError())
                .hasValue("java.lang.IllegalStateException: Compaction deadline exceeded");
        replies.get(0).complete(success(starts.get(0), 22));
        drain();
        assertThat(progress(id).succeeded()).isZero();
        assertCounts(progress(id));
    }

    @Test
    void testPlanBudgetFailureClearsActiveWithoutDispatch() throws Exception {
        manager.close();
        events.runAll();
        store =
                new CompactionStore(
                        zk, directory.toString(), new LocalFileSystem(), 64 * 1024, 256 * 1024, 1);
        manager =
                new CompactionManager(
                        context,
                        store,
                        zk,
                        mock(RpcClient.class),
                        "INTERNAL",
                        () -> config,
                        events,
                        io,
                        mock(Scheduler.class),
                        clock,
                        86400000,
                        1000);
        drain();
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        CompletableFuture<String> rejected = manager.compact(request("orders"));
        drain();
        assertThatThrownBy(rejected::join).hasRootCauseMessage("Compaction plan budget exceeded");
        assertThat(zk.getCompactionActive().data()).isEmpty();
        assertThat(store.list())
                .singleElement()
                .satisfies(
                        task -> {
                            assertThat(task.info().status()).isEqualTo(CompactionStatus.FAILED);
                            assertThat(task.failed()).isEqualTo(task.total());
                            assertThat(task.cancelled()).isZero();
                        });
        assertThat(starts).isEmpty();
    }

    @Test
    void testFailureCountsEveryRemainingBucket() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 2, null);
        bucket(new TableBucket(1, 2), 1, null);
        String id = submit(request("orders"));
        assertThat(starts).hasSize(2);
        replies.get(0).completeExceptionally(new IllegalStateException("Disk protection active"));
        drain();
        CompactionProgress failed = progress(id);
        assertThat(failed.info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(failed.succeeded()).isZero();
        assertThat(failed.failed()).isEqualTo(3);
        assertThat(failed.cancelled()).isZero();
        assertCounts(failed);
        assertThat(store.load(id).failed()).isEqualTo(3);
    }

    @Test
    void testFailurePreservesPublishedSuccessAndFailsRemainingBuckets() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 2, null);
        bucket(new TableBucket(1, 2), 1, null);
        bucket(new TableBucket(1, 3), 1, null);
        String id = submit(request("orders"));
        replies.get(0).complete(success(starts.get(0), 31));
        drain();
        cycle();
        cycle();
        assertThat(progress(id).succeeded()).isEqualTo(1);
        assertThat(starts).hasSize(3);
        replies.get(1).completeExceptionally(new IllegalStateException("Tablet server failed"));
        drain();
        CompactionProgress failed = progress(id);
        assertThat(failed.info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(failed.succeeded()).isEqualTo(1);
        assertThat(failed.failed()).isEqualTo(3);
        assertThat(failed.cancelled()).isZero();
        assertCounts(failed);
        assertThat(store.load(id).succeeded()).isEqualTo(1);
        assertThat(store.load(id).failed()).isEqualTo(3);
    }

    @Test
    void testConfigurationChangeFailsAllRemainingBuckets() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 2, null);
        bucket(new TableBucket(1, 2), 1, null);
        String id = submit(request("orders"));
        config.set(
                ConfigOptions.KV_PARTITIONED_INDEX_FILTERS,
                !config.get(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS));
        cycle();
        CompactionProgress failed = progress(id);
        assertThat(failed.info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(failed.failed()).isEqualTo(3);
        assertThat(failed.cancelled()).isZero();
        assertCounts(failed);
    }

    @Test
    void testMissingLeaderFailsAllRemainingBuckets() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), -1, null);
        String id = submit(request("orders"));
        CompactionProgress failed = progress(id);
        assertThat(failed.info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(failed.failed()).isEqualTo(2);
        assertThat(failed.cancelled()).isZero();
        assertCounts(failed);
        assertThat(store.readProgress(store.load(id).progressHandle(), store.load(id).succeeded()))
                .isEmpty();
        assertThat(starts).hasSize(1);
    }

    @Test
    void testPublicationFailureDoesNotStartNextBucketOrCountSuccess() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 1, null);
        String id = submit(request("orders"));
        replies.get(0).complete(success(starts.get(0), 21));
        events.runAll();
        manager.controlCycle();
        events.runAll();
        zk.fenceBecomeCoordinatorLeader("replacement");
        drain();
        assertThat(starts).hasSize(1);
        assertThat(store.load(id).succeeded()).isZero();
        CompletableFuture<String> rejected = manager.compact(request("orders"));
        drain();
        assertThat(rejected).isCompletedExceptionally();
    }

    @Test
    void testRunningStatusUsesIdempotentStartWithSingleInflightRequest() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        StartBucketCompactionResponse running = success(starts.get(0), 17);
        running.getStatus().setStatus("RUNNING").setPhase("COMPACTION");
        clock.advanceTime(10, TimeUnit.MILLISECONDS);
        replies.get(0).complete(running);
        drain();
        manager.controlCycle();
        drain();
        assertThat(starts).hasSize(2);
        assertThat(progress(id).lastProgressTime()).isEqualTo(1010);
        assertThat(progress(id).persistedTime()).isZero();
        assertThat(progress(id).currentExecutions())
                .singleElement()
                .satisfies(execution -> assertThat(execution.phase()).isEqualTo("COMPACTION"));
        manager.controlCycle();
        drain();
        StartBucketCompactionResponse done = new StartBucketCompactionResponse();
        done.setStatus().copyFrom(success(starts.get(0), 17).getStatus());
        replies.get(1).complete(done);
        drain();
        cycle();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.SUCCEEDED);
        assertThat(starts).hasSize(2);
    }

    @Test
    void testRecoveryRestartsUnpublishedBuckets() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 1, null);
        bucket(new TableBucket(1, 2), 1, null);
        String id = submit(request("orders"));
        long deadline = store.load(id).deadline();
        replies.get(0).complete(success(starts.get(0), 41));
        drain();
        cycle();
        cycle();
        assertThat(starts).hasSize(2);
        assertThat(store.load(id).succeeded()).isEqualTo(1);
        replies.get(1).complete(success(starts.get(1), 42));
        drain();
        assertThat(store.load(id).succeeded()).isEqualTo(1);
        restart();
        drain();
        cycle();
        assertThat(starts).hasSize(3);
        assertThat(starts.get(2).getTableBucket().getBucketId()).isEqualTo(1);
        verify(rpc, times(0)).sendRequest(any(), eq(ApiKeys.CANCEL_BUCKET_COMPACTION), any());
        replies.get(2).complete(success(starts.get(2), 43));
        drain();
        cycle();
        cycle();
        assertThat(starts).hasSize(4);
        assertThat(starts.get(3).getTableBucket().getBucketId()).isEqualTo(2);
        replies.get(3).complete(success(starts.get(3), 44));
        drain();
        cycle();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.SUCCEEDED);
        assertThat(progress(id).succeeded()).isEqualTo(3);
        assertThat(store.load(id).deadline()).isEqualTo(deadline);
        assertThat(starts).allSatisfy(start -> assertThat(start.getDeadline()).isEqualTo(deadline));
        assertCounts(progress(id));
    }

    @Test
    void testCancellationDuringSuccessPublication() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 2, null);
        String id = submit(request("orders"));
        replies.get(0).complete(success(starts.get(0), 42));
        drain();
        clock.advanceTime(1000, TimeUnit.MILLISECONDS);
        manager.controlCycle();
        events.runAll();
        assertThat(io.tasks).hasSize(1);
        CompletableFuture<Void> cancelled = manager.cancelCompaction(id);
        events.runAll();
        drain();
        assertThat(cancelled).isCompleted();
        CompactionTask terminal = store.load(id);
        assertThat(terminal.info().status()).isEqualTo(CompactionStatus.CANCELLED);
        assertThat(terminal.succeeded()).isEqualTo(1);
        assertThat(terminal.cancelled()).isEqualTo(1);
        assertThat(store.readProgress(terminal.progressHandle(), terminal.succeeded()))
                .containsEntry(new TableBucket(1, 0), 42L);
        replies.get(1).complete(success(starts.get(1), 43));
        drain();
        cycle();
        assertThat(store.load(id).version()).isEqualTo(terminal.version());
        assertCounts(progress(id));
    }

    @Test
    void testLostStartResponseRetriesOnControlCycle() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        replies.get(0)
                .completeExceptionally(new org.apache.fluss.exception.NetworkException("lost"));
        drain();
        assertThat(starts).hasSize(1);
        cycle();
        assertThat(starts).hasSize(2);
        assertThat(starts.get(1).getOperationId()).isEqualTo(starts.get(0).getOperationId());
        assertThat(starts.get(1).getTableBucket().getTableId())
                .isEqualTo(starts.get(0).getTableBucket().getTableId());
        assertThat(starts.get(1).getTableBucket().getBucketId())
                .isEqualTo(starts.get(0).getTableBucket().getBucketId());
        assertThat(starts.get(1).getCoordinatorEpoch())
                .isEqualTo(starts.get(0).getCoordinatorEpoch());
        assertThat(starts.get(1).getLeaderEpoch()).isEqualTo(starts.get(0).getLeaderEpoch());
        assertThat(starts.get(1).getDeadline()).isEqualTo(starts.get(0).getDeadline());
        replies.get(1).complete(success(starts.get(1), 42));
        drain();
        cycle();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.SUCCEEDED);
    }

    @Test
    void testLeaderDriftReschedulesAndIgnoresOldSuccess() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        context.putBucketLeaderAndIsr(
                new TableBucket(1, 0),
                new LeaderAndIsr(
                        2,
                        99,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        context.getCoordinatorEpoch(),
                        1));
        cycle();
        assertThat(starts).hasSize(2);
        assertThat(starts.get(1).getLeaderEpoch()).isEqualTo(99);
        replies.get(0).complete(success(starts.get(0), 42));
        drain();
        assertThat(progress(id).succeeded()).isZero();
        replies.get(1).complete(success(starts.get(1), 43));
        drain();
        cycle();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.SUCCEEDED);
    }

    @Test
    void testLostStartAcknowledgementStillCancelsWorker() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        replies.get(0)
                .completeExceptionally(new org.apache.fluss.exception.NetworkException("lost"));
        drain();
        manager.cancelCompaction(id);
        drain();
        verify(rpc).sendRequest(any(), eq(ApiKeys.CANCEL_BUCKET_COMPACTION), any());
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.CANCELLED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRecoveredBusyStartWaitsForExitOrDeadline(boolean expire) throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 1, null);
        String id = submit(request("orders"));
        long deadline = starts.get(0).getDeadline();
        restart();
        drain();
        cycle();
        assertThat(starts).hasSize(2);
        // Model the recovered worker still holding its native slot until actual exit.
        for (int i = 1; i <= 2; i++) {
            replies.get(i)
                    .completeExceptionally(
                            new org.apache.fluss.exception.CompactionInProgressException("busy"));
            drain();
            assertThat(starts).hasSize(i + 1);
            assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.RUNNING);
            if (expire && i == 2) {
                clock.advanceTime(deadline - clock.milliseconds(), TimeUnit.MILLISECONDS);
            }
            cycle();
        }
        if (expire) {
            assertThat(starts).hasSize(3);
            assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.FAILED);
            cycle();
            assertThat(starts).hasSize(3);
        } else {
            assertThat(starts).hasSize(4);
            replies.get(3).complete(success(starts.get(3), 42));
            drain();
            cycle();
            assertThat(progress(id).succeeded()).isEqualTo(1);
            cycle();
            assertThat(starts.get(4).getTableBucket().getBucketId()).isEqualTo(1);
            replies.get(4).complete(success(starts.get(4), 43));
            drain();
            cycle();
            assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.SUCCEEDED);
            assertThat(progress(id).succeeded()).isEqualTo(2);
        }
        assertThat(starts.subList(0, expire ? 3 : 4))
                .allSatisfy(
                        start -> {
                            assertThat(start.getOperationId()).isEqualTo(id);
                            assertThat(start.getTableBucket().getBucketId()).isZero();
                            assertThat(start.getCoordinatorEpoch())
                                    .isEqualTo(starts.get(0).getCoordinatorEpoch());
                            assertThat(start.getLeaderEpoch())
                                    .isEqualTo(starts.get(0).getLeaderEpoch());
                            assertThat(start.getDeadline()).isEqualTo(deadline);
                        });
        assertThat(store.load(id).deadline()).isEqualTo(deadline);
        assertCounts(progress(id));
    }

    @Test
    void testObservationTransportFailureRetriesOnNextControlCycle() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        StartBucketCompactionResponse running = success(starts.get(0), 1);
        running.getStatus().setStatus("RUNNING");
        replies.get(0).complete(running);
        drain();
        cycle();
        replies.get(1)
                .completeExceptionally(new org.apache.fluss.exception.NetworkException("lost"));
        drain();
        assertThat(starts).hasSize(2);
        cycle();
        assertThat(starts).hasSize(3);
        replies.get(2).complete(success(starts.get(2), 42));
        drain();
        cycle();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.SUCCEEDED);
    }

    @Test
    void testListMatchesContainingScopesWithoutReadingPlans() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(new CompactRequest().setType("OPTIMIZE"));
        CompletableFuture<List<CompactionInfo>> result =
                manager.listCompactions(
                        new ListCompactionsRequest().setDatabaseName("db").setTableName("orders"));
        drain();
        assertThat(result.join()).extracting(CompactionInfo::operationId).containsExactly(id);
    }

    @Test
    void testGetDoesNotRescanCompletedBuckets() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 1, null);
        bucket(new TableBucket(1, 2), 1, null);
        String id = submit(request("orders"));
        clock.advanceTime(10, TimeUnit.MILLISECONDS);
        replies.get(0).complete(success(starts.get(0), 42));
        drain();
        cycle();
        cycle();
        long published = store.load(id).persistedTime();
        clock.advanceTime(10, TimeUnit.MILLISECONDS);
        restart();
        drain();
        assertThat(progress(id).persistedTime()).isEqualTo(published);
        assertThat(progress(id).lastProgressTime()).isEqualTo(published);
        cycle();
        replies.get(2).complete(success(starts.get(2), 43));
        drain();
        cycle();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.RUNNING);
        assertThat(progress(id).persistedTime()).isEqualTo(clock.milliseconds());
        fs.failReads = true;
        for (int i = 0; i < 3; i++) {
            assertThat(progress(id).succeeded()).isEqualTo(2);
        }
        CompletableFuture<List<CompactionInfo>> listed =
                manager.listCompactions(new ListCompactionsRequest());
        drain();
        assertThat(listed.join()).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"plan", "journal", "expired-journal"})
    void testUnreadableRecoveryPublishesFailureFromDurableCounts(String failure) throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 2, null);
        bucket(new TableBucket(1, 2), 1, null);
        bucket(new TableBucket(1, 3), 1, null);
        String id = submit(request("orders"));
        replies.get(0).complete(success(starts.get(0), 42));
        drain();
        cycle();
        cycle();
        fs.failReads = true;
        fs.failReadSuffix = failure.equals("plan") ? null : ".progress";
        fs.unreadableOperation = id;
        if (failure.equals("expired-journal")) {
            clock.advanceTime(86400000, TimeUnit.MILLISECONDS);
        }
        restart();
        events.runAll();
        io.runAll();
        events.runAll();
        CompletableFuture<CompactionProgress> beforePublication = manager.getCompaction(id);
        CompletableFuture<String> admission = manager.compact(request("orders"));
        events.runAll();
        assertThat(beforePublication).isCompletedExceptionally();
        assertThat(admission).isCompletedExceptionally();
        assertThat(store.load(id).info().status()).isEqualTo(CompactionStatus.RUNNING);
        drain();
        CompactionTask terminal = store.load(id);
        assertThat(terminal.info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(terminal.succeeded()).isEqualTo(1);
        assertThat(terminal.failed()).isEqualTo(3);
        assertThat(terminal.cancelled()).isZero();
        assertThat(terminal.firstError()).contains("remote read failed");
        assertThat(zk.getCompactionActive().data()).isEmpty();
        assertCounts(progress(id));
        String next = submit(request("orders"));
        assertThat(next).isNotEqualTo(id);
        assertThat(progress(next).info().status()).isEqualTo(CompactionStatus.RUNNING);
    }

    @Test
    void testExpiredSummariesAreHiddenWhenRemoteDeletionFails() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        manager.cancelCompaction(id);
        drain();
        clock.advanceTime(7 * 86400000L + 1, TimeUnit.MILLISECONDS);
        fs.failDeletes = true;
        CompletableFuture<CompactionProgress> cached = manager.getCompaction(id);
        CompletableFuture<List<CompactionInfo>> listed =
                manager.listCompactions(new ListCompactionsRequest());
        drain();
        assertThat(cached).isCompletedExceptionally();
        assertThat(listed.join()).isEmpty();
        manager.cleanupCycle();
        drain();
        assertThat(store.list()).hasSize(1);
        CompletableFuture<CompactionProgress> stored = manager.getCompaction(id);
        drain();
        assertThat(stored).isCompletedExceptionally();
        restart();
        drain();
        CompletableFuture<List<CompactionInfo>> reloaded =
                manager.listCompactions(new ListCompactionsRequest());
        drain();
        assertThat(reloaded.join()).isEmpty();
    }

    @Test
    void testRemoteWriteFailurePublishesTerminalWithKnownCounts() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 1, null);
        String id = submit(request("orders"));
        fs.failWrites = true;
        replies.get(0).complete(success(starts.get(0), 42));
        drain();
        cycle();
        CompactionProgress failed = progress(id);
        assertThat(failed.info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(failed.succeeded()).isZero();
        assertThat(failed.failed()).isEqualTo(2);
        assertThat(failed.cancelled()).isZero();
        assertThat(zk.getCompactionActive().data()).isEmpty();
    }

    @Test
    void testRecoveryRetainsPublishedSuccessAndRestartsRemainingBuckets() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        bucket(new TableBucket(1, 1), 2, null);
        bucket(new TableBucket(1, 2), 1, null);
        bucket(new TableBucket(1, 3), 1, null);
        String id = submit(request("orders"));
        replies.get(0).complete(success(starts.get(0), 42));
        drain();
        cycle();
        cycle();
        restart();
        drain();
        cycle();
        assertThat(progress(id).succeeded()).isEqualTo(1);
        assertThat(starts).hasSize(5);
        assertThat(starts.subList(3, 5))
                .noneMatch(start -> start.getTableBucket().getBucketId() == 0);
        replies.get(3).completeExceptionally(new IllegalStateException("execution failed"));
        drain();
        CompactionProgress failed = progress(id);
        assertThat(failed.succeeded()).isEqualTo(1);
        assertThat(failed.failed()).isEqualTo(3);
        assertThat(failed.cancelled()).isZero();
    }

    @Test
    void testRecoveryOfUnpublishedPlanAndOriginalDeadline() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        CompletableFuture<String> admission = manager.compact(request("orders"));
        events.runAll();
        io.runAll();
        String interrupted = zk.getCompactionActive().data();
        manager.close();
        drain();
        assertThat(admission).isCompletedExceptionally();
        restart();
        drain();
        assertThat(progress(interrupted).info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(progress(interrupted).failed()).isEqualTo(1);
        assertThat(progress(interrupted).cancelled()).isZero();
        String expired = submit(request("orders"));
        clock.advanceTime(86400000, TimeUnit.MILLISECONDS);
        restart();
        drain();
        assertThat(progress(expired).failed()).isEqualTo(1);
        assertThat(store.load(expired).deadline()).isEqualTo(86400000);
    }

    @Test
    void testCloseCancelsTimersRequestsAndPreventsQueuedScheduling() throws Exception {
        manager.close();
        drain();
        Scheduler timer = mock(Scheduler.class);
        ScheduledFuture<?> control = mock(ScheduledFuture.class);
        ScheduledFuture<?> cleanup = mock(ScheduledFuture.class);
        org.mockito.Mockito.doReturn(control, cleanup)
                .when(timer)
                .schedule(any(), any(Runnable.class), anyLong(), anyLong());
        manager =
                new CompactionManager(
                        context,
                        store,
                        zk,
                        rpc,
                        "INTERNAL",
                        () -> config,
                        events,
                        io,
                        timer,
                        clock,
                        86400000,
                        1000);
        drain();
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        submit(request("orders"));
        manager.close();
        manager.controlCycle();
        manager.cleanupCycle();
        drain();
        assertThat(replies.get(0)).isCancelled();
        verify(rpc, times(0)).sendRequest(any(), eq(ApiKeys.CANCEL_BUCKET_COMPACTION), any());
        assertThat(starts).hasSize(1);
        verify(control).cancel(false);
        verify(cleanup).cancel(false);
        verify(timer, times(2)).schedule(any(), any(Runnable.class), anyLong(), anyLong());
    }

    @Test
    void testZkFailureDoesNotClaimTerminalAndConvergesBeforeAdmission() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        failZkWrites = true;
        CompletableFuture<Void> cancellation = manager.cancelCompaction(id);
        drain();
        assertThat(cancellation).isCompletedExceptionally();
        assertThat(replies.get(0)).isCancelled();
        assertThat(store.load(id).info().status()).isEqualTo(CompactionStatus.RUNNING);
        assertThat(zk.getCompactionActive().data()).isEqualTo(id);
        verify(rpc, times(0)).sendRequest(any(), eq(ApiKeys.CANCEL_BUCKET_COMPACTION), any());
        CompletableFuture<String> rejected = manager.compact(request("orders"));
        drain();
        assertThat(rejected).isCompletedExceptionally();
        manager.controlCycle();
        drain();
        failZkWrites = false;
        manager.controlCycle();
        drain();
        CompletableFuture<String> sameOperation = manager.compact(request("orders"));
        drain();
        assertThat(sameOperation.join()).isEqualTo(id);
        cycle();
        assertThat(starts).hasSize(2);
        replies.get(1).completeExceptionally(new IllegalStateException("execution failed"));
        drain();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.FAILED);
        assertThat(progress(id).failed()).isEqualTo(1);
        assertThat(submit(request("orders"))).isNotEqualTo(id);
    }

    @Test
    void testSupersededExecutionIgnoresLateFailureOnSameServer() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        StartBucketCompactionResponse running = success(starts.get(0), 1);
        running.getStatus().setStatus("RUNNING");
        replies.get(0).complete(running);
        drain();
        cycle();
        assertThat(starts).hasSize(2);
        context.putBucketLeaderAndIsr(
                new TableBucket(1, 0),
                new LeaderAndIsr(
                        1,
                        99,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        context.getCoordinatorEpoch(),
                        1));
        cycle();
        assertThat(starts).hasSize(3);
        StartBucketCompactionResponse response = new StartBucketCompactionResponse();
        response.setStatus().copyFrom(running.getStatus()).setStatus("FAILED");
        replies.get(1).complete(response);
        drain();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.RUNNING);
        replies.get(2).complete(success(starts.get(2), 42));
        drain();
        cycle();
        assertThat(progress(id).info().status()).isEqualTo(CompactionStatus.SUCCEEDED);
    }

    @Test
    void testRetentionCleanupDropsCachedSummaryAndIsRepeatable() throws Exception {
        table(1, "orders", true, false);
        bucket(new TableBucket(1, 0), 1, null);
        String id = submit(request("orders"));
        manager.cancelCompaction(id);
        drain();
        clock.advanceTime(7 * 86400000L + 1, TimeUnit.MILLISECONDS);
        manager.cleanupCycle();
        drain();
        assertThat(store.list()).isEmpty();
        CompletableFuture<CompactionProgress> missing = manager.getCompaction(id);
        drain();
        assertThat(missing).isCompletedExceptionally();
        manager.cleanupCycle();
        drain();
        assertThat(store.list()).isEmpty();
    }

    private static final class FailingFileSystem extends LocalFileSystem {
        private boolean failReads;
        private String failReadSuffix;
        private String unreadableOperation;
        private boolean failWrites;
        private boolean failDeletes;

        @Override
        public boolean delete(FsPath path, boolean recursive) throws IOException {
            if (failDeletes) {
                throw new IOException("remote delete failed");
            }
            return super.delete(path, recursive);
        }

        @Override
        public FSDataInputStream open(FsPath path) throws IOException {
            if (failReads
                    && (unreadableOperation == null
                            || path.getParent().getName().equals(unreadableOperation))
                    && (failReadSuffix == null || path.getName().endsWith(failReadSuffix))) {
                throw new IOException("remote read failed");
            }
            return super.open(path);
        }

        @Override
        public FSDataOutputStream create(FsPath path, FileSystem.WriteMode mode)
                throws IOException {
            if (failWrites) {
                throw new IOException("remote write failed");
            }
            return super.create(path, mode);
        }
    }

    private void restart() {
        manager.close();
        events.runAll();
        manager =
                new CompactionManager(
                        context,
                        store,
                        zk,
                        rpc,
                        "INTERNAL",
                        () -> config,
                        events,
                        io,
                        mock(Scheduler.class),
                        clock,
                        86400000,
                        1000);
    }

    private CompactRequest request(String table) {
        return new CompactRequest().setDatabaseName("db").setTableName(table).setType("OPTIMIZE");
    }

    private void table(long id, String name, boolean primary, boolean partitioned) {
        Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT());
        if (partitioned) {
            schema.column("day", DataTypes.STRING());
        }
        if (primary) {
            schema.primaryKey(
                    partitioned ? Arrays.asList("id", "day") : Collections.singletonList("id"));
        }
        TablePath path = TablePath.of("db", name);
        context.putTablePath(id, path);
        context.putTableInfo(
                new TableInfo(
                        path,
                        id,
                        1,
                        schema.build(),
                        Collections.emptyList(),
                        partitioned ? Collections.singletonList("day") : Collections.emptyList(),
                        3,
                        new Configuration(),
                        new Configuration(),
                        directory.toString(),
                        null,
                        0,
                        0));
    }

    private void bucket(TableBucket bucket, int server, String partition) {
        context.updateBucketReplicaAssignment(bucket, Collections.singletonList(server));
        context.putBucketLeaderAndIsr(
                bucket, new LeaderAndIsr(server, context.getCoordinatorEpoch()));
        if (partition != null) {
            context.putPartition(
                    bucket.getPartitionId(),
                    PhysicalTablePath.of(context.getTablePathById(bucket.getTableId()), partition));
        }
    }

    private StartBucketCompactionResponse success(
            StartBucketCompactionRequest request, long snapshot) {
        StartBucketCompactionResponse response = new StartBucketCompactionResponse();
        PbBucketCompactionStatus status = response.setStatus();
        status.setOperationId(request.getOperationId())
                .setLeaderEpoch(request.getLeaderEpoch())
                .setStatus("SUCCEEDED")
                .setPhase("SNAPSHOT")
                .setPhaseStartTime(1)
                .setSnapshotId(snapshot);
        status.setTableBucket().copyFrom(request.getTableBucket());
        return response;
    }

    private String submit(CompactRequest request) {
        CompletableFuture<String> result = manager.compact(request);
        drain();
        cycle();
        return result.join();
    }

    private void cycle() {
        clock.advanceTime(1000, TimeUnit.MILLISECONDS);
        manager.controlCycle();
        drain();
    }

    private CompactionProgress progress(String id) {
        CompletableFuture<CompactionProgress> result = manager.getCompaction(id);
        events.runAll();
        return result.join();
    }

    private void drain() {
        while (!events.tasks.isEmpty() || !io.tasks.isEmpty()) {
            events.runAll();
            io.runAll();
        }
    }

    private void assertCounts(CompactionProgress progress) {
        assertThat(
                        progress.pending()
                                + progress.running()
                                + progress.succeeded()
                                + progress.failed()
                                + progress.cancelled())
                .isEqualTo(progress.total());
    }

    private static final class ManualExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        void runAll() {
            while (!tasks.isEmpty()) {
                tasks.remove().run();
            }
        }
    }
}
