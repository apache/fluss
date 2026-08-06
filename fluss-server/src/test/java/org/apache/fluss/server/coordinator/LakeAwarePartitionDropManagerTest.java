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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.FreezePartitionRequest;
import org.apache.fluss.rpc.messages.FreezePartitionResponse;
import org.apache.fluss.server.entity.FreezePartitionResultForBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFreezePartitionData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFreezePartitionResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link LakeAwarePartitionDropManager}. */
class LakeAwarePartitionDropManagerTest {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final String PARTITION_NAME = "20260806";
    private static final long TABLE_ID = 11L;
    private static final long PARTITION_ID = 22L;
    private static final int SERVER_ID = 1;
    private static final int LEADER_EPOCH = 7;
    private static final long FROZEN_OFFSET = 10L;
    private static final long DEFAULT_FREEZE_TIMEOUT_MS = 5_000L;
    private static final TableInfo TABLE_INFO = createTableInfo();
    private static final TableBucket BUCKET_0 = new TableBucket(TABLE_ID, PARTITION_ID, 0);
    private static final TableBucket BUCKET_1 = new TableBucket(TABLE_ID, PARTITION_ID, 1);

    @Test
    void testDropsUnfrozenPartitionAfterFreezeAndTiering() throws Exception {
        try (TestContext context = new TestContext()) {
            context.returnStableFreezeResponse();
            context.returnLakeSnapshotAt(FROZEN_OFFSET);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.metadataManager)
                    .markPartitionFrozen(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
            ArgumentCaptor<FreezePartitionRequest> requestCaptor =
                    ArgumentCaptor.forClass(FreezePartitionRequest.class);
            verify(context.channelManager)
                    .sendFreezePartitionRequest(eq(SERVER_ID), requestCaptor.capture());
            assertThat(getFreezePartitionData(requestCaptor.getValue()))
                    .containsExactlyInAnyOrderEntriesOf(context.expectedLeaderEpochs());
            verify(context.zooKeeperClient).getLakeTableSnapshot(TABLE_ID, null);
            verify(context.metadataManager)
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
            assertThat(context.currentRegistration.get()).isNull();
        }
    }

    @Test
    void testAlreadyFrozenPartitionSkipsRegistrationUpdate() throws Exception {
        try (TestContext context = new TestContext()) {
            context.currentRegistration.set(context.frozenRegistration);
            context.returnStableFreezeResponse();
            context.returnLakeSnapshotAt(FROZEN_OFFSET);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.metadataManager, never())
                    .markPartitionFrozen(any(), any(), eq(TABLE_ID), eq(PARTITION_ID));
            verify(context.channelManager)
                    .sendFreezePartitionRequest(eq(SERVER_ID), any(FreezePartitionRequest.class));
            verify(context.metadataManager)
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
        }
    }

    @Test
    void testRetriesFreezeUntilHighWatermarkReachesLogEndOffset() throws Exception {
        try (TestContext context = new TestContext()) {
            when(context.channelManager.sendFreezePartitionRequest(
                            eq(SERVER_ID), any(FreezePartitionRequest.class)))
                    .thenReturn(
                            CompletableFuture.completedFuture(
                                    freezeResponse(FROZEN_OFFSET - 1, FROZEN_OFFSET)),
                            CompletableFuture.completedFuture(
                                    freezeResponse(FROZEN_OFFSET, FROZEN_OFFSET)));
            context.returnLakeSnapshotAt(FROZEN_OFFSET);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.zooKeeperClient, never()).getLakeTableSnapshot(TABLE_ID, null);
            verify(context.metadataManager, never())
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.channelManager, times(2))
                    .sendFreezePartitionRequest(eq(SERVER_ID), any(FreezePartitionRequest.class));
            verify(context.metadataManager, times(1))
                    .markPartitionFrozen(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
            verify(context.metadataManager)
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
        }
    }

    @Test
    void testRetriesLakeCheckWithoutFreezingAgain() throws Exception {
        try (TestContext context = new TestContext()) {
            context.returnStableFreezeResponse();
            when(context.zooKeeperClient.getLakeTableSnapshot(eq(TABLE_ID), isNull()))
                    .thenReturn(
                            Optional.of(lakeSnapshotAt(FROZEN_OFFSET - 1)),
                            Optional.of(lakeSnapshotAt(FROZEN_OFFSET)));

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.metadataManager, never())
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.channelManager, times(1))
                    .sendFreezePartitionRequest(eq(SERVER_ID), any(FreezePartitionRequest.class));
            verify(context.zooKeeperClient, times(2)).getLakeTableSnapshot(TABLE_ID, null);
            verify(context.metadataManager)
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
        }
    }

    @Test
    void testRetriesAfterFreezeRpcFailure() throws Exception {
        try (TestContext context = new TestContext()) {
            CompletableFuture<FreezePartitionResponse> failedFreeze = new CompletableFuture<>();
            failedFreeze.completeExceptionally(new RuntimeException("expected RPC failure"));
            when(context.channelManager.sendFreezePartitionRequest(
                            eq(SERVER_ID), any(FreezePartitionRequest.class)))
                    .thenReturn(
                            failedFreeze,
                            CompletableFuture.completedFuture(
                                    freezeResponse(FROZEN_OFFSET, FROZEN_OFFSET)));
            context.returnLakeSnapshotAt(FROZEN_OFFSET);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.metadataManager, never())
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.channelManager, times(2))
                    .sendFreezePartitionRequest(eq(SERVER_ID), any(FreezePartitionRequest.class));
            verify(context.metadataManager)
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
        }
    }

    @Test
    void testRetriesAfterFreezeRpcTimeout() throws Exception {
        try (TestContext context = new TestContext(Runnable::run, 20L)) {
            CompletableFuture<FreezePartitionResponse> pendingFreeze = new CompletableFuture<>();
            when(context.channelManager.sendFreezePartitionRequest(
                            eq(SERVER_ID), any(FreezePartitionRequest.class)))
                    .thenReturn(
                            pendingFreeze,
                            CompletableFuture.completedFuture(
                                    freezeResponse(FROZEN_OFFSET, FROZEN_OFFSET)));
            context.returnLakeSnapshotAt(FROZEN_OFFSET);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.metadataManager, never())
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);

            verify(context.channelManager, times(2))
                    .sendFreezePartitionRequest(eq(SERVER_ID), any(FreezePartitionRequest.class));
            verify(context.metadataManager)
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
        }
    }

    @Test
    void testConcurrentTryDropForSamePartitionIsSuppressed() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch secondTaskFinished = new CountDownLatch(1);
        CountDownLatch firstTaskFinished = new CountDownLatch(1);
        AtomicInteger taskIds = new AtomicInteger();
        Executor trackingExecutor =
                command -> {
                    int taskId = taskIds.incrementAndGet();
                    executorService.execute(
                            () -> {
                                try {
                                    command.run();
                                } finally {
                                    if (taskId == 1) {
                                        firstTaskFinished.countDown();
                                    } else if (taskId == 2) {
                                        secondTaskFinished.countDown();
                                    }
                                }
                            });
                };

        try (TestContext context = new TestContext(trackingExecutor, DEFAULT_FREEZE_TIMEOUT_MS)) {
            CountDownLatch firstRequestStarted = new CountDownLatch(1);
            AtomicReference<FreezePartitionRequest> firstRequest = new AtomicReference<>();
            CompletableFuture<FreezePartitionResponse> pendingFreeze = new CompletableFuture<>();
            when(context.channelManager.sendFreezePartitionRequest(
                            eq(SERVER_ID), any(FreezePartitionRequest.class)))
                    .thenAnswer(
                            invocation -> {
                                firstRequest.set(invocation.getArgument(1));
                                firstRequestStarted.countDown();
                                return pendingFreeze;
                            });
            context.returnLakeSnapshotAt(FROZEN_OFFSET);

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);
            assertThat(firstRequestStarted.await(5, TimeUnit.SECONDS)).isTrue();

            context.manager.tryDrop(TABLE_INFO, PARTITION_NAME);
            assertThat(secondTaskFinished.await(5, TimeUnit.SECONDS)).isTrue();
            verify(context.channelManager, times(1))
                    .sendFreezePartitionRequest(eq(SERVER_ID), any(FreezePartitionRequest.class));

            pendingFreeze.complete(freezeResponse(FROZEN_OFFSET, FROZEN_OFFSET));
            assertThat(firstTaskFinished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(getFreezePartitionData(firstRequest.get()))
                    .containsExactlyInAnyOrderEntriesOf(context.expectedLeaderEpochs());
            verify(context.metadataManager)
                    .dropFrozenPartition(TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID);
        } finally {
            executorService.shutdownNow();
            assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static TableInfo createTableInfo() {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("c1", DataTypes.INT()).build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ZERO)
                        .distributedBy(2)
                        .build();
        return TableInfo.of(
                TABLE_PATH,
                TABLE_ID,
                1,
                tableDescriptor,
                "/remote",
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    private static FreezePartitionResponse freezeResponse(long highWatermark, long logEndOffset) {
        List<FreezePartitionResultForBucket> results = new ArrayList<>();
        results.add(new FreezePartitionResultForBucket(BUCKET_0, highWatermark, logEndOffset));
        results.add(new FreezePartitionResultForBucket(BUCKET_1, highWatermark, logEndOffset));
        return makeFreezePartitionResponse(results);
    }

    private static LakeTableSnapshot lakeSnapshotAt(long offset) {
        Map<TableBucket, Long> bucketOffsets = new LinkedHashMap<>();
        bucketOffsets.put(BUCKET_0, offset);
        bucketOffsets.put(BUCKET_1, offset);
        return new LakeTableSnapshot(1L, bucketOffsets);
    }

    private static final class TestContext implements AutoCloseable {
        private final MetadataManager metadataManager = mock(MetadataManager.class);
        private final ZooKeeperClient zooKeeperClient = mock(ZooKeeperClient.class);
        private final CoordinatorChannelManager channelManager =
                mock(CoordinatorChannelManager.class);
        private final PartitionRegistration unfrozenRegistration =
                new PartitionRegistration(TABLE_ID, PARTITION_ID, "/remote");
        private final PartitionRegistration frozenRegistration = unfrozenRegistration.withFrozen();
        private final AtomicReference<PartitionRegistration> currentRegistration =
                new AtomicReference<>(unfrozenRegistration);
        private final LakeAwarePartitionDropManager manager;

        private TestContext() throws Exception {
            this(Runnable::run, DEFAULT_FREEZE_TIMEOUT_MS);
        }

        private TestContext(Executor executor, long freezeTimeoutMs) throws Exception {
            manager =
                    new LakeAwarePartitionDropManager(
                            metadataManager,
                            zooKeeperClient,
                            channelManager,
                            executor,
                            freezeTimeoutMs);

            when(metadataManager.getOptionalPartitionRegistration(TABLE_PATH, PARTITION_NAME))
                    .thenAnswer(ignored -> Optional.ofNullable(currentRegistration.get()));
            when(metadataManager.markPartitionFrozen(
                            TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID))
                    .thenAnswer(
                            ignored -> {
                                currentRegistration.set(frozenRegistration);
                                return Optional.of(frozenRegistration);
                            });
            when(metadataManager.dropFrozenPartition(
                            TABLE_PATH, PARTITION_NAME, TABLE_ID, PARTITION_ID))
                    .thenAnswer(
                            ignored -> {
                                currentRegistration.set(null);
                                return true;
                            });

            Map<Integer, BucketAssignment> bucketAssignments = new LinkedHashMap<>();
            bucketAssignments.put(0, BucketAssignment.of(SERVER_ID));
            bucketAssignments.put(1, BucketAssignment.of(SERVER_ID));
            when(zooKeeperClient.getPartitionAssignment(PARTITION_ID))
                    .thenReturn(Optional.of(new PartitionAssignment(TABLE_ID, bucketAssignments)));

            LeaderAndIsr leaderAndIsr =
                    new LeaderAndIsr(
                            SERVER_ID,
                            LEADER_EPOCH,
                            Collections.singletonList(SERVER_ID),
                            Collections.emptyList(),
                            1,
                            1);
            Map<TableBucket, LeaderAndIsr> leaders = new LinkedHashMap<>();
            leaders.put(BUCKET_0, leaderAndIsr);
            leaders.put(BUCKET_1, leaderAndIsr);
            when(zooKeeperClient.getLeaderAndIsrs(anyCollection())).thenReturn(leaders);
        }

        private void returnStableFreezeResponse() {
            when(channelManager.sendFreezePartitionRequest(
                            eq(SERVER_ID), any(FreezePartitionRequest.class)))
                    .thenReturn(
                            CompletableFuture.completedFuture(
                                    freezeResponse(FROZEN_OFFSET, FROZEN_OFFSET)));
        }

        private void returnLakeSnapshotAt(long offset) throws Exception {
            when(zooKeeperClient.getLakeTableSnapshot(eq(TABLE_ID), isNull()))
                    .thenReturn(Optional.of(lakeSnapshotAt(offset)));
        }

        private Map<TableBucket, Integer> expectedLeaderEpochs() {
            Map<TableBucket, Integer> expected = new LinkedHashMap<>();
            expected.put(BUCKET_0, LEADER_EPOCH);
            expected.put(BUCKET_1, LEADER_EPOCH);
            return expected;
        }

        @Override
        public void close() {
            manager.close();
        }
    }
}
