/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.log.internals;

import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.table.scanner.log.LogScannerStatus;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.NotConsumerGroupOwnerException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsRequestTable;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbCommitConsumerOffsetsResponseTable;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsRequestTable;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseBucket;
import org.apache.fluss.rpc.messages.PbGetConsumerOffsetsResponseTable;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.utils.concurrent.FutureUtils.completedExceptionally;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ConsumerGroupClient}. */
class ConsumerGroupClientTest {

    private final TestingLogScannerStatus logScannerStatus = new TestingLogScannerStatus();
    private final Configuration conf =
            new Configuration()
                    .set(ConfigOptions.CLIENT_CONSUMER_GROUP_ID, "group-1")
                    .set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED, false);

    @Test
    void testOwnerDiscoveryIsCachedAndCanBeInvalidated() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<ServerNode> owner = new AtomicReference<>(TestingMetadataUpdater.NODE2);
        TestTabletServerGateway gateway =
                discoveryGateway(
                        request -> {
                            requests.incrementAndGet();
                            return CompletableFuture.completedFuture(ownerResponse(owner.get()));
                        });
        TestTabletServerGateway nextOwnerGateway =
                discoveryGateway(gateway::findConsumerGroupOwner);
        Map<Integer, TestTabletServerGateway> gateways = new HashMap<>();
        gateways.put(TestingMetadataUpdater.NODE1.id(), gateway);
        gateways.put(TestingMetadataUpdater.NODE2.id(), gateway);
        gateways.put(TestingMetadataUpdater.NODE3.id(), nextOwnerGateway);
        TestingMetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        TestingMetadataUpdater.COORDINATOR,
                        Arrays.asList(
                                TestingMetadataUpdater.NODE1,
                                TestingMetadataUpdater.NODE2,
                                TestingMetadataUpdater.NODE3),
                        Collections.emptyMap(),
                        gateways,
                        new Configuration());
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater, logScannerStatus);

        assertThat(manager.getOrDiscoverOwner().get()).isSameAs(gateway);
        assertThat(manager.getOrDiscoverOwner().get()).isSameAs(gateway);
        assertThat(requests).hasValue(1);

        owner.set(TestingMetadataUpdater.NODE3);
        manager.markOwnerUnknown();
        assertThat(manager.getOrDiscoverOwner().get()).isSameAs(nextOwnerGateway);
        assertThat(requests).hasValue(2);
    }

    @Test
    void testOffsetRequestsReuseOwnerGatewayUntilInvalidated() {
        AtomicInteger gatewayCreations = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request ->
                                CompletableFuture.completedFuture(
                                        new CommitConsumerOffsetsResponse()));
        Map<Integer, TestTabletServerGateway> gateways = new HashMap<>();
        gateways.put(TestingMetadataUpdater.NODE1.id(), gateway);
        gateways.put(TestingMetadataUpdater.NODE2.id(), gateway);
        TestingMetadataUpdater updater =
                new TestingMetadataUpdater(
                        TestingMetadataUpdater.COORDINATOR,
                        Arrays.asList(TestingMetadataUpdater.NODE1, TestingMetadataUpdater.NODE2),
                        Collections.emptyMap(),
                        gateways,
                        new Configuration()) {
                    @Override
                    public TabletServerGateway newTabletServerClientForNode(ServerNode node) {
                        gatewayCreations.incrementAndGet();
                        return super.newTabletServerClientForNode(node);
                    }
                };
        ConsumerGroupClient manager = new ConsumerGroupClient(conf, updater, logScannerStatus);

        assertThat(manager.commitOffsetsSync(sampleOffsets(), 5000L)).isTrue();
        assertThat(manager.commitOffsetsSync(sampleOffsets(), 5000L)).isTrue();
        assertThat(manager.fetchCommittedOffsets(sampleOffsets().keySet(), 5000L)).isEmpty();
        // One gateway for discovery and one for all requests to the owner.
        assertThat(gatewayCreations).hasValue(2);

        manager.markOwnerUnknown();
        assertThat(manager.commitOffsetsSync(sampleOffsets(), 5000L)).isTrue();
        assertThat(gatewayCreations).hasValue(4);
    }

    @Test
    void testCachedOwnerIsUsedWhenMetadataNoLongerContainsOwner() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        TestTabletServerGateway gateway =
                discoveryGateway(
                        request -> {
                            requests.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    ownerResponse(TestingMetadataUpdater.NODE2));
                        });
        TestingMetadataUpdater metadataUpdater = metadataUpdater(gateway);
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater, logScannerStatus);

        assertThat(manager.getOrDiscoverOwner().get()).isSameAs(gateway);

        // The discovered owner remains usable even if the client metadata cache no longer
        // contains the node; the owner response already provided the endpoint.
        metadataUpdater.updateCluster(Cluster.empty());
        CompletableFuture<TabletServerGateway> cachedOwner = manager.getOrDiscoverOwner();

        assertThat(cachedOwner).isCompleted();
        assertThat(cachedOwner.get()).isSameAs(gateway);
        assertThat(requests).hasValue(1);
    }

    @Test
    void testConcurrentDiscoverySharesOneRequest() throws Exception {
        CompletableFuture<FindConsumerGroupOwnerResponse> rpcResponse = new CompletableFuture<>();
        AtomicInteger requests = new AtomicInteger();
        TestTabletServerGateway gateway =
                discoveryGateway(
                        request -> {
                            requests.incrementAndGet();
                            return rpcResponse;
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        int callers = 4;
        CyclicBarrier startBarrier = new CyclicBarrier(callers);
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        try {
            List<Future<CompletableFuture<TabletServerGateway>>> submitted = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                submitted.add(
                        executor.submit(
                                () -> {
                                    startBarrier.await();
                                    return manager.getOrDiscoverOwner();
                                }));
            }

            List<CompletableFuture<TabletServerGateway>> discoveries = new ArrayList<>();
            for (Future<CompletableFuture<TabletServerGateway>> caller : submitted) {
                discoveries.add(caller.get());
            }

            // The first caller publishes one discovery; the others must reuse it instead of
            // each sending their own FindConsumerGroupOwner request.
            assertThat(discoveries.stream().distinct().count()).isEqualTo(1);
            rpcResponse.complete(ownerResponse(TestingMetadataUpdater.NODE2));
            for (CompletableFuture<TabletServerGateway> discovery : discoveries) {
                assertThat(discovery.get()).isSameAs(gateway);
            }
            assertThat(requests).hasValue(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testDiscoveryFailsWhenNoTabletServerIsKnown() {
        TestingMetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        TestingMetadataUpdater.COORDINATOR,
                        Collections.emptyList(),
                        Collections.<TablePath, TableInfo>emptyMap(),
                        Collections.emptyMap(),
                        new Configuration());
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater, logScannerStatus);

        assertThatThrownBy(() -> manager.getOrDiscoverOwner().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ConsumerGroupOwnerNotAvailableException.class);
    }

    @Test
    void testDiscoveryCompletionMakesCacheUsable() {
        CompletableFuture<FindConsumerGroupOwnerResponse> rpcResponse = new CompletableFuture<>();
        AtomicInteger requests = new AtomicInteger();
        TestTabletServerGateway gateway =
                discoveryGateway(
                        request -> {
                            requests.incrementAndGet();
                            return rpcResponse;
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        CompletableFuture<TabletServerGateway> discovery = manager.getOrDiscoverOwner();
        AtomicBoolean cacheIsUsableOnCompletion = new AtomicBoolean(false);
        discovery.whenComplete(
                (owner, error) ->
                        cacheIsUsableOnCompletion.set(
                                manager.getOrDiscoverOwner().getNow(null) == owner));

        rpcResponse.complete(ownerResponse(TestingMetadataUpdater.NODE2));

        assertThat(cacheIsUsableOnCompletion).isTrue();
        assertThat(requests).hasValue(1);
    }

    @Test
    void testDiscoveryFailureAllowsRetry() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<CompletableFuture<FindConsumerGroupOwnerResponse>> rpcResponse =
                new AtomicReference<>(new CompletableFuture<>());
        TestTabletServerGateway gateway =
                discoveryGateway(
                        request -> {
                            requests.incrementAndGet();
                            return rpcResponse.get();
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        CompletableFuture<TabletServerGateway> firstDiscovery = manager.getOrDiscoverOwner();
        CompletableFuture<TabletServerGateway> retryDiscovery;
        rpcResponse.get().completeExceptionally(new RuntimeException("discovery failed"));

        assertThatThrownBy(firstDiscovery::get)
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("discovery failed");

        rpcResponse.set(new CompletableFuture<>());
        retryDiscovery = manager.getOrDiscoverOwner();

        assertThat(retryDiscovery).isNotSameAs(firstDiscovery);
        assertThat(requests).hasValue(2);

        rpcResponse.get().complete(ownerResponse(TestingMetadataUpdater.NODE2));
        assertThat(retryDiscovery.get()).isSameAs(gateway);
    }

    @Test
    void testCloseCancelsDiscoveryAndRejectsLateResponse() throws Exception {
        CompletableFuture<FindConsumerGroupOwnerResponse> rpcResponse = new CompletableFuture<>();
        TestTabletServerGateway gateway = discoveryGateway(request -> rpcResponse);
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        CompletableFuture<TabletServerGateway> discovery = manager.getOrDiscoverOwner();
        manager.close();
        rpcResponse.complete(ownerResponse(TestingMetadataUpdater.NODE2));

        assertThat(discovery).isCancelled();
        assertThatThrownBy(manager::getOrDiscoverOwner).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testGetOrDiscoverOwnerAfterClose() {
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet());
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        manager.close();

        assertThatThrownBy(manager::getOrDiscoverOwner)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Consumer group client is closed.");
    }

    @Test
    void testCancelledDiscoveryDoesNotCacheOwner() {
        CompletableFuture<FindConsumerGroupOwnerResponse> rpcResponse = new CompletableFuture<>();
        AtomicInteger requests = new AtomicInteger();
        TestTabletServerGateway gateway =
                discoveryGateway(
                        request -> {
                            requests.incrementAndGet();
                            return rpcResponse;
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        CompletableFuture<TabletServerGateway> discovery = manager.getOrDiscoverOwner();
        discovery.cancel(false);
        rpcResponse.complete(ownerResponse(TestingMetadataUpdater.NODE2));

        assertThat(discovery).isCancelled();
        assertThat(manager.getOrDiscoverOwner()).isNotSameAs(discovery);
        assertThat(requests).hasValue(2);
    }

    @Test
    void testCancelledDiscoveryIsNotReturnedToLaterCallers() throws Exception {
        CompletableFuture<FindConsumerGroupOwnerResponse> firstRpcResponse =
                new CompletableFuture<>();
        AtomicReference<CompletableFuture<FindConsumerGroupOwnerResponse>> rpcResponse =
                new AtomicReference<>(firstRpcResponse);
        AtomicInteger requests = new AtomicInteger();
        TestTabletServerGateway gateway =
                discoveryGateway(
                        request -> {
                            requests.incrementAndGet();
                            return rpcResponse.get();
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        CompletableFuture<TabletServerGateway> firstDiscovery = manager.getOrDiscoverOwner();
        firstDiscovery.cancel(false);

        CompletableFuture<FindConsumerGroupOwnerResponse> secondRpcResponse =
                new CompletableFuture<>();
        rpcResponse.set(secondRpcResponse);
        CompletableFuture<TabletServerGateway> secondDiscovery = manager.getOrDiscoverOwner();

        assertThat(secondDiscovery).isNotSameAs(firstDiscovery);
        assertThat(secondDiscovery).isNotCancelled();
        assertThat(requests).hasValue(2);

        firstRpcResponse.complete(ownerResponse(TestingMetadataUpdater.NODE2));
        assertThat(firstDiscovery).isCancelled();
        assertThat(manager.getOrDiscoverOwner()).isSameAs(secondDiscovery);

        secondRpcResponse.complete(ownerResponse(TestingMetadataUpdater.NODE3));
        assertThat(secondDiscovery.get()).isSameAs(gateway);
    }

    @Test
    void testDecodeFailureIsNotWrappedInCompletionException() {
        TestTabletServerGateway gateway =
                discoveryGateway(
                        request -> {
                            // The response violates the owner contract, so decoding must fail.
                            return CompletableFuture.completedFuture(
                                    new FindConsumerGroupOwnerResponse());
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        AtomicReference<Throwable> error = new AtomicReference<>();
        manager.getOrDiscoverOwner().whenComplete((owner, e) -> error.set(e));

        // Async composition must observe the declared failure type instead of the
        // CompletionException wrapper that thenApply adds around decode errors.
        assertThat(error.get()).isInstanceOf(ConsumerGroupOwnerNotAvailableException.class);
    }

    @Test
    void testDecodeFailureOnIncompleteOwner() {
        TestTabletServerGateway gateway =
                discoveryGateway(
                        request -> {
                            // The owner is present but its host and port are missing.
                            return CompletableFuture.completedFuture(
                                    new FindConsumerGroupOwnerResponse()
                                            .setOwner(new PbServerNode().setNodeId(1)));
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        AtomicReference<Throwable> error = new AtomicReference<>();
        manager.getOrDiscoverOwner().whenComplete((owner, e) -> error.set(e));

        assertThat(error.get())
                .isInstanceOf(ConsumerGroupOwnerNotAvailableException.class)
                .hasMessageContaining("incomplete owner");
    }

    @Test
    void testCommitRequestIsEncodedWithAllBuckets() throws Exception {
        AtomicReference<CommitConsumerOffsetsRequest> captured = new AtomicReference<>();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> {
                            captured.set(request);
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        Map<TableBucket, OffsetMetadata> offsets = new LinkedHashMap<>();
        offsets.put(new TableBucket(2L, null, 3), new OffsetMetadata(23L, "second"));
        offsets.put(new TableBucket(1L, 5L, 2), new OffsetMetadata(12L, "partitioned"));
        offsets.put(new TableBucket(1L, null, 1), new OffsetMetadata(11L));

        CompletableFuture<Exception> callbackError = new CompletableFuture<>();
        manager.commitOffsetsAsync(offsets, (ignored, error) -> callbackError.complete(error));

        CommitConsumerOffsetsRequest request = captured.get();
        assertThat(request.getGroupId()).isEqualTo("group-1");
        assertThat(request.getTablesList()).hasSize(2);
        // Verify both tables are present regardless of order
        assertThat(request.getTablesList())
                .extracting(table -> table.getTableId())
                .containsExactlyInAnyOrder(1L, 2L);
        // Verify table 1 has both buckets with correct partitionId
        PbCommitConsumerOffsetsRequestTable table1 =
                request.getTablesList().stream()
                        .filter(t -> t.getTableId() == 1L)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("table 1 not found"));
        assertThat(table1.getBucketsList())
                .extracting(bucket -> bucket.getBucketId())
                .containsExactlyInAnyOrder(1, 2);
        assertThat(
                        table1.getBucketsList().stream()
                                .filter(b -> b.getBucketId() == 2)
                                .findFirst()
                                .orElseThrow(() -> new AssertionError("bucket 2 not found"))
                                .getPartitionId())
                .isEqualTo(5L);
        manager.invokeCompletedOffsetCommitCallbacks();
        assertThat(callbackError.get(5, TimeUnit.SECONDS)).isNull();
    }

    @Test
    void testCommitAsyncReportsBucketErrorAndForgetsOwner() throws Exception {
        AtomicInteger discoveries = new AtomicInteger();
        CommitConsumerOffsetsResponse response =
                errorResponse(Errors.NOT_CONSUMER_GROUP_OWNER, "owner moved");
        TestTabletServerGateway gateway =
                gateway(
                        request -> {
                            discoveries.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    ownerResponse(TestingMetadataUpdater.NODE2));
                        },
                        request -> CompletableFuture.completedFuture(response));
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);

        CompletableFuture<Exception> callbackError = new CompletableFuture<>();
        manager.commitOffsetsAsync(
                sampleOffsets(), (ignored, error) -> callbackError.complete(error));

        manager.invokeCompletedOffsetCommitCallbacks();
        assertThat(callbackError.get(5, TimeUnit.SECONDS))
                .isInstanceOf(NotConsumerGroupOwnerException.class);
        manager.getOrDiscoverOwner().get();
        assertThat(discoveries).hasValue(2);
    }

    @Test
    void testCommitAsyncCallbackForwardsStableSnapshotOnce() throws Exception {
        CompletableFuture<CommitConsumerOffsetsResponse> response = new CompletableFuture<>();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> response);
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);
        Map<TableBucket, OffsetMetadata> offsets = Collections.unmodifiableMap(sampleOffsets());
        AtomicInteger callbacks = new AtomicInteger();
        CompletableFuture<Map<TableBucket, OffsetMetadata>> callbackOffsets =
                new CompletableFuture<>();
        CompletableFuture<Exception> callbackError = new CompletableFuture<>();

        manager.commitOffsetsAsync(
                offsets,
                (committed, error) -> {
                    callbacks.incrementAndGet();
                    callbackOffsets.complete(committed);
                    callbackError.complete(error);
                });
        response.complete(new CommitConsumerOffsetsResponse());
        manager.close();
        manager.invokeCompletedOffsetCommitCallbacks();
        Map<TableBucket, OffsetMetadata> committed = callbackOffsets.get(5, TimeUnit.SECONDS);

        assertThat(callbacks).hasValue(1);
        assertThat(callbackError.get(5, TimeUnit.SECONDS)).isNull();
        assertThat(committed).isSameAs(offsets);
        assertThatThrownBy(committed::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testCommitAsyncCallbackRunsOnScannerThread() throws Exception {
        CompletableFuture<CommitConsumerOffsetsResponse> response = new CompletableFuture<>();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> response);
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);
        CompletableFuture<Exception> callbackError = new CompletableFuture<>();
        AtomicReference<Thread> callbackThread = new AtomicReference<>();

        manager.commitOffsetsAsync(
                sampleOffsets(),
                (ignored, error) -> {
                    callbackThread.set(Thread.currentThread());
                    callbackError.complete(error);
                });
        Thread rpcThread = new Thread(() -> response.complete(new CommitConsumerOffsetsResponse()));
        rpcThread.start();

        rpcThread.join();
        assertThat(callbackError).isNotDone();
        manager.invokeCompletedOffsetCommitCallbacks();
        assertThat(callbackError.get(5, TimeUnit.SECONDS)).isNull();
        assertThat(callbackThread.get()).isSameAs(Thread.currentThread());
        manager.close();
    }

    @Test
    void testCommitAsyncCallbackDoesNotWaitForeverForRpc() throws Exception {
        CompletableFuture<CommitConsumerOffsetsResponse> response = new CompletableFuture<>();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> response);
        conf.set(ConfigOptions.CLIENT_REQUEST_TIMEOUT, Duration.ofMillis(50));
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, new ManualClock(), 10L);
        CompletableFuture<Exception> callbackError = new CompletableFuture<>();
        AtomicInteger callbacks = new AtomicInteger();

        manager.commitOffsetsAsync(
                sampleOffsets(),
                (ignored, error) -> {
                    callbacks.incrementAndGet();
                    callbackError.complete(error);
                });

        CommonTestUtils.waitUntil(
                () -> {
                    manager.invokeCompletedOffsetCommitCallbacks();
                    return callbackError.isDone();
                },
                Duration.ofSeconds(2),
                "Async commit timeout callback was not delivered");
        assertThat(callbackError.get(2, TimeUnit.SECONDS))
                .isInstanceOf(org.apache.fluss.exception.TimeoutException.class);
        response.complete(new CommitConsumerOffsetsResponse());
        manager.invokeCompletedOffsetCommitCallbacks();
        assertThat(callbacks).hasValue(1);
        manager.close();
    }

    @Test
    void testCloseWaitsForPendingCommitAndInvokesCallbacksOnClosingThread() throws Exception {
        CompletableFuture<FindConsumerGroupOwnerResponse> discovery = new CompletableFuture<>();
        CompletableFuture<CommitConsumerOffsetsResponse> response = new CompletableFuture<>();
        TestTabletServerGateway gateway = gateway(request -> discovery, request -> response);
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);
        CompletableFuture<Thread> firstCallback = new CompletableFuture<>();
        CompletableFuture<Thread> secondCallback = new CompletableFuture<>();
        manager.commitOffsetsAsync(
                Collections.emptyMap(),
                (offsets, error) -> {
                    assertThat(error).isNull();
                    firstCallback.complete(Thread.currentThread());
                });
        manager.commitOffsetsAsync(
                sampleOffsets(),
                (offsets, error) -> {
                    assertThat(error).isNull();
                    secondCallback.complete(Thread.currentThread());
                });

        FutureTask<Void> closing = new FutureTask<>(manager::close, null);
        Thread closingThread = new Thread(closing, "consumer-group-close");
        closingThread.start();
        try {
            assertThat(firstCallback.get(5, TimeUnit.SECONDS)).isSameAs(closingThread);
            assertThat(closing).isNotDone();
            assertThat(secondCallback).isNotDone();
        } finally {
            discovery.complete(ownerResponse(TestingMetadataUpdater.NODE2));
            response.complete(new CommitConsumerOffsetsResponse());
        }
        closing.get(5, TimeUnit.SECONDS);
        assertThat(secondCallback).isCompletedWithValue(closingThread);
    }

    @Test
    void testCloseDeadlineDropsUnfinishedCommitCallback() throws Exception {
        CompletableFuture<CommitConsumerOffsetsResponse> response = new CompletableFuture<>();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> response);
        ManualClock clock = new ManualClock();
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 10L);
        Duration elapsed = conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT);
        manager.commitOffsetsAsync(
                Collections.emptyMap(), (offsets, error) -> clock.advanceTime(elapsed));
        AtomicInteger callbacks = new AtomicInteger();
        manager.commitOffsetsAsync(
                sampleOffsets(), (offsets, error) -> callbacks.incrementAndGet());

        CompletableFuture.runAsync(manager::close).get(5, TimeUnit.SECONDS);
        response.complete(new CommitConsumerOffsetsResponse());
        manager.close();
        manager.invokeCompletedOffsetCommitCallbacks();

        assertThat(callbacks).hasValue(0);
    }

    @Test
    void testCloseSkipsAutoCommitBeforeIntervalElapses() {
        conf.set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED, true);
        ManualClock clock = new ManualClock();
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf,
                        metadataUpdater(new TestTabletServerGateway(false, Collections.emptySet())),
                        logScannerStatus,
                        clock,
                        10L);
        clock.advanceTime(
                conf.get(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL).minusMillis(1));
        manager.close();
        assertThat(logScannerStatus.snapshots).isZero();
    }

    @Test
    void testCloseSkipsCallbacksWhenFinalAutoCommitExhaustsTimeout() {
        conf.set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED, true);
        ManualClock clock = new ManualClock();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> {
                            clock.advanceTime(conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT));
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 10L);
        AtomicInteger callbacks = new AtomicInteger();
        manager.commitOffsetsAsync(
                Collections.emptyMap(), (offsets, error) -> callbacks.incrementAndGet());
        Duration interval = conf.get(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL);
        clock.advanceTime(interval);

        manager.close();

        assertThat(clock.milliseconds())
                .isEqualTo(
                        interval.plus(conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT)).toMillis());
        assertThat(callbacks).hasValue(0);
    }

    @Test
    void testEmptyCommitCompletesWithoutRpc() throws Exception {
        AtomicInteger commits = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> {
                            commits.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        ConsumerGroupClient manager =
                new ConsumerGroupClient(conf, metadataUpdater(gateway), logScannerStatus);
        AtomicInteger callbacks = new AtomicInteger();
        CompletableFuture<Exception> callbackError = new CompletableFuture<>();

        manager.commitOffsetsAsync(
                Collections.emptyMap(),
                (offsets, error) -> {
                    callbacks.incrementAndGet();
                    callbackError.complete(error);
                });

        manager.invokeCompletedOffsetCommitCallbacks();
        assertThat(callbackError.get(5, TimeUnit.SECONDS)).isNull();
        assertThat(callbacks).hasValue(1);
        assertThat(commits).hasValue(0);
        assertThat(manager.commitOffsetsSync(Collections.emptyMap(), 1L)).isTrue();
    }

    @Test
    void testCommitSyncRetriesRetriableFailure() {
        AtomicInteger commits = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> {
                            if (commits.getAndIncrement() == 0) {
                                return completedExceptionally(
                                        new ConsumerGroupOwnerNotAvailableException(
                                                "owner unavailable"));
                            }
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        TickingClock clock = new TickingClock();
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 10L);

        assertThat(manager.commitOffsetsSync(sampleOffsets(), 100L)).isTrue();
        assertThat(commits).hasValue(2);
    }

    @Test
    void testCommitSyncRecoversAfterOwnerMove() {
        AtomicInteger discoveries = new AtomicInteger();
        AtomicInteger commits = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request -> {
                            int count = discoveries.incrementAndGet();
                            ServerNode owner =
                                    count == 1
                                            ? TestingMetadataUpdater.NODE2
                                            : TestingMetadataUpdater.NODE3;
                            return CompletableFuture.completedFuture(ownerResponse(owner));
                        },
                        request -> {
                            if (commits.getAndIncrement() == 0) {
                                return CompletableFuture.completedFuture(
                                        errorResponse(
                                                Errors.NOT_CONSUMER_GROUP_OWNER, "owner moved"));
                            }
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        TickingClock clock = new TickingClock();
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 10L);

        // First commit fails (NOT_CONSUMER_GROUP_OWNER) → owner invalidated → rediscovery
        // finds NODE3 → second commit succeeds.
        assertThat(manager.commitOffsetsSync(sampleOffsets(), 100L)).isTrue();
        assertThat(commits).hasValue(2);
        assertThat(discoveries).hasValue(2);
    }

    @Test
    void testFetchRecoversAfterOwnerMove() {
        AtomicInteger discoveries = new AtomicInteger();
        AtomicInteger fetches = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request -> {
                            int count = discoveries.incrementAndGet();
                            ServerNode owner =
                                    count == 1
                                            ? TestingMetadataUpdater.NODE2
                                            : TestingMetadataUpdater.NODE3;
                            return CompletableFuture.completedFuture(ownerResponse(owner));
                        },
                        request ->
                                CompletableFuture.completedFuture(
                                        new CommitConsumerOffsetsResponse()),
                        request -> {
                            if (fetches.getAndIncrement() == 0) {
                                return CompletableFuture.completedFuture(
                                        fetchErrorResponse(Errors.NOT_CONSUMER_GROUP_OWNER));
                            }
                            return CompletableFuture.completedFuture(fetchResponse());
                        });
        TickingClock clock = new TickingClock();
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 10L);

        Map<TableBucket, OffsetMetadata> offsets =
                manager.fetchCommittedOffsets(
                        Collections.singleton(new TableBucket(1L, null, 0)), 100L);
        assertThat(offsets).isNotEmpty();
        assertThat(fetches).hasValue(2);
        assertThat(discoveries).hasValue(2);
    }

    @Test
    void testCommitSyncRecoversWhenOwnerUnreachable() {
        AtomicInteger discoveries = new AtomicInteger();
        AtomicInteger commits = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request -> {
                            int count = discoveries.incrementAndGet();
                            ServerNode owner =
                                    count == 1
                                            ? TestingMetadataUpdater.NODE2
                                            : TestingMetadataUpdater.NODE3;
                            return CompletableFuture.completedFuture(ownerResponse(owner));
                        },
                        request -> {
                            if (commits.getAndIncrement() == 0) {
                                // Owner is down during failover — RPC layer wraps this as
                                // NetworkException, which must trigger owner invalidation.
                                return completedExceptionally(
                                        new NetworkException("owner unreachable"));
                            }
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        TickingClock clock = new TickingClock();
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 10L);

        assertThat(manager.commitOffsetsSync(sampleOffsets(), 100L)).isTrue();
        assertThat(commits).hasValue(2);
        assertThat(discoveries).hasValue(2);
    }

    @Test
    void testFetchCommittedOffsetsEncodesRequestAndDecodesResponse() {
        AtomicReference<GetConsumerOffsetsRequest> captured = new AtomicReference<>();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request ->
                                CompletableFuture.completedFuture(
                                        new CommitConsumerOffsetsResponse()),
                        request -> {
                            captured.set(request);
                            return CompletableFuture.completedFuture(fetchResponse());
                        });
        TickingClock clock = new TickingClock();
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 10L);
        Set<TableBucket> buckets =
                new java.util.LinkedHashSet<>(
                        Arrays.asList(
                                new TableBucket(2L, null, 3),
                                new TableBucket(1L, 5L, 2),
                                new TableBucket(1L, null, 1)));

        Map<TableBucket, OffsetMetadata> offsets = manager.fetchCommittedOffsets(buckets, 100L);

        GetConsumerOffsetsRequest request = captured.get();
        assertThat(request.getGroupId()).isEqualTo("group-1");
        assertThat(request.isAllOffsets()).isFalse();
        assertThat(request.getTablesList()).hasSize(2);
        assertThat(request.getTablesList())
                .extracting(table -> table.getTableId())
                .containsExactlyInAnyOrder(1L, 2L);
        PbGetConsumerOffsetsRequestTable table1 =
                request.getTablesList().stream()
                        .filter(t -> t.getTableId() == 1L)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("table 1 not found"));
        assertThat(table1.getBucketsList())
                .extracting(bucket -> bucket.getBucketId())
                .containsExactlyInAnyOrder(1, 2);
        assertThat(offsets)
                .containsEntry(new TableBucket(1L, null, 1), new OffsetMetadata(11L, "first"))
                .containsEntry(new TableBucket(1L, 5L, 2), new OffsetMetadata(12L, "partitioned"));
        assertThat(offsets).doesNotContainKey(new TableBucket(2L, null, 3));
        assertThatThrownBy(offsets::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testFetchTimeoutReusesSameInFlightRequest() {
        CompletableFuture<GetConsumerOffsetsResponse> response = new CompletableFuture<>();
        AtomicInteger fetches = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request ->
                                CompletableFuture.completedFuture(
                                        new CommitConsumerOffsetsResponse()),
                        request -> {
                            fetches.incrementAndGet();
                            return response;
                        });
        TickingClock clock = new TickingClock();
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 0L);
        Set<TableBucket> buckets = Collections.singleton(new TableBucket(1L, null, 0));

        // The first call sends the fetch and times out waiting for the shared response.
        assertThat(manager.fetchCommittedOffsets(buckets, 2L)).isNull();
        assertThat(fetches).hasValue(1);
        response.complete(fetchResponse());
        assertThat(manager.fetchCommittedOffsets(buckets, 100L)).isNotNull();
        assertThat(fetches).hasValue(1);
    }

    @Test
    void testFetchOwnerErrorInvalidatesCachedOwner() throws Exception {
        AtomicInteger discoveries = new AtomicInteger();
        AtomicInteger fetches = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request -> {
                            discoveries.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    ownerResponse(TestingMetadataUpdater.NODE2));
                        },
                        request ->
                                CompletableFuture.completedFuture(
                                        new CommitConsumerOffsetsResponse()),
                        request ->
                                CompletableFuture.completedFuture(
                                        fetches.getAndIncrement() == 0
                                                ? fetchErrorResponse(
                                                        Errors.NOT_CONSUMER_GROUP_OWNER)
                                                : fetchResponse()));
        TickingClock clock = new TickingClock();
        ConsumerGroupClient manager =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 10L);

        assertThat(
                        manager.fetchCommittedOffsets(
                                Collections.singleton(new TableBucket(1L, null, 0)), 100L))
                .isNotNull();
        assertThat(discoveries).hasValue(2);
        assertThat(fetches).hasValue(2);
    }

    @Test
    void testAutoCommitHonorsConfigurationAndInterval() {
        AtomicInteger commits = new AtomicInteger();
        TestTabletServerGateway gateway =
                gateway(
                        request ->
                                CompletableFuture.completedFuture(
                                        ownerResponse(TestingMetadataUpdater.NODE2)),
                        request -> {
                            commits.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new CommitConsumerOffsetsResponse());
                        });
        // nextAutoCommitDeadlineMs = 0 + 50 = 50 because ManualClock starts at 0.
        ManualClock clock = new ManualClock();
        Configuration autoCommitConf =
                new Configuration(conf)
                        .set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED, true)
                        .set(
                                ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL,
                                Duration.ofMillis(50));
        ConsumerGroupClient enabled =
                new ConsumerGroupClient(
                        autoCommitConf, metadataUpdater(gateway), logScannerStatus, clock, 10L);

        clock.advanceTime(49L, TimeUnit.MILLISECONDS);
        assertThat(enabled.maybeAutoCommitOffsetsAsync()).isFalse();
        assertThat(logScannerStatus.snapshots).isZero();
        clock.advanceTime(1L, TimeUnit.MILLISECONDS);
        assertThat(enabled.maybeAutoCommitOffsetsAsync()).isTrue();
        assertThat(logScannerStatus.snapshots).isEqualTo(1);
        clock.advanceTime(49L, TimeUnit.MILLISECONDS);
        assertThat(enabled.maybeAutoCommitOffsetsAsync()).isFalse();
        assertThat(logScannerStatus.snapshots).isEqualTo(1);
        clock.advanceTime(1L, TimeUnit.MILLISECONDS);
        enabled.close();
        clock.advanceTime(50L, TimeUnit.MILLISECONDS);
        enabled.close();
        assertThat(logScannerStatus.snapshots).isEqualTo(2);
        assertThat(commits).hasValue(2);

        ConsumerGroupClient disabled =
                new ConsumerGroupClient(
                        conf, metadataUpdater(gateway), logScannerStatus, clock, 10L);
        assertThat(disabled.maybeAutoCommitOffsetsAsync()).isFalse();
        disabled.close();
        assertThat(logScannerStatus.snapshots).isEqualTo(2);
        assertThat(commits).hasValue(2);
    }

    private static Map<TableBucket, OffsetMetadata> sampleOffsets() {
        Map<TableBucket, OffsetMetadata> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, null, 0), new OffsetMetadata(10L, "metadata"));
        return offsets;
    }

    private static CommitConsumerOffsetsResponse errorResponse(Errors error, String message) {
        return new CommitConsumerOffsetsResponse()
                .addAllTables(
                        Collections.singletonList(
                                new PbCommitConsumerOffsetsResponseTable()
                                        .setTableId(1L)
                                        .addAllBuckets(
                                                Collections.singletonList(
                                                        new PbCommitConsumerOffsetsResponseBucket()
                                                                .setBucketId(0)
                                                                .setErrorCode(error.code())
                                                                .setErrorMessage(message)))));
    }

    private static GetConsumerOffsetsResponse fetchResponse() {
        return new GetConsumerOffsetsResponse()
                .addAllTables(
                        Collections.singletonList(
                                new PbGetConsumerOffsetsResponseTable()
                                        .setTableId(1L)
                                        .addAllBuckets(
                                                Arrays.asList(
                                                        new PbGetConsumerOffsetsResponseBucket()
                                                                .setBucketId(1)
                                                                .setOffset(11L)
                                                                .setMetadata("first"),
                                                        new PbGetConsumerOffsetsResponseBucket()
                                                                .setPartitionId(5L)
                                                                .setBucketId(2)
                                                                .setOffset(12L)
                                                                .setMetadata("partitioned")))))
                .addAllTables(
                        Collections.singletonList(
                                new PbGetConsumerOffsetsResponseTable()
                                        .setTableId(2L)
                                        .addAllBuckets(
                                                Collections.singletonList(
                                                        new PbGetConsumerOffsetsResponseBucket()
                                                                .setBucketId(3)))));
    }

    private static GetConsumerOffsetsResponse fetchErrorResponse(Errors error) {
        return new GetConsumerOffsetsResponse()
                .addAllTables(
                        Collections.singletonList(
                                new PbGetConsumerOffsetsResponseTable()
                                        .setTableId(1L)
                                        .addAllBuckets(
                                                Collections.singletonList(
                                                        new PbGetConsumerOffsetsResponseBucket()
                                                                .setBucketId(0)
                                                                .setErrorCode(error.code())))));
    }

    private static TestTabletServerGateway discoveryGateway(OwnerRequestHandler ownerHandler) {
        return new TestTabletServerGateway(false, Collections.emptySet()) {
            @Override
            public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                    FindConsumerGroupOwnerRequest request) {
                return ownerHandler.handle(request);
            }
        };
    }

    private static TestTabletServerGateway gateway(
            OwnerRequestHandler ownerHandler, CommitRequestHandler commitHandler) {
        return gateway(
                ownerHandler,
                commitHandler,
                request -> CompletableFuture.completedFuture(new GetConsumerOffsetsResponse()));
    }

    private static TestTabletServerGateway gateway(
            OwnerRequestHandler ownerHandler,
            CommitRequestHandler commitHandler,
            FetchRequestHandler fetchHandler) {
        return new TestTabletServerGateway(false, Collections.emptySet()) {
            @Override
            public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                    FindConsumerGroupOwnerRequest request) {
                return ownerHandler.handle(request);
            }

            @Override
            public CompletableFuture<CommitConsumerOffsetsResponse> commitConsumerOffsets(
                    CommitConsumerOffsetsRequest request) {
                return commitHandler.handle(request);
            }

            @Override
            public CompletableFuture<GetConsumerOffsetsResponse> getConsumerOffsets(
                    GetConsumerOffsetsRequest request) {
                return fetchHandler.handle(request);
            }
        };
    }

    private static TestingMetadataUpdater metadataUpdater(TestTabletServerGateway gateway) {
        Map<Integer, TestTabletServerGateway> gateways = new HashMap<>();
        gateways.put(TestingMetadataUpdater.NODE1.id(), gateway);
        gateways.put(TestingMetadataUpdater.NODE2.id(), gateway);
        gateways.put(TestingMetadataUpdater.NODE3.id(), gateway);
        return new TestingMetadataUpdater(
                TestingMetadataUpdater.COORDINATOR,
                Arrays.asList(
                        TestingMetadataUpdater.NODE1,
                        TestingMetadataUpdater.NODE2,
                        TestingMetadataUpdater.NODE3),
                Collections.emptyMap(),
                gateways,
                new Configuration());
    }

    private static FindConsumerGroupOwnerResponse ownerResponse(ServerNode owner) {
        return new FindConsumerGroupOwnerResponse()
                .setOwner(
                        new PbServerNode()
                                .setNodeId(owner.id())
                                .setHost(owner.host())
                                .setPort(owner.port()));
    }

    private interface OwnerRequestHandler {
        CompletableFuture<FindConsumerGroupOwnerResponse> handle(
                FindConsumerGroupOwnerRequest request);
    }

    private interface CommitRequestHandler {
        CompletableFuture<CommitConsumerOffsetsResponse> handle(
                CommitConsumerOffsetsRequest request);
    }

    private interface FetchRequestHandler {
        CompletableFuture<GetConsumerOffsetsResponse> handle(GetConsumerOffsetsRequest request);
    }

    private static final class TestingLogScannerStatus extends LogScannerStatus {
        private int snapshots;

        @Override
        public Map<TableBucket, OffsetMetadata> allConsumed() {
            snapshots++;
            return sampleOffsets();
        }
    }

    private static final class TickingClock implements Clock {
        private long nowNanos;

        @Override
        public long milliseconds() {
            return TimeUnit.NANOSECONDS.toMillis(nowNanos);
        }

        @Override
        public long nanoseconds() {
            nowNanos += TimeUnit.MILLISECONDS.toNanos(1L);
            return nowNanos;
        }
    }
}
