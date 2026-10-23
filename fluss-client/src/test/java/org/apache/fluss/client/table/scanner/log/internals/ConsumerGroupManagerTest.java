/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ConsumerGroupManager}. */
class ConsumerGroupManagerTest {

    @Test
    void testOwnerDiscoveryIsCachedAndCanBeInvalidated() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<ServerNode> owner = new AtomicReference<>(TestingMetadataUpdater.NODE2);
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                            FindConsumerGroupOwnerRequest request) {
                        requests.incrementAndGet();
                        ServerNode currentOwner = owner.get();
                        return CompletableFuture.completedFuture(ownerResponse(currentOwner));
                    }
                };
        TestingMetadataUpdater metadataUpdater = metadataUpdater(gateway);
        ConsumerGroupManager manager = new ConsumerGroupManager("group-1", metadataUpdater);

        assertOwner(manager.getOrDiscoverOwner().get(), TestingMetadataUpdater.NODE2);
        assertOwner(manager.getOrDiscoverOwner().get(), TestingMetadataUpdater.NODE2);
        assertThat(requests).hasValue(1);
        assertThat(manager.cachedOwnerGateway())
                .isSameAs(
                        metadataUpdater.newTabletServerClientForNode(
                                TestingMetadataUpdater.NODE2.id()));

        owner.set(TestingMetadataUpdater.NODE3);
        manager.markOwnerUnknown();
        assertOwner(manager.getOrDiscoverOwner().get(), TestingMetadataUpdater.NODE3);
        assertThat(requests).hasValue(2);
    }

    @Test
    void testConcurrentDiscoverySharesOneRequest() throws Exception {
        CompletableFuture<FindConsumerGroupOwnerResponse> rpcResponse = new CompletableFuture<>();
        AtomicInteger requests = new AtomicInteger();
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                            FindConsumerGroupOwnerRequest request) {
                        requests.incrementAndGet();
                        return rpcResponse;
                    }
                };
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));

        int callers = 4;
        CyclicBarrier startBarrier = new CyclicBarrier(callers);
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        try {
            List<Future<CompletableFuture<ServerNode>>> submitted = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                submitted.add(
                        executor.submit(
                                () -> {
                                    startBarrier.await();
                                    return manager.getOrDiscoverOwner();
                                }));
            }

            List<CompletableFuture<ServerNode>> discoveries = new ArrayList<>();
            for (Future<CompletableFuture<ServerNode>> caller : submitted) {
                discoveries.add(caller.get());
            }

            // The first caller publishes one discovery; the others must reuse it instead of
            // each sending their own FindConsumerGroupOwner request.
            assertThat(discoveries.stream().distinct().count()).isEqualTo(1);
            rpcResponse.complete(ownerResponse(TestingMetadataUpdater.NODE2));
            for (CompletableFuture<ServerNode> discovery : discoveries) {
                assertOwner(discovery.get(), TestingMetadataUpdater.NODE2);
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
        ConsumerGroupManager manager = new ConsumerGroupManager("group-1", metadataUpdater);

        assertThatThrownBy(() -> manager.getOrDiscoverOwner().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ConsumerGroupOwnerNotAvailableException.class);
    }

    @Test
    void testDecodeFailureIsNotWrappedInCompletionException() {
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                            FindConsumerGroupOwnerRequest request) {
                        // The response violates the owner contract, so decoding must fail.
                        return CompletableFuture.completedFuture(
                                new FindConsumerGroupOwnerResponse());
                    }
                };
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));

        AtomicReference<Throwable> error = new AtomicReference<>();
        manager.getOrDiscoverOwner().whenComplete((owner, e) -> error.set(e));

        // Async composition must observe the declared failure type instead of the
        // CompletionException wrapper that thenApply adds around decode errors.
        assertThat(error.get()).isInstanceOf(ConsumerGroupOwnerNotAvailableException.class);
    }

    @Test
    void testDecodeFailureOnIncompleteOwner() {
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                            FindConsumerGroupOwnerRequest request) {
                        // The owner is present but its host and port are missing.
                        return CompletableFuture.completedFuture(
                                new FindConsumerGroupOwnerResponse()
                                        .setOwner(new PbServerNode().setNodeId(1)));
                    }
                };
        ConsumerGroupManager manager =
                new ConsumerGroupManager("group-1", metadataUpdater(gateway));

        AtomicReference<Throwable> error = new AtomicReference<>();
        manager.getOrDiscoverOwner().whenComplete((owner, e) -> error.set(e));

        assertThat(error.get())
                .isInstanceOf(ConsumerGroupOwnerNotAvailableException.class)
                .hasMessageContaining("incomplete owner");
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

    private static void assertOwner(ServerNode actual, ServerNode expected) {
        assertThat(actual.id()).isEqualTo(expected.id());
        assertThat(actual.host()).isEqualTo(expected.host());
        assertThat(actual.port()).isEqualTo(expected.port());
    }
}
