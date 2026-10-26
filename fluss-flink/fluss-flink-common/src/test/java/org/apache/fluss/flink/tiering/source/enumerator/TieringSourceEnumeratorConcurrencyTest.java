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

package org.apache.fluss.flink.tiering.source.enumerator;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Concurrency tests for heartbeat claims and enumerator lifecycle events. */
class TieringSourceEnumeratorConcurrencyTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSlowHeartbeatDoesNotBlockEventsAndCloseReleasesLateClaim(boolean readerFailover)
            throws Exception {
        BlockingGateway gateway = new BlockingGateway();
        CountDownLatch closeStarted = new CountDownLatch(1);
        ManuallyTriggeredScheduledExecutorService timer =
                new ManuallyTriggeredScheduledExecutorService() {
                    @Override
                    public List<Runnable> shutdownNow() {
                        closeStarted.countDown();
                        return super.shutdownNow();
                    }
                };
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try (FlussMockSplitEnumeratorContext<FlinkTieringSplit> context =
                        new FlussMockSplitEnumeratorContext<>(1);
                TieringSourceEnumerator enumerator =
                        new TieringSourceEnumerator(
                                new Configuration(),
                                new Configuration(),
                                context,
                                new TestingLakeTieringFactory(),
                                1000,
                                timer)) {
            try {
                // No split generation is needed: the delayed claim is released during shutdown.
                enumerator.start(gateway, null, null);
                context.registerSourceReader(0, 0, "localhost");
                enumerator.addReader(0);
                Future<?> heartbeat =
                        executor.submit(
                                () -> {
                                    try {
                                        context.runPeriodicCallable(0);
                                    } catch (Throwable t) {
                                        throw new AssertionError(t);
                                    }
                                });
                assertThat(gateway.requestStarted.await(10, TimeUnit.SECONDS)).isTrue();

                // These events must complete while the RPC response is still blocked.
                executor.submit(
                                () -> {
                                    if (readerFailover) {
                                        context.registerSourceReader(0, 1, "localhost");
                                    }
                                    enumerator.addReader(0);
                                    enumerator.handleSourceEvent(0, new FinishedTieringEvent(123));
                                    enumerator.handleSplitRequest(0, "localhost");
                                })
                        .get(10, TimeUnit.SECONDS);
                assertThat(gateway.requests).hasSize(2);
                assertThat(gateway.response).isNotDone();

                Future<?> closing =
                        executor.submit(
                                () -> {
                                    enumerator.close();
                                    return null;
                                });
                assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
                executor.submit(() -> enumerator.addReader(0)).get(10, TimeUnit.SECONDS);
                assertThat(closing.isDone()).isFalse();

                LakeTieringHeartbeatResponse claimed =
                        new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1);
                claimed.setTieringTable()
                        .setTableId(42)
                        .setTieringEpoch(7)
                        .setTablePath()
                        .setDatabaseName("db")
                        .setTableName("late");
                gateway.response.complete(claimed);
                heartbeat.get(10, TimeUnit.SECONDS);
                closing.get(10, TimeUnit.SECONDS);
                enumerator.close();
                assertThat(gateway.requests).hasSize(3);
                assertThat(gateway.requests.get(2).getFailedTablesList())
                        .singleElement()
                        .satisfies(
                                table -> {
                                    assertThat(table.getTableId()).isEqualTo(42);
                                    assertThat(table.getTieringEpoch()).isEqualTo(7);
                                });
                assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            } finally {
                gateway.response.complete(
                        new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1));
            }
        } finally {
            gateway.response.complete(new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1));
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static class BlockingGateway extends TestCoordinatorGateway {
        private final List<LakeTieringHeartbeatRequest> requests = new CopyOnWriteArrayList<>();
        private final CountDownLatch requestStarted = new CountDownLatch(1);
        private final CompletableFuture<LakeTieringHeartbeatResponse> response =
                new CompletableFuture<>();

        @Override
        public CompletableFuture<LakeTieringHeartbeatResponse> lakeTieringHeartbeat(
                LakeTieringHeartbeatRequest request) {
            requests.add(request);
            if (requests.size() == 2) {
                requestStarted.countDown();
                return response;
            }
            return CompletableFuture.completedFuture(
                    new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1));
        }
    }
}
