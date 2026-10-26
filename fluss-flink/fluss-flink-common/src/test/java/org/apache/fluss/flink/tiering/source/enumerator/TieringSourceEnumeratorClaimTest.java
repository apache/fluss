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

import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.client.tiering.TieringSplitGenerator;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests claim lifetime across asynchronous callbacks and reader failover. */
class TieringSourceEnumeratorClaimTest {
    @ParameterizedTest
    @ValueSource(strings = {"queued", "generating", "empty", "failure", "pending"})
    void testClaimLifetimeAcrossCallbacks(String timing) throws Exception {
        AtomicReference<Runnable> failover = new AtomicReference<>();
        AtomicInteger claims = new AtomicInteger();
        AtomicInteger generations = new AtomicInteger();
        TestAdminAdapter admin =
                new TestAdminAdapter() {
                    @Override
                    public CompletableFuture<TableInfo> getTableInfo(TablePath path) {
                        return CompletableFuture.completedFuture(DATA1_TABLE_INFO);
                    }

                    @Override
                    public void close() {}
                };
        TieringSplitGenerator generator =
                new TieringSplitGenerator(admin) {
                    @Override
                    public List<TieringSplit> generateTableSplits(TableInfo info) {
                        if (generations.getAndIncrement() == 0
                                && !timing.equals("queued")
                                && !timing.equals("pending")) {
                            failover.get().run();
                            if (timing.equals("failure")) {
                                throw new IllegalStateException("Old split generation failed");
                            }
                            if (timing.equals("empty")) {
                                return Collections.emptyList();
                            }
                        }
                        return Collections.singletonList(
                                new TieringLogSplit(
                                        info.getTablePath(),
                                        new TableBucket(info.getTableId(), 0),
                                        null,
                                        0,
                                        1));
                    }
                };
        TestCoordinatorGateway gateway =
                new TestCoordinatorGateway() {
                    @Override
                    public CompletableFuture<LakeTieringHeartbeatResponse> lakeTieringHeartbeat(
                            LakeTieringHeartbeatRequest request) {
                        LakeTieringHeartbeatResponse response =
                                new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1);
                        if (request.hasRequestTable() && request.isRequestTable()) {
                            response.setTieringTable()
                                    .setTableId(DATA1_TABLE_INFO.getTableId())
                                    .setTieringEpoch(claims.incrementAndGet())
                                    .setTablePath()
                                    .setDatabaseName(
                                            DATA1_TABLE_INFO.getTablePath().getDatabaseName())
                                    .setTableName(DATA1_TABLE_INFO.getTablePath().getTableName());
                        }
                        return CompletableFuture.completedFuture(response);
                    }
                };
        try (DeferredContext context = new DeferredContext();
                TieringSourceEnumerator enumerator =
                        new TieringSourceEnumerator(
                                new Configuration(),
                                new Configuration(),
                                context,
                                new TestingLakeTieringFactory(),
                                1000,
                                new ManuallyTriggeredScheduledExecutorService())) {
            enumerator.start(gateway, admin, generator);
            failover.set(
                    () -> {
                        context.registerSourceReader(0, 1, "localhost");
                        enumerator.addReader(0);
                        // Claim the new epoch before the old callback resumes publishing or
                        // failing.
                        context.requests.remove().run();
                    });
            context.registerSourceReader(0, 0, "localhost");
            enumerator.addReader(0);
            enumerator.handleSplitRequest(0, "localhost");
            context.requests.remove().run();
            if (timing.equals("pending")) {
                // A second heartbeat may run while the first claim's callback is queued.
                enumerator.handleSplitRequest(0, "localhost");
                context.requests.remove().run();
                assertThat(claims.get()).isOne();
            } else if (timing.equals("queued")) {
                failover.get().run();
            }
            context.callbacks.remove().run();
            if (timing.equals("pending")) {
                assertThat(context.getSplitsAssignmentSequence()).hasSize(1);
            } else {
                assertThat(context.getSplitsAssignmentSequence()).isEmpty();
                // Stale success, empty results, and failures must preserve the new claim.
                context.callbacks.remove().run();
                assertThat(claims.get()).isEqualTo(2);
                assertThat(context.getSplitsAssignmentSequence()).hasSize(1);
            }
        }
    }

    private static class DeferredContext
            extends FlussMockSplitEnumeratorContext<FlinkTieringSplit> {
        private final Queue<Runnable> requests = new ArrayDeque<>();
        private final Queue<Runnable> callbacks = new ArrayDeque<>();

        private DeferredContext() {
            super(1);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            requests.add(
                    () -> {
                        try {
                            T result = callable.call();
                            callbacks.add(() -> handler.accept(result, null));
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    });
        }

        @Override
        public <T> void callAsync(
                Callable<T> callable, BiConsumer<T, Throwable> handler, long delay, long period) {}
    }
}
