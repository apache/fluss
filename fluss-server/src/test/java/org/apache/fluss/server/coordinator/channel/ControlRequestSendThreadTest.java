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

package org.apache.fluss.server.coordinator.channel;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.TestMetricGroup;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ControlRequestSendThread}. */
class ControlRequestSendThreadTest {

    private static final int TABLET_SERVER_ID = 1;
    private static final int EPOCH = 1;

    private ControlRequestSendThread thread;

    @AfterEach
    void afterEach() throws InterruptedException {
        if (thread != null) {
            thread.initiateShutdown();
            thread.awaitShutdown();
        }
    }

    /** U1: happy path — single item dequeued, dispatched, callback invoked. */
    @Test
    void testHappyPath() throws Exception {
        BlockingQueue<QueueItem> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        StopReplicaResponse response = new StopReplicaResponse();
        AtomicInteger invocationCount = new AtomicInteger(0);
        TabletServerGateway gateway =
                stubGateway(
                        req -> {
                            invocationCount.incrementAndGet();
                            return CompletableFuture.completedFuture(response);
                        });

        AtomicReference<Object> callbackResponse = new AtomicReference<>();
        CountDownLatch callbackLatch = new CountDownLatch(1);

        thread = createThread(queue, () -> Optional.of(gateway), () -> EPOCH, metricGroup);
        thread.start();

        queue.put(
                new QueueItem(
                        ApiKeys.STOP_REPLICA,
                        new StopReplicaRequest(),
                        (resp, err) -> {
                            callbackResponse.set(resp);
                            callbackLatch.countDown();
                        },
                        EPOCH,
                        System.currentTimeMillis(),
                        null));

        assertThat(callbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(callbackResponse.get()).isSameAs(response);
        assertThat(getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount()).isEqualTo(0);
        assertThat(getCounter(metricGroup, MetricNames.SENDER_STALE_DROP_COUNT).getCount())
                .isEqualTo(0);
        assertThat(getGaugeValue(metricGroup, MetricNames.SENDER_ALIVE)).isEqualTo(1);
        assertThat(invocationCount.get()).isEqualTo(1);
    }

    /** U2: retry then succeed — gateway fails once, succeeds on second attempt. */
    @Test
    void testRetryThenSucceed() throws Exception {
        BlockingQueue<QueueItem> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        StopReplicaResponse response = new StopReplicaResponse();
        AtomicInteger callCount = new AtomicInteger(0);
        TabletServerGateway gateway =
                stubGateway(
                        req -> {
                            if (callCount.incrementAndGet() == 1) {
                                CompletableFuture<StopReplicaResponse> fail =
                                        new CompletableFuture<>();
                                fail.completeExceptionally(new RuntimeException("transient error"));
                                return fail;
                            }
                            return CompletableFuture.completedFuture(response);
                        });

        CountDownLatch callbackLatch = new CountDownLatch(1);

        thread = createThread(queue, () -> Optional.of(gateway), () -> EPOCH, metricGroup);
        thread.start();

        queue.put(
                new QueueItem(
                        ApiKeys.STOP_REPLICA,
                        new StopReplicaRequest(),
                        (resp, err) -> callbackLatch.countDown(),
                        EPOCH,
                        System.currentTimeMillis(),
                        null));

        assertThat(callbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount())
                .isGreaterThanOrEqualTo(1);
    }

    /** U4: stale-epoch drop — item with older epoch is dropped, staleDropCount increments. */
    @Test
    void testStaleEpochDrop() throws Exception {
        BlockingQueue<QueueItem> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        StopReplicaResponse response = new StopReplicaResponse();
        TabletServerGateway gateway =
                stubGateway(req -> CompletableFuture.completedFuture(response));

        CountDownLatch sentinelLatch = new CountDownLatch(1);

        thread = createThread(queue, () -> Optional.of(gateway), () -> 5, metricGroup);
        thread.start();

        // stale item (epoch 1 < current 5)
        queue.put(
                new QueueItem(
                        ApiKeys.STOP_REPLICA,
                        new StopReplicaRequest(),
                        (resp, err) -> {},
                        1,
                        System.currentTimeMillis(),
                        null));

        // sentinel item with current epoch to detect when stale item was processed
        queue.put(
                new QueueItem(
                        ApiKeys.STOP_REPLICA,
                        new StopReplicaRequest(),
                        (resp, err) -> sentinelLatch.countDown(),
                        5,
                        System.currentTimeMillis(),
                        null));

        assertThat(sentinelLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(getCounter(metricGroup, MetricNames.SENDER_STALE_DROP_COUNT).getCount())
                .isEqualTo(1);
    }

    /** U5: gateway absent then present — retries until gateway becomes available. */
    @Test
    void testGatewayAbsentThenPresent() throws Exception {
        BlockingQueue<QueueItem> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        StopReplicaResponse response = new StopReplicaResponse();
        TabletServerGateway gateway =
                stubGateway(req -> CompletableFuture.completedFuture(response));

        AtomicInteger gatewayCallCount = new AtomicInteger(0);
        Supplier<Optional<TabletServerGateway>> gatewaySupplier =
                () -> {
                    if (gatewayCallCount.incrementAndGet() <= 3) {
                        return Optional.empty();
                    }
                    return Optional.of(gateway);
                };

        CountDownLatch callbackLatch = new CountDownLatch(1);

        thread = createThread(queue, gatewaySupplier, () -> EPOCH, metricGroup);
        thread.start();

        queue.put(
                new QueueItem(
                        ApiKeys.STOP_REPLICA,
                        new StopReplicaRequest(),
                        (resp, err) -> callbackLatch.countDown(),
                        EPOCH,
                        System.currentTimeMillis(),
                        null));

        assertThat(callbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount())
                .isGreaterThanOrEqualTo(3);
    }

    /** U7: shutdown cancels in-flight — shutdown completes quickly, callback never invoked. */
    @Test
    void testShutdownCancelsInFlight() throws Exception {
        BlockingQueue<QueueItem> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        CountDownLatch invokedLatch = new CountDownLatch(1);
        CompletableFuture<StopReplicaResponse> neverCompleteFuture = new CompletableFuture<>();
        TabletServerGateway gateway =
                stubGateway(
                        req -> {
                            invokedLatch.countDown();
                            return neverCompleteFuture;
                        });

        AtomicInteger callbackCount = new AtomicInteger(0);

        thread = createThread(queue, () -> Optional.of(gateway), () -> EPOCH, metricGroup);
        thread.start();

        queue.put(
                new QueueItem(
                        ApiKeys.STOP_REPLICA,
                        new StopReplicaRequest(),
                        (resp, err) -> callbackCount.incrementAndGet(),
                        EPOCH,
                        System.currentTimeMillis(),
                        null));

        assertThat(invokedLatch.await(5, TimeUnit.SECONDS)).isTrue();

        long shutdownStart = System.currentTimeMillis();
        thread.shutdown();
        long shutdownDuration = System.currentTimeMillis() - shutdownStart;

        assertThat(shutdownDuration).isLessThan(2000);
        assertThat(callbackCount.get()).isEqualTo(0);
        assertThat(getGaugeValue(metricGroup, MetricNames.SENDER_ALIVE)).isEqualTo(0);
        thread = null;
    }

    /** U8: callback throws after successful send — thread survives, no re-send. */
    @Test
    void testCallbackThrowsDoesNotRetry() throws Exception {
        BlockingQueue<QueueItem> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        StopReplicaResponse response = new StopReplicaResponse();
        AtomicInteger invocationCount = new AtomicInteger(0);
        TabletServerGateway gateway =
                stubGateway(
                        req -> {
                            invocationCount.incrementAndGet();
                            return CompletableFuture.completedFuture(response);
                        });

        CountDownLatch secondCallbackLatch = new CountDownLatch(1);

        thread = createThread(queue, () -> Optional.of(gateway), () -> EPOCH, metricGroup);
        thread.start();

        // first item: callback throws
        queue.put(
                new QueueItem(
                        ApiKeys.STOP_REPLICA,
                        new StopReplicaRequest(),
                        (resp, err) -> {
                            throw new RuntimeException("buggy callback");
                        },
                        EPOCH,
                        System.currentTimeMillis(),
                        null));

        // second item: proves thread survived
        queue.put(
                new QueueItem(
                        ApiKeys.STOP_REPLICA,
                        new StopReplicaRequest(),
                        (resp, err) -> secondCallbackLatch.countDown(),
                        EPOCH,
                        System.currentTimeMillis(),
                        null));

        assertThat(secondCallbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        // gateway invoked exactly twice (once per item, no re-send from callback failure)
        assertThat(invocationCount.get()).isEqualTo(2);
        assertThat(getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount()).isEqualTo(0);
        assertThat(getGaugeValue(metricGroup, MetricNames.SENDER_ALIVE)).isEqualTo(1);
    }

    private static TabletServerGateway stubGateway(
            Function<StopReplicaRequest, CompletableFuture<StopReplicaResponse>> stopReplicaFn) {
        return (TabletServerGateway)
                Proxy.newProxyInstance(
                        TabletServerGateway.class.getClassLoader(),
                        new Class<?>[] {TabletServerGateway.class},
                        (proxy, method, args) -> {
                            if ("stopReplica".equals(method.getName())) {
                                return stopReplicaFn.apply((StopReplicaRequest) args[0]);
                            }
                            throw new UnsupportedOperationException(
                                    "Stub does not support: " + method.getName());
                        });
    }

    private ControlRequestSendThread createThread(
            BlockingQueue<QueueItem> queue,
            Supplier<Optional<TabletServerGateway>> gatewaySupplier,
            IntSupplier epochSupplier,
            MetricGroup metricGroup) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REQUEST_RETRY_BACKOFF, java.time.Duration.ofMillis(10));
        return new ControlRequestSendThread(
                TABLET_SERVER_ID, queue, gatewaySupplier, epochSupplier, conf, metricGroup);
    }

    private static Counter getCounter(MetricGroup group, String name) {
        return (Counter) ((TestMetricGroup) group).getMetric(name);
    }

    @SuppressWarnings("unchecked")
    private static int getGaugeValue(MetricGroup group, String name) {
        return ((Gauge<Integer>) ((TestMetricGroup) group).getMetric(name)).getValue();
    }
}
