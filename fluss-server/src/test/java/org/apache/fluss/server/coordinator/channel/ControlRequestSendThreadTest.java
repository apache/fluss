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
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.TestMetricGroup;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void testHappyPath() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        ApiVersionsResponse response = new ApiVersionsResponse();
        AtomicInteger invocationCount = new AtomicInteger(0);
        TabletServerGateway gateway = unusedGateway();

        AtomicReference<Object> callbackResponse = new AtomicReference<>();
        CountDownLatch callbackLatch = new CountDownLatch(1);

        thread = createThread(queue, () -> Optional.of(gateway), () -> EPOCH, metricGroup);
        thread.start();

        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> {
                            invocationCount.incrementAndGet();
                            return CompletableFuture.completedFuture(response);
                        },
                        (resp, err) -> {
                            callbackResponse.set(resp);
                            callbackLatch.countDown();
                        },
                        EPOCH,
                        System.currentTimeMillis()));

        assertThat(callbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(callbackResponse.get()).isSameAs(response);
        assertThat(getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount()).isEqualTo(0);
        assertThat(getCounter(metricGroup, MetricNames.SENDER_STALE_DROP_COUNT).getCount())
                .isEqualTo(0);
        assertThat(getGaugeValue(metricGroup, MetricNames.SENDER_ALIVE)).isEqualTo(1);
        assertThat(invocationCount.get()).isEqualTo(1);
    }

    @Test
    void testRetryThenSucceed() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        ApiVersionsResponse response = new ApiVersionsResponse();
        AtomicInteger callCount = new AtomicInteger(0);
        TabletServerGateway gateway = unusedGateway();

        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicInteger invalidationCount = new AtomicInteger(0);

        thread =
                createThread(
                        queue,
                        () -> Optional.of(gateway),
                        () -> {
                            invalidationCount.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        },
                        () -> EPOCH,
                        metricGroup,
                        Duration.ofSeconds(30));
        thread.start();

        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> {
                            if (callCount.incrementAndGet() == 1) {
                                CompletableFuture<ApiVersionsResponse> fail =
                                        new CompletableFuture<>();
                                fail.completeExceptionally(new NetworkException("transient error"));
                                return fail;
                            }
                            return CompletableFuture.completedFuture(response);
                        },
                        (resp, err) -> callbackLatch.countDown(),
                        EPOCH,
                        System.currentTimeMillis()));

        assertThat(callbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount())
                .isGreaterThanOrEqualTo(1);
        assertThat(invalidationCount.get()).isEqualTo(1);
    }

    @Test
    void testNonRetriableFailureAdvancesQueue() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();
        TabletServerGateway gateway = unusedGateway();

        IllegalStateException permanentFailure = new IllegalStateException("invalid request");
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        AtomicInteger firstRequestInvocations = new AtomicInteger(0);
        CountDownLatch secondCallbackLatch = new CountDownLatch(1);

        thread = createThread(queue, () -> Optional.of(gateway), () -> EPOCH, metricGroup);
        thread.start();

        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> {
                            firstRequestInvocations.incrementAndGet();
                            throw permanentFailure;
                        },
                        (response, failure) -> callbackFailure.set(failure),
                        EPOCH,
                        System.currentTimeMillis()));
        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> CompletableFuture.completedFuture(new ApiVersionsResponse()),
                        (response, failure) -> secondCallbackLatch.countDown(),
                        EPOCH,
                        System.currentTimeMillis()));

        assertThat(secondCallbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(callbackFailure.get()).isSameAs(permanentFailure);
        assertThat(firstRequestInvocations.get()).isEqualTo(1);
        assertThat(getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount()).isEqualTo(0);
    }

    @Test
    void testTimeoutInvalidatesConnectionBeforeRetry() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();
        TabletServerGateway gateway = unusedGateway();

        CompletableFuture<ApiVersionsResponse> firstAttempt = new CompletableFuture<>();
        ApiVersionsResponse response = new ApiVersionsResponse();
        AtomicInteger invocationCount = new AtomicInteger(0);
        AtomicInteger invalidationCount = new AtomicInteger(0);
        CountDownLatch callbackLatch = new CountDownLatch(1);

        thread =
                createThread(
                        queue,
                        () -> Optional.of(gateway),
                        () -> {
                            invalidationCount.incrementAndGet();
                            firstAttempt.completeExceptionally(
                                    new NetworkException("connection invalidated"));
                            return CompletableFuture.completedFuture(null);
                        },
                        () -> EPOCH,
                        metricGroup,
                        Duration.ofMillis(20));
        thread.start();

        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored ->
                                invocationCount.incrementAndGet() == 1
                                        ? firstAttempt
                                        : CompletableFuture.completedFuture(response),
                        (callbackResponse, failure) -> callbackLatch.countDown(),
                        EPOCH,
                        System.currentTimeMillis()));

        assertThat(callbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(firstAttempt.isCompletedExceptionally()).isTrue();
        assertThat(invalidationCount.get()).isEqualTo(1);
        assertThat(invocationCount.get()).isEqualTo(2);
    }

    @Test
    void testConnectionInvalidationFailureRetainsQueueHead() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();
        TabletServerGateway gateway = unusedGateway();
        ApiVersionsResponse response = new ApiVersionsResponse();
        AtomicInteger sendCount = new AtomicInteger();
        AtomicInteger invalidationCount = new AtomicInteger();
        AtomicReference<ApiVersionsResponse> callbackResponse = new AtomicReference<>();
        CountDownLatch callbackLatch = new CountDownLatch(1);

        thread =
                createThread(
                        queue,
                        () -> Optional.of(gateway),
                        () -> {
                            if (invalidationCount.incrementAndGet() == 1) {
                                CompletableFuture<Void> failure = new CompletableFuture<>();
                                failure.completeExceptionally(
                                        new NetworkException("invalidation failed"));
                                return failure;
                            }
                            return CompletableFuture.completedFuture(null);
                        },
                        () -> EPOCH,
                        metricGroup,
                        Duration.ofSeconds(30));
        thread.start();

        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> {
                            if (sendCount.incrementAndGet() == 1) {
                                CompletableFuture<ApiVersionsResponse> failure =
                                        new CompletableFuture<>();
                                failure.completeExceptionally(
                                        new NetworkException("request failed"));
                                return failure;
                            }
                            return CompletableFuture.completedFuture(response);
                        },
                        (callbackResult, failure) -> {
                            callbackResponse.set(callbackResult);
                            callbackLatch.countDown();
                        },
                        EPOCH,
                        System.currentTimeMillis()));

        assertThat(callbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(callbackResponse.get()).isSameAs(response);
        assertThat(sendCount.get()).isEqualTo(2);
        assertThat(invalidationCount.get()).isEqualTo(2);
    }

    @Test
    void testConnectionInvalidationTimeoutDoesNotResendRequest() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();
        TabletServerGateway gateway = unusedGateway();
        CompletableFuture<ApiVersionsResponse> firstAttempt = new CompletableFuture<>();
        CompletableFuture<Void> invalidationFuture = new CompletableFuture<>();
        ApiVersionsResponse response = new ApiVersionsResponse();
        AtomicInteger sendCount = new AtomicInteger();
        AtomicInteger invalidationCount = new AtomicInteger();
        CountDownLatch invalidationStarted = new CountDownLatch(1);
        CountDownLatch callbackLatch = new CountDownLatch(1);

        thread =
                createThread(
                        queue,
                        () -> Optional.of(gateway),
                        () -> {
                            invalidationCount.incrementAndGet();
                            invalidationStarted.countDown();
                            return invalidationFuture;
                        },
                        () -> EPOCH,
                        metricGroup,
                        Duration.ofMillis(20));
        thread.start();

        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored ->
                                sendCount.incrementAndGet() == 1
                                        ? firstAttempt
                                        : CompletableFuture.completedFuture(response),
                        (callbackResponse, failure) -> callbackLatch.countDown(),
                        EPOCH,
                        System.currentTimeMillis()));

        assertThat(invalidationStarted.await(5, TimeUnit.SECONDS)).isTrue();
        waitUntil(
                () -> getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount() > 0,
                Duration.ofSeconds(5),
                "The invalidation wait did not time out");
        assertThat(sendCount.get()).isEqualTo(1);
        assertThat(invalidationCount.get()).isEqualTo(1);
        assertThat(callbackLatch.getCount()).isEqualTo(1);

        firstAttempt.completeExceptionally(new NetworkException("connection invalidated"));
        invalidationFuture.complete(null);

        assertThat(callbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(sendCount.get()).isEqualTo(2);
    }

    @Test
    void testAsyncRequestErrorIsRethrown() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();
        AssertionError error = new AssertionError("fatal request error");
        CompletableFuture<ApiVersionsResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(error);

        thread = createThread(queue, () -> Optional.of(unusedGateway()), () -> EPOCH, metricGroup);
        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> failedFuture,
                        (response, failure) -> {},
                        EPOCH,
                        System.currentTimeMillis()));

        assertThatThrownBy(thread::doWork).isSameAs(error);
    }

    @Test
    void testAsyncConnectionInvalidationErrorIsRethrown() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();
        CompletableFuture<ApiVersionsResponse> requestFailure = new CompletableFuture<>();
        requestFailure.completeExceptionally(new NetworkException("transient error"));
        AssertionError error = new AssertionError("fatal invalidation error");
        CompletableFuture<Void> invalidationFailure = new CompletableFuture<>();
        invalidationFailure.completeExceptionally(error);

        thread =
                createThread(
                        queue,
                        () -> Optional.of(unusedGateway()),
                        () -> invalidationFailure,
                        () -> EPOCH,
                        metricGroup,
                        Duration.ofSeconds(30));
        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> requestFailure,
                        (response, failure) -> {},
                        EPOCH,
                        System.currentTimeMillis()));

        assertThatThrownBy(thread::doWork).isSameAs(error);
    }

    @Test
    void testStaleEpochDrop() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        ApiVersionsResponse response = new ApiVersionsResponse();
        TabletServerGateway gateway = unusedGateway();

        CountDownLatch sentinelLatch = new CountDownLatch(1);

        thread = createThread(queue, () -> Optional.of(gateway), () -> 5, metricGroup);
        thread.start();

        // stale item (epoch 1 < current 5)
        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> CompletableFuture.completedFuture(response),
                        (resp, err) -> {},
                        1,
                        System.currentTimeMillis()));

        // sentinel item with current epoch to detect when stale item was processed
        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> CompletableFuture.completedFuture(response),
                        (resp, err) -> sentinelLatch.countDown(),
                        5,
                        System.currentTimeMillis()));

        assertThat(sentinelLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(getCounter(metricGroup, MetricNames.SENDER_STALE_DROP_COUNT).getCount())
                .isEqualTo(1);
    }

    @Test
    void testGatewayAbsentThenPresent() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        ApiVersionsResponse response = new ApiVersionsResponse();
        TabletServerGateway gateway = unusedGateway();

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
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> CompletableFuture.completedFuture(response),
                        (resp, err) -> callbackLatch.countDown(),
                        EPOCH,
                        System.currentTimeMillis()));

        assertThat(callbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount())
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    void testShutdownCancelsInFlight() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        CountDownLatch invokedLatch = new CountDownLatch(1);
        CompletableFuture<ApiVersionsResponse> neverCompleteFuture = new CompletableFuture<>();
        TabletServerGateway gateway = unusedGateway();

        AtomicInteger callbackCount = new AtomicInteger(0);

        thread = createThread(queue, () -> Optional.of(gateway), () -> EPOCH, metricGroup);
        thread.start();

        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> {
                            invokedLatch.countDown();
                            return neverCompleteFuture;
                        },
                        (resp, err) -> callbackCount.incrementAndGet(),
                        EPOCH,
                        System.currentTimeMillis()));

        assertThat(invokedLatch.await(5, TimeUnit.SECONDS)).isTrue();

        long shutdownStart = System.currentTimeMillis();
        thread.initiateShutdown();
        thread.awaitShutdown();
        long shutdownDuration = System.currentTimeMillis() - shutdownStart;

        assertThat(shutdownDuration).isLessThan(2000);
        assertThat(callbackCount.get()).isEqualTo(0);
        assertThat(getGaugeValue(metricGroup, MetricNames.SENDER_ALIVE)).isEqualTo(0);
        thread = null;
    }

    @Test
    void testCallbackThrowsDoesNotRetry() throws Exception {
        BlockingQueue<QueueItem<?>> queue = new LinkedBlockingQueue<>();
        MetricGroup metricGroup = TestMetricGroup.createTestMetricGroup();

        ApiVersionsResponse response = new ApiVersionsResponse();
        AtomicInteger invocationCount = new AtomicInteger(0);
        TabletServerGateway gateway = unusedGateway();

        CountDownLatch secondCallbackLatch = new CountDownLatch(1);

        thread = createThread(queue, () -> Optional.of(gateway), () -> EPOCH, metricGroup);
        thread.start();

        // first item: callback throws
        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> {
                            invocationCount.incrementAndGet();
                            return CompletableFuture.completedFuture(response);
                        },
                        (resp, err) -> {
                            throw new RuntimeException("buggy callback");
                        },
                        EPOCH,
                        System.currentTimeMillis()));

        // second item: proves thread survived
        queue.put(
                new QueueItem<>(
                        ApiKeys.API_VERSIONS,
                        ignored -> {
                            invocationCount.incrementAndGet();
                            return CompletableFuture.completedFuture(response);
                        },
                        (resp, err) -> secondCallbackLatch.countDown(),
                        EPOCH,
                        System.currentTimeMillis()));

        assertThat(secondCallbackLatch.await(5, TimeUnit.SECONDS)).isTrue();
        // request sender invoked exactly twice (once per item, no re-send from callback failure)
        assertThat(invocationCount.get()).isEqualTo(2);
        assertThat(getCounter(metricGroup, MetricNames.SENDER_RETRY_COUNT).getCount()).isEqualTo(0);
        assertThat(getGaugeValue(metricGroup, MetricNames.SENDER_ALIVE)).isEqualTo(1);
    }

    private static TabletServerGateway unusedGateway() {
        return (TabletServerGateway)
                Proxy.newProxyInstance(
                        TabletServerGateway.class.getClassLoader(),
                        new Class<?>[] {TabletServerGateway.class},
                        (proxy, method, args) -> {
                            throw new UnsupportedOperationException(
                                    "Unexpected gateway call: " + method.getName());
                        });
    }

    private ControlRequestSendThread createThread(
            BlockingQueue<QueueItem<?>> queue,
            Supplier<Optional<TabletServerGateway>> gatewaySupplier,
            IntSupplier epochSupplier,
            MetricGroup metricGroup) {
        return createThread(
                queue,
                gatewaySupplier,
                () -> CompletableFuture.completedFuture(null),
                epochSupplier,
                metricGroup,
                Duration.ofSeconds(30));
    }

    private ControlRequestSendThread createThread(
            BlockingQueue<QueueItem<?>> queue,
            Supplier<Optional<TabletServerGateway>> gatewaySupplier,
            Supplier<CompletableFuture<Void>> connectionInvalidator,
            IntSupplier epochSupplier,
            MetricGroup metricGroup,
            Duration requestTimeout) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_CONTROL_REQUEST_RETRY_BACKOFF, Duration.ofMillis(10));
        conf.set(ConfigOptions.COORDINATOR_CONTROL_REQUEST_TIMEOUT, requestTimeout);
        return new ControlRequestSendThread(
                TABLET_SERVER_ID,
                queue,
                gatewaySupplier,
                connectionInvalidator,
                epochSupplier,
                conf,
                metricGroup);
    }

    private static Counter getCounter(MetricGroup group, String name) {
        return (Counter) ((TestMetricGroup) group).getMetric(name);
    }

    @SuppressWarnings("unchecked")
    private static int getGaugeValue(MetricGroup group, String name) {
        return ((Gauge<Integer>) ((TestMetricGroup) group).getMetric(name)).getValue();
    }
}
