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
import org.apache.fluss.exception.DisconnectException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static org.apache.fluss.utils.ExceptionUtils.stripCompletionException;
import static org.apache.fluss.utils.ExceptionUtils.stripExecutionException;

/**
 * Per-tablet-server sender thread that drains the control-plane request queue and retries on
 * transient RPC failures.
 *
 * <p>Each invocation of {@link #doWork()} takes one {@link QueueItem} from the queue. Stale items
 * (whose {@code coordinatorEpoch} is less than the current epoch) are dropped immediately.
 * Otherwise the item is sent to the tablet server via the gateway, retrying with a configurable
 * backoff after transport failures until the send succeeds or the thread is shut down. Before a
 * retry, the old connection is closed so its in-flight request cannot accumulate in the RPC layer.
 * Explicit server rejections and local request construction failures are completed through the
 * callback without retrying, allowing the next FIFO item to make progress.
 *
 * <p>The callback is invoked outside the retry loop so a response-handler failure does not retry a
 * request that the tablet server has already completed.
 */
public class ControlRequestSendThread extends ShutdownableThread {

    private static final int HISTOGRAM_WINDOW_SIZE = 100;

    private final int tabletServerId;
    private final BlockingQueue<QueueItem<?>> queue;
    private final Supplier<Optional<TabletServerGateway>> gatewaySupplier;
    private final Supplier<CompletableFuture<Void>> connectionInvalidator;
    private final IntSupplier epochSupplier;
    private final long backoffMs;
    private final long requestTimeoutMs;

    private final Histogram queueTimeMsHistogram;
    private final Counter retryCount;
    private final Counter staleDropCount;
    private final AtomicInteger aliveFlag;

    private final AtomicReference<CompletableFuture<? extends ApiMessage>> inFlight =
            new AtomicReference<>();

    public ControlRequestSendThread(
            int tabletServerId,
            BlockingQueue<QueueItem<?>> queue,
            Supplier<Optional<TabletServerGateway>> gatewaySupplier,
            Supplier<CompletableFuture<Void>> connectionInvalidator,
            IntSupplier epochSupplier,
            Configuration conf,
            MetricGroup metricGroup) {
        super("coordinator-control-request-sender-" + tabletServerId, true);
        this.tabletServerId = tabletServerId;
        this.queue = queue;
        this.gatewaySupplier = gatewaySupplier;
        this.connectionInvalidator = connectionInvalidator;
        this.epochSupplier = epochSupplier;
        this.backoffMs =
                conf.get(ConfigOptions.COORDINATOR_CONTROL_REQUEST_RETRY_BACKOFF).toMillis();
        this.requestTimeoutMs =
                conf.get(ConfigOptions.COORDINATOR_CONTROL_REQUEST_TIMEOUT).toMillis();

        this.queueTimeMsHistogram =
                metricGroup.histogram(
                        MetricNames.SENDER_QUEUE_TIME_MS,
                        new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
        this.retryCount = metricGroup.counter(MetricNames.SENDER_RETRY_COUNT);
        this.staleDropCount = metricGroup.counter(MetricNames.SENDER_STALE_DROP_COUNT);
        this.aliveFlag = new AtomicInteger(0);
        metricGroup.gauge(MetricNames.SENDER_ALIVE, aliveFlag::get);
    }

    @Override
    public void run() {
        aliveFlag.set(1);
        try {
            super.run();
        } finally {
            aliveFlag.set(0);
        }
    }

    @Override
    public boolean initiateShutdown() {
        boolean initiated = super.initiateShutdown();
        if (initiated) {
            CompletableFuture<? extends ApiMessage> f = inFlight.getAndSet(null);
            if (f != null) {
                f.cancel(true);
            }
        }
        return initiated;
    }

    @Override
    public void doWork() throws Exception {
        try {
            QueueItem<?> item = queue.take();
            queueTimeMsHistogram.update(System.currentTimeMillis() - item.getEnqueueTimeMs());

            int currentEpoch = epochSupplier.getAsInt();
            if (item.getCoordinatorEpoch() < currentEpoch) {
                staleDropCount.inc();
                log.warn(
                        "Dropping stale {} for tabletServer {}: itemEpoch={} < currentEpoch={}",
                        item.getApiKey(),
                        tabletServerId,
                        item.getCoordinatorEpoch(),
                        currentEpoch);
                return;
            }

            send(item);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private <ResponseT extends ApiMessage> void send(QueueItem<ResponseT> item)
            throws InterruptedException {
        while (isRunning()) {
            Optional<TabletServerGateway> gatewayOpt;
            try {
                gatewayOpt = gatewaySupplier.get();
            } catch (RuntimeException e) {
                Throwable failure = unwrapFailure(e);
                log.error(
                        "Failed to resolve the gateway for {} to tabletServer {}; "
                                + "the request will not be retried",
                        item.getApiKey(),
                        tabletServerId,
                        failure);
                invokeCallback(item, null, failure);
                return;
            }
            if (!gatewayOpt.isPresent()) {
                retryCount.inc();
                backoff();
                continue;
            }

            CompletableFuture<ResponseT> future = null;
            ResponseT response;
            Throwable failure;
            try {
                future = item.send(gatewayOpt.get());
                inFlight.set(future);
                response = future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
                invokeCallback(item, response, null);
                return;
            } catch (TimeoutException e) {
                failure = e;
            } catch (ExecutionException e) {
                failure = unwrapFailure(e);
            } catch (RuntimeException e) {
                failure = unwrapFailure(e);
            } finally {
                if (future != null) {
                    inFlight.compareAndSet(future, null);
                }
            }

            if (!isRunning()) {
                return;
            }

            if (!isRetriableTransportFailure(failure)) {
                log.error(
                        "Failed to send {} to tabletServer {}; the failure is not retriable",
                        item.getApiKey(),
                        tabletServerId,
                        failure);
                invokeCallback(item, null, failure);
                return;
            }

            if (!invalidateConnection(item)) {
                return;
            }

            retryCount.inc();
            log.warn(
                    "Failed to send {} to tabletServer {}; will retry after {}ms",
                    item.getApiKey(),
                    tabletServerId,
                    backoffMs,
                    failure);
            backoff();
        }
    }

    private boolean invalidateConnection(QueueItem<?> item) throws InterruptedException {
        CompletableFuture<Void> invalidationFuture = null;
        while (isRunning()) {
            if (invalidationFuture == null) {
                try {
                    invalidationFuture = connectionInvalidator.get();
                } catch (RuntimeException e) {
                    logInvalidationFailure(item, unwrapFailure(e));
                    retryCount.inc();
                    backoff();
                    continue;
                }
            }

            try {
                invalidationFuture.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
                return true;
            } catch (TimeoutException e) {
                log.warn(
                        "Timed out waiting to invalidate the connection to tabletServer {} after "
                                + "{}; the queue head will be retained and invalidation will be "
                                + "checked again after {}ms",
                        tabletServerId,
                        item.getApiKey(),
                        backoffMs,
                        e);
            } catch (ExecutionException e) {
                logInvalidationFailure(item, unwrapFailure(e));
                invalidationFuture = null;
            } catch (RuntimeException e) {
                logInvalidationFailure(item, unwrapFailure(e));
                invalidationFuture = null;
            }

            retryCount.inc();
            backoff();
        }
        return false;
    }

    private void logInvalidationFailure(QueueItem<?> item, Throwable failure) {
        log.warn(
                "Failed to invalidate the connection to tabletServer {} after {}; the queue head "
                        + "will be retained and invalidation will be retried after {}ms",
                tabletServerId,
                item.getApiKey(),
                backoffMs,
                failure);
    }

    private static Throwable unwrapFailure(Throwable failure) {
        Throwable unwrapped = failure;
        Throwable previous;
        do {
            previous = unwrapped;
            unwrapped = stripCompletionException(stripExecutionException(unwrapped));
        } while (unwrapped != previous);

        if (unwrapped instanceof Error) {
            throw (Error) unwrapped;
        }
        return unwrapped;
    }

    private static boolean isRetriableTransportFailure(Throwable failure) {
        return failure instanceof TimeoutException
                || failure instanceof org.apache.fluss.exception.TimeoutException
                || failure instanceof NetworkException
                || failure instanceof DisconnectException;
    }

    private <ResponseT extends ApiMessage> void invokeCallback(
            QueueItem<ResponseT> item, ResponseT response, Throwable failure) {
        if (item.getCallback() == null) {
            return;
        }
        try {
            item.getCallback().accept(response, failure);
        } catch (RuntimeException e) {
            Throwable callbackFailure = unwrapFailure(e);
            log.error(
                    "Callback for {} to tabletServer {} threw; the request will not be retried.",
                    item.getApiKey(),
                    tabletServerId,
                    callbackFailure);
        }
    }

    private void backoff() throws InterruptedException {
        pause(backoffMs, TimeUnit.MILLISECONDS);
    }
}
