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
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Per-tablet-server sender thread that drains the control-plane request queue and retries on
 * transient RPC failures. Mirrors Kafka's {@code RequestSendThread}
 * (ControllerChannelManager.scala:226-300).
 *
 * <p>Each invocation of {@link #doWork()} takes one {@link QueueItem} from the queue. Stale items
 * (whose {@code coordinatorEpoch} is less than the current epoch) are dropped immediately.
 * Otherwise the item is sent to the tablet server via the gateway, retrying with a configurable
 * backoff until the send succeeds or the thread is shut down.
 *
 * <p>The callback is invoked in its own {@code try/catch} OUTSIDE the retry loop (alignment fix F),
 * so a buggy response handler cannot re-enter the send retry path and cannot reach the outer
 * Scenario-E catch.
 */
public class ControlRequestSendThread extends ShutdownableThread {

    private static final int HISTOGRAM_WINDOW_SIZE = 100;

    private final int tabletServerId;
    private final BlockingQueue<QueueItem> queue;
    private final Supplier<Optional<TabletServerGateway>> gatewaySupplier;
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
            BlockingQueue<QueueItem> queue,
            Supplier<Optional<TabletServerGateway>> gatewaySupplier,
            IntSupplier epochSupplier,
            Configuration conf,
            MetricGroup metricGroup) {
        super("coordinator-control-request-sender-" + tabletServerId, true);
        this.tabletServerId = tabletServerId;
        this.queue = queue;
        this.gatewaySupplier = gatewaySupplier;
        this.epochSupplier = epochSupplier;
        this.backoffMs = conf.get(ConfigOptions.COORDINATOR_REQUEST_RETRY_BACKOFF).toMillis();
        this.requestTimeoutMs = conf.get(ConfigOptions.COORDINATOR_REQUEST_TIMEOUT).toMillis();

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
        super.run();
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
    public void shutdown() throws InterruptedException {
        try {
            super.shutdown();
        } finally {
            aliveFlag.set(0);
        }
    }

    @Override
    public void doWork() throws Exception {
        try {
            QueueItem item = queue.take();
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

            ApiMessage response = null;
            boolean sendSuccessful = false;
            while (isRunning() && !sendSuccessful) {
                Optional<TabletServerGateway> gatewayOpt = gatewaySupplier.get();
                if (!gatewayOpt.isPresent()) {
                    retryCount.inc();
                    backoff();
                    continue;
                }
                try {
                    CompletableFuture<? extends ApiMessage> future = invoke(gatewayOpt.get(), item);
                    inFlight.set(future);
                    response = future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
                    sendSuccessful = true;
                } catch (Throwable t) {
                    retryCount.inc();
                    log.warn(
                            "Failed to send {} to tabletServer {}; will retry after {}ms",
                            item.getApiKey(),
                            tabletServerId,
                            backoffMs,
                            t);
                    backoff();
                } finally {
                    inFlight.set(null);
                }
            }

            if (sendSuccessful && item.getCallback() != null) {
                try {
                    item.getCallback().accept(response, null);
                } catch (Throwable t) {
                    log.error(
                            "Callback for {} to tabletServer {} threw; the request itself "
                                    + "succeeded so no retry is attempted.",
                            item.getApiKey(),
                            tabletServerId,
                            t);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            log.error("Unexpected error in {}; thread continues", getName(), t);
        }
    }

    private void backoff() throws InterruptedException {
        pause(backoffMs, TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<? extends ApiMessage> invoke(
            TabletServerGateway gateway, QueueItem item) {
        switch (item.getApiKey()) {
            case STOP_REPLICA:
                return gateway.stopReplica((StopReplicaRequest) item.getRequest());
            default:
                throw new IllegalArgumentException("Unsupported ApiKey: " + item.getApiKey());
        }
    }
}
