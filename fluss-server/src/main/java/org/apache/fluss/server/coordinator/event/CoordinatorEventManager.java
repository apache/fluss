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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * A manager for the events happens in Coordinator Server. It will poll the event from a queue and
 * then process it.
 */
@Internal
public final class CoordinatorEventManager implements EventManager {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEventManager.class);

    private static final String COORDINATOR_EVENT_THREAD_NAME = "coordinator-event-thread";

    private final EventProcessor eventProcessor;
    private final CoordinatorMetricGroup coordinatorMetricGroup;

    private final LinkedBlockingQueue<QueuedEvent> queue = new LinkedBlockingQueue<>();
    private final CoordinatorEventThread thread =
            new CoordinatorEventThread(COORDINATOR_EVENT_THREAD_NAME);
    private final Lock putLock = new ReentrantLock();

    // metrics
    private Histogram eventProcessTime;
    private Histogram eventQueueTime;

    private static final int WINDOW_SIZE = 100;

    public CoordinatorEventManager(
            EventProcessor eventProcessor, CoordinatorMetricGroup coordinatorMetricGroup) {
        this.eventProcessor = eventProcessor;
        this.coordinatorMetricGroup = coordinatorMetricGroup;
        registerMetrics();
    }

    private void registerMetrics() {
        coordinatorMetricGroup.gauge(MetricNames.EVENT_QUEUE_SIZE, queue::size);

        eventProcessTime =
                coordinatorMetricGroup.histogram(
                        MetricNames.EVENT_PROCESS_TIME_MS,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));

        eventQueueTime =
                coordinatorMetricGroup.histogram(
                        MetricNames.EVENT_QUEUE_TIME_MS,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));
    }

    public void start() {
        thread.start();
    }

    public void close() {
        try {
            thread.initiateShutdown();
            clearAndPut(new ShutdownEventThreadEvent());
            thread.awaitShutdown();
        } catch (InterruptedException e) {
            LOG.error("Fail to close coordinator event thread.");
        }
    }

    public void put(CoordinatorEvent event) {
        inLock(
                putLock,
                () -> {
                    try {
                        QueuedEvent queuedEvent =
                                new QueuedEvent(event, System.currentTimeMillis());
                        queue.put(queuedEvent);
                    } catch (InterruptedException e) {
                        LOG.error("Fail to put coordinator event {}.", event, e);
                    }
                });
    }

    public void clearAndPut(CoordinatorEvent event) {
        inLock(
                putLock,
                () -> {
                    queue.clear();
                    put(event);
                });
    }

    private class CoordinatorEventThread extends ShutdownableThread {

        public CoordinatorEventThread(String name) {
            super(name, false);
        }

        @Override
        public void doWork() throws Exception {
            QueuedEvent queuedEvent = queue.take();
            CoordinatorEvent coordinatorEvent = queuedEvent.event;

            long eventStartTimeMs = System.currentTimeMillis();

            try {
                if (!(coordinatorEvent instanceof ShutdownEventThreadEvent)) {
                    eventQueueTime.update(System.currentTimeMillis() - queuedEvent.enqueueTimeMs);
                    eventProcessor.process(coordinatorEvent);
                }
            } catch (Throwable e) {
                log.error("Uncaught error processing event {}.", coordinatorEvent, e);
            } finally {
                long eventFinishTimeMs = System.currentTimeMillis();
                eventProcessTime.update(eventFinishTimeMs - eventStartTimeMs);
            }
        }
    }

    private static class QueuedEvent {
        private final CoordinatorEvent event;
        private final long enqueueTimeMs;

        public QueuedEvent(CoordinatorEvent event, long enqueueTimeMs) {
            this.event = event;
            this.enqueueTimeMs = enqueueTimeMs;
        }
    }
}
