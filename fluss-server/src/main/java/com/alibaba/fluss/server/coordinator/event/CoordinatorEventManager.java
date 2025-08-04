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

package com.alibaba.fluss.server.coordinator.event;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableBucketReplica;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metrics.DescriptiveStatisticsHistogram;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.server.coordinator.CoordinatorContext;
import com.alibaba.fluss.server.coordinator.statemachine.ReplicaState;
import com.alibaba.fluss.server.metrics.group.CoordinatorMetricGroup;
import com.alibaba.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionSuccessful;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

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
    private Histogram eventProcessingTime;
    private Histogram eventQueueTime;

    // Coordinator metrics moved from CoordinatorEventProcessor
    private volatile int tabletServerCount;
    private volatile int offlineBucketCount;
    private volatile int tableCount;
    private volatile int bucketCount;
    private volatile int replicasToDeleteCount;

    private static final int WINDOW_SIZE = 100;
    private static final long METRICS_UPDATE_INTERVAL_MS = 5000; // 5 seconds

    public CoordinatorEventManager(
            EventProcessor eventProcessor, CoordinatorMetricGroup coordinatorMetricGroup) {
        this.eventProcessor = eventProcessor;
        this.coordinatorMetricGroup = coordinatorMetricGroup;
        registerMetrics();
    }

    private void registerMetrics() {
        coordinatorMetricGroup.gauge(MetricNames.EVENT_QUEUE_SIZE, queue::size);

        eventProcessingTime =
                coordinatorMetricGroup.histogram(
                        MetricNames.EVENT_PROCESSING_TIME_MS,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));

        eventQueueTime =
                coordinatorMetricGroup.histogram(
                        MetricNames.EVENT_QUEUE_TIME_MS,
                        new DescriptiveStatisticsHistogram(WINDOW_SIZE));

        // Register coordinator metrics
        coordinatorMetricGroup.gauge(MetricNames.ACTIVE_COORDINATOR_COUNT, () -> 1);
        coordinatorMetricGroup.gauge(
                MetricNames.ACTIVE_TABLET_SERVER_COUNT, () -> tabletServerCount);
        coordinatorMetricGroup.gauge(MetricNames.OFFLINE_BUCKET_COUNT, () -> offlineBucketCount);
        coordinatorMetricGroup.gauge(MetricNames.BUCKET_COUNT, () -> bucketCount);
        coordinatorMetricGroup.gauge(MetricNames.TABLE_COUNT, () -> tableCount);
        coordinatorMetricGroup.gauge(
                MetricNames.REPLICAS_TO_DELETE_COUNT, () -> replicasToDeleteCount);
    }

    /** Not thread safety! this method can only be executed in the CoordinatorEventThread. */
    private void updateMetricsViaAccessContext() {
        // Create AccessContextEvent to safely access CoordinatorContext
        AccessContextEvent<MetricsData> accessContextEvent =
                new AccessContextEvent<>(
                        context -> {
                            int tabletServerCount = context.getLiveTabletServers().size();
                            int tableCount = context.allTables().size();
                            int bucketCount = context.bucketLeaderAndIsr().size();
                            int offlineBucketCount = context.getOfflineBucketCount();

                            int replicasToDeletes = 0;
                            // for replica in partitions to be deleted
                            for (TablePartition tablePartition :
                                    context.getPartitionsToBeDeleted()) {
                                for (TableBucketReplica replica :
                                        context.getAllReplicasForPartition(
                                                tablePartition.getTableId(),
                                                tablePartition.getPartitionId())) {
                                    replicasToDeletes =
                                            isReplicaToDelete(replica, context)
                                                    ? replicasToDeletes + 1
                                                    : replicasToDeletes;
                                }
                            }
                            // for replica in tables to be deleted
                            for (long tableId : context.getTablesToBeDeleted()) {
                                for (TableBucketReplica replica :
                                        context.getAllReplicasForTable(tableId)) {
                                    replicasToDeletes =
                                            isReplicaToDelete(replica, context)
                                                    ? replicasToDeletes + 1
                                                    : replicasToDeletes;
                                }
                            }

                            return new MetricsData(
                                    tabletServerCount,
                                    tableCount,
                                    bucketCount,
                                    offlineBucketCount,
                                    replicasToDeletes);
                        });

        eventProcessor.process(accessContextEvent);

        // Wait for the result and update local metrics
        try {
            MetricsData metricsData = accessContextEvent.getResultFuture().get();
            this.tabletServerCount = metricsData.tabletServerCount;
            this.tableCount = metricsData.tableCount;
            this.bucketCount = metricsData.bucketCount;
            this.offlineBucketCount = metricsData.offlineBucketCount;
            this.replicasToDeleteCount = metricsData.replicasToDeleteCount;
        } catch (Exception e) {
            LOG.warn("Failed to update metrics via AccessContextEvent", e);
        }
    }

    private boolean isReplicaToDelete(TableBucketReplica replica, CoordinatorContext context) {
        ReplicaState replicaState = context.getReplicaState(replica);
        return replicaState != null && replicaState != ReplicaDeletionSuccessful;
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

                        LOG.debug(
                                "Put coordinator event {} of event type {}.",
                                event,
                                event.getClass());
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

        private long lastMetricsUpdateTime = System.currentTimeMillis();

        public CoordinatorEventThread(String name) {
            super(name, false);
        }

        @Override
        public void doWork() throws Exception {
            // Check if it's time to update metrics (before taking event from queue)
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastMetricsUpdateTime >= METRICS_UPDATE_INTERVAL_MS) {
                updateMetricsViaAccessContext();
                lastMetricsUpdateTime = currentTime;
            }

            QueuedEvent queuedEvent = queue.take();
            CoordinatorEvent coordinatorEvent = queuedEvent.event;

            long eventStartTimeMs = System.currentTimeMillis();

            LOG.debug(
                    "Start processing event {} of event type {}.",
                    coordinatorEvent,
                    coordinatorEvent.getClass());
            try {
                if (!(coordinatorEvent instanceof ShutdownEventThreadEvent)) {
                    eventQueueTime.update(System.currentTimeMillis() - queuedEvent.enqueueTimeMs);
                    eventProcessor.process(coordinatorEvent);
                }
            } catch (Throwable e) {
                LOG.error("Uncaught error processing event {}.", coordinatorEvent, e);
            } finally {
                long costTimeMs = System.currentTimeMillis() - eventStartTimeMs;
                eventProcessingTime.update(costTimeMs);
                LOG.debug(
                        "Finished processing event {} of event type {} in {}ms.",
                        coordinatorEvent,
                        coordinatorEvent.getClass(),
                        costTimeMs);
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

    private static class MetricsData {
        private final int tabletServerCount;
        private final int tableCount;
        private final int bucketCount;
        private final int offlineBucketCount;
        private final int replicasToDeleteCount;

        public MetricsData(
                int tabletServerCount,
                int tableCount,
                int bucketCount,
                int offlineBucketCount,
                int replicasToDeleteCount) {
            this.tabletServerCount = tabletServerCount;
            this.tableCount = tableCount;
            this.bucketCount = bucketCount;
            this.offlineBucketCount = offlineBucketCount;
            this.replicasToDeleteCount = replicasToDeleteCount;
        }
    }
}
