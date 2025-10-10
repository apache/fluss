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

package org.apache.fluss.flink.sink.shuffle;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Iterables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ThrowableCatchingRunnable;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Coordinator for collecting global data statistics.
 *
 * <p>NOTE: This class is inspired from Iceberg project.
 */
@Internal
class DataStatisticsCoordinator implements OperatorCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(DataStatisticsCoordinator.class);

    private final String operatorName;
    private final OperatorCoordinator.Context context;
    private final ExecutorService coordinatorExecutor;
    private final SubtaskGateways subtaskGateways;
    private final CoordinatorExecutorThreadFactory coordinatorThreadFactory;
    private final TypeSerializer<DataStatistics> statisticsSerializer;

    private transient boolean started;
    private transient AggregatedStatisticsTracker aggregatedStatisticsTracker;

    DataStatisticsCoordinator(String operatorName, OperatorCoordinator.Context context) {
        this.operatorName = operatorName;
        this.context = context;
        this.coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(
                        "DataStatisticsCoordinator-" + operatorName,
                        context.getUserCodeClassloader());
        this.coordinatorExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
        this.subtaskGateways = new SubtaskGateways(operatorName, context.currentParallelism());
        this.statisticsSerializer = new DataStatisticsSerializer();
    }

    @Override
    public void start() throws Exception {
        LOG.debug("Starting data statistics coordinator: {}.", operatorName);
        this.started = true;

        // statistics are restored already in resetToCheckpoint() before start() called
        this.aggregatedStatisticsTracker =
                new AggregatedStatisticsTracker(operatorName, context.currentParallelism());
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        runInCoordinatorThread(
                () -> {
                    LOG.debug(
                            "Handling event from subtask {} (#{}) of {}: {}",
                            subtask,
                            attemptNumber,
                            operatorName,
                            event);
                    if (event instanceof StatisticsEvent) {
                        handleDataStatisticRequest(subtask, ((StatisticsEvent) event));
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid operator event type: "
                                        + event.getClass().getCanonicalName());
                    }
                },
                String.format(
                        Locale.ROOT,
                        "handling operator event %s from subtask %d (#%d)",
                        event.getClass(),
                        subtask,
                        attemptNumber));
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
        resultFuture.complete(new byte[0]);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        checkState(
                !started,
                "The coordinator %s can only be reset if it was not yet started",
                operatorName);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        runInCoordinatorThread(
                () -> {
                    LOG.info(
                            "Operator {} subtask {} is reset to checkpoint {}",
                            operatorName,
                            subtask,
                            checkpointId);
                    checkState(this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
                    subtaskGateways.reset(subtask);
                },
                String.format(
                        Locale.ROOT,
                        "handling subtask %d recovery to checkpoint %d",
                        subtask,
                        checkpointId));
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        runInCoordinatorThread(
                () -> {
                    LOG.info(
                            "Unregistering gateway after failure for subtask {} (#{}) of data statistics {}",
                            subtask,
                            attemptNumber,
                            operatorName);
                    checkState(this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
                    subtaskGateways.unregisterSubtaskGateway(subtask, attemptNumber);
                },
                String.format(
                        Locale.ROOT, "handling subtask %d (#%d) failure", subtask, attemptNumber));
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        checkArgument(subtask == gateway.getSubtask());
        checkArgument(attemptNumber == gateway.getExecution().getAttemptNumber());
        runInCoordinatorThread(
                () -> {
                    checkState(this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
                    subtaskGateways.registerSubtaskGateway(gateway);
                },
                String.format(
                        Locale.ROOT,
                        "making event gateway to subtask %d (#%d) available",
                        subtask,
                        attemptNumber));
    }

    @Override
    public void close() throws Exception {
        coordinatorExecutor.shutdown();
        this.aggregatedStatisticsTracker = null;
        this.started = false;
        LOG.info("Closed data statistics coordinator: {}.", operatorName);
    }

    private void runInCoordinatorThread(Runnable runnable) {
        this.coordinatorExecutor.execute(
                new ThrowableCatchingRunnable(
                        throwable ->
                                this.coordinatorThreadFactory.uncaughtException(
                                        Thread.currentThread(), throwable),
                        runnable));
    }

    private void runInCoordinatorThread(ThrowingRunnable<Throwable> action, String actionString) {
        ensureStarted();
        runInCoordinatorThread(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                        LOG.error(
                                "Uncaught exception in the data statistics coordinator: {} while {}. Triggering job failover",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    private void ensureStarted() {
        checkState(started, "The coordinator of %s has not started yet.", operatorName);
    }

    private void handleDataStatisticRequest(int subtask, StatisticsEvent event) {
        DataStatistics maybeCompletedStatistics =
                aggregatedStatisticsTracker.updateAndCheckCompletion(subtask, event);
        if (maybeCompletedStatistics != null) {
            if (maybeCompletedStatistics.isEmpty()) {
                LOG.debug(
                        "Skip aggregated statistics for checkpoint {} as it is empty.",
                        event.checkpointId());
            } else {
                LOG.debug(
                        "Completed statistics aggregation for checkpoint {}", event.checkpointId());
                sendGlobalStatisticsToSubtasks(maybeCompletedStatistics, event.checkpointId());
            }
        }
    }

    private void sendGlobalStatisticsToSubtasks(DataStatistics statistics, long checkpointId) {
        runInCoordinatorThread(
                () -> {
                    LOG.info(
                            "Broadcast latest global statistics from checkpoint {} to all subtasks",
                            checkpointId);
                    // applyImmediately is set to false so that operator subtasks can
                    // apply the change at checkpoint boundary
                    StatisticsEvent statisticsEvent =
                            StatisticsEvent.createStatisticsEvent(
                                    checkpointId, statistics, statisticsSerializer);
                    for (int i = 0; i < context.currentParallelism(); ++i) {
                        // Ignore future return value for potential error (e.g. subtask down).
                        // Upon restart, subtasks send request to coordinator to refresh statistics
                        // if there is any difference
                        subtaskGateways.getSubtaskGateway(i).sendEvent(statisticsEvent);
                    }
                },
                String.format(
                        Locale.ROOT,
                        "Failed to send operator %s coordinator global data statistics for checkpoint %d",
                        operatorName,
                        checkpointId));
    }

    @VisibleForTesting
    void callInCoordinatorThread(Callable<Void> callable, String errorMessage) {
        ensureStarted();
        // Ensure the task is done by the coordinator executor.
        if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()) {
            try {
                Callable<Void> guardedCallable =
                        () -> {
                            try {
                                return callable.call();
                            } catch (Throwable t) {
                                LOG.error(
                                        "Uncaught Exception in data statistics coordinator: {} executor",
                                        operatorName,
                                        t);
                                ExceptionUtils.rethrowException(t);
                                return null;
                            }
                        };

                coordinatorExecutor.submit(guardedCallable).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new FlinkRuntimeException(errorMessage, e);
            }
        } else {
            try {
                callable.call();
            } catch (Throwable t) {
                LOG.error(
                        "Uncaught Exception in data statistics coordinator: {} executor",
                        operatorName,
                        t);
                throw new FlinkRuntimeException(errorMessage, t);
            }
        }
    }

    @VisibleForTesting
    public SubtaskGateways getSubtaskGateways() {
        return subtaskGateways;
    }

    static class SubtaskGateways {
        private final String operatorName;
        private final Map<Integer, SubtaskGateway>[] gateways;

        @SuppressWarnings("unchecked")
        private SubtaskGateways(String operatorName, int parallelism) {
            this.operatorName = operatorName;
            gateways = new Map[parallelism];

            for (int i = 0; i < parallelism; ++i) {
                gateways[i] = new HashMap<>();
            }
        }

        private void registerSubtaskGateway(OperatorCoordinator.SubtaskGateway gateway) {
            int subtaskIndex = gateway.getSubtask();
            int attemptNumber = gateway.getExecution().getAttemptNumber();
            checkState(
                    !gateways[subtaskIndex].containsKey(attemptNumber),
                    "Coordinator of %s already has a subtask gateway for %d (#%d)",
                    operatorName,
                    subtaskIndex,
                    attemptNumber);
            LOG.debug(
                    "Coordinator of {} registers gateway for subtask {} attempt {}",
                    operatorName,
                    subtaskIndex,
                    attemptNumber);
            gateways[subtaskIndex].put(attemptNumber, gateway);
        }

        private void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
            LOG.debug(
                    "Coordinator of {} unregisters gateway for subtask {} attempt {}",
                    operatorName,
                    subtaskIndex,
                    attemptNumber);
            gateways[subtaskIndex].remove(attemptNumber);
        }

        protected OperatorCoordinator.SubtaskGateway getSubtaskGateway(int subtaskIndex) {
            checkState(
                    !gateways[subtaskIndex].isEmpty(),
                    "Coordinator of %s subtask %d is not ready yet to receive events",
                    operatorName,
                    subtaskIndex);
            return Iterables.getOnlyElement(gateways[subtaskIndex].values());
        }

        private void reset(int subtaskIndex) {
            gateways[subtaskIndex].clear();
        }
    }

    private static class CoordinatorExecutorThreadFactory
            implements ThreadFactory, Thread.UncaughtExceptionHandler {

        private final String coordinatorThreadName;
        private final ClassLoader classLoader;
        private final Thread.UncaughtExceptionHandler errorHandler;

        @javax.annotation.Nullable private Thread thread;

        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName, final ClassLoader contextClassLoader) {
            this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
        }

        @org.apache.flink.annotation.VisibleForTesting
        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName,
                final ClassLoader contextClassLoader,
                final Thread.UncaughtExceptionHandler errorHandler) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.classLoader = contextClassLoader;
            this.errorHandler = errorHandler;
        }

        @Override
        public synchronized Thread newThread(@Nonnull Runnable runnable) {
            thread = new Thread(runnable, coordinatorThreadName);
            thread.setContextClassLoader(classLoader);
            thread.setUncaughtExceptionHandler(this);
            return thread;
        }

        @Override
        public synchronized void uncaughtException(Thread t, Throwable e) {
            errorHandler.uncaughtException(t, e);
        }

        boolean isCurrentThreadCoordinatorThread() {
            return Thread.currentThread() == thread;
        }
    }
}
