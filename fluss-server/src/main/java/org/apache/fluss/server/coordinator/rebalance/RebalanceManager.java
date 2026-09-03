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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.exception.NoRebalanceInProgressException;
import org.apache.fluss.exception.RebalanceFailureException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.FinalizeRebalanceEvent;
import org.apache.fluss.server.coordinator.event.RebalanceMaxInflightTasksChangedEvent;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
import org.apache.fluss.server.coordinator.event.ReconcileRebalanceTaskEvent;
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
import org.apache.fluss.server.coordinator.rebalance.goal.Goal;
import org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizer;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.RackModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.CANCELED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FINAL_STATUSES;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A rebalance manager to generate rebalance plan, and execution rebalance plan.
 *
 * <p>This manager is used in the coordinator event loop as a single-threaded model. Non-event
 * threads enqueue coordinator events instead of directly advancing rebalance state.
 */
public class RebalanceManager implements ServerReconfigurable {
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    /** Hardcoded timeout for an in-flight rebalance task: 2 minutes. */
    private static final long REBALANCE_TASK_TIMEOUT_MS = 2 * 60 * 1000L;

    /** Hardcoded interval for the periodic timeout check: 30 seconds. */
    private static final long TIMEOUT_CHECK_INTERVAL_MS = 30 * 1000L;

    /** Hardcoded upper bound for the exponential reconciliation backoff: 5 minutes. */
    private static final long MAX_RECONCILE_BACKOFF_MS = 5 * 60 * 1000L;

    /**
     * Hardcoded time after which a timed-out task is given up on while it cannot make progress at
     * all, because a target replica is not hosted by a live tablet server: 30 minutes.
     */
    private static final long TARGET_UNAVAILABLE_TIMEOUT_MS = 30 * 60 * 1000L;

    /**
     * Hardcoded time after which a timed-out task is given up on even though its target replicas
     * are live: 24 hours. This is only a safety net that keeps a rebalance from staying non-final
     * forever, a single bucket that does not change any observable state for that long is broken.
     */
    private static final long NO_PROGRESS_TIMEOUT_MS = 24 * 60 * 60 * 1000L;

    /**
     * Hardcoded upper bound on the number of timed-out tasks tracked at the same time. Every
     * tracked task keeps costing coordinator and ZooKeeper work on each reconciliation, and each
     * admitted task adds one more concurrent replica migration.
     */
    private static final int MAX_TRACKED_TIMED_OUT_TASKS = 8;

    private final ZooKeeperClient zkClient;
    private final RebalanceExecutor rebalanceExecutor;
    private final EventManager eventManager;
    private final Clock clock;
    private final ScheduledExecutorService timeoutChecker;

    /** A queue of bucket tasks that have not started. */
    private final Queue<TableBucket> pendingRebalanceTasks = new ArrayDeque<>();

    /** A mapping from table bucket to rebalance status of pending and running tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> inProgressRebalanceTasks =
            new ConcurrentHashMap<>();

    /** Normally running tasks that occupy configured execution slots. */
    private final Map<TableBucket, RebalanceTaskAttempt> runningRebalanceTasks =
            new ConcurrentHashMap<>();

    /** Soft-timed-out tasks that no longer occupy the normal execution slot. */
    private final Map<TableBucket, RebalanceTaskAttempt> timedOutRebalanceTasks =
            new ConcurrentHashMap<>();

    private final Set<RebalanceExecutionKey> queuedTimeoutEvents = ConcurrentHashMap.newKeySet();
    private final Set<RebalanceExecutionKey> queuedReconcileEvents = ConcurrentHashMap.newKeySet();

    /** A mapping from table bucket to rebalance status of failed or completed tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> finishedRebalanceTasks =
            new ConcurrentHashMap<>();

    private final GoalOptimizer goalOptimizer;
    private int maxInflightRebalanceTasks;
    private int queuedMaxInflightRebalanceTasks;
    private volatile long registerTime;
    private volatile @Nullable RebalanceStatus rebalanceStatus;
    private volatile @Nullable String currentRebalanceId;
    private volatile boolean recoveryPending;
    private volatile boolean cancelRequested;
    private volatile boolean finalizationPending;
    private volatile boolean finalizationEventQueued;
    private volatile boolean isClosed = false;
    private long nextAttemptId;

    public RebalanceManager(
            RebalanceExecutor rebalanceExecutor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock,
            Configuration conf) {
        this(
                rebalanceExecutor,
                zkClient,
                eventManager,
                clock,
                conf,
                // TODO: Reuse the CoordinatorServer shared scheduler for this lightweight
                // coordinator timeout checker instead of creating a component-owned scheduler.
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("rebalance-timeout")));
    }

    @VisibleForTesting
    RebalanceManager(
            RebalanceExecutor rebalanceExecutor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock,
            Configuration conf,
            ScheduledExecutorService timeoutChecker) {
        this.rebalanceExecutor = rebalanceExecutor;
        this.zkClient = zkClient;
        this.eventManager = eventManager;
        this.clock = clock == null ? SystemClock.getInstance() : clock;
        this.timeoutChecker = timeoutChecker;
        this.goalOptimizer = new GoalOptimizer();
        validate(conf);
        this.maxInflightRebalanceTasks =
                conf.get(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS);
        this.queuedMaxInflightRebalanceTasks = maxInflightRebalanceTasks;
    }

    public void startup() {
        LOG.info("Start up rebalance manager.");
        initialize();
    }

    /** Starts the periodic timeout checker. Call after {@link #startup()}. */
    public void start() {
        timeoutChecker.scheduleWithFixedDelay(
                this::checkTimeoutSafely,
                TIMEOUT_CHECK_INTERVAL_MS,
                TIMEOUT_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        LOG.info(
                "RebalanceManager timeout checker started: timeoutMs={}, checkIntervalMs={}",
                REBALANCE_TASK_TIMEOUT_MS,
                TIMEOUT_CHECK_INTERVAL_MS);
    }

    public @Nullable String getRebalanceId() {
        return currentRebalanceId;
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        int newMaxInflightRebalanceTasks =
                newConfig.get(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS);
        if (newMaxInflightRebalanceTasks < 0) {
            throw new ConfigException(
                    String.format(
                            "Invalid %s: must be non-negative, but was %s",
                            ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS.key(),
                            newMaxInflightRebalanceTasks));
        }
    }

    @Override
    public synchronized void reconfigure(Configuration newConfig) {
        int newMaxInflightRebalanceTasks =
                newConfig.get(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS);
        if (newMaxInflightRebalanceTasks == queuedMaxInflightRebalanceTasks) {
            LOG.debug(
                    "{} unchanged: {}",
                    ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS.key(),
                    newMaxInflightRebalanceTasks);
            return;
        }

        int oldQueuedMaxInflightRebalanceTasks = queuedMaxInflightRebalanceTasks;
        queuedMaxInflightRebalanceTasks = newMaxInflightRebalanceTasks;
        LOG.info(
                "{} change queued: {} -> {}",
                ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS.key(),
                oldQueuedMaxInflightRebalanceTasks,
                newMaxInflightRebalanceTasks);
        if (!isClosed) {
            eventManager.put(
                    new RebalanceMaxInflightTasksChangedEvent(newMaxInflightRebalanceTasks));
        }
    }

    private void initialize() {
        try {
            zkClient.getRebalanceTask()
                    .ifPresent(
                            rebalanceTask -> {
                                recoveryPending = true;
                                eventManager.put(new RecoverRebalanceEvent(rebalanceTask));
                            });
        } catch (Exception e) {
            LOG.error(
                    "Failed to get rebalance plan from zookeeper. New rebalance requests will be "
                            + "rejected until the coordinator is restarted and recovery succeeds.",
                    e);
            recoveryPending = true;
        }
    }

    public synchronized void registerRebalance(
            String rebalanceId,
            Map<TableBucket, RebalancePlanForBucket> rebalancePlan,
            RebalanceStatus newStatus) {
        checkNotClosed();
        resetRebalance(rebalanceId, false);
        if (rebalancePlan.isEmpty()) {
            finalizeRebalance();
            return;
        }

        for (Map.Entry<TableBucket, RebalancePlanForBucket> entry : rebalancePlan.entrySet()) {
            if (FINAL_STATUSES.contains(newStatus)) {
                finishedRebalanceTasks.put(
                        entry.getKey(), RebalanceResultForBucket.of(entry.getValue(), newStatus));
            } else {
                addPendingTask(entry.getKey(), entry.getValue());
            }
        }

        if (!pendingRebalanceTasks.isEmpty()) {
            rebalanceStatus = REBALANCING;
            processNewRebalanceTasks();
        } else {
            rebalanceStatus = newStatus;
        }
    }

    /** Recovers a persisted task by comparing every bucket plan with current coordinator state. */
    public synchronized void recoverRebalance(RebalanceTask rebalanceTask) {
        checkNotClosed();
        if (FINAL_STATUSES.contains(rebalanceTask.getRebalanceStatus())) {
            resetRebalance(rebalanceTask.getRebalanceId(), rebalanceTask.isCancelRequested());
            for (Map.Entry<TableBucket, RebalancePlanForBucket> entry :
                    rebalanceTask.getExecutePlan().entrySet()) {
                finishedRebalanceTasks.put(
                        entry.getKey(),
                        RebalanceResultForBucket.of(
                                entry.getValue(), rebalanceTask.getRebalanceStatus()));
            }
            rebalanceStatus = rebalanceTask.getRebalanceStatus();
            return;
        }

        boolean recoveringCancellation =
                rebalanceTask.isCancelRequested() || rebalanceTask.getRebalanceStatus() == CANCELED;
        resetRebalance(rebalanceTask.getRebalanceId(), recoveringCancellation);

        for (Map.Entry<TableBucket, RebalancePlanForBucket> entry :
                rebalanceTask.getExecutePlan().entrySet()) {
            TableBucket tableBucket = entry.getKey();
            RebalancePlanForBucket plan = entry.getValue();
            if (rebalanceExecutor.isRebalanceTaskComplete(plan)) {
                finishedRebalanceTasks.put(
                        tableBucket, RebalanceResultForBucket.of(plan, COMPLETED));
            } else if (recoveringCancellation && rebalanceExecutor.isRebalanceTaskAtOrigin(plan)) {
                finishedRebalanceTasks.put(
                        tableBucket, RebalanceResultForBucket.of(plan, CANCELED));
            } else {
                addPendingTask(tableBucket, plan);
            }
        }

        if (inProgressRebalanceTasks.isEmpty()) {
            rebalanceStatus = recoveringCancellation ? CANCELED : aggregateFinalStatus();
            persistFinalStatus();
        } else {
            rebalanceStatus = REBALANCING;
            processNewRebalanceTasks();
        }
    }

    public synchronized void finishRebalanceTask(
            TableBucket tableBucket, RebalanceStatus statusForBucket) {
        RebalanceExecutionKey executionKey = getExecutionKey(tableBucket);
        if (executionKey != null) {
            finishRebalanceTask(executionKey, statusForBucket);
        }
    }

    public synchronized boolean finishRebalanceTask(
            RebalanceExecutionKey executionKey, RebalanceStatus statusForBucket) {
        checkNotClosed();
        checkArgument(statusForBucket != TIMEOUT, "Use timeoutRebalanceTask for soft timeouts.");
        RebalanceTaskAttempt attempt = findActiveAttempt(executionKey);
        if (attempt == null) {
            LOG.debug("Ignore stale completion for {}.", executionKey);
            return false;
        }

        TableBucket tableBucket = executionKey.getTableBucket();
        runningRebalanceTasks.remove(tableBucket);
        timedOutRebalanceTasks.remove(tableBucket);
        queuedTimeoutEvents.remove(executionKey);
        queuedReconcileEvents.remove(executionKey);
        RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.remove(tableBucket);
        if (resultForBucket == null) {
            return false;
        }
        finishedRebalanceTasks.put(
                tableBucket, RebalanceResultForBucket.of(resultForBucket.plan(), statusForBucket));
        LOG.info(
                "Rebalance {} progress: {} pending, {} running, {} timed out and tracking, "
                        + "{} finished.",
                currentRebalanceId,
                pendingRebalanceTasks.size(),
                runningRebalanceTasks.size(),
                timedOutRebalanceTasks.size(),
                finishedRebalanceTasks.size());

        if (inProgressRebalanceTasks.isEmpty()) {
            finalizeRebalance();
        } else {
            processNewRebalanceTasks();
        }
        return true;
    }

    public synchronized @Nullable RebalanceProgress listRebalanceProgress(
            @Nullable String rebalanceId) {
        checkNotClosed();
        if (rebalanceId != null
                && currentRebalanceId != null
                && !rebalanceId.equals(currentRebalanceId)) {
            LOG.warn(
                    "Ignore the list rebalance task because it is not the current"
                            + " rebalance task.");
            throw new NoRebalanceInProgressException(
                    String.format(
                            "Rebalance task id %s to list is not the current rebalance task id %s.",
                            rebalanceId, currentRebalanceId));
        }

        if (currentRebalanceId == null) {
            return null;
        }

        Map<TableBucket, RebalanceResultForBucket> progressForBucketMap = new HashMap<>();
        progressForBucketMap.putAll(inProgressRebalanceTasks);
        progressForBucketMap.putAll(finishedRebalanceTasks);
        // the progress will be set at client.
        return new RebalanceProgress(
                currentRebalanceId, rebalanceStatus, 0.0, progressForBucketMap);
    }

    public synchronized void cancelRebalance(@Nullable String rebalanceId) {
        checkNotClosed();

        if (currentRebalanceId == null) {
            return;
        }

        if (rebalanceId != null
                && currentRebalanceId != null
                && !rebalanceId.equals(currentRebalanceId)) {
            // do nothing.
            LOG.warn(
                    "Ignore the cancel rebalance task because it is not the current"
                            + " rebalance task.");
            throw new NoRebalanceInProgressException(
                    String.format(
                            "Rebalance task id %s to cancel is not the current rebalance task id %s.",
                            rebalanceId, currentRebalanceId));
        }

        if (rebalanceStatus != null && FINAL_STATUSES.contains(rebalanceStatus)) {
            // do nothing for the final state rebalance task.
            return;
        }

        Map<TableBucket, RebalancePlanForBucket> executePlan = allRebalancePlans();
        try {
            zkClient.registerRebalanceTask(
                    new RebalanceTask(currentRebalanceId, REBALANCING, executePlan, true));
        } catch (Exception e) {
            throw new RebalanceFailureException(
                    "Failed to persist rebalance cancellation request.", e);
        }

        cancelRequested = true;
        TableBucket pending;
        while ((pending = pendingRebalanceTasks.poll()) != null) {
            RebalanceResultForBucket result = inProgressRebalanceTasks.remove(pending);
            if (result != null) {
                finishedRebalanceTasks.put(
                        pending, RebalanceResultForBucket.of(result.plan(), CANCELED));
            }
        }

        // Admitted tasks that have not changed anything yet can be given up on right away: there
        // is no half-applied assignment to drain, so cancellation does not have to wait for them.
        for (RebalanceTaskAttempt attempt : activeAttempts()) {
            RebalanceResultForBucket result =
                    inProgressRebalanceTasks.get(attempt.executionKey.getTableBucket());
            if (result != null && rebalanceExecutor.isRebalanceTaskAtOrigin(result.plan())) {
                finishRebalanceTask(attempt.executionKey, CANCELED);
            }
        }

        if (inProgressRebalanceTasks.isEmpty() && !FINAL_STATUSES.contains(rebalanceStatus)) {
            finalizeRebalance();
        }
        LOG.info(
                "Accepted cancellation for rebalance {}. Running and timed-out tasks will be "
                        + "drained before the rebalance becomes canceled.",
                currentRebalanceId);
    }

    public synchronized boolean hasInProgressRebalance() {
        checkNotClosed();
        return recoveryPending || finalizationPending || !inProgressRebalanceTasks.isEmpty();
    }

    public RebalanceTask generateRebalanceTask(List<Goal> goalsByPriority) {
        checkNotClosed();
        List<RebalancePlanForBucket> rebalancePlanForBuckets;
        String rebalanceId = UUID.randomUUID().toString();
        try {
            // Generate the latest cluster model.
            long startTime = System.currentTimeMillis();
            ClusterModel clusterModel =
                    buildClusterModel(rebalanceExecutor.getCoordinatorContext());
            LOG.info(
                    "Build cluster model for rebalance id {} with {} ms.",
                    rebalanceId,
                    System.currentTimeMillis() - startTime);

            // do optimize.
            startTime = System.currentTimeMillis();
            rebalancePlanForBuckets = goalOptimizer.doOptimizeOnce(clusterModel, goalsByPriority);
            LOG.info(
                    "Do optimize for rebalance id {} with {} ms.",
                    rebalanceId,
                    System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            LOG.error("Failed to generate rebalance plan.", e);
            throw e;
        }

        // group by tableId and partitionId to generate rebalance plan.
        return buildRebalanceTask(rebalanceId, rebalancePlanForBuckets);
    }

    public synchronized @Nullable RebalancePlanForBucket getRebalancePlanForBucket(
            TableBucket tableBucket) {
        checkNotClosed();
        if (!runningRebalanceTasks.containsKey(tableBucket)
                && !timedOutRebalanceTasks.containsKey(tableBucket)) {
            return null;
        }
        RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
        if (resultForBucket != null) {
            return resultForBucket.plan();
        }
        return null;
    }

    public synchronized @Nullable RebalanceExecutionKey getExecutionKey(TableBucket tableBucket) {
        RebalanceTaskAttempt attempt = runningRebalanceTasks.get(tableBucket);
        if (attempt == null) {
            attempt = timedOutRebalanceTasks.get(tableBucket);
        }
        return attempt == null ? null : attempt.executionKey;
    }

    public synchronized boolean timeoutRebalanceTask(RebalanceExecutionKey executionKey) {
        checkNotClosed();
        queuedTimeoutEvents.remove(executionKey);
        RebalanceTaskAttempt attempt = runningRebalanceTasks.get(executionKey.getTableBucket());
        if (attempt == null || !attempt.executionKey.equals(executionKey)) {
            LOG.debug("Ignore stale timeout for {}.", executionKey);
            return false;
        }
        if (timedOutRebalanceTasks.size() >= MAX_TRACKED_TIMED_OUT_TASKS) {
            // Keep the attempt in the normal running set until a reconciliation slot becomes
            // available. The timeout checker will enqueue another timeout event on its next pass.
            LOG.info(
                    "Keep timed-out rebalance task {} in the running set because {} other "
                            + "timed-out tasks are still being reconciled.",
                    executionKey,
                    timedOutRebalanceTasks.size());
            return false;
        }

        TableBucket tableBucket = executionKey.getTableBucket();
        runningRebalanceTasks.remove(tableBucket);
        timedOutRebalanceTasks.put(tableBucket, attempt);
        RebalanceResultForBucket result = inProgressRebalanceTasks.get(tableBucket);
        if (result == null) {
            timedOutRebalanceTasks.remove(tableBucket);
            return false;
        }
        inProgressRebalanceTasks.put(
                tableBucket, RebalanceResultForBucket.of(result.plan(), TIMEOUT));
        attempt.onTimedOut(clock.milliseconds(), observeBucketState(tableBucket));
        enqueueReconciliation(attempt);
        processNewRebalanceTasks();
        return true;
    }

    /**
     * Returns the plan to reconcile for the given attempt, or null if the attempt is stale or has
     * just been given up on.
     *
     * <p>Reconciliation has to terminate. Otherwise a bucket that can never converge, for example
     * because a target server is gone for good, keeps the overall rebalance in a non-final status
     * and every later rebalance request is rejected forever.
     */
    public synchronized @Nullable RebalancePlanForBucket getPlanForReconciliation(
            RebalanceExecutionKey executionKey) {
        queuedReconcileEvents.remove(executionKey);
        TableBucket tableBucket = executionKey.getTableBucket();
        RebalanceTaskAttempt attempt = timedOutRebalanceTasks.get(tableBucket);
        if (attempt == null || !attempt.executionKey.equals(executionKey)) {
            return null;
        }
        RebalanceResultForBucket result = inProgressRebalanceTasks.get(tableBucket);
        if (result == null) {
            return null;
        }

        // Called on the coordinator event loop, so reading the coordinator state is safe here.
        long now = clock.milliseconds();
        String observedState = observeBucketState(tableBucket);
        boolean targetsLive =
                rebalanceExecutor
                        .getCoordinatorContext()
                        .liveTabletServerSet()
                        .containsAll(result.plan().getNewReplicas());
        if (!observedState.equals(attempt.observedState)) {
            attempt.onProgress(now, observedState);
        } else if (targetsLive) {
            attempt.onTargetsAvailable();
        } else {
            attempt.onTargetsUnavailable(now);
        }

        if (attempt.blockedForMs(now) > TARGET_UNAVAILABLE_TIMEOUT_MS
                || now - attempt.lastProgressMs > NO_PROGRESS_TIMEOUT_MS) {
            LOG.error(
                    "Giving up on rebalance task {} after {} ms without progress, target replicas "
                            + "live: {}. The bucket may be left with the intermediate assignment "
                            + "and can be moved again by a new rebalance.",
                    executionKey,
                    now - attempt.lastProgressMs,
                    targetsLive);
            finishRebalanceTask(executionKey, FAILED);
            return null;
        }

        attempt.onReconcileDispatched(now);
        return result.plan();
    }

    public synchronized void retryFinalizeRebalance(String rebalanceId) {
        finalizationEventQueued = false;
        if (finalizationPending && rebalanceId.equals(currentRebalanceId)) {
            persistFinalStatus();
        }
    }

    private void processNewRebalanceTasks() {
        if (timedOutRebalanceTasks.size() >= MAX_TRACKED_TIMED_OUT_TASKS) {
            // Stop admitting work until some of the timed-out tasks reach a final status, so that
            // a long cluster operation cannot grow the tracked set, and with it the reconciliation
            // work and the number of concurrent replica migrations, without bound.
            LOG.info(
                    "Hold back new tasks of rebalance {} because {} timed-out tasks are still "
                            + "being reconciled.",
                    currentRebalanceId,
                    timedOutRebalanceTasks.size());
            return;
        }

        while (runningRebalanceTasks.size() < maxInflightRebalanceTasks) {
            TableBucket tableBucket = pendingRebalanceTasks.poll();
            if (tableBucket == null) {
                return;
            }
            RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
            if (resultForBucket == null || resultForBucket.status() != NOT_STARTED) {
                continue;
            }
            RebalanceExecutionKey executionKey =
                    new RebalanceExecutionKey(currentRebalanceId, tableBucket, ++nextAttemptId);
            runningRebalanceTasks.put(
                    tableBucket, new RebalanceTaskAttempt(executionKey, clock.milliseconds()));
            inProgressRebalanceTasks.put(
                    tableBucket, RebalanceResultForBucket.of(resultForBucket.plan(), REBALANCING));
            rebalanceExecutor.tryToExecuteRebalanceTask(resultForBucket.plan());
        }
    }

    /**
     * Applies a new rebalance concurrency limit on the coordinator event loop.
     *
     * <p>Increasing the limit admits pending tasks immediately. Decreasing it does not cancel
     * running tasks; new tasks are admitted only after the number of running tasks falls below the
     * new limit.
     */
    public synchronized void updateMaxInflightRebalanceTasks(int newMaxInflightRebalanceTasks) {
        checkArgument(
                newMaxInflightRebalanceTasks >= 0,
                "%s must be non-negative.",
                ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS.key());
        int oldMaxInflightRebalanceTasks = maxInflightRebalanceTasks;
        if (newMaxInflightRebalanceTasks == oldMaxInflightRebalanceTasks) {
            LOG.debug(
                    "{} unchanged: {}",
                    ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS.key(),
                    newMaxInflightRebalanceTasks);
            return;
        }

        maxInflightRebalanceTasks = newMaxInflightRebalanceTasks;
        LOG.info(
                "{} reconfigured: {} -> {}",
                ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS.key(),
                oldMaxInflightRebalanceTasks,
                newMaxInflightRebalanceTasks);
        if (!isClosed) {
            processNewRebalanceTasks();
        }
    }

    /** Returns the rebalance concurrency limit currently applied on the coordinator event loop. */
    @VisibleForTesting
    public synchronized int getMaxInflightRebalanceTasks() {
        return maxInflightRebalanceTasks;
    }

    private void finalizeRebalance() {
        finalizationPending = true;
        persistFinalStatus();
    }

    private void persistFinalStatus() {
        checkNotClosed();
        RebalanceStatus finalStatus = cancelRequested ? CANCELED : aggregateFinalStatus();
        try {
            zkClient.registerRebalanceTask(
                    new RebalanceTask(
                            currentRebalanceId, finalStatus, allRebalancePlans(), cancelRequested));
        } catch (Exception e) {
            rebalanceStatus = REBALANCING;
            finalizationPending = true;
            LOG.error(
                    "Failed to persist final state for rebalance {}. It will be retried.",
                    currentRebalanceId,
                    e);
            return;
        }

        rebalanceStatus = finalStatus;
        finalizationPending = false;
        finalizationEventQueued = false;
        inProgressRebalanceTasks.clear();
        pendingRebalanceTasks.clear();
        runningRebalanceTasks.clear();
        timedOutRebalanceTasks.clear();
        queuedTimeoutEvents.clear();
        queuedReconcileEvents.clear();

        LOG.info(
                "Rebalance {} reached final status {} in {} ms.",
                currentRebalanceId,
                finalStatus,
                System.currentTimeMillis() - registerTime);
    }

    private void resetRebalance(String rebalanceId, boolean cancelRequested) {
        registerTime = System.currentTimeMillis();
        currentRebalanceId = rebalanceId;
        recoveryPending = false;
        this.cancelRequested = cancelRequested;
        finalizationPending = false;
        finalizationEventQueued = false;
        inProgressRebalanceTasks.clear();
        pendingRebalanceTasks.clear();
        runningRebalanceTasks.clear();
        timedOutRebalanceTasks.clear();
        finishedRebalanceTasks.clear();
        queuedTimeoutEvents.clear();
        queuedReconcileEvents.clear();
    }

    private void addPendingTask(TableBucket tableBucket, RebalancePlanForBucket plan) {
        pendingRebalanceTasks.add(tableBucket);
        inProgressRebalanceTasks.put(tableBucket, RebalanceResultForBucket.of(plan, NOT_STARTED));
    }

    private @Nullable RebalanceTaskAttempt findActiveAttempt(RebalanceExecutionKey executionKey) {
        RebalanceTaskAttempt attempt = runningRebalanceTasks.get(executionKey.getTableBucket());
        if (attempt == null) {
            attempt = timedOutRebalanceTasks.get(executionKey.getTableBucket());
        }
        return attempt != null && attempt.executionKey.equals(executionKey) ? attempt : null;
    }

    private List<RebalanceTaskAttempt> activeAttempts() {
        List<RebalanceTaskAttempt> attempts = new ArrayList<>(runningRebalanceTasks.values());
        attempts.addAll(timedOutRebalanceTasks.values());
        return attempts;
    }

    /**
     * Returns the observable state of a bucket, used to detect whether a timed-out task is still
     * making progress.
     *
     * <p>The leader and bucket epochs are deliberately left out: a reconciliation re-sends the
     * current state and can bump them without the migration moving forward at all.
     */
    private String observeBucketState(TableBucket tableBucket) {
        CoordinatorContext coordinatorContext = rebalanceExecutor.getCoordinatorContext();
        StringBuilder observed =
                new StringBuilder(coordinatorContext.getAssignment(tableBucket).toString());
        coordinatorContext
                .getBucketLeaderAndIsr(tableBucket)
                .ifPresent(
                        leaderAndIsr ->
                                observed.append("|leader=")
                                        .append(leaderAndIsr.leader())
                                        .append("|isr=")
                                        .append(new TreeSet<>(leaderAndIsr.isr())));
        return observed.toString();
    }

    private static long reconcileBackoffMs(int dispatchedAttempts) {
        long backoff = TIMEOUT_CHECK_INTERVAL_MS << Math.min(dispatchedAttempts, 8);
        return Math.min(backoff, MAX_RECONCILE_BACKOFF_MS);
    }

    private void enqueueReconciliation(RebalanceTaskAttempt attempt) {
        if (queuedReconcileEvents.add(attempt.executionKey)) {
            eventManager.put(new ReconcileRebalanceTaskEvent(attempt.executionKey));
        }
    }

    private Map<TableBucket, RebalancePlanForBucket> allRebalancePlans() {
        Map<TableBucket, RebalancePlanForBucket> plans = new HashMap<>();
        for (Map.Entry<TableBucket, RebalanceResultForBucket> entry :
                inProgressRebalanceTasks.entrySet()) {
            plans.put(entry.getKey(), entry.getValue().plan());
        }
        for (Map.Entry<TableBucket, RebalanceResultForBucket> entry :
                finishedRebalanceTasks.entrySet()) {
            plans.put(entry.getKey(), entry.getValue().plan());
        }
        return plans;
    }

    private RebalanceStatus aggregateFinalStatus() {
        for (RebalanceResultForBucket result : finishedRebalanceTasks.values()) {
            if (result.status() == FAILED || result.status() == CANCELED) {
                return FAILED;
            }
        }
        return COMPLETED;
    }

    private ClusterModel buildClusterModel(CoordinatorContext coordinatorContext) {
        Map<Integer, ServerInfo> liveTabletServers = coordinatorContext.getLiveTabletServers();
        Map<Integer, ServerTag> serverTags = coordinatorContext.getServerTags();

        Map<Integer, ServerModel> serverModelMap = new HashMap<>();
        for (ServerInfo serverInfo : liveTabletServers.values()) {
            Integer id = serverInfo.id();
            String rack = serverInfo.rack() == null ? RackModel.DEFAULT_RACK : serverInfo.rack();
            if (serverTags.containsKey(id)) {
                serverModelMap.put(
                        id, new ServerModel(id, rack, isOfflineTagged(serverTags.get(id))));
            } else {
                serverModelMap.put(id, new ServerModel(id, rack, false));
            }
        }

        ClusterModel clusterModel = initialClusterModel(serverModelMap);

        // Try to update the cluster model with the latest bucket states.
        Set<TableBucket> allBuckets = coordinatorContext.getAllBuckets();
        for (TableBucket tableBucket : allBuckets) {
            List<Integer> assignment = coordinatorContext.getAssignment(tableBucket);
            Optional<LeaderAndIsr> bucketLeaderAndIsrOpt =
                    coordinatorContext.getBucketLeaderAndIsr(tableBucket);
            // Skip the bucket if leader and ISR information is not available yet
            // This can happen during table creation when leader election is not completed
            if (!bucketLeaderAndIsrOpt.isPresent()) {
                continue;
            }
            LeaderAndIsr isr = bucketLeaderAndIsrOpt.get();
            int leader = isr.leader();
            // Skip the bucket if it is in a transient state (e.g., during table creation)
            // where the leader is elected but not yet present in the assignment list.
            if (leader == -1 || !assignment.contains(leader)) {
                continue;
            }
            for (int i = 0; i < assignment.size(); i++) {
                int replica = assignment.get(i);
                clusterModel.createReplica(replica, tableBucket, i, leader == replica);
            }
        }
        return clusterModel;
    }

    private RebalanceTask buildRebalanceTask(
            String rebalanceId, List<RebalancePlanForBucket> rebalancePlanForBuckets) {
        Map<TableBucket, RebalancePlanForBucket> bucketPlan = new HashMap<>();
        for (RebalancePlanForBucket rebalancePlanForBucket : rebalancePlanForBuckets) {
            bucketPlan.put(rebalancePlanForBucket.getTableBucket(), rebalancePlanForBucket);
        }
        return new RebalanceTask(rebalanceId, NOT_STARTED, bucketPlan);
    }

    private boolean isOfflineTagged(ServerTag serverTag) {
        return serverTag == ServerTag.PERMANENT_OFFLINE || serverTag == ServerTag.TEMPORARY_OFFLINE;
    }

    private ClusterModel initialClusterModel(Map<Integer, ServerModel> serverModelMap) {
        SortedSet<ServerModel> servers = new TreeSet<>(serverModelMap.values());
        return new ClusterModel(servers);
    }

    private void checkTimeoutSafely() {
        try {
            checkTimeout();
        } catch (Throwable t) {
            LOG.error("Unexpected error in RebalanceManager timeout check.", t);
        }
    }

    @VisibleForTesting
    void checkTimeout() {
        long now = clock.milliseconds();
        for (RebalanceTaskAttempt attempt : new HashMap<>(runningRebalanceTasks).values()) {
            long elapsed = now - attempt.startMs;
            if (elapsed > REBALANCE_TASK_TIMEOUT_MS
                    && queuedTimeoutEvents.add(attempt.executionKey)) {
                LOG.warn(
                        "In-flight rebalance task {} timed out after {}ms. It will continue to be "
                                + "tracked while the next pending task is admitted.",
                        attempt.executionKey,
                        elapsed);
                eventManager.put(new RebalanceTaskTimeoutEvent(attempt.executionKey));
            }
        }

        // Reconcile timed-out tasks on a growing backoff, so that a long cluster operation such as
        // a rolling upgrade does not turn into a constant retry storm on the event loop.
        for (RebalanceTaskAttempt attempt : new HashMap<>(timedOutRebalanceTasks).values()) {
            if (now >= attempt.nextReconcileMs) {
                enqueueReconciliation(attempt);
            }
        }

        String rebalanceId = currentRebalanceId;
        if (finalizationPending && rebalanceId != null && !finalizationEventQueued) {
            finalizationEventQueued = true;
            eventManager.put(new FinalizeRebalanceEvent(rebalanceId));
        }
    }

    private void checkNotClosed() {
        checkArgument(!isClosed, "RebalanceManager is already closed.");
    }

    public void close() {
        isClosed = true;
        timeoutChecker.shutdownNow();
    }

    @VisibleForTesting
    public ClusterModel buildClusterModel() {
        return buildClusterModel(rebalanceExecutor.getCoordinatorContext());
    }

    @VisibleForTesting
    @Nullable
    RebalanceStatus getRebalanceStatus() {
        return rebalanceStatus;
    }

    @VisibleForTesting
    boolean isCancelRequested() {
        return cancelRequested;
    }

    private static final class RebalanceTaskAttempt {
        private final RebalanceExecutionKey executionKey;
        private final long startMs;

        /** The last time this task was observed to change any bucket state. */
        private long lastProgressMs;

        /** The bucket state observed at {@link #lastProgressMs}. */
        private String observedState = "";

        /** Since when the target replicas are not all live, or -1 if they are. */
        private long blockedSinceMs = -1;

        /** The number of reconciliations already dispatched, used to grow the backoff. */
        private int reconcileAttempts;

        /** Read by the timeout checker thread, written on the coordinator event loop. */
        private volatile long nextReconcileMs;

        private RebalanceTaskAttempt(RebalanceExecutionKey executionKey, long startMs) {
            this.executionKey = executionKey;
            this.startMs = startMs;
        }

        private void onTimedOut(long nowMs, String observedState) {
            this.lastProgressMs = nowMs;
            this.observedState = observedState;
            this.blockedSinceMs = -1;
            this.reconcileAttempts = 0;
            this.nextReconcileMs = nowMs;
        }

        private void onProgress(long nowMs, String observedState) {
            this.lastProgressMs = nowMs;
            this.observedState = observedState;
            this.blockedSinceMs = -1;
            // A task that moves forward is worth probing at the base interval again.
            this.reconcileAttempts = 0;
        }

        private void onTargetsAvailable() {
            this.blockedSinceMs = -1;
        }

        private void onTargetsUnavailable(long nowMs) {
            if (blockedSinceMs < 0) {
                this.blockedSinceMs = nowMs;
            }
        }

        private long blockedForMs(long nowMs) {
            return blockedSinceMs < 0 ? 0 : nowMs - blockedSinceMs;
        }

        private void onReconcileDispatched(long nowMs) {
            this.nextReconcileMs = nowMs + reconcileBackoffMs(reconcileAttempts);
            this.reconcileAttempts++;
        }
    }
}
