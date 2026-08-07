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
import org.apache.fluss.exception.NoRebalanceInProgressException;
import org.apache.fluss.exception.RebalanceFailureException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.FinalizeRebalanceEvent;
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
 * <p>This manager can only be used in the coordinator event loop as a single threaded model.
 */
public class RebalanceManager {
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    /** Hardcoded timeout for an in-flight rebalance task: 2 minutes. */
    private static final long REBALANCE_TASK_TIMEOUT_MS = 2 * 60 * 1000L;

    /** Hardcoded interval for the periodic timeout check: 30 seconds. */
    private static final long TIMEOUT_CHECK_INTERVAL_MS = 30 * 1000L;

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

    /** Normally running tasks. This map contains at most one entry until concurrency is added. */
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
            Clock clock) {
        this(
                rebalanceExecutor,
                zkClient,
                eventManager,
                clock,
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
            ScheduledExecutorService timeoutChecker) {
        this.rebalanceExecutor = rebalanceExecutor;
        this.zkClient = zkClient;
        this.eventManager = eventManager;
        this.clock = clock == null ? SystemClock.getInstance() : clock;
        this.timeoutChecker = timeoutChecker;
        this.goalOptimizer = new GoalOptimizer();
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
            processNewRebalanceTask();
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
            processNewRebalanceTask();
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
            processNewRebalanceTask();
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

        if (inProgressRebalanceTasks.isEmpty()) {
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
        enqueueReconciliation(attempt);
        processNewRebalanceTask();
        return true;
    }

    public synchronized @Nullable RebalancePlanForBucket getPlanForReconciliation(
            RebalanceExecutionKey executionKey) {
        queuedReconcileEvents.remove(executionKey);
        RebalanceTaskAttempt attempt = timedOutRebalanceTasks.get(executionKey.getTableBucket());
        if (attempt == null || !attempt.executionKey.equals(executionKey)) {
            return null;
        }
        RebalanceResultForBucket result =
                inProgressRebalanceTasks.get(executionKey.getTableBucket());
        return result == null ? null : result.plan();
    }

    public synchronized void retryFinalizeRebalance(String rebalanceId) {
        finalizationEventQueued = false;
        if (finalizationPending && rebalanceId.equals(currentRebalanceId)) {
            persistFinalStatus();
        }
    }

    private void processNewRebalanceTask() {
        if (!runningRebalanceTasks.isEmpty()) {
            return;
        }
        TableBucket tableBucket;
        while ((tableBucket = pendingRebalanceTasks.poll()) != null) {
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
            return;
        }
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
        for (RebalanceTaskAttempt attempt : new HashMap<>(runningRebalanceTasks).values()) {
            long elapsed = clock.milliseconds() - attempt.startMs;
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

        for (RebalanceTaskAttempt attempt : new HashMap<>(timedOutRebalanceTasks).values()) {
            enqueueReconciliation(attempt);
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

        private RebalanceTaskAttempt(RebalanceExecutionKey executionKey, long startMs) {
            this.executionKey = executionKey;
            this.startMs = startMs;
        }
    }
}
