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
import org.apache.fluss.cluster.rebalance.RebalanceInfo;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.NoRebalanceInProgressException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorEventProcessor;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
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
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FINAL_STATUSES;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A rebalance manager to generate rebalance plan, and execution rebalance plan.
 *
 * <p>This manager can only be used in {@link CoordinatorEventProcessor} as a single threaded model.
 */
public class RebalanceManager {
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    /** Hardcoded timeout for an in-flight rebalance task: 2 minutes. */
    private static final long REBALANCE_TASK_TIMEOUT_MS = 2 * 60 * 1000L;

    /** Hardcoded interval for the periodic timeout check: 30 seconds. */
    private static final long TIMEOUT_CHECK_INTERVAL_MS = 30 * 1000L;

    /** Hardcoded bound on the number of completed rebalance tasks retained in ZK history. */
    private static final int HISTORY_RETENTION_COUNT = 10;

    private final ZooKeeperClient zkClient;
    private final CoordinatorEventProcessor eventProcessor;
    private final EventManager eventManager;
    private final Clock clock;
    private final ScheduledExecutorService timeoutChecker;

    /** A queue of in progress table bucket to rebalance. */
    private final Queue<TableBucket> inProgressRebalanceTasksQueue = new ArrayDeque<>();

    /** A mapping from table bucket to rebalance status of pending and running tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> inProgressRebalanceTasks =
            new ConcurrentHashMap<>();

    /** A mapping from table bucket to rebalance status of failed or completed tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> finishedRebalanceTasks =
            new ConcurrentHashMap<>();

    private final GoalOptimizer goalOptimizer;
    private volatile long registerTime;
    private volatile @Nullable RebalanceStatus rebalanceStatus;
    private volatile @Nullable String currentRebalanceId;

    /** The started/completed timestamps of {@link #currentRebalanceId}, or -1 if unset. */
    private volatile long currentStartedAtMs = -1;

    private volatile long currentCompletedAtMs = -1;

    private volatile boolean isClosed = false;

    /**
     * Timestamp when the current in-flight task was started, or -1 if idle.
     *
     * <p>Write ordering contract (volatile publication idiom): always write {@code
     * inflightTaskStartMs} BEFORE {@code inflightTaskBucket} when setting, and clear {@code
     * inflightTaskBucket} BEFORE {@code inflightTaskStartMs} when resetting. The timeout checker
     * reads in reverse order (bucket first, then startMs), ensuring it never observes a stale
     * startMs paired with a new bucket.
     */
    private volatile long inflightTaskStartMs = -1;

    /** The bucket of the current in-flight task, or null if idle. Acts as the "gate" variable. */
    private volatile @Nullable TableBucket inflightTaskBucket;

    public RebalanceManager(
            CoordinatorEventProcessor eventProcessor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock) {
        this(
                eventProcessor,
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
            CoordinatorEventProcessor eventProcessor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock,
            ScheduledExecutorService timeoutChecker) {
        this.eventProcessor = eventProcessor;
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
                            rebalancePlan ->
                                    registerRebalance(
                                            rebalancePlan.getRebalanceId(),
                                            rebalancePlan.getExecutePlan(),
                                            rebalancePlan.getRebalanceStatus(),
                                            rebalancePlan.getStartedAtMs(),
                                            rebalancePlan.getCompletedAtMs()));
        } catch (Exception e) {
            LOG.error(
                    "Failed to get rebalance plan from zookeeper, it will be treated as no"
                            + "rebalance tasks.",
                    e);
        }
    }

    public void registerRebalance(
            String rebalanceId,
            Map<TableBucket, RebalancePlanForBucket> rebalancePlan,
            RebalanceStatus newStatus) {
        registerRebalance(rebalanceId, rebalancePlan, newStatus, clock.milliseconds(), -1);
    }

    /** Registers a rebalance task, retaining its started/completed timestamps. */
    public void registerRebalance(
            String rebalanceId,
            Map<TableBucket, RebalancePlanForBucket> rebalancePlan,
            RebalanceStatus newStatus,
            long startedAtMs,
            long completedAtMs) {
        checkNotClosed();
        registerTime = System.currentTimeMillis();
        // first clear all exists tasks.
        inProgressRebalanceTasks.clear();
        inProgressRebalanceTasksQueue.clear();
        finishedRebalanceTasks.clear();
        // Clear gate (bucket) first, then data (startMs).
        inflightTaskBucket = null;
        inflightTaskStartMs = -1;

        currentRebalanceId = rebalanceId;
        currentStartedAtMs = startedAtMs;
        currentCompletedAtMs = completedAtMs;
        if (rebalancePlan.isEmpty()) {
            if (FINAL_STATUSES.contains(newStatus)) {
                // Restoring an already-final empty-plan task on failover (see initialize()): the
                // timestamps above already came from the restored task, so just adopt its status
                // rather than re-running completion (which would re-stamp completedAtMs and
                // rewrite history).
                rebalanceStatus = newStatus;
                return;
            }
            completeRebalance();
            return;
        }

        rebalancePlan.forEach(
                ((tableBucket, planForBucket) -> {
                    if (FINAL_STATUSES.contains(newStatus)) {
                        finishedRebalanceTasks.put(
                                tableBucket, RebalanceResultForBucket.of(planForBucket, newStatus));
                    } else {
                        inProgressRebalanceTasksQueue.add(tableBucket);
                        inProgressRebalanceTasks.put(
                                tableBucket,
                                RebalanceResultForBucket.of(planForBucket, NOT_STARTED));
                    }
                }));

        if (!inProgressRebalanceTasksQueue.isEmpty()) {
            // Trigger one rebalance task to execute.
            rebalanceStatus = REBALANCING;
            processNewRebalanceTask();
        } else {
            rebalanceStatus = newStatus;
        }
    }

    public void finishRebalanceTask(TableBucket tableBucket, RebalanceStatus statusForBucket) {
        checkNotClosed();
        if (inProgressRebalanceTasksQueue.contains(tableBucket)) {
            inProgressRebalanceTasksQueue.remove(tableBucket);
            RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.remove(tableBucket);
            checkNotNull(resultForBucket, "RebalanceResultForBucket is null.");
            finishedRebalanceTasks.put(
                    tableBucket,
                    RebalanceResultForBucket.of(resultForBucket.plan(), statusForBucket));
            // Clear gate (bucket) first, then data (startMs).
            inflightTaskBucket = null;
            inflightTaskStartMs = -1;
            LOG.info(
                    "Rebalance task {} in progress: {} tasks pending, {} completed.",
                    currentRebalanceId,
                    inProgressRebalanceTasksQueue.size(),
                    finishedRebalanceTasks.size());

            if (inProgressRebalanceTasksQueue.isEmpty()) {
                // All rebalance tasks are completed.
                completeRebalance();
            } else {
                // Trigger one rebalance task to execute.
                processNewRebalanceTask();
            }
        }
    }

    public @Nullable RebalanceProgress listRebalanceProgress(@Nullable String rebalanceId) {
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
                currentRebalanceId,
                rebalanceStatus,
                0.0,
                progressForBucketMap,
                currentStartedAtMs,
                currentCompletedAtMs);
    }

    /**
     * Returns a summary of the current rebalance, or {@code null} if there is none.
     *
     * <p>Must be called from the coordinator event thread so the id/status/timestamp fields form a
     * consistent snapshot.
     */
    public @Nullable RebalanceInfo currentRebalanceInfo() {
        checkNotClosed();
        if (currentRebalanceId == null) {
            return null;
        }
        return new RebalanceInfo(
                currentRebalanceId, rebalanceStatus, currentStartedAtMs, currentCompletedAtMs);
    }

    /** Event-thread convenience overload of {@link #listRebalances(RebalanceInfo)}. */
    @VisibleForTesting
    List<RebalanceInfo> listRebalances() {
        return listRebalances(currentRebalanceInfo());
    }

    /**
     * Returns the given current-rebalance summary (if any) followed by the ZooKeeper history,
     * newest first.
     *
     * <p>A finished rebalance stays current while also appearing in history, so the matching
     * history entry is skipped to avoid a duplicate.
     *
     * <p>Reads only ZooKeeper and the {@code currentRebalance}, so it may run off the coordinator
     * event thread.
     */
    public List<RebalanceInfo> listRebalances(@Nullable RebalanceInfo currentRebalance) {
        checkNotClosed();
        List<RebalanceInfo> rebalanceInfos = new ArrayList<>();
        String currentId = currentRebalance == null ? null : currentRebalance.rebalanceId();
        if (currentRebalance != null) {
            rebalanceInfos.add(currentRebalance);
        }

        List<RebalanceTask> history;
        try {
            history = zkClient.getRebalanceHistory();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to get rebalance history from zookeeper.", e);
        }
        for (RebalanceTask historyTask : history) {
            if (historyTask.getRebalanceId().equals(currentId)) {
                continue;
            }
            rebalanceInfos.add(
                    new RebalanceInfo(
                            historyTask.getRebalanceId(),
                            historyTask.getRebalanceStatus(),
                            historyTask.getStartedAtMs(),
                            historyTask.getCompletedAtMs()));
        }
        return rebalanceInfos;
    }

    public void cancelRebalance(@Nullable String rebalanceId) {
        checkNotClosed();

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

        long completedAtMs = clock.milliseconds();
        try {
            Optional<RebalanceTask> rebalanceTaskOpt = zkClient.getRebalanceTask();
            if (rebalanceTaskOpt.isPresent()) {
                RebalanceTask rebalanceTask = rebalanceTaskOpt.get();
                long startedAtMs = resolveStartedAtMs(rebalanceTask);
                RebalanceTask finalTask =
                        new RebalanceTask(
                                rebalanceTask.getRebalanceId(),
                                CANCELED,
                                rebalanceTask.getExecutePlan(),
                                startedAtMs,
                                completedAtMs);
                zkClient.registerRebalanceTask(finalTask);
                try {
                    zkClient.registerRebalanceHistory(finalTask, HISTORY_RETENTION_COUNT);
                } catch (Exception e) {
                    LOG.error(
                            "Error when writing rebalance task {} to history.",
                            finalTask.getRebalanceId(),
                            e);
                }
            }
        } catch (Exception e) {
            LOG.error("Error when delete rebalance plan from zookeeper.", e);
        }

        currentCompletedAtMs = completedAtMs;
        rebalanceStatus = CANCELED;
        inProgressRebalanceTasksQueue.clear();
        inProgressRebalanceTasks.clear();
        // Clear gate (bucket) first, then data (startMs).
        inflightTaskBucket = null;
        inflightTaskStartMs = -1;
        // Here, it will not clear finishedRebalanceTasks, because it will be used by
        // listRebalanceProgress. It will be cleared when next register.

        LOG.info("Cancel rebalance task success.");
    }

    public boolean hasInProgressRebalance() {
        checkNotClosed();
        return !inProgressRebalanceTasks.isEmpty() || !inProgressRebalanceTasksQueue.isEmpty();
    }

    public RebalanceTask generateRebalanceTask(List<Goal> goalsByPriority) {
        checkNotClosed();
        List<RebalancePlanForBucket> rebalancePlanForBuckets;
        String rebalanceId = UUID.randomUUID().toString();
        try {
            // Generate the latest cluster model.
            long startTime = System.currentTimeMillis();
            ClusterModel clusterModel = buildClusterModel(eventProcessor.getCoordinatorContext());
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

    public @Nullable RebalancePlanForBucket getRebalancePlanForBucket(TableBucket tableBucket) {
        checkNotClosed();
        RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
        if (resultForBucket != null) {
            return resultForBucket.plan();
        }
        return null;
    }

    private void processNewRebalanceTask() {
        TableBucket tableBucket = inProgressRebalanceTasksQueue.peek();
        if (tableBucket != null && inProgressRebalanceTasks.containsKey(tableBucket)) {
            // Write data (startMs) first, then publish gate (bucket).
            inflightTaskStartMs = clock.milliseconds();
            inflightTaskBucket = tableBucket;
            RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
            RebalanceResultForBucket rebalanceResultForBucket =
                    RebalanceResultForBucket.of(resultForBucket.plan(), REBALANCING);
            eventProcessor.tryToExecuteRebalanceTask(rebalanceResultForBucket.plan());
        }
    }

    private void completeRebalance() {
        checkNotClosed();
        long completedAtMs = clock.milliseconds();
        try {
            Optional<RebalanceTask> rebalanceTaskOpt = zkClient.getRebalanceTask();
            Map<TableBucket, RebalancePlanForBucket> bucketPlan;
            // When the read below finds no task at all, keep the in-memory startedAtMs.
            long startedAtMs = currentStartedAtMs;
            if (rebalanceTaskOpt.isPresent()) {
                RebalanceTask rebalanceTask = rebalanceTaskOpt.get();
                bucketPlan = rebalanceTask.getExecutePlan();
                startedAtMs = resolveStartedAtMs(rebalanceTask);
            } else {
                LOG.warn(
                        "Rebalance task is empty in zk when complete rebalance. "
                                + "It will be treated as no rebalance tasks.");
                bucketPlan = new HashMap<>();
            }
            RebalanceTask finalTask =
                    new RebalanceTask(
                            currentRebalanceId, COMPLETED, bucketPlan, startedAtMs, completedAtMs);
            zkClient.registerRebalanceTask(finalTask);
            // Only record history once the current-task znode carries the final status; a
            // COMPLETED history entry next to a non-final current task would re-execute the
            // finished rebalance on failover.
            try {
                zkClient.registerRebalanceHistory(finalTask, HISTORY_RETENTION_COUNT);
            } catch (Exception e) {
                LOG.error(
                        "Error when writing rebalance task {} to history.", currentRebalanceId, e);
            }
        } catch (Exception e) {
            LOG.error("Error when update rebalance plan from zookeeper.", e);
        }

        currentCompletedAtMs = completedAtMs;
        rebalanceStatus = COMPLETED;
        inProgressRebalanceTasks.clear();
        inProgressRebalanceTasksQueue.clear();

        // Here, it will not clear finishedRebalanceTasks, because it will be used by
        // listRebalanceProgress. It will be cleared when next register.

        LOG.info("Rebalance complete with {} ms.", System.currentTimeMillis() - registerTime);
    }

    /**
     * Prefers the just-read task's startedAtMs; falls back to the in-memory value when the znode
     * predates the timestamp fields (version 1), where both are -1.
     */
    private long resolveStartedAtMs(RebalanceTask rebalanceTask) {
        return rebalanceTask.getStartedAtMs() >= 0
                ? rebalanceTask.getStartedAtMs()
                : currentStartedAtMs;
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
        return new RebalanceTask(rebalanceId, NOT_STARTED, bucketPlan, clock.milliseconds(), -1);
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
        // Read gate (bucket) first, then data (startMs).
        // If bucket is non-null, happens-before guarantees startMs is at least as
        // fresh as the value written before bucket was published.
        TableBucket bucket = inflightTaskBucket;
        long startMs = inflightTaskStartMs;
        if (bucket == null || startMs < 0) {
            return;
        }
        long elapsed = clock.milliseconds() - startMs;
        if (elapsed > REBALANCE_TASK_TIMEOUT_MS) {
            LOG.warn(
                    "In-flight rebalance task for {} timed out after {}ms. "
                            + "Treating it as timed out and advancing to the next task.",
                    bucket,
                    elapsed);
            // Clear gate (bucket) first, then data (startMs), matching the
            // publication idiom so the next checkTimeout sees bucket==null.
            inflightTaskBucket = null;
            inflightTaskStartMs = -1;
            eventManager.put(new RebalanceTaskTimeoutEvent(bucket));
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
        return buildClusterModel(eventProcessor.getCoordinatorContext());
    }

    @VisibleForTesting
    @Nullable
    RebalanceStatus getRebalanceStatus() {
        return rebalanceStatus;
    }

    @VisibleForTesting
    long getCurrentStartedAtMs() {
        return currentStartedAtMs;
    }

    @VisibleForTesting
    long getCurrentCompletedAtMs() {
        return currentCompletedAtMs;
    }
}
