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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorEventProcessor;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RebalanceMaxInflightTasksChangedEvent;
import org.apache.fluss.server.coordinator.event.RebalanceTaskTimeoutEvent;
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
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FINAL_STATUSES;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A rebalance manager to generate rebalance plan, and execution rebalance plan.
 *
 * <p>This manager is used in {@link CoordinatorEventProcessor}'s single-threaded event model.
 * Non-event threads, such as startup recovery, dynamic configuration callbacks, and the timeout
 * checker, must enqueue coordinator events instead of directly submitting tasks or advancing
 * rebalance state.
 */
public class RebalanceManager implements ServerReconfigurable {
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    /** Hardcoded timeout for an in-flight rebalance task: 2 minutes. */
    private static final long REBALANCE_TASK_TIMEOUT_MS = 2 * 60 * 1000L;

    /** Hardcoded interval for the periodic timeout check: 30 seconds. */
    private static final long TIMEOUT_CHECK_INTERVAL_MS = 30 * 1000L;

    private final ZooKeeperClient zkClient;
    private final CoordinatorEventProcessor eventProcessor;
    private final EventManager eventManager;
    private final Clock clock;
    private final ScheduledExecutorService timeoutChecker;

    /** A queue of pending table buckets to rebalance. */
    private final Queue<TableBucket> pendingRebalanceTasksQueue = new ArrayDeque<>();

    /** A mapping from table bucket to rebalance status of pending and running tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> inProgressRebalanceTasks =
            new ConcurrentHashMap<>();

    /** A mapping from running table bucket to the time when the task was started. */
    private final Map<TableBucket, Long> inflightRebalanceTaskStartMs = new ConcurrentHashMap<>();

    /** A mapping from table bucket to rebalance status of failed or completed tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> finishedRebalanceTasks =
            new ConcurrentHashMap<>();

    private final GoalOptimizer goalOptimizer;
    private volatile int maxInflightRebalanceTasks;
    private volatile int queuedMaxInflightRebalanceTasks;
    private volatile long registerTime;
    private volatile @Nullable RebalanceStatus rebalanceStatus;
    private volatile @Nullable String currentRebalanceId;
    private volatile boolean isClosed = false;

    public RebalanceManager(
            CoordinatorEventProcessor eventProcessor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock) {
        this(eventProcessor, zkClient, eventManager, clock, new Configuration());
    }

    public RebalanceManager(
            CoordinatorEventProcessor eventProcessor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock,
            Configuration conf) {
        this(
                eventProcessor,
                zkClient,
                eventManager,
                clock,
                conf,
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
        this(eventProcessor, zkClient, eventManager, clock, new Configuration(), timeoutChecker);
    }

    @VisibleForTesting
    RebalanceManager(
            CoordinatorEventProcessor eventProcessor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock,
            Configuration conf,
            ScheduledExecutorService timeoutChecker) {
        this.eventProcessor = eventProcessor;
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
                            rebalanceTask ->
                                    eventManager.put(new RecoverRebalanceEvent(rebalanceTask)));
        } catch (Exception e) {
            LOG.error(
                    "Failed to get rebalance plan from zookeeper, it will be treated as no"
                            + "rebalance tasks.",
                    e);
        }
    }

    public synchronized void registerRebalance(
            String rebalanceId,
            Map<TableBucket, RebalancePlanForBucket> rebalancePlan,
            RebalanceStatus newStatus) {
        checkNotClosed();
        registerTime = System.currentTimeMillis();
        // first clear all exists tasks.
        inProgressRebalanceTasks.clear();
        pendingRebalanceTasksQueue.clear();
        inflightRebalanceTaskStartMs.clear();
        finishedRebalanceTasks.clear();

        currentRebalanceId = rebalanceId;
        if (rebalancePlan.isEmpty()) {
            completeRebalance();
            return;
        }

        rebalancePlan.forEach(
                ((tableBucket, planForBucket) -> {
                    if (FINAL_STATUSES.contains(newStatus)) {
                        finishedRebalanceTasks.put(
                                tableBucket, RebalanceResultForBucket.of(planForBucket, newStatus));
                    } else {
                        pendingRebalanceTasksQueue.add(tableBucket);
                        inProgressRebalanceTasks.put(
                                tableBucket,
                                RebalanceResultForBucket.of(planForBucket, NOT_STARTED));
                    }
                }));

        if (!pendingRebalanceTasksQueue.isEmpty()) {
            // Trigger rebalance tasks to execute.
            rebalanceStatus = REBALANCING;
            processNewRebalanceTasks();
        } else {
            rebalanceStatus = newStatus;
        }
    }

    public synchronized void finishRebalanceTask(
            TableBucket tableBucket, RebalanceStatus statusForBucket) {
        checkNotClosed();
        RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
        if (resultForBucket == null || resultForBucket.status() != REBALANCING) {
            return;
        }

        inProgressRebalanceTasks.remove(tableBucket);
        pendingRebalanceTasksQueue.remove(tableBucket);
        inflightRebalanceTaskStartMs.remove(tableBucket);
        finishedRebalanceTasks.put(
                tableBucket, RebalanceResultForBucket.of(resultForBucket.plan(), statusForBucket));
        LOG.info(
                "Rebalance task {} in progress: {} tasks pending, {} tasks in-flight, {} completed.",
                currentRebalanceId,
                pendingRebalanceTasksQueue.size(),
                inflightRebalanceTaskStartMs.size(),
                finishedRebalanceTasks.size());

        if (inProgressRebalanceTasks.isEmpty()) {
            // All rebalance tasks are completed.
            completeRebalance();
        } else {
            // Trigger new rebalance tasks to execute if there is available capacity.
            processNewRebalanceTasks();
        }
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

        try {
            Optional<RebalanceTask> rebalanceTaskOpt = zkClient.getRebalanceTask();
            if (rebalanceTaskOpt.isPresent()) {
                RebalanceTask rebalanceTask = rebalanceTaskOpt.get();
                zkClient.registerRebalanceTask(
                        new RebalanceTask(
                                rebalanceTask.getRebalanceId(),
                                CANCELED,
                                rebalanceTask.getExecutePlan()));
            }
        } catch (Exception e) {
            LOG.error("Error when delete rebalance plan from zookeeper.", e);
        }

        rebalanceStatus = CANCELED;
        pendingRebalanceTasksQueue.clear();
        inProgressRebalanceTasks.clear();
        inflightRebalanceTaskStartMs.clear();
        // Here, it will not clear finishedRebalanceTasks, because it will be used by
        // listRebalanceProgress. It will be cleared when next register.

        LOG.info("Cancel rebalance task success.");
    }

    public synchronized boolean hasInProgressRebalance() {
        checkNotClosed();
        return !inProgressRebalanceTasks.isEmpty() || !pendingRebalanceTasksQueue.isEmpty();
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

    public synchronized @Nullable RebalancePlanForBucket getRebalancePlanForBucket(
            TableBucket tableBucket) {
        checkNotClosed();
        RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
        if (resultForBucket != null && resultForBucket.status() == REBALANCING) {
            return resultForBucket.plan();
        }
        return null;
    }

    private void processNewRebalanceTasks() {
        while (inflightRebalanceTaskStartMs.size() < maxInflightRebalanceTasks) {
            TableBucket tableBucket = pendingRebalanceTasksQueue.poll();
            if (tableBucket == null) {
                return;
            }

            RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
            if (resultForBucket == null || resultForBucket.status() != NOT_STARTED) {
                continue;
            }

            RebalanceResultForBucket rebalanceResultForBucket =
                    RebalanceResultForBucket.of(resultForBucket.plan(), REBALANCING);
            inProgressRebalanceTasks.put(tableBucket, rebalanceResultForBucket);
            inflightRebalanceTaskStartMs.put(tableBucket, clock.milliseconds());
            eventProcessor.tryToExecuteRebalanceTask(rebalanceResultForBucket.plan());
        }
    }

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

    private void completeRebalance() {
        checkNotClosed();
        try {
            Optional<RebalanceTask> rebalanceTaskOpt = zkClient.getRebalanceTask();
            Map<TableBucket, RebalancePlanForBucket> bucketPlan;
            if (rebalanceTaskOpt.isPresent()) {
                bucketPlan = rebalanceTaskOpt.get().getExecutePlan();
            } else {
                LOG.warn(
                        "Rebalance task is empty in zk when complete rebalance. "
                                + "It will be treated as no rebalance tasks.");
                bucketPlan = new HashMap<>();
            }
            zkClient.registerRebalanceTask(
                    new RebalanceTask(currentRebalanceId, COMPLETED, bucketPlan));
        } catch (Exception e) {
            LOG.error("Error when update rebalance plan from zookeeper.", e);
        }

        rebalanceStatus = COMPLETED;
        inProgressRebalanceTasks.clear();
        pendingRebalanceTasksQueue.clear();
        inflightRebalanceTaskStartMs.clear();

        // Here, it will not clear finishedRebalanceTasks, because it will be used by
        // listRebalanceProgress. It will be cleared when next register.

        LOG.info("Rebalance complete with {} ms.", System.currentTimeMillis() - registerTime);
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
        for (Map.Entry<TableBucket, Long> entry :
                new HashMap<>(inflightRebalanceTaskStartMs).entrySet()) {
            TableBucket bucket = entry.getKey();
            long startMs = entry.getValue();
            long elapsed = clock.milliseconds() - startMs;
            if (elapsed > REBALANCE_TASK_TIMEOUT_MS) {
                LOG.warn(
                        "In-flight rebalance task for {} timed out after {}ms. "
                                + "Treating it as timed out and advancing to the next task.",
                        bucket,
                        elapsed);
                eventManager.put(new RebalanceTaskTimeoutEvent(bucket));
            }
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
    int getMaxInflightRebalanceTasks() {
        return maxInflightRebalanceTasks;
    }
}
