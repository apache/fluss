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
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FINAL_STATUSES;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A rebalance manager to generate rebalance plan, and execution rebalance plan.
 *
 * <p>The in-memory state belongs to the {@link CoordinatorEventProcessor} event thread, and every
 * method that reads or writes it must run there. The exceptions are {@link
 * #historicalRebalanceProgress} and {@link #listRebalances}, which read only ZooKeeper and their
 * arguments, so the processor runs them on its IO executor.
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

    /**
     * A mapping from table bucket to the bucket epoch its planned leader must acknowledge before
     * the in-flight task completes.
     */
    private final Map<TableBucket, Integer> awaitedLeaderAckBucketEpochs =
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
        Optional<RebalanceTask> rebalanceTaskOpt;
        try {
            rebalanceTaskOpt = zkClient.getRebalanceTask();
        } catch (Exception e) {
            LOG.error(
                    "Failed to get rebalance plan from zookeeper, it will be treated as no"
                            + "rebalance tasks.",
                    e);
            return;
        }
        rebalanceTaskOpt.ifPresent(
                rebalanceTask -> {
                    addMissingHistoryEntry(rebalanceTask);
                    eventManager.put(new RecoverRebalanceEvent(rebalanceTask));
                });
    }

    /**
     * Adds a finished task that the current znode holds but the history does not. A coordinator
     * from before the history existed wrote such a task, and the next rebalance would otherwise
     * overwrite it. A no-op rebalance stays out of the history, as in {@link #completeRebalance}.
     */
    private void addMissingHistoryEntry(RebalanceTask rebalanceTask) {
        if (!FINAL_STATUSES.contains(rebalanceTask.getRebalanceStatus())
                || rebalanceTask.getExecutePlan().isEmpty()) {
            return;
        }
        try {
            if (!zkClient.getRebalanceHistoryTask(rebalanceTask.getRebalanceId()).isPresent()) {
                zkClient.registerRebalanceHistoryTask(
                        rebalanceTask,
                        HISTORY_RETENTION_COUNT,
                        eventProcessor.getCoordinatorZkVersion());
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to add rebalance task {} to the rebalance history.",
                    rebalanceTask.getRebalanceId(),
                    e);
        }
    }

    /** Registers a new rebalance task started now. Test-only shorthand. */
    @VisibleForTesting
    public void registerRebalance(
            String rebalanceId,
            Map<TableBucket, RebalancePlanForBucket> rebalancePlan,
            RebalanceStatus newStatus) {
        registerRebalance(
                new RebalanceTask(rebalanceId, newStatus, rebalancePlan, clock.milliseconds(), -1));
    }

    /**
     * Registers a rebalance task. A task restored in a final status keeps each bucket's own final
     * status (see {@link RebalanceTask#getBucketStatus}).
     */
    public void registerRebalance(RebalanceTask task) {
        String rebalanceId = task.getRebalanceId();
        Map<TableBucket, RebalancePlanForBucket> rebalancePlan = task.getExecutePlan();
        RebalanceStatus newStatus = task.getRebalanceStatus();
        checkNotClosed();
        registerTime = System.currentTimeMillis();
        // first clear all exists tasks.
        inProgressRebalanceTasks.clear();
        inProgressRebalanceTasksQueue.clear();
        finishedRebalanceTasks.clear();
        awaitedLeaderAckBucketEpochs.clear();
        // Clear gate (bucket) first, then data (startMs).
        inflightTaskBucket = null;
        inflightTaskStartMs = -1;

        currentRebalanceId = rebalanceId;
        currentStartedAtMs = task.getStartedAtMs();
        currentCompletedAtMs = task.getCompletedAtMs();
        // A final task is restored on failover (see initialize()) and keeps its own timestamps.
        boolean isFinal = FINAL_STATUSES.contains(newStatus);
        if (rebalancePlan.isEmpty() && !isFinal) {
            completeRebalance();
            return;
        }

        if (isFinal) {
            finishedRebalanceTasks.putAll(toResults(task));
        } else {
            rebalancePlan.forEach(
                    (tableBucket, planForBucket) -> {
                        inProgressRebalanceTasksQueue.add(tableBucket);
                        inProgressRebalanceTasks.put(
                                tableBucket,
                                RebalanceResultForBucket.of(planForBucket, NOT_STARTED));
                    });
        }

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
            awaitedLeaderAckBucketEpochs.remove(tableBucket);
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

    /**
     * Returns the in-memory progress of the current rebalance, or {@code null} if there is none.
     * Reads no ZooKeeper, so it is safe on the coordinator event thread; the caller routes a
     * historical id (see {@link #isHistorical}) to {@link #historicalRebalanceProgress} instead.
     */
    public @Nullable RebalanceProgress listRebalanceProgress(@Nullable String rebalanceId) {
        checkNotClosed();
        checkArgument(
                !isHistorical(rebalanceId),
                "Rebalance task id %s is not the current rebalance task.",
                rebalanceId);
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
     * Returns whether the progress of the given id is served from the ZooKeeper history rather than
     * from memory. Must be called from the coordinator event thread.
     */
    public boolean isHistorical(@Nullable String rebalanceId) {
        return rebalanceId != null && !rebalanceId.equals(currentRebalanceId);
    }

    /**
     * Serves a finished rebalance from the ZooKeeper-backed history. Historical entries carry the
     * full per-bucket plan with each bucket's final status.
     *
     * <p>Throws {@link NoRebalanceInProgressException} for an id missing from the history.
     *
     * <p>Reads only ZooKeeper, so it may run off the coordinator event thread.
     */
    public RebalanceProgress historicalRebalanceProgress(String rebalanceId) {
        checkNotClosed();
        Optional<RebalanceTask> taskOpt;
        try {
            taskOpt = zkClient.getRebalanceHistoryTask(rebalanceId);
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to get rebalance history from zookeeper.", e);
        }
        if (!taskOpt.isPresent()) {
            throw new NoRebalanceInProgressException(
                    String.format(
                            "Rebalance task id %s is neither the current rebalance task nor in the"
                                    + " retained history. Known rebalances are listed by"
                                    + " Admin#listRebalances().",
                            rebalanceId));
        }
        RebalanceTask task = taskOpt.get();
        // the progress will be set at client.
        return new RebalanceProgress(
                task.getRebalanceId(),
                task.getRebalanceStatus(),
                0.0,
                toResults(task),
                task.getStartedAtMs(),
                task.getCompletedAtMs());
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

    /**
     * Returns the ZooKeeper history, newest first, preceded by the given current-rebalance summary
     * if it is not in the history yet (an unfinished or no-op rebalance).
     *
     * <p>A finished rebalance is listed from the history, not from the summary. The summary is
     * taken on the event thread before the history is read, so newer rebalances may have finished
     * in between; using the history entry keeps the list newest first.
     *
     * <p>Reads only ZooKeeper and the {@code currentRebalance}, so it may run off the coordinator
     * event thread.
     */
    public List<RebalanceInfo> listRebalances(@Nullable RebalanceInfo currentRebalance) {
        checkNotClosed();
        List<RebalanceTask> history;
        try {
            history = zkClient.getRebalanceHistory();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to get rebalance history from zookeeper.", e);
        }
        List<RebalanceInfo> rebalanceInfos = new ArrayList<>();
        if (currentRebalance != null
                && history.stream()
                        .noneMatch(
                                task ->
                                        task.getRebalanceId()
                                                .equals(currentRebalance.rebalanceId()))) {
            rebalanceInfos.add(currentRebalance);
        }
        for (RebalanceTask historyTask : history) {
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

        if (currentRebalanceId == null) {
            // Nothing to cancel. Never touch the znode here: the coordinator does not run the
            // task it holds, so it cannot tell whether the given id names it.
            if (rebalanceId != null) {
                throw new NoRebalanceInProgressException(
                        String.format(
                                "Rebalance task id %s to cancel is not the current rebalance task:"
                                        + " no rebalance task is current.",
                                rebalanceId));
            }
            return;
        }

        if (rebalanceId != null && !rebalanceId.equals(currentRebalanceId)) {
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
        // Every unfinished bucket becomes CANCELED, including the in-flight one: cancel stops
        // tracking it (getRebalancePlanForBucket() returns null from now on), so its move is
        // neither rolled back nor reported again.
        Map<TableBucket, RebalanceResultForBucket> unfinished = new HashMap<>();
        inProgressRebalanceTasks.forEach(
                (bucket, result) ->
                        unfinished.put(
                                bucket, RebalanceResultForBucket.of(result.plan(), CANCELED)));
        try {
            Optional<RebalanceTask> rebalanceTaskOpt = zkClient.getRebalanceTask();
            if (rebalanceTaskOpt.isPresent()) {
                RebalanceTask rebalanceTask = rebalanceTaskOpt.get();
                Map<TableBucket, RebalanceStatus> bucketStatuses = finishedBucketStatuses();
                unfinished.forEach((bucket, result) -> bucketStatuses.put(bucket, result.status()));
                RebalanceTask finalTask =
                        new RebalanceTask(
                                currentRebalanceId,
                                CANCELED,
                                rebalanceTask.getExecutePlan(),
                                bucketStatuses,
                                currentStartedAtMs,
                                completedAtMs);
                zkClient.registerFinalRebalanceTask(
                        finalTask,
                        HISTORY_RETENTION_COUNT,
                        eventProcessor.getCoordinatorZkVersion());
            }
        } catch (Exception e) {
            LOG.error("Error when writing the canceled rebalance task to zookeeper.", e);
        }

        currentCompletedAtMs = completedAtMs;
        rebalanceStatus = CANCELED;
        // Keep the unfinished buckets, matching the final task in ZooKeeper, so the current view
        // and the history (or failover-restored) view list the same buckets.
        finishedRebalanceTasks.putAll(unfinished);
        inProgressRebalanceTasksQueue.clear();
        inProgressRebalanceTasks.clear();
        awaitedLeaderAckBucketEpochs.clear();
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

    /**
     * Marks that the coordinator applied the final state of the in-flight task for the bucket. The
     * task completes once the planned leader acknowledges a request with at least the given bucket
     * epoch.
     */
    public void awaitLeaderAck(TableBucket tableBucket, int bucketEpoch) {
        checkNotClosed();
        if (inProgressRebalanceTasks.containsKey(tableBucket)) {
            awaitedLeaderAckBucketEpochs.put(tableBucket, bucketEpoch);
        }
    }

    /**
     * Returns the bucket epoch the planned leader must acknowledge, or null if the task for the
     * bucket is not waiting for the leader acknowledgement.
     */
    public @Nullable Integer getAwaitedLeaderAckBucketEpoch(TableBucket tableBucket) {
        checkNotClosed();
        return awaitedLeaderAckBucketEpochs.get(tableBucket);
    }

    /** Event-thread convenience overload of {@link #listRebalances(RebalanceInfo)}. */
    @VisibleForTesting
    List<RebalanceInfo> listRebalances() {
        return listRebalances(currentRebalanceInfo());
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
            if (rebalanceTaskOpt.isPresent()) {
                RebalanceTask rebalanceTask = rebalanceTaskOpt.get();
                RebalanceTask finalTask =
                        new RebalanceTask(
                                currentRebalanceId,
                                COMPLETED,
                                rebalanceTask.getExecutePlan(),
                                finishedBucketStatuses(),
                                currentStartedAtMs,
                                completedAtMs);
                int epochZkVersion = eventProcessor.getCoordinatorZkVersion();
                if (finalTask.getExecutePlan().isEmpty()) {
                    // Nothing was moved: keep a no-op rebalance out of the bounded history, so
                    // repeated calls on a balanced cluster cannot push real rebalances out.
                    zkClient.registerRebalanceTask(finalTask, epochZkVersion);
                } else {
                    zkClient.registerFinalRebalanceTask(
                            finalTask, HISTORY_RETENTION_COUNT, epochZkVersion);
                }
            } else {
                LOG.warn(
                        "Rebalance task is empty in zk when complete rebalance. "
                                + "It will be treated as no rebalance tasks.");
            }
        } catch (Exception e) {
            LOG.error("Error when writing the completed rebalance task to zookeeper.", e);
        }

        currentCompletedAtMs = completedAtMs;
        rebalanceStatus = COMPLETED;
        inProgressRebalanceTasks.clear();
        inProgressRebalanceTasksQueue.clear();
        awaitedLeaderAckBucketEpochs.clear();

        // Here, it will not clear finishedRebalanceTasks, because it will be used by
        // listRebalanceProgress. It will be cleared when next register.

        LOG.info("Rebalance complete with {} ms.", System.currentTimeMillis() - registerTime);
    }

    /**
     * Returns the final status of each finished bucket. {@link RebalanceTask} drops the ones equal
     * to the task status.
     */
    private Map<TableBucket, RebalanceStatus> finishedBucketStatuses() {
        Map<TableBucket, RebalanceStatus> statuses = new HashMap<>();
        finishedRebalanceTasks.forEach((bucket, result) -> statuses.put(bucket, result.status()));
        return statuses;
    }

    /** Returns each bucket's plan with its final status (see {@link RebalanceTask}). */
    private static Map<TableBucket, RebalanceResultForBucket> toResults(RebalanceTask task) {
        Map<TableBucket, RebalanceResultForBucket> results = new HashMap<>();
        task.getExecutePlan()
                .forEach(
                        (bucket, plan) ->
                                results.put(
                                        bucket,
                                        RebalanceResultForBucket.of(
                                                plan, task.getBucketStatus(bucket))));
        return results;
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
