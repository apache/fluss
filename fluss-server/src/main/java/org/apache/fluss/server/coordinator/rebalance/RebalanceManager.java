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
import org.apache.fluss.exception.NoRebalanceInProgressException;
import org.apache.fluss.exception.RebalanceFailureException;
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
import org.apache.fluss.server.zk.data.RebalanceExecution;
import org.apache.fluss.server.zk.data.RebalanceRound;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

    private final ZooKeeperClient zkClient;
    private final CoordinatorEventProcessor eventProcessor;
    private final EventManager eventManager;
    private final Clock clock;
    private final ScheduledExecutorService timeoutChecker;
    private final int rebalanceMaxBucketsPerRound;

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
    private volatile @Nullable RebalanceExecution currentRebalanceExecution;
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
                Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("rebalance-timeout")),
                conf);
    }

    @VisibleForTesting
    RebalanceManager(
            CoordinatorEventProcessor eventProcessor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock,
            ScheduledExecutorService timeoutChecker) {
        this(eventProcessor, zkClient, eventManager, clock, timeoutChecker, new Configuration());
    }

    @VisibleForTesting
    RebalanceManager(
            CoordinatorEventProcessor eventProcessor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock,
            ScheduledExecutorService timeoutChecker,
            Configuration conf) {
        this.eventProcessor = eventProcessor;
        this.zkClient = zkClient;
        this.eventManager = eventManager;
        this.clock = clock == null ? SystemClock.getInstance() : clock;
        this.timeoutChecker = timeoutChecker;
        this.rebalanceMaxBucketsPerRound =
                Math.max(0, conf.getInt(ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND));
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
            Optional<RebalanceExecution> rebalanceExecution = zkClient.getRebalanceExecution();
            if (rebalanceExecution.isPresent()) {
                restoreRoundBasedRebalance(rebalanceExecution.get());
                return;
            }

            Optional<RebalanceTask> rebalanceTask = zkClient.getRebalanceTask();
            if (rebalanceTask.isPresent()) {
                RebalanceTask task = rebalanceTask.get();
                currentRebalanceExecution = null;
                registerRebalance(
                        task.getRebalanceId(), task.getExecutePlan(), task.getRebalanceStatus());
            }
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
        Map<TableBucket, RebalanceResultForBucket> rebalanceResults = new HashMap<>();
        for (Map.Entry<TableBucket, RebalancePlanForBucket> entry : rebalancePlan.entrySet()) {
            rebalanceResults.put(
                    entry.getKey(), RebalanceResultForBucket.of(entry.getValue(), newStatus));
        }
        registerRebalanceResults(rebalanceId, rebalanceResults, newStatus);
    }

    private void registerRebalanceResults(
            String rebalanceId,
            Map<TableBucket, RebalanceResultForBucket> rebalanceResults,
            RebalanceStatus newStatus) {
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
        if (rebalanceResults.isEmpty()) {
            completeRebalance();
            return;
        }

        rebalanceResults.forEach(
                ((tableBucket, resultForBucket) -> {
                    if (FINAL_STATUSES.contains(resultForBucket.status())) {
                        finishedRebalanceTasks.put(
                                tableBucket,
                                RebalanceResultForBucket.of(
                                        resultForBucket.plan(), resultForBucket.status()));
                    } else {
                        inProgressRebalanceTasksQueue.add(tableBucket);
                        inProgressRebalanceTasks.put(
                                tableBucket,
                                RebalanceResultForBucket.of(
                                        resultForBucket.plan(), resultForBucket.status()));
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
            updateCurrentRoundBucketStatus(tableBucket, resultForBucket.plan(), statusForBucket);
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
                if (currentRebalanceExecution == null) {
                    completeRebalance();
                } else {
                    completeRoundAndMaybeStartNext();
                }
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

        Optional<RebalanceProgress> roundBasedProgress = listRoundBasedRebalanceProgress();
        if (roundBasedProgress.isPresent()) {
            return roundBasedProgress.get();
        }

        Map<TableBucket, RebalanceResultForBucket> progressForBucketMap = new HashMap<>();
        progressForBucketMap.putAll(inProgressRebalanceTasks);
        progressForBucketMap.putAll(finishedRebalanceTasks);
        // the progress will be set at client.
        return new RebalanceProgress(
                currentRebalanceId, rebalanceStatus, 0.0, progressForBucketMap);
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

        cancelRoundBasedRebalance();

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
        RebalanceExecution rebalanceExecution = currentRebalanceExecution;
        if (rebalanceExecution != null
                && !FINAL_STATUSES.contains(rebalanceExecution.getRebalanceStatus())) {
            return true;
        }
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

    /**
     * Generates and registers a rebalance task. Large plans are split into recoverable rounds when
     * {@link ConfigOptions#COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND} is configured with a
     * positive value.
     */
    public RebalanceTask generateAndRegisterRebalance(List<Goal> goalsByPriority) {
        checkNotClosed();
        RebalanceTask rebalanceTask = generateRebalanceTask(goalsByPriority);
        try {
            return registerGeneratedRebalanceTask(rebalanceTask);
        } catch (Exception e) {
            throw new RebalanceFailureException(
                    String.format(
                            "Failed to generate plan and execute rebalance. The root cause: %s",
                            e.getMessage()),
                    e);
        }
    }

    @VisibleForTesting
    RebalanceTask registerGeneratedRebalanceTask(RebalanceTask rebalanceTask) throws Exception {
        Map<TableBucket, RebalancePlanForBucket> executePlan = rebalanceTask.getExecutePlan();

        zkClient.deleteRebalanceTask();
        if (!shouldSplitRebalancePlan(executePlan)) {
            currentRebalanceExecution = null;
            zkClient.registerRebalanceTask(rebalanceTask);
            registerRebalance(
                    rebalanceTask.getRebalanceId(), executePlan, RebalanceStatus.NOT_STARTED);
            return rebalanceTask;
        }

        List<RebalanceRound> rebalanceRounds = splitRebalancePlan(executePlan);
        RebalanceExecution rebalanceExecution =
                new RebalanceExecution(
                        rebalanceTask.getRebalanceId(),
                        REBALANCING,
                        rebalanceMaxBucketsPerRound,
                        0,
                        rebalanceRounds.size());

        for (RebalanceRound rebalanceRound : rebalanceRounds) {
            zkClient.registerRebalanceRound(rebalanceRound);
        }
        zkClient.registerRebalanceExecution(rebalanceExecution);

        currentRebalanceExecution = rebalanceExecution;
        RebalanceRound firstRound = rebalanceRounds.get(0);
        RebalanceTask firstRoundTask =
                new RebalanceTask(
                        rebalanceTask.getRebalanceId(),
                        RebalanceStatus.NOT_STARTED,
                        firstRound.getExecutePlan());
        zkClient.registerRebalanceTask(firstRoundTask);
        registerRebalanceResults(
                rebalanceTask.getRebalanceId(),
                firstRound.getProgressForBucketMap(),
                RebalanceStatus.NOT_STARTED);
        LOG.info(
                "Split rebalance task {} into {} rounds by {}={}, current round buckets {}.",
                rebalanceTask.getRebalanceId(),
                rebalanceRounds.size(),
                ConfigOptions.COORDINATOR_REBALANCE_MAX_BUCKETS_PER_ROUND.key(),
                rebalanceMaxBucketsPerRound,
                firstRound.getProgressForBucketMap().size());
        return firstRoundTask;
    }

    private boolean shouldSplitRebalancePlan(Map<TableBucket, RebalancePlanForBucket> executePlan) {
        return rebalanceMaxBucketsPerRound > 0 && executePlan.size() > rebalanceMaxBucketsPerRound;
    }

    private List<RebalanceRound> splitRebalancePlan(
            Map<TableBucket, RebalancePlanForBucket> executePlan) {
        List<Map.Entry<TableBucket, RebalancePlanForBucket>> sortedEntries =
                new ArrayList<>(executePlan.entrySet());
        sortedEntries.sort(
                Comparator.comparing(
                                (Map.Entry<TableBucket, RebalancePlanForBucket> entry) ->
                                        entry.getKey().getTableId())
                        .thenComparing(
                                entry ->
                                        entry.getKey().getPartitionId() == null
                                                ? Long.MIN_VALUE
                                                : entry.getKey().getPartitionId())
                        .thenComparing(entry -> entry.getKey().getBucket()));

        List<RebalanceRound> rebalanceRounds = new ArrayList<>();
        for (int i = 0; i < sortedEntries.size(); i += rebalanceMaxBucketsPerRound) {
            int end = Math.min(i + rebalanceMaxBucketsPerRound, sortedEntries.size());
            Map<TableBucket, RebalancePlanForBucket> roundPlan = new LinkedHashMap<>();
            for (int j = i; j < end; j++) {
                Map.Entry<TableBucket, RebalancePlanForBucket> entry = sortedEntries.get(j);
                roundPlan.put(entry.getKey(), entry.getValue());
            }
            rebalanceRounds.add(RebalanceRound.ofPlan(rebalanceRounds.size(), roundPlan));
        }
        return rebalanceRounds;
    }

    private void restoreRoundBasedRebalance(RebalanceExecution rebalanceExecution)
            throws Exception {
        List<RebalanceRound> rebalanceRounds = zkClient.getRebalanceRounds();
        currentRebalanceId = rebalanceExecution.getRebalanceId();
        rebalanceStatus = rebalanceExecution.getRebalanceStatus();
        currentRebalanceExecution = rebalanceExecution;

        if (FINAL_STATUSES.contains(rebalanceExecution.getRebalanceStatus())) {
            LOG.info(
                    "Restore final round-based rebalance {} with status {}.",
                    rebalanceExecution.getRebalanceId(),
                    rebalanceExecution.getRebalanceStatus());
            return;
        }

        Optional<RebalanceRound> roundToResume = findFirstUnfinishedRound(rebalanceRounds);
        if (!roundToResume.isPresent()) {
            RebalanceExecution completedExecution = rebalanceExecution.withStatus(COMPLETED);
            zkClient.registerRebalanceExecution(completedExecution);
            currentRebalanceExecution = completedExecution;
            rebalanceStatus = COMPLETED;
            return;
        }

        RebalanceRound rebalanceRound = roundToResume.get();
        RebalanceExecution resumedExecution =
                rebalanceExecution.withStatusAndCurrentRound(
                        REBALANCING, rebalanceRound.getRoundIndex());
        zkClient.registerRebalanceExecution(resumedExecution);
        currentRebalanceExecution = resumedExecution;
        zkClient.registerRebalanceTask(
                new RebalanceTask(
                        resumedExecution.getRebalanceId(),
                        RebalanceStatus.NOT_STARTED,
                        rebalanceRound.getExecutePlan()));
        registerRebalanceResults(
                resumedExecution.getRebalanceId(),
                rebalanceRound.getProgressForBucketMap(),
                RebalanceStatus.NOT_STARTED);
    }

    private Optional<RebalanceRound> findFirstUnfinishedRound(List<RebalanceRound> rounds) {
        for (RebalanceRound round : rounds) {
            if (round.hasUnfinishedTasks()) {
                return Optional.of(round);
            }
        }
        return Optional.empty();
    }

    private void updateCurrentRoundBucketStatus(
            TableBucket tableBucket,
            RebalancePlanForBucket planForBucket,
            RebalanceStatus rebalanceStatus) {
        RebalanceExecution rebalanceExecution = currentRebalanceExecution;
        if (rebalanceExecution == null) {
            return;
        }

        try {
            Optional<RebalanceRound> rebalanceRound =
                    zkClient.getRebalanceRound(rebalanceExecution.getCurrentRound());
            if (!rebalanceRound.isPresent()) {
                LOG.warn(
                        "No rebalance round {} found when updating bucket {} status to {}.",
                        rebalanceExecution.getCurrentRound(),
                        tableBucket,
                        rebalanceStatus);
                return;
            }
            zkClient.registerRebalanceRound(
                    rebalanceRound
                            .get()
                            .withBucketStatus(tableBucket, planForBucket, rebalanceStatus));
        } catch (Exception e) {
            throw new RebalanceFailureException(
                    String.format(
                            "Failed to update rebalance round status for bucket %s. The root cause: %s",
                            tableBucket, e.getMessage()),
                    e);
        }
    }

    private void completeRoundAndMaybeStartNext() {
        RebalanceExecution rebalanceExecution = checkNotNull(currentRebalanceExecution);
        try {
            int nextRoundIndex = rebalanceExecution.getCurrentRound() + 1;
            if (nextRoundIndex >= rebalanceExecution.getTotalRounds()) {
                RebalanceExecution completedExecution = rebalanceExecution.withStatus(COMPLETED);
                zkClient.registerRebalanceExecution(completedExecution);
                currentRebalanceExecution = completedExecution;
                Optional<RebalanceRound> currentRound =
                        zkClient.getRebalanceRound(rebalanceExecution.getCurrentRound());
                zkClient.registerRebalanceTask(
                        new RebalanceTask(
                                rebalanceExecution.getRebalanceId(),
                                COMPLETED,
                                currentRound
                                        .map(RebalanceRound::getExecutePlan)
                                        .orElseGet(HashMap::new)));
                completeRebalanceInMemory();
                return;
            }

            Optional<RebalanceRound> nextRound = zkClient.getRebalanceRound(nextRoundIndex);
            if (!nextRound.isPresent()) {
                throw new RebalanceFailureException(
                        String.format(
                                "Rebalance round %s does not exist for rebalance id %s.",
                                nextRoundIndex, rebalanceExecution.getRebalanceId()));
            }

            RebalanceExecution nextExecution =
                    rebalanceExecution.withStatusAndCurrentRound(REBALANCING, nextRoundIndex);
            zkClient.registerRebalanceExecution(nextExecution);
            currentRebalanceExecution = nextExecution;
            zkClient.registerRebalanceTask(
                    new RebalanceTask(
                            nextExecution.getRebalanceId(),
                            RebalanceStatus.NOT_STARTED,
                            nextRound.get().getExecutePlan()));
            registerRebalanceResults(
                    nextExecution.getRebalanceId(),
                    nextRound.get().getProgressForBucketMap(),
                    RebalanceStatus.NOT_STARTED);
            LOG.info(
                    "Start rebalance round {}/{} for rebalance id {} with {} buckets.",
                    nextRoundIndex + 1,
                    nextExecution.getTotalRounds(),
                    nextExecution.getRebalanceId(),
                    nextRound.get().getProgressForBucketMap().size());
        } catch (Exception e) {
            throw new RebalanceFailureException(
                    String.format(
                            "Failed to complete current rebalance round. The root cause: %s",
                            e.getMessage()),
                    e);
        }
    }

    private Optional<RebalanceProgress> listRoundBasedRebalanceProgress() {
        RebalanceExecution rebalanceExecution = currentRebalanceExecution;
        if (rebalanceExecution == null) {
            try {
                Optional<RebalanceExecution> persistedExecution = zkClient.getRebalanceExecution();
                if (persistedExecution.isPresent()) {
                    rebalanceExecution = persistedExecution.get();
                }
            } catch (Exception e) {
                LOG.error("Error when loading round-based rebalance execution from zookeeper.", e);
                return Optional.empty();
            }
        }

        if (rebalanceExecution == null) {
            return Optional.empty();
        }

        try {
            Map<TableBucket, RebalanceResultForBucket> progressForBucketMap = new HashMap<>();
            for (RebalanceRound rebalanceRound : zkClient.getRebalanceRounds()) {
                progressForBucketMap.putAll(rebalanceRound.getProgressForBucketMap());
            }
            return Optional.of(
                    new RebalanceProgress(
                            rebalanceExecution.getRebalanceId(),
                            rebalanceExecution.getRebalanceStatus(),
                            0.0,
                            progressForBucketMap));
        } catch (Exception e) {
            LOG.error("Error when loading round-based rebalance progress from zookeeper.", e);
            return Optional.empty();
        }
    }

    private void cancelRoundBasedRebalance() {
        RebalanceExecution rebalanceExecution = currentRebalanceExecution;
        if (rebalanceExecution == null) {
            return;
        }

        try {
            for (RebalanceRound rebalanceRound : zkClient.getRebalanceRounds()) {
                zkClient.registerRebalanceRound(rebalanceRound.withUnfinishedStatus(CANCELED));
            }
            RebalanceExecution canceledExecution = rebalanceExecution.withStatus(CANCELED);
            zkClient.registerRebalanceExecution(canceledExecution);
            currentRebalanceExecution = canceledExecution;
        } catch (Exception e) {
            throw new RebalanceFailureException(
                    String.format(
                            "Failed to cancel round-based rebalance. The root cause: %s",
                            e.getMessage()),
                    e);
        }
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

        completeRebalanceInMemory();
    }

    private void completeRebalanceInMemory() {
        rebalanceStatus = COMPLETED;
        inProgressRebalanceTasks.clear();
        inProgressRebalanceTasksQueue.clear();
        // Clear gate (bucket) first, then data (startMs).
        inflightTaskBucket = null;
        inflightTaskStartMs = -1;

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
}
