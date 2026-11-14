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
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.exception.NoRebalanceInProgressException;
import org.apache.fluss.exception.RebalanceFailureException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.event.CheckRebalanceTimeoutEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.RebalancePersistenceCompleteEvent;
import org.apache.fluss.server.coordinator.event.ReconcileRebalanceTaskEvent;
import org.apache.fluss.server.coordinator.event.RecoverRebalanceEvent;
import org.apache.fluss.server.coordinator.rebalance.RebalanceExecutor.ExecutionResult;
import org.apache.fluss.server.coordinator.rebalance.goal.Goal;
import org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizer;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.coordinator.rebalance.model.RackModel;
import org.apache.fluss.server.coordinator.rebalance.model.ServerModel;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.CANCELED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.COMPLETED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FAILED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FINAL_STATUSES;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.NOT_STARTED;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.REBALANCING;
import static org.apache.fluss.cluster.rebalance.RebalanceStatus.TIMEOUT;
import static org.apache.fluss.server.coordinator.rebalance.goal.GoalOptimizerUtils.getDiff;
import static org.apache.fluss.server.zk.data.CoordinatorAddress.REMOTE_REBALANCE_TASK_FEATURE;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Generates rebalance plans and tracks their bucket migrations through completion.
 *
 * <p>Bucket task state transitions:
 *
 * <pre>
 * NOT_STARTED --admit------------------------> REBALANCING
 * REBALANCING --current leader acknowledges--> COMPLETED
 * REBALANCING --execution timeout------------> TIMEOUT
 * TIMEOUT     --reconcile with backoff-------> TIMEOUT
 * TIMEOUT     --current leader acknowledges--> COMPLETED
 * TIMEOUT     --unavailable/no-progress limit-> FAILED
 * NOT_STARTED / REBALANCING / TIMEOUT
 *             --cancel before migration-----> CANCELED
 * Any non-final task --bucket deleted--------> COMPLETED
 * </pre>
 *
 * <p>Normally running buckets occupy the configured execution slots. TIMEOUT releases a slot but
 * remains non-final and tracked; reaching the configured tracking limit pauses admission of new
 * buckets. Cancellation is persisted before local state changes, including which buckets were
 * canceled. New pending tasks are canceled directly; recovered or admitted tasks must still be at
 * their origin to be canceled. Recovery admits intermediate tasks into reconciliation up to the
 * configured tracking limit, and still requires a current leader acknowledgement even when the
 * persisted state already matches the target.
 *
 * <p>The overall rebalance remains REBALANCING while buckets are pending, running, or timed out,
 * and while its final state cannot be persisted. Once all buckets are final, any failed bucket
 * yields FAILED; otherwise a cancellation request yields CANCELED, and all completed buckets yield
 * COMPLETED. Final-state persistence is retried before another rebalance can be admitted. A failed
 * bucket can retain an intermediate assignment for a subsequent rebalance to clean up.
 *
 * <p>All rebalance state is confined to the coordinator event thread. The shared scheduler only
 * reads {@link #hasActiveRebalance} and enqueues a timeout-check event; the event thread performs
 * the actual timeout and reconciliation work. The dynamic concurrency setting may be updated by a
 * configuration callback, so that scalar is independently volatile. Recovery I/O runs on the
 * coordinator I/O executor, but only enqueues a recovered task or clears the volatile empty-task
 * gate; it never mutates event-confined task state. Startup recovery is tracked by {@link
 * #recoveryPending}, which fences new rebalance requests until recovery has finished regardless of
 * when the event loop starts.
 */
@NotThreadSafe
public class RebalanceManager implements ServerReconfigurable {
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceManager.class);

    /** Hardcoded timeout for an in-flight rebalance task: 2 minutes. */
    private static final long REBALANCE_TASK_TIMEOUT_MS = 2 * 60 * 1000L;

    /** Hardcoded interval for the periodic timeout check: 30 seconds. */
    private static final long TIMEOUT_CHECK_INTERVAL_MS = 30 * 1000L;

    /** Hardcoded upper bound for the exponential reconciliation backoff: 5 minutes. */
    private static final long MAX_RECONCILE_BACKOFF_MS = 5 * 60 * 1000L;

    private static final long INITIAL_RECOVERY_RETRY_DELAY_MS = 5000L;
    private static final long MAX_RECOVERY_RETRY_DELAY_MS = 5 * 60 * 1000L;

    private final RebalanceTaskStore rebalanceTaskStore;
    private final RebalanceExecutor rebalanceExecutor;
    private final EventManager eventManager;
    private final Executor ioExecutor;
    private final Clock clock;
    private final Scheduler scheduler;
    private final long targetUnavailableTimeoutMs;
    private final long noProgressTimeoutMs;
    private final int maxTrackedTimedOutTasks;
    private final AtomicInteger recoveryFailures = new AtomicInteger();

    /** Tasks awaiting admission. */
    private final Queue<TableBucket> pendingRebalanceTasks = new ArrayDeque<>();

    /** Recovered intermediate tasks awaiting bounded reconciliation admission. */
    private final Queue<TableBucket> pendingRecoveredTasks = new ArrayDeque<>();

    /** A mapping from table bucket to rebalance status of all non-final tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> inProgressRebalanceTasks =
            new HashMap<>();

    /** Normally running tasks that occupy configured execution slots. */
    private final Map<TableBucket, RebalanceTaskAttempt> runningRebalanceTasks = new HashMap<>();

    /** Soft-timed-out tasks that no longer occupy the normal execution slot. */
    private final Map<TableBucket, RebalanceTaskAttempt> timedOutRebalanceTasks = new HashMap<>();

    private final Set<RebalanceExecutionKey> queuedReconcileEvents = new HashSet<>();

    /** A mapping from table bucket to rebalance status of failed or completed tasks. */
    private final Map<TableBucket, RebalanceResultForBucket> finishedRebalanceTasks =
            new HashMap<>();

    private final GoalOptimizer goalOptimizer;

    /** The only dynamically reconfigured state that may be updated outside the event thread. */
    private volatile int maxInflightRebalanceTasks;

    /** Cross-thread signal read by the shared scheduler; all task details remain event-confined. */
    private volatile boolean hasActiveRebalance;

    /**
     * Whether the startup recovery workflow is pending, not whether the recovered rebalance itself
     * is still running.
     *
     * <p>The coordinator event loop may receive a new rebalance request before the persisted task
     * has been loaded and its recovery event has updated this manager. Without this fence, the new
     * request would observe no in-memory rebalance, overwrite the task in ZooKeeper, and then be
     * replaced locally when the delayed recovery event runs. Keep the flag set until ZooKeeper is
     * confirmed empty or the recovered task has been fully applied to the in-memory state.
     *
     * <p>Recovery with a persisted task completes on the coordinator event thread. Empty recovery
     * completes on the I/O thread, hence volatile.
     */
    private volatile boolean recoveryPending = true;

    private volatile @Nullable ScheduledFuture<?> recoveryRetryTask;
    private volatile boolean recoveryStopped;

    private @Nullable ScheduledFuture<?> timeoutCheckTask;
    private long registerTime;
    private @Nullable RebalanceStatus rebalanceStatus;
    private @Nullable String currentRebalanceId;
    private @Nullable String pendingPersistenceRebalanceId;

    /** Pending tasks loaded from ZooKeeper may have started before the coordinator restarted. */
    private boolean recoveredRebalance;

    private boolean cancelRequested;
    private boolean finalizationPending;
    private boolean persistenceInProgress;
    private boolean isClosed = false;

    /** Creates a manager with validated coordinator rebalance settings. */
    public RebalanceManager(
            RebalanceExecutor rebalanceExecutor,
            ZooKeeperClient zkClient,
            EventManager eventManager,
            Clock clock,
            Configuration conf,
            Scheduler scheduler,
            Executor ioExecutor) {
        this.targetUnavailableTimeoutMs =
                getTimeoutMillis(
                        conf, ConfigOptions.COORDINATOR_REBALANCE_TARGET_UNAVAILABLE_TIMEOUT);
        this.noProgressTimeoutMs =
                getTimeoutMillis(conf, ConfigOptions.COORDINATOR_REBALANCE_NO_PROGRESS_TIMEOUT);
        this.maxTrackedTimedOutTasks =
                conf.get(ConfigOptions.COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS);
        checkArgument(
                maxTrackedTimedOutTasks > 0,
                "%s must be at least 1, but was %s.",
                ConfigOptions.COORDINATOR_REBALANCE_MAX_TRACKED_TIMED_OUT_TASKS.key(),
                maxTrackedTimedOutTasks);
        this.rebalanceExecutor = rebalanceExecutor;
        this.rebalanceTaskStore =
                new RebalanceTaskStore(zkClient, zkClient.getDefaultRemoteDataDir());
        this.eventManager = eventManager;
        this.ioExecutor = ioExecutor;
        this.clock = clock == null ? SystemClock.getInstance() : clock;
        this.scheduler = scheduler;
        this.goalOptimizer = new GoalOptimizer();
        validate(conf);
        this.maxInflightRebalanceTasks =
                conf.get(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS);
    }

    public void startup() {
        LOG.info("Start up rebalance manager.");
        timeoutCheckTask =
                scheduler.schedule(
                        "rebalance-timeout-check",
                        this::enqueueTimeoutCheckSafely,
                        TIMEOUT_CHECK_INTERVAL_MS,
                        TIMEOUT_CHECK_INTERVAL_MS);
        LOG.info(
                "RebalanceManager timeout checker started: timeoutMs={}, checkIntervalMs={}",
                REBALANCE_TASK_TIMEOUT_MS,
                TIMEOUT_CHECK_INTERVAL_MS);
    }

    public @Nullable String getRebalanceId() {
        return pendingPersistenceRebalanceId == null
                ? currentRebalanceId
                : pendingPersistenceRebalanceId;
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        int newMaxInflightRebalanceTasks =
                newConfig.get(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS);
        if (newMaxInflightRebalanceTasks < 1) {
            throw new ConfigException(
                    String.format(
                            "Invalid %s: must be at least 1, but was %s",
                            ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS.key(),
                            newMaxInflightRebalanceTasks));
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) {
        int newMaxInflightRebalanceTasks =
                newConfig.get(ConfigOptions.COORDINATOR_REBALANCE_MAX_INFLIGHT_TASKS);
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
    }

    /** Starts loading the persisted task without blocking coordinator startup. */
    public void startRecovery() {
        if (recoveryStopped) {
            return;
        }
        clearRecoveryRetryTask();

        try {
            ioExecutor.execute(this::loadPersistedTask);
        } catch (Throwable t) {
            scheduleRecoveryRetry("load the persisted rebalance task", this::startRecovery, t);
        }
    }

    /** Stops asynchronous recovery before the coordinator event loop is shut down. */
    public void stopRecovery() {
        recoveryStopped = true;
        clearRecoveryRetryTask();
    }

    Optional<RebalanceTask> loadPersistedRebalanceTask() throws Exception {
        return rebalanceTaskStore.load();
    }

    /** Persists a task off the event thread and completes on the event thread. */
    public void persistRebalanceTask(RebalanceTask task, Consumer<Throwable> completion) {
        checkNotClosed();
        checkState(!persistenceInProgress, "Another rebalance persistence is already in progress.");
        CoordinatorContext context = rebalanceExecutor.getCoordinatorContext();
        int coordinatorZkVersion = context.getCoordinatorZkVersion();
        boolean storeRemotely = allLiveCoordinatorsSupportRemoteTasks(context);
        persistenceInProgress = true;
        pendingPersistenceRebalanceId = task.getRebalanceId();
        try {
            ioExecutor.execute(
                    () -> {
                        Throwable failure = null;
                        try {
                            rebalanceTaskStore.persist(task, coordinatorZkVersion, storeRemotely);
                        } catch (Throwable t) {
                            failure = t;
                        }
                        Throwable persistenceFailure = failure;
                        try {
                            eventManager.put(
                                    new RebalancePersistenceCompleteEvent(
                                            () -> {
                                                persistenceInProgress = false;
                                                pendingPersistenceRebalanceId = null;
                                                completion.accept(persistenceFailure);
                                            }));
                        } catch (Throwable t) {
                            LOG.warn(
                                    "Failed to enqueue persistence completion for rebalance {}.",
                                    task.getRebalanceId(),
                                    t);
                        }
                    });
        } catch (Throwable t) {
            persistenceInProgress = false;
            pendingPersistenceRebalanceId = null;
            completion.accept(t);
        }
    }

    private boolean allLiveCoordinatorsSupportRemoteTasks(CoordinatorContext context) {
        List<CoordinatorAddress> standbys = context.getCoordinatorStandbys();
        if (context.getLiveCoordinatorServers().size() != standbys.size() + 1) {
            return false;
        }
        for (CoordinatorAddress standby : standbys) {
            if (!standby.supportsFeature(REMOTE_REBALANCE_TASK_FEATURE)) {
                return false;
            }
        }
        return true;
    }

    private void loadPersistedTask() {
        try {
            Optional<RebalanceTask> rebalanceTask = loadPersistedRebalanceTask();
            if (!recoveryStopped) {
                if (rebalanceTask.isPresent()) {
                    eventManager.put(new RecoverRebalanceEvent(rebalanceTask.get()));
                } else {
                    completeEmptyRecovery();
                }
            }
        } catch (Throwable t) {
            scheduleRecoveryRetry("load the persisted rebalance task", this::startRecovery, t);
        }
        // Do not delay recovery completion on the best-effort directory scan.
        rebalanceTaskStore.cleanupOrphanFiles();
    }

    private void scheduleRecoveryRetry(String action, Runnable retryAction, Throwable failure) {
        if (recoveryStopped) {
            return;
        }
        long retryDelayMs = nextRecoveryRetryDelayMs();
        LOG.warn("Failed to {}. Retrying after {} ms.", action, retryDelayMs, failure);
        try {
            recoveryRetryTask =
                    scheduler.scheduleOnce("rebalance-recovery-retry", retryAction, retryDelayMs);
        } catch (Throwable schedulingFailure) {
            LOG.warn("Failed to schedule rebalance recovery retry.", schedulingFailure);
        }
    }

    private long nextRecoveryRetryDelayMs() {
        int failureCount = recoveryFailures.getAndIncrement();
        int shift = Math.min(failureCount, 16);
        return Math.min(INITIAL_RECOVERY_RETRY_DELAY_MS << shift, MAX_RECOVERY_RETRY_DELAY_MS);
    }

    private void clearRecoveryRetryTask() {
        ScheduledFuture<?> retryTask = recoveryRetryTask;
        recoveryRetryTask = null;
        if (retryTask != null) {
            retryTask.cancel(false);
        }
    }

    /** Marks startup recovery complete when ZooKeeper contains no persisted rebalance task. */
    public void completeEmptyRecovery() {
        recoveryFailures.set(0);
        recoveryPending = false;
    }

    /** Returns whether startup recovery has not yet completed. */
    public boolean isRecoveryPending() {
        return recoveryPending;
    }

    /** Registers a new rebalance; persisted tasks are handled by {@link #recoverRebalance}. */
    public void registerRebalance(
            String rebalanceId, Map<TableBucket, RebalancePlanForBucket> rebalancePlan) {
        checkNotClosed();
        checkState(
                !recoveryPending,
                "Cannot register a rebalance before startup recovery has completed.");
        resetRebalance(rebalanceId, false);
        if (rebalancePlan.isEmpty()) {
            finalizeRebalance();
            return;
        }

        for (Map.Entry<TableBucket, RebalancePlanForBucket> entry : rebalancePlan.entrySet()) {
            addPendingTask(entry.getKey(), entry.getValue());
        }

        rebalanceStatus = REBALANCING;
        processNewRebalanceTasks();
    }

    /** Recovers a persisted task by comparing every bucket plan with current coordinator state. */
    public boolean recoverRebalance(RebalanceTask rebalanceTask) {
        try {
            recoverRebalanceInternal(rebalanceTask);
            recoveryFailures.set(0);
            clearRecoveryRetryTask();
            recoveryPending = false;
            return true;
        } catch (Throwable t) {
            clearFailedRecoveryState();
            scheduleRecoveryRetry(
                    "apply persisted rebalance task " + rebalanceTask.getRebalanceId(),
                    () -> eventManager.put(new RecoverRebalanceEvent(rebalanceTask)),
                    t);
            return false;
        }
    }

    private void clearFailedRecoveryState() {
        currentRebalanceId = null;
        rebalanceStatus = null;
        recoveredRebalance = false;
        cancelRequested = false;
        finalizationPending = false;
        inProgressRebalanceTasks.clear();
        pendingRebalanceTasks.clear();
        pendingRecoveredTasks.clear();
        runningRebalanceTasks.clear();
        timedOutRebalanceTasks.clear();
        finishedRebalanceTasks.clear();
        queuedReconcileEvents.clear();
        hasActiveRebalance = false;
    }

    private void recoverRebalanceInternal(RebalanceTask rebalanceTask) {
        checkNotClosed();
        resetRebalance(rebalanceTask.getRebalanceId(), rebalanceTask.isCancelRequested());
        recoveredRebalance = true;
        if (FINAL_STATUSES.contains(rebalanceTask.getRebalanceStatus())) {
            for (Map.Entry<TableBucket, RebalancePlanForBucket> entry :
                    rebalanceTask.getExecutePlan().entrySet()) {
                finishedRebalanceTasks.put(
                        entry.getKey(),
                        RebalanceResultForBucket.of(
                                entry.getValue(), rebalanceTask.getRebalanceStatus()));
            }
            rebalanceStatus = rebalanceTask.getRebalanceStatus();
            hasActiveRebalance = false;
            return;
        }

        for (Map.Entry<TableBucket, RebalancePlanForBucket> entry :
                rebalanceTask.getExecutePlan().entrySet()) {
            RebalancePlanForBucket plan = entry.getValue();
            boolean atOrigin = rebalanceExecutor.isRebalanceTaskAtOrigin(plan);
            boolean explicitlyCanceled =
                    rebalanceTask.getCanceledBuckets().contains(entry.getKey());
            boolean replicaMigration =
                    !new HashSet<>(plan.getOriginReplicas())
                            .equals(new HashSet<>(plan.getNewReplicas()));
            if (explicitlyCanceled && (!replicaMigration || atOrigin)) {
                finishedRebalanceTasks.put(
                        entry.getKey(), RebalanceResultForBucket.of(plan, CANCELED));
            } else if (atOrigin) {
                addPendingTask(entry.getKey(), plan);
            } else {
                // A replica migration recorded as canceled may still have started after an
                // ambiguous cancellation write. Keep any intermediate assignment tracked.
                addRecoveredIntermediateTask(entry.getKey(), plan);
            }
        }
        if (cancelRequested) {
            cancelPendingTasks(findTasksToCancel());
        }

        if (inProgressRebalanceTasks.isEmpty()) {
            finalizeRebalance();
        } else {
            rebalanceStatus = REBALANCING;
            processNewRebalanceTasks();
        }
    }

    public void finishRebalanceTask(TableBucket tableBucket, RebalanceStatus statusForBucket) {
        RebalanceExecutionKey executionKey = getExecutionKey(tableBucket);
        if (executionKey != null) {
            finishRebalanceTask(executionKey, statusForBucket);
        }
    }

    public boolean finishRebalanceTask(
            RebalanceExecutionKey executionKey, RebalanceStatus statusForBucket) {
        boolean completed = completeRebalanceTaskState(executionKey, statusForBucket);
        if (completed && !inProgressRebalanceTasks.isEmpty()) {
            processNewRebalanceTasks();
        }
        return completed;
    }

    private boolean completeRebalanceTaskState(
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
        }
        return true;
    }

    public @Nullable RebalanceProgress listRebalanceProgress(@Nullable String rebalanceId) {
        checkNotClosed();
        if (isNewRebalancePersistenceInProgress()) {
            throw new RebalanceFailureException(
                    "Rebalance persistence is still in progress. Please retry later.");
        }
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

    public void cancelRebalance(@Nullable String rebalanceId, Consumer<Throwable> completion) {
        checkNotClosed();

        if (isNewRebalancePersistenceInProgress()) {
            completion.accept(
                    new RebalanceFailureException(
                            "Rebalance persistence is still in progress. Please retry "
                                    + "cancellation later."));
            return;
        }

        if (currentRebalanceId == null) {
            completion.accept(null);
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
            completion.accept(null);
            return;
        }

        if (cancelRequested || finalizationPending) {
            completion.accept(null);
            return;
        }
        if (persistenceInProgress) {
            completion.accept(
                    new RebalanceFailureException(
                            "Another rebalance state update is still being persisted. Please "
                                    + "retry cancellation later."));
            return;
        }

        Set<TableBucket> tasksToCancel = findTasksToCancel();
        Set<TableBucket> canceledBuckets = canceledBuckets();
        canceledBuckets.addAll(tasksToCancel);
        RebalanceTask cancellationTask =
                new RebalanceTask(
                        currentRebalanceId,
                        REBALANCING,
                        allRebalancePlans(),
                        true,
                        canceledBuckets);
        persistRebalanceTask(
                cancellationTask,
                failure -> {
                    if (failure != null) {
                        completion.accept(
                                new RebalanceFailureException(
                                        "Failed to persist rebalance cancellation request.",
                                        failure));
                        return;
                    }

                    try {
                        cancelRequested = true;
                        cancelPendingTasks(tasksToCancel);

                        // An admitted task still at its origin has no intermediate assignment to
                        // drain, so cancellation can finish it immediately.
                        for (RebalanceTaskAttempt attempt : activeAttempts()) {
                            TableBucket bucket = attempt.executionKey.getTableBucket();
                            RebalanceResultForBucket result = inProgressRebalanceTasks.get(bucket);
                            if (tasksToCancel.contains(bucket)
                                    && result != null
                                    && rebalanceExecutor.isRebalanceTaskAtOrigin(result.plan())) {
                                finishRebalanceTask(attempt.executionKey, CANCELED);
                            }
                        }

                        if (inProgressRebalanceTasks.isEmpty()
                                && !FINAL_STATUSES.contains(rebalanceStatus)) {
                            finalizeRebalance();
                        }
                        LOG.info(
                                "Accepted cancellation for rebalance {}. Running and timed-out "
                                        + "tasks will be drained before the rebalance becomes "
                                        + "canceled.",
                                currentRebalanceId);
                        completion.accept(null);
                    } catch (Throwable t) {
                        completion.accept(t);
                    }
                });
    }

    private boolean isNewRebalancePersistenceInProgress() {
        return persistenceInProgress
                && !Objects.equals(pendingPersistenceRebalanceId, currentRebalanceId);
    }

    private Set<TableBucket> findTasksToCancel() {
        Set<TableBucket> tasksToCancel = new HashSet<>();
        for (TableBucket bucket : pendingRebalanceTasks) {
            RebalanceResultForBucket result = inProgressRebalanceTasks.get(bucket);
            if (result != null
                    && (!recoveredRebalance
                            || rebalanceExecutor.isRebalanceTaskAtOrigin(result.plan()))) {
                tasksToCancel.add(bucket);
            }
        }
        for (RebalanceTaskAttempt attempt : activeAttempts()) {
            TableBucket bucket = attempt.executionKey.getTableBucket();
            RebalanceResultForBucket result = inProgressRebalanceTasks.get(bucket);
            if (result != null && rebalanceExecutor.isRebalanceTaskAtOrigin(result.plan())) {
                tasksToCancel.add(bucket);
            }
        }
        return tasksToCancel;
    }

    private void cancelPendingTasks(Set<TableBucket> tasksToCancel) {
        Iterator<TableBucket> pending = pendingRebalanceTasks.iterator();
        while (pending.hasNext()) {
            TableBucket bucket = pending.next();
            if (tasksToCancel.contains(bucket)) {
                RebalanceResultForBucket result = inProgressRebalanceTasks.get(bucket);
                pending.remove();
                inProgressRebalanceTasks.remove(bucket);
                finishedRebalanceTasks.put(
                        bucket, RebalanceResultForBucket.of(result.plan(), CANCELED));
            }
        }
    }

    /**
     * Returns whether a rebalance is in progress. This also fences new rebalance requests until
     * startup recovery has completed.
     */
    public boolean hasInProgressRebalance() {
        checkNotClosed();
        return recoveryPending
                || persistenceInProgress
                || finalizationPending
                || !inProgressRebalanceTasks.isEmpty();
    }

    public RebalanceTask generateRebalanceTask(List<Goal> goalsByPriority) {
        checkNotClosed();
        List<RebalancePlanForBucket> rebalancePlanForBuckets;
        String rebalanceId = UUID.randomUUID().toString();
        try {
            // Generate the latest cluster model.
            long startTime = System.currentTimeMillis();
            CoordinatorContext context = rebalanceExecutor.getCoordinatorContext();
            ClusterModel clusterModel = buildClusterModel(context);
            Map<TableBucket, List<Integer>> actualAssignments = new HashMap<>();
            Map<TableBucket, Integer> actualLeaders = new HashMap<>();
            for (TableBucket bucket : clusterModel.getReplicaDistribution().keySet()) {
                actualAssignments.put(bucket, new ArrayList<>(context.getAssignment(bucket)));
                actualLeaders.put(bucket, context.getBucketLeaderAndIsr(bucket).get().leader());
            }
            LOG.info(
                    "Build cluster model for rebalance id {} with {} ms.",
                    rebalanceId,
                    System.currentTimeMillis() - startTime);

            // do optimize.
            startTime = System.currentTimeMillis();
            goalOptimizer.doOptimizeOnce(clusterModel, goalsByPriority);
            // The optimization model excludes excess replicas left by a failed migration. Diff
            // against the real assignment so that cleanup is still executed and persisted, even
            // when the requested goals make no further changes to the model.
            rebalancePlanForBuckets = getDiff(actualAssignments, actualLeaders, clusterModel);
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

    /**
     * Returns the active execution key. This method must be called on the coordinator event thread.
     */
    public @Nullable RebalanceExecutionKey getExecutionKey(TableBucket tableBucket) {
        RebalanceTaskAttempt attempt = runningRebalanceTasks.get(tableBucket);
        if (attempt == null) {
            attempt = timedOutRebalanceTasks.get(tableBucket);
        }
        return attempt == null ? null : attempt.executionKey;
    }

    public boolean timeoutRebalanceTask(RebalanceExecutionKey executionKey) {
        checkNotClosed();
        RebalanceTaskAttempt attempt = runningRebalanceTasks.get(executionKey.getTableBucket());
        if (attempt == null || !attempt.executionKey.equals(executionKey)) {
            LOG.debug("Ignore stale timeout for {}.", executionKey);
            return false;
        }
        if (timedOutRebalanceTasks.size() >= maxTrackedTimedOutTasks) {
            // Keep the attempt in the normal running set until a reconciliation slot becomes
            // available. The next periodic check will try to time it out again.
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
        attempt.onTimedOut(
                clock.milliseconds(), observeReconciliationState(tableBucket, result.plan()));
        enqueueReconciliation(attempt);
        processNewRebalanceTasks();
        return true;
    }

    /**
     * Prepares the given attempt for reconciliation and returns its plan, or null if the attempt is
     * stale or has just been completed or given up on.
     *
     * <p>Reconciliation has to terminate. Otherwise a bucket that can never converge, for example
     * because a target server is gone for good, keeps the overall rebalance in a non-final status
     * and every later rebalance request is rejected forever.
     */
    public @Nullable RebalancePlanForBucket prepareReconciliation(
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
        if (rebalanceExecutor.isRebalanceTaskDeleted(tableBucket)) {
            finishRebalanceTask(executionKey, COMPLETED);
            return null;
        }

        // Called on the coordinator event loop, so reading the coordinator state is safe here.
        long now = clock.milliseconds();
        String observedState = observeReconciliationState(tableBucket, result.plan());
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

        if (attempt.blockedForMs(now) > targetUnavailableTimeoutMs
                || now - attempt.lastProgressMs > noProgressTimeoutMs) {
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

    private void processNewRebalanceTasks() {
        TableBucket tableBucket;
        while (timedOutRebalanceTasks.size() < maxTrackedTimedOutTasks
                && (tableBucket = pendingRecoveredTasks.poll()) != null) {
            RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
            if (resultForBucket == null || resultForBucket.status() != TIMEOUT) {
                continue;
            }
            long now = clock.milliseconds();
            RebalanceTaskAttempt attempt =
                    new RebalanceTaskAttempt(
                            new RebalanceExecutionKey(currentRebalanceId, tableBucket), now);
            attempt.onTimedOut(
                    now, observeReconciliationState(tableBucket, resultForBucket.plan()));
            timedOutRebalanceTasks.put(tableBucket, attempt);
            enqueueReconciliation(attempt);
        }
        if (!pendingRecoveredTasks.isEmpty()) {
            return;
        }
        if (timedOutRebalanceTasks.size() >= maxTrackedTimedOutTasks) {
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
        while (runningRebalanceTasks.size() < maxInflightRebalanceTasks
                && (tableBucket = pendingRebalanceTasks.poll()) != null) {
            RebalanceResultForBucket resultForBucket = inProgressRebalanceTasks.get(tableBucket);
            if (resultForBucket == null || resultForBucket.status() != NOT_STARTED) {
                continue;
            }
            RebalanceExecutionKey executionKey =
                    new RebalanceExecutionKey(currentRebalanceId, tableBucket);
            runningRebalanceTasks.put(
                    tableBucket, new RebalanceTaskAttempt(executionKey, clock.milliseconds()));
            inProgressRebalanceTasks.put(
                    tableBucket, RebalanceResultForBucket.of(resultForBucket.plan(), REBALANCING));
            ExecutionResult executionResult =
                    rebalanceExecutor.tryToExecuteRebalanceTask(resultForBucket.plan());
            if (executionResult == ExecutionResult.COMPLETED) {
                completeRebalanceTaskState(executionKey, COMPLETED);
            }
        }
    }

    /** Returns the current rebalance concurrency limit. */
    @VisibleForTesting
    public int getMaxInflightRebalanceTasks() {
        return maxInflightRebalanceTasks;
    }

    private void finalizeRebalance() {
        finalizationPending = true;
        persistFinalStatus();
    }

    private void persistFinalStatus() {
        checkNotClosed();
        if (persistenceInProgress) {
            return;
        }
        RebalanceStatus finalStatus = aggregateFinalStatus();
        persistRebalanceTask(
                new RebalanceTask(
                        currentRebalanceId,
                        finalStatus,
                        allRebalancePlans(),
                        cancelRequested,
                        canceledBuckets()),
                failure -> {
                    if (failure != null) {
                        rebalanceStatus = REBALANCING;
                        finalizationPending = true;
                        LOG.error(
                                "Failed to persist final state for rebalance {}. It will be "
                                        + "retried.",
                                currentRebalanceId,
                                failure);
                        return;
                    }

                    rebalanceStatus = finalStatus;
                    finalizationPending = false;
                    inProgressRebalanceTasks.clear();
                    pendingRebalanceTasks.clear();
                    pendingRecoveredTasks.clear();
                    runningRebalanceTasks.clear();
                    timedOutRebalanceTasks.clear();
                    queuedReconcileEvents.clear();
                    hasActiveRebalance = false;

                    LOG.info(
                            "Rebalance {} reached final status {} in {} ms.",
                            currentRebalanceId,
                            finalStatus,
                            System.currentTimeMillis() - registerTime);
                });
    }

    private void resetRebalance(String rebalanceId, boolean cancelRequested) {
        registerTime = System.currentTimeMillis();
        currentRebalanceId = rebalanceId;
        recoveredRebalance = false;
        this.cancelRequested = cancelRequested;
        finalizationPending = false;
        inProgressRebalanceTasks.clear();
        pendingRebalanceTasks.clear();
        pendingRecoveredTasks.clear();
        runningRebalanceTasks.clear();
        timedOutRebalanceTasks.clear();
        finishedRebalanceTasks.clear();
        queuedReconcileEvents.clear();
        hasActiveRebalance = true;
    }

    private void addPendingTask(TableBucket tableBucket, RebalancePlanForBucket plan) {
        pendingRebalanceTasks.add(tableBucket);
        inProgressRebalanceTasks.put(tableBucket, RebalanceResultForBucket.of(plan, NOT_STARTED));
    }

    private void addRecoveredIntermediateTask(
            TableBucket tableBucket, RebalancePlanForBucket plan) {
        pendingRecoveredTasks.add(tableBucket);
        inProgressRebalanceTasks.put(tableBucket, RebalanceResultForBucket.of(plan, TIMEOUT));
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
     * Returns the target-relevant state of a bucket, used to detect whether a timed-out task is
     * still making progress.
     *
     * <p>The leader and bucket epochs are deliberately left out: a reconciliation re-sends the
     * current state and can bump them without the migration moving forward at all. Assignment,
     * leader, and ISR changes involving only replicas outside the target are also excluded because
     * they do not move the migration toward its target.
     */
    private String observeReconciliationState(
            TableBucket tableBucket, RebalancePlanForBucket plan) {
        CoordinatorContext coordinatorContext = rebalanceExecutor.getCoordinatorContext();
        Set<Integer> targetReplicas = new HashSet<>(plan.getNewReplicas());
        List<Integer> assignment = coordinatorContext.getAssignment(tableBucket);
        Set<Integer> assignedTargetReplicas = new TreeSet<>(assignment);
        assignedTargetReplicas.retainAll(targetReplicas);
        StringBuilder observed =
                new StringBuilder("targetAssignment=")
                        .append(assignment.equals(plan.getNewReplicas()))
                        .append("|assignedTargets=")
                        .append(assignedTargetReplicas);
        coordinatorContext
                .getBucketLeaderAndIsr(tableBucket)
                .ifPresent(
                        leaderAndIsr -> {
                            Set<Integer> targetIsr = new TreeSet<>(leaderAndIsr.isr());
                            targetIsr.retainAll(targetReplicas);
                            observed.append("|targetLeader=")
                                    .append(leaderAndIsr.leader() == plan.getNewLeader())
                                    .append("|targetIsr=")
                                    .append(targetIsr);
                        });
        return observed.toString();
    }

    private static long getTimeoutMillis(Configuration conf, ConfigOption<Duration> option) {
        Duration timeout = conf.get(option);
        checkArgument(
                timeout.compareTo(Duration.ofMillis(1)) >= 0
                        && timeout.compareTo(Duration.ofMillis(Long.MAX_VALUE)) <= 0,
                "%s must be between 1 ms and %s ms, but was %s.",
                option.key(),
                Long.MAX_VALUE,
                timeout);
        return timeout.toMillis();
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

    private Set<TableBucket> canceledBuckets() {
        Set<TableBucket> canceledBuckets = new HashSet<>();
        for (Map.Entry<TableBucket, RebalanceResultForBucket> entry :
                finishedRebalanceTasks.entrySet()) {
            if (entry.getValue().status() == CANCELED) {
                canceledBuckets.add(entry.getKey());
            }
        }
        return canceledBuckets;
    }

    private RebalanceStatus aggregateFinalStatus() {
        for (RebalanceResultForBucket result : finishedRebalanceTasks.values()) {
            if (result.status() == FAILED) {
                return FAILED;
            }
        }
        return cancelRequested ? CANCELED : COMPLETED;
    }

    private ClusterModel buildClusterModel(CoordinatorContext coordinatorContext) {
        Map<Integer, ServerInfo> liveTabletServers = coordinatorContext.getLiveTabletServers();
        Set<Integer> liveTabletServerIds = coordinatorContext.liveTabletServerSet();
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

        // Failed migrations can leave an unavailable target in the persisted assignment. Keep
        // such servers in the model as ineligible sources so the goals can evacuate their replicas.
        Set<TableBucket> allBuckets = coordinatorContext.getAllBuckets();
        for (TableBucket tableBucket : allBuckets) {
            for (Integer replica : coordinatorContext.getAssignment(tableBucket)) {
                serverModelMap.computeIfAbsent(
                        replica, id -> new ServerModel(id, RackModel.DEFAULT_RACK, true));
            }
        }
        ClusterModel clusterModel = initialClusterModel(serverModelMap);

        // Try to update the cluster model with the latest bucket states.
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
            assignment =
                    assignmentForOptimization(
                            coordinatorContext, liveTabletServerIds, tableBucket, assignment, isr);
            for (int i = 0; i < assignment.size(); i++) {
                int replica = assignment.get(i);
                clusterModel.createReplica(replica, tableBucket, i, leader == replica);
            }
        }
        return clusterModel;
    }

    private List<Integer> assignmentForOptimization(
            CoordinatorContext context,
            Set<Integer> liveTabletServerIds,
            TableBucket bucket,
            List<Integer> assignment,
            LeaderAndIsr leaderAndIsr) {
        TableInfo tableInfo = context.getTableInfoById(bucket.getTableId());
        int replicationFactor =
                tableInfo == null
                        ? assignment.size()
                        : tableInfo
                                .getProperties()
                                .getOptional(ConfigOptions.TABLE_REPLICATION_FACTOR)
                                .orElse(assignment.size());
        if (assignment.size() <= replicationFactor) {
            return assignment;
        }

        // A Phase A union is not a new replication factor. Prefer the current leader and live,
        // caught-up replicas when selecting the model's starting assignment. The requested goals
        // can then relocate these replicas; execution retains the full real origin for cleanup.
        List<Integer> replicas = new ArrayList<>(assignment);
        replicas.sort(
                Comparator.comparing((Integer replica) -> replica != leaderAndIsr.leader())
                        .thenComparing(replica -> !liveTabletServerIds.contains(replica))
                        .thenComparing(replica -> !leaderAndIsr.isr().contains(replica))
                        .thenComparing(
                                replica ->
                                        context.getServerTag(replica)
                                                .map(this::isOfflineTagged)
                                                .orElse(false)));
        return new ArrayList<>(replicas.subList(0, replicationFactor));
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

    private void enqueueTimeoutCheckSafely() {
        if (!hasActiveRebalance) {
            return;
        }
        try {
            eventManager.put(new CheckRebalanceTimeoutEvent());
        } catch (Throwable t) {
            LOG.error("Failed to enqueue rebalance timeout check.", t);
        }
    }

    /** Checks timeout and reconciliation state on the coordinator event thread. */
    public void checkTimeout() {
        if (!hasActiveRebalance) {
            return;
        }

        long now = clock.milliseconds();
        List<RebalanceExecutionKey> timedOutAttempts = new ArrayList<>();
        for (RebalanceTaskAttempt attempt : runningRebalanceTasks.values()) {
            long elapsed = now - attempt.startMs;
            if (elapsed > REBALANCE_TASK_TIMEOUT_MS) {
                LOG.warn(
                        "In-flight rebalance task {} timed out after {}ms. It will continue to be "
                                + "tracked while the next pending task is admitted.",
                        attempt.executionKey,
                        elapsed);
                timedOutAttempts.add(attempt.executionKey);
            }
        }
        for (RebalanceExecutionKey executionKey : timedOutAttempts) {
            timeoutRebalanceTask(executionKey);
        }

        // Reconcile timed-out tasks on a growing backoff, so that a long cluster operation such as
        // a rolling upgrade does not turn into a constant retry storm on the event loop.
        for (RebalanceTaskAttempt attempt : timedOutRebalanceTasks.values()) {
            if (now >= attempt.nextReconcileMs) {
                enqueueReconciliation(attempt);
            }
        }

        if (finalizationPending) {
            persistFinalStatus();
        }
    }

    private void checkNotClosed() {
        checkState(!isClosed, "RebalanceManager is already closed.");
    }

    /** Stops the periodic producer before the coordinator event loop is shut down. */
    public void stopTimeoutChecker() {
        hasActiveRebalance = false;
        if (timeoutCheckTask != null) {
            timeoutCheckTask.cancel(false);
            timeoutCheckTask = null;
        }
    }

    public void close() {
        stopRecovery();
        stopTimeoutChecker();
        isClosed = true;
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

        private long nextReconcileMs;

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
