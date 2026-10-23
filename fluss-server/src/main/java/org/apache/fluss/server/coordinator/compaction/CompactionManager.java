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

package org.apache.fluss.server.coordinator.compaction;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.compaction.CompactionInfo;
import org.apache.fluss.cluster.compaction.CompactionProgress;
import org.apache.fluss.cluster.compaction.CompactionStatus;
import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.CompactionInProgressException;
import org.apache.fluss.exception.CompactionNotExistException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.CancelBucketCompactionRequest;
import org.apache.fluss.rpc.messages.CompactRequest;
import org.apache.fluss.rpc.messages.ListCompactionsRequest;
import org.apache.fluss.rpc.messages.PbBucketCompactionStatus;
import org.apache.fluss.rpc.messages.PbTableBucket;
import org.apache.fluss.rpc.messages.StartBucketCompactionRequest;
import org.apache.fluss.rpc.messages.StartBucketCompactionResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Serializes admission and compaction scheduling on the coordinator event thread. Remote storage
 * runs on the supplied I/O executor; the event executor must post to the existing coordinator event
 * thread.
 */
public final class CompactionManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionManager.class);

    private static final long TASK_TIMEOUT_MS = Duration.ofHours(24).toMillis();
    private static final long CONTROL_PERIOD_MS = Duration.ofSeconds(1).toMillis();
    private static final long RETENTION_MS = Duration.ofDays(7).toMillis();
    private static final long CLEANUP_PERIOD_MS = Duration.ofMinutes(1).toMillis();
    private static final int ERROR_LIMIT = 4 * 1024;

    private final CoordinatorContext context;
    private final CompactionStore store;
    private final ZooKeeperClient zk;
    private final RpcClient rpc;
    private final String listener;
    private final Supplier<Configuration> configuration;
    private final Executor events;
    private final Executor io;
    private final Clock clock;
    private final long taskTimeout;
    private final ScheduledFuture<?> control;
    private final ScheduledFuture<?> cleanup;
    private final long retention;
    private boolean initialized;
    private boolean recovering;
    private boolean cleaning;

    @Nullable private Operation active;
    @Nullable private CompactionTask last;
    @Nullable private Throwable storageFailure;
    private volatile boolean closed;

    /**
     * Creates a manager using the production deadline and control period. Executors are borrowed.
     * Construct only after this coordinator has acquired its active epoch and loaded its context.
     * Startup reconstructs the published active task before admission; standby coordinators must
     * not construct a manager. Close it before stopping the event executor on leadership loss.
     */
    public CompactionManager(
            CoordinatorContext context,
            CompactionStore store,
            ZooKeeperClient zk,
            RpcClient rpc,
            String listener,
            Supplier<Configuration> configuration,
            Executor events,
            Executor io,
            Scheduler timer) {
        this(
                context,
                store,
                zk,
                rpc,
                listener,
                configuration,
                events,
                io,
                timer,
                SystemClock.getInstance(),
                TASK_TIMEOUT_MS,
                CONTROL_PERIOD_MS);
    }

    CompactionManager(
            CoordinatorContext context,
            CompactionStore store,
            ZooKeeperClient zk,
            RpcClient rpc,
            String listener,
            Supplier<Configuration> configuration,
            Executor events,
            Executor io,
            Scheduler timer,
            Clock clock,
            long taskTimeout,
            long period) {
        this(
                context,
                store,
                zk,
                rpc,
                listener,
                configuration,
                events,
                io,
                timer,
                clock,
                taskTimeout,
                period,
                RETENTION_MS,
                CLEANUP_PERIOD_MS);
    }

    CompactionManager(
            CoordinatorContext context,
            CompactionStore store,
            ZooKeeperClient zk,
            RpcClient rpc,
            String listener,
            Supplier<Configuration> configuration,
            Executor events,
            Executor io,
            Scheduler timer,
            Clock clock,
            long taskTimeout,
            long period,
            long retention,
            long cleanupPeriod) {
        checkArgument(taskTimeout > 0 && period > 0, "Invalid compaction timings");
        this.context = context;
        this.store = store;
        this.zk = zk;
        this.rpc = rpc;
        this.listener = listener;
        this.configuration = configuration;
        this.events = events;
        this.io = io;
        this.clock = clock;
        this.taskTimeout = taskTimeout;
        checkArgument(retention > 0 && cleanupPeriod > 0, "Invalid compaction retention");
        this.retention = retention;
        this.control = timer.schedule("compaction-control", this::controlCycle, period, period);
        this.cleanup =
                timer.schedule(
                        "compaction-cleanup", this::cleanupCycle, cleanupPeriod, cleanupPeriod);
        events.execute(this::recover);
    }

    /** Resolves and fixes the target set, or returns the exactly matching active submission. */
    public CompletableFuture<String> compact(CompactRequest request) {
        CompactRequest copy = new CompactRequest().copyFrom(request);
        CompletableFuture<String> result = new CompletableFuture<>();
        execute(
                result,
                () -> {
                    checkStorage();
                    checkState(initialized && !recovering, "Compaction recovery in progress");
                    Scope scope = resolve(copy);
                    Map<String, String> baseline = baseline();
                    if (active != null) {
                        if (active.scope.equals(scope)
                                && active.plan.configuration().equals(baseline)) {
                            active.admission.whenComplete(
                                    (id, error) -> complete(result, id, error));
                        } else {
                            result.completeExceptionally(busy(active.task.info().operationId()));
                        }
                        return;
                    }
                    long now = clock.milliseconds();
                    CompactionInfo info =
                            new CompactionInfo(
                                    UUID.randomUUID().toString(),
                                    scope.type,
                                    CompactionStatus.PENDING,
                                    now,
                                    scope.database,
                                    scope.table,
                                    scope.partition);
                    CompactionTask task =
                            new CompactionTask(
                                    info,
                                    Math.addExact(now, taskTimeout),
                                    scope.buckets.size(),
                                    0,
                                    0,
                                    0,
                                    0,
                                    now,
                                    null,
                                    null,
                                    null,
                                    0);
                    Operation operation =
                            new Operation(
                                    task, scope, new CompactionTask.Plan(scope.buckets, baseline));
                    active = operation;
                    operation.admission.whenComplete((id, error) -> complete(result, id, error));
                    create(operation);
                });
        return result;
    }

    /** Lists retained summaries filtered by the originally submitted scope. */
    public CompletableFuture<List<CompactionInfo>> listCompactions(ListCompactionsRequest request) {
        ListCompactionsRequest copy = new ListCompactionsRequest().copyFrom(request);
        CompletableFuture<List<CompactionInfo>> result = new CompletableFuture<>();
        execute(
                result,
                () -> {
                    validateScope(
                            copy.hasDatabaseName(), copy.hasTableName(), copy.hasPartitionName());
                    read(
                            () -> {
                                List<CompactionInfo> infos = new ArrayList<>();
                                for (CompactionTask task : store.list()) {
                                    if (expired(task)) {
                                        continue;
                                    }
                                    CompactionInfo info = task.info();
                                    if ((!copy.hasDatabaseName()
                                                    || !info.databaseName().isPresent()
                                                    || info.databaseName()
                                                            .get()
                                                            .equals(copy.getDatabaseName()))
                                            && (!copy.hasTableName()
                                                    || !info.tablePath().isPresent()
                                                    || info.tablePath()
                                                            .get()
                                                            .getTableName()
                                                            .equals(copy.getTableName()))
                                            && (!copy.hasPartitionName()
                                                    || !info.partitionName().isPresent()
                                                    || info.partitionName()
                                                            .get()
                                                            .equals(copy.getPartitionName()))) {
                                        infos.add(info);
                                    }
                                }
                                infos.sort(
                                        Comparator.comparingLong(CompactionInfo::createTime)
                                                .reversed());
                                return infos;
                            },
                            (value, error) -> complete(result, value, error));
                });
        return result;
    }

    /** Returns a current observation, with success counted only after durable publication. */
    public CompletableFuture<CompactionProgress> getCompaction(String operationId) {
        CompletableFuture<CompactionProgress> result = new CompletableFuture<>();
        execute(
                result,
                () -> {
                    Operation operation = find(operationId);
                    if (operation != null) {
                        checkStorage();
                        checkState(initialized, "Compaction recovery in progress");
                        result.complete(progress(operation));
                    } else if (last != null && last.info().operationId().equals(operationId)) {
                        if (expired(last)) {
                            throw new CompactionNotExistException(
                                    "Compaction expired: " + operationId);
                        }
                        result.complete(progress(last));
                    } else {
                        read(
                                () -> load(operationId),
                                (task, error) -> {
                                    if (error == null) {
                                        if (expired(task)) {
                                            result.completeExceptionally(
                                                    new CompactionNotExistException(
                                                            "Compaction expired: " + operationId));
                                        } else if (!task.isTerminal()) {
                                            result.completeExceptionally(
                                                    new IllegalStateException(
                                                            "Compaction recovery in progress",
                                                            storageFailure));
                                        } else {
                                            result.complete(progress(task));
                                        }
                                    } else {
                                        result.completeExceptionally(error);
                                    }
                                });
                    }
                });
        return result;
    }

    /** Cancels remaining work and acknowledges only after the terminal record is published. */
    public CompletableFuture<Void> cancelCompaction(String operationId) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        execute(
                result,
                () -> {
                    Operation operation = find(operationId);
                    if (operation != null) {
                        checkStorage();
                        checkState(initialized, "Compaction recovery in progress");
                        if (operation.task.isTerminal()) {
                            result.complete(null);
                        } else {
                            operation.termination.whenComplete(
                                    (value, error) -> complete(result, value, error));
                            end(operation, CompactionStatus.CANCELLED, null);
                        }
                    } else {
                        read(
                                () -> load(operationId),
                                (task, error) -> {
                                    if (error != null) {
                                        result.completeExceptionally(error);
                                    } else if (task.isTerminal()) {
                                        result.complete(null);
                                    } else {
                                        result.completeExceptionally(
                                                new IllegalStateException(
                                                        "Compaction is not loaded: "
                                                                + operationId));
                                    }
                                });
                    }
                });
        return result;
    }

    /**
     * Stops callbacks and timers without shutting down borrowed executors or blocking the event
     * thread.
     */
    @Override
    public void close() {
        closeAsync();
    }

    /** Completes after event-thread cleanup, before the owner stops its event queue. */
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        if (control != null) {
            control.cancel(false);
        }
        if (cleanup != null) {
            cleanup.cancel(false);
        }
        events.execute(
                () -> {
                    closed = true;
                    if (active != null) {
                        cancelExecutions(active);
                        IllegalStateException error =
                                new IllegalStateException("Compaction manager closed");
                        active.admission.completeExceptionally(error);
                        active.termination.completeExceptionally(error);
                        active = null;
                    }
                    completion.complete(null);
                });
        return completion;
    }

    void controlCycle() {
        events.execute(
                () -> {
                    if (closed) {
                        return;
                    }
                    if (!initialized || storageFailure != null) {
                        recover();
                    } else if (active != null && !active.task.isTerminal()) {
                        if (clock.milliseconds() >= active.task.deadline()) {
                            end(
                                    active,
                                    CompactionStatus.FAILED,
                                    new IllegalStateException("Compaction deadline exceeded"));
                        } else if (!recovering) {
                            advance(active);
                        }
                    }
                });
    }

    void cleanupCycle() {
        events.execute(
                () -> {
                    if (closed
                            || !initialized
                            || recovering
                            || storageFailure != null
                            || cleaning) {
                        return;
                    }
                    cleaning = true;
                    long cutoff = clock.milliseconds() - retention;
                    int epoch = context.getCoordinatorZkVersion();
                    read(
                            () -> store.cleanup(cutoff, epoch),
                            (ignored, error) -> {
                                cleaning = false;
                                if (error != null) {
                                    LOG.warn(
                                            "Failed to clean compaction tasks older than {}",
                                            cutoff,
                                            error);
                                }
                                if (last != null && last.finishTime() < cutoff) {
                                    last = null;
                                }
                            });
                });
    }

    private void recover() {
        if (closed || recovering) {
            return;
        }
        recovering = true;
        initialized = false;
        int epoch = context.getCoordinatorZkVersion();
        read(
                () -> {
                    store.checkCoordinatorEpoch(epoch);
                    ZooKeeperClient.VersionedData<String> pointer = zk.getCompactionActive();
                    if (pointer.data().isEmpty()) {
                        return null;
                    }
                    CompactionTask task = load(pointer.data());
                    try {
                        CompactionTask.Plan plan =
                                task.planHandle() == null
                                        ? new CompactionTask.Plan(
                                                Collections.emptyList(), Collections.emptyMap())
                                        : store.readPlan(task.planHandle());
                        return new Recovery(
                                task,
                                plan,
                                store.readProgress(task.progressHandle(), task.succeeded()),
                                pointer.zkVersion(),
                                null);
                    } catch (Exception remoteFailure) {
                        return new Recovery(
                                task,
                                new CompactionTask.Plan(
                                        Collections.emptyList(), Collections.emptyMap()),
                                Collections.emptyMap(),
                                pointer.zkVersion(),
                                remoteFailure);
                    }
                },
                (recovery, error) -> {
                    if (closed) {
                        return;
                    }
                    if (error != null) {
                        storageFailure = error;
                        recovering = false;
                        return;
                    }
                    storageFailure = null;
                    initialized = true;
                    if (recovery == null) {
                        active = null;
                        recovering = false;
                        return;
                    }
                    if (recovery.readFailure != null) {
                        failRecovery(recovery, recovery.readFailure);
                        return;
                    }
                    try {
                        CompactionTask task = recovery.task;
                        checkState(
                                task.planHandle() == null
                                        || task.total() == recovery.plan.buckets().size(),
                                "Invalid compaction plan size");
                        Set<TableBucket> identities = new HashSet<>();
                        recovery.plan
                                .buckets()
                                .forEach(
                                        bucket ->
                                                identities.add(
                                                        new TableBucket(
                                                                bucket.getTableId(),
                                                                bucket.getPartitionId(),
                                                                0)));
                        Scope scope =
                                new Scope(
                                        task.info().databaseName().orElse(null),
                                        task.info().tablePath().orElse(null),
                                        task.info().partitionName().orElse(null),
                                        task.info().type(),
                                        identities,
                                        recovery.plan.buckets());
                        Operation operation = new Operation(task, scope, recovery.plan);
                        checkState(
                                operation.states.size() == recovery.plan.buckets().size(),
                                "Duplicate compaction plan bucket");
                        operation.activeVersion = recovery.activeVersion;
                        operation.planHandle = task.planHandle();
                        operation.error = task.firstError();
                        active = operation;
                        for (Map.Entry<TableBucket, Long> entry : recovery.snapshots.entrySet()) {
                            checkState(
                                    operation.states.containsKey(entry.getKey())
                                            && entry.getValue() >= 0,
                                    "Invalid compaction success journal");
                            operation.states.put(entry.getKey(), CompactionStatus.SUCCEEDED);
                        }
                        operation.admission.complete(task.info().operationId());
                        if (task.planHandle() == null || clock.milliseconds() >= task.deadline()) {
                            end(
                                    operation,
                                    CompactionStatus.FAILED,
                                    new IllegalStateException(
                                            task.planHandle() == null
                                                    ? "Compaction plan was not published"
                                                    : "Compaction deadline exceeded"));
                            return;
                        }
                        recovering = false;
                    } catch (Throwable failure) {
                        failRecovery(recovery, failure);
                    }
                });
    }

    private void failRecovery(Recovery recovery, Throwable failure) {
        initialized = false;
        if (active != null) {
            cancelExecutions(active);
        }
        CompactionTask task = recovery.task;
        Scope scope =
                new Scope(
                        task.info().databaseName().orElse(null),
                        task.info().tablePath().orElse(null),
                        task.info().partitionName().orElse(null),
                        task.info().type(),
                        Collections.emptySet(),
                        Collections.emptyList());
        Operation operation =
                new Operation(
                        task,
                        scope,
                        new CompactionTask.Plan(Collections.emptyList(), Collections.emptyMap()));
        operation.activeVersion = recovery.activeVersion;
        operation.planHandle = task.planHandle();
        operation.error = task.firstError();
        active = operation;
        end(operation, CompactionStatus.FAILED, failure);
    }

    private void create(Operation operation) {
        operation.publishing = true;
        CompactionTask pending = operation.task;
        int epochVersion = context.getCoordinatorZkVersion();
        read(
                () -> {
                    ZooKeeperClient.VersionedData<String> pointer = zk.getCompactionActive();
                    if (!pointer.data().isEmpty()) {
                        throw busy(pointer.data());
                    }
                    store.create(pending, pointer.zkVersion(), epochVersion);
                    return pointer.zkVersion() + 1;
                },
                (version, error) -> {
                    if (error != null) {
                        operation.publishing = false;
                        operation.admission.completeExceptionally(error);
                        operation.termination.completeExceptionally(error);
                        active = null;
                        storageFailure = error;
                        initialized = false;
                        return;
                    }
                    operation.activeVersion = version;
                    read(
                            () -> store.writePlan(pending.info().operationId(), operation.plan),
                            (handle, failure) -> {
                                operation.publishing = false;
                                if (failure != null) {
                                    operation.admission.completeExceptionally(failure);
                                    end(operation, CompactionStatus.FAILED, failure);
                                } else {
                                    operation.planHandle = handle;
                                    publish(operation);
                                }
                            });
                });
    }

    private void advance(Operation operation) {
        if (operation.terminal != null) {
            publish(operation);
            return;
        }
        if (operation.task.planHandle() == null) {
            return;
        }
        try {
            checkState(
                    clock.milliseconds() < operation.task.deadline(),
                    "Compaction deadline exceeded");
            checkState(
                    operation.plan.configuration().equals(baseline()),
                    "Compaction configuration changed");
            for (Execution execution : new ArrayList<>(operation.executions.values())) {
                if (operation.states.get(execution.bucket) == CompactionStatus.SUCCEEDED
                        || operation.snapshots.containsKey(execution.bucket)
                        || execution.completing) {
                    continue;
                }
                if (!sameLeader(execution)) {
                    operation.executions.remove(execution.serverId);
                    operation.states.put(execution.bucket, CompactionStatus.PENDING);
                    continue;
                }
                if (!execution.inFlight) {
                    start(operation, execution);
                }
            }
            for (TableBucket bucket : operation.plan.buckets()) {
                if (operation.terminal != null) {
                    return;
                }
                if (operation.states.get(bucket) != CompactionStatus.PENDING) {
                    continue;
                }
                LeaderAndIsr leader = leader(bucket);
                if (!operation.executions.containsKey(leader.leader())) {
                    Execution execution =
                            new Execution(
                                    bucket,
                                    leader.leader(),
                                    leader.leaderEpoch(),
                                    node(leader.leader()),
                                    clock.milliseconds());
                    operation.executions.put(leader.leader(), execution);
                    operation.states.put(bucket, CompactionStatus.RUNNING);
                    start(operation, execution);
                }
            }
            publish(operation);
        } catch (Throwable error) {
            end(operation, CompactionStatus.FAILED, error);
        }
    }

    private void publish(Operation operation) {
        if (operation.publishing
                || operation.task.isTerminal()
                || storageFailure != null
                || closed) {
            return;
        }
        boolean first = operation.task.planHandle() == null && operation.planHandle != null;
        if (!first && operation.terminal == null && operation.snapshots.isEmpty()) {
            return;
        }
        Map<TableBucket, Long> snapshots = new LinkedHashMap<>();
        operation.snapshots.forEach(
                (bucket, snapshot) -> {
                    if (operation.states.get(bucket) != CompactionStatus.SUCCEEDED) {
                        snapshots.put(bucket, snapshot);
                    }
                });
        operation.snapshots.clear();
        snapshots
                .keySet()
                .forEach(
                        bucket ->
                                operation.executions.values().stream()
                                        .filter(e -> e.bucket.equals(bucket))
                                        .forEach(e -> e.completing = true));
        long successes = operation.task.succeeded() + snapshots.size();
        CompactionStatus status =
                operation.terminal != null
                        ? operation.terminal
                        : successes == operation.task.total()
                                ? CompactionStatus.SUCCEEDED
                                : CompactionStatus.RUNNING;
        boolean terminal = status != CompactionStatus.RUNNING;
        long failed = status == CompactionStatus.FAILED ? operation.task.total() - successes : 0;
        long cancelled =
                status == CompactionStatus.CANCELLED ? operation.task.total() - successes : 0;
        CompactionTask old = operation.task;
        CompactionInfo info = withStatus(old.info(), status);
        int activeVersion = operation.activeVersion;
        long persistedTime = clock.milliseconds();
        long finish = terminal ? persistedTime : 0;
        operation.publishing = true;
        FsPath planHandle = operation.planHandle;
        String firstError = operation.error;
        int epochVersion = context.getCoordinatorZkVersion();
        read(
                () -> {
                    FsPath progress = old.progressHandle();
                    if (!snapshots.isEmpty()) {
                        try {
                            progress =
                                    store.writeProgress(
                                            old.info().operationId(), progress, snapshots);
                        } catch (IOException remoteFailure) {
                            throw new RemoteWriteException(remoteFailure);
                        }
                    }
                    CompactionTask next =
                            new CompactionTask(
                                    info,
                                    old.deadline(),
                                    old.total(),
                                    successes,
                                    failed,
                                    cancelled,
                                    finish,
                                    persistedTime,
                                    planHandle,
                                    progress,
                                    firstError,
                                    old.version() + 1);
                    store.publish(next, old.version(), activeVersion, epochVersion);
                    return next;
                },
                (published, error) -> {
                    operation.publishing = false;
                    if (error instanceof RemoteWriteException) {
                        operation.snapshots.clear();
                        operation.executions.values().forEach(e -> e.completing = false);
                        operation.terminal = null;
                        end(operation, CompactionStatus.FAILED, error.getCause());
                        return;
                    }
                    if (error != null) {
                        storageFailure = error;
                        initialized = false;
                        recovering = false;
                        cancelExecutions(operation);
                        operation.admission.completeExceptionally(error);
                        operation.termination.completeExceptionally(error);
                        return;
                    }
                    operation.task = published;
                    initialized = true;
                    recovering = false;
                    operation.activeVersion++;
                    operation.lastProgressTime = published.persistedTime();
                    for (TableBucket bucket : snapshots.keySet()) {
                        operation.states.put(bucket, CompactionStatus.SUCCEEDED);
                        operation.executions.values().removeIf(e -> e.bucket.equals(bucket));
                    }
                    operation.admission.complete(published.info().operationId());
                    if (published.isTerminal()) {
                        cancelExecutions(operation);
                        operation.executions.clear();
                        operation.termination.complete(null);
                        last = published;
                        active = null;
                        recovering = false;
                        return;
                    }
                    if (first && operation.terminal == null) {
                        advance(operation);
                    }
                    if (operation.terminal != null) {
                        publish(operation);
                    }
                });
    }

    private void start(Operation operation, Execution execution) {
        if (!sameLeader(execution)) {
            operation.executions.remove(execution.serverId);
            operation.states.put(execution.bucket, CompactionStatus.PENDING);
            return;
        }
        checkState(
                clock.milliseconds() < operation.task.deadline(), "Compaction deadline exceeded");
        StartBucketCompactionRequest request =
                new StartBucketCompactionRequest()
                        .setOperationId(operation.task.info().operationId())
                        .setType(operation.task.info().type().name())
                        .setCoordinatorEpoch(context.getCoordinatorEpoch())
                        .setLeaderEpoch(execution.leaderEpoch)
                        .setDeadline(operation.task.deadline());
        setBucket(request.setTableBucket(), execution.bucket);
        operation
                .plan
                .configuration()
                .forEach((key, value) -> request.addConfigBaseline().setKey(key).setValue(value));
        send(operation, execution, request);
    }

    private boolean sameLeader(Execution execution) {
        try {
            LeaderAndIsr leader = leader(execution.bucket);
            return leader.leader() == execution.serverId
                    && leader.leaderEpoch() == execution.leaderEpoch;
        } catch (IllegalStateException missing) {
            return false;
        }
    }

    private void send(
            Operation operation, Execution execution, StartBucketCompactionRequest request) {
        execution.inFlight = true;
        try {
            execution.request =
                    rpc.sendRequest(execution.node, ApiKeys.START_BUCKET_COMPACTION, request);
            execution.request.whenComplete(
                    (response, error) ->
                            events.execute(
                                    () -> {
                                        receive(operation, execution, response, error);
                                    }));
        } catch (Throwable error) {
            receive(operation, execution, null, error);
        }
    }

    private void receive(
            Operation operation, Execution execution, ApiMessage response, Throwable error) {
        if (closed
                || storageFailure != null
                || active != operation
                || operation.terminal != null
                || operation.task.isTerminal()
                || operation.executions.get(execution.serverId) != execution) {
            return;
        }
        execution.inFlight = false;
        if (!sameLeader(execution)) {
            operation.executions.remove(execution.serverId);
            operation.states.put(execution.bucket, CompactionStatus.PENDING);
            return;
        }
        if (error instanceof NetworkException
                || error instanceof TimeoutException
                || error instanceof java.util.concurrent.TimeoutException
                || error instanceof IOException
                || error instanceof CompactionInProgressException) {
            return;
        }
        try {
            if (error != null) {
                throw error;
            }
            PbBucketCompactionStatus status =
                    ((StartBucketCompactionResponse) response).getStatus();
            checkState(
                    status.getOperationId().equals(operation.task.info().operationId())
                            && ServerRpcMessageUtils.toTableBucket(status.getTableBucket())
                                    .equals(execution.bucket)
                            && status.getLeaderEpoch() == execution.leaderEpoch,
                    "Compaction response identity changed");
            if (!execution.phase.equals(status.getPhase())
                    || execution.phaseTime != status.getPhaseStartTime()) {
                operation.lastProgressTime = clock.milliseconds();
            }
            execution.phase = status.getPhase();
            execution.phaseTime = status.getPhaseStartTime();
            execution.snapshotId = status.hasSnapshotId() ? status.getSnapshotId() : null;
            execution.reason = status.hasReason() ? boundedText(status.getReason()) : null;
            CompactionStatus state = CompactionStatus.valueOf(status.getStatus());
            if (state == CompactionStatus.SUCCEEDED) {
                checkState(status.hasSnapshotId(), "Compaction succeeded without a snapshot");
                operation.snapshots.put(execution.bucket, status.getSnapshotId());
            } else if (state == CompactionStatus.FAILED || state == CompactionStatus.CANCELLED) {
                throw new IllegalStateException(
                        status.hasReason() ? status.getReason() : "Bucket compaction failed");
            }
        } catch (Throwable failure) {
            end(operation, CompactionStatus.FAILED, failure);
        }
    }

    private void end(Operation operation, CompactionStatus status, @Nullable Throwable error) {
        if (operation.terminal != null || operation.task.isTerminal()) {
            return;
        }
        operation.terminal = status;
        operation.snapshots.clear();
        if (error != null && operation.error == null) {
            operation.error = boundedError(error);
        }
        publish(operation);
    }

    private void cancelExecutions(Operation operation) {
        for (Execution execution : operation.executions.values()) {
            if (execution.request != null) {
                execution.request.cancel(false);
            }
            if (operation.task.isTerminal() && !execution.cancelSent) {
                execution.cancelSent = true;
                CancelBucketCompactionRequest request =
                        new CancelBucketCompactionRequest()
                                .setOperationId(operation.task.info().operationId());
                setBucket(request.setTableBucket(), execution.bucket);
                try {
                    rpc.sendRequest(execution.node, ApiKeys.CANCEL_BUCKET_COMPACTION, request);
                } catch (Exception ignored) {
                    // The persisted terminal task and absolute deadline also fence tablet work.
                }
            }
        }
    }

    private Scope resolve(CompactRequest request) {
        validateScope(
                request.hasDatabaseName(), request.hasTableName(), request.hasPartitionName());
        String database = request.hasDatabaseName() ? request.getDatabaseName() : null;
        TablePath path =
                request.hasTableName() ? TablePath.of(database, request.getTableName()) : null;
        String partition = request.hasPartitionName() ? request.getPartitionName() : null;
        CompactionType type = CompactionType.valueOf(request.getType());
        List<TableBucket> buckets = new ArrayList<>();
        Set<TableBucket> allBuckets = context.getAllBuckets();
        Set<TableBucket> identities = new HashSet<>();
        for (Map.Entry<Long, TablePath> entry : context.allTables().entrySet()) {
            if ((path != null
                            && (!path.equals(entry.getValue())
                                    || !entry.getKey().equals(context.getTableIdByPath(path))))
                    || (database != null && !database.equals(entry.getValue().getDatabaseName()))
                    || context.getTablesToBeDeleted().contains(entry.getKey())) {
                continue;
            }
            TableInfo table = context.getTableInfoById(entry.getKey());
            checkArgument(table != null, "Table metadata unavailable: %s", entry.getValue());
            if (!table.hasPrimaryKey()) {
                checkArgument(path == null, "Compaction requires a primary-key table");
                continue;
            }
            if (partition != null) {
                checkArgument(table.isPartitioned(), "Table is not partitioned: %s", path);
                String name =
                        ResolvedPartitionSpec.fromPartitionName(table.getPartitionKeys(), partition)
                                .getPartitionName();
                checkArgument(name.equals(partition), "Invalid partition name: %s", partition);
                checkArgument(
                        !HISTORICAL_PARTITION_VALUE.equals(name),
                        "Historical partitions do not support compaction");
                Long partitionId =
                        context.getPartitionId(PhysicalTablePath.of(path, name)).orElse(null);
                checkArgument(partitionId != null, "Partition does not exist: %s", partition);
                identities.add(new TableBucket(table.getTableId(), partitionId, 0));
                buckets.addAll(context.getAllBucketsForPartition(table.getTableId(), partitionId));
            } else if (table.isPartitioned()) {
                for (TableBucket bucket : allBuckets) {
                    if (bucket.getTableId() == table.getTableId()) {
                        checkArgument(
                                bucket.getPartitionId() != null,
                                "Invalid partitioned bucket: %s",
                                bucket);
                        checkArgument(
                                !HISTORICAL_PARTITION_VALUE.equals(
                                        context.getPartitionName(bucket.getPartitionId())),
                                "Historical partitions do not support compaction");
                        identities.add(
                                new TableBucket(bucket.getTableId(), bucket.getPartitionId(), 0));
                        buckets.add(bucket);
                    }
                }
            } else {
                identities.add(new TableBucket(table.getTableId(), 0));
                buckets.addAll(context.getAllBucketsForTable(table.getTableId()));
            }
        }
        checkArgument(!buckets.isEmpty(), "Compaction target is empty");
        for (TableBucket bucket : buckets) {
            checkArgument(
                    !context.isToBeDeleted(bucket),
                    "Compaction target is being deleted: %s",
                    bucket);
        }
        buckets.sort(
                Comparator.comparingLong(TableBucket::getTableId)
                        .thenComparing(
                                TableBucket::getPartitionId,
                                Comparator.nullsFirst(Comparator.naturalOrder()))
                        .thenComparingInt(TableBucket::getBucket));
        return new Scope(database, path, partition, type, identities, buckets);
    }

    private Map<String, String> baseline() {
        Configuration config = configuration.get();
        checkArgument(
                config.get(ConfigOptions.KV_COMPACTION_STYLE)
                        == ConfigOptions.CompactionStyle.LEVEL,
                "Manual compaction requires LEVEL compaction style");
        Map<String, String> baseline = new LinkedHashMap<>();
        baseline.put(
                ConfigOptions.KV_COMPACTION_STYLE.key(),
                config.get(ConfigOptions.KV_COMPACTION_STYLE).name());
        baseline.put(
                ConfigOptions.KV_PARTITIONED_INDEX_FILTERS.key(),
                config.get(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS).toString());
        return baseline;
    }

    private LeaderAndIsr leader(TableBucket bucket) {
        checkState(
                context.containsTableId(bucket.getTableId())
                        && (bucket.getPartitionId() == null
                                || context.containsPartitionId(bucket.getPartitionId()))
                        && !context.getTablesToBeDeleted().contains(bucket.getTableId())
                        && !context.isToBeDeleted(bucket),
                "Compaction target disappeared: %s",
                bucket);
        checkState(
                context.isLeaderActive(bucket),
                "No active leader for compaction bucket %s",
                bucket);
        return context.getBucketLeaderAndIsr(bucket).get();
    }

    private ServerNode node(int serverId) {
        return context.getLiveTabletServers().get(serverId).nodeOrThrow(listener);
    }

    private CompactionProgress progress(Operation operation) {
        if (operation.task.isTerminal()) {
            return progress(operation.task);
        }
        List<CompactionProgress.Execution> executions = new ArrayList<>();
        for (Execution execution : operation.executions.values()) {
            executions.add(
                    new CompactionProgress.Execution(
                            execution.serverId,
                            execution.bucket,
                            execution.leaderEpoch,
                            execution.phase,
                            execution.phaseTime,
                            execution.snapshotId,
                            execution.reason));
        }
        executions.sort(Comparator.comparingInt(CompactionProgress.Execution::serverId));
        long running = executions.size();
        return new CompactionProgress(
                operation.task.info(),
                operation.task.total(),
                operation.task.total() - running - operation.task.succeeded(),
                running,
                operation.task.succeeded(),
                0,
                0,
                executions,
                operation.task.firstError(),
                operation.lastProgressTime,
                clock.milliseconds(),
                operation.task.persistedTime());
    }

    private CompactionProgress progress(CompactionTask task) {
        return new CompactionProgress(
                task.info(),
                task.total(),
                task.total() - task.succeeded() - task.failed() - task.cancelled(),
                0,
                task.succeeded(),
                task.failed(),
                task.cancelled(),
                Collections.emptyList(),
                task.firstError(),
                task.persistedTime(),
                clock.milliseconds(),
                task.persistedTime());
    }

    private boolean expired(CompactionTask task) {
        return task.isTerminal() && task.finishTime() < clock.milliseconds() - retention;
    }

    private CompactionTask load(String id) throws Exception {
        try {
            return store.load(id);
        } catch (KeeperException.NoNodeException error) {
            throw new CompactionNotExistException("Compaction does not exist: " + id);
        }
    }

    @Nullable
    private Operation find(String id) {
        return active != null && active.task.info().operationId().equals(id) ? active : null;
    }

    private void checkStorage() {
        if (storageFailure != null) {
            throw new IllegalStateException("Compaction persistence failed", storageFailure);
        }
    }

    private <T> void execute(CompletableFuture<T> result, Runnable action) {
        try {
            events.execute(
                    () -> {
                        try {
                            checkState(!closed, "Compaction manager closed");
                            action.run();
                        } catch (Throwable error) {
                            result.completeExceptionally(error);
                        }
                    });
        } catch (Throwable error) {
            result.completeExceptionally(error);
        }
    }

    private <T> void read(Callable<T> action, BiConsumer<T, Throwable> completion) {
        try {
            io.execute(
                    () -> {
                        T result = null;
                        Throwable error = null;
                        try {
                            checkState(!closed, "Compaction manager closed");
                            result = action.call();
                        } catch (Throwable failure) {
                            error = failure;
                        }
                        T value = result;
                        Throwable failure = error;
                        events.execute(
                                () -> {
                                    completion.accept(
                                            value,
                                            closed
                                                    ? new IllegalStateException(
                                                            "Compaction manager closed")
                                                    : failure);
                                });
                    });
        } catch (Throwable error) {
            completion.accept(null, error);
        }
    }

    private static void validateScope(boolean database, boolean table, boolean partition) {
        checkArgument(!table || database, "Table scope requires a database");
        checkArgument(!partition || table, "Partition scope requires a table");
    }

    private static void setBucket(PbTableBucket target, TableBucket bucket) {
        target.setTableId(bucket.getTableId()).setBucketId(bucket.getBucket());
        if (bucket.getPartitionId() != null) {
            target.setPartitionId(bucket.getPartitionId());
        }
    }

    private static CompactionInfo withStatus(CompactionInfo info, CompactionStatus status) {
        return new CompactionInfo(
                info.operationId(),
                info.type(),
                status,
                info.createTime(),
                info.databaseName().orElse(null),
                info.tablePath().orElse(null),
                info.partitionName().orElse(null));
    }

    private static CompactionInProgressException busy(String id) {
        return new CompactionInProgressException("Compaction already in progress: " + id);
    }

    private static String boundedError(Throwable error) {
        return boundedText(error.toString());
    }

    private static String boundedText(String text) {
        if (text.length() > ERROR_LIMIT) {
            text = text.substring(0, ERROR_LIMIT);
        }
        while (text.getBytes(StandardCharsets.UTF_8).length > ERROR_LIMIT) {
            text = text.substring(0, text.length() - 1);
        }
        return text;
    }

    private static <T> void complete(CompletableFuture<T> future, T result, Throwable error) {
        if (error == null) {
            future.complete(result);
        } else {
            future.completeExceptionally(error);
        }
    }

    private static final class RemoteWriteException extends IOException {
        private RemoteWriteException(IOException cause) {
            super(cause);
        }
    }

    private static final class Recovery {
        private final CompactionTask task;
        private final CompactionTask.Plan plan;
        private final Map<TableBucket, Long> snapshots;
        private final int activeVersion;
        @Nullable private final Throwable readFailure;

        private Recovery(
                CompactionTask task,
                CompactionTask.Plan plan,
                Map<TableBucket, Long> snapshots,
                int activeVersion,
                @Nullable Throwable readFailure) {
            this.task = task;
            this.plan = plan;
            this.snapshots = snapshots;
            this.activeVersion = activeVersion;
            this.readFailure = readFailure;
        }
    }

    private static final class Scope {
        @Nullable private final String database;
        @Nullable private final TablePath table;
        @Nullable private final String partition;
        private final CompactionType type;
        private final Set<TableBucket> identities;
        private final List<TableBucket> buckets;

        private Scope(
                String database,
                TablePath table,
                String partition,
                CompactionType type,
                Set<TableBucket> identities,
                List<TableBucket> buckets) {
            this.database = database;
            this.table = table;
            this.partition = partition;
            this.type = type;
            this.identities = identities;
            this.buckets = buckets;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Scope)) {
                return false;
            }
            Scope that = (Scope) other;
            return Objects.equals(database, that.database)
                    && Objects.equals(table, that.table)
                    && Objects.equals(partition, that.partition)
                    && type == that.type
                    && identities.equals(that.identities);
        }

        @Override
        public int hashCode() {
            return Objects.hash(database, table, partition, type, identities);
        }
    }

    private static final class Operation {
        private CompactionTask task;
        private final Scope scope;
        private final CompactionTask.Plan plan;
        private final CompletableFuture<String> admission = new CompletableFuture<>();
        private final CompletableFuture<Void> termination = new CompletableFuture<>();
        private final Map<TableBucket, CompactionStatus> states = new LinkedHashMap<>();
        private final Map<Integer, Execution> executions = new HashMap<>();
        private final Map<TableBucket, Long> snapshots = new LinkedHashMap<>();
        @Nullable private FsPath planHandle;
        @Nullable private CompactionStatus terminal;
        @Nullable private String error;
        private int activeVersion;
        private boolean publishing;
        private long lastProgressTime;

        private Operation(CompactionTask task, Scope scope, CompactionTask.Plan plan) {
            this.task = task;
            this.lastProgressTime = task.persistedTime();
            this.scope = scope;
            this.plan = plan;
            plan.buckets().forEach(bucket -> states.put(bucket, CompactionStatus.PENDING));
        }
    }

    private static final class Execution {
        private final TableBucket bucket;
        private final int serverId;
        private final int leaderEpoch;
        private final ServerNode node;
        private String phase = "DISPATCHED";
        private long phaseTime;
        @Nullable private Long snapshotId;
        @Nullable private String reason;
        private boolean inFlight;
        private boolean completing;
        private boolean cancelSent;
        @Nullable private CompletableFuture<ApiMessage> request;

        private Execution(
                TableBucket bucket,
                int serverId,
                int leaderEpoch,
                ServerNode node,
                long phaseTime) {
            this.bucket = bucket;
            this.serverId = serverId;
            this.leaderEpoch = leaderEpoch;
            this.node = node;
            this.phaseTime = phaseTime;
        }
    }
}
