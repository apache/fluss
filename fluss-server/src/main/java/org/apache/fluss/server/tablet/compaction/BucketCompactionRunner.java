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

package org.apache.fluss.server.tablet.compaction;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.compaction.CompactionProgress;
import org.apache.fluss.cluster.compaction.CompactionStatus;
import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.CompactionInProgressException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.PbBucketCompactionStatus;
import org.apache.fluss.rpc.messages.PbKeyValue;
import org.apache.fluss.rpc.messages.StartBucketCompactionRequest;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.PeriodicSnapshotManager;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.utils.CancellationToken;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.Scheduler;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/** Owns one local compaction slot until the worker and snapshot resource users have exited. */
@Internal
@ThreadSafe
public class BucketCompactionRunner {
    private final Object lock = new Object();
    private final int serverId;
    private final Runnable admission;
    private int lastCoordinatorEpoch;
    private final Function<TableBucket, Replica> replicas;
    private final Function<String, CompletableFuture<CompactionProgress>> taskLookup;
    private final Scheduler scheduler;
    private final Clock clock;
    private final ExecutorService worker;
    private final Supplier<CancellationToken> cancellations;

    @GuardedBy("lock")
    @Nullable
    private Execution current;

    @GuardedBy("lock")
    @Nullable
    private PbBucketCompactionStatus lastResult;

    @GuardedBy("lock")
    @Nullable
    private ScheduledFuture<?> resultExpiry;

    @GuardedBy("lock")
    private long lastDeadline;

    @GuardedBy("lock")
    private boolean closed;

    /** Creates the dedicated worker, using the tablet server's existing timer. */
    public BucketCompactionRunner(
            int serverId,
            Function<TableBucket, Replica> replicas,
            Function<String, CompletableFuture<CompactionProgress>> taskLookup,
            Scheduler scheduler,
            Clock clock,
            Runnable admission) {
        this(
                serverId,
                replicas,
                taskLookup,
                scheduler,
                clock,
                admission,
                Executors.newSingleThreadExecutor(
                        runnable -> new Thread(runnable, "bucket-compaction")),
                CancellationToken::new);
    }

    @VisibleForTesting
    BucketCompactionRunner(
            int serverId,
            Function<TableBucket, Replica> replicas,
            Function<String, CompletableFuture<CompactionProgress>> taskLookup,
            Scheduler scheduler,
            Clock clock,
            Runnable admission,
            ExecutorService worker,
            Supplier<CancellationToken> cancellations) {
        this.serverId = serverId;
        this.admission = admission;
        this.replicas = replicas;
        this.taskLookup = taskLookup;
        this.scheduler = scheduler;
        this.clock = clock;
        this.worker = worker;
        this.cancellations = cancellations;
    }

    /** Returns matching work or reserves the empty slot and submits a worker immediately. */
    public CompletableFuture<PbBucketCompactionStatus> startBucketCompaction(
            StartBucketCompactionRequest request) {
        StartBucketCompactionRequest command = new StartBucketCompactionRequest().copyFrom(request);
        final Execution execution;
        try {
            synchronized (lock) {
                if (closed) {
                    throw new IllegalStateException("Compaction runner is closed.");
                }
                if (current != null) {
                    if (matches(current.status, command.getOperationId(), bucket(command))
                            && current.command.getCoordinatorEpoch()
                                    == command.getCoordinatorEpoch()
                            && current.command.getLeaderEpoch() == command.getLeaderEpoch()) {
                        return CompletableFuture.completedFuture(observe(current));
                    }
                    throw new CompactionInProgressException(
                            "Compaction slot is busy: " + current.command.getOperationId());
                }
                if (lastResult != null
                        && (lastResult.getStatus().equals(CompactionStatus.SUCCEEDED.name())
                                || lastCoordinatorEpoch == command.getCoordinatorEpoch())
                        && lastResult.getLeaderEpoch() == command.getLeaderEpoch()
                        && clock.milliseconds() < lastDeadline
                        && matches(lastResult, command.getOperationId(), bucket(command))) {
                    return CompletableFuture.completedFuture(
                            new PbBucketCompactionStatus().copyFrom(lastResult));
                }
                if (clock.milliseconds() >= command.getDeadline()) {
                    throw new IllegalStateException("Compaction deadline has expired.");
                }
                admission.run();
                Replica replica = replicas.apply(bucket(command));
                if (replica == null) {
                    throw new IllegalStateException("Compaction replica is unavailable.");
                }
                clearResult();
                execution = new Execution(command, replica);
                current = execution;
            }
        } catch (Throwable failure) {
            return failed(failure);
        }
        PbBucketCompactionStatus accepted =
                new PbBucketCompactionStatus().copyFrom(execution.status);
        try {
            execution.timer =
                    scheduler.scheduleOnce(
                            "compaction-deadline",
                            execution.cancellation::cancel,
                            Math.max(0, command.getDeadline() - clock.milliseconds()));
            worker.execute(() -> run(execution));
            return CompletableFuture.completedFuture(accepted);
        } catch (Throwable failure) {
            if (execution.timer != null) {
                execution.timer.cancel(false);
            }
            synchronized (lock) {
                if (current == execution) {
                    current = null;
                }
            }
            execution.exited.complete(null);
            return failed(failure);
        }
    }

    private PbBucketCompactionStatus observe(Execution execution) {
        PbBucketCompactionStatus status = new PbBucketCompactionStatus().copyFrom(execution.status);
        if (execution.cancellation.isCancelled()) {
            status.setStatus(CompactionStatus.CANCELLED.name()).setReason("Compaction cancelled.");
        }
        return status;
    }

    /** Acknowledges cancellation immediately while the slot remains occupied until actual exit. */
    public CompletableFuture<Void> cancelBucketCompaction(String operationId, TableBucket bucket) {
        Execution execution;
        synchronized (lock) {
            execution = current;
            if (execution == null || !matches(execution.status, operationId, bucket)) {
                return CompletableFuture.completedFuture(null);
            }
        }
        execution.cancellation.cancel();
        return CompletableFuture.completedFuture(null);
    }

    /** Rejects new work and drains the current worker without closing the shared timer. */
    public CompletableFuture<Void> closeAsync() {
        Execution execution;
        synchronized (lock) {
            closed = true;
            clearResult();
            execution = current;
        }
        if (execution != null) {
            execution.cancellation.cancel();
        }
        worker.shutdown();
        return execution == null ? CompletableFuture.completedFuture(null) : execution.exited;
    }

    private void run(Execution execution) {
        StartBucketCompactionRequest command = execution.command;
        CancellationToken cancellation = execution.cancellation;
        AutoCloseable wakeup = cancellation.register(Thread.currentThread()::interrupt);
        AutoCloseable binding = null;
        PeriodicSnapshotManager snapshots = null;
        boolean snapshotRequested = false;
        Long snapshotId = null;
        Throwable failure = null;
        try {
            checkActive(execution);
            validateDispatch(command, await(execution, taskLookup.apply(command.getOperationId())));
            Replica replica = execution.replica;
            if (replicas.apply(bucket(command)) != replica
                    || replica.getCoordinatorEpoch() != command.getCoordinatorEpoch()) {
                throw new IOException("Compaction replica or coordinator epoch changed.");
            }
            KvTablet tablet = replica.getKvTablet();
            snapshots = replica.getKvSnapshotManager();
            if (tablet == null || snapshots == null) {
                throw new IOException("Compaction requires an active primary-key replica.");
            }
            binding = replica.registerCompaction(tablet, command.getLeaderEpoch(), cancellation);
            checkActive(execution);
            String layout =
                    command.getConfigBaselinesList().stream()
                            .filter(
                                    entry ->
                                            entry.getKey()
                                                    .equals(
                                                            ConfigOptions
                                                                    .KV_PARTITIONED_INDEX_FILTERS
                                                                    .key()))
                            .map(PbKeyValue::getValue)
                            .findFirst()
                            .orElse(null);
            if ((!"true".equals(layout) && !"false".equals(layout))
                    || tablet.isPartitionedIndexEnabled() != Boolean.parseBoolean(layout)) {
                throw new IOException(
                        "Opened KV layout does not match the compaction configuration baseline.");
            }
            tablet.ensureCompactionAllowed();
            long offset = replica.getLogHighWatermark();
            phase(execution, "FLUSH", offset);
            await(execution, replica.flushForCompaction(tablet, command.getLeaderEpoch(), offset));
            checkActive(execution);
            phase(execution, "COMPACTION", offset);
            tablet.compact(CompactionType.valueOf(command.getType()), cancellation);
            checkActive(execution);
            phase(execution, "SNAPSHOT", offset);
            CompletableFuture<CompletedSnapshotHandle> result =
                    replica.snapshotAfterCompaction(tablet, command.getLeaderEpoch());
            snapshotRequested = true;
            snapshotId = await(execution, result).getSnapshotId();
            checkActive(execution);
        } catch (Throwable error) {
            if (error instanceof TimeoutException) {
                cancellation.cancel();
            }
            failure = error;
        } finally {
            // The logical deadline cannot release resources still owned by a native or snapshot
            // user.
            if (snapshotRequested) {
                try {
                    snapshots.cancelSnapshotAfterCompaction().join();
                } catch (Throwable error) {
                    if (failure == null) {
                        failure = error;
                    }
                }
            }
            if (execution.timer != null) {
                execution.timer.cancel(false);
            }
            try {
                if (binding != null) {
                    binding.close();
                }
            } catch (Exception error) {
                if (failure == null) {
                    failure = error;
                }
            }
            try {
                wakeup.close();
            } catch (Exception error) {
                if (failure == null) {
                    failure = error;
                }
            }
            Thread.interrupted();
            synchronized (lock) {
                CompactionStatus state =
                        cancellation.isCancelled()
                                ? CompactionStatus.CANCELLED
                                : failure == null
                                        ? CompactionStatus.SUCCEEDED
                                        : CompactionStatus.FAILED;
                execution.status =
                        copy(
                                execution.status,
                                state,
                                execution.status.getPhase(),
                                execution.status.getPhaseStartTime(),
                                execution.status.hasCoveredOffset()
                                        ? execution.status.getCoveredOffset()
                                        : null,
                                snapshotId,
                                state == CompactionStatus.CANCELLED
                                        ? "Compaction cancelled."
                                        : failure == null ? null : reason(failure));
                current = null;
                if (!closed && clock.milliseconds() < command.getDeadline()) {
                    lastResult = execution.status;
                    lastDeadline = command.getDeadline();
                    lastCoordinatorEpoch = command.getCoordinatorEpoch();
                    PbBucketCompactionStatus retained = lastResult;
                    try {
                        resultExpiry =
                                scheduler.scheduleOnce(
                                        "compaction-result-expiry",
                                        () -> {
                                            synchronized (lock) {
                                                if (lastResult == retained) {
                                                    lastResult = null;
                                                    resultExpiry = null;
                                                }
                                            }
                                        },
                                        command.getDeadline() - clock.milliseconds());
                    } catch (RuntimeException unavailable) {
                        lastResult = null;
                    }
                }
            }
            execution.exited.complete(null);
        }
    }

    private void validateDispatch(StartBucketCompactionRequest command, CompactionProgress progress)
            throws IOException {
        if (!progress.info().operationId().equals(command.getOperationId())
                || progress.info().type() != CompactionType.valueOf(command.getType())
                || progress.info().status() != CompactionStatus.RUNNING
                || progress.currentExecutions().stream()
                        .noneMatch(
                                item ->
                                        item.serverId() == serverId
                                                && item.bucket().equals(bucket(command))
                                                && item.leaderEpoch()
                                                        == command.getLeaderEpoch())) {
            throw new IOException("Compaction is no longer the current coordinator dispatch.");
        }
    }

    private <T> T await(Execution execution, CompletableFuture<T> future) throws Exception {
        checkActive(execution);
        T result =
                future.get(
                        Math.max(1, execution.command.getDeadline() - clock.milliseconds()),
                        TimeUnit.MILLISECONDS);
        checkActive(execution);
        return result;
    }

    private void checkActive(Execution execution) throws IOException, TimeoutException {
        if (clock.milliseconds() >= execution.command.getDeadline()) {
            execution.cancellation.cancel();
            throw new TimeoutException("Compaction deadline has expired.");
        }
        execution.cancellation.checkCancelled();
    }

    private void phase(Execution execution, String phase, long offset) {
        synchronized (lock) {
            execution.status =
                    copy(
                            execution.status,
                            CompactionStatus.RUNNING,
                            phase,
                            clock.milliseconds(),
                            offset,
                            null,
                            null);
        }
    }

    @GuardedBy("lock")
    private void clearResult() {
        lastResult = null;
        if (resultExpiry != null) {
            resultExpiry.cancel(false);
            resultExpiry = null;
        }
    }

    private static TableBucket bucket(StartBucketCompactionRequest command) {
        return ServerRpcMessageUtils.toTableBucket(command.getTableBucket());
    }

    private static boolean matches(
            PbBucketCompactionStatus status, String operation, TableBucket bucket) {
        return status.getOperationId().equals(operation)
                && ServerRpcMessageUtils.toTableBucket(status.getTableBucket()).equals(bucket);
    }

    private static PbBucketCompactionStatus copy(
            PbBucketCompactionStatus source,
            CompactionStatus state,
            String phase,
            long phaseStartTime,
            Long offset,
            Long snapshotId,
            String reason) {
        PbBucketCompactionStatus result =
                new PbBucketCompactionStatus()
                        .copyFrom(source)
                        .setStatus(state.name())
                        .setPhase(phase)
                        .setPhaseStartTime(phaseStartTime);
        if (offset != null) {
            result.setCoveredOffset(offset);
        }
        if (snapshotId != null) {
            result.setSnapshotId(snapshotId);
        }
        if (reason != null) {
            result.setReason(reason);
        }
        return result;
    }

    private static String reason(Throwable failure) {
        // At most 1024 UTF-16 code units occupy no more than the 4 KiB UTF-8 error budget.
        String text = failure.toString();
        return text.substring(0, Math.min(text.length(), 1024));
    }

    private static <T> CompletableFuture<T> failed(Throwable failure) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(failure);
        return future;
    }

    private final class Execution {
        private final StartBucketCompactionRequest command;
        private final Replica replica;
        private final CancellationToken cancellation = cancellations.get();
        private final CompletableFuture<Void> exited = new CompletableFuture<>();

        @GuardedBy("lock")
        private PbBucketCompactionStatus status;

        @Nullable private ScheduledFuture<?> timer;

        private Execution(StartBucketCompactionRequest command, Replica replica) {
            this.command = command;
            this.replica = replica;
            this.status =
                    new PbBucketCompactionStatus()
                            .setOperationId(command.getOperationId())
                            .setLeaderEpoch(command.getLeaderEpoch())
                            .setStatus(CompactionStatus.RUNNING.name())
                            .setPhase("FLUSH")
                            .setPhaseStartTime(clock.milliseconds());
            this.status.setTableBucket().copyFrom(command.getTableBucket());
        }
    }
}
