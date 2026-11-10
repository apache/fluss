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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.AbortBulkLoadResponse;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadResponse;
import org.apache.fluss.rpc.messages.GetInProgressBulkLoadResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.messages.PbTableInfo;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.CoordinatorChannelManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadReplicaConvergence.ReplicaConvergenceContext;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.BulkLoadMaintenanceEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.ServerProtocolVersion;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.fromPhysicalTablePath;

/**
 * Event-thread orchestration of a fixed-target, shared BulkLoad transaction.
 *
 * <table>
 * <caption>Durable transaction transitions</caption>
 * <tr><th>State</th><th>Work</th><th>Outcome</th></tr>
 * <tr><td>BEGUN</td><td>Prepare fixed target metadata and validate the common input</td>
 * <td>COMMITTING, or atomic ABORTED with all owners released</td></tr>
 * <tr><td>COMMITTING</td><td>Adopt snapshots and confirm every current leader</td>
 * <td>Atomic COMMITTED with all owners released</td></tr>
 * <tr><td>Terminal</td><td>Complete requests and retain the result</td><td>Decision preserved</td></tr>
 * </table>
 *
 * <p>The event thread schedules work and publishes matching callbacks. Storage I/O runs on the
 * coordinator I/O executor. A pending operation retains its identity until it exits. Abort competes
 * with the commit decision independently of read-only validation. Process-owned capacity remains
 * held until the terminal decision or epoch shutdown and every outstanding task have completed.
 */
@Internal
public final class BulkLoadManager {
    private final ZooKeeperClient zkClient;
    private final BulkLoadMetadataStore metadataStore;
    private final Configuration configuration;
    private final CoordinatorContext coordinatorContext;
    private final MetadataManager metadataManager;
    private final ExecutorService ioExecutor;
    private final EventManager eventManager;
    private final BulkLoadReplicaConvergence convergence;
    private final BulkLoadResultGc resultGc;
    private final CompletedSnapshotStoreManager completedSnapshotStoreManager;
    private final Semaphore capacity;
    private final Map<BulkLoadHandle, PendingBulkLoad> pending = new HashMap<>();
    private final Set<PendingBulkLoad> admitting = new HashSet<>();
    private volatile boolean closed;

    /** Creates the manager with capacity shared by coordinator epochs in this process. */
    public BulkLoadManager(
            ZooKeeperClient zkClient,
            Configuration configuration,
            CoordinatorContext context,
            MetadataManager metadataManager,
            CoordinatorChannelManager channelManager,
            ExecutorService ioExecutor,
            EventManager events,
            CompletedSnapshotStoreManager snapshots,
            Semaphore capacity) {
        this.zkClient = zkClient;
        this.metadataStore = new BulkLoadMetadataStore(zkClient);
        this.configuration = configuration;
        this.coordinatorContext = context;
        this.metadataManager = metadataManager;
        this.ioExecutor = ioExecutor;
        this.eventManager = events;
        this.completedSnapshotStoreManager = snapshots;
        this.capacity = capacity;
        this.convergence = new BulkLoadReplicaConvergence(channelManager);
        this.resultGc = new BulkLoadResultGc(zkClient, ioExecutor);
    }

    /** Schedules recovery without performing storage I/O on the event thread. */
    public void restoreTransactions(List<BulkLoadHandle> handles) {
        for (BulkLoadHandle handle : handles) {
            pending.computeIfAbsent(handle, ignored -> new PendingBulkLoad());
        }
    }

    /** Retries each independently owned transaction. */
    public void resumeActiveTransactions() {
        for (BulkLoadHandle handle : new ArrayList<>(pending.keySet())) {
            PendingBulkLoad work = pending.get(handle);
            if (work.preCommitDeadline <= System.currentTimeMillis() && !work.abortRequested) {
                abort(handle, BulkLoadAbortReason.DEADLINE_EXCEEDED);
            }
            advance(handle);
        }
    }

    /** Releases epoch ownership while workers retain their capacity until they actually exit. */
    public void close() {
        closed = true;
        Set<PendingBulkLoad> owned = new HashSet<>(pending.values());
        owned.addAll(admitting);
        for (PendingBulkLoad work : owned) {
            if (work.lifetime != null) {
                work.lifetime.finish();
            }
            failWaiters(work, new FlussRuntimeException("Coordinator epoch ended."));
            if (work.abortResult != null) {
                work.abortResult.completeExceptionally(invalid("Coordinator epoch ended."));
            }
            if (work.beginResult != null) {
                work.beginResult.completeExceptionally(invalid("Coordinator epoch ended."));
            }
        }
        pending.clear();
        admitting.clear();
    }

    /** Begins one metadata-only transaction for the whole selected target set. */
    public CompletableFuture<BeginBulkLoadResponse> begin(
            List<PhysicalTablePath> targets, @Nullable Long timeoutMs, FlussPrincipal creator) {
        long timeout =
                timeoutMs == null
                        ? configuration
                                .get(ConfigOptions.COORDINATOR_BULKLOAD_TIMEOUT_DEFAULT)
                                .toMillis()
                        : timeoutMs;
        if (timeout <= 0
                || timeout
                        > configuration
                                .get(ConfigOptions.COORDINATOR_BULKLOAD_TIMEOUT_MAX)
                                .toMillis()) {
            return FutureUtils.completedExceptionally(invalid("Invalid BulkLoad timeout."));
        }
        final long deadline = Math.addExact(System.currentTimeMillis(), timeout);
        if (!capacity.tryAcquire()) {
            return FutureUtils.completedExceptionally(
                    invalid("BulkLoad active work limit reached."));
        }
        PendingBulkLoad work = new PendingBulkLoad();
        work.lifetime = new WorkLifetime(capacity);
        CompletableFuture<BeginBulkLoadResponse> response = new CompletableFuture<>();
        work.beginResult = response;
        admitting.add(work);
        int epoch = coordinatorEpochVersion();
        io(
                        work,
                        () -> {
                            requireClusterCapabilities();
                            if (targets.isEmpty()
                                    || new HashSet<>(targets).size() != targets.size()) {
                                throw invalid(
                                        "BulkLoad requires nonempty, duplicate-free targets.");
                            }
                            TablePath path = targets.get(0).getTablePath();
                            Versioned<TableRegistration> table =
                                    requireFound(
                                            ZkData.TableZNode.path(path),
                                            ZkData.TableZNode::decode,
                                            "table registration");
                            TableInfo info = metadataManager.getTable(path);
                            SchemaInfo schema = metadataManager.getLatestSchema(path);
                            if (info.getPrimaryKeys().isEmpty()
                                    || !schema.getSchema()
                                            .getAutoIncrementColumnNames()
                                            .isEmpty()) {
                                throw invalid(
                                        "BulkLoad requires a primary-key table without auto-increment columns.");
                            }
                            List<BulkLoadTarget> frozen = new ArrayList<>();
                            List<Versioned<?>> registrations = new ArrayList<>();
                            List<Versioned<? extends TableAssignment>> assignments =
                                    new ArrayList<>();
                            Map<PhysicalTablePath, TableInfo> infos = new HashMap<>();
                            for (PhysicalTablePath target : targets) {
                                if (!path.equals(target.getTablePath())
                                        || (info.isPartitioned()
                                                != (target.getPartitionName() != null))) {
                                    throw invalid(
                                            "BulkLoad targets must be physical targets of one table.");
                                }
                                PhysicalTarget physical = readPhysicalTarget(target);
                                if (physical.tableInfo.getTableId() != info.getTableId()
                                        || BulkLoadMetadataStore.owner(
                                                        physical.registration.getValue())
                                                != null) {
                                    throw invalid("BulkLoad target identity or owner changed.");
                                }
                                if (physical.remoteDataDir == null
                                        || physical.remoteDataDir.isEmpty()) {
                                    throw invalid("Missing remote data directory.");
                                }
                                Versioned<? extends TableAssignment> assignment =
                                        readAssignment(info.getTableId(), physical.partitionId);
                                requireCompleteAssignment(
                                        info.getTableId(),
                                        physical.partitionId,
                                        assignment.getValue(),
                                        physical.tableInfo.getNumBuckets());
                                long[] ids = new long[physical.tableInfo.getNumBuckets()];
                                for (int bucket = 0; bucket < ids.length; bucket++) {
                                    ids[bucket] =
                                            zkClient.allocateTableBucketSnapshotId(
                                                    new TableBucket(
                                                            info.getTableId(),
                                                            physical.partitionId,
                                                            bucket));
                                }
                                frozen.add(
                                        new BulkLoadTarget(
                                                target,
                                                physical.partitionId,
                                                physical.remoteDataDir,
                                                schema.getSchemaId(),
                                                physical.registration.getVersion() + 1,
                                                ids));
                                registrations.add(physical.registration);
                                assignments.add(assignment);
                                infos.put(target, physical.tableInfo);
                            }
                            BulkLoadHandle handle =
                                    new BulkLoadHandle(
                                            path, info.getTableId(), UUID.randomUUID().toString());
                            BulkLoadTransaction transaction =
                                    new BulkLoadTransaction(
                                            handle,
                                            BulkLoadState.BEGUN,
                                            creator.getName(),
                                            creator.getType(),
                                            frozen,
                                            deadline,
                                            null,
                                            null,
                                            null);
                            // The shared table registration protects table identity and the
                            // layout/schema context.
                            if (table.getValue().tableId != info.getTableId()) {
                                throw invalid("BulkLoad table identity changed.");
                            }
                            work.attemptedHandle = handle;
                            if (System.currentTimeMillis() >= deadline) {
                                throw invalid(
                                        "BulkLoad Begin preparation deadline expired before ownership.");
                            }
                            Versioned<BulkLoadTransaction> created =
                                    metadataStore.createTransactionAndOwn(
                                            registrations, assignments, table, transaction, epoch);
                            BeginBulkLoadResponse result =
                                    new BeginBulkLoadResponse().setHandle(toPbHandle(handle));
                            for (BulkLoadTarget target : transaction.getTargets()) {
                                TableInfo effective = infos.get(target.getTarget());
                                result.addAllTargetInfos(
                                        Collections.singletonList(toTargetInfo(target, effective)));
                            }
                            return new BeginResult(created, result);
                        })
                .whenComplete(
                        (result, failure) -> {
                            if (closed) {
                                work.lifetime.finish();
                                response.completeExceptionally(invalid("Coordinator epoch ended."));
                                return;
                            }
                            onEvent(
                                    () -> {
                                        admitting.remove(work);
                                        if (failure != null) {
                                            if (work.attemptedHandle == null) {
                                                work.lifetime.finish();
                                            } else {
                                                pending.put(work.attemptedHandle, work);
                                                advance(work.attemptedHandle);
                                            }
                                            response.completeExceptionally(unwrap(failure));
                                            work.beginResult = null;
                                            return;
                                        }
                                        work.preCommitDeadline =
                                                result.transaction.getValue().getDeadlineMs();
                                        pending.put(
                                                result.transaction.getValue().getHandle(), work);
                                        response.complete(result.response);
                                        work.beginResult = null;
                                    });
                        });
        return response;
    }

    /** Finds the shared owner of one physical target. */
    public CompletableFuture<GetInProgressBulkLoadResponse> getInProgress(
            PhysicalTablePath target, FlussPrincipal creator, boolean canAlter) {
        return io(
                null,
                () -> {
                    PhysicalTarget physical = readPhysicalTarget(target);
                    String owner = BulkLoadMetadataStore.owner(physical.registration.getValue());
                    GetInProgressBulkLoadResponse response = new GetInProgressBulkLoadResponse();
                    if (owner != null) {
                        BulkLoadTransaction transaction =
                                readTransactionVersioned(
                                                new BulkLoadHandle(
                                                        target.getTablePath(),
                                                        physical.tableInfo.getTableId(),
                                                        owner))
                                        .getValue();
                        if (!canAlter
                                && !(creator.getName().equals(transaction.getCreatorName())
                                        && creator.getType()
                                                .equals(transaction.getCreatorType()))) {
                            throw invalid("BulkLoad target is occupied by another transaction.");
                        }
                        response.setStatus(toStatus(transaction));
                    }
                    return response;
                });
    }

    /** Freezes common input and waits for all current leaders to restore it. */
    public CompletableFuture<CommitBulkLoadResponse> commit(
            BulkLoadHandle handle, @Nullable String digest) {
        PendingBulkLoad work = pending.get(handle);
        if (work == null) {
            if (!capacity.tryAcquire()) {
                return FutureUtils.completedExceptionally(
                        invalid("BulkLoad active work limit reached."));
            }
            work = new PendingBulkLoad();
            work.lifetime = new WorkLifetime(capacity);
            pending.put(handle, work);
        }
        if (work.inputRequested != null && digest != null && !work.inputRequested.equals(digest)) {
            return FutureUtils.completedExceptionally(
                    invalid("Commit conflicts with the frozen input."));
        }
        if (digest != null) {
            work.inputRequested = digest;
        }
        if (work.result == null) {
            work.result = new CompletableFuture<>();
        }
        CompletableFuture<BulkLoadTransaction> result = work.result;
        work.commitRequested = true;
        advance(handle);
        return result.thenApply(
                transaction -> {
                    if (transaction.getState() != BulkLoadState.COMMITTED) {
                        throw invalid("BulkLoad transaction is aborted.");
                    }
                    if (digest != null && !digest.equals(transaction.getInputSha256())) {
                        throw invalid("Commit conflicts with the frozen input.");
                    }
                    return new CommitBulkLoadResponse().setStatus(toStatus(transaction));
                });
    }

    /** Persists Abort independently of outstanding read-only validation. */
    public CompletableFuture<AbortBulkLoadResponse> abort(BulkLoadHandle handle) {
        return abort(handle, BulkLoadAbortReason.ABORTED_BY_CALLER);
    }

    private CompletableFuture<AbortBulkLoadResponse> abort(
            BulkLoadHandle handle, BulkLoadAbortReason reason) {
        PendingBulkLoad existing = pending.get(handle);
        PendingBulkLoad work = existing == null ? new PendingBulkLoad() : existing;
        if (work.abortResult != null) {
            return work.abortResult;
        }
        if (existing == null) {
            admitting.add(work);
        }
        work.abortRequested = true;
        int epoch = coordinatorEpochVersion();
        work.abortResult = new CompletableFuture<>();
        CompletableFuture<AbortBulkLoadResponse> response = work.abortResult;
        io(
                        work,
                        () -> {
                            Versioned<BulkLoadTransaction> current =
                                    readTransactionVersioned(handle);
                            BulkLoadTransaction value = current.getValue();
                            if (value.getState() == BulkLoadState.ABORTED) {
                                return current;
                            }
                            if (value.getState() != BulkLoadState.BEGUN) {
                                throw invalid("BulkLoad Abort requires BEGUN.");
                            }
                            return metadataStore.update(
                                    current,
                                    value.aborted(reason, null, expiry()),
                                    readOwnedRegistrations(value),
                                    null,
                                    epoch);
                        })
                .whenComplete(
                        (finished, failure) -> {
                            if (closed) {
                                response.completeExceptionally(invalid("Coordinator epoch ended."));
                                return;
                            }
                            onEvent(
                                    () -> {
                                        admitting.remove(work);
                                        work.abortResult = null;
                                        if (failure != null) {
                                            work.abortRequested = false;
                                            response.completeExceptionally(unwrap(failure));
                                            return;
                                        }
                                        completeTerminal(handle, work, finished.getValue());
                                        response.complete(
                                                new AbortBulkLoadResponse()
                                                        .setStatus(toStatus(finished.getValue())));
                                    });
                        });
        return response;
    }

    /** Retries recoverable work and collects retained terminal results. */
    public void process(BulkLoadMaintenanceEvent event) {
        resumeActiveTransactions();
        resultGc.runMaintenance(System.currentTimeMillis(), coordinatorEpochVersion());
    }

    void advance(BulkLoadHandle handle) {
        PendingBulkLoad work = pending.get(handle);
        if (closed || work == null || work.inFlight != null) {
            return;
        }
        if (work.lifetime == null) {
            if (!capacity.tryAcquire()) {
                return;
            }
            work.lifetime = new WorkLifetime(capacity);
        }
        Object operation = new Object();
        work.inFlight = operation;
        int epoch = coordinatorEpochVersion();
        int coordinatorEpoch = coordinatorContext.getCoordinatorEpoch();
        String requestedInput = work.inputRequested;
        boolean requestedCommit = work.commitRequested;
        // Own the complete chain, including gaps between I/O tasks and outstanding sends.
        work.lifetime.tasks.incrementAndGet();
        io(
                        work,
                        () ->
                                prepareAndConfirm(
                                        handle,
                                        work,
                                        requestedInput,
                                        requestedCommit,
                                        epoch,
                                        coordinatorEpoch))
                .thenCompose(Function.identity())
                .whenComplete(
                        (finished, failure) -> {
                            work.lifetime.taskExited();
                            onEvent(
                                    () -> {
                                        if (!isCurrent(handle, work, operation)) {
                                            return;
                                        }
                                        work.inFlight = null;
                                        if (work.terminalResult != null) {
                                            completeTerminal(handle, work, work.terminalResult);
                                        } else if (failure == null) {
                                            completeTerminal(handle, work, finished.getValue());
                                        } else {
                                            if (!retryable(failure)) {
                                                failWaiters(work, unwrap(failure));
                                            }
                                            if (ExceptionUtils.findThrowable(
                                                            failure,
                                                            BulkLoadNotFoundException.class)
                                                    .isPresent()) {
                                                pending.remove(handle, work);
                                                work.lifetime.finish();
                                            }
                                        }
                                    });
                        });
    }

    // Called on the I/O thread. The future covers confirmation and terminal I/O.
    private CompletableFuture<Versioned<BulkLoadTransaction>> prepareAndConfirm(
            BulkLoadHandle handle,
            PendingBulkLoad work,
            String requestedInput,
            boolean requestedCommit,
            int epoch,
            int coordinatorEpoch)
            throws Exception {
        Versioned<BulkLoadTransaction> current = readTransactionVersioned(handle);
        BulkLoadTransaction value = current.getValue();
        work.preCommitDeadline =
                value.getState() == BulkLoadState.BEGUN ? value.getDeadlineMs() : Long.MAX_VALUE;
        if (value.getState().isTerminal()) {
            return CompletableFuture.completedFuture(current);
        }
        List<Versioned<?>> registrations = readOwnedRegistrations(value);
        if (value.getState() == BulkLoadState.BEGUN) {
            BulkLoadAbortReason expired = deadlineReason(value, System.currentTimeMillis());
            if (expired != null) {
                return CompletableFuture.completedFuture(
                        metadataStore.update(
                                current,
                                value.aborted(expired, null, expiry()),
                                registrations,
                                null,
                                epoch));
            }
            if (requestedInput != null) {
                if (value.getInputSha256() != null
                        && !value.getInputSha256().equals(requestedInput)) {
                    throw invalid("Commit conflicts with the frozen input.");
                }
                if (value.getInputSha256() == null) {
                    current =
                            metadataStore.update(
                                    current,
                                    value.withInput(requestedInput),
                                    registrations,
                                    null,
                                    epoch);
                    value = current.getValue();
                }
            }
            if (value.getInputSha256() == null) {
                if (requestedCommit) {
                    throw invalid("Commit requires an input digest.");
                }
                return CompletableFuture.completedFuture(current);
            }
        }
        TableInfo info = metadataManager.getTable(handle.getTablePath());
        if (info.getTableId() != handle.getTableId()) {
            throw invalid("BulkLoad table identity changed.");
        }
        int schemaId = metadataManager.getLatestSchema(handle.getTablePath()).getSchemaId();
        for (BulkLoadTarget target : value.getTargets()) {
            if (schemaId != target.getSchemaId()) {
                throw invalid("BulkLoad schema changed.");
            }
        }
        if (work.snapshotsToAdopt == null) {
            work.snapshotsToAdopt =
                    new ArrayDeque<>(
                            new BulkLoadSnapshotValidator()
                                    .validate(
                                            value,
                                            info.getTableConfig().getChangelogImage(),
                                            configuration
                                                    .get(
                                                            ConfigOptions
                                                                    .BULKLOAD_SNAPSHOT_METADATA_MAX_SIZE)
                                                    .getBytes()));
        }
        Versioned<BulkLoadTransaction> validated = current;
        current = readTransactionVersioned(handle);
        value = current.getValue();
        if (value.getState().isTerminal()) {
            return CompletableFuture.completedFuture(current);
        }
        if (current.getVersion() != validated.getVersion()) {
            throw invalid("BulkLoad changed during validation.");
        }
        registrations = readOwnedRegistrations(value);
        if (value.getState() == BulkLoadState.BEGUN) {
            if (work.abortRequested) {
                return CompletableFuture.completedFuture(current);
            }
            BulkLoadAbortReason expired = deadlineReason(value, System.currentTimeMillis());
            if (expired != null) {
                return CompletableFuture.completedFuture(
                        metadataStore.update(
                                current,
                                value.aborted(expired, null, expiry()),
                                registrations,
                                null,
                                epoch));
            }
            current = metadataStore.update(current, value.committing(), registrations, null, epoch);
            value = current.getValue();
        }
        if (value.getState() != BulkLoadState.COMMITTING) {
            throw invalid("BulkLoad requires a durable commit decision.");
        }
        work.preCommitDeadline = Long.MAX_VALUE;
        Versioned<BulkLoadTransaction> decision = current;
        Map<Long, Versioned<?>> registrationsByPartition = new HashMap<>();
        for (Versioned<?> registration : registrations) {
            Long partitionId =
                    registration.getValue() instanceof PartitionRegistration
                            ? ((PartitionRegistration) registration.getValue()).getPartitionId()
                            : null;
            registrationsByPartition.put(partitionId, registration);
        }
        while (!work.snapshotsToAdopt.isEmpty()) {
            if (closed) {
                throw invalid("Coordinator epoch ended.");
            }
            CompletedSnapshot snapshot = work.snapshotsToAdopt.peekFirst();
            completedSnapshotStoreManager
                    .getOrCreateCompletedSnapshotStore(
                            handle.getTablePath(), snapshot.getTableBucket())
                    .registerExternalSnapshot(
                            snapshot,
                            () ->
                                    metadataStore.adoptBucketMetadata(
                                            decision,
                                            registrationsByPartition.get(
                                                    snapshot.getTableBucket().getPartitionId()),
                                            snapshot.getTableBucket(),
                                            new BucketSnapshot(
                                                    snapshot.getSnapshotID(),
                                                    snapshot.getLogOffset(),
                                                    snapshot.getMetadataFilePath().toString()),
                                            epoch));
            work.snapshotsToAdopt.removeFirst();
        }
        ReplicaConvergenceContext round = readRound(value);
        return convergence
                .apply(value, round, coordinatorEpoch)
                .thenCompose(
                        ignored ->
                                io(
                                        work,
                                        () ->
                                                metadataStore.update(
                                                        decision,
                                                        decision.getValue().committed(expiry()),
                                                        readOwnedRegistrations(decision.getValue()),
                                                        round,
                                                        epoch)));
    }

    private ReplicaConvergenceContext readRound(BulkLoadTransaction value) throws Exception {
        List<Versioned<? extends TableAssignment>> assignments = new ArrayList<>();
        Map<TableBucket, Versioned<LeaderAndIsr>> leaders = new HashMap<>();
        Map<Integer, Versioned<TabletServerRegistration>> holders = new HashMap<>();
        for (BulkLoadTarget target : value.getTargets()) {
            Versioned<? extends TableAssignment> assignment =
                    readAssignment(value.getTableId(), target.getPartitionId());
            long[] snapshotIds = target.getSnapshotIds();
            requireCompleteAssignment(
                    value.getTableId(),
                    target.getPartitionId(),
                    assignment.getValue(),
                    snapshotIds.length);
            assignments.add(assignment);
            for (int id = 0; id < snapshotIds.length; id++) {
                TableBucket bucket =
                        new TableBucket(value.getTableId(), target.getPartitionId(), id);
                Versioned<LeaderAndIsr> leader =
                        readOptional(
                                        ZkData.LeaderAndIsrZNode.path(bucket),
                                        ZkData.LeaderAndIsrZNode::decode)
                                .orElseThrow(
                                        () ->
                                                new org.apache.fluss.exception.TimeoutException(
                                                        "Current leader is not available."));
                int serverId = leader.getValue().leader();
                if (!assignment
                        .getValue()
                        .getBucketAssignment(id)
                        .getReplicas()
                        .contains(serverId)) {
                    throw new org.apache.fluss.exception.TimeoutException(
                            "Current leader is outside the observed assignment.");
                }
                if (!holders.containsKey(serverId)) {
                    Versioned<TabletServerRegistration> server =
                            readOptional(
                                            ZkData.ServerIdZNode.path(serverId),
                                            ZkData.ServerIdZNode::decode)
                                    .orElseThrow(
                                            () ->
                                                    new org.apache.fluss.exception.TimeoutException(
                                                            "Current leader session is not available."));
                    holders.put(serverId, server);
                }
                leaders.put(bucket, leader);
            }
        }
        return new ReplicaConvergenceContext(
                assignments,
                leaders,
                holders,
                metadataManager.getTable(value.getHandle().getTablePath()).getBucketCountEpoch());
    }

    private boolean isCurrent(BulkLoadHandle handle, PendingBulkLoad work, Object operation) {
        return !closed && pending.get(handle) == work && work.inFlight == operation;
    }

    private void completeTerminal(
            BulkLoadHandle handle, PendingBulkLoad work, BulkLoadTransaction value) {
        if (!value.getState().isTerminal()) {
            return;
        }
        work.terminalResult = value;
        if (work.result != null) {
            work.result.complete(value);
            work.result = null;
        }
        if (work.lifetime != null) {
            work.lifetime.finish();
        }
        // A running stage still owns its capacity and its callback identity.
        if (work.inFlight == null) {
            pending.remove(handle, work);
        }
    }

    private void failWaiters(PendingBulkLoad work, Throwable failure) {
        if (work.result != null) {
            work.result.completeExceptionally(failure);
            work.result = null;
        }
    }

    private long expiry() {
        return Math.addExact(
                System.currentTimeMillis(),
                configuration.get(ConfigOptions.BULKLOAD_RESULT_RETENTION).toMillis());
    }

    static @Nullable BulkLoadAbortReason deadlineReason(BulkLoadTransaction value, long now) {
        return value.getState() == BulkLoadState.BEGUN && now >= value.getDeadlineMs()
                ? BulkLoadAbortReason.DEADLINE_EXCEEDED
                : null;
    }

    private int coordinatorEpochVersion() {
        return coordinatorContext.getCoordinatorZkVersion();
    }

    private <T> CompletableFuture<T> io(@Nullable PendingBulkLoad work, Callable<T> action) {
        WorkLifetime lifetime = work == null ? null : work.lifetime;
        if (lifetime != null) {
            lifetime.tasks.incrementAndGet();
        }
        try {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            if (closed) {
                                throw invalid("Coordinator epoch ended.");
                            }
                            return action.call();
                        } catch (Exception failure) {
                            throw new CompletionException(failure);
                        } finally {
                            if (lifetime != null) {
                                lifetime.taskExited();
                            }
                        }
                    },
                    ioExecutor);
        } catch (RuntimeException rejected) {
            if (lifetime != null) {
                lifetime.taskExited();
            }
            return FutureUtils.completedExceptionally(rejected);
        }
    }

    private void onEvent(Runnable action) {
        if (!closed) {
            eventManager.put(
                    new AccessContextEvent<Void>(
                            ignored -> {
                                action.run();
                                return null;
                            }));
        }
    }

    private static boolean retryable(Throwable failure) {
        return ExceptionUtils.findThrowable(
                                failure, org.apache.fluss.exception.RetriableException.class)
                        .isPresent()
                || ExceptionUtils.findThrowable(
                                failure, java.util.concurrent.TimeoutException.class)
                        .isPresent()
                || ExceptionUtils.findThrowable(failure, java.io.IOException.class).isPresent()
                || ExceptionUtils.findThrowable(
                                failure,
                                org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper
                                        .KeeperException.class)
                        .isPresent();
    }

    private static Throwable unwrap(Throwable failure) {
        return ExceptionUtils.stripCompletionException(failure);
    }

    private static InvalidBulkLoadRequestException invalid(String message) {
        return new InvalidBulkLoadRequestException(message);
    }

    Versioned<BulkLoadTransaction> readTransactionVersioned(BulkLoadHandle handle) {
        return readTransaction(handle)
                .orElseThrow(
                        () ->
                                new BulkLoadNotFoundException(
                                        "BulkLoad transaction does not exist."));
    }

    Optional<Versioned<BulkLoadTransaction>> readTransaction(BulkLoadHandle handle) {
        Optional<Versioned<BulkLoadTransaction>> result =
                readOptional(
                        BulkLoadMetadataStore.transactionPath(handle),
                        ZkData.BulkLoadTransactionsZNode::decode);
        if (result.isPresent() && !result.get().getValue().getHandle().equals(handle)) {
            throw invalid("BulkLoad handle does not match the persisted transaction.");
        }
        return result;
    }

    public static BulkLoadTransaction readTransaction(
            ZooKeeperClient client, BulkLoadHandle handle) {
        try {
            BulkLoadTransaction value =
                    new BulkLoadMetadataStore(client)
                            .readOptional(
                                    BulkLoadMetadataStore.transactionPath(handle),
                                    ZkData.BulkLoadTransactionsZNode::decode)
                            .orElseThrow(
                                    () ->
                                            new BulkLoadNotFoundException(
                                                    "BulkLoad transaction does not exist."))
                            .getValue();
            if (!value.getHandle().equals(handle)) {
                throw invalid("BulkLoad handle does not match the persisted transaction.");
            }
            return value;
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new FlussRuntimeException("Failed to read BulkLoad transaction.", failure);
        }
    }

    List<Versioned<?>> readOwnedRegistrations(BulkLoadTransaction value) {
        List<Versioned<?>> registrations = new ArrayList<>();
        for (BulkLoadTarget target : value.getTargets()) {
            Versioned<?> registration = readRegistration(target.getTarget());
            requireControl(value, target, registration);
            registrations.add(registration);
        }
        return registrations;
    }

    static void requireControl(
            BulkLoadTransaction value, BulkLoadTarget target, Versioned<?> registration) {
        Object metadata = registration.getValue();
        boolean identity =
                metadata instanceof TableRegistration
                        ? target.getPartitionId() == null
                                && ((TableRegistration) metadata).tableId == value.getTableId()
                        : target.getPartitionId() != null
                                && ((PartitionRegistration) metadata).getTableId()
                                        == value.getTableId()
                                && ((PartitionRegistration) metadata).getPartitionId()
                                        == target.getPartitionId();
        if (!identity
                || !target.getMetadataPath().equals(registration.getPath())
                || target.getRegistrationVersion() != registration.getVersion()
                || !value.getBulkLoadId().equals(BulkLoadMetadataStore.owner(metadata))) {
            throw invalid("BulkLoad target identity, ownership or version changed.");
        }
    }

    private Versioned<?> readRegistration(PhysicalTablePath target) {
        return target.getPartitionName() == null
                ? requireFound(
                        ZkData.TableZNode.path(target.getTablePath()),
                        ZkData.TableZNode::decode,
                        "table registration")
                : requireFound(
                        ZkData.PartitionZNode.path(
                                target.getTablePath(), target.getPartitionName()),
                        ZkData.PartitionZNode::decode,
                        "partition registration");
    }

    private Versioned<? extends TableAssignment> readAssignment(
            long tableId, @Nullable Long partitionId) {
        return partitionId == null
                ? requireFound(
                        ZkData.TableIdZNode.path(tableId),
                        ZkData.TableIdZNode::decode,
                        "table assignment")
                : requireFound(
                        ZkData.PartitionIdZNode.path(partitionId),
                        ZkData.PartitionIdZNode::decode,
                        "partition assignment");
    }

    <T> Versioned<T> requireFound(String path, Function<byte[], T> decoder, String description) {
        return readOptional(path, decoder)
                .orElseThrow(() -> invalid("Missing BulkLoad " + description + '.'));
    }

    private <T> Optional<Versioned<T>> readOptional(String path, Function<byte[], T> decoder) {
        try {
            return metadataStore.readOptional(path, decoder);
        } catch (Exception failure) {
            throw new InvalidBulkLoadRequestException(
                    "Unreadable BulkLoad metadata " + path, failure);
        }
    }

    private static void requireCompleteAssignment(
            long tableId, @Nullable Long partitionId, TableAssignment assignment, int count) {
        Set<Integer> expected = new HashSet<>();
        for (int id = 0; id < count; id++) {
            expected.add(id);
        }
        if (!assignment.getBuckets().equals(expected)
                || (partitionId != null
                        && (!(assignment instanceof PartitionAssignment)
                                || ((PartitionAssignment) assignment).getTableId() != tableId))) {
            throw invalid("BulkLoad assignment does not cover its physical target.");
        }
    }

    private PhysicalTarget readPhysicalTarget(PhysicalTablePath path) throws Exception {
        TableInfo info = metadataManager.getTable(path.getTablePath());
        Versioned<?> registration = readRegistration(path);
        Long partitionId = null;
        long tableId;
        String remoteDataDir;
        if (registration.getValue() instanceof TableRegistration) {
            TableRegistration table = (TableRegistration) registration.getValue();
            tableId = table.tableId;
            remoteDataDir = table.remoteDataDir;
        } else {
            PartitionRegistration partition = (PartitionRegistration) registration.getValue();
            tableId = partition.getTableId();
            partitionId = partition.getPartitionId();
            remoteDataDir = partition.getRemoteDataDir();
            info =
                    new TableInfo(
                            info.getTablePath(),
                            info.getTableId(),
                            info.getSchemaId(),
                            info.getSchema(),
                            info.getBucketKeys(),
                            info.getPartitionKeys(),
                            partition.getBucketCountOrDefault(
                                    info.getNumBuckets(), info.getBucketCountEpoch()),
                            info.getProperties(),
                            info.getCustomProperties(),
                            remoteDataDir,
                            info.getComment().orElse(null),
                            info.getCreatedTime(),
                            info.getModifiedTime(),
                            info.getBucketCountEpoch());
        }
        if (tableId != info.getTableId()) {
            throw new InvalidBulkLoadRequestException("BulkLoad physical target identity changed.");
        }
        return new PhysicalTarget(info, registration, partitionId, remoteDataDir);
    }

    private static final class PhysicalTarget {
        private final TableInfo tableInfo;
        private final Versioned<?> registration;
        private final Long partitionId;
        private final String remoteDataDir;

        private PhysicalTarget(
                TableInfo tableInfo,
                Versioned<?> registration,
                Long partitionId,
                String remoteDataDir) {
            this.tableInfo = tableInfo;
            this.registration = registration;
            this.partitionId = partitionId;
            this.remoteDataDir = remoteDataDir;
        }
    }

    private void requireClusterCapabilities() throws Exception {
        List<String> coordinatorIds = zkClient.getCoordinatorServerList();
        if (coordinatorIds.isEmpty()) {
            throw new UnsupportedVersionException(
                    "Every Coordinator must cover the BulkLoad capability set.");
        }
        for (String id : coordinatorIds) {
            CoordinatorAddress registration =
                    requireFound(
                                    ZkData.CoordinatorIdZNode.path(id),
                                    ZkData.CoordinatorIdZNode::decode,
                                    "Coordinator registration")
                            .getValue();
            if (registration.getServerProtocolVersion() < ServerProtocolVersion.BULK_LOAD) {
                throw new UnsupportedVersionException(
                        "Every Coordinator must cover the BulkLoad capability set.");
            }
        }
        int[] tabletIds = zkClient.getSortedTabletServerList();
        if (tabletIds.length == 0) {
            throw new UnsupportedVersionException(
                    "Every TabletServer must cover the BulkLoad capability set.");
        }
        for (int id : tabletIds) {
            TabletServerRegistration registration =
                    requireFound(
                                    ZkData.ServerIdZNode.path(id),
                                    ZkData.ServerIdZNode::decode,
                                    "TabletServer registration")
                            .getValue();
            if (registration.getServerProtocolVersion() < ServerProtocolVersion.BULK_LOAD) {
                throw new UnsupportedVersionException(
                        "Every TabletServer must cover the BulkLoad capability set.");
            }
        }
    }

    private static PbBulkLoadHandle toPbHandle(BulkLoadHandle handle) {
        return new PbBulkLoadHandle()
                .setTablePath(
                        new PbTablePath()
                                .setDatabaseName(handle.getTablePath().getDatabaseName())
                                .setTableName(handle.getTablePath().getTableName()))
                .setTableId(handle.getTableId())
                .setBulkLoadId(handle.getBulkLoadId());
    }

    private static PbBulkLoadStatus toStatus(BulkLoadTransaction value) {
        PbBulkLoadStatus status =
                new PbBulkLoadStatus()
                        .setHandle(toPbHandle(value.getHandle()))
                        .setState(value.getState().getCode());
        if (value.getAbortReason() != null) {
            status.setAbortReason(value.getAbortReason().getCode());
        }
        if (value.getAbortMessage() != null) {
            status.setAbortMessage(value.getAbortMessage());
        }
        return status;
    }

    private static PbBulkLoadTargetInfo toTargetInfo(BulkLoadTarget target, TableInfo info) {
        PbTableInfo tableInfo =
                new PbTableInfo()
                        .setTableId(info.getTableId())
                        .setSchemaId(target.getSchemaId())
                        .setTableJson(info.toTableDescriptor().toJsonBytes())
                        .setCreatedTime(info.getCreatedTime())
                        .setModifiedTime(info.getModifiedTime())
                        .setRemoteDataDir(target.getRemoteDataDir())
                        .setBucketCountEpoch(info.getBucketCountEpoch());
        PbBulkLoadTargetInfo result =
                new PbBulkLoadTargetInfo()
                        .setTarget(fromPhysicalTablePath(target.getTarget()))
                        .setTableInfo(tableInfo)
                        .setSnapshotIds(target.getSnapshotIds());
        if (target.getPartitionId() != null) {
            result.setPartitionId(target.getPartitionId());
        }
        return result;
    }

    private static final class PendingBulkLoad {
        private BulkLoadTransaction terminalResult;
        private BulkLoadHandle attemptedHandle;
        private volatile long preCommitDeadline = Long.MAX_VALUE;
        private WorkLifetime lifetime;
        private Object inFlight;
        private volatile boolean abortRequested;
        private boolean commitRequested;
        private String inputRequested;
        private CompletableFuture<BulkLoadTransaction> result;
        private CompletableFuture<BeginBulkLoadResponse> beginResult;
        private CompletableFuture<AbortBulkLoadResponse> abortResult;

        // Owned by the one in-flight I/O chain. Null means full validation has not succeeded.
        // A failed or uncertain adoption retains the head for idempotent replay.
        private Deque<CompletedSnapshot> snapshotsToAdopt;
    }

    private static final class WorkLifetime {
        private final Semaphore capacity;
        private final AtomicInteger tasks = new AtomicInteger();
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicBoolean released = new AtomicBoolean();

        private WorkLifetime(Semaphore capacity) {
            this.capacity = capacity;
        }

        private void finish() {
            finished.set(true);
            release();
        }

        private void taskExited() {
            tasks.decrementAndGet();
            release();
        }

        private void release() {
            if (finished.get() && tasks.get() == 0 && released.compareAndSet(false, true)) {
                capacity.release();
            }
        }
    }

    private static final class BeginResult {
        private final Versioned<BulkLoadTransaction> transaction;
        private final BeginBulkLoadResponse response;

        private BeginResult(
                Versioned<BulkLoadTransaction> transaction, BeginBulkLoadResponse response) {
            this.transaction = transaction;
            this.response = response;
        }
    }
}
