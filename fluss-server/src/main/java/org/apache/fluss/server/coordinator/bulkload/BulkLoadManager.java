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
import org.apache.fluss.exception.CorruptMessageException;
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
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.CoordinatorChannelManager;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.RegisteredServer;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadReplicaConvergence.ReplicaRound;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.BulkLoadMaintenanceEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.DataWithStat;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.ServerProtocolVersion;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
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
import java.util.function.Function;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.fromPhysicalTablePath;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Coordinator event-thread owner of the metadata-only BulkLoad lifecycle. */
@Internal
public final class BulkLoadManager {

    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadManager.class);

    private final ZooKeeperClient zkClient;
    private final BulkLoadMetadataStore metadataStore;
    private final Configuration configuration;
    private final CoordinatorContext coordinatorContext;
    private final MetadataManager metadataManager;
    private final ExecutorService ioExecutor;
    private final EventManager eventManager;
    private final Map<Integer, RegisteredServer> readyRegistrations = new HashMap<>();
    private final BulkLoadReplicaConvergence convergence;
    private final BulkLoadResultGc resultGc;
    private final CompletedSnapshotStoreManager completedSnapshotStoreManager;
    private final Map<BulkLoadHandle, PendingBulkLoad> pending = new HashMap<>();

    /** Creates the production metadata-only manager. */
    public BulkLoadManager(
            ZooKeeperClient zkClient,
            Configuration configuration,
            CoordinatorContext coordinatorContext,
            MetadataManager metadataManager,
            CoordinatorChannelManager channelManager,
            ExecutorService ioExecutor,
            EventManager eventManager,
            CompletedSnapshotStoreManager completedSnapshotStoreManager) {
        this.zkClient = checkNotNull(zkClient);
        this.metadataStore = new BulkLoadMetadataStore(zkClient);
        this.configuration = checkNotNull(configuration);
        this.coordinatorContext = checkNotNull(coordinatorContext);
        this.metadataManager = checkNotNull(metadataManager);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.eventManager = checkNotNull(eventManager);
        this.completedSnapshotStoreManager = checkNotNull(completedSnapshotStoreManager);
        this.convergence = new BulkLoadReplicaConvergence(coordinatorContext, channelManager);
        this.resultGc = new BulkLoadResultGc(zkClient, ioExecutor);
    }

    /** Restores owned transactions before the ordinary replica state machines start. */
    public void restoreTransactions(List<BulkLoadHandle> handles) {
        pending.clear();
        for (BulkLoadHandle handle : handles) {
            recordActiveTransaction(readTransactionVersioned(handle));
        }
    }

    /** Resumes recovered transactions after ordinary replica state has been initialized. */
    public void resumeRecoveredTransactions() throws Exception {
        for (BulkLoadHandle handle : new ArrayList<>(pending.keySet())) {
            advance(handle);
        }
    }

    Versioned<BulkLoadTransaction> recordActiveTransaction(
            Versioned<BulkLoadTransaction> transaction) {
        pending.computeIfAbsent(
                                transaction.getValue().getHandle(),
                                ignored -> new PendingBulkLoad())
                        .transaction =
                transaction;
        return transaction;
    }

    /** Replays current persisted state independently for every active target. */
    public void resumeActiveTransactions() {
        for (BulkLoadHandle handle : new ArrayList<>(pending.keySet())) {
            try {
                advance(handle);
            } catch (Exception failure) {
                LOG.warn("Failed to advance active BulkLoad {}.", handle, failure);
            }
        }
    }

    /**
     * Resumes a transaction from its persisted decision and target metadata.
     *
     * <p>This is the only entry point every trigger uses: the Begin, Commit and Abort requests, the
     * completion of an asynchronous round, a TabletServer coming or going, the maintenance tick,
     * and Coordinator startup recovery. A transaction with an outstanding operation is left alone,
     * so a trigger can never start a second round for the same target.
     */
    void advance(BulkLoadHandle handle) throws Exception {
        PendingBulkLoad work = pending.computeIfAbsent(handle, ignored -> new PendingBulkLoad());
        if (work.inFlight != null) {
            return;
        }
        try {
            Optional<Versioned<BulkLoadTransaction>> read =
                    work.transaction == null
                            ? readTransaction(handle)
                            : Optional.of(work.transaction);
            if (!read.isPresent()) {
                pending.remove(handle);
                return;
            }
            Versioned<BulkLoadTransaction> transaction = read.get();
            recordActiveTransaction(transaction);
            BulkLoadTransaction value = transaction.getValue();
            if (value.getState().isTerminal()) {
                completeTerminalWaiters(value);
            } else if (value.getAbortReason() != null) {
                startConvergenceRound(transaction);
            } else if (value.getState() == BulkLoadState.BEGUN) {
                BulkLoadAbortReason abort = abortDecision(value);
                if (abort != null) {
                    startAbortDecision(transaction, abort, null);
                } else if (!value.isFenceReady()) {
                    startFenceRound(transaction);
                } else if (value.getInputSha256() == null) {
                    completePendingBegin(value);
                } else {
                    startValidateSnapshots(transaction);
                }
            } else if (registrationState(readRegistration(handle)).state
                    == BulkLoadDataState.LOADING) {
                startValidateSnapshots(transaction);
            } else {
                requireControl(transaction, BulkLoadDataState.ACTIVE);
                startConvergenceRound(transaction);
            }
        } catch (Exception failure) {
            invalidate(handle);
            throw failure;
        }
    }

    /** Returns the reason this transaction must abort, or null while it may still commit. */
    private @Nullable BulkLoadAbortReason abortDecision(BulkLoadTransaction transaction) {
        if (transaction.getState() != BulkLoadState.BEGUN || transaction.getAbortReason() != null) {
            return null;
        }
        PendingBulkLoad work = pending.get(transaction.getHandle());
        if (work != null && work.abortRequested) {
            return BulkLoadAbortReason.ABORTED_BY_CALLER;
        }
        return deadlineReason(transaction, System.currentTimeMillis());
    }

    /** Returns the deadline this transaction has already missed, or null while it is in time. */
    static @Nullable BulkLoadAbortReason deadlineReason(BulkLoadTransaction transaction, long now) {
        if (transaction.getState() != BulkLoadState.BEGUN
                || transaction.getAbortReason() != null
                || now < transaction.getDeadlineMs()) {
            return null;
        }
        return transaction.getInputSha256() == null
                ? BulkLoadAbortReason.BUILD_DEADLINE_EXCEEDED
                : BulkLoadAbortReason.COMMIT_DECISION_DEADLINE_EXCEEDED;
    }

    /** Binds the exact registrations whose gateways were installed during Coordinator startup. */
    public void bindStartupTabletServerGateways(List<ServerInfo> servers) {
        readyRegistrations.clear();
        for (ServerInfo server : servers) {
            bindTabletServerGateway(server);
        }
    }

    /**
     * Binds the exact current registration after its gateway is ready in the Coordinator event
     * thread.
     */
    public void bindTabletServerGateway(ServerInfo server) {
        int serverId = server.id();
        readyRegistrations.remove(serverId);
        Optional<DataWithStat> observed;
        try {
            observed = zkClient.getDataWithStatIfExists(ZkData.ServerIdZNode.path(serverId));
        } catch (Exception failure) {
            throw new FlussRuntimeException(
                    "Failed to bind the ready TabletServer registration.", failure);
        }
        if (!observed.isPresent()) {
            return;
        }
        DataWithStat data = observed.get();
        RegisteredServer current =
                new RegisteredServer(
                        serverId,
                        new Versioned<>(
                                ZkData.ServerIdZNode.decode(data.getData()),
                                ZkData.ServerIdZNode.path(serverId),
                                data.getStat().getVersion(),
                                data.getStat().getEphemeralOwner()));
        ServerInfo currentLive = coordinatorContext.getLiveTabletServers().get(serverId);
        if (currentLive == null
                || !currentLive.equals(server)
                || !BulkLoadReplicaConvergence.matchesLive(current, currentLive)) {
            return;
        }
        readyRegistrations.put(serverId, current);
    }

    /** Invalidates a registration before its live identity and gateway are removed. */
    public void unbindTabletServerGateway(int serverId) {
        readyRegistrations.remove(serverId);
    }

    /** Handles Begin on the Coordinator event thread. */
    public CompletableFuture<BeginBulkLoadResponse> begin(
            PhysicalTablePath target, @Nullable Long buildTimeoutMs, FlussPrincipal creator) {
        CompletableFuture<BeginBulkLoadResponse> future = new CompletableFuture<>();
        BulkLoadHandle attemptedHandle = null;
        try {
            requireClusterCapabilities();
            BeginAdmission admission = readBeginAdmission(target, buildTimeoutMs, creator);
            attemptedHandle = admission.transaction.getHandle();
            PendingBulkLoad work = new PendingBulkLoad();
            work.beginResult = future;
            pending.put(attemptedHandle, work);
            Versioned<BulkLoadTransaction> created =
                    metadataStore.createTransactionAndFence(
                            admission.registration,
                            admission.assignment,
                            admission.transaction,
                            coordinatorEpochVersion());
            recordActiveTransaction(created);
            advance(created.getValue().getHandle());
        } catch (Throwable failure) {
            if (attemptedHandle != null) {
                PendingBulkLoad work = pending.get(attemptedHandle);
                if (work != null) {
                    work.beginResult = null;
                }
            }
            future.completeExceptionally(failure);
        }
        return future;
    }

    /** Handles an ownership-scoped in-progress lookup on the Coordinator event thread. */
    public CompletableFuture<GetInProgressBulkLoadResponse> getInProgress(
            PhysicalTablePath target, FlussPrincipal creator, boolean canAlter) {
        CompletableFuture<GetInProgressBulkLoadResponse> future = new CompletableFuture<>();
        try {
            GetInProgressBulkLoadResponse response = new GetInProgressBulkLoadResponse();
            BulkLoadHandle existing = findExistingTarget(target);
            if (existing != null) {
                BulkLoadTransaction transaction = readTransactionVersioned(existing).getValue();
                if (!sameCreator(creator, transaction) && !canAlter) {
                    throw new InvalidBulkLoadRequestException(
                            "BulkLoad target is occupied by another transaction.");
                }
                response.setStatus(toStatus(transaction));
            }
            future.complete(response);
        } catch (Throwable failure) {
            future.completeExceptionally(failure);
        }
        return future;
    }

    /** Handles Commit on the Coordinator event thread. */
    public CompletableFuture<CommitBulkLoadResponse> commit(
            BulkLoadHandle handle, @Nullable String inputSha256) {
        try {
            Versioned<BulkLoadTransaction> transaction = readTransactionVersioned(handle);
            if (transaction.getValue().getState() == BulkLoadState.COMMITTED) {
                if (inputSha256 != null) {
                    requireSameInput(transaction.getValue(), inputSha256);
                }
                completeTerminalWaiters(transaction.getValue());
                return CompletableFuture.completedFuture(
                        new CommitBulkLoadResponse().setStatus(toStatus(transaction.getValue())));
            }
            if (transaction.getValue().getState() == BulkLoadState.BEGUN) {
                requireControl(transaction, BulkLoadDataState.LOADING);
                if (!transaction.getValue().isFenceReady()) {
                    throw new InvalidBulkLoadRequestException(
                            "BulkLoad Commit requires an established fence.");
                }
                BulkLoadAbortReason expired =
                        deadlineReason(transaction.getValue(), System.currentTimeMillis());
                if (expired != null) {
                    throw new InvalidBulkLoadRequestException(
                            "BulkLoad deadline expired before Commit: " + expired.name());
                }
                if (transaction.getValue().getAbortReason() != null) {
                    throw new InvalidBulkLoadRequestException(
                            "BulkLoad Commit conflicts with a persisted abort decision.");
                }
                if (transaction.getValue().getInputSha256() == null) {
                    if (inputSha256 == null) {
                        throw new InvalidBulkLoadRequestException(
                                "BulkLoad Commit requires an input digest.");
                    }
                    long deadline =
                            Math.addExact(
                                    System.currentTimeMillis(),
                                    configuration
                                            .get(ConfigOptions.BULKLOAD_COMMIT_DECISION_TIMEOUT)
                                            .toMillis());
                    BulkLoadTransaction frozen =
                            transaction.getValue().withInput(inputSha256, deadline);
                    transaction =
                            recordActiveTransaction(
                                    metadataStore.update(
                                            transaction,
                                            frozen,
                                            readRegistration(handle),
                                            null,
                                            coordinatorEpochVersion()));
                }
            } else if (transaction.getValue().getState() != BulkLoadState.COMMITTING) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad Commit requires BEGUN or COMMITTING.");
            }
            if (inputSha256 != null) {
                requireSameInput(transaction.getValue(), inputSha256);
            }
            return awaitTerminal(handle)
                    .thenApply(
                            finished -> {
                                if (finished.getState() == BulkLoadState.ABORTED) {
                                    throw abortedFailure(finished);
                                }
                                return new CommitBulkLoadResponse().setStatus(toStatus(finished));
                            });
        } catch (Throwable failure) {
            invalidate(handle);
            return FutureUtils.completedExceptionally(failure);
        }
    }

    /** Handles Abort on the Coordinator event thread. */
    public CompletableFuture<AbortBulkLoadResponse> abort(BulkLoadHandle handle) {
        try {
            Versioned<BulkLoadTransaction> transaction = readTransactionVersioned(handle);
            if (transaction.getValue().getState() == BulkLoadState.ABORTED) {
                completeTerminalWaiters(transaction.getValue());
                return CompletableFuture.completedFuture(
                        new AbortBulkLoadResponse().setStatus(toStatus(transaction.getValue())));
            }
            if (transaction.getValue().getState() != BulkLoadState.BEGUN) {
                throw new InvalidBulkLoadRequestException("BulkLoad Abort requires BEGUN.");
            }
            pending.get(handle).abortRequested = true;
            return awaitTerminal(handle)
                    .thenApply(
                            finished -> new AbortBulkLoadResponse().setStatus(toStatus(finished)));
        } catch (Throwable failure) {
            return FutureUtils.completedExceptionally(failure);
        }
    }

    /** Retries active transactions and collects expired terminal results. */
    public void process(BulkLoadMaintenanceEvent event) {
        resumeActiveTransactions();
        resultGc.runMaintenance(
                System.currentTimeMillis(), coordinatorContext.getCoordinatorZkVersion());
    }

    private void startFenceRound(Versioned<BulkLoadTransaction> transaction) throws Exception {
        BulkLoadHandle handle = transaction.getValue().getHandle();
        requireControl(transaction, BulkLoadDataState.LOADING);
        Versioned<? extends TableAssignment> assignment = readAssignment(handle);
        ReplicaRound round = registrationsForRound(assignment, true);
        if (round == null) {
            return;
        }
        int bucketCount = targetBucketCount(handle);
        await(
                transaction,
                convergence
                        .apply(transaction.getValue(), round, BulkLoadDataState.LOADING, true)
                        .thenCompose(
                                ignored ->
                                        io(
                                                () -> {
                                                    proveRemoteEmpty(
                                                            handle,
                                                            assignment.getValue(),
                                                            bucketCount);
                                                    return allocateSnapshotIds(handle, bucketCount);
                                                })),
                round,
                true,
                (current, snapshotIds) -> {
                    Versioned<BulkLoadTransaction> ready =
                            recordActiveTransaction(
                                    metadataStore.update(
                                            current,
                                            current.getValue().withSnapshotIds(snapshotIds),
                                            readRegistration(handle),
                                            round,
                                            coordinatorEpochVersion()));
                    completePendingBegin(ready.getValue());
                });
    }

    private void startValidateSnapshots(Versioned<BulkLoadTransaction> transaction)
            throws Exception {
        requireControl(transaction, BulkLoadDataState.LOADING);
        BulkLoadTransaction value = transaction.getValue();
        BulkLoadHandle handle = value.getHandle();
        TableInfo tableInfo = tableInfo(handle);
        long maxMetadataBytes =
                configuration.get(ConfigOptions.BULKLOAD_SNAPSHOT_METADATA_MAX_SIZE).getBytes();
        await(
                transaction,
                io(
                        () ->
                                new BulkLoadSnapshotValidator()
                                        .validate(
                                                handle,
                                                value.getRemoteDataDir(),
                                                value.getInputSha256(),
                                                checkNotNull(value.getSnapshotIds()),
                                                tableInfo.getTableConfig().getChangelogImage(),
                                                maxMetadataBytes)),
                null,
                false,
                (current, snapshots) -> {
                    Versioned<?> registration = readRegistration(handle);
                    requireControl(current.getValue(), registration, BulkLoadDataState.LOADING);
                    if (current.getValue().getState() == BulkLoadState.BEGUN) {
                        current =
                                recordActiveTransaction(
                                        metadataStore.update(
                                                current,
                                                current.getValue().committing(),
                                                registration,
                                                null,
                                                coordinatorEpochVersion()));
                    }
                    installBuckets(current, registration, snapshots);
                });
    }

    private void installBuckets(
            Versioned<BulkLoadTransaction> transaction,
            Versioned<?> registration,
            List<CompletedSnapshot> snapshots) {
        BulkLoadHandle handle = transaction.getValue().getHandle();
        int epochVersion = coordinatorEpochVersion();
        await(
                transaction,
                io(
                        () -> {
                            for (CompletedSnapshot snapshot : snapshots) {
                                completedSnapshotStoreManager
                                        .getOrCreateCompletedSnapshotStore(
                                                handle.getTarget().getTablePath(),
                                                snapshot.getTableBucket())
                                        .registerExternalSnapshot(
                                                snapshot,
                                                () ->
                                                        metadataStore.adoptBucketMetadata(
                                                                transaction,
                                                                registration,
                                                                snapshot.getTableBucket()
                                                                        .getBucket(),
                                                                new BucketSnapshot(
                                                                        snapshot.getSnapshotID(),
                                                                        snapshot.getLogOffset(),
                                                                        snapshot.getMetadataFilePath()
                                                                                .toString()),
                                                                epochVersion));
                            }
                            return null;
                        }),
                null,
                false,
                (current, ignored) -> {
                    requireControl(current, BulkLoadDataState.LOADING);
                    startConvergenceRound(current);
                });
    }

    private void startConvergenceRound(Versioned<BulkLoadTransaction> transaction)
            throws Exception {
        BulkLoadTransaction value = transaction.getValue();
        BulkLoadHandle handle = value.getHandle();
        Versioned<? extends TableAssignment> assignment = readAssignment(handle);
        ReplicaRound confirmation = registrationsForRound(assignment, false);
        if (confirmation == null) {
            return;
        }
        BulkLoadDataState targetState = registrationState(readRegistration(handle)).state;
        await(
                transaction,
                convergence.apply(value, confirmation, targetState, false),
                confirmation,
                false,
                (current, ignored) -> {
                    Versioned<?> registration = readRegistration(handle);
                    requireControl(current.getValue(), registration, targetState);
                    BulkLoadTransaction next =
                            targetState == BulkLoadDataState.LOADING
                                    ? current.getValue()
                                            .activated(
                                                    registration.getVersion() + 1,
                                                    current.getValue().getAbortReason(),
                                                    current.getValue().getAbortMessage())
                                    : current.getValue()
                                            .released(
                                                    registration.getVersion() + 1,
                                                    Math.addExact(
                                                            System.currentTimeMillis(),
                                                            configuration
                                                                    .get(
                                                                            ConfigOptions
                                                                                    .BULKLOAD_RESULT_RETENTION)
                                                                    .toMillis()));
                    recordActiveTransaction(
                            metadataStore.update(
                                    current,
                                    next,
                                    registration,
                                    confirmation,
                                    coordinatorEpochVersion()));
                    advance(handle);
                });
    }

    /** Persists the abort decision and the rollback to ordinary ACTIVE metadata in one write. */
    private void startAbortDecision(
            Versioned<BulkLoadTransaction> transaction,
            BulkLoadAbortReason reason,
            @Nullable String lastError)
            throws Exception {
        BulkLoadHandle handle = transaction.getValue().getHandle();
        Versioned<?> registration = readRegistration(handle);
        requireControl(transaction.getValue(), registration, BulkLoadDataState.LOADING);
        recordActiveTransaction(
                metadataStore.update(
                        transaction,
                        transaction
                                .getValue()
                                .activated(registration.getVersion() + 1, reason, lastError),
                        registration,
                        null,
                        coordinatorEpochVersion()));
        advance(handle);
    }

    private void completeTerminalWaiters(BulkLoadTransaction finished) {
        PendingBulkLoad work = pending.remove(finished.getHandle());
        if (work == null) {
            return;
        }
        if (finished.getState() == BulkLoadState.ABORTED && work.beginResult != null) {
            work.beginResult.completeExceptionally(abortedFailure(finished));
            work.beginResult = null;
        }
        if (work.terminalResult != null) {
            work.terminalResult.complete(finished);
        }
    }

    private static InvalidBulkLoadRequestException abortedFailure(BulkLoadTransaction finished) {
        return new InvalidBulkLoadRequestException(
                "BulkLoad aborted while the request was pending: "
                        + finished.getAbortReason()
                        + (finished.getAbortMessage() == null
                                ? ""
                                : ": " + finished.getAbortMessage()));
    }

    private <T> CompletableFuture<T> io(Callable<T> operation) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return operation.call();
                    } catch (Exception failure) {
                        throw new CompletionException(failure);
                    }
                },
                ioExecutor);
    }

    /** Rechecks durable state and replica observations before continuing on the event thread. */
    private <T> void await(
            Versioned<BulkLoadTransaction> observed,
            CompletableFuture<T> completion,
            @Nullable ReplicaRound confirmation,
            boolean provingEmpty,
            Continuation<T> continuation) {
        BulkLoadHandle handle = observed.getValue().getHandle();
        Object operation = new Object();
        PendingBulkLoad work = pending.get(handle);
        work.inFlight = operation;
        completion.whenComplete(
                (result, failure) ->
                        eventManager.put(
                                new AccessContextEvent<Void>(
                                        ignored -> {
                                            if (pending.get(handle) != work
                                                    || work.inFlight != operation) {
                                                return null;
                                            }
                                            work.inFlight = null;
                                            try {
                                                Versioned<BulkLoadTransaction> current =
                                                        readTransactionVersioned(handle);
                                                if (current.getVersion() != observed.getVersion()) {
                                                    advance(handle);
                                                    return null;
                                                }
                                                if (confirmation != null
                                                        && !confirmation.matchesRoundFacts(
                                                                readAssignment(handle),
                                                                coordinatorContext,
                                                                readRegisteredServers(),
                                                                readyRegistrations)) {
                                                    advance(handle);
                                                    return null;
                                                }
                                                if (failure != null) {
                                                    if (provingEmpty
                                                            && ExceptionUtils.findThrowable(
                                                                            failure,
                                                                            InvalidBulkLoadRequestException
                                                                                    .class)
                                                                    .isPresent()) {
                                                        startAbortDecision(
                                                                current,
                                                                BulkLoadAbortReason
                                                                        .TARGET_NOT_EMPTY,
                                                                ExceptionUtils.stringifyException(
                                                                        failure));
                                                        return null;
                                                    }
                                                    throw new CompletionException(unwrap(failure));
                                                }
                                                BulkLoadAbortReason abort =
                                                        abortDecision(current.getValue());
                                                if (abort != null) {
                                                    startAbortDecision(current, abort, null);
                                                } else {
                                                    continuation.run(current, result);
                                                }
                                            } catch (Throwable error) {
                                                invalidate(handle);
                                                failWaiters(handle, unwrap(error));
                                            }
                                            return null;
                                        })));
    }

    @FunctionalInterface
    private interface Continuation<T> {
        void run(Versioned<BulkLoadTransaction> transaction, T result) throws Exception;
    }

    private long[] allocateSnapshotIds(BulkLoadHandle handle, int count) throws Exception {
        long[] snapshotIds = new long[count];
        for (int bucketId = 0; bucketId < count; bucketId++) {
            TableBucket tableBucket =
                    new TableBucket(handle.getTableId(), handle.getPartitionId(), bucketId);
            snapshotIds[bucketId] = zkClient.allocateTableBucketSnapshotId(tableBucket);
        }
        return snapshotIds;
    }

    private void proveRemoteEmpty(BulkLoadHandle handle, TableAssignment assignment, int count)
            throws Exception {
        requireCompleteAssignment(handle, assignment, count);
        for (int bucketId = 0; bucketId < count; bucketId++) {
            TableBucket bucket =
                    new TableBucket(handle.getTableId(), handle.getPartitionId(), bucketId);
            if (!zkClient.listBucketSnapshotIds(bucket).isEmpty()
                    || zkClient.getRemoteLogManifestHandle(bucket).isPresent()) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad target has ordinary remote data for " + bucket + '.');
            }
        }
    }

    private BeginAdmission readBeginAdmission(
            PhysicalTablePath target, @Nullable Long buildTimeoutMs, FlussPrincipal creator)
            throws Exception {
        TablePath tablePath = target.getTablePath();
        PhysicalTarget physical = readPhysicalTarget(target);
        TableInfo tableInfo = physical.tableInfo;
        if (tableInfo.getPrimaryKeys().isEmpty()) {
            throw new InvalidBulkLoadRequestException("BulkLoad requires a primary-key table.");
        }
        SchemaInfo schemaInfo = metadataManager.getLatestSchema(tablePath);
        if (!schemaInfo.getSchema().getAutoIncrementColumnNames().isEmpty()) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad does not support auto-increment columns.");
        }
        Long partitionId = physical.partitionId;
        Versioned<?> registration = physical.registration;
        String remoteDataDir = physical.remoteDataDir;
        RegistrationState state = registrationState(registration);
        if (state.state != BulkLoadDataState.ACTIVE || state.bulkLoadId != null) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad target is occupied or its data state changed.");
        }
        BulkLoadHandle handle =
                new BulkLoadHandle(
                        target, tableInfo.getTableId(), partitionId, UUID.randomUUID().toString());
        Versioned<? extends TableAssignment> assignment = readAssignment(handle);
        if (remoteDataDir == null || remoteDataDir.isEmpty()) {
            throw new InvalidBulkLoadRequestException("BulkLoad remote data directory is missing.");
        }
        requireCompleteAssignment(handle, assignment.getValue(), tableInfo.getNumBuckets());
        int active = pending.size();
        int limit = configuration.get(ConfigOptions.BULKLOAD_MAX_ACTIVE_TRANSACTIONS);
        if (active >= limit) {
            throw new InvalidBulkLoadRequestException("BulkLoad active transaction limit reached.");
        }
        requireFound(
                ZkData.SchemaZNode.path(tablePath, schemaInfo.getSchemaId()),
                ZkData.SchemaZNode::decode,
                "schema");
        long timeout =
                buildTimeoutMs == null
                        ? configuration.get(ConfigOptions.BULKLOAD_BUILD_TIMEOUT_DEFAULT).toMillis()
                        : buildTimeoutMs;
        if (timeout <= 0
                || timeout
                        > configuration.get(ConfigOptions.BULKLOAD_BUILD_TIMEOUT_MAX).toMillis()) {
            throw new InvalidBulkLoadRequestException("Invalid BulkLoad build timeout.");
        }
        long now = System.currentTimeMillis();
        BulkLoadTransaction transaction =
                new BulkLoadTransaction(
                        handle,
                        BulkLoadState.BEGUN,
                        creator.getName(),
                        creator.getType(),
                        remoteDataDir,
                        schemaInfo.getSchemaId(),
                        registration.getVersion() + 1,
                        null,
                        Math.addExact(now, timeout),
                        null,
                        null,
                        null);
        return new BeginAdmission(registration, assignment, transaction);
    }

    private void requireClusterCapabilities() throws Exception {
        List<CoordinatorAddress> coordinators = new ArrayList<>();
        for (String id : zkClient.getCoordinatorServerList()) {
            coordinators.add(
                    requireFound(
                                    ZkData.CoordinatorIdZNode.path(id),
                                    ZkData.CoordinatorIdZNode::decode,
                                    "Coordinator registration")
                            .getValue());
        }
        List<RegisteredServer> tablets = readRegisteredServers();
        if (!ServerProtocolVersion.coordinatorsSupportBulkLoad(coordinators)) {
            throw new UnsupportedVersionException(
                    "Every Coordinator must cover the BulkLoad capability set.");
        }
        validateTabletCapabilities(tablets);
    }

    private static void validateTabletCapabilities(List<RegisteredServer> registrations) {
        List<TabletServerRegistration> values = new ArrayList<>();
        for (RegisteredServer registration : registrations) {
            values.add(registration.registration.getValue());
        }
        if (!ServerProtocolVersion.tabletServersSupportBulkLoad(values)) {
            throw new UnsupportedVersionException(
                    "Every TabletServer must cover the BulkLoad capability set.");
        }
    }

    List<RegisteredServer> readRegisteredServers() throws Exception {
        List<RegisteredServer> result = new ArrayList<>();
        for (int id : zkClient.getSortedTabletServerList()) {
            result.add(
                    new RegisteredServer(
                            id,
                            requireFound(
                                    ZkData.ServerIdZNode.path(id),
                                    ZkData.ServerIdZNode::decode,
                                    "TabletServer registration")));
        }
        return Collections.unmodifiableList(result);
    }

    private @Nullable ReplicaRound registrationsForRound(
            Versioned<? extends TableAssignment> assignment, boolean requireAllHolders)
            throws Exception {
        List<RegisteredServer> registrations = readRegisteredServers();
        List<RegisteredServer> recipients =
                BulkLoadReplicaConvergence.liveRecipients(
                        coordinatorContext, registrations, readyRegistrations);
        if (!recipients.isEmpty()) {
            validateTabletCapabilities(recipients);
        }
        Set<Integer> registeredIds = new HashSet<>();
        for (RegisteredServer registration : registrations) {
            registeredIds.add(registration.serverId);
        }
        ReplicaRound round = new ReplicaRound(assignment, recipients, Collections.emptyMap());
        for (Map.Entry<Integer, RegisteredServer> holder : round.holders.entrySet()) {
            if (holder.getValue() == null
                    && (requireAllHolders || registeredIds.contains(holder.getKey()))) {
                return null;
            }
        }
        return round;
    }

    private void requireControl(
            Versioned<BulkLoadTransaction> transaction, BulkLoadDataState state) {
        BulkLoadTransaction value = transaction.getValue();
        requireControl(value, readRegistration(value.getHandle()), state);
    }

    static void requireControl(
            BulkLoadTransaction value, Versioned<?> observation, BulkLoadDataState expected) {
        RegistrationState registration = registrationState(observation);
        if (!hasSameTargetIdentity(value.getHandle(), observation.getValue())
                || registration.state != expected
                || !value.getBulkLoadId().equals(registration.bulkLoadId)
                || !value.getMetadataPath().equals(registration.path)
                || value.getMetadataVersion() != registration.version) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad " + expected + " metadata identity changed.");
        }
    }

    static boolean hasSameTargetIdentity(BulkLoadHandle handle, Object registration) {
        if (registration instanceof TableRegistration) {
            return handle.getPartitionId() == null
                    && handle.getTableId() == ((TableRegistration) registration).tableId;
        }
        PartitionRegistration partition = (PartitionRegistration) registration;
        return handle.getPartitionId() != null
                && handle.getTableId() == partition.getTableId()
                && handle.getPartitionId() == partition.getPartitionId();
    }

    Versioned<BulkLoadTransaction> readTransactionVersioned(BulkLoadHandle handle) {
        PendingBulkLoad work = pending.get(handle);
        if (work != null && work.transaction != null) {
            return work.transaction;
        }
        Versioned<BulkLoadTransaction> transaction =
                readTransaction(handle)
                        .orElseThrow(
                                () ->
                                        new InvalidBulkLoadRequestException(
                                                "Missing BulkLoad transaction."));
        if (!transaction.getValue().getState().isTerminal() || work != null) {
            recordActiveTransaction(transaction);
        }
        return transaction;
    }

    private void invalidate(BulkLoadHandle handle) {
        PendingBulkLoad work = pending.get(handle);
        if (work != null) {
            work.transaction = null;
        }
    }

    Optional<Versioned<BulkLoadTransaction>> readTransaction(BulkLoadHandle handle) {
        Optional<Versioned<BulkLoadTransaction>> read =
                readOptional(
                        BulkLoadMetadataStore.transactionPath(handle),
                        ZkData.BulkLoadTransactionsZNode::decode);
        if (read.isPresent() && !read.get().getValue().getHandle().equals(handle)) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad handle does not match the persisted transaction.");
        }
        return read;
    }

    Versioned<? extends TableAssignment> readAssignment(BulkLoadHandle handle) {
        return handle.getPartitionId() == null
                ? requireFound(
                        ZkData.TableIdZNode.path(handle.getTableId()),
                        ZkData.TableIdZNode::decode,
                        "table assignment")
                : requireFound(
                        ZkData.PartitionIdZNode.path(handle.getPartitionId()),
                        ZkData.PartitionIdZNode::decode,
                        "partition assignment");
    }

    Versioned<?> readRegistration(BulkLoadHandle handle) {
        return readRegistration(handle.getTarget());
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

    <T> Versioned<T> requireFound(String path, Function<byte[], T> decoder, String description) {
        return readOptional(path, decoder)
                .orElseThrow(
                        () ->
                                new InvalidBulkLoadRequestException(
                                        "Missing BulkLoad " + description + '.'));
    }

    private <T> Optional<Versioned<T>> readOptional(String path, Function<byte[], T> decoder) {
        try {
            return metadataStore.readOptional(path, decoder);
        } catch (Exception failure) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad metadata is unreadable: " + path, failure);
        }
    }

    int coordinatorEpochVersion() {
        return coordinatorContext.getCoordinatorZkVersion();
    }

    private int targetBucketCount(BulkLoadHandle handle) throws Exception {
        return tableInfo(handle).getNumBuckets();
    }

    private TableInfo tableInfo(BulkLoadHandle handle) throws Exception {
        PhysicalTarget target = readPhysicalTarget(handle.getTarget());
        if (!hasSameTargetIdentity(handle, target.registration.getValue())) {
            throw new InvalidBulkLoadRequestException("BulkLoad physical target identity changed.");
        }
        return target.tableInfo;
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

    private @Nullable BulkLoadHandle findExistingTarget(PhysicalTablePath path) throws Exception {
        PhysicalTarget target = readPhysicalTarget(path);
        String owner = registrationState(target.registration).bulkLoadId;
        return owner == null
                ? null
                : new BulkLoadHandle(
                        path, target.tableInfo.getTableId(), target.partitionId, owner);
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

    private static boolean sameCreator(FlussPrincipal creator, BulkLoadTransaction transaction) {
        return creator.getName().equals(transaction.getCreatorName())
                && creator.getType().equals(transaction.getCreatorType());
    }

    private void completePendingBegin(BulkLoadTransaction transaction) {
        PendingBulkLoad work = pending.get(transaction.getHandle());
        CompletableFuture<BeginBulkLoadResponse> result = work.beginResult;
        work.beginResult = null;
        if (result != null) {
            if (transaction.getState() != BulkLoadState.BEGUN || !transaction.isFenceReady()) {
                result.completeExceptionally(
                        new InvalidBulkLoadRequestException(
                                "BulkLoad Begin did not reach a fence-ready state."));
                return;
            }
            try {
                result.complete(
                        new BeginBulkLoadResponse()
                                .setTargetInfo(
                                        toTargetInfo(
                                                transaction, tableInfo(transaction.getHandle()))));
            } catch (Exception failure) {
                result.completeExceptionally(failure);
            }
        }
    }

    private CompletableFuture<BulkLoadTransaction> awaitTerminal(BulkLoadHandle handle) {
        PendingBulkLoad work = pending.get(handle);
        if (work.terminalResult == null) {
            work.terminalResult = new CompletableFuture<>();
        }
        CompletableFuture<BulkLoadTransaction> result = work.terminalResult;
        try {
            advance(handle);
        } catch (Throwable failure) {
            failWaiters(handle, failure);
        }
        return result;
    }

    private void failWaiters(BulkLoadHandle handle, Throwable failure) {
        PendingBulkLoad work = pending.get(handle);
        if (work == null) {
            return;
        }
        work.inFlight = null;
        if (work.beginResult != null) {
            work.beginResult.completeExceptionally(failure);
            work.beginResult = null;
        }
        if (work.terminalResult != null) {
            work.terminalResult.completeExceptionally(failure);
            work.terminalResult = null;
        }
    }

    private static final class PendingBulkLoad {
        private Versioned<BulkLoadTransaction> transaction;
        private Object inFlight;
        private boolean abortRequested;
        private CompletableFuture<BeginBulkLoadResponse> beginResult;
        private CompletableFuture<BulkLoadTransaction> terminalResult;
    }

    static RegistrationState registrationState(Versioned<?> registration) {
        if (registration.getValue() instanceof TableRegistration) {
            TableRegistration table = (TableRegistration) registration.getValue();
            return new RegistrationState(
                    registration.getPath(),
                    registration.getVersion(),
                    table.dataState,
                    table.bulkLoadId);
        }
        PartitionRegistration partition = (PartitionRegistration) registration.getValue();
        return new RegistrationState(
                registration.getPath(),
                registration.getVersion(),
                partition.getDataState(),
                partition.getBulkLoadId());
    }

    private static void requireSameInput(BulkLoadTransaction transaction, String inputSha256) {
        if (!inputSha256.equals(transaction.getInputSha256())) {
            throw new InvalidBulkLoadRequestException(
                    "Commit retry conflicts with the frozen input.");
        }
    }

    private static void requireCompleteAssignment(
            BulkLoadHandle handle, TableAssignment assignment, int count) {
        requireCompleteAssignment(handle.getTableId(), handle.getPartitionId(), assignment, count);
    }

    private static void requireCompleteAssignment(
            long tableId, @Nullable Long partitionId, TableAssignment assignment, int count) {
        Set<Integer> expected = new HashSet<>();
        for (int bucket = 0; bucket < count; bucket++) {
            expected.add(bucket);
        }
        if (!assignment.getBuckets().equals(expected)) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad assignment does not cover the complete physical target.");
        }
        if (partitionId != null
                && (!(assignment instanceof PartitionAssignment)
                        || ((PartitionAssignment) assignment).getTableId() != tableId)) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad partition assignment identity changed.");
        }
    }

    private static Throwable unwrap(@Nullable Throwable failure) {
        return failure == null ? null : ExceptionUtils.stripCompletionException(failure);
    }

    public static BulkLoadTransaction readTransaction(
            ZooKeeperClient zkClient, BulkLoadHandle handle) {
        try {
            Optional<DataWithStat> data =
                    zkClient.getDataWithStatIfExists(BulkLoadMetadataStore.transactionPath(handle));
            if (!data.isPresent()) {
                throw new BulkLoadNotFoundException("BulkLoad transaction does not exist.");
            }
            BulkLoadTransaction transaction =
                    ZkData.BulkLoadTransactionsZNode.decode(data.get().getData());
            if (!transaction.getHandle().equals(handle)) {
                throw new InvalidBulkLoadRequestException(
                        "BulkLoad handle does not match the persisted transaction.");
            }
            return transaction;
        } catch (BulkLoadNotFoundException
                | InvalidBulkLoadRequestException
                | CorruptMessageException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to read BulkLoad transaction.", e);
        }
    }

    private static PbBulkLoadStatus toStatus(BulkLoadTransaction transaction) {
        PbBulkLoadStatus status =
                new PbBulkLoadStatus()
                        .setHandle(toPbHandle(transaction.getHandle()))
                        .setState(transaction.getState().getCode());
        if (transaction.getAbortReason() != null) {
            status.setAbortReason(transaction.getAbortReason().getCode());
        }
        if (transaction.getAbortMessage() != null) {
            status.setAbortMessage(transaction.getAbortMessage());
        }
        return status;
    }

    private static PbBulkLoadHandle toPbHandle(BulkLoadHandle handle) {
        PbBulkLoadHandle result =
                new PbBulkLoadHandle()
                        .setTarget(fromPhysicalTablePath(handle.getTarget()))
                        .setTableId(handle.getTableId())
                        .setBulkLoadId(handle.getBulkLoadId());
        if (handle.getPartitionId() != null) {
            result.setPartitionId(handle.getPartitionId());
        }
        return result;
    }

    private static PbBulkLoadTargetInfo toTargetInfo(
            BulkLoadTransaction transaction, TableInfo tableInfo) {
        long[] snapshotIds = checkNotNull(transaction.getSnapshotIds());
        if (snapshotIds.length != tableInfo.getNumBuckets()) {
            throw new IllegalArgumentException(
                    "BulkLoad Snapshot IDs must cover every physical bucket.");
        }
        return new PbBulkLoadTargetInfo()
                .setHandle(toPbHandle(transaction.getHandle()))
                .setSchemaId(transaction.getSchemaId())
                .setTableJson(tableInfo.toTableDescriptor().toJsonBytes())
                .setCreatedTime(tableInfo.getCreatedTime())
                .setModifiedTime(tableInfo.getModifiedTime())
                .setRemoteDataDir(transaction.getRemoteDataDir())
                .setSnapshotIds(snapshotIds);
    }

    /** Exact ZooKeeper observations needed by the Begin checked multi. */
    private static final class BeginAdmission {
        private final Versioned<?> registration;
        private final Versioned<? extends TableAssignment> assignment;
        private final BulkLoadTransaction transaction;

        private BeginAdmission(
                Versioned<?> registration,
                Versioned<? extends TableAssignment> assignment,
                BulkLoadTransaction transaction) {
            this.registration = registration;
            this.assignment = assignment;
            this.transaction = transaction;
        }
    }

    static final class RegistrationState {
        final String path;
        final int version;
        final BulkLoadDataState state;
        final @Nullable String bulkLoadId;

        private RegistrationState(
                String path, int version, BulkLoadDataState state, @Nullable String bulkLoadId) {
            this.path = path;
            this.version = version;
            this.state = state;
            this.bulkLoadId = bulkLoadId;
        }
    }
}
