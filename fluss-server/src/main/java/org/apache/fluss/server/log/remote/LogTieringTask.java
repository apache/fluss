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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogManifestReplacementPlanner;
import org.apache.fluss.remote.RemoteLogManifestReplacementPlanner.PlanType;
import org.apache.fluss.remote.RemoteLogManifestV2Migration;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.remote.UnreferencedRemoteLogSegment;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.entity.RemoteLogManifestCommitResult;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TableMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.VersionedRemoteLogManifestHandle;
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeCommitRemoteLogManifestRequest;

/**
 * A task to copy log segments to remote storage and delete expired remote log segments from remote.
 */
public class LogTieringTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LogTieringTask.class);

    private final Replica replica;
    private final RemoteLogTablet remoteLog;
    private final PhysicalTablePath physicalTablePath;
    private final TableBucket tableBucket;
    private final RemoteLogStorage remoteLogStorage;
    private final RemoteLogIndexCache remoteLogIndexCache;
    private final CoordinatorGateway coordinatorGateway;
    private final RemoteLogManifestCommitter remoteLogManifestCommitter;
    private final ZooKeeperClient zooKeeperClient;
    private final Clock clock;
    private final int maxUploadSegmentsPerTask;
    private final boolean manifestV2WriterEnabled;
    private final boolean manifestV2GcEnabled;
    private final long manifestV2GcGracePeriodMs;

    // The copied offset is empty initially for a new leader LogTieringTask, and needs to
    // be fetched inside the task's run() method.
    /** Exclusive offset from which the next remote copy should resume. */
    private volatile Long nextCopyOffset = null;

    private volatile boolean cancelled = false;

    public LogTieringTask(
            Replica replica,
            RemoteLogTablet remoteLog,
            RemoteLogStorage remoteLogStorage,
            RemoteLogIndexCache remoteLogIndexCache,
            CoordinatorGateway coordinatorGateway,
            ZooKeeperClient zooKeeperClient,
            Clock clock,
            int maxUploadSegmentsPerTask,
            boolean manifestV2WriterEnabled,
            boolean manifestV2GcEnabled,
            long manifestV2GcGracePeriodMs) {
        this.replica = replica;
        this.remoteLog = remoteLog;
        this.physicalTablePath = replica.getPhysicalTablePath();
        this.tableBucket = replica.getTableBucket();
        this.remoteLogStorage = remoteLogStorage;
        this.remoteLogIndexCache = remoteLogIndexCache;
        this.coordinatorGateway = coordinatorGateway;
        this.zooKeeperClient = zooKeeperClient;
        this.remoteLogManifestCommitter =
                new RemoteLogManifestCommitter(coordinatorGateway, zooKeeperClient);
        this.clock = clock;
        this.maxUploadSegmentsPerTask = maxUploadSegmentsPerTask;
        this.manifestV2WriterEnabled = manifestV2WriterEnabled;
        this.manifestV2GcEnabled = manifestV2GcEnabled;
        this.manifestV2GcGracePeriodMs = manifestV2GcGracePeriodMs;
    }

    @Override
    public void run() {
        if (isCancelled()) {
            return;
        }

        try {
            // Try to copy these candidate copy log segments to remote storage and try to clean
            // up these expired remote log segments from remote.
            runOnce();
        } catch (InterruptedException ex) {
            if (!isCancelled()) {
                LOG.warn(
                        "Current thread for table-bucket {} is interrupted. Reason: {}",
                        tableBucket,
                        ex.getMessage());
            }
        } catch (RetriableException ex) {
            LOG.debug(
                    "Encountered a retryable error while executing current task for table-bucket {}",
                    tableBucket,
                    ex);
        } catch (Exception ex) {
            if (!isCancelled()) {
                LOG.warn(
                        "Current task for table-bucket {} received error but it will be scheduled. "
                                + "Reason: {}",
                        tableBucket,
                        ex.getMessage());
            }
        }
    }

    private void runOnce() throws InterruptedException {
        if (isCancelled()) {
            LOG.info("Returning from LogTieringTask runOnes as the task state is changed");
            return;
        }

        try {
            LogTablet logTablet = replica.getLogTablet();
            TableMetricGroup metricGroup = replica.tableMetrics();
            maybeInitializeNextCopyOffset(logTablet);
            if (manifestV2WriterEnabled) {
                runOnceV2(logTablet, metricGroup, true);
                return;
            }

            // Get these candidate log segments to copy and these expired remote log segments to
            // clean up.
            List<EnrichedLogSegment> candidateToCopySegments =
                    candidateToCopyLogSegments(logTablet);
            // Only delete segments that have been tiered to lake to ensure data safety
            List<RemoteLogSegment> expiredRemoteLogSegments =
                    remoteLog.expiredRemoteLogSegments(
                            clock.milliseconds(),
                            logTablet.isDataLakeEnabled() ? logTablet.getLakeLogEndOffset() : null);

            // 1. For these candidateToCopySegments, we will first copy segment files to
            // remote before commit the remote log manifest.
            List<RemoteLogSegment> copiedSegments = new ArrayList<>();
            long endOffset =
                    copyLogSegmentFilesToRemote(
                            logTablet, candidateToCopySegments, copiedSegments, metricGroup);

            // 2. try to commit the remote log manifest snapshot to coordinator server and
            // update the local cache of remote log manifest.
            if (!copiedSegments.isEmpty() || !expiredRemoteLogSegments.isEmpty()) {
                boolean success =
                        tryToCommitRemoteLogManifest(
                                remoteLog, expiredRemoteLogSegments, copiedSegments);

                if (success) {
                    if (!expiredRemoteLogSegments.isEmpty()) {
                        // 3. For these expiredRemoteLogSegments, we will delete remote log
                        // segment files from remote after commit the remote log manifest.
                        // TODO introduce the read reference count to avoid deleting remote log
                        // segments while there are readers is in progress.
                        deleteRemoteLogSegmentFiles(expiredRemoteLogSegments, metricGroup);

                        remoteLogIndexCache.removeAll(
                                expiredRemoteLogSegments.stream()
                                        .map(RemoteLogSegment::remoteLogSegmentId)
                                        .collect(Collectors.toList()));
                    }

                    if (endOffset >= 0) {
                        nextCopyOffset = endOffset;
                    }
                } else {
                    LOG.error(
                            "Failed commit remote log manifest snapshot to coordinator server "
                                    + "for bucket: {}, copied segments: {}, expired segments: {}",
                            tableBucket,
                            copiedSegments,
                            expiredRemoteLogSegments);

                    if (!copiedSegments.isEmpty()) {
                        // 4. For these copiedSegments, if snapshot commit failed, we need to
                        // delete remote log segment files already copied in step 1.
                        deleteRemoteLogSegmentFiles(copiedSegments, metricGroup);
                    }
                }
            }

        } catch (InterruptedException | RetriableException ex) {
            throw ex;
        } catch (Exception ex) {
            if (!isCancelled()) {
                LOG.error(
                        "Error occurred while copying log segments of bucket: {}", tableBucket, ex);
            }
        }
    }

    private void runOnceV2(
            LogTablet logTablet, TableMetricGroup metricGroup, boolean allowConflictReplan)
            throws Exception {
        RemoteLogManifest baseManifest = remoteLog.currentManifest();
        VersionedRemoteLogManifestHandle baseHandle = remoteLog.currentHandle();
        if (baseHandle != null) {
            RemoteLogManager.validateManifestHandle(baseHandle.handle(), baseManifest);
        } else if (!baseManifest.getRemoteLogSegmentList().isEmpty()) {
            throw new IllegalStateException("Remote manifest has no authoritative handle");
        }

        long now = clock.milliseconds();
        if (manifestV2GcEnabled) {
            sweepOrphanObjects(baseManifest, baseHandle, now, metricGroup);
        }
        long targetGeneration =
                baseHandle == null
                        ? 1L
                        : baseHandle.handle().getManifestGeneration().orElse(0L) + 1L;
        RemoteLogManifest normalizedBase =
                normalizeBaseManifest(baseManifest, targetGeneration, now);
        List<EnrichedLogSegment> candidates = candidateToCopyLogSegments(logTablet);
        List<CandidatePlan> candidatePlans = planCandidates(normalizedBase, candidates, now);

        RemoteLogManifest resultManifest = normalizedBase;
        List<RemoteLogSegment> copiedSegments = new ArrayList<>();
        List<PlanType> appliedPlanTypes = new ArrayList<>();
        long successfulNextCopyOffset = nextCopyOffset;
        for (CandidatePlan candidatePlan : candidatePlans) {
            if (candidatePlan.planType == PlanType.GAP) {
                LOG.warn(
                        "Stopping Manifest V2 planning at remote gap for bucket {} and segment {}",
                        tableBucket,
                        candidatePlan.remoteLogSegment);
                break;
            }
            if (candidatePlan.planType == PlanType.ALREADY_COVERED) {
                appliedPlanTypes.add(candidatePlan.planType);
                successfulNextCopyOffset = candidatePlan.enrichedLogSegment.nextSegmentOffset;
                continue;
            }
            if (!copyPlannedSegment(logTablet, candidatePlan, metricGroup)) {
                break;
            }
            copiedSegments.add(candidatePlan.remoteLogSegment);
            RemoteLogManifestReplacementPlanner.Result replayed =
                    RemoteLogManifestReplacementPlanner.plan(
                            resultManifest, candidatePlan.remoteLogSegment, now);
            resultManifest = withGeneration(replayed.resultManifest(), targetGeneration);
            appliedPlanTypes.add(candidatePlan.planType);
            successfulNextCopyOffset = candidatePlan.enrichedLogSegment.nextSegmentOffset;
        }

        RemoteLogManifest beforeExpiration = resultManifest;
        resultManifest =
                RemoteLogManifestReplacementPlanner.expireContinuousPrefix(
                        resultManifest,
                        now,
                        replica.getLogTTLMs(),
                        logTablet.isDataLakeEnabled() ? logTablet.getLakeLogEndOffset() : null,
                        now);
        boolean expirationChanged = resultManifest != beforeExpiration;
        RemoteLogManifest beforeGarbageCollection = resultManifest;
        if (manifestV2GcEnabled) {
            resultManifest = collectUnreferencedSegments(resultManifest, now, metricGroup);
        }
        boolean garbageCollectionChanged = resultManifest != beforeGarbageCollection;
        if (copiedSegments.isEmpty() && !expirationChanged && !garbageCollectionChanged) {
            nextCopyOffset = successfulNextCopyOffset;
            return;
        }

        RemoteLogManifestUpdatePlan updatePlan =
                new RemoteLogManifestUpdatePlan(
                        baseHandle,
                        resultManifest,
                        copiedSegments,
                        newlyUnreferencedSegments(baseManifest, resultManifest),
                        appliedPlanTypes,
                        successfulNextCopyOffset);
        FsPath manifestPath;
        try {
            manifestPath = remoteLogStorage.writeRemoteLogManifestSnapshot(resultManifest);
        } catch (Exception e) {
            deleteRemoteLogSegmentFiles(copiedSegments, metricGroup);
            throw e;
        }

        CommitRemoteLogManifestData commitData =
                updatePlan.toCommitData(
                        manifestPath, replica.getCoordinatorEpoch(), replica.getBucketEpoch());
        RemoteLogManifestCommitResult commitResult;
        try {
            commitResult =
                    remoteLogManifestCommitter.commitAndApply(
                            commitData, resultManifest, remoteLog);
        } catch (Exception unknownResult) {
            // The result is UNKNOWN or was superseded before local apply. Objects must remain
            // untouched until a later authoritative reconciliation proves they are unreferenced.
            throw unknownResult;
        }

        if (commitResult == RemoteLogManifestCommitResult.COMMITTED) {
            applyPublishedOffsets(logTablet, resultManifest);
            nextCopyOffset = updatePlan.nextCopyOffset();
            return;
        }

        cleanupRejectedPlan(updatePlan, manifestPath, metricGroup, logTablet);
        if (commitResult == RemoteLogManifestCommitResult.CONFLICT && allowConflictReplan) {
            nextCopyOffset = findNextCopyOffset(logTablet);
            runOnceV2(logTablet, metricGroup, false);
        }
    }

    private RemoteLogManifest collectUnreferencedSegments(
            RemoteLogManifest manifest, long now, TableMetricGroup metricGroup) {
        List<UnreferencedRemoteLogSegment> retained = new ArrayList<>();
        List<UUID> deletedSegmentIds = new ArrayList<>();
        for (UnreferencedRemoteLogSegment unreferenced :
                manifest.getUnreferencedRemoteLogSegments()) {
            if (!gracePeriodElapsed(unreferenced.unreferencedAtMs(), now)) {
                retained.add(unreferenced);
                continue;
            }
            try {
                remoteLogStorage.deleteLogSegmentFiles(unreferenced.remoteLogSegment());
                metricGroup.remoteLogDeleteRequests().inc();
                deletedSegmentIds.add(unreferenced.remoteLogSegment().remoteLogSegmentId());
            } catch (Exception e) {
                retained.add(unreferenced);
                metricGroup.remoteLogDeleteErrors().inc();
                metricGroup.remoteGcFailures().inc();
                LOG.warn(
                        "Manifest V2 GC will retry deleting unreferenced segment {} for {}",
                        unreferenced.remoteLogSegment().remoteLogSegmentId(),
                        tableBucket,
                        e);
            }
        }
        if (retained.size() == manifest.getUnreferencedRemoteLogSegments().size()) {
            return manifest;
        }
        remoteLogIndexCache.removeAll(deletedSegmentIds);
        return RemoteLogManifest.createV2(
                manifest.getGeneration(),
                manifest.getPhysicalTablePath(),
                manifest.getTableBucket(),
                manifest.getRemoteLogSegmentList(),
                manifest.getRemoteLogSegmentList().isEmpty()
                        ? null
                        : manifest.getRemoteLogStartOffset(),
                retained);
    }

    private void sweepOrphanObjects(
            RemoteLogManifest baseManifest,
            VersionedRemoteLogManifestHandle baseHandle,
            long now,
            TableMetricGroup metricGroup) {
        try {
            if (!authoritativeHandleUnchanged(baseHandle)) {
                LOG.info("Skipping orphan sweep for {} because its handle changed", tableBucket);
                return;
            }
            Set<UUID> referencedSegmentIds = new HashSet<>();
            baseManifest
                    .getRemoteLogSegmentList()
                    .forEach(segment -> referencedSegmentIds.add(segment.remoteLogSegmentId()));
            baseManifest
                    .getUnreferencedRemoteLogSegments()
                    .forEach(
                            segment ->
                                    referencedSegmentIds.add(
                                            segment.remoteLogSegment().remoteLogSegmentId()));

            for (RemoteLogStorageObject object :
                    remoteLogStorage.listRemoteLogSegmentObjects(physicalTablePath, tableBucket)) {
                UUID segmentId = UUID.fromString(object.path().getName());
                if (!referencedSegmentIds.contains(segmentId)
                        && orphanGracePeriodElapsed(object.modificationTimeMs(), now)
                        && authoritativeHandleUnchanged(baseHandle)) {
                    try {
                        remoteLogStorage.deleteRemoteLogSegmentObject(
                                physicalTablePath, tableBucket, segmentId);
                        metricGroup.remoteLogDeleteRequests().inc();
                    } catch (Exception e) {
                        metricGroup.remoteGcFailures().inc();
                        LOG.warn(
                                "Failed to delete orphan segment object {} for {}",
                                object.path(),
                                tableBucket,
                                e);
                    }
                }
            }

            FsPath authoritativeManifestPath =
                    baseHandle == null ? null : baseHandle.handle().getRemoteLogManifestPath();
            List<RemoteLogStorageObject> manifestSnapshots =
                    remoteLogStorage.listRemoteLogManifestSnapshots(physicalTablePath, tableBucket);
            long orphanManifestCount = 0L;
            for (RemoteLogStorageObject object : manifestSnapshots) {
                if (sameStoragePath(object.path(), authoritativeManifestPath)) {
                    continue;
                }
                boolean deleted = false;
                if (orphanGracePeriodElapsed(object.modificationTimeMs(), now)
                        && authoritativeHandleUnchanged(baseHandle)) {
                    try {
                        remoteLogStorage.deleteRemoteLogManifestSnapshot(object.path());
                        deleted = true;
                    } catch (Exception e) {
                        metricGroup.remoteGcFailures().inc();
                        LOG.warn(
                                "Failed to delete orphan manifest snapshot {} for {}",
                                object.path(),
                                tableBucket,
                                e);
                    }
                }
                if (!deleted) {
                    orphanManifestCount++;
                }
            }
            remoteLog.updateOrphanManifestCount(orphanManifestCount);
        } catch (Exception e) {
            metricGroup.remoteGcFailures().inc();
            LOG.warn("Failed to sweep orphan remote objects for {}", tableBucket, e);
        }
    }

    private boolean authoritativeHandleUnchanged(VersionedRemoteLogManifestHandle expected)
            throws Exception {
        Optional<VersionedRemoteLogManifestHandle> current =
                zooKeeperClient.getVersionedRemoteLogManifestHandle(tableBucket);
        if (expected == null) {
            return !current.isPresent();
        }
        if (!current.isPresent()) {
            return false;
        }
        VersionedRemoteLogManifestHandle actual = current.get();
        return actual.zkVersion() == expected.zkVersion()
                && actual.handle()
                        .getRemoteLogManifestPath()
                        .equals(expected.handle().getRemoteLogManifestPath())
                && actual.handle().getManifestGeneration().orElse(0L)
                        == expected.handle().getManifestGeneration().orElse(0L);
    }

    private boolean gracePeriodElapsed(long timestampMs, long now) {
        return timestampMs >= 0L
                && timestampMs != Long.MAX_VALUE
                && timestampMs <= now
                && now - timestampMs >= manifestV2GcGracePeriodMs;
    }

    private boolean orphanGracePeriodElapsed(long modificationTimeMs, long now) {
        // A zero or unavailable timestamp is not trustworthy enough for destructive orphan GC.
        return modificationTimeMs > 0L && gracePeriodElapsed(modificationTimeMs, now);
    }

    private static boolean sameStoragePath(FsPath first, FsPath second) {
        return first != null && second != null && first.getPath().equals(second.getPath());
    }

    private List<CandidatePlan> planCandidates(
            RemoteLogManifest normalizedBase,
            List<EnrichedLogSegment> candidates,
            long unreferencedAtMs)
            throws IOException {
        List<CandidatePlan> plans = new ArrayList<>();
        RemoteLogManifest workingManifest = normalizedBase;
        for (EnrichedLogSegment candidate : candidates) {
            RemoteLogSegment remoteLogSegment = createRemoteLogSegment(candidate);
            RemoteLogManifestReplacementPlanner.Result result =
                    RemoteLogManifestReplacementPlanner.plan(
                            workingManifest, remoteLogSegment, unreferencedAtMs);
            plans.add(new CandidatePlan(candidate, remoteLogSegment, result.planType()));
            if (result.planType() == PlanType.GAP) {
                break;
            }
            if (result.requiresManifestCommit()) {
                workingManifest =
                        withGeneration(result.resultManifest(), normalizedBase.getGeneration());
            }
        }
        return plans;
    }

    private RemoteLogManifest normalizeBaseManifest(
            RemoteLogManifest baseManifest, long targetGeneration, long unreferencedAtMs) {
        if (baseManifest.getVersion() == RemoteLogManifest.VERSION_1) {
            return RemoteLogManifestV2Migration.migrate(
                    baseManifest, targetGeneration, unreferencedAtMs);
        }
        return RemoteLogManifest.createV2(
                targetGeneration,
                baseManifest.getPhysicalTablePath(),
                baseManifest.getTableBucket(),
                baseManifest.getRemoteLogSegmentList(),
                baseManifest.getRemoteLogSegmentList().isEmpty()
                        ? null
                        : baseManifest.getRemoteLogStartOffset(),
                baseManifest.getUnreferencedRemoteLogSegments());
    }

    private static RemoteLogManifest withGeneration(RemoteLogManifest manifest, long generation) {
        return RemoteLogManifest.createV2(
                generation,
                manifest.getPhysicalTablePath(),
                manifest.getTableBucket(),
                manifest.getRemoteLogSegmentList(),
                manifest.getRemoteLogSegmentList().isEmpty()
                        ? null
                        : manifest.getRemoteLogStartOffset(),
                manifest.getUnreferencedRemoteLogSegments());
    }

    private static List<RemoteLogSegment> newlyUnreferencedSegments(
            RemoteLogManifest baseManifest, RemoteLogManifest resultManifest) {
        Set<UUID> baseUnreferencedIds = new HashSet<>();
        for (UnreferencedRemoteLogSegment entry : baseManifest.getUnreferencedRemoteLogSegments()) {
            baseUnreferencedIds.add(entry.remoteLogSegment().remoteLogSegmentId());
        }
        List<RemoteLogSegment> result = new ArrayList<>();
        for (UnreferencedRemoteLogSegment entry :
                resultManifest.getUnreferencedRemoteLogSegments()) {
            if (!baseUnreferencedIds.contains(entry.remoteLogSegment().remoteLogSegmentId())) {
                result.add(entry.remoteLogSegment());
            }
        }
        return result;
    }

    private RemoteLogSegment createRemoteLogSegment(EnrichedLogSegment enrichedSegment)
            throws IOException {
        LogSegment segment = enrichedSegment.logSegment;
        return RemoteLogSegment.Builder.builder()
                .physicalTablePath(physicalTablePath)
                .tableBucket(tableBucket)
                .remoteLogSegmentId(UUID.randomUUID())
                .remoteLogStartOffset(segment.getBaseOffset())
                .remoteLogEndOffset(enrichedSegment.nextSegmentOffset)
                .maxTimestamp(segment.maxTimestampSoFar())
                .segmentSizeInBytes(segment.getFileLogRecords().sizeInBytes())
                .build();
    }

    private boolean copyPlannedSegment(
            LogTablet logTablet, CandidatePlan candidatePlan, TableMetricGroup metricGroup)
            throws Exception {
        LogSegment segment = candidatePlan.enrichedLogSegment.logSegment;
        File writerIdSnapshotFile =
                logTablet
                        .writerStateManager()
                        .fetchSnapshot(candidatePlan.enrichedLogSegment.nextSegmentOffset)
                        .orElse(null);
        LogSegmentFiles logSegmentFiles =
                new LogSegmentFiles(
                        segment.getFileLogRecords().file().toPath(),
                        toPathIfExists(segment.offsetIndex().file()),
                        toPathIfExists(segment.timeIndex().file()),
                        writerIdSnapshotFile == null ? null : writerIdSnapshotFile.toPath());
        try {
            remoteLogStorage.copyLogSegmentFiles(candidatePlan.remoteLogSegment, logSegmentFiles);
        } catch (RemoteStorageException e) {
            metricGroup.remoteLogCopyErrors().inc();
            LOG.warn(
                    "Failed to copy planned Manifest V2 segment {} for bucket {}",
                    candidatePlan.remoteLogSegment,
                    tableBucket,
                    e);
            return false;
        }
        metricGroup.remoteLogCopyRequests().inc();
        metricGroup.remoteLogCopyBytes().inc(candidatePlan.remoteLogSegment.segmentSizeInBytes());
        return true;
    }

    private void cleanupRejectedPlan(
            RemoteLogManifestUpdatePlan updatePlan,
            FsPath manifestPath,
            TableMetricGroup metricGroup,
            LogTablet logTablet)
            throws Exception {
        Optional<VersionedRemoteLogManifestHandle> authoritativeHandleOpt =
                zooKeeperClient.getVersionedRemoteLogManifestHandle(tableBucket);
        Set<UUID> authoritativeSegmentIds = new HashSet<>();
        if (authoritativeHandleOpt.isPresent()) {
            VersionedRemoteLogManifestHandle authoritativeHandle = authoritativeHandleOpt.get();
            RemoteLogManifest authoritativeManifest =
                    remoteLogStorage.readRemoteLogManifestSnapshot(
                            authoritativeHandle.handle().getRemoteLogManifestPath());
            RemoteLogManager.validateManifestHandle(
                    authoritativeHandle.handle(), authoritativeManifest);
            authoritativeManifest
                    .getRemoteLogSegmentList()
                    .forEach(segment -> authoritativeSegmentIds.add(segment.remoteLogSegmentId()));
            authoritativeManifest
                    .getUnreferencedRemoteLogSegments()
                    .forEach(
                            segment ->
                                    authoritativeSegmentIds.add(
                                            segment.remoteLogSegment().remoteLogSegmentId()));
            remoteLog.replaceManifest(authoritativeManifest, authoritativeHandle);
            applyPublishedOffsets(logTablet, authoritativeManifest);
        } else {
            remoteLog.replaceManifest(
                    new RemoteLogManifest(physicalTablePath, tableBucket, Collections.emptyList()),
                    null);
        }

        for (RemoteLogSegment copiedSegment : updatePlan.segmentsToCopy()) {
            if (authoritativeSegmentIds.contains(copiedSegment.remoteLogSegmentId())) {
                throw new IllegalStateException(
                        "Rejected plan segment is referenced by the authoritative manifest: "
                                + copiedSegment.remoteLogSegmentId());
            }
        }
        deleteRemoteLogSegmentFiles(updatePlan.segmentsToCopy(), metricGroup);
        remoteLogStorage.deleteRemoteLogManifestSnapshot(manifestPath);
    }

    private void applyPublishedOffsets(LogTablet logTablet, RemoteLogManifest manifest) {
        logTablet.updateRemoteLogStartOffset(manifest.getRemoteLogStartOffset());
        logTablet.updateRemoteLogEndOffset(manifest.getRemoteLogEndOffset());
        logTablet.updateRemoteLogSize(manifest.getRemoteLogSize());
    }

    private List<EnrichedLogSegment> candidateToCopyLogSegments(LogTablet log) {
        List<EnrichedLogSegment> candidateLogSegments = new ArrayList<>();
        // Get highWatermark.
        long highWatermark = log.getHighWatermark();
        if (highWatermark < 0) {
            LOG.warn(
                    "The highWatermark for bucket {} is {}, which should not be negative",
                    tableBucket,
                    highWatermark);
        } else if (highWatermark > 0 && nextCopyOffset < highWatermark) {
            // local-log-start-offset can be ahead of the next-copy-offset, when enabling the
            // remote log for the first time
            long fromOffset = Math.max(nextCopyOffset, log.localLogStartOffset());
            candidateLogSegments = candidateLogSegments(log, fromOffset, highWatermark);
            LOG.debug(
                    "Candidate log segments for bucket {}: logLocalStartOffset: {}, nextCopyOffset: {}, "
                            + "fromOffset: {}, highWatermark: {} and candidateLogSegments: {}",
                    tableBucket,
                    log.localLogStartOffset(),
                    nextCopyOffset,
                    fromOffset,
                    highWatermark,
                    candidateLogSegments);
            if (candidateLogSegments.isEmpty()) {
                LOG.debug(
                        "no segments found to be copied for bucket {} which next-copy-offset: {} and active segment's base-offset: {}",
                        tableBucket,
                        nextCopyOffset,
                        log.activeLogSegment().getBaseOffset());
            }
        } else {
            LOG.debug(
                    "Skipping copying segments for bucket {} to remote, next-copy-offset:{}, and highWatermark:{}",
                    tableBucket,
                    nextCopyOffset,
                    highWatermark);
        }

        return candidateLogSegments;
    }

    /**
     * Copy the given log segments to remote and add the successfully copied segment to the {@code
     * copiedSegments} parameter.
     *
     * <p>If a segment copy fails (e.g., due to rate limiting or transient errors), the method stops
     * copying further segments but retains all previously successful copies so they can still be
     * committed, avoiding wasted uploads.
     *
     * @return the end offset of the last segment successfully copied to remote, or -1 if no
     *     segments were copied.
     */
    private long copyLogSegmentFilesToRemote(
            LogTablet log,
            List<EnrichedLogSegment> segments,
            List<RemoteLogSegment> copiedSegments,
            TableMetricGroup metricGroup)
            throws Exception {
        long endOffset = -1;
        for (EnrichedLogSegment enrichedSegment : segments) {
            LogSegment segment = enrichedSegment.logSegment;
            File logFile = segment.getFileLogRecords().file();
            String logFileName = logFile.getName();
            LOG.info(
                    "Copying {} of table {} bucket {} to remote storage.",
                    logFileName,
                    physicalTablePath,
                    tableBucket.getBucket());
            long segmentEndOffset = enrichedSegment.nextSegmentOffset;

            File writerIdSnapshotFile =
                    log.writerStateManager().fetchSnapshot(segmentEndOffset).orElse(null);
            LogSegmentFiles logSegmentFiles =
                    new LogSegmentFiles(
                            logFile.toPath(),
                            toPathIfExists(segment.offsetIndex().file()),
                            toPathIfExists(segment.timeIndex().file()),
                            writerIdSnapshotFile != null ? writerIdSnapshotFile.toPath() : null);

            UUID remoteLogSegmentId = UUID.randomUUID();
            int sizeInBytes = segment.getFileLogRecords().sizeInBytes();
            RemoteLogSegment copyRemoteLogSegment =
                    RemoteLogSegment.Builder.builder()
                            .physicalTablePath(physicalTablePath)
                            .tableBucket(tableBucket)
                            .remoteLogSegmentId(remoteLogSegmentId)
                            .remoteLogStartOffset(segment.getBaseOffset())
                            .remoteLogEndOffset(segmentEndOffset)
                            .maxTimestamp(segment.maxTimestampSoFar())
                            .segmentSizeInBytes(sizeInBytes)
                            .build();
            try {
                remoteLogStorage.copyLogSegmentFiles(copyRemoteLogSegment, logSegmentFiles);
            } catch (RemoteStorageException e) {
                metricGroup.remoteLogCopyErrors().inc();
                LOG.warn(
                        "Failed to copy {} of table {} bucket {} to remote storage. "
                                + "Stopping further segment copies. "
                                + "{} segment(s) already copied successfully will be committed.",
                        logFileName,
                        physicalTablePath,
                        tableBucket.getBucket(),
                        copiedSegments.size(),
                        e);
                break;
            }
            LOG.info(
                    "Copied {} of table {} bucket {} to remote storage as remote log segment: {}.",
                    logFileName,
                    physicalTablePath,
                    tableBucket,
                    copyRemoteLogSegment.remoteLogSegmentId());
            metricGroup.remoteLogCopyRequests().inc();
            metricGroup.remoteLogCopyBytes().inc(sizeInBytes);
            copiedSegments.add(copyRemoteLogSegment);
            endOffset = segmentEndOffset;
        }
        return endOffset;
    }

    /**
     * Try to commit remote log manifest. Including three steps.
     *
     * <pre>
     *     1. apply the build snapshot method (may be copy to/delete from remote)
     *     2. upload the remote log manifest file to remote storage.
     *     3. sending the CommitRemoteLogManifestRequest to coordinator server to try to commit this snapshot.
     *        - If commit success, we will apply the commit success action (e.g., delete expired remote segments), and return true.
     *        - If commit failed, we will apply rollback action (i.e., delete the new added remote segments), and return false.
     * </pre>
     */
    public boolean tryToCommitRemoteLogManifest(
            RemoteLogTablet remoteLogTablet,
            List<RemoteLogSegment> expiredSegments,
            List<RemoteLogSegment> newAddedSegments) {

        // 1. apply the build snapshot method.
        RemoteLogManifest newRemoteLogManifest =
                remoteLogTablet.currentManifest().trimAndMerge(expiredSegments, newAddedSegments);

        FsPath remoteLogManifestPath;
        try {
            // 1. upload the remote log manifest file to remote storage.
            remoteLogManifestPath =
                    remoteLogStorage.writeRemoteLogManifestSnapshot(newRemoteLogManifest);
        } catch (Exception e) {
            LOG.error(
                    "Write remote log manifest file to remote storage failed for bucket {}.",
                    tableBucket,
                    e);
            return false;
        }

        // 2. sending the CommitRemoteLogManifestRequest to coordinator server
        // to try to commit this snapshot.
        long newRemoteLogStartOffset = newRemoteLogManifest.getRemoteLogStartOffset();
        long newRemoteLogEndOffset = newRemoteLogManifest.getRemoteLogEndOffset();
        long newRemoteLogSize = newRemoteLogManifest.getRemoteLogSize();
        int retrySendCommitTimes = 1;
        while (retrySendCommitTimes <= 10) {
            try {
                boolean success =
                        commitRemoteLogManifest(
                                new CommitRemoteLogManifestData(
                                        tableBucket,
                                        remoteLogManifestPath,
                                        newRemoteLogStartOffset,
                                        newRemoteLogEndOffset,
                                        // TODO: manifest snapshot should include the epoch info,
                                        //  and this should be moved into Replica under read lock of
                                        //  leaderIsrUpdateLock, see FLUSS-56282058
                                        replica.getCoordinatorEpoch(),
                                        replica.getBucketEpoch()),
                                newRemoteLogManifest);
                if (!success) {
                    // the commit failed, it means the commit snapshot is invalid or register zk
                    // failed, we will revert this commit and delete the remote log manifest
                    // file.
                    // TODO: add the fail reason in the future.
                    LOG.error(
                            "Commit remote log manifest failed for table bucket {}. We will delete the"
                                    + " written remote log manifest file",
                            tableBucket);
                    remoteLogStorage.deleteRemoteLogManifestSnapshot(remoteLogManifestPath);
                    return false;
                } else {
                    // commit succeed.
                    if (newRemoteLogManifest.getVersion() == RemoteLogManifest.VERSION_1) {
                        // The legacy V1 writer mutates the in-memory indexes incrementally. V2
                        // commitAndApply has already installed the complete immutable snapshot.
                        remoteLogTablet.addAndDeleteLogSegments(newAddedSegments, expiredSegments);
                    }
                    LogTablet logTablet = replica.getLogTablet();
                    logTablet.updateRemoteLogStartOffset(newRemoteLogStartOffset);
                    // make the local log cleaner clean log segments that are committed to remote.
                    logTablet.updateRemoteLogEndOffset(newRemoteLogEndOffset);
                    logTablet.updateRemoteLogSize(newRemoteLogSize);
                    return true;
                }
            } catch (Exception e) {
                // the commit failed with unexpected exception, like network error, we will
                // retry send.
                LOG.error(
                        "The {} time try to commit remote log manifest failed for bucket {}.",
                        retrySendCommitTimes,
                        tableBucket,
                        e);
                retrySendCommitTimes++;
            }
        }

        LOG.error(
                "Commit remote log manifest failed after retry 10 times for table-bucket {}. "
                        + "We will ignore this commit but don't delete the remote log "
                        + "manifest file",
                tableBucket);
        return false;
    }

    private boolean commitRemoteLogManifest(
            CommitRemoteLogManifestData data, RemoteLogManifest manifest) throws Exception {
        if (data.isV2CasCommit()) {
            return remoteLogManifestCommitter.commitAndApply(data, manifest, remoteLog)
                    == RemoteLogManifestCommitResult.COMMITTED;
        }
        CommitRemoteLogManifestRequest request = makeCommitRemoteLogManifestRequest(data);
        return coordinatorGateway.commitRemoteLogManifest(request).get().isCommitSuccess();
    }

    private Path toPathIfExists(File file) {
        return file.exists() ? file.toPath() : null;
    }

    private void maybeInitializeNextCopyOffset(LogTablet logTablet) {
        if (nextCopyOffset == null) {
            nextCopyOffset = findNextCopyOffset(logTablet);
            LOG.info(
                    "Found the next remote copy offset: {} for bucket {} after becoming leader",
                    nextCopyOffset,
                    tableBucket);
        }
    }

    private long findNextCopyOffset(LogTablet logTablet) {
        OptionalLong remoteLogEndOffsetOpt = remoteLog.getRemoteLogEndOffset();
        long nextOffset;
        if (remoteLogEndOffsetOpt.isPresent()) {
            long remoteLogEndOffset = remoteLogEndOffsetOpt.getAsLong();
            long localEndOffset = logTablet.localLogEndOffset();
            if (localEndOffset <= remoteLogEndOffset) {
                LOG.warn(
                        "Local end offset should be greater than remote end offset, "
                                + "but the offset of bucket {} is local: {} and remote: {}. "
                                + "Reset remote end offset to local end offset.",
                        tableBucket,
                        localEndOffset,
                        remoteLogEndOffset);
                nextOffset = localEndOffset;
            } else {
                nextOffset = remoteLogEndOffset;
            }
        } else {
            nextOffset = 0L;
        }

        return nextOffset;
    }

    /**
     * Returns up to {@code maxUploadSegmentsPerTask} segments eligible for copying to remote
     * storage. A segment is eligible if it meets the following criteria:
     *
     * <p>1. Segment is not the active segment.
     *
     * <p>2. The segment's exclusive end offset is not greater than the high watermark, as remote
     * storage should contain only committed/acked records.
     *
     * <p>The number of returned segments is capped at {@code maxUploadSegmentsPerTask} to prevent
     * overwhelming the remote storage when there is a large backlog.
     */
    private List<EnrichedLogSegment> candidateLogSegments(
            LogTablet log, long fromOffset, long highWatermark) {
        List<EnrichedLogSegment> candidateLogSegments = new ArrayList<>();
        List<LogSegment> segments = log.logSegments(fromOffset, Long.MAX_VALUE);
        if (!segments.isEmpty()) {
            for (int idx = 1; idx < segments.size(); idx++) {
                LogSegment previousSeg = segments.get(idx - 1);
                LogSegment currentSeg = segments.get(idx);
                long curSegBaseOffset = currentSeg.getBaseOffset();
                if (curSegBaseOffset <= highWatermark) {
                    candidateLogSegments.add(new EnrichedLogSegment(previousSeg, curSegBaseOffset));
                    // Limit the number of segments to upload per task execution to prevent
                    // overwhelming the remote storage when there is a large backlog.
                    if (candidateLogSegments.size() >= maxUploadSegmentsPerTask) {
                        break;
                    }
                }
            }
            // Discard the last active segment
        }
        return candidateLogSegments;
    }

    /** Delete the remote log segment files. */
    private void deleteRemoteLogSegmentFiles(
            List<RemoteLogSegment> remoteLogSegmentList, TableMetricGroup metricGroup) {
        for (RemoteLogSegment remoteLogSegment : remoteLogSegmentList) {
            try {
                remoteLogStorage.deleteLogSegmentFiles(remoteLogSegment);
                metricGroup.remoteLogDeleteRequests().inc();
            } catch (Exception e) {
                LOG.error(
                        "Error occurred while deleting remote log segment files: {} for bucket {}, "
                                + "the delete files operation will be skipped.",
                        tableBucket,
                        remoteLogSegment,
                        e);
                metricGroup.remoteLogDeleteErrors().inc();
            }
        }
    }

    public void cancel() {
        cancelled = true;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public String toString() {
        return this.getClass() + "[" + tableBucket + "]";
    }

    private static final class CandidatePlan {
        private final EnrichedLogSegment enrichedLogSegment;
        private final RemoteLogSegment remoteLogSegment;
        private final PlanType planType;

        private CandidatePlan(
                EnrichedLogSegment enrichedLogSegment,
                RemoteLogSegment remoteLogSegment,
                PlanType planType) {
            this.enrichedLogSegment = enrichedLogSegment;
            this.remoteLogSegment = remoteLogSegment;
            this.planType = planType;
        }
    }

    private static class EnrichedLogSegment {
        private final LogSegment logSegment;
        private final long nextSegmentOffset;

        public EnrichedLogSegment(LogSegment logSegment, long nextSegmentOffset) {
            this.logSegment = logSegment;
            this.nextSegmentOffset = nextSegmentOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EnrichedLogSegment that = (EnrichedLogSegment) o;
            return nextSegmentOffset == that.nextSegmentOffset
                    && Objects.equals(logSegment, that.logSegment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(logSegment, nextSegmentOffset);
        }

        @Override
        public String toString() {
            return "EnrichedLogSegment{"
                    + "logSegment="
                    + logSegment
                    + ", nextSegmentOffset="
                    + nextSegmentOffset
                    + '}';
        }
    }
}
