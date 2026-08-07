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
import org.apache.fluss.utils.ExceptionUtils;
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
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;
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
    private final BooleanSupplier manifestV2WriterEnabledSupplier;
    private final long manifestV2GcGracePeriodMs;

    // The copied offset is empty initially for a new leader LogTieringTask, and needs to
    // be fetched inside the task's run() method.
    /** Exclusive offset from which the next remote copy should resume. */
    private volatile Long nextCopyOffset = null;

    private volatile boolean coordinatorV2WriterReady = true;

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
            BooleanSupplier manifestV2WriterEnabled,
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
        this.manifestV2WriterEnabledSupplier = manifestV2WriterEnabled;
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
                        "Current task for table-bucket {} received an error but will be scheduled again.",
                        tableBucket,
                        ex);
            }
        }
    }

    private void runOnce() throws Exception {
        if (isCancelled()) {
            LOG.info("Returning from LogTieringTask runOnce as the task state is changed");
            return;
        }

        LogTablet logTablet = replica.getLogTablet();
        TableMetricGroup metricGroup = replica.tableMetrics();
        maybeInitializeNextCopyOffset(logTablet);
        if (manifestV2WriterEnabledSupplier.getAsBoolean()) {
            runOnceV2(logTablet, metricGroup, true, !coordinatorV2WriterReady);
        } else {
            runOnceV1(logTablet, metricGroup);
        }
    }

    private void runOnceV1(LogTablet logTablet, TableMetricGroup metricGroup) throws Exception {
        if (remoteLog.currentManifest().getVersion() == RemoteLogManifest.VERSION_2) {
            LOG.info(
                    "Skipping the Manifest V1 tiering path for {} because its authoritative "
                            + "manifest is already V2. The local writer gate has not converged yet.",
                    tableBucket);
            return;
        }

        // Get these candidate log segments to copy and these expired remote log segments to clean
        // up.
        List<EnrichedLogSegment> candidateToCopySegments = candidateToCopyLogSegments(logTablet);
        // Only delete segments that have been tiered to lake to ensure data safety
        List<RemoteLogSegment> expiredRemoteLogSegments =
                remoteLog.expiredRemoteLogSegments(
                        clock.milliseconds(),
                        logTablet.isDataLakeEnabled() ? logTablet.getLakeLogEndOffset() : null);

        // 1. For these candidateToCopySegments, we will first copy segment files to remote before
        // commit the remote log manifest.
        List<RemoteLogSegment> copiedSegments = new ArrayList<>();
        long endOffset =
                copyLogSegmentFilesToRemote(
                        logTablet, candidateToCopySegments, copiedSegments, metricGroup);

        // 2. try to commit the remote log manifest snapshot to coordinator server and update the
        // local cache of remote log manifest.
        if (!copiedSegments.isEmpty() || !expiredRemoteLogSegments.isEmpty()) {
            boolean success =
                    tryToCommitRemoteLogManifestV1(
                            remoteLog, expiredRemoteLogSegments, copiedSegments);

            if (success) {
                if (!expiredRemoteLogSegments.isEmpty()) {
                    // 3. Delete expired remote log segment files only after committing the remote
                    // log manifest.
                    // TODO introduce the read reference count to avoid deleting remote log segments
                    // while there are readers is in progress.
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
                    // 4. For these copiedSegments, if snapshot commit failed, we need to delete
                    // remote log segment files already copied in step 1.
                    deleteRemoteLogSegmentFiles(copiedSegments, metricGroup);
                }
            }
        }
    }

    private void runOnceV2(
            LogTablet logTablet,
            TableMetricGroup metricGroup,
            boolean allowConflictReplan,
            boolean probeCoordinatorOnly)
            throws Exception {
        RemoteLogTablet.ManifestSnapshot baseSnapshot = remoteLog.currentManifestSnapshot();
        RemoteLogManifest baseManifest = baseSnapshot.manifest();
        VersionedRemoteLogManifestHandle baseHandle = baseSnapshot.handle();

        long now = clock.milliseconds();
        long targetGeneration =
                baseHandle == null
                        ? 1L
                        : baseHandle.handle().getManifestGeneration().orElse(0L) + 1L;
        boolean migrationChanged = baseManifest.getVersion() == RemoteLogManifest.VERSION_1;
        RemoteLogManifest normalizedBase;
        Long migrationRebuildStartOffset = null;
        if (baseManifest.getVersion() == RemoteLogManifest.VERSION_1) {
            RemoteLogManifestV2Migration.Result migration =
                    RemoteLogManifestV2Migration.migrate(baseManifest, targetGeneration);
            normalizedBase = migration.resultManifest();
            if (migration.requiresRebuild()) {
                migrationRebuildStartOffset = logTablet.localLogStartOffset();
                long previousRemoteStartOffset = baseManifest.getRemoteLogStartOffset();
                boolean locallyRecoverable =
                        migrationRebuildStartOffset <= previousRemoteStartOffset;
                boolean lakeRecoverable =
                        logTablet.isDataLakeEnabled()
                                && logTablet.canFetchFromLakeLog(previousRemoteStartOffset)
                                && logTablet.getLakeLogEndOffset() >= migrationRebuildStartOffset;
                if (!locallyRecoverable && !lakeRecoverable) {
                    LOG.warn(
                            "Deferring gapped V1 manifest migration for bucket {} because neither "
                                    + "local log [{}, ...) nor lake log [{}, {}) covers the old "
                                    + "remote start offset {} through the rebuild point",
                            tableBucket,
                            migrationRebuildStartOffset,
                            logTablet.getLakeLogStartOffset(),
                            logTablet.getLakeLogEndOffset(),
                            previousRemoteStartOffset);
                    return;
                }
                // A capability probe must not publish the empty recovery base before its active
                // range has been rebuilt.
                probeCoordinatorOnly = false;
                LOG.warn(
                        "Rebuilding gapped V1 manifest for bucket {} from local log start offset {} "
                                + "after detecting V1 gap [{}, {})",
                        tableBucket,
                        migrationRebuildStartOffset,
                        migration.gapStartOffset(),
                        migration.gapEndOffset());
            }
        } else {
            normalizedBase = normalizeV2Generation(baseManifest, targetGeneration);
        }
        RemoteLogManifest afterGarbageCollection =
                collectUnreferencedSegments(normalizedBase, now, metricGroup);
        boolean garbageCollectionChanged = afterGarbageCollection != normalizedBase;
        RemoteLogManifest eligibleBase =
                baseManifest.getVersion() == RemoteLogManifest.VERSION_2
                        ? RemoteLogManifestReplacementPlanner.markUnreferencedSegmentsGcEligible(
                                afterGarbageCollection, now)
                        : afterGarbageCollection;
        boolean eligibilityChanged = eligibleBase != afterGarbageCollection;
        List<EnrichedLogSegment> candidates =
                probeCoordinatorOnly
                        ? Collections.emptyList()
                        : candidateToCopyLogSegments(
                                logTablet,
                                migrationRebuildStartOffset == null
                                        ? nextCopyOffset
                                        : migrationRebuildStartOffset);
        RemoteLogManifest resultManifest = eligibleBase;
        List<RemoteLogSegment> copiedSegments = new ArrayList<>();
        long successfulNextCopyOffset = nextCopyOffset;
        for (EnrichedLogSegment candidate : candidates) {
            RemoteLogSegment remoteLogSegment = createRemoteLogSegment(candidate);
            RemoteLogManifestReplacementPlanner.Result planningResult =
                    migrationRebuildStartOffset != null
                                    && resultManifest.getRemoteLogSegmentList().isEmpty()
                            ? RemoteLogManifestReplacementPlanner.initialCopy(
                                    resultManifest, remoteLogSegment, migrationRebuildStartOffset)
                            : RemoteLogManifestReplacementPlanner.plan(
                                    resultManifest, remoteLogSegment);
            if (planningResult.planType() == PlanType.GAP) {
                long localLogStartOffset = logTablet.localLogStartOffset();
                if (resultManifest.getRemoteLogEndOffset() >= localLogStartOffset) {
                    throw new IllegalStateException(
                            String.format(
                                    "Remote gap [%s, %s) for bucket %s is still locally available",
                                    resultManifest.getRemoteLogEndOffset(),
                                    remoteLogSegment.remoteLogStartOffset(),
                                    tableBucket));
                }
                planningResult =
                        RemoteLogManifestReplacementPlanner.restartAfterGap(
                                resultManifest, remoteLogSegment, localLogStartOffset);
                LOG.info(
                        "Restarting Manifest V2 range at local log start offset {} for bucket {} "
                                + "because local retention removed remote gap [{}, {})",
                        localLogStartOffset,
                        tableBucket,
                        resultManifest.getRemoteLogEndOffset(),
                        remoteLogSegment.remoteLogStartOffset());
            }
            if (planningResult.planType() == PlanType.ALREADY_COVERED) {
                successfulNextCopyOffset = candidate.nextSegmentOffset;
                continue;
            }
            if (!copyPlannedSegment(logTablet, candidate, remoteLogSegment, metricGroup)) {
                break;
            }
            copiedSegments.add(remoteLogSegment);
            resultManifest = planningResult.resultManifest();
            successfulNextCopyOffset = candidate.nextSegmentOffset;
        }

        RemoteLogManifest beforeExpiration = resultManifest;
        resultManifest =
                RemoteLogManifestReplacementPlanner.expireContinuousPrefix(
                        resultManifest,
                        now,
                        replica.getLogTTLMs(),
                        logTablet.isDataLakeEnabled() ? logTablet.getLakeLogEndOffset() : null);
        boolean expirationChanged = resultManifest != beforeExpiration;
        if (copiedSegments.isEmpty()
                && !migrationChanged
                && !eligibilityChanged
                && !expirationChanged
                && !garbageCollectionChanged) {
            nextCopyOffset = successfulNextCopyOffset;
            return;
        }

        FsPath manifestPath;
        try {
            manifestPath = remoteLogStorage.writeRemoteLogManifestSnapshot(resultManifest);
        } catch (Exception e) {
            deleteRemoteLogSegmentFiles(copiedSegments, metricGroup);
            throw e;
        }

        CommitRemoteLogManifestData commitData =
                createV2CommitData(baseHandle, resultManifest, manifestPath);
        RemoteLogManifestCommitResult commitResult =
                remoteLogManifestCommitter.commitAndApply(commitData, resultManifest, remoteLog);

        if (commitResult == RemoteLogManifestCommitResult.COMMITTED) {
            coordinatorV2WriterReady = true;
            applyPublishedOffsets(logTablet, resultManifest);
            nextCopyOffset = successfulNextCopyOffset;
            if (probeCoordinatorOnly) {
                runOnceV2(logTablet, metricGroup, true, false);
            }
            return;
        }

        cleanupRejectedPlan(copiedSegments, manifestPath, metricGroup, logTablet);
        if (commitResult == RemoteLogManifestCommitResult.CONFLICT && allowConflictReplan) {
            coordinatorV2WriterReady = true;
            nextCopyOffset = findNextCopyOffset(logTablet);
            runOnceV2(logTablet, metricGroup, false, false);
        } else if (commitResult == RemoteLogManifestCommitResult.V2_WRITER_DISABLED) {
            coordinatorV2WriterReady = false;
            LOG.info(
                    "Falling back to Manifest V1 for {} because the coordinator V2 writer gate "
                            + "is disabled.",
                    tableBucket);
            runOnceV1(logTablet, metricGroup);
        }
    }

    private CommitRemoteLogManifestData createV2CommitData(
            VersionedRemoteLogManifestHandle baseHandle,
            RemoteLogManifest resultManifest,
            FsPath manifestPath) {
        if (baseHandle == null) {
            return CommitRemoteLogManifestData.v2CreateIfAbsent(
                    tableBucket,
                    manifestPath,
                    resultManifest.getRemoteLogStartOffset(),
                    resultManifest.getRemoteLogEndOffset(),
                    resultManifest.getHighestCopiedEndOffset(),
                    resultManifest.getGeneration(),
                    replica.getCoordinatorEpoch(),
                    replica.getBucketEpoch());
        }
        return CommitRemoteLogManifestData.v2CompareAndSet(
                tableBucket,
                manifestPath,
                resultManifest.getRemoteLogStartOffset(),
                resultManifest.getRemoteLogEndOffset(),
                resultManifest.getHighestCopiedEndOffset(),
                resultManifest.getGeneration(),
                baseHandle.zkVersion(),
                replica.getCoordinatorEpoch(),
                replica.getBucketEpoch());
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
                manifest.getHighestCopiedEndOffset(),
                retained);
    }

    private boolean gracePeriodElapsed(long timestampMs, long now) {
        return timestampMs >= 0L
                && timestampMs != Long.MAX_VALUE
                && timestampMs <= now
                && now - timestampMs >= manifestV2GcGracePeriodMs;
    }

    private RemoteLogManifest normalizeV2Generation(
            RemoteLogManifest baseManifest, long targetGeneration) {
        return RemoteLogManifest.createV2(
                targetGeneration,
                baseManifest.getPhysicalTablePath(),
                baseManifest.getTableBucket(),
                baseManifest.getRemoteLogSegmentList(),
                baseManifest.getRemoteLogSegmentList().isEmpty()
                        ? null
                        : baseManifest.getRemoteLogStartOffset(),
                baseManifest.getHighestCopiedEndOffset(),
                baseManifest.getUnreferencedRemoteLogSegments());
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
            LogTablet logTablet,
            EnrichedLogSegment candidate,
            RemoteLogSegment remoteLogSegment,
            TableMetricGroup metricGroup)
            throws Exception {
        LogSegment segment = candidate.logSegment;
        File writerIdSnapshotFile =
                logTablet
                        .writerStateManager()
                        .fetchSnapshot(candidate.nextSegmentOffset)
                        .orElse(null);
        LogSegmentFiles logSegmentFiles =
                new LogSegmentFiles(
                        segment.getFileLogRecords().file().toPath(),
                        toPathIfExists(segment.offsetIndex().file()),
                        toPathIfExists(segment.timeIndex().file()),
                        writerIdSnapshotFile == null ? null : writerIdSnapshotFile.toPath());
        try {
            remoteLogStorage.copyLogSegmentFiles(remoteLogSegment, logSegmentFiles);
        } catch (RemoteStorageException e) {
            metricGroup.remoteLogCopyErrors().inc();
            Optional<InterruptedException> interruption =
                    ExceptionUtils.findThrowable(e, InterruptedException.class);
            if (interruption.isPresent()) {
                Thread.currentThread().interrupt();
                throw interruption.get();
            }
            LOG.warn(
                    "Failed to copy planned Manifest V2 segment {} for bucket {}",
                    remoteLogSegment,
                    tableBucket,
                    e);
            return false;
        }
        metricGroup.remoteLogCopyRequests().inc();
        metricGroup.remoteLogCopyBytes().inc(remoteLogSegment.segmentSizeInBytes());
        return true;
    }

    private void cleanupRejectedPlan(
            List<RemoteLogSegment> copiedSegments,
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

        for (RemoteLogSegment copiedSegment : copiedSegments) {
            if (authoritativeSegmentIds.contains(copiedSegment.remoteLogSegmentId())) {
                throw new IllegalStateException(
                        "Rejected plan segment is referenced by the authoritative manifest: "
                                + copiedSegment.remoteLogSegmentId());
            }
        }
        deleteRemoteLogSegmentFiles(copiedSegments, metricGroup);
        remoteLogStorage.deleteRemoteLogManifestSnapshot(manifestPath);
    }

    private void applyPublishedOffsets(LogTablet logTablet, RemoteLogManifest manifest) {
        logTablet.updateRemoteLogStartOffset(manifest.getRemoteLogStartOffset());
        logTablet.updateRemoteLogEndOffset(manifest.getRemoteLogEndOffset());
        logTablet.updateHighestCopiedEndOffset(manifest.getHighestCopiedEndOffset());
        logTablet.updateRemoteLogSize(manifest.getRemoteLogSize());
    }

    private List<EnrichedLogSegment> candidateToCopyLogSegments(LogTablet log) {
        return candidateToCopyLogSegments(log, nextCopyOffset);
    }

    private List<EnrichedLogSegment> candidateToCopyLogSegments(LogTablet log, long copyOffset) {
        List<EnrichedLogSegment> candidateLogSegments = new ArrayList<>();
        // Get highWatermark.
        long highWatermark = log.getHighWatermark();
        if (highWatermark < 0) {
            LOG.warn(
                    "The highWatermark for bucket {} is {}, which should not be negative",
                    tableBucket,
                    highWatermark);
        } else if (highWatermark > 0 && copyOffset < highWatermark) {
            // local-log-start-offset can be ahead of the next-copy-offset, when enabling the
            // remote log for the first time
            long fromOffset = Math.max(copyOffset, log.localLogStartOffset());
            candidateLogSegments = candidateLogSegments(log, fromOffset, highWatermark);
            LOG.debug(
                    "Candidate log segments for bucket {}: logLocalStartOffset: {}, nextCopyOffset: {}, "
                            + "fromOffset: {}, highWatermark: {} and candidateLogSegments: {}",
                    tableBucket,
                    log.localLogStartOffset(),
                    copyOffset,
                    fromOffset,
                    highWatermark,
                    candidateLogSegments);
            if (candidateLogSegments.isEmpty()) {
                LOG.debug(
                        "no segments found to be copied for bucket {} which next-copy-offset: {} and active segment's base-offset: {}",
                        tableBucket,
                        copyOffset,
                        log.activeLogSegment().getBaseOffset());
            }
        } else {
            LOG.debug(
                    "Skipping copying segments for bucket {} to remote, next-copy-offset:{}, and highWatermark:{}",
                    tableBucket,
                    copyOffset,
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
    private boolean tryToCommitRemoteLogManifestV1(
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
                                        replica.getBucketEpoch()));
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
                    remoteLogTablet.addAndDeleteLogSegments(newAddedSegments, expiredSegments);
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

    private boolean commitRemoteLogManifest(CommitRemoteLogManifestData data) throws Exception {
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
        long highestCopiedEndOffset = remoteLog.getHighestCopiedEndOffset();
        long nextOffset;
        if (highestCopiedEndOffset >= 0L) {
            long localEndOffset = logTablet.localLogEndOffset();
            if (localEndOffset <= highestCopiedEndOffset) {
                LOG.warn(
                        "Local end offset should be greater than highest copied end offset, "
                                + "but the offset of bucket {} is local: {} and remote: {}. "
                                + "Reset remote end offset to local end offset.",
                        tableBucket,
                        localEndOffset,
                        highestCopiedEndOffset);
                nextOffset = localEndOffset;
            } else {
                nextOffset = highestCopiedEndOffset;
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
