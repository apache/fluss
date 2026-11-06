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

import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestResponse;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TableMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
    private final Clock clock;
    private final int maxUploadSegmentsPerTask;

    // The copied offset is empty initially for a new leader LogTieringTask, and needs to
    // be fetched inside the task's run() method.
    private volatile Long copiedOffset = null;

    private volatile boolean cancelled = false;
    private final Object commitLock = new Object();
    private final ZooKeeperClient zkClient;
    private volatile PendingCommit pendingCommit;
    private boolean refreshRequired;
    private boolean running;

    public LogTieringTask(
            Replica replica,
            RemoteLogTablet remoteLog,
            RemoteLogStorage remoteLogStorage,
            RemoteLogIndexCache remoteLogIndexCache,
            CoordinatorGateway coordinatorGateway,
            ZooKeeperClient zkClient,
            Clock clock,
            int maxUploadSegmentsPerTask) {
        this.replica = replica;
        this.remoteLog = remoteLog;
        this.physicalTablePath = replica.getPhysicalTablePath();
        this.tableBucket = replica.getTableBucket();
        this.remoteLogStorage = remoteLogStorage;
        this.remoteLogIndexCache = remoteLogIndexCache;
        this.coordinatorGateway = coordinatorGateway;
        this.zkClient = zkClient;
        this.clock = clock;
        this.maxUploadSegmentsPerTask = maxUploadSegmentsPerTask;
    }

    @Override
    public void run() {
        synchronized (commitLock) {
            if (cancelled || running) {
                return;
            }
            running = true;
        }
        try {
            runOnce();
        } catch (InterruptedException e) {
            if (!cancelled) {
                LOG.warn("Remote log task for {} was interrupted.", tableBucket, e);
            }
        } finally {
            PendingCommit retired = null;
            synchronized (commitLock) {
                running = false;
                if (cancelled) {
                    retired = pendingCommit;
                    pendingCommit = null;
                }
            }
            if (retired != null) {
                cleanupRetired(retired);
            }
        }
    }

    /**
     * Transfers idle cancelled work to the existing worker executor without blocking cancellation.
     */
    void cleanupAfterCancellation(Executor executor) {
        PendingCommit retired;
        synchronized (commitLock) {
            if (!cancelled || running) {
                return;
            }
            retired = pendingCommit;
            pendingCommit = null;
        }
        if (retired != null) {
            try {
                executor.execute(() -> cleanupRetired(retired));
            } catch (RuntimeException e) {
                LOG.warn(
                        "Could not schedule retirement cleanup for manifest {} of {}.",
                        retired.manifestPath,
                        tableBucket,
                        e);
            }
        }
    }

    private void cleanupRetired(PendingCommit batch) {
        if (batch.confirmed) {
            if (batch.phase == CommitPhase.PUBLISH) {
                batch.cleanupSegments =
                        segmentsToDeleteAfterCommit(
                                batch.previous, batch.uploadedSegments, batch.manifest);
                batch.cleanupIndex = 0;
            }
        } else if (batch.submitted && batch.phase != CommitPhase.CLEANUP) {
            LOG.warn(
                    "Retaining unresolved manifest {} for retired task {}.",
                    batch.manifestPath,
                    tableBucket);
            return;
        }
        try {
            cleanupPending(batch);
        } catch (Exception e) {
            LOG.warn(
                    "Retirement cleanup failed for manifest {} of {}.",
                    batch.manifestPath,
                    tableBucket,
                    e);
        }
    }

    private void runOnce() throws InterruptedException {
        PendingCommit batch = pendingCommit;
        try {
            if (isCancelled()) {
                return;
            }
            if (refreshRequired && batch == null) {
                refreshManifest();
            }
            if (batch == null) {
                LogTablet log = replica.getLogTablet();
                maybeUpdateCopiedOffset(log);
                List<EnrichedLogSegment> candidates = candidateToCopyLogSegments(log);
                TableConfig tableConfig = replica.getTableInfo().getTableConfig();
                List<RemoteLogSegment> expired =
                        remoteLog.expiredRemoteLogSegments(
                                clock.milliseconds(),
                                tableConfig.isDataLakeEnabled() ? log.getLakeLogEndOffset() : null,
                                tableConfig.getLogTTLMs());
                if (candidates.isEmpty() && expired.isEmpty()) {
                    return;
                }
                batch =
                        new PendingCommit(
                                remoteLog.currentManifest(), remoteLog.currentManifestPath());
                synchronized (commitLock) {
                    if (cancelled) {
                        return;
                    }
                    pendingCommit = batch;
                }
                try {
                    copyLogSegmentFilesToRemote(
                            log,
                            candidates,
                            batch.copiedSegments,
                            batch.uploadedSegments,
                            replica.tableMetrics());
                    if (!batch.copiedSegments.isEmpty() || !expired.isEmpty()) {
                        batch.manifest = batch.previous.trimAndMerge(expired, batch.copiedSegments);
                        batch.manifestPath =
                                remoteLogStorage.writeRemoteLogManifestSnapshot(
                                        remoteLog.getRemoteLogDir(), batch.manifest);
                        batch.phase = CommitPhase.COMMIT;
                    }
                } catch (Exception e) {
                    try {
                        cleanupPending(batch);
                    } catch (Exception cleanupFailure) {
                        e.addSuppressed(cleanupFailure);
                    }
                    throw e;
                }
            }
            resumePending(batch);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            if (!isCancelled()) {
                LOG.warn(
                        "Remote log round for {} will be retried without new uploads.",
                        tableBucket,
                        e);
            }
        }
    }

    private void resumePending(PendingCommit batch) throws Exception {
        if (batch.phase == CommitPhase.COMMIT) {
            if (!commitPending(batch)) {
                return;
            }
        }
        if (batch.phase == CommitPhase.PUBLISH) {
            synchronized (commitLock) {
                if (cancelled) {
                    return;
                }
                remoteLog.loadRemoteLogManifest(
                        batch.manifest,
                        batch.manifestPath == null ? "" : batch.manifestPath.toString());
                LogTablet log = replica.getLogTablet();
                log.publishRemoteLogOffsets(
                        batch.manifest.getRemoteLogStartOffset(),
                        batch.manifest.getRemoteLogEndOffset(),
                        batch.manifest.getHighestCopiedEndOffset());
                log.updateRemoteLogSize(batch.manifest.getRemoteLogSize());
                batch.cleanupSegments =
                        segmentsToDeleteAfterCommit(
                                batch.previous, batch.uploadedSegments, batch.manifest);
                batch.phase = CommitPhase.CLEANUP;
            }
        }
        if (batch.phase == CommitPhase.CLEANUP) {
            cleanupPending(batch);
        }
    }

    private void cleanupPending(PendingCommit batch) throws Exception {
        while (batch.cleanupIndex < batch.cleanupSegments.size()) {
            RemoteLogSegment segment = batch.cleanupSegments.get(batch.cleanupIndex);
            try {
                remoteLogStorage.deleteLogSegmentFiles(remoteLog.getRemoteLogDir(), segment);
                replica.tableMetrics().remoteLogDeleteRequests().inc();
                remoteLogIndexCache.removeAll(
                        Collections.singletonList(segment.remoteLogSegmentId()));
                batch.cleanupIndex++;
            } catch (Exception e) {
                replica.tableMetrics().remoteLogDeleteErrors().inc();
                throw e;
            }
        }
        if (!batch.confirmed && batch.manifestPath != null) {
            remoteLogStorage.deleteRemoteLogManifestSnapshot(batch.manifestPath);
        }
        if (batch.confirmed && !cancelled) {
            // This method reads the currently published boundary; it performs I/O outside
            // commitLock.
            replica.getLogTablet().deleteSegmentsAlreadyExistsInRemote();
            copiedOffset = findCopiedOffset(replica.getLogTablet());
        }
        synchronized (commitLock) {
            if (pendingCommit == batch) {
                pendingCommit = null;
            }
        }
    }

    private void refreshManifest() throws Exception {
        Optional<RemoteLogManifestHandle> handle = zkClient.getRemoteLogManifestHandle(tableBucket);
        RemoteLogManifest manifest =
                handle.isPresent()
                        ? remoteLogStorage.readRemoteLogManifestSnapshot(
                                handle.get().getRemoteLogManifestPath())
                        : new RemoteLogManifest(physicalTablePath, tableBucket, new ArrayList<>());
        synchronized (commitLock) {
            if (cancelled) {
                return;
            }
            remoteLog.loadRemoteLogManifest(
                    manifest, handle.map(h -> h.getRemoteLogManifestPath().toString()).orElse(""));
            replica.getLogTablet()
                    .publishRemoteLogOffsets(
                            manifest.getRemoteLogStartOffset(),
                            manifest.getRemoteLogEndOffset(),
                            manifest.getHighestCopiedEndOffset());
            replica.getLogTablet().updateRemoteLogSize(manifest.getRemoteLogSize());
            copiedOffset = findCopiedOffset(replica.getLogTablet());
            refreshRequired = false;
        }
        replica.getLogTablet().deleteSegmentsAlreadyExistsInRemote();
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
        } else if (highWatermark > 0 && copiedOffset < highWatermark) {
            // local-log-start-offset can be ahead of the copied-offset, when enabling the
            // remote log for the first time
            long fromOffset = Math.max(copiedOffset + 1, log.localLogStartOffset());
            candidateLogSegments = candidateLogSegments(log, fromOffset, highWatermark);
            LOG.debug(
                    "Candidate log segments for bucket {}: logLocalStartOffset: {}, copiedOffset: {}, "
                            + "fromOffset: {}, highWatermark: {} and candidateLogSegments: {}",
                    tableBucket,
                    log.localLogStartOffset(),
                    copiedOffset,
                    fromOffset,
                    highWatermark,
                    candidateLogSegments);
            if (candidateLogSegments.isEmpty()) {
                LOG.debug(
                        "no segments found to be copied for bucket {} which copied-offset: {} and active segment's base-offset: {}",
                        tableBucket,
                        copiedOffset,
                        log.activeLogSegment().getBaseOffset());
            }
        } else {
            LOG.debug(
                    "Skipping copying segments for bucket {} to remote, current read-offset:{}, and highWatermark:{}",
                    tableBucket,
                    copiedOffset,
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
     */
    private void copyLogSegmentFilesToRemote(
            LogTablet log,
            List<EnrichedLogSegment> segments,
            List<RemoteLogSegment> copiedSegments,
            List<RemoteLogSegment> uploadedSegments,
            TableMetricGroup metricGroup)
            throws Exception {
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
            uploadedSegments.add(copyRemoteLogSegment);
            try {
                remoteLogStorage.copyLogSegmentFiles(
                        remoteLog.getRemoteLogDir(), copyRemoteLogSegment, logSegmentFiles);
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
        }
    }

    private boolean commitPending(PendingCommit batch) throws Exception {
        for (int attempt = 1; attempt <= 10; attempt++) {
            CommitRemoteLogManifestRequest request =
                    makeCommitRemoteLogManifestRequest(
                            new CommitRemoteLogManifestData(
                                    tableBucket,
                                    batch.manifestPath,
                                    batch.manifest.getRemoteLogStartOffset(),
                                    batch.manifest.getRemoteLogEndOffset(),
                                    batch.manifest.getHighestCopiedEndOffset(),
                                    batch.coordinatorEpoch,
                                    batch.bucketEpoch,
                                    batch.legacy ? null : batch.expectedPath));
            CompletableFuture<CommitRemoteLogManifestResponse> response;
            try {
                synchronized (commitLock) {
                    if (cancelled) {
                        return false;
                    }
                    batch.submitted = true;
                    response = coordinatorGateway.commitRemoteLogManifest(request);
                }
                if (response.get().isCommitSuccess()) {
                    batch.confirmed = true;
                    batch.phase = CommitPhase.PUBLISH;
                    return true;
                }
                if (batch.legacy) {
                    // v0 false also covers an uncertain ZooKeeper write.
                    batch.unknown = true;
                    return false;
                }
                if (batch.unknown) {
                    return resolveUnknownRejection(batch);
                }
                refreshRequired = true;
                batch.phase = CommitPhase.CLEANUP;
                return true;
            } catch (InterruptedException e) {
                batch.unknown = true;
                throw e;
            } catch (Exception e) {
                Throwable cause = ExceptionUtils.stripExecutionException(e);
                if (cause instanceof UnsupportedVersionException && !batch.legacy) {
                    // The transport rejects conditional v1 requests before dispatch to a v0 peer.
                    if (batch.unknown) {
                        // An uncertain conditional submission must not be replayed unconditionally.
                        return false;
                    }
                    batch.submitted = false;
                    batch.legacy = true;
                    continue;
                }
                batch.unknown = true;
                LOG.debug(
                        "Remote log commit attempt {} for {} has an unknown outcome.",
                        attempt,
                        tableBucket,
                        e);
            }
        }
        return false;
    }

    private boolean resolveUnknownRejection(PendingCommit batch) throws Exception {
        Optional<RemoteLogManifestHandle> handle = zkClient.getRemoteLogManifestHandle(tableBucket);
        String currentPath = handle.map(h -> h.getRemoteLogManifestPath().toString()).orElse("");
        if (currentPath.equals(batch.expectedPath)
                || currentPath.equals(batch.manifestPath.toString())) {
            // A transient validation failure did not move the head. Keep the same upload owner.
            return false;
        }
        RemoteLogManifest current =
                handle.isPresent()
                        ? remoteLogStorage.readRemoteLogManifestSnapshot(
                                handle.get().getRemoteLogManifestPath())
                        : new RemoteLogManifest(physicalTablePath, tableBucket, new ArrayList<>());
        // The head advanced past the predecessor: delayed conditional requests cannot install this
        // candidate. Transfer files referenced by that head, and retain ownership of the rest until
        // cleanup succeeds. A possibly committed manifest snapshot follows the existing history
        // policy.
        batch.manifest = current;
        batch.manifestPath =
                handle.map(RemoteLogManifestHandle::getRemoteLogManifestPath).orElse(null);
        batch.confirmed = true;
        batch.phase = CommitPhase.PUBLISH;
        return true;
    }

    private List<RemoteLogSegment> segmentsToDeleteAfterCommit(
            RemoteLogManifest previousManifest,
            List<RemoteLogSegment> copiedSegments,
            RemoteLogManifest newManifest) {
        List<RemoteLogSegment> segmentsToDelete = new ArrayList<>();
        Set<UUID> activeSegmentIds =
                newManifest.getRemoteLogSegmentList().stream()
                        .map(RemoteLogSegment::remoteLogSegmentId)
                        .collect(Collectors.toSet());
        Set<UUID> segmentIdsToDelete = new HashSet<>();
        addInactiveSegments(
                segmentsToDelete,
                segmentIdsToDelete,
                activeSegmentIds,
                previousManifest.getRemoteLogSegmentList());
        addInactiveSegments(segmentsToDelete, segmentIdsToDelete, activeSegmentIds, copiedSegments);
        return segmentsToDelete;
    }

    private void addInactiveSegments(
            List<RemoteLogSegment> target,
            Set<UUID> targetIds,
            Set<UUID> activeSegmentIds,
            List<RemoteLogSegment> candidates) {
        for (RemoteLogSegment candidate : candidates) {
            UUID segmentId = candidate.remoteLogSegmentId();
            if (!activeSegmentIds.contains(segmentId) && targetIds.add(segmentId)) {
                target.add(candidate);
            }
        }
    }

    private Path toPathIfExists(File file) {
        return file.exists() ? file.toPath() : null;
    }

    private void maybeUpdateCopiedOffset(LogTablet logTablet) {
        if (copiedOffset == null) {
            copiedOffset = findCopiedOffset(logTablet);
            LOG.info(
                    "Found copied offset {} for bucket {} after becoming leader",
                    copiedOffset,
                    tableBucket);
        }
    }

    private long findCopiedOffset(LogTablet logTablet) {
        long highestCopiedEndOffset = remoteLog.getHighestCopiedEndOffset();
        if (highestCopiedEndOffset < 0L) {
            return -1L;
        }

        long localEndOffset = logTablet.localLogEndOffset();
        if (localEndOffset < highestCopiedEndOffset) {
            LOG.warn(
                    "Local end offset is behind the highest copied end offset for bucket {}: "
                            + "local={}, copied={}. Reset copied progress to the local end.",
                    tableBucket,
                    localEndOffset,
                    highestCopiedEndOffset);
        }
        return Math.min(localEndOffset, highestCopiedEndOffset) - 1L;
    }

    /**
     * Returns up to {@code maxUploadSegmentsPerTask} segments eligible for copying to remote
     * storage. A segment is eligible if it meets the following criteria:
     *
     * <p>1. Segment is not the active segment.
     *
     * <p>2. Segment end-offset is less than the highWatermark as remote storage should contain only
     * committed/acked records.
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

    public void cancel() {
        synchronized (commitLock) {
            cancelled = true;
        }
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public String toString() {
        return this.getClass() + "[" + tableBucket + "]";
    }

    private enum CommitPhase {
        COMMIT,
        PUBLISH,
        CLEANUP
    }

    /** One owned upload and cleanup set; no streams or local files are retained between rounds. */
    private final class PendingCommit {
        private final RemoteLogManifest previous;
        private final String expectedPath;
        private final int coordinatorEpoch = replica.getCoordinatorEpoch();
        private final int bucketEpoch = replica.getBucketEpoch();
        private final List<RemoteLogSegment> copiedSegments = new ArrayList<>();
        private final List<RemoteLogSegment> uploadedSegments = new ArrayList<>();
        private List<RemoteLogSegment> cleanupSegments = uploadedSegments;
        private int cleanupIndex;
        private RemoteLogManifest manifest;
        private FsPath manifestPath;
        private CommitPhase phase = CommitPhase.CLEANUP;
        private boolean unknown;
        private boolean submitted;
        private boolean confirmed;
        private boolean legacy;

        private PendingCommit(RemoteLogManifest previous, String expectedPath) {
            this.previous = previous;
            this.expectedPath = expectedPath;
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
