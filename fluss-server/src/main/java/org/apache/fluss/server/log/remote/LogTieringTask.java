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
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.PhysicalTableMetricGroup;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;

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
    private final CoordinatorGateway coordinatorGateway;
    private final Clock clock;

    // The copied offset is empty initially for a new leader LogTieringTask, and needs to
    // be fetched inside the task's run() method.
    private volatile Long copiedOffset = null;

    private volatile boolean cancelled = false;

    public LogTieringTask(
            Replica replica,
            RemoteLogTablet remoteLog,
            RemoteLogStorage remoteLogStorage,
            CoordinatorGateway coordinatorGateway,
            Clock clock) {
        this.replica = replica;
        this.remoteLog = remoteLog;
        this.physicalTablePath = replica.getPhysicalTablePath();
        this.tableBucket = replica.getTableBucket();
        this.remoteLogStorage = remoteLogStorage;
        this.coordinatorGateway = coordinatorGateway;
        this.clock = clock;
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
            PhysicalTableMetricGroup metricGroup = replica.tableMetrics();
            maybeUpdateCopiedOffset(logTablet);

            // Get these candidate log segments to copy and these expired remote log segments to
            // clean up.
            List<EnrichedLogSegment> candidateToCopySegments =
                    candidateToCopyLogSegments(logTablet);
            List<RemoteLogSegment> expiredRemoteLogSegments =
                    remoteLog.expiredRemoteLogSegments(clock.milliseconds());

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
                    }

                    if (endOffset > 0) {
                        // endOffset is the next segment base offset, so we need to decrement it
                        // by 1 to get the last copied segment's highest offset.
                        copiedOffset = endOffset - 1;
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
                    "Candidate log segments: logLocalStartOffset: {}, copiedOffset: {}, "
                            + "fromOffset: {}, highWatermark: {} and candidateLogSegments: {}",
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
                    "Skipping copying segments to remote, current read-offset:{}, and highWatermark:{}",
                    copiedOffset,
                    highWatermark);
        }

        return candidateLogSegments;
    }

    /**
     * Copy the given log segments to remote and add the successfully copied segment to the {@code
     * copiedSegments} parameter.
     *
     * @return the end offset of the last segment copied to remote.
     */
    private long copyLogSegmentFilesToRemote(
            LogTablet log,
            List<EnrichedLogSegment> segments,
            List<RemoteLogSegment> copiedSegments,
            PhysicalTableMetricGroup metricGroup)
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
            endOffset = enrichedSegment.nextSegmentOffset;

            File writerIdSnapshotFile =
                    log.writerStateManager().fetchSnapshot(endOffset).orElse(null);
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
                            .remoteLogEndOffset(endOffset)
                            .maxTimestamp(segment.maxTimestampSoFar())
                            .segmentSizeInBytes(sizeInBytes)
                            .build();
            try {
                remoteLogStorage.copyLogSegmentFiles(copyRemoteLogSegment, logSegmentFiles);
            } catch (RemoteStorageException e) {
                metricGroup.remoteLogCopyErrors().inc();
                throw e;
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
            LOG.error("Write remote log manifest file to remote storage failed.", e);
            return false;
        }

        // 2. sending the CommitRemoteLogManifestRequest to coordinator server
        // to try to commit this snapshot.
        long newRemoteLogStartOffset = newRemoteLogManifest.getRemoteLogStartOffset();
        long newRemoteLogEndOffset = newRemoteLogManifest.getRemoteLogEndOffset();
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
                    // TODO: commit with version to avoid the manifest has been updated
                    remoteLogTablet.addAndDeleteLogSegments(newAddedSegments, expiredSegments);
                    LogTablet logTablet = replica.getLogTablet();
                    logTablet.updateRemoteLogStartOffset(newRemoteLogStartOffset);
                    // make the local log cleaner clean log segments that are committed to remote.
                    logTablet.updateRemoteLogEndOffset(newRemoteLogEndOffset);
                    return true;
                }
            } catch (Exception e) {
                // the commit failed with unexpected exception, like network error, we will
                // retry send.
                LOG.error(
                        "The {} time try to commit remote log manifest failed.",
                        retrySendCommitTimes,
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

    private void maybeUpdateCopiedOffset(LogTablet logTablet) {
        if (copiedOffset == null) {
            copiedOffset = findRemoteLogEndOffset(logTablet);
            LOG.info(
                    "Found the copied remote log end offset: {} for bucket {} after becoming leader",
                    copiedOffset,
                    tableBucket);
        }
    }

    private long findRemoteLogEndOffset(LogTablet logTablet) {
        OptionalLong remoteLogEndOffsetOpt = remoteLog.getRemoteLogEndOffset();
        long newRemoteLogEndOffset;
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
                newRemoteLogEndOffset = localEndOffset;
            } else {
                newRemoteLogEndOffset = remoteLogEndOffset;
            }
        } else {
            newRemoteLogEndOffset = -1L;
        }

        return newRemoteLogEndOffset;
    }

    /**
     * Segments which match the following criteria are eligible for copying to remote storage:
     *
     * <p>1. Segment is not the active segment.
     *
     * <p>2. Segment end-offset is less than the highWatermark as remote storage should contain only
     * committed/acked records.
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
                }
            }
            // Discard the last active segment
        }
        return candidateLogSegments;
    }

    /** Delete the remote log segment files. */
    private void deleteRemoteLogSegmentFiles(
            List<RemoteLogSegment> remoteLogSegmentList, PhysicalTableMetricGroup metricGroup) {
        for (RemoteLogSegment remoteLogSegment : remoteLogSegmentList) {
            try {
                remoteLogStorage.deleteLogSegmentFiles(remoteLogSegment);
                metricGroup.remoteLogDeleteRequests().inc();
            } catch (Exception e) {
                LOG.error(
                        "Error occurred while deleting remote log segment files: {}, "
                                + "the delete files operation will be skipped.",
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
