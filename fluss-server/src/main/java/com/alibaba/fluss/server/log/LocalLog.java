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

package com.alibaba.fluss.server.log;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.LogOffsetOutOfRangeException;
import com.alibaba.fluss.exception.LogStorageException;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metrics.Counter;
import com.alibaba.fluss.metrics.Histogram;
import com.alibaba.fluss.record.FileLogProjection;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.server.metrics.group.TabletServerMetricGroup;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.FileUtils.flushDirIfExists;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An append-only log for storing messages locally. The log is a sequence of LogSegments, each with
 * a base offset. New log segments are created according to a configurable policy that controls the
 * size in bytes or time interval for a given segment.
 *
 * <p>NOTE: this class is not thread-safe, and it relies on the thread safety provided by the Log
 * class.
 */
@NotThreadSafe
public final class LocalLog {
    private static final Logger LOG = LoggerFactory.getLogger(LocalLog.class);

    public static final long UNKNOWN_OFFSET = -1L;

    private final Configuration config;
    private final LogSegments segments;
    private final TableBucket tableBucket;
    private final LogFormat logFormat;
    // Last time the log was flushed
    private final AtomicLong lastFlushedTime;

    private final Counter flushCount;
    private final Histogram flushLatencyHistogram;

    private volatile File logTabletDir;
    private volatile long recoveryPoint;
    private volatile long localLogStartOffset;
    private volatile long localMaxTimestamp;
    private volatile LogOffsetMetadata nextOffsetMetadata;
    private volatile boolean isMemoryMappedBufferClosed = false;

    public LocalLog(
            File logTabletDir,
            Configuration config,
            TabletServerMetricGroup serverMetricGroup,
            LogSegments segments,
            long recoveryPoint,
            LogOffsetMetadata nextOffsetMetadata,
            TableBucket tableBucket,
            LogFormat logFormat)
            throws IOException {
        this.logTabletDir = logTabletDir;
        this.config = config;
        this.segments = segments;
        this.recoveryPoint = recoveryPoint;
        this.nextOffsetMetadata = nextOffsetMetadata;
        this.tableBucket = tableBucket;
        this.logFormat = logFormat;

        lastFlushedTime = new AtomicLong(System.currentTimeMillis());
        flushCount = serverMetricGroup.logFlushCount();
        flushLatencyHistogram = serverMetricGroup.logFlushLatencyHistogram();
        localLogStartOffset = segments.isEmpty() ? 0L : segments.firstSegmentBaseOffset().get();
        localMaxTimestamp =
                segments.isEmpty() ? 0L : segments.lastSegment().get().maxTimestampSoFar();
    }

    LogSegments getSegments() {
        return segments;
    }

    File getLogTabletDir() {
        return logTabletDir;
    }

    long getRecoveryPoint() {
        return recoveryPoint;
    }

    Histogram getFlushLatencyHistogram() {
        return flushLatencyHistogram;
    }

    Counter getFlushCount() {
        return flushCount;
    }

    /** The offset metadata of the next message that will be appended to the log. */
    @VisibleForTesting
    LogOffsetMetadata getLocalLogEndOffsetMetadata() {
        return nextOffsetMetadata;
    }

    long getLocalLogEndOffset() {
        return nextOffsetMetadata.getMessageOffset();
    }

    public long getLocalLogStartOffset() {
        return localLogStartOffset;
    }

    public long getLocalMaxTimestamp() {
        return localMaxTimestamp;
    }

    TableBucket getTableBucket() {
        return tableBucket;
    }

    long unflushedMessages() {
        return nextOffsetMetadata.getMessageOffset() - recoveryPoint;
    }

    boolean renameDir(String name) throws IOException {
        File renamedDir = new File(logTabletDir.getParent(), name);
        FileUtils.atomicMoveWithFallback(logTabletDir.toPath(), renamedDir.toPath());
        if (!renamedDir.getAbsolutePath().equals(logTabletDir.getAbsolutePath())) {
            logTabletDir = renamedDir;
            segments.updateParentDir(renamedDir);
            return true;
        } else {
            return false;
        }
    }

    void checkIfMemoryMappedBufferClosed() {
        if (isMemoryMappedBufferClosed) {
            throw new FlussRuntimeException(
                    "The memory mapped buffer for log of " + tableBucket + " is already closed");
        }
    }

    private void updateRecoveryPoint(long newRecoveryPoint) {
        recoveryPoint = newRecoveryPoint;
    }

    /**
     * Update recoveryPoint to provided offset and mark the log as flushed, if the offset is greater
     * than the existing recoveryPoint.
     *
     * @param offset the offset to be updated
     */
    void markFlushed(long offset) {
        checkIfMemoryMappedBufferClosed();
        if (offset > recoveryPoint) {
            updateRecoveryPoint(offset);
            lastFlushedTime.set(System.currentTimeMillis());
        }
    }

    /**
     * Flush local log segments for all offsets up to offset - 1. Does not update the recovery
     * point.
     *
     * @param offset The offset to flush up to (non-inclusive)
     */
    void flush(long offset) throws IOException {
        long currentRecoverPoint = recoveryPoint;
        if (currentRecoverPoint <= offset) {
            List<LogSegment> segmentsToFlush = segments.values(currentRecoverPoint, offset);
            for (LogSegment segment : segmentsToFlush) {
                long start = System.nanoTime();
                segment.flush();
                flushCount.inc();
                flushLatencyHistogram.update((System.nanoTime() - start) / 1_000_000);
            }

            // If there are any new segments, we need to flush the parent directory for crash
            // consistency.
            if (segmentsToFlush.stream()
                    .anyMatch(logSegment -> logSegment.getBaseOffset() >= currentRecoverPoint)) {
                flushDirIfExists(logTabletDir.toPath());
            }
        }
    }

    /**
     * Update end offset of the log, and update the recoveryPoint.
     *
     * @param logEndOffset the new end offset of the log
     */
    private void updateLogEndOffset(long logEndOffset) {
        nextOffsetMetadata =
                new LogOffsetMetadata(
                        logEndOffset,
                        segments.activeSegment().getBaseOffset(),
                        segments.activeSegment().getSizeInBytes());
        if (recoveryPoint > logEndOffset) {
            updateRecoveryPoint(logEndOffset);
        }
    }

    /**
     * Close file handlers used by log but don't write to disk. This is called if the log directory
     * is offline.
     */
    void closeHandlers() {
        segments.closeHandlers();
        isMemoryMappedBufferClosed = true;
    }

    /** Closes the segments of the log. */
    void close() {
        checkIfMemoryMappedBufferClosed();
        segments.close();
    }

    /** Completely delete this log directory with no delay. */
    void deleteEmptyDir() throws IOException {
        if (!segments.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot delete directory when "
                            + segments.numberOfSegments()
                            + " segments are still present");
        }
        if (!isMemoryMappedBufferClosed) {
            throw new IllegalStateException(
                    "Cannot delete directory when memory mapped buffer for log of "
                            + tableBucket
                            + " is still open.");
        }

        FileUtils.deleteDirectory(logTabletDir);
    }

    /**
     * Completely delete all segments with no delay.
     *
     * @return the deleted segments
     */
    Iterable<LogSegment> deleteAllSegments() throws IOException {
        List<LogSegment> deletableSegments = segments.values();
        removeAndDeleteSegments(deletableSegments, SegmentDeletionReason.LOG_DELETION);
        isMemoryMappedBufferClosed = true;
        return deletableSegments;
    }

    /**
     * This method deletes the given log segments by doing the following for each of them: - It
     * removes the segment from the segment map so that it will no longer be used for reads. - It
     * renames the index and log files by appending .deleted to the respective file name.
     *
     * <p>This method does not convert IOException to {@link LogStorageException}, the immediate
     * caller is expected to catch and handle IOException.
     *
     * @param segmentsToDelete The log segments to schedule for deletion
     * @param reason The reason for the segment deletion
     */
    void removeAndDeleteSegments(List<LogSegment> segmentsToDelete, SegmentDeletionReason reason)
            throws IOException {
        if (!segmentsToDelete.isEmpty()) {
            // Most callers hold an iterator into the `segments` collection and
            // `removeAndDeleteSegment` mutates it by removing the deleted segment, we should force
            // materialization of the iterator here, so that results of the iteration remain valid
            // and deterministic. We should also pass only the materialized view of the iterator to
            // the logic that actually deletes the segments.
            List<LogSegment> toDelete = new ArrayList<>(segmentsToDelete);
            for (LogSegment segment : toDelete) {
                segments.remove(segment.getBaseOffset());
            }
            deleteSegmentFiles(toDelete, reason);
            localLogStartOffset = segments.isEmpty() ? 0L : segments.firstSegmentBaseOffset().get();
        }
    }

    /**
     * This method deletes the given segment and creates a new segment with the given new base
     * offset. It ensures an active segment exists in the log at all times during this process.
     *
     * <p>This method does not convert IOException to {@link LogStorageException}, the immediate
     * caller is expected to catch and handle IOException.
     *
     * @param newOffset The base offset of the new segment
     * @param segmentToDelete The old active segment to schedule for deletion
     * @param reason The reason for the segment deletion
     */
    LogSegment createAndDeleteSegment(
            long newOffset, LogSegment segmentToDelete, SegmentDeletionReason reason)
            throws IOException {
        // delete the old segment.
        if (newOffset == segmentToDelete.getBaseOffset()) {
            deleteSegmentFiles(Collections.singletonList(segmentToDelete), reason);
        }
        reason.logReason(Collections.singletonList(segmentToDelete));

        // open a new segment.
        LogSegment newSegment = LogSegment.open(logTabletDir, newOffset, config, logFormat);
        segments.add(newSegment);

        if (newOffset != segmentToDelete.getBaseOffset()) {
            segments.remove(segmentToDelete.getBaseOffset());
        }
        return newSegment;
    }

    /**
     * Given a message offset, find its corresponding offset metadata in the log. If the message
     * offset is out of range, throw an OffsetOutOfRangeException.
     */
    LogOffsetMetadata convertToOffsetMetadataOrThrow(long offset) throws IOException {
        FetchDataInfo fetchDataInfo = read(offset, 1, false, nextOffsetMetadata, null);
        return fetchDataInfo.getFetchOffsetMetadata();
    }

    /**
     * Read messages from the log.
     *
     * @param readOffset The offset to begin reading at
     * @param maxLength The maximum number of bytes to read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds
     *     `maxLength` (if one exists)
     * @param maxOffsetMetadata The metadata of the maximum offset to be fetched
     * @throws LogOffsetOutOfRangeException If startOffset is beyond the log start and end offset
     * @return The fetch data information including fetch starting offset metadata and messages
     *     read.
     */
    public FetchDataInfo read(
            long readOffset,
            int maxLength,
            boolean minOneMessage,
            LogOffsetMetadata maxOffsetMetadata,
            @Nullable FileLogProjection projection)
            throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Reading maximum {} bytes at offset {} from log with total length {} bytes",
                    maxLength,
                    readOffset,
                    segments.sizeInBytes());
        }

        long startOffset = localLogStartOffset;
        LogOffsetMetadata endOffsetMetadata = nextOffsetMetadata;
        long endOffset = endOffsetMetadata.getMessageOffset();
        Optional<LogSegment> segmentOpt = segments.floorSegment(readOffset);
        // return error on attempt to read beyond the log start and end offset
        if (readOffset < startOffset || readOffset > endOffset || !segmentOpt.isPresent()) {
            throw new LogOffsetOutOfRangeException(
                    "Received request for offset "
                            + readOffset
                            + " for table bucket "
                            + tableBucket
                            + ", but we only have log segments from offset "
                            + startOffset
                            + " up to "
                            + endOffset
                            + ".");
        }
        if (readOffset == maxOffsetMetadata.getMessageOffset()) {
            return emptyFetchDataInfo(maxOffsetMetadata);
        } else if (readOffset > maxOffsetMetadata.getMessageOffset()) {
            return emptyFetchDataInfo(convertToOffsetMetadataOrThrow(readOffset));
        } else {
            // Do the read on the segment with a base offset less than the target offset but if that
            // segment doesn't contain any messages with an offset greater than that continue to
            // read from successive segments until we get some messages, or we reach the end of the
            // log
            FetchDataInfo fetchDataInfo = null;
            while (fetchDataInfo == null && segmentOpt.isPresent()) {
                LogSegment segment = segmentOpt.get();
                long baseOffset = segment.getBaseOffset();
                long maxPosition =
                        // Use the max offset position if it is on this segment;
                        // otherwise, the segment size is the limit.
                        (maxOffsetMetadata.getSegmentBaseOffset() == segment.getBaseOffset())
                                ? maxOffsetMetadata.getRelativePositionInSegment()
                                : segment.getSizeInBytes();
                fetchDataInfo =
                        segment.read(readOffset, maxLength, maxPosition, minOneMessage, projection);
                if (fetchDataInfo == null) {
                    segmentOpt = segments.higherSegment(baseOffset);
                }
            }
            if (fetchDataInfo != null) {
                return fetchDataInfo;
            } else {
                // okay we are beyond the end of the last segment with no data fetched although the
                // start offset is in range, this can happen when all messages with offset larger
                // than start offsets have been deleted. In this case, we will return the empty set
                // with log end offset metadata
                return new FetchDataInfo(nextOffsetMetadata, MemoryLogRecords.EMPTY);
            }
        }
    }

    void append(
            long lastOffset,
            long maxTimestamp,
            long startOffsetOfMaxTimestamp,
            MemoryLogRecords records)
            throws IOException {
        segments.activeSegment()
                .append(lastOffset, maxTimestamp, startOffsetOfMaxTimestamp, records);
        if (maxTimestamp > localMaxTimestamp) {
            localMaxTimestamp = maxTimestamp;
        }
        updateLogEndOffset(lastOffset + 1);
    }

    long lookupOffsetForTimestamp(long startTimestamp) throws IOException {
        for (LogSegment segment : segments.values()) {
            if (segment.maxTimestampSoFar() >= startTimestamp) {
                return segment.findOffsetByTimestamp(startTimestamp, 0)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "No offset found for timestamp " + startTimestamp))
                        .getOffset();
            }
        }

        return -1L;
    }

    /**
     * Roll the log over to a new active segment starting with the current logEndOffset. This will
     * trim the index to the exact size of the number of entries it currently contains.
     *
     * @param expectedNextOffset The expected next offset after the segment is rolled
     * @return The newly rolled segment
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    LogSegment roll(Optional<Long> expectedNextOffset) throws IOException {
        checkIfMemoryMappedBufferClosed();
        long newOffset = Math.max(expectedNextOffset.orElse(0L), getLocalLogEndOffset());
        File logFile = FlussPaths.logFile(logTabletDir, newOffset);
        LogSegment activeSegment = segments.activeSegment();
        if (segments.contains(newOffset)) {
            // segment with the same base offset already exists and loaded
            if (activeSegment.getBaseOffset() == newOffset && activeSegment.getSizeInBytes() == 0) {
                // We have seen this happen (see KAFKA-6388) after shouldRoll() returns
                // true for an active segment of size zero because one of the indexes is
                // "full" (due to _maxEntries == 0).
                LOG.warn(
                        "Trying to roll a new log segment with start offset "
                                + newOffset
                                + " =max(provided offset = "
                                + expectedNextOffset
                                + ", LEO = "
                                + getLocalLogEndOffset()
                                + ") while it already exists and is active with size 0."
                                + ", size of offset index: "
                                + activeSegment.offsetIndex().entries()
                                + ".");
                LogSegment newSegment =
                        createAndDeleteSegment(
                                newOffset, activeSegment, SegmentDeletionReason.LOG_ROLL);
                updateLogEndOffset(getLocalLogEndOffset());
                LOG.info("Rolled new log segment at offset " + newOffset);
                return newSegment;
            } else {
                throw new FlussRuntimeException(
                        "Trying to roll a new log segment for table bucket "
                                + tableBucket
                                + " with start offset "
                                + newOffset
                                + " =max(provided offset = "
                                + expectedNextOffset
                                + ", LEO = "
                                + getLocalLogEndOffset()
                                + ") while it already exists. Existing segment is "
                                + segments.get(newOffset)
                                + ".");
            }
        } else if (!segments.isEmpty() && newOffset < activeSegment.getBaseOffset()) {
            throw new FlussRuntimeException(
                    "Trying to roll a new log segment for table bucket "
                            + tableBucket
                            + " with start offset "
                            + newOffset
                            + " =max(provided offset = "
                            + expectedNextOffset
                            + ", LEO = "
                            + getLocalLogEndOffset()
                            + ") lower than start offset of the active segment "
                            + activeSegment
                            + ".");
        } else {
            File offsetIdxFile = FlussPaths.offsetIndexFile(logTabletDir, newOffset);
            File timeIndexFile = FlussPaths.timeIndexFile(logTabletDir, newOffset);
            for (File file : Arrays.asList(logFile, offsetIdxFile, timeIndexFile)) {
                if (file.exists()) {
                    LOG.warn(
                            "Newly rolled segment file "
                                    + file.getAbsolutePath()
                                    + " already exists; deleting it first");
                    Files.delete(file.toPath());
                }
            }
            LogSegment logSegment = segments.lastSegment().get();
            logSegment.onBecomeInactiveSegment();
        }

        LogSegment newSegment = LogSegment.open(logTabletDir, newOffset, config, logFormat);
        segments.add(newSegment);
        // We need to update the segment base offset and append position data of the
        // metadata when log rolls.
        // The next offset should not change.
        updateLogEndOffset(getLocalLogEndOffset());
        LOG.info("Rolled new log segment at offset " + newOffset);
        return newSegment;
    }

    /**
     * Delete all data in the local log and start at the new offset.
     *
     * @param newOffset The new offset to start the log with
     * @return the list of segments that were scheduled for deletion
     */
    List<LogSegment> truncateFullyAndStartAt(long newOffset) throws IOException {
        LOG.debug("Truncate and start at offset " + newOffset);

        checkIfMemoryMappedBufferClosed();
        List<LogSegment> segmentsToDelete = segments.values();
        if (!segmentsToDelete.isEmpty()) {
            removeAndDeleteSegments(
                    segmentsToDelete.subList(0, segmentsToDelete.size() - 1),
                    SegmentDeletionReason.LOG_TRUNCATION);
            // Use createAndDeleteSegment() to create new segment first and then delete the old last
            // segment to prevent missing active segment during the deletion process
            createAndDeleteSegment(
                    newOffset,
                    segmentsToDelete.get(segmentsToDelete.size() - 1),
                    SegmentDeletionReason.LOG_TRUNCATION);
        }
        localLogStartOffset = newOffset;
        updateLogEndOffset(newOffset);
        return segmentsToDelete;
    }

    /**
     * Truncate this log so that it ends with the greatest offset < targetOffset.
     *
     * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after
     *     truncation is complete.
     * @return the list of segments that were scheduled for deletion
     */
    List<LogSegment> truncateTo(long targetOffset) throws IOException {
        List<LogSegment> deletableSegments =
                segments.values().stream()
                        .filter(segment -> segment.getBaseOffset() > targetOffset)
                        .collect(Collectors.toList());
        removeAndDeleteSegments(deletableSegments, SegmentDeletionReason.LOG_TRUNCATION);
        segments.activeSegment().truncateTo(targetOffset);
        updateLogEndOffset(targetOffset);
        return deletableSegments;
    }

    // --------------------------------------------------------------------------------------------
    // Static method
    // --------------------------------------------------------------------------------------------

    static boolean isLogFile(File file) {
        return file.getPath().endsWith(FlussPaths.LOG_FILE_SUFFIX);
    }

    static boolean isIndexFile(File file) {
        String fileName = file.getName();
        return fileName.endsWith(FlussPaths.INDEX_FILE_SUFFIX)
                || fileName.endsWith(FlussPaths.TIME_INDEX_FILE_SUFFIX);
    }

    private static FetchDataInfo emptyFetchDataInfo(LogOffsetMetadata fetchOffsetMetadata) {
        return new FetchDataInfo(fetchOffsetMetadata, MemoryLogRecords.EMPTY);
    }

    static void deleteSegmentFiles(List<LogSegment> segmentsToDelete, SegmentDeletionReason reason)
            throws IOException {
        reason.logReason(segmentsToDelete);
        for (LogSegment segment : segmentsToDelete) {
            segment.deleteIfExists();
        }
    }

    /** Reason for deleting segments. */
    public interface SegmentDeletionReason {
        SegmentDeletionReason LOG_TRUNCATION = new LogTruncation();
        SegmentDeletionReason LOG_ROLL = new LogRoll();
        SegmentDeletionReason LOG_DELETION = new LogDeletion();
        SegmentDeletionReason LOG_MOVE_TO_REMOTE = new LogMoveToRemote();

        void logReason(List<LogSegment> toDelete);
    }

    /** Delete with truncation. */
    private static class LogTruncation implements SegmentDeletionReason {
        @Override
        public void logReason(List<LogSegment> toDelete) {
            LOG.info("Deleting segments as part of log truncation: " + toDelete);
        }
    }

    /** Delete with roll. */
    private static class LogRoll implements SegmentDeletionReason {
        @Override
        public void logReason(List<LogSegment> toDelete) {
            LOG.info("Deleting segments as part of log roll: " + toDelete);
        }
    }

    /** Delete. */
    private static class LogDeletion implements SegmentDeletionReason {
        @Override
        public void logReason(List<LogSegment> toDelete) {
            LOG.info("Deleting segments as the log has been deleted: " + toDelete);
        }
    }

    /** Move to remote storage. */
    private static class LogMoveToRemote implements SegmentDeletionReason {
        @Override
        public void logReason(List<LogSegment> toDelete) {
            LOG.info("Deleting segments as the log has been moved to remote: " + toDelete);
        }
    }
}
