/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.BytesViewLogRecords;
import org.apache.fluss.record.DefaultLogRecordBatch;
import org.apache.fluss.record.FileLogInputStream;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The shadow log of one column group of a log bucket (FIP-45).
 *
 * <p>A column group is stored as a chain of standard {@link LogSegment}s that live in their own
 * directory next to the base segments ({@code {tabletDir}/col-{group}/}). Every batch holds only
 * the group's columns and is addressed by the <em>base-log offsets</em> it fills: the batch base
 * offset is the source offset of its first row and rows are contiguous from there. This makes the
 * group's log end offset the <em>enrichment watermark</em> (the exclusive offset up to which the
 * group is filled) and the group's high watermark the <em>committed enrichment watermark</em>, so
 * the base log's offset index, recovery, truncation and zero-copy read machinery apply unchanged.
 *
 * <p>All mutating methods must be called while holding the owning {@link LogTablet}'s lock.
 */
@Internal
@NotThreadSafe
public final class ColumnGroupLog implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnGroupLog.class);

    private final File groupDir;
    private final String groupName;
    private final Configuration conf;
    private final LogSegments segments;
    private final int maxSegmentFileSize;
    private final TableBucket tableBucket;

    /** Exclusive end offset of the filled range: the enrichment watermark of the group. */
    private volatile long logEndOffset;

    /** Committed enrichment watermark: filled range replicated to the ISR (min over ISR LEOs). */
    private volatile long highWatermark;

    /** The first offset the group log holds data for. */
    private volatile long logStartOffset;

    private ColumnGroupLog(
            File groupDir,
            String groupName,
            Configuration conf,
            LogSegments segments,
            TableBucket tableBucket,
            long logStartOffset,
            long logEndOffset) {
        this.groupDir = groupDir;
        this.groupName = groupName;
        this.conf = conf;
        this.segments = segments;
        this.tableBucket = tableBucket;
        this.maxSegmentFileSize = (int) conf.get(ConfigOptions.LOG_SEGMENT_FILE_SIZE).getBytes();
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
        this.highWatermark = 0L;
    }

    /** Names of the column groups that have a log directory under {@code tabletDir}. */
    public static Set<String> discoverGroups(File tabletDir) {
        Set<String> groups = new HashSet<>();
        File[] children = tabletDir.listFiles();
        if (children == null) {
            return groups;
        }
        for (File child : children) {
            if (child.isDirectory()) {
                String group = FlussPaths.columnGroupNameFromDir(child);
                if (group != null) {
                    groups.add(group);
                }
            }
        }
        return groups;
    }

    /** Opens (or creates) the column-group log for {@code groupName}, recovering it if needed. */
    public static ColumnGroupLog load(
            File tabletDir,
            String groupName,
            Configuration conf,
            TableBucket tableBucket,
            boolean isCleanShutdown)
            throws IOException {
        File groupDir = FlussPaths.columnGroupLogDir(tabletDir, groupName);
        Files.createDirectories(groupDir.toPath());
        LogSegments segments = new LogSegments(tableBucket);

        File[] files = groupDir.listFiles();
        if (files != null) {
            Arrays.sort(files, Comparator.comparing(File::getName));
            for (File file : files) {
                if (!file.isFile()) {
                    continue;
                }
                if (LocalLog.isIndexFile(file)) {
                    long offset = FlussPaths.offsetFromFile(file);
                    if (!FlussPaths.logFile(groupDir, offset).exists()) {
                        Files.deleteIfExists(file.toPath());
                    }
                } else if (LocalLog.isLogFile(file)) {
                    long baseOffset = FlussPaths.offsetFromFile(file);
                    LogSegment segment =
                            LogSegment.open(groupDir, baseOffset, conf, true, 0, LogFormat.ARROW);
                    try {
                        segment.sanityCheck();
                    } catch (NoSuchFileException e) {
                        LOG.warn(
                                "Rebuilding index of column group '{}' segment {} for bucket {}",
                                groupName,
                                baseOffset,
                                tableBucket);
                        segment.recover();
                    }
                    segments.add(segment);
                }
            }
        }

        long logEndOffset = 0L;
        long logStartOffset = 0L;
        if (!segments.isEmpty()) {
            LogSegment last = segments.lastSegment().get();
            if (!isCleanShutdown) {
                int truncated = last.recover();
                if (truncated > 0) {
                    LOG.warn(
                            "Truncated {} bytes of column group '{}' for bucket {} during recovery",
                            truncated,
                            groupName,
                            tableBucket);
                }
            }
            logEndOffset = last.readNextOffset();
            logStartOffset = segments.firstSegmentBaseOffset().get();
        }
        return new ColumnGroupLog(
                groupDir, groupName, conf, segments, tableBucket, logStartOffset, logEndOffset);
    }

    public String getGroupName() {
        return groupName;
    }

    /** The enrichment watermark: exclusive end of the contiguously filled offset range. */
    public long logEndOffset() {
        return logEndOffset;
    }

    /** The committed enrichment watermark of the group. */
    public long highWatermark() {
        return highWatermark;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public List<LogSegment> segments() {
        return segments.values();
    }

    /** Raises the high watermark to {@code newHighWatermark}, bounded by the log end offset. */
    public boolean maybeIncrementHighWatermark(long newHighWatermark) {
        long bounded = Math.min(newHighWatermark, logEndOffset);
        if (bounded > highWatermark) {
            highWatermark = bounded;
            return true;
        }
        return false;
    }

    /**
     * Appends {@code records} whose first row fills base offset {@code firstOffset}. The caller has
     * validated that {@code firstOffset == logEndOffset()}. Batch base offsets and commit
     * timestamps are stamped in place; the CRC does not cover them.
     */
    public ColumnGroupAppendInfo append(
            MemoryLogRecords records, long firstOffset, long commitTimestamp) throws IOException {
        long nextOffset = firstOffset;
        int rowCount = 0;
        for (LogRecordBatch batch : records.batches()) {
            if (!(batch instanceof DefaultLogRecordBatch)) {
                throw new FlussRuntimeException(
                        "Currently, we only support DefaultLogRecordBatch.");
            }
            DefaultLogRecordBatch defaultBatch = (DefaultLogRecordBatch) batch;
            defaultBatch.setBaseLogOffset(nextOffset);
            defaultBatch.setCommitTimestamp(commitTimestamp);
            rowCount += batch.getRecordCount();
            nextOffset = batch.nextLogOffset();
        }
        if (rowCount == 0) {
            return new ColumnGroupAppendInfo(firstOffset, firstOffset - 1, 0, false);
        }
        long lastOffset = nextOffset - 1;

        if (segments.isEmpty()) {
            segments.add(LogSegment.open(groupDir, firstOffset, conf, LogFormat.ARROW));
            logStartOffset = firstOffset;
        }
        LogSegment active = segments.activeSegment();
        if (active.shouldRoll(
                new RollParams(maxSegmentFileSize, lastOffset, records.sizeInBytes()))) {
            active.onBecomeInactiveSegment();
            active = LogSegment.open(groupDir, firstOffset, conf, LogFormat.ARROW);
            segments.add(active);
            LOG.info(
                    "Rolled new segment for column group '{}' of bucket {} at offset {}",
                    groupName,
                    tableBucket,
                    firstOffset);
        }
        active.append(lastOffset, commitTimestamp, firstOffset, records);
        logEndOffset = nextOffset;
        return new ColumnGroupAppendInfo(firstOffset, lastOffset, rowCount, false);
    }

    /**
     * Reads the group records covering base offsets {@code [startOffset, endOffsetInclusive]} as
     * zero-copy file slices. The result may start at a batch containing {@code startOffset} (thus
     * include earlier rows) and, when {@code maxBytes} is exhausted, may end before {@code
     * endOffsetInclusive}; readers stitch by offset and stop at the last offset covered.
     */
    public LogRecords read(long startOffset, long endOffsetInclusive, int maxBytes)
            throws IOException {
        if (segments.isEmpty() || endOffsetInclusive < startOffset) {
            return MemoryLogRecords.EMPTY;
        }
        MultiBytesView.Builder builder = MultiBytesView.builder();
        boolean wroteAny = false;
        int budget = maxBytes;
        Optional<LogSegment> segmentOpt = segments.floorSegment(startOffset);
        if (!segmentOpt.isPresent()) {
            segmentOpt = segments.firstSegment();
        }
        long cursor = startOffset;
        while (segmentOpt.isPresent() && cursor <= endOffsetInclusive) {
            LogSegment segment = segmentOpt.get();
            FileLogRecords fileRecords = segment.getFileLogRecords();
            FileLogRecords.LogOffsetPosition startPos =
                    segment.translateOffset(Math.max(cursor, segment.getBaseOffset()));
            if (startPos == null) {
                segmentOpt = segments.higherSegment(segment.getBaseOffset());
                continue;
            }
            FileLogRecords.LogOffsetPosition endPos =
                    fileRecords.searchForOffsetWithSize(endOffsetInclusive, startPos.getPosition());
            int endPosition =
                    endPos == null
                            ? fileRecords.sizeInBytes()
                            : endPos.getPosition() + endPos.getSize();
            int length = boundedLength(fileRecords, startPos.getPosition(), endPosition, budget);
            if (length <= 0) {
                break;
            }
            builder.addBytes(fileRecords.channel(), startPos.getPosition(), length);
            wroteAny = true;
            budget -= length;
            if (endPos != null || length < endPosition - startPos.getPosition()) {
                break;
            }
            cursor = segment.readNextOffset();
            segmentOpt = segments.higherSegment(segment.getBaseOffset());
        }
        return wroteAny ? new BytesViewLogRecords(builder.build()) : MemoryLogRecords.EMPTY;
    }

    /** Length of whole batches in {@code [start, end)} that fit {@code budget} (at least one). */
    private static int boundedLength(FileLogRecords fileRecords, int start, int end, int budget) {
        if (end - start <= budget) {
            return end - start;
        }
        int length = 0;
        for (FileLogInputStream.FileChannelLogRecordBatch batch :
                (Iterable<FileLogInputStream.FileChannelLogRecordBatch>)
                        () -> fileRecords.batchIterator(start, end)) {
            int size = batch.sizeInBytes();
            if (length > 0 && length + size > budget) {
                break;
            }
            length += size;
            if (length >= budget) {
                break;
            }
        }
        return length;
    }

    /** Truncates so that the log ends with the greatest offset below {@code targetOffset}. */
    public void truncateTo(long targetOffset) throws IOException {
        if (targetOffset >= logEndOffset) {
            return;
        }
        if (targetOffset <= logStartOffset || segments.isEmpty()) {
            truncateFullyAndStartAt(targetOffset);
            return;
        }
        List<LogSegment> deletable = new ArrayList<>();
        for (LogSegment segment : segments.values()) {
            if (segment.getBaseOffset() > targetOffset) {
                deletable.add(segment);
            }
        }
        for (LogSegment segment : deletable) {
            segments.remove(segment.getBaseOffset());
        }
        LocalLog.deleteSegmentFiles(deletable, LocalLog.SegmentDeletionReason.LOG_TRUNCATION);
        // like the base log: batches are truncated whole, but the log end offset becomes the
        // requested offset so that the group stays aligned with the base log
        segments.activeSegment().truncateTo(targetOffset);
        logEndOffset = targetOffset;
        highWatermark = Math.min(highWatermark, logEndOffset);
        LOG.info(
                "Truncated column group '{}' of bucket {} to offset {}",
                groupName,
                tableBucket,
                targetOffset);
    }

    /** Deletes all data and restarts the log at {@code newOffset}. */
    public void truncateFullyAndStartAt(long newOffset) throws IOException {
        List<LogSegment> all = segments.values();
        for (LogSegment segment : all) {
            segments.remove(segment.getBaseOffset());
        }
        LocalLog.deleteSegmentFiles(all, LocalLog.SegmentDeletionReason.LOG_TRUNCATION);
        logStartOffset = newOffset;
        logEndOffset = newOffset;
        highWatermark = Math.min(highWatermark, newOffset);
    }

    /**
     * Retention advance: base offsets below {@code newStartOffset} no longer exist, so the group is
     * trivially complete up to there. Moves the log start, log end and high watermark forward when
     * they are behind.
     */
    public void advanceStartOffsetTo(long newStartOffset) throws IOException {
        if (newStartOffset <= logEndOffset) {
            return;
        }
        LOG.info(
                "Advancing column group '{}' of bucket {} from {} to base log start offset {}",
                groupName,
                tableBucket,
                logEndOffset,
                newStartOffset);
        List<LogSegment> all = segments.values();
        for (LogSegment segment : all) {
            segments.remove(segment.getBaseOffset());
        }
        LocalLog.deleteSegmentFiles(all, LocalLog.SegmentDeletionReason.LOG_RETENTION);
        logStartOffset = newStartOffset;
        logEndOffset = newStartOffset;
        highWatermark = Math.max(highWatermark, newStartOffset);
    }

    public void flush() throws IOException {
        for (LogSegment segment : segments.values()) {
            segment.flush();
        }
    }

    @Override
    public void close() {
        segments.close();
    }

    @Override
    public String toString() {
        return "ColumnGroupLog("
                + "group="
                + groupName
                + ", bucket="
                + tableBucket
                + ", logStartOffset="
                + logStartOffset
                + ", logEndOffset="
                + logEndOffset
                + ", highWatermark="
                + highWatermark
                + ')';
    }
}
