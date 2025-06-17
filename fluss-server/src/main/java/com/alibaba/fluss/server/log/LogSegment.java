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

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.CorruptRecordException;
import com.alibaba.fluss.exception.InvalidColumnProjectionException;
import com.alibaba.fluss.exception.InvalidRecordException;
import com.alibaba.fluss.exception.LogSegmentOffsetOverflowException;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.record.FileChannelChunk;
import com.alibaba.fluss.record.FileLogProjection;
import com.alibaba.fluss.record.FileLogRecords;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.TimestampAndOffset;
import com.alibaba.fluss.shaded.guava32.com.google.common.collect.Iterables;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static com.alibaba.fluss.record.DefaultLogRecordBatch.RECORD_BATCH_HEADER_SIZE;
import static com.alibaba.fluss.utils.IOUtils.closeQuietly;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a
 * FileRecords containing the actual messages. The index is an OffsetIndex that maps from logical
 * offsets to physical file positions. Each segment has a base offset which is an offset <= the
 * least offset of any message in this segment and > any offset in any previous segment.
 *
 * <p>A segment with a base offset of [base_offset] would be stored in two files, a
 * [base_offset].index and a [base_offset].log file.
 */
@NotThreadSafe
@Internal
public final class LogSegment {

    private static final Logger LOG = LoggerFactory.getLogger(LogSegment.class);

    // the log format of the log segment
    private final LogFormat logFormat;

    // The file records containing log entries.
    private final FileLogRecords fileLogRecords;

    // The offset indexes.
    private final LazyIndex<OffsetIndex> lazyOffsetIndex;

    // The time indexes.
    private final LazyIndex<TimeIndex> lazyTimeIndex;

    // A lower bound on the offsets in this segment.
    private final long baseOffset;

    // The approximate number of bytes between entries in the index.
    private final int indexIntervalBytes;

    private int bytesSinceLastIndexEntry = 0;

    // The maximum timestamp and start offset we see so far
    private volatile TimestampOffset maxTimestampAndStartOffsetSoFar = TimestampOffset.UNKNOWN;

    public LogSegment(
            LogFormat logFormat,
            FileLogRecords fileLogRecords,
            LazyIndex<OffsetIndex> lazyOffsetIndex,
            LazyIndex<TimeIndex> lazyTimeIndex,
            long baseOffset,
            int indexIntervalBytes) {
        this.logFormat = logFormat;
        this.fileLogRecords = fileLogRecords;
        this.lazyOffsetIndex = lazyOffsetIndex;
        this.lazyTimeIndex = lazyTimeIndex;
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
    }

    public FileLogRecords getFileLogRecords() {
        return fileLogRecords;
    }

    public LazyIndex<OffsetIndex> getLazyOffsetIndex() {
        return lazyOffsetIndex;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public static LogSegment open(
            File dir, long baseOffset, Configuration logConfig, LogFormat logFormat)
            throws IOException {

        int initFileSize = 0;
        if (logConfig.getBoolean(ConfigOptions.LOG_FILE_PREALLOCATE)) {
            initFileSize = (int) logConfig.get(ConfigOptions.LOG_SEGMENT_FILE_SIZE).getBytes();
        }

        return open(dir, baseOffset, logConfig, false, initFileSize, logFormat);
    }

    public static LogSegment open(
            File dir,
            long baseOffset,
            Configuration logConfig,
            boolean fileAlreadyExists,
            int initFileSize,
            LogFormat logFormat)
            throws IOException {
        int maxIndexSize = (int) logConfig.get(ConfigOptions.LOG_INDEX_FILE_SIZE).getBytes();

        return new LogSegment(
                logFormat,
                FileLogRecords.open(
                        FlussPaths.logFile(dir, baseOffset),
                        fileAlreadyExists,
                        initFileSize,
                        logConfig.getBoolean(ConfigOptions.LOG_FILE_PREALLOCATE)),
                LazyIndex.forOffset(
                        FlussPaths.offsetIndexFile(dir, baseOffset), baseOffset, maxIndexSize),
                LazyIndex.forTime(
                        FlussPaths.timeIndexFile(dir, baseOffset), baseOffset, maxIndexSize),
                baseOffset,
                (int) logConfig.get(ConfigOptions.LOG_INDEX_INTERVAL_SIZE).getBytes());
    }

    public OffsetIndex offsetIndex() throws IOException {
        return lazyOffsetIndex.get();
    }

    public TimeIndex timeIndex() throws IOException {
        return lazyTimeIndex.get();
    }

    public File timeIndexFile() {
        return lazyTimeIndex.file();
    }

    public void resizeIndexes(int size) throws IOException {
        offsetIndex().resize(size);
        timeIndex().resize(size);
    }

    /**
     * The maximum timestamp we see so far.
     *
     * <p>Note that this may result in time index materialization.
     */
    public long maxTimestampSoFar() throws IOException {
        return readMaxTimestampAndStartOffsetSoFar().timestamp;
    }

    /** Note that this may result in time index materialization. */
    private long startOffsetOfMaxTimestampSoFar() throws IOException {
        return readMaxTimestampAndStartOffsetSoFar().offset;
    }

    /**
     * The first time this is invoked, it will result in a time index lookup (including potential
     * materialization of the time index).
     */
    private TimestampOffset readMaxTimestampAndStartOffsetSoFar() throws IOException {
        if (maxTimestampAndStartOffsetSoFar == TimestampOffset.UNKNOWN) {
            maxTimestampAndStartOffsetSoFar = readLargestTimestamp();
        }
        return maxTimestampAndStartOffsetSoFar;
    }

    /** Check whether this segment should roll. */
    public boolean shouldRoll(RollParams rollParams) throws IOException {
        return getSizeInBytes() > rollParams.maxSegmentBytes - rollParams.messagesSize
                || offsetIndex().isFull()
                || timeIndex().isFull()
                || !canConvertToRelativeOffset(rollParams.maxOffsetInMessages);
    }

    public int getSizeInBytes() {
        return fileLogRecords.sizeInBytes();
    }

    /**
     * Append the given messages starting with the given offset. Add an entry to the index if
     * needed.
     *
     * <p>It is assumed this method is being called from within a lock.
     *
     * @param largestOffset The last offset in the message set
     * @param maxTimestampMs The max timestamp in the message set.
     * @param startOffsetOfMaxTimestamp The start offset of the message that has the max timestamp
     *     in the message to append.
     * @param records The log entries to append.
     * @throws LogSegmentOffsetOverflowException if the largest offset causes index offset overflow
     */
    public void append(
            long largestOffset,
            long maxTimestampMs,
            long startOffsetOfMaxTimestamp,
            MemoryLogRecords records)
            throws IOException {
        if (records.sizeInBytes() > 0) {
            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Inserting "
                                + records.sizeInBytes()
                                + " bytes at end offset "
                                + largestOffset
                                + " at position "
                                + fileLogRecords.sizeInBytes());
            }

            int physicalPosition = fileLogRecords.sizeInBytes();
            ensureOffsetInRange(largestOffset);
            int appendedBytes = fileLogRecords.append(records);

            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Appended "
                                + appendedBytes
                                + " to "
                                + fileLogRecords.file()
                                + " at end offset "
                                + largestOffset);
            }

            // Update the in memory max timestamp and corresponding start offset.
            if (maxTimestampMs > maxTimestampSoFar()) {
                maxTimestampAndStartOffsetSoFar =
                        new TimestampOffset(maxTimestampMs, startOffsetOfMaxTimestamp);
            }

            if (bytesSinceLastIndexEntry > indexIntervalBytes) {
                offsetIndex().append(largestOffset, physicalPosition);
                timeIndex().maybeAppend(maxTimestampSoFar(), startOffsetOfMaxTimestampSoFar());
                bytesSinceLastIndexEntry = 0;
            }
            bytesSinceLastIndexEntry += records.sizeInBytes();
        }
    }

    public void deleteIfExists() throws IOException {
        delete(fileLogRecords::deleteIfExists, "log", fileLogRecords.file(), true);
        delete(lazyOffsetIndex::deleteIfExists, "offset index", lazyOffsetIndex.file(), true);
        delete(lazyTimeIndex::deleteIfExists, "time index", lazyTimeIndex.file(), true);
    }

    public boolean deleted() {
        return !fileLogRecords.file().exists()
                && !lazyOffsetIndex.file().exists()
                && !lazyTimeIndex.file().exists();
    }

    /**
     * Run recovery on the given segment. This will rebuild the index from the log file and lop off
     * any invalid bytes from the end of the log and index.
     */
    public int recover() throws Exception {
        offsetIndex().reset();
        timeIndex().reset();
        int validBytes = 0;
        int lastIndexEntry = 0;
        maxTimestampAndStartOffsetSoFar = TimestampOffset.UNKNOWN;
        try {
            for (LogRecordBatch batch : fileLogRecords.batches()) {
                batch.ensureValid();
                ensureOffsetInRange(batch.lastLogOffset());

                // The max timestamp is exposed at the batch level, so no need to iterate the
                // records
                if (batch.commitTimestamp() > maxTimestampSoFar()) {
                    maxTimestampAndStartOffsetSoFar =
                            new TimestampOffset(batch.commitTimestamp(), batch.baseLogOffset());
                }

                if (validBytes - lastIndexEntry > indexIntervalBytes) {
                    offsetIndex().append(batch.lastLogOffset(), validBytes);
                    timeIndex().maybeAppend(maxTimestampSoFar(), startOffsetOfMaxTimestampSoFar());
                    lastIndexEntry = validBytes;
                }

                // TODO Adding assign partition leader epoch follow KIP-101

                validBytes += batch.sizeInBytes();
            }
        } catch (CorruptRecordException | InvalidRecordException e) {
            LOG.warn(
                    "Found invalid messages in log segment "
                            + fileLogRecords.file().getAbsolutePath()
                            + " at byte offset "
                            + validBytes
                            + ": "
                            + e.getMessage()
                            + ". "
                            + e.getCause());
        }

        int truncated = fileLogRecords.sizeInBytes() - validBytes;
        if (truncated > 0) {
            LOG.debug(
                    "Truncated "
                            + truncated
                            + " invalid bytes at the end of segment "
                            + fileLogRecords.file().getAbsolutePath()
                            + " during recovery");
        }
        fileLogRecords.truncateTo(validBytes);
        offsetIndex().trimToValidSize();
        timeIndex().trimToValidSize();
        return truncated;
    }

    /**
     * Truncate off all index and log entries with offsets >= the given offset. If the given offset
     * is larger than the largest message in this segment, do nothing.
     *
     * @param offset The offset to truncate to
     * @return The number of log bytes truncated
     */
    public int truncateTo(long offset) throws IOException {
        // Do offset translation before truncating the index to avoid needless scanning
        // in case we truncate the full index.
        FileLogRecords.LogOffsetPosition mapping = translateOffset(offset, 0);

        OffsetIndex offsetIndex = offsetIndex();
        TimeIndex timeIndex = timeIndex();

        offsetIndex.truncateTo(offset);
        timeIndex.truncateTo(offset);

        offsetIndex.resize(offsetIndex.maxIndexSize());
        timeIndex.resize(timeIndex.maxIndexSize());

        int bytesTruncated =
                (mapping == null) ? 0 : fileLogRecords.truncateTo(mapping.getPosition());
        bytesSinceLastIndexEntry = 0;
        if (maxTimestampSoFar() >= 0) {
            maxTimestampAndStartOffsetSoFar = readLargestTimestamp();
        }

        return bytesTruncated;
    }

    /** Trim the log and indexes. */
    public void onBecomeInactiveSegment() throws IOException {
        offsetIndex().trimToValidSize();
        timeIndex().trimToValidSize();
        fileLogRecords.trim();
    }

    /**
     * Calculate the offset that would be used for the next message to be appended to this segment.
     * Note that this is expensive.
     */
    public long readNextOffset() throws IOException {
        FetchDataInfo fetchData =
                read(
                        offsetIndex().lastOffset(),
                        fileLogRecords.sizeInBytes(),
                        fileLogRecords.sizeInBytes(),
                        false);
        if (fetchData == null) {
            return baseOffset;
        } else {
            Iterable<LogRecordBatch> batches = fetchData.getRecords().batches();
            LogRecordBatch lastBatch = Iterables.getLast(batches);
            if (lastBatch != null) {
                return lastBatch.nextLogOffset();
            } else {
                return baseOffset;
            }
        }
    }

    /** Flush this log segment to disk. */
    public void flush() throws IOException {
        fileLogRecords.flush();
        offsetIndex().flush();
        timeIndex().flush();
    }

    /**
     * Update the directory reference for the log and indices in this segment. This would typically
     * be called after a directory is renamed.
     */
    public void updateParentDir(File dir) {
        fileLogRecords.updateParentDir(dir);
        lazyOffsetIndex.updateParentDir(dir);
        lazyTimeIndex.updateParentDir(dir);
    }

    /**
     * Equivalent to {@code translateOffset(offset, 0)}.
     *
     * <p>See {@link #translateOffset(long, int)} for details.
     */
    public FileLogRecords.LogOffsetPosition translateOffset(long offset) throws IOException {
        return translateOffset(offset, 0);
    }

    /**
     * Find the physical file position for the first message with offset >= the requested offset.
     *
     * <p>The startingFilePosition argument is an optimization that can be used if we already know a
     * valid starting position in the file higher than the greatest-lower-bound from the index.
     */
    private FileLogRecords.LogOffsetPosition translateOffset(long offset, int startingFilePosition)
            throws IOException {
        OffsetPosition mapping = offsetIndex().lookup(offset);
        return fileLogRecords.searchForOffsetWithSize(
                offset, Math.max(mapping.getPosition(), startingFilePosition));
    }

    /**
     * Read a message set from this segment beginning with the first offset >= startOffset. The
     * message set will include no more than maxSize bytes and will end before maxOffset if a
     * maxOffset is specified.
     *
     * @param startOffset A lower bound on the first offset to include in the message set we read
     * @param maxSize The maximum number of bytes to include in the message set we read
     * @param maxPosition The maximum position in the log segment that should be exposed for read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds
     *     `maxSize` (if one exists)
     * @return The fetched data and the offset metadata of the first message whose offset is >=
     *     startOffset, or null if the startOffset is larger than the largest offset in this log
     */
    @Nullable
    public FetchDataInfo read(
            long startOffset, int maxSize, long maxPosition, boolean minOneMessage)
            throws IOException {
        return read(startOffset, maxSize, maxPosition, minOneMessage, null);
    }

    /**
     * Read a message set from this segment beginning with the first offset >= startOffset. The
     * message set will include no more than maxSize bytes and will end before maxOffset if a
     * maxOffset is specified.
     *
     * @param startOffset A lower bound on the first offset to include in the message set we read
     * @param maxSize The maximum number of bytes to include in the message set we read
     * @param maxPosition The maximum position in the log segment that should be exposed for read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds
     *     `maxSize` (if one exists)
     * @param projection The column projection to apply to the log records
     * @return The fetched data and the offset metadata of the first message whose offset is >=
     *     startOffset, or null if the startOffset is larger than the largest offset in this log
     */
    @Nullable
    public FetchDataInfo read(
            long startOffset,
            int maxSize,
            long maxPosition,
            boolean minOneMessage,
            @Nullable FileLogProjection projection)
            throws IOException {
        if (maxSize < 0) {
            throw new IllegalArgumentException(
                    "Invalid max size " + maxSize + " for log read from segment " + fileLogRecords);
        }
        FileLogRecords.LogOffsetPosition startOffsetAndSize = translateOffset(startOffset, 0);
        if (startOffsetAndSize == null) {
            return null;
        }
        int startPosition = startOffsetAndSize.getPosition();
        LogOffsetMetadata offsetMetadata =
                new LogOffsetMetadata(startOffset, this.baseOffset, startPosition);
        int adjustedMaxSize =
                minOneMessage ? Math.max(maxSize, startOffsetAndSize.getSize()) : maxSize;
        if (adjustedMaxSize <= RECORD_BATCH_HEADER_SIZE) {
            return new FetchDataInfo(offsetMetadata, MemoryLogRecords.EMPTY);
        }
        if (projection == null) {
            int fetchSize = Math.min((int) (maxPosition - startPosition), adjustedMaxSize);
            return new FetchDataInfo(
                    offsetMetadata, fileLogRecords.slice(startPosition, fetchSize));
        } else {
            if (logFormat != LogFormat.ARROW) {
                throw new InvalidColumnProjectionException(
                        "Only Arrow log format supports column projection, but is: " + logFormat);
            }
            // allow to fetch all the data available in the segment
            int fetchSize = (int) (maxPosition - startPosition);
            FileChannelChunk chunk = fileLogRecords.slice(startPosition, fetchSize).toChunk();
            LogRecords projectedRecords =
                    projection.project(
                            chunk.getFileChannel(),
                            chunk.getPosition(),
                            chunk.getPosition() + chunk.getSize(),
                            adjustedMaxSize);
            return new FetchDataInfo(offsetMetadata, projectedRecords);
        }
    }

    public void changeFileSuffixes(String oldSuffix, String newSuffix) throws IOException {
        fileLogRecords.renameTo(
                new File(
                        FileUtils.replaceSuffix(
                                fileLogRecords.file().getPath(), oldSuffix, newSuffix)));
        lazyOffsetIndex.renameTo(
                new File(
                        FileUtils.replaceSuffix(
                                lazyOffsetIndex.file().getPath(), oldSuffix, newSuffix)));
        lazyTimeIndex.renameTo(
                new File(
                        FileUtils.replaceSuffix(
                                lazyTimeIndex.file().getPath(), oldSuffix, newSuffix)));
    }

    private void ensureOffsetInRange(long offset) throws IOException {
        if (!canConvertToRelativeOffset(offset)) {
            throw new LogSegmentOffsetOverflowException(
                    "Detected offset overflow at offset " + offset + " in segment" + this);
        }
    }

    private boolean canConvertToRelativeOffset(long offset) throws IOException {
        return offsetIndex().canAppendOffset(offset);
    }

    private void delete(
            StorageAction<Boolean, IOException> delete,
            String fileType,
            File file,
            boolean logIfMissing)
            throws IOException {
        try {
            Boolean result = delete.execute();
            if (result) {
                LOG.info("Deleted " + fileType + " " + file.getAbsolutePath() + ".");
            } else if (logIfMissing) {
                LOG.info(
                        "Failed to delete "
                                + fileType
                                + " "
                                + file.getAbsolutePath()
                                + " because it does not exist.");
            }
        } catch (IOException e) {
            throw new IOException(
                    "Delete of " + fileType + " " + file.getAbsolutePath() + " failed.", e);
        }
    }

    /**
     * Search the base offset based on timestamp and input starting offset.
     *
     * <p>This method returns an optional {@link TimestampAndOffset}. The returned value is
     * determined using the following ordered list of rules:
     *
     * <pre>
     *    - If all recordBatch in the segment have smaller offsets, return None
     *    - If all recordBatch in the segment have smaller commitTimestamp, return None
     *    - If all recordBatch in the segment have larger timestamps, returned the offset
     *      will be max(the base offset of the segment, startingOffset) and the timestamp will be -1L
     *     - Otherwise, return an option of TimestampOffset. The offset is the offset of the first recordBatch
     *       whose commit timestamp is greater than or equals to the target timestamp and whose offset is
     *       greater than or equals to the startingOffset.
     * </pre>
     *
     * <p>This method only returns None when 1) all recordBatch's endOffset < startOffset or 2) the
     * log is not empty, but we did not see any recordBatch when scanning the log from the indexed
     * position. The latter could happen if the log is truncated after we get the indexed position
     * but before we scan the log from there. In this case we simply return None and the caller will
     * need to check on the truncated log and maybe retry or even do the search on another log
     * segment.
     *
     * @param timestampMs The timestamp to search for.
     * @param startingOffset The starting offset to search.
     * @return the timestamp and baseOffset of the first recordBatch that meets the requirements.
     *     None will be returned if there is no such record.
     */
    public Optional<TimestampAndOffset> findOffsetByTimestamp(long timestampMs, long startingOffset)
            throws IOException {
        // Get the index entry with a timestamp less than or equal to the target timestamp.
        TimestampOffset timestampOffset = timeIndex().lookup(timestampMs);
        int position =
                offsetIndex()
                        .lookup(Math.max(timestampOffset.offset, startingOffset))
                        .getPosition();

        // search the timestamp.
        return Optional.ofNullable(
                fileLogRecords.searchForTimestamp(timestampMs, position, startingOffset));
    }

    private TimestampOffset readLargestTimestamp() throws IOException {
        // Get the last time index entry. If the time index is empty, it will return (-1,
        // baseOffset).
        TimestampOffset lastTimeIndexEntry = timeIndex().lastEntry();
        OffsetPosition offsetPosition = offsetIndex().lookup(lastTimeIndexEntry.offset);

        // Scan the rest of the log segment to see if there is a larger timestamp after the last
        // time index entry.
        TimestampAndOffset maxTimestampAndOffsetAfterLastEntry =
                fileLogRecords.largestTimestampAfter(offsetPosition.getPosition());
        if (maxTimestampAndOffsetAfterLastEntry.getTimestamp() > lastTimeIndexEntry.timestamp) {
            return new TimestampOffset(
                    maxTimestampAndOffsetAfterLastEntry.getTimestamp(),
                    maxTimestampAndOffsetAfterLastEntry.getOffset());
        } else {
            return lastTimeIndexEntry;
        }
    }

    public void close() {
        closeQuietly(lazyOffsetIndex, "offset index file");
        closeQuietly(lazyTimeIndex, "time index file");
        closeQuietly(fileLogRecords, "log file");
    }

    public void closeHandlers() {
        try {
            lazyOffsetIndex.closeHandler();
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }

        try {
            lazyTimeIndex.closeHandler();
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }

        try {
            fileLogRecords.closeHandlers();
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "LogSegment(baseOffset="
                + baseOffset
                + ", size="
                + fileLogRecords.sizeInBytes()
                + ", lastModifiedTime="
                + fileLogRecords.file().lastModified()
                + ", maxTimestamp="
                + maxTimestampAndStartOffsetSoFar.timestamp
                + ")";
    }
}
