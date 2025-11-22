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

package org.apache.fluss.server.log;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidOffsetException;
import org.apache.fluss.exception.LogSegmentOffsetOverflowException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Loader to load log segments. */
final class LogLoader {
    private static final Logger LOG = LoggerFactory.getLogger(LogLoader.class);

    private final File logTabletDir;
    private final Configuration conf;
    private final LogSegments logSegments;
    private final long recoveryPointCheckpoint;
    private final LogFormat logFormat;
    private final WriterStateManager writerStateManager;
    private final boolean isCleanShutdown;

    public LogLoader(
            File logTabletDir,
            Configuration conf,
            LogSegments logSegments,
            long recoveryPointCheckpoint,
            LogFormat logFormat,
            WriterStateManager writerStateManager,
            boolean isCleanShutdown) {
        this.logTabletDir = logTabletDir;
        this.conf = conf;
        this.logSegments = logSegments;
        this.recoveryPointCheckpoint = recoveryPointCheckpoint;
        this.logFormat = logFormat;
        this.writerStateManager = writerStateManager;
        this.isCleanShutdown = isCleanShutdown;
    }

    /**
     * Load the log segments from the log files on disk, and returns the components of the loaded
     * log.
     *
     * <p>In the context of the calling thread, this function does not need to convert IOException
     * to {@link LogStorageException} because it is only called before all logs are loaded.
     *
     * @return the offsets of the Log successfully loaded from disk
     */
    public LoadedLogOffsets load() throws IOException {
        // load all the log and index files.
        logSegments.close();
        logSegments.clear();
        loadSegmentFiles();
        long newRecoveryPoint;
        long nextOffset;
        Tuple2<Long, Long> result = recoverLog();
        newRecoveryPoint = result.f0;
        nextOffset = result.f1;

        // Any segment loading or recovery code must not use writerStateManager, so that we can
        // build the full state here from scratch.
        if (!writerStateManager.isEmpty()) {
            throw new IllegalStateException("Writer state must be empty during log initialization");
        }

        // Reload all snapshots into the WriterStateManager cache, the intermediate
        // WriterStateManager used during log recovery may have deleted some files without the
        // LogLoader.writerStateManager instance witnessing the deletion.
        writerStateManager.removeStraySnapshots(logSegments.baseOffsets());

        // TODO, Here, we use 0 as the logStartOffset passed into rebuildWriterState. The reason is
        // that the current implementation of logStartOffset in Fluss is not yet fully refined, and
        // there may be cases where logStartOffset is not updated. As a result, logStartOffset is
        // not yet reliable. Once the issue with correctly updating logStartOffset is resolved in
        // issue https://github.com/apache/fluss/issues/744, we can use logStartOffset here.
        // Additionally, using 0 versus using logStartOffset does not affect correctness—they both
        // can restore the complete WriterState. The only difference is that using logStartOffset
        // can potentially skip over more segments.
        LogTablet.rebuildWriterState(
                writerStateManager, logSegments, 0, nextOffset, isCleanShutdown);

        LogSegment activeSegment = logSegments.lastSegment().get();
        activeSegment.resizeIndexes((int) conf.get(ConfigOptions.LOG_INDEX_FILE_SIZE).getBytes());
        return new LoadedLogOffsets(
                newRecoveryPoint,
                new LogOffsetMetadata(
                        nextOffset, activeSegment.getBaseOffset(), activeSegment.getSizeInBytes()));
    }

    /**
     * Just recovers the given segment, without adding it to the provided segments.
     *
     * @param segment Segment to recover
     * @return The number of bytes truncated from the segment
     * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index
     *     offset overflow
     */
    private int recoverSegment(LogSegment segment) throws IOException {
        WriterStateManager writerStateManager =
                new WriterStateManager(
                        logSegments.getTableBucket(),
                        logTabletDir,
                        this.writerStateManager.writerExpirationMs());
        // TODO, Here, we use 0 as the logStartOffset passed into rebuildWriterState. The reason is
        // that the current implementation of logStartOffset in Fluss is not yet fully refined, and
        // there may be cases where logStartOffset is not updated. As a result, logStartOffset is
        // not yet reliable. Once the issue with correctly updating logStartOffset is resolved in
        // issue https://github.com/apache/fluss/issues/744, we can use logStartOffset here.
        // Additionally, using 0 versus using logStartOffset does not affect correctness—they both
        // can restore the complete WriterState. The only difference is that using logStartOffset
        // can potentially skip over more segments.
        LogTablet.rebuildWriterState(
                writerStateManager, logSegments, 0, segment.getBaseOffset(), false);
        int bytesTruncated = segment.recover();
        // once we have recovered the segment's data, take a snapshot to ensure that we won't
        // need to reload the same segment again while recovering another segment.
        writerStateManager.takeSnapshot();
        return bytesTruncated;
    }

    /**
     * Recover the log segments (if there was an unclean shutdown). Ensures there is at least one
     * active segment, and returns the updated recovery point and next offset after recovery.
     *
     * <p>This method does not need to convert IOException to {@link LogStorageException} because it
     * is only called before all logs are loaded.
     *
     * @return a tuple containing (newRecoveryPoint, nextOffset).
     * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset
     *     overflow
     */
    private Tuple2<Long, Long> recoverLog() throws IOException {
        if (!isCleanShutdown) {
            List<LogSegment> unflushed =
                    logSegments.values(recoveryPointCheckpoint, Long.MAX_VALUE);
            int numUnflushed = unflushed.size();
            Iterator<LogSegment> unflushedIter = unflushed.iterator();
            boolean truncated = false;
            int numFlushed = 1;

            while (unflushedIter.hasNext() && !truncated) {
                LogSegment segment = unflushedIter.next();
                LOG.info(
                        "Recovering unflushed segment {}. {}/{} recovered for bucket {}",
                        segment.getBaseOffset(),
                        numFlushed,
                        numUnflushed,
                        logSegments.getTableBucket());

                int truncatedBytes = -1;
                try {
                    truncatedBytes = recoverSegment(segment);
                } catch (Exception e) {
                    if (e instanceof InvalidOffsetException) {
                        long startOffset = segment.getBaseOffset();
                        LOG.warn(
                                "Found invalid offset during recovery for bucket {}. Deleting the corrupt segment "
                                        + "and creating an empty one with starting offset {}",
                                logSegments.getTableBucket(),
                                startOffset);
                        truncatedBytes = segment.truncateTo(startOffset);
                    } else {
                        throw e;
                    }
                }

                if (truncatedBytes > 0) {
                    // we had an invalid message, delete all remaining log
                    LOG.warn(
                            "Corruption found in segment {} for bucket {}, truncating to offset {}",
                            segment.getBaseOffset(),
                            logSegments.getTableBucket(),
                            segment.readNextOffset());
                    removeAndDeleteSegments(unflushedIter);
                    truncated = true;
                } else {
                    numFlushed += 1;
                }
            }
        }

        if (logSegments.isEmpty()) {
            // TODO: use logStartOffset if issue https://github.com/apache/fluss/issues/744 ready
            logSegments.add(LogSegment.open(logTabletDir, 0L, conf, logFormat));
        }
        long logEndOffset = logSegments.lastSegment().get().readNextOffset();
        return Tuple2.of(recoveryPointCheckpoint, logEndOffset);
    }

    /**
     * This method deletes the given log segments and the associated writer snapshots.
     *
     * <p>This method does not need to convert IOException to {@link LogStorageException} because it
     * is either called before all logs are loaded or the immediate caller will catch and handle
     * IOException
     *
     * @param segmentsToDelete The log segments to schedule for deletion
     */
    private void removeAndDeleteSegments(Iterator<LogSegment> segmentsToDelete) {
        if (segmentsToDelete.hasNext()) {
            List<LogSegment> toDelete = new ArrayList<>();
            segmentsToDelete.forEachRemaining(toDelete::add);

            LOG.info(
                    "Deleting segments for bucket {} as part of log recovery: {}",
                    logSegments.getTableBucket(),
                    toDelete.stream().map(LogSegment::toString).collect(Collectors.joining(",")));
            toDelete.forEach(segment -> logSegments.remove(segment.getBaseOffset()));

            try {
                LocalLog.deleteSegmentFiles(
                        toDelete, LocalLog.SegmentDeletionReason.LOG_TRUNCATION);
            } catch (IOException e) {
                LOG.error(
                        "Failed to delete truncated segments {} for bucket {}",
                        toDelete,
                        logSegments.getTableBucket(),
                        e);
            }

            try {
                LogTablet.deleteWriterSnapshots(toDelete, writerStateManager);
            } catch (IOException e) {
                LOG.error(
                        "Failed to delete truncated writer snapshots {} for bucket {}",
                        toDelete,
                        logSegments.getTableBucket(),
                        e);
            }
        }
    }

    /** Loads segments from disk into the provided segments. */
    private void loadSegmentFiles() throws IOException {
        File[] sortedFiles = logTabletDir.listFiles();
        if (sortedFiles != null) {
            Arrays.sort(sortedFiles, Comparator.comparing(File::getName));
            for (File file : sortedFiles) {
                if (file.isFile()) {
                    if (LocalLog.isIndexFile(file)) {
                        long offset = FlussPaths.offsetFromFile(file);
                        File logFile = FlussPaths.logFile(logTabletDir, offset);
                        if (!logFile.exists()) {
                            LOG.warn(
                                    "Found an orphaned index file {} for bucket {}, with no corresponding log file.",
                                    logSegments.getTableBucket(),
                                    file.getAbsolutePath());
                            Files.deleteIfExists(file.toPath());
                        }
                    } else if (LocalLog.isLogFile(file)) {
                        long baseOffset = FlussPaths.offsetFromFile(file);
                        boolean timeIndexFileNewlyCreated =
                                !FlussPaths.timeIndexFile(logTabletDir, baseOffset).exists();
                        LogSegment segment =
                                LogSegment.open(logTabletDir, baseOffset, conf, true, 0, logFormat);

                        try {
                            segment.sanityCheck(timeIndexFileNewlyCreated);
                        } catch (IOException e) {
                            if (e instanceof NoSuchFileException) {
                                if (isCleanShutdown
                                        || segment.getBaseOffset() < recoveryPointCheckpoint) {
                                    LOG.error(
                                            "Could not find offset index file corresponding to log file {} "
                                                    + "for bucket {}, recovering segment and rebuilding index files...",
                                            logSegments.getTableBucket(),
                                            segment.getFileLogRecords().file().getAbsoluteFile());
                                }
                                recoverSegment(segment);
                            } else {
                                throw e;
                            }
                        }
                        logSegments.add(segment);
                    }
                }
            }
        }
    }
}
