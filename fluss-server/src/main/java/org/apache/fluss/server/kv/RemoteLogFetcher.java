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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.log.remote.RemoteLogStorage;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly;

/**
 * A utility class that fetches remote log segments and makes them available as {@link
 * FileLogRecords} for KV recovery. It downloads remote log data into a local temporary directory
 * using a UUID to avoid conflicts with other concurrent recovery operations.
 *
 * <p>The fetcher is {@link Closeable} and the caller must close it after use to clean up the
 * temporary directory.
 */
public class RemoteLogFetcher implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogFetcher.class);

    private static final String REMOTE_LOG_RECOVERY_DIR_PREFIX = "remote-log-recovery-";

    private final RemoteLogManager remoteLogManager;
    private final TableBucket tableBucket;
    private final Path tempDir;

    public RemoteLogFetcher(
            RemoteLogManager remoteLogManager, TableBucket tableBucket, File dataDir)
            throws IOException {
        this(
                remoteLogManager,
                tableBucket,
                Files.createDirectories(
                        dataDir.toPath()
                                .resolve("tmp")
                                .resolve(REMOTE_LOG_RECOVERY_DIR_PREFIX + UUID.randomUUID())));
    }

    @VisibleForTesting
    RemoteLogFetcher(RemoteLogManager remoteLogManager, TableBucket tableBucket, Path tempDir)
            throws IOException {
        this.remoteLogManager = remoteLogManager;
        this.tableBucket = tableBucket;
        this.tempDir = tempDir;
        Files.createDirectories(tempDir);
    }

    /**
     * Fetches all relevant remote log segments that cover the range from {@code startOffset} up to
     * {@code localLogStartOffset}, and iterates over the log record batches in order.
     *
     * @param startOffset the offset to start fetching from (inclusive)
     * @param localLogStartOffset the local log start offset (exclusive, stop before this)
     * @return an iterator over all {@link LogRecordBatch} from the fetched remote segments
     * @throws Exception if any error occurs during fetching or reading
     */
    public Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset)
            throws Exception {
        List<RemoteLogSegment> segments =
                remoteLogManager.relevantRemoteLogSegments(tableBucket, startOffset);
        if (segments.isEmpty()) {
            throw new RemoteStorageException(
                    String.format(
                            "No remote log segments found for table bucket %s at offset %d",
                            tableBucket, startOffset));
        }

        LOG.info(
                "Found {} remote log segments for table bucket {} from offset {} to local log start offset {}",
                segments.size(),
                tableBucket,
                startOffset,
                localLogStartOffset);

        return () -> new RemoteLogBatchIterator(segments, startOffset, localLogStartOffset);
    }

    @Override
    public void close() {
        LOG.info("Cleaning up remote log recovery temp dir: {}", tempDir);
        deleteDirectoryQuietly(tempDir.toFile());
    }

    @VisibleForTesting
    Path getTempDir() {
        return tempDir;
    }

    /**
     * Downloads the log data of a remote log segment to a local temporary file.
     *
     * @return the local file containing the downloaded log data
     */
    private File downloadSegment(RemoteLogSegment segment) throws IOException {
        File localFile =
                tempDir.resolve(
                                FlussPaths.filenamePrefixFromOffset(segment.remoteLogStartOffset())
                                        + ".log")
                        .toFile();

        RemoteLogStorage remoteLogStorage = remoteLogManager.getRemoteLogStorage();
        LOG.info(
                "Downloading remote log segment {} (offsets {}-{}) to {}",
                segment.remoteLogSegmentId(),
                segment.remoteLogStartOffset(),
                segment.remoteLogEndOffset(),
                localFile);

        try (InputStream inputStream = remoteLogStorage.fetchLogData(segment);
                OutputStream outputStream = Files.newOutputStream(localFile.toPath())) {
            IOUtils.copyBytes(inputStream, outputStream, false);
        } catch (RemoteStorageException e) {
            throw new IOException(
                    "Failed to download remote log segment: " + segment.remoteLogSegmentId(), e);
        }
        return localFile;
    }

    /**
     * An iterator that lazily downloads remote log segments and iterates over their batches in
     * order. It respects the startOffset and localLogStartOffset boundaries, yielding only batches
     * within [startOffset, localLogStartOffset).
     */
    private class RemoteLogBatchIterator implements Iterator<LogRecordBatch> {
        private final List<RemoteLogSegment> segments;
        private final long localLogStartOffset;

        /** Tracks the current read offset, advancing as batches are consumed. */
        private long currentOffset;

        private int currentSegmentIndex = 0;
        private FileLogRecords currentFileLogRecords;
        private Iterator<LogRecordBatch> currentBatchIterator;
        private LogRecordBatch nextBatch;
        private boolean finished = false;

        RemoteLogBatchIterator(
                List<RemoteLogSegment> segments, long startOffset, long localLogStartOffset) {
            this.segments = segments;
            this.currentOffset = startOffset;
            this.localLogStartOffset = localLogStartOffset;
        }

        @Override
        public boolean hasNext() {
            // Lazily advance: only fetch next batch when needed. This ensures the
            // previously returned FileChannelLogRecordBatch has been fully consumed
            // by the caller before advance() potentially closes its underlying file.
            if (nextBatch == null && !finished) {
                advance();
            }
            return nextBatch != null;
        }

        @Override
        public LogRecordBatch next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            LogRecordBatch result = nextBatch;
            nextBatch = null;
            return result;
        }

        private void advance() {
            nextBatch = null;
            while (!finished) {
                // try to get next batch from current iterator
                if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
                    LogRecordBatch batch = currentBatchIterator.next();
                    // skip batches entirely before currentOffset
                    if (batch.nextLogOffset() <= currentOffset) {
                        continue;
                    }
                    // stop if we've reached localLogStartOffset
                    if (batch.baseLogOffset() >= localLogStartOffset) {
                        finished = true;
                        closeCurrentFileLogRecords();
                        return;
                    }
                    nextBatch = batch;
                    // advance currentOffset so subsequent segments use updated position
                    currentOffset = batch.nextLogOffset();
                    return;
                }

                // close current file log records
                closeCurrentFileLogRecords();

                // move to next segment
                if (currentSegmentIndex >= segments.size()) {
                    finished = true;
                    return;
                }

                RemoteLogSegment segment = segments.get(currentSegmentIndex++);
                // skip segments entirely before currentOffset
                if (segment.remoteLogEndOffset() <= currentOffset) {
                    continue;
                }
                // skip segments that start at or after localLogStartOffset
                if (segment.remoteLogStartOffset() >= localLogStartOffset) {
                    finished = true;
                    return;
                }

                try {
                    File localFile = downloadSegment(segment);
                    currentFileLogRecords = FileLogRecords.open(localFile, false);
                    int startPosition = 0;
                    // if this segment contains data before currentOffset, find the right position
                    if (segment.remoteLogStartOffset() < currentOffset) {
                        startPosition =
                                remoteLogManager.lookupPositionForOffset(segment, currentOffset);
                    }
                    if (startPosition > 0) {
                        FileLogRecords sliced =
                                currentFileLogRecords.slice(startPosition, Integer.MAX_VALUE);
                        currentBatchIterator = sliced.batches().iterator();
                    } else {
                        currentBatchIterator = currentFileLogRecords.batches().iterator();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to fetch remote log segment: " + segment.remoteLogSegmentId(),
                            e);
                }
            }
        }

        private void closeCurrentFileLogRecords() {
            if (currentFileLogRecords != null) {
                IOUtils.closeQuietly(currentFileLogRecords, "FileLogRecords");
                currentFileLogRecords = null;
                currentBatchIterator = null;
            }
        }
    }
}
