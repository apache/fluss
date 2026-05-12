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
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.utils.FileUtils.deleteDirectoryQuietly;

/**
 * Fetches remote log segments as {@link FileLogRecords} for KV recovery, downloading into a
 * per-instance UUID temp directory. Must be closed after use (prefer try-with-resources).
 *
 * <p>Segments are prefetched in a bounded sliding window ({@code prefetchNum} slots, {@code
 * downloadThreads} concurrent downloads). As the consumer advances, consumed slots are freed and
 * back-filled, overlapping network I/O with local iteration.
 */
@NotThreadSafe
public class RemoteLogFetcher implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogFetcher.class);

    private static final String REMOTE_LOG_RECOVERY_DIR_PREFIX = "remote-log-recovery-";
    private static final long DOWNLOAD_RETRY_BACKOFF_INITIAL_MS = 100L;
    private static final int DOWNLOAD_RETRY_BACKOFF_MULTIPLIER = 2;
    private static final long DOWNLOAD_RETRY_BACKOFF_MAX_MS = 5_000L;
    private static final double DOWNLOAD_RETRY_BACKOFF_JITTER = 0.25D;
    @VisibleForTesting static final int DOWNLOAD_MAX_RETRIES = 5;

    private final RemoteLogManager remoteLogManager;
    private final TableBucket tableBucket;
    private final Path tempDir;
    private final ExecutorService downloadExecutor;
    private final int prefetchNum;

    /** Tracks the currently active iterator to ensure proper cleanup on close. */
    private RemoteLogBatchIterator activeIterator;

    public RemoteLogFetcher(
            RemoteLogManager remoteLogManager,
            TableBucket tableBucket,
            File logTabletDir,
            int prefetchNum,
            int downloadThreads) {
        this(
                remoteLogManager,
                tableBucket,
                logTabletDir
                        .toPath()
                        .resolve("tmp")
                        .resolve(REMOTE_LOG_RECOVERY_DIR_PREFIX + UUID.randomUUID()),
                prefetchNum,
                downloadThreads);
    }

    @VisibleForTesting
    RemoteLogFetcher(
            RemoteLogManager remoteLogManager,
            TableBucket tableBucket,
            Path tempDir,
            int prefetchNum,
            int downloadThreads) {
        this.remoteLogManager = remoteLogManager;
        this.tableBucket = tableBucket;
        this.tempDir = tempDir;
        this.prefetchNum = Math.max(1, prefetchNum);
        int threads = Math.max(1, Math.min(downloadThreads, this.prefetchNum));
        AtomicInteger threadIndex = new AtomicInteger();
        this.downloadExecutor =
                Executors.newFixedThreadPool(
                        threads,
                        runnable -> {
                            Thread thread =
                                    new Thread(
                                            runnable,
                                            "remote-log-fetcher-download-"
                                                    + tableBucket.getTableId()
                                                    + "-"
                                                    + tableBucket.getBucket()
                                                    + "-"
                                                    + threadIndex.getAndIncrement());
                            thread.setDaemon(true);
                            return thread;
                        });
    }

    /**
     * Fetches all relevant remote log segments that cover the range from {@code startOffset} up to
     * {@code localLogStartOffset}, and iterates over the log record batches in order.
     *
     * <p>The returned {@link Iterable} is lazily loaded - remote log segments are downloaded and
     * processed only when iterating through the batches. This means that file downloads and I/O
     * operations occur during iteration, not when this method is called.
     *
     * @param startOffset the offset to start fetching from (inclusive)
     * @param localLogStartOffset the local log start offset (exclusive, stop before this)
     * @return an iterable over all {@link LogRecordBatch} from the fetched remote segments. The
     *     iterator lazily downloads segments as needed.
     * @throws Exception if any error occurs during fetching or reading
     */
    public Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset)
            throws Exception {
        // Lazily create the temp directory on first fetch.
        if (!Files.exists(tempDir)) {
            Files.createDirectories(tempDir);
        }

        // Close any previously active iterator before creating a new one to avoid leaking file
        // descriptors.
        RemoteLogBatchIterator prev = this.activeIterator;
        if (prev != null) {
            prev.close();
        }

        List<RemoteLogSegment> segments =
                remoteLogManager.relevantRemoteLogSegments(tableBucket, startOffset);
        if (segments.isEmpty()) {
            throw new RemoteStorageException(
                    String.format(
                            "No remote log segments found for table bucket %s at offset %d",
                            tableBucket, startOffset));
        }

        LOG.info(
                "Found {} remote log segments for table bucket {} from offset {} to localLogStartOffset {} "
                        + "(prefetchNum={}, downloadThreads={})",
                segments.size(),
                tableBucket,
                startOffset,
                localLogStartOffset,
                prefetchNum,
                ((java.util.concurrent.ThreadPoolExecutor) downloadExecutor).getCorePoolSize());

        RemoteLogBatchIterator iterator =
                new RemoteLogBatchIterator(segments, startOffset, localLogStartOffset);
        this.activeIterator = iterator;
        // Kick off the initial prefetch window so downloads start before the first advance().
        iterator.fillPrefetchWindow();
        return () -> iterator;
    }

    @Override
    public void close() {
        try {
            // Close any active iterator to release file handles
            if (activeIterator != null) {
                activeIterator.close();
                activeIterator = null;
            }
        } finally {
            downloadExecutor.shutdownNow();

            boolean terminated = false;
            try {
                terminated =
                        downloadExecutor.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn(
                        "Interrupted while waiting for remote log fetcher download executor termination for table bucket {}.",
                        tableBucket,
                        e);
            }

            if (!terminated) {
                LOG.warn(
                        "Download executor did not terminate within 1 second for table bucket {}. "
                                + "Proceeding with best-effort cleanup.",
                        tableBucket);
            }

            // Best-effort cleanup: always attempt to delete our own tempDir regardless of
            // whether the executor terminated. If a download is still in progress the
            // directory may not be fully removable, which is fine — we log and move on.
            if (Files.exists(tempDir)) {
                LOG.info("Cleaning up remote log recovery temp dir: {}", tempDir);
                deleteDirectoryQuietly(tempDir.toFile());
                if (Files.exists(tempDir)) {
                    LOG.warn(
                            "Failed to fully clean up remote log recovery temp dir {} for table bucket {}. "
                                    + "Some files may still be in use by an ongoing download.",
                            tempDir,
                            tableBucket);
                }
            }
        }
    }

    @VisibleForTesting
    Path getTempDir() {
        return tempDir;
    }

    @VisibleForTesting
    int getPrefetchNum() {
        return prefetchNum;
    }

    /**
     * Returns an immutable snapshot of in-flight prefetch futures in submission order. Must be
     * called on the consumer thread while it is not advancing the iterator.
     */
    @VisibleForTesting
    List<Future<File>> snapshotPrefetchFuturesForTest() {
        RemoteLogBatchIterator iterator = this.activeIterator;
        if (iterator == null) {
            return Collections.emptyList();
        }
        return iterator.snapshotPrefetchFuturesForTest();
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

        boolean success = false;
        try (InputStream inputStream = remoteLogStorage.fetchLogData(segment);
                OutputStream outputStream = Files.newOutputStream(localFile.toPath())) {
            IOUtils.copyBytes(inputStream, outputStream, false);
            success = true;
        } catch (RemoteStorageException e) {
            throw new IOException(
                    "Failed to download remote log segment: " + segment.remoteLogSegmentId(), e);
        } finally {
            // Most InputStreams don't honor Thread.interrupt() mid-read, so the copy may
            // succeed despite interruption. Treat interrupt-after-success as failure to
            // avoid leaving stale files in tempDir.
            if (!success || Thread.currentThread().isInterrupted()) {
                try {
                    Files.deleteIfExists(localFile.toPath());
                } catch (IOException cleanupException) {
                    LOG.warn(
                            "Failed to cleanup partial/interrupted local segment file {} for segment {}.",
                            localFile,
                            segment.remoteLogSegmentId(),
                            cleanupException);
                }
                // File was deleted above; throw instead of returning a dangling reference
                // so the retry layer or consumer sees a clear failure.
                if (success && Thread.currentThread().isInterrupted()) {
                    throw new IOException(
                            "Download completed but was interrupted for segment "
                                    + segment.remoteLogSegmentId());
                }
            }
        }
        return localFile;
    }

    private File downloadSegmentWithRetry(RemoteLogSegment segment) throws IOException {
        ExponentialBackoff backoff =
                new ExponentialBackoff(
                        DOWNLOAD_RETRY_BACKOFF_INITIAL_MS,
                        DOWNLOAD_RETRY_BACKOFF_MULTIPLIER,
                        DOWNLOAD_RETRY_BACKOFF_MAX_MS,
                        DOWNLOAD_RETRY_BACKOFF_JITTER);

        IOException lastException = null;
        for (int attempt = 0; attempt <= DOWNLOAD_MAX_RETRIES; attempt++) {
            try {
                return downloadSegment(segment);
            } catch (IOException e) {
                lastException = e;
                if (attempt == DOWNLOAD_MAX_RETRIES) {
                    break;
                }

                long retryDelayMs = backoff.backoff(attempt);
                LOG.warn(
                        "Failed to download remote log segment {} on attempt {}/{}. Retry after {} ms.",
                        segment.remoteLogSegmentId(),
                        attempt + 1,
                        DOWNLOAD_MAX_RETRIES + 1,
                        retryDelayMs,
                        e);

                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while retrying remote log segment download: "
                                    + segment.remoteLogSegmentId(),
                            interruptedException);
                }
            }
        }

        throw new IOException(
                "Failed to download remote log segment after retries: "
                        + segment.remoteLogSegmentId(),
                lastException);
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

        /**
         * Ring of in-flight prefetch futures, indexed by {@code seq % prefetchNum}. A non-null slot
         * means "seq is in-flight or ready for consumption"; a null slot means "free".
         */
        private final Future<File>[] prefetchSlots;

        /**
         * Parallel ring of the {@link RemoteLogSegment} each slot's future is downloading. Kept
         * alongside {@link #prefetchSlots} so the consumer can still verify (via {@link
         * #isSameSegmentId}) that the ring head matches the segment it is about to request — a
         * defensive check against test-injected mismatches and potential future FIFO-ordering
         * regressions.
         */
        private final RemoteLogSegment[] prefetchSegments;

        /**
         * Index of the next segment to be submitted for prefetching. Always monotonically
         * increasing.
         */
        private int nextPrefetchIndex = 0;

        /**
         * Index of the next segment to be consumed by the iterator. The invariant {@code
         * nextPrefetchIndex - nextConsumeIndex <= prefetchNum} enforces the window size.
         */
        private int nextConsumeIndex = 0;

        private LogRecordBatch nextBatch;
        /** The local .log file currently opened as {@link #currentFileLogRecords}. */
        private File currentLocalFile;

        private boolean finished = false;
        private boolean closed = false;

        RemoteLogBatchIterator(
                List<RemoteLogSegment> segments, long startOffset, long localLogStartOffset) {
            this.segments = segments;
            this.currentOffset = startOffset;
            this.localLogStartOffset = localLogStartOffset;
            @SuppressWarnings({"unchecked", "rawtypes"})
            Future<File>[] slots = (Future<File>[]) new Future[prefetchNum];
            this.prefetchSlots = slots;
            this.prefetchSegments = new RemoteLogSegment[prefetchNum];
        }

        /** Current number of segments in-flight or ready for consumption. */
        private int prefetchWindowSize() {
            return nextPrefetchIndex - nextConsumeIndex;
        }

        /** Closes this iterator and releases all held resources. */
        public void close() {
            if (!closed) {
                closed = true;
                finished = true; // Mark as finished to stop any ongoing processing
                nextBatch = null; // Clear any pending batch
                drainPrefetchWindow();
                closeCurrentFileLogRecords();
            }
        }

        @Override
        public boolean hasNext() {
            // Return false immediately if closed
            if (closed) {
                return false;
            }
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
                throw new NoSuchElementException();
            }
            LogRecordBatch result = nextBatch;
            nextBatch = null;
            return result;
        }

        private void advance() {
            // Early return if closed
            if (closed) {
                finished = true;
                return;
            }

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
                // remoteLogEndOffset() is inclusive — last offset in segment
                if (segment.remoteLogEndOffset() < currentOffset) {
                    continue;
                }
                // skip segments that start at or after localLogStartOffset
                if (segment.remoteLogStartOffset() >= localLogStartOffset) {
                    finished = true;
                    return;
                }

                try {
                    File localFile = fetchSegmentFile(segment);
                    currentLocalFile = localFile;
                    currentFileLogRecords = FileLogRecords.open(localFile, false);
                    // fetchSegmentFile() already calls fillPrefetchWindow() in its finally block.
                    int startPosition = 0;
                    // if this segment contains data before currentOffset, find the right position
                    if (segment.remoteLogStartOffset() < currentOffset) {
                        startPosition =
                                remoteLogManager.lookupPositionForOffset(segment, currentOffset);
                    }
                    if (startPosition > 0) {
                        // Calculate actual length to avoid potential issues with Integer.MAX_VALUE
                        int remainingLength =
                                (int)
                                        Math.min(
                                                Integer.MAX_VALUE,
                                                currentFileLogRecords.sizeInBytes()
                                                        - startPosition);
                        FileLogRecords sliced =
                                currentFileLogRecords.slice(startPosition, remainingLength);
                        currentBatchIterator = sliced.batches().iterator();
                    } else {
                        currentBatchIterator = currentFileLogRecords.batches().iterator();
                    }
                } catch (Exception e) {
                    // Ensure resources are cleaned up if an exception occurs during segment
                    // loading.
                    // After a failed segment, the recovery cannot continue, so drop the entire
                    // window.
                    drainPrefetchWindow();
                    closeCurrentFileLogRecords();
                    throw new RuntimeException(
                            "Failed to fetch remote log segment: " + segment.remoteLogSegmentId(),
                            e);
                }
            }
        }

        /**
         * Obtain the local file for {@code segment}, ideally reusing a prefetched download. On
         * success the ring head is consumed and a refill is triggered, so the window stays full as
         * long as more segments remain.
         */
        private File fetchSegmentFile(RemoteLogSegment segment) throws IOException {
            // advance() and fillPrefetchWindow() use identical skip rules in FIFO order,
            // so the ring head always matches the segment advance() is requesting.
            int headSlot = nextConsumeIndex % prefetchNum;
            RemoteLogSegment headSegment = prefetchSegments[headSlot];
            if (!isSameSegmentId(segment, headSegment)) {
                // Ring head mismatch — drain the window and fail fast rather than consume
                // the wrong file.
                drainPrefetchWindow();
                throw new IOException(
                        "Prefetch ring head mismatch: requested "
                                + segment.remoteLogSegmentId()
                                + " but head is "
                                + (headSegment == null
                                        ? "null"
                                        : headSegment.remoteLogSegmentId()));
            }
            Future<File> headFuture = prefetchSlots[headSlot];
            // Null the slot eagerly so the ring never keeps stale references.
            prefetchSlots[headSlot] = null;
            prefetchSegments[headSlot] = null;
            nextConsumeIndex++;
            try {
                return headFuture.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "Interrupted while waiting for remote log segment download: "
                                + segment.remoteLogSegmentId(),
                        e);
            } catch (CancellationException e) {
                LOG.warn(
                        "Prefetched segment {} was cancelled, fallback to sync download.",
                        segment.remoteLogSegmentId(),
                        e);
                return downloadSegmentWithRetry(segment);
            } catch (ExecutionException e) {
                LOG.warn(
                        "Prefetched segment {} failed even after async retries.",
                        segment.remoteLogSegmentId(),
                        e.getCause());
                throw new IOException(
                        "Failed to download remote log segment after async retries: "
                                + segment.remoteLogSegmentId(),
                        e.getCause());
            } finally {
                // Window slot released — try to submit the next prefetch right away.
                fillPrefetchWindow();
            }
        }

        private boolean isSameSegmentId(RemoteLogSegment left, RemoteLogSegment right) {
            return left != null
                    && right != null
                    && left.remoteLogSegmentId().equals(right.remoteLogSegmentId());
        }

        /**
         * Submit as many download tasks as fit in the window. Non-blocking: if the window is full
         * we stop and wait for the consumer to advance.
         */
        private void fillPrefetchWindow() {
            if (closed) {
                return;
            }

            while (nextPrefetchIndex < segments.size()) {
                RemoteLogSegment segment = segments.get(nextPrefetchIndex);

                // segment entirely before currentOffset — skip and advance index.
                if (segment.remoteLogEndOffset() < currentOffset) {
                    nextPrefetchIndex++;
                    continue;
                }
                // segment starts at or beyond localLogStartOffset — nothing more to prefetch.
                if (segment.remoteLogStartOffset() >= localLogStartOffset) {
                    return;
                }

                // Window full — back off and wait for the consumer to advance.
                if (prefetchWindowSize() >= prefetchNum) {
                    return;
                }

                final RemoteLogSegment target = segment;
                Future<File> future;
                try {
                    // submit(Callable) returns a FutureTask whose cancel(true) interrupts the
                    // worker thread, unlike CompletableFuture which only flips state.
                    future =
                            downloadExecutor.submit(
                                    (Callable<File>) () -> downloadSegmentWithRetry(target));
                } catch (Throwable submitError) {
                    // The executor rejected the task (e.g. we're shutting down); stop refilling.
                    // No slot has been claimed yet, so there is nothing to roll back.
                    LOG.debug(
                            "Failed to submit prefetch for segment {} (executor likely shutting down).",
                            target.remoteLogSegmentId(),
                            submitError);
                    return;
                }

                int slot = nextPrefetchIndex % prefetchNum;
                prefetchSlots[slot] = future;
                prefetchSegments[slot] = target;
                nextPrefetchIndex++;
                LOG.debug(
                        "Prefetching remote log segment {} for bucket {} (window size={}, free slots={}).",
                        target.remoteLogSegmentId(),
                        tableBucket,
                        prefetchWindowSize(),
                        prefetchNum - prefetchWindowSize());
            }
        }

        /** Cancel or clean up all entries in the window, leaving it empty. */
        private void drainPrefetchWindow() {
            while (nextConsumeIndex < nextPrefetchIndex) {
                int slot = nextConsumeIndex % prefetchNum;
                Future<File> future = prefetchSlots[slot];
                prefetchSlots[slot] = null;
                prefetchSegments[slot] = null;
                nextConsumeIndex++;
                if (future.isDone()) {
                    if (!cleanupCompletedFuture(future)) {
                        return;
                    }
                } else {
                    if (!future.cancel(true) && !cleanupCompletedFuture(future)) {
                        return;
                    }
                }
            }
        }

        /**
         * Retrieve and delete the file from a completed future. Returns {@code false} if
         * interrupted (caller should bail out).
         */
        private boolean cleanupCompletedFuture(Future<File> future) {
            try {
                // isDone() is true so get() will not block; it returns either the
                // successfully downloaded file, or throws Cancellation/Execution.
                cleanupUnusedPrefetchedFile(future.get());
            } catch (CancellationException | ExecutionException ignored) {
                // no local file to clean up
            } catch (InterruptedException e) {
                // Interrupt came from elsewhere; restore flag and bail out. close()'s
                // shutdownNow path will reclaim remaining slots.
                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        }

        private void cleanupUnusedPrefetchedFile(File prefetchedFile) {
            if (prefetchedFile == null) {
                return;
            }

            try {
                Files.deleteIfExists(prefetchedFile.toPath());
            } catch (IOException cleanupException) {
                LOG.warn(
                        "Failed to cleanup unused prefetched segment file {} for table bucket {}.",
                        prefetchedFile,
                        tableBucket,
                        cleanupException);
            }
        }

        private void closeCurrentFileLogRecords() {
            if (currentFileLogRecords != null) {
                IOUtils.closeQuietly(currentFileLogRecords, "FileLogRecords");
                currentFileLogRecords = null;
                currentBatchIterator = null;
            }
            if (currentLocalFile != null) {
                try {
                    Files.deleteIfExists(currentLocalFile.toPath());
                } catch (IOException e) {
                    LOG.warn("Failed to delete consumed segment file {}", currentLocalFile, e);
                }
                currentLocalFile = null;
            }
        }

        List<Future<File>> snapshotPrefetchFuturesForTest() {
            List<Future<File>> snapshot = new ArrayList<>(prefetchWindowSize());
            for (int i = nextConsumeIndex; i < nextPrefetchIndex; i++) {
                snapshot.add(prefetchSlots[i % prefetchNum]);
            }
            return Collections.unmodifiableList(snapshot);
        }
    }
}
