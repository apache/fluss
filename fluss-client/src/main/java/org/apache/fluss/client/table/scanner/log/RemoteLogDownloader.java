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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentFile;

/**
 * Reads remote log segment files in chunks via streaming I/O. Each chunk (configurable via {@code
 * client.scanner.remote-log.chunk-size}, default 8 MB) is read from the remote filesystem, appended
 * to a local temporary file, and delivered as a {@link FileLogRecords} slice. This reduces JVM heap
 * pressure by leveraging OS page cache.
 *
 * <p>Flow control: each segment has at most {@code maxPrefetchChunks} unconsumed chunks. The
 * downloader pauses when this limit is reached and resumes when chunks are consumed.
 */
@ThreadSafe
@Internal
public class RemoteLogDownloader implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogDownloader.class);

    private static final long POLL_TIMEOUT = 5000L;

    /**
     * A queue to hold the remote log segment files to be fetched. The queue is ordered by the
     * max_timestamp of the remote log segment. So we download the remote log segments from the
     * older to the newer.
     */
    private final PriorityBlockingQueue<RemoteLogDownloadRequest> segmentsToFetch;

    /** Queue for continuation chunks of segments that are already being streamed. */
    private final BlockingQueue<RemoteLogDownloadRequest> continuationQueue;

    private final Semaphore prefetchSemaphore;

    private final DownloadRemoteLogThread downloadThread;

    private final ScannerMetricGroup scannerMetricGroup;

    private final long pollTimeout;

    private final File tmpDir;

    private final int chunkSize;

    private final int maxPrefetchChunks;

    public RemoteLogDownloader(
            TablePath tablePath, Configuration conf, ScannerMetricGroup scannerMetricGroup) {
        // default we give a 5s long interval to avoid frequent loop
        this(tablePath, conf, scannerMetricGroup, POLL_TIMEOUT);
    }

    @VisibleForTesting
    RemoteLogDownloader(
            TablePath tablePath,
            Configuration conf,
            ScannerMetricGroup scannerMetricGroup,
            long pollTimeout) {
        this.segmentsToFetch = new PriorityBlockingQueue<>();
        this.continuationQueue = new LinkedBlockingQueue<>();
        this.scannerMetricGroup = scannerMetricGroup;
        this.pollTimeout = pollTimeout;
        this.prefetchSemaphore =
                new Semaphore(conf.getInt(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM));
        this.chunkSize =
                (int) conf.get(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_CHUNK_SIZE).getBytes();
        this.maxPrefetchChunks =
                conf.getInt(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_MAX_PREFETCH_CHUNKS);
        this.tmpDir = new File(conf.getString(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR));
        if (!tmpDir.exists() && !tmpDir.mkdirs()) {
            throw new IllegalStateException(
                    "Failed to create temp directory for remote log chunks: " + tmpDir);
        }
        this.downloadThread = new DownloadRemoteLogThread(tablePath);
    }

    public void start() {
        downloadThread.start();
    }

    /**
     * Request to read a remote log segment in chunks starting from the given position. This method
     * is non-blocking and returns a future for the first chunk.
     */
    public RemoteLogDownloadFuture requestRemoteLog(
            FsPath logTabletDir, RemoteLogSegment segment, int startPosition) {
        CompletableFuture<LogRecords> chunkFuture = new CompletableFuture<>();
        RemoteLogDownloadRequest request =
                new RemoteLogDownloadRequest(segment, logTabletDir, startPosition, chunkFuture);
        RemoteLogDownloadFuture downloadFuture =
                new RemoteLogDownloadFuture(chunkFuture, () -> onChunkConsumed(request));
        // Assign downloadFuture before publishing the request to segmentsToFetch, so that the
        // download thread never sees a null downloadFuture when reading request.downloadFuture
        // inside createAndQueueNextChunk.
        request.downloadFuture = downloadFuture;
        segmentsToFetch.add(request);
        return downloadFuture;
    }

    /**
     * Called when a chunk has been consumed (drained). Increments the consumed counter and attempts
     * to schedule the next chunk download or cleanup.
     */
    private void onChunkConsumed(RemoteLogDownloadRequest request) {
        request.chunksConsumed.incrementAndGet();
        tryScheduleNextChunk(request);
    }

    /**
     * Core scheduling logic. Called from both the download thread (after writing a chunk) and the
     * consumer thread (after consuming a chunk). Uses synchronized(request) to avoid race
     * conditions between the two threads.
     *
     * <p><b>Lock ordering note:</b> To avoid deadlock with {@code LogFetchBuffer}'s internal lock
     * (which may call {@code drain()} → {@code recycleCallback} → this method), we must never
     * invoke external callbacks while holding {@code synchronized(request)}. All callbacks are
     * captured inside the lock and invoked <em>after</em> releasing it.
     */
    private void tryScheduleNextChunk(RemoteLogDownloadRequest request) {
        // Capture any pending callback to fire outside the lock.
        Consumer<RemoteLogDownloadFuture> pendingCallback = null;
        RemoteLogDownloadFuture pendingNextFuture = null;
        boolean shouldCleanupAndRelease = false;

        synchronized (request) {
            if (request.queuedForContinuation || request.cleanedUp) {
                return;
            }

            boolean exhausted = request.reader == null || request.reader.isExhausted();

            if (!exhausted) {
                int unconsumed = request.chunksWritten.get() - request.chunksConsumed.get();
                if (unconsumed < maxPrefetchChunks) {
                    // Build the next future inside the lock, but defer the callback invocation.
                    RemoteLogDownloadFuture nextFuture = buildNextChunkFuture(request);
                    pendingCallback = nextFuture != null ? nextFuture.getNextChunkCallback() : null;
                    pendingNextFuture = nextFuture;
                    request.queuedForContinuation = true;
                    // Re-queue as a continuation (higher priority than new segments).
                    continuationQueue.add(request);
                }
                // else: too many unconsumed chunks, pause downloading
            } else if (request.chunksConsumed.get() >= request.chunksWritten.get()) {
                // All chunks consumed and segment exhausted: cleanup
                cleanupRequest(request);
                shouldCleanupAndRelease = true;
            }
        }

        // Fire the external callback outside the lock to prevent lock-ordering deadlocks.
        if (pendingCallback != null && pendingNextFuture != null) {
            pendingCallback.accept(pendingNextFuture);
        }
        if (shouldCleanupAndRelease) {
            prefetchSemaphore.release();
        }
    }

    /**
     * Builds a new {@link RemoteLogDownloadFuture} for the next chunk and updates {@code
     * request.chunkFuture} and {@code request.downloadFuture}. Must be called within {@code
     * synchronized(request)}. Does NOT invoke any external callbacks.
     *
     * @return the newly created future, with the inherited {@code nextChunkCallback} already set.
     */
    private RemoteLogDownloadFuture buildNextChunkFuture(RemoteLogDownloadRequest request) {
        Consumer<RemoteLogDownloadFuture> callback =
                request.downloadFuture != null
                        ? request.downloadFuture.getNextChunkCallback()
                        : null;

        CompletableFuture<LogRecords> nextChunkFuture = new CompletableFuture<>();
        request.chunkFuture = nextChunkFuture;

        RemoteLogDownloadFuture nextFuture =
                new RemoteLogDownloadFuture(nextChunkFuture, () -> onChunkConsumed(request));
        if (callback != null) {
            nextFuture.setNextChunkCallback(callback);
        }
        request.downloadFuture = nextFuture;
        return nextFuture;
    }

    /** Cleans up resources for a completed request: closes reader, deletes temp file. */
    private void cleanupRequest(RemoteLogDownloadRequest request) {
        if (request.cleanedUp) {
            return;
        }
        request.cleanedUp = true;
        if (request.reader != null) {
            request.reader.close();
            request.reader = null;
        }
        if (request.localFileRecords != null) {
            try {
                request.localFileRecords.deleteIfExists();
            } catch (IOException e) {
                LOG.warn(
                        "Failed to delete temp file for segment {}.",
                        request.segment.remoteLogSegmentId(),
                        e);
            }
            request.localFileRecords = null;
        }
    }

    /**
     * Reads one chunk from a remote log segment. Continuations (subsequent chunks of an
     * already-opened segment) are processed first without acquiring the semaphore. New segments
     * require a semaphore permit.
     */
    void fetchOnce() throws Exception {
        // Priority 1: process continuations (no semaphore needed).
        RemoteLogDownloadRequest request = continuationQueue.poll();
        if (request != null) {
            synchronized (request) {
                request.queuedForContinuation = false;
            }
            processChunkRead(request);
            return;
        }

        // Priority 2: process new segments (semaphore needed).
        // Use tryAcquire with timeout so the thread can periodically loop back and check
        // the continuation queue for new chunk requests from already-active segments.
        if (!prefetchSemaphore.tryAcquire(pollTimeout, TimeUnit.MILLISECONDS)) {
            return;
        }
        try {
            request = segmentsToFetch.poll(pollTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // Release the permit before propagating the interrupt, so that
            // shutdown does not leak semaphore permits.
            prefetchSemaphore.release();
            throw e;
        }
        if (request == null) {
            prefetchSemaphore.release();
            return;
        }
        processChunkRead(request);
    }

    private void processChunkRead(RemoteLogDownloadRequest request) {
        TableBucket tableBucket = request.getTableBucket();
        // Capture the chunkFuture early. A concurrent consumer thread may call
        // createAndQueueNextChunk (via onChunkConsumed) and replace request.chunkFuture
        // while we are reading/writing the chunk data.
        final CompletableFuture<LogRecords> currentChunkFuture = request.chunkFuture;
        try {
            // Lazily initialize the chunk reader and local temp file on first access.
            if (request.reader == null) {
                FsPath remotePath =
                        getRemoteLogFilePath(request.remoteLogTabletDir, request.segment);
                FileSystem fs = remotePath.getFileSystem();
                FSDataInputStream inputStream = fs.open(remotePath);
                request.reader =
                        new RemoteSegmentChunkReader(
                                inputStream,
                                request.startPosition,
                                request.segment.segmentSizeInBytes());

                // Create a temp file for this segment. The File reference is owned by
                // localFileRecords; deleteIfExists() in cleanupRequest handles removal.
                File tempFile =
                        new File(
                                tmpDir,
                                "remote-chunk-"
                                        + request.segment.remoteLogSegmentId()
                                        + "-"
                                        + UUID.randomUUID()
                                        + ".tmp");
                request.localFileRecords = FileLogRecords.open(tempFile, true, false, 0, false);
                request.localFileWrittenPosition = 0;
            }

            long startTime = System.currentTimeMillis();

            // Read chunk from remote into memory (temporary).
            MemoryLogRecords chunkData = request.reader.readNextChunk(chunkSize);

            // Handle empty chunk (EOF or segment exhausted). This can happen when:
            // 1. The segment has 0 bytes (degenerate case)
            // 2. A continuation read reaches EOF (e.g., segment size metadata was larger
            //    than the actual file, or the previous chunk already read all data)
            //
            // For the empty sentinel we do NOT increment chunksWritten. Instead, we directly
            // invoke tryScheduleNextChunk which will find exhausted=true and
            // chunksConsumed >= chunksWritten, triggering cleanup and semaphore release on the
            // download thread. The consumer's later recycleCallback (from drain()) is harmless
            // because cleanedUp=true will cause tryScheduleNextChunk to return immediately.
            if (chunkData.sizeInBytes() == 0) {
                currentChunkFuture.complete(MemoryLogRecords.EMPTY);
                tryScheduleNextChunk(request);
                return;
            }

            scannerMetricGroup.remoteFetchRequestCount().inc();

            // Append to local temp file and create a slice for this chunk.
            int chunkStart = request.localFileWrittenPosition;
            int written = request.localFileRecords.append(chunkData);
            request.localFileWrittenPosition += written;

            FileLogRecords slice = request.localFileRecords.slice(chunkStart, written);
            request.chunksWritten.incrementAndGet();

            scannerMetricGroup.remoteFetchBytes().inc(written);
            LOG.debug(
                    "Read remote log chunk of {} bytes for bucket {} in {} ms.",
                    written,
                    tableBucket,
                    System.currentTimeMillis() - startTime);

            currentChunkFuture.complete(slice);

            // Try to pre-fetch next chunk if flow control allows.
            tryScheduleNextChunk(request);
        } catch (Throwable t) {
            synchronized (request) {
                cleanupRequest(request);
            }
            currentChunkFuture.completeExceptionally(t);
            // Release semaphore: cleanedUp flag prevents double-release via
            // tryScheduleNextChunk when the consumer later calls recycle.
            prefetchSemaphore.release();
            scannerMetricGroup.remoteFetchErrorCount().inc();
            LOG.error("Failed to read remote log chunk for table bucket {}.", tableBucket, t);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            downloadThread.shutdown();
        } catch (InterruptedException e) {
            // ignore
        }
        // Cleanup all pending requests.
        for (RemoteLogDownloadRequest req : segmentsToFetch) {
            synchronized (req) {
                cleanupRequest(req);
            }
        }
        for (RemoteLogDownloadRequest req : continuationQueue) {
            synchronized (req) {
                cleanupRequest(req);
            }
        }
    }

    @VisibleForTesting
    Semaphore getPrefetchSemaphore() {
        return prefetchSemaphore;
    }

    @VisibleForTesting
    int getSizeOfSegmentsToFetch() {
        return segmentsToFetch.size();
    }

    /** Returns the remote file path for the given segment. */
    @VisibleForTesting
    static FsPath getRemoteLogFilePath(FsPath remoteLogTabletDir, RemoteLogSegment segment) {
        return remoteLogSegmentFile(
                remoteLogSegmentDir(remoteLogTabletDir, segment.remoteLogSegmentId()),
                segment.remoteLogStartOffset());
    }

    /**
     * Thread to read remote log chunks. The thread will keep reading chunks until it is
     * interrupted.
     */
    private class DownloadRemoteLogThread extends ShutdownableThread {
        public DownloadRemoteLogThread(TablePath tablePath) {
            super(String.format("DownloadRemoteLog-[%s]", tablePath.toString()), true);
        }

        @Override
        public void doWork() throws Exception {
            fetchOnce();
        }
    }

    @VisibleForTesting
    int getMaxPrefetchChunks() {
        return maxPrefetchChunks;
    }

    /** Represents a request to read a remote log segment in chunks. */
    static class RemoteLogDownloadRequest implements Comparable<RemoteLogDownloadRequest> {
        final RemoteLogSegment segment;
        final FsPath remoteLogTabletDir;
        final int startPosition;
        CompletableFuture<LogRecords> chunkFuture;
        RemoteSegmentChunkReader reader;
        RemoteLogDownloadFuture downloadFuture;

        // Local temp file state.
        FileLogRecords localFileRecords;
        int localFileWrittenPosition;

        // Flow control counters.
        final AtomicInteger chunksWritten = new AtomicInteger(0);
        final AtomicInteger chunksConsumed = new AtomicInteger(0);
        volatile boolean queuedForContinuation;
        boolean cleanedUp;

        RemoteLogDownloadRequest(
                RemoteLogSegment segment,
                FsPath remoteLogTabletDir,
                int startPosition,
                CompletableFuture<LogRecords> chunkFuture) {
            this.segment = segment;
            this.remoteLogTabletDir = remoteLogTabletDir;
            this.startPosition = startPosition;
            this.chunkFuture = chunkFuture;
        }

        public TableBucket getTableBucket() {
            return segment.tableBucket();
        }

        @Override
        public int compareTo(RemoteLogDownloadRequest o) {
            if (segment.tableBucket().equals(o.segment.tableBucket())) {
                // strictly download in the offset order if they belong to the same bucket
                return Long.compare(
                        segment.remoteLogStartOffset(), o.segment.remoteLogStartOffset());
            } else {
                // download segment from old to new across buckets
                return Long.compare(segment.maxTimestamp(), o.segment.maxTimestamp());
            }
        }
    }
}
