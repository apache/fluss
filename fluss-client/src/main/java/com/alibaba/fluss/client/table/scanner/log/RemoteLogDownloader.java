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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.client.metrics.ScannerMetricGroup;
import com.alibaba.fluss.client.table.scanner.RemoteFileDownloader;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.FsPathAndFileName;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.FileUtils.deleteDirectoryQuietly;
import static com.alibaba.fluss.utils.FlussPaths.LOG_FILE_SUFFIX;
import static com.alibaba.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static com.alibaba.fluss.utils.FlussPaths.remoteLogSegmentFile;

/** Downloader to read remote log files to local disk. */
@ThreadSafe
@Internal
public class RemoteLogDownloader implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogDownloader.class);

    private static final long POLL_SLEEP_TIMEOUT = 500L;

    private final Path localLogDir;

    private final BlockingQueue<RemoteLogDownloadRequest> segmentsToFetch;

    private final BlockingQueue<RemoteLogSegment> segmentsToRecycle;

    private final Semaphore prefetchSemaphore;

    private final DownloadRemoteLogThread downloadThread;

    private final RemoteFileDownloader remoteFileDownloader;

    private final ScannerMetricGroup scannerMetricGroup;

    private final long pollSleepTimeout;

    public RemoteLogDownloader(
            TablePath tablePath,
            Configuration conf,
            RemoteFileDownloader remoteFileDownloader,
            ScannerMetricGroup scannerMetricGroup) {
        // default we give a 5s long interval to avoid frequent loop
        this(tablePath, conf, remoteFileDownloader, scannerMetricGroup, POLL_SLEEP_TIMEOUT);
    }

    @VisibleForTesting
    RemoteLogDownloader(
            TablePath tablePath,
            Configuration conf,
            RemoteFileDownloader remoteFileDownloader,
            ScannerMetricGroup scannerMetricGroup,
            long pollSleepTimeout) {
        this.segmentsToFetch = new LinkedBlockingQueue<>();
        this.segmentsToRecycle = new LinkedBlockingQueue<>();
        this.remoteFileDownloader = remoteFileDownloader;
        this.scannerMetricGroup = scannerMetricGroup;
        this.pollSleepTimeout = pollSleepTimeout;
        this.prefetchSemaphore =
                new Semaphore(conf.getInt(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM));
        // The local tmp dir to store the fetched log segment files,
        // add UUID to avoid conflict between tasks.
        this.localLogDir =
                Paths.get(
                        conf.get(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR),
                        "remote-logs-" + UUID.randomUUID());
        this.downloadThread = new DownloadRemoteLogThread(tablePath);
    }

    public void start() {
        downloadThread.start();
    }

    /** Request to fetch remote log segment to local. This method is non-blocking. */
    public RemoteLogDownloadFuture requestRemoteLog(FsPath logTabletDir, RemoteLogSegment segment) {
        RemoteLogDownloadRequest request = new RemoteLogDownloadRequest(segment, logTabletDir);
        segmentsToFetch.add(request);
        return new RemoteLogDownloadFuture(request.future, () -> recycleRemoteLog(segment));
    }

    /**
     * Recycle the consumed remote log. The removal of the log file is async in the {@link
     * #downloadThread}.
     */
    void recycleRemoteLog(RemoteLogSegment segment) {
        segmentsToRecycle.add(segment);
        prefetchSemaphore.release();
    }

    /**
     * Fetch a remote log segment file to local. This method will block until there is a log segment
     * to fetch.
     */
    void fetchOnce() throws Exception {
        int availablePermits = prefetchSemaphore.availablePermits();
        List<RemoteLogDownloadRequest> requests = new ArrayList<>(availablePermits);
        segmentsToFetch.drainTo(requests, availablePermits);
        if (requests.size() <= 0) {
            Thread.sleep(pollSleepTimeout);
            return;
        }

        prefetchSemaphore.acquire(requests.size());
        try {
            // 1. cleanup the finished logs first to free up disk space
            cleanupRemoteLogs();

            List<FsPathAndFileName> fsPathAndFileNames = new ArrayList<>();
            for (RemoteLogDownloadRequest request : requests) {
                FsPathAndFileName fsPathAndFileName = request.getFsPathAndFileName();
                fsPathAndFileNames.add(fsPathAndFileName);
                scannerMetricGroup.remoteFetchRequestCount().inc();
            }
            // download the remote file to local

            long startTime = System.currentTimeMillis();
            List<String> fileNames =
                    fsPathAndFileNames.stream()
                            .map(FsPathAndFileName::getFileName)
                            .collect(Collectors.toList());
            LOG.info(
                    "Start to download {} remote log segment files {} to local.",
                    fileNames.size(),
                    fileNames);
            remoteFileDownloader.transferAllToDirectory(
                    fsPathAndFileNames, localLogDir, new CloseableRegistry());
            LOG.info(
                    "Download {} remote log segment files {} to local cost {} ms.",
                    fileNames.size(),
                    fileNames,
                    System.currentTimeMillis() - startTime);
            for (int i = 0; i < fsPathAndFileNames.size(); i++) {
                RemoteLogDownloadRequest request = requests.get(i);
                FsPathAndFileName fsPathAndFileName = fsPathAndFileNames.get(i);
                File localFile = new File(localLogDir.toFile(), fsPathAndFileName.getFileName());
                scannerMetricGroup.remoteFetchBytes().inc(localFile.length());
                request.future.complete(localFile);
            }
        } catch (Throwable t) {
            prefetchSemaphore.release(requests.size());
            // add back the request to the queue
            segmentsToFetch.addAll(requests);
            scannerMetricGroup.remoteFetchErrorCount().inc(requests.size());
            // log the error and continue instead of shutdown the download thread
            LOG.error("Failed to download remote log segment.", t);
        }
    }

    private void cleanupRemoteLogs() {
        RemoteLogSegment segment;
        while ((segment = segmentsToRecycle.poll()) != null) {
            cleanupFinishedRemoteLog(segment);
        }
    }

    private void cleanupFinishedRemoteLog(RemoteLogSegment segment) {
        try {
            Path logFile = localLogDir.resolve(getLocalFileNameOfRemoteSegment(segment));
            Files.deleteIfExists(logFile);
            LOG.info(
                    "Consumed and deleted the fetched log segment file {} for bucket {}.",
                    logFile.getFileName(),
                    segment.tableBucket());
        } catch (IOException e) {
            LOG.warn("Failed to delete the local fetch segment file {}.", localLogDir, e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            downloadThread.shutdown();
        } catch (InterruptedException e) {
            // ignore
        }

        deleteDirectoryQuietly(localLogDir.toFile());
    }

    @VisibleForTesting
    Semaphore getPrefetchSemaphore() {
        return prefetchSemaphore;
    }

    @VisibleForTesting
    Path getLocalLogDir() {
        return localLogDir;
    }

    protected static FsPathAndFileName getFsPathAndFileName(
            FsPath remoteLogTabletDir, RemoteLogSegment segment) {
        FsPath remotePath =
                remoteLogSegmentFile(
                        remoteLogSegmentDir(remoteLogTabletDir, segment.remoteLogSegmentId()),
                        segment.remoteLogStartOffset());
        return new FsPathAndFileName(remotePath, getLocalFileNameOfRemoteSegment(segment));
    }

    /**
     * Get the local file name of the remote log segment.
     *
     * <p>The file name is in pattern:
     *
     * <pre>
     *     {$remote_segment_id}_{$offset_prefix}.log
     * </pre>
     */
    private static String getLocalFileNameOfRemoteSegment(RemoteLogSegment segment) {
        return segment.remoteLogSegmentId()
                + "_"
                + FlussPaths.filenamePrefixFromOffset(segment.remoteLogStartOffset())
                + LOG_FILE_SUFFIX;
    }

    /**
     * Thread to download remote log files to local. The thread will keep fetching remote log files
     * until it is interrupted.
     */
    private class DownloadRemoteLogThread extends ShutdownableThread {
        public DownloadRemoteLogThread(TablePath tablePath) {
            super(String.format("DownloadRemoteLog-[%s]", tablePath.toString()), true);
        }

        @Override
        public void doWork() throws Exception {
            fetchOnce();
            cleanupRemoteLogs();
        }
    }

    /** Represents a request to download a remote log segment file to local. */
    private static class RemoteLogDownloadRequest {
        private final RemoteLogSegment segment;
        private final FsPath remoteLogTabletDir;
        private final CompletableFuture<File> future = new CompletableFuture<>();

        public RemoteLogDownloadRequest(RemoteLogSegment segment, FsPath remoteLogTabletDir) {
            this.segment = segment;
            this.remoteLogTabletDir = remoteLogTabletDir;
        }

        public FsPathAndFileName getFsPathAndFileName() {
            return RemoteLogDownloader.getFsPathAndFileName(remoteLogTabletDir, segment);
        }
    }
}
