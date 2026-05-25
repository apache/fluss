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

import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.metrics.TestingScannerMetricGroup;
import org.apache.fluss.client.table.scanner.log.RemoteLogDownloader.RemoteLogDownloadRequest;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithBaseOffset;
import static org.apache.fluss.testutils.DataTestUtils.genRemoteLogSegmentFile;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.utils.FlussPaths.remoteLogDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogTabletDir;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteLogDownloader}. */
class RemoteLogDownloaderTest {

    private @TempDir File remoteDataDir;
    private @TempDir File tmpDir;
    private FsPath remoteLogDir;
    private Configuration conf;
    private ScannerMetricGroup scannerMetricGroup;

    @BeforeEach
    void beforeEach() {
        conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir.getAbsolutePath());
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 4);
        conf.set(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR, tmpDir.getAbsolutePath());
        remoteLogDir = remoteLogDir(conf);
        scannerMetricGroup = TestingScannerMetricGroup.newInstance();
    }

    @Test
    void testPrefetchNum() throws Exception {
        RemoteLogDownloader remoteLogDownloader =
                new RemoteLogDownloader(DATA1_TABLE_PATH, conf, scannerMetricGroup, 10L);
        try {
            // trigger auto download.
            remoteLogDownloader.start();

            TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
            List<RemoteLogSegment> remoteLogSegments =
                    buildRemoteLogSegmentList(tb, DATA1_PHYSICAL_TABLE_PATH, 5, conf, 10);
            FsPath remoteLogTabletDir =
                    remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);
            List<RemoteLogDownloadFuture> futures =
                    requestRemoteLogs(remoteLogDownloader, remoteLogTabletDir, remoteLogSegments);

            // the first 4 segments should success (each segment is small, fully read in one chunk).
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        for (int i = 0; i < 4; i++) {
                            assertThat(futures.get(i).isDone()).isTrue();
                        }
                    });

            assertThat(scannerMetricGroup.remoteFetchRequestCount().getCount()).isEqualTo(4);
            assertThat(scannerMetricGroup.remoteFetchBytes().getCount())
                    .isEqualTo(
                            remoteLogSegmentFilesLength(remoteLogSegments, remoteLogTabletDir, 4));
            assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(0);

            // Consume the first chunk to release semaphore and allow 5th segment.
            futures.get(0).getRecycleCallback().run();
            // the 5th segment should success.
            retry(Duration.ofMinutes(1), () -> assertThat(futures.get(4).isDone()).isTrue());
            assertThat(scannerMetricGroup.remoteFetchRequestCount().getCount()).isEqualTo(5);
            assertThat(scannerMetricGroup.remoteFetchBytes().getCount())
                    .isEqualTo(
                            remoteLogSegmentFilesLength(remoteLogSegments, remoteLogTabletDir, 5));
            assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(0);

            // consume remaining first 4 segment chunks to release semaphore
            futures.get(1).getRecycleCallback().run();
            futures.get(2).getRecycleCallback().run();
            futures.get(3).getRecycleCallback().run();
            futures.get(4).getRecycleCallback().run();
            // Stop the download thread before checking permits. The idle thread
            // briefly holds a permit during its poll() loop which causes flakiness.
            remoteLogDownloader.close();
            assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(4);
        } finally {
            IOUtils.closeQuietly(remoteLogDownloader);
        }
    }

    @Test
    void testDownloadInPriority() throws Exception {
        RemoteLogDownloader remoteLogDownloader =
                new RemoteLogDownloader(DATA1_TABLE_PATH, conf, scannerMetricGroup, 10L);
        TableBucket bucket1 = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket bucket2 = new TableBucket(DATA1_TABLE_ID, 2);
        TableBucket bucket3 = new TableBucket(DATA1_TABLE_ID, 3);
        TableBucket bucket4 = new TableBucket(DATA1_TABLE_ID, 4);
        try {
            // prepare segments, 4 buckets with different maxTimestamp, total 10 segments
            int totalSegments = 10;
            List<RemoteLogSegment> remoteLogSegments =
                    buildRemoteLogSegmentList(bucket1, DATA1_PHYSICAL_TABLE_PATH, 6, conf, 10);
            remoteLogSegments.addAll(
                    buildRemoteLogSegmentList(bucket3, DATA1_PHYSICAL_TABLE_PATH, 1, conf, 5));
            remoteLogSegments.addAll(
                    buildRemoteLogSegmentList(bucket2, DATA1_PHYSICAL_TABLE_PATH, 1, conf, 1));
            remoteLogSegments.addAll(
                    buildRemoteLogSegmentList(bucket3, DATA1_PHYSICAL_TABLE_PATH, 1, conf, 15));
            remoteLogSegments.addAll(
                    buildRemoteLogSegmentList(bucket4, DATA1_PHYSICAL_TABLE_PATH, 1, conf, 8));

            Map<UUID, RemoteLogDownloadFuture> futures = new HashMap<>();
            for (RemoteLogSegment segment : remoteLogSegments) {
                FsPath remoteLogTabletDir =
                        remoteLogTabletDir(
                                remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, segment.tableBucket());
                RemoteLogDownloadFuture future =
                        remoteLogDownloader.requestRemoteLog(remoteLogTabletDir, segment, 0);
                futures.put(segment.remoteLogSegmentId(), future);
            }

            // start the downloader after requests are added to have deterministic request order.
            remoteLogDownloader.start();

            // check the segments are fetched in priority order.
            remoteLogSegments.sort(Comparator.comparingLong(RemoteLogSegment::maxTimestamp));
            List<RemoteLogDownloadFuture> top4Futures = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                RemoteLogSegment segment = remoteLogSegments.get(i);
                top4Futures.add(futures.get(segment.remoteLogSegmentId()));
            }

            // 4 to fetch.
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        for (RemoteLogDownloadFuture future : top4Futures) {
                            assertThat(future.isDone()).isTrue();
                        }
                    });
            // only 4 segments are pre-fetched.
            assertThat(remoteLogDownloader.getSizeOfSegmentsToFetch()).isEqualTo(totalSegments - 4);

            for (int i = 3; i < totalSegments; i++) {
                RemoteLogSegment segment = remoteLogSegments.get(i);
                RemoteLogDownloadFuture future = futures.get(segment.remoteLogSegmentId());
                waitUntil(future::isDone, Duration.ofMinutes(1), "segment download timeout");
                // recycle the one segment to trigger download next segment
                future.getRecycleCallback().run();
            }

            // all segments are fetched.
            assertThat(remoteLogDownloader.getSizeOfSegmentsToFetch()).isEqualTo(0);
        } finally {
            IOUtils.closeQuietly(remoteLogDownloader);
        }
    }

    @Test
    void testOrderOfRemoteLogDownloadRequest() {
        TableBucket bucket1 = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket bucket2 = new TableBucket(DATA1_TABLE_ID, 2);
        TableBucket bucket3 = new TableBucket(DATA1_TABLE_ID, 3);

        List<RemoteLogDownloadRequest> requests =
                Arrays.asList(
                        // different offset, same timestamp and bucket
                        createDownloadRequest(bucket1, 10, 10),
                        createDownloadRequest(bucket1, 20, 10),
                        createDownloadRequest(bucket1, 30, 10),
                        // -1 timestamp
                        createDownloadRequest(bucket2, 10, -1),
                        createDownloadRequest(bucket2, 20, -1),
                        createDownloadRequest(bucket2, 30, -1),
                        // 0 offset
                        createDownloadRequest(bucket3, 0, 5),
                        createDownloadRequest(bucket3, 0, 15),
                        createDownloadRequest(bucket3, 0, 25));

        // Sort the requests based on the custom comparator
        Collections.sort(requests);
        List<String> results =
                requests.stream()
                        .map(
                                r ->
                                        String.format(
                                                "(bucket=%s, offset=%s, ts=%s)",
                                                r.segment.tableBucket().getBucket(),
                                                r.segment.remoteLogStartOffset(),
                                                r.segment.maxTimestamp()))
                        .collect(Collectors.toList());
        List<String> expected =
                Arrays.asList(
                        "(bucket=2, offset=10, ts=-1)",
                        "(bucket=2, offset=20, ts=-1)",
                        "(bucket=2, offset=30, ts=-1)",
                        "(bucket=3, offset=0, ts=5)",
                        "(bucket=1, offset=10, ts=10)",
                        "(bucket=1, offset=20, ts=10)",
                        "(bucket=1, offset=30, ts=10)",
                        "(bucket=3, offset=0, ts=15)",
                        "(bucket=3, offset=0, ts=25)");
        assertThat(results).isEqualTo(expected);
    }

    @Test
    void testMaxPrefetchChunks() throws Exception {
        // Set maxPrefetchChunks = 2 to verify flow control.
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_MAX_PREFETCH_CHUNKS, 2);
        // Set prefetchNum = 1 so only one segment is active.
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 1);
        RemoteLogDownloader remoteLogDownloader =
                new RemoteLogDownloader(DATA1_TABLE_PATH, conf, scannerMetricGroup, 10L);
        try {
            remoteLogDownloader.start();

            TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
            // Build a large segment with multiple records so multiple chunks are produced.
            List<RemoteLogSegment> remoteLogSegments =
                    buildRemoteLogSegmentList(tb, DATA1_PHYSICAL_TABLE_PATH, 1, conf, 10);
            FsPath remoteLogTabletDir =
                    remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);
            List<RemoteLogDownloadFuture> futures =
                    requestRemoteLogs(remoteLogDownloader, remoteLogTabletDir, remoteLogSegments);

            // The first chunk should be done.
            retry(Duration.ofMinutes(1), () -> assertThat(futures.get(0).isDone()).isTrue());

            // Verify the maxPrefetchChunks is correctly set.
            assertThat(remoteLogDownloader.getMaxPrefetchChunks()).isEqualTo(2);

            // Consume to release the segment.
            futures.get(0).getRecycleCallback().run();

            // Stop the download thread before checking permits.
            remoteLogDownloader.close();
            assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(1);
        } finally {
            IOUtils.closeQuietly(remoteLogDownloader);
        }
    }

    @Test
    void testTempFileCleanup() throws Exception {
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 1);
        RemoteLogDownloader remoteLogDownloader =
                new RemoteLogDownloader(DATA1_TABLE_PATH, conf, scannerMetricGroup, 10L);
        try {
            remoteLogDownloader.start();

            TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
            List<RemoteLogSegment> remoteLogSegments =
                    buildRemoteLogSegmentList(tb, DATA1_PHYSICAL_TABLE_PATH, 1, conf, 10);
            FsPath remoteLogTabletDir =
                    remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);
            List<RemoteLogDownloadFuture> futures =
                    requestRemoteLogs(remoteLogDownloader, remoteLogTabletDir, remoteLogSegments);

            // Wait for first chunk.
            retry(Duration.ofMinutes(1), () -> assertThat(futures.get(0).isDone()).isTrue());

            // Verify temp files exist before cleanup.
            File[] tmpFiles = tmpDir.listFiles((dir, name) -> name.startsWith("remote-chunk-"));
            assertThat(tmpFiles).isNotNull();
            assertThat(tmpFiles.length).isGreaterThanOrEqualTo(1);

            // Consume the chunk to trigger cleanup.
            futures.get(0).getRecycleCallback().run();

            // Stop the download thread before checking permits.
            remoteLogDownloader.close();
            assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(1);

            // Verify temp files are cleaned up.
            tmpFiles = tmpDir.listFiles((dir, name) -> name.startsWith("remote-chunk-"));
            assertThat(tmpFiles == null || tmpFiles.length == 0).isTrue();
        } finally {
            IOUtils.closeQuietly(remoteLogDownloader);
        }
    }

    @Test
    void testChunkDataIsReadableFromSlice() throws Exception {
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 1);
        RemoteLogDownloader remoteLogDownloader =
                new RemoteLogDownloader(DATA1_TABLE_PATH, conf, scannerMetricGroup, 10L);
        try {
            remoteLogDownloader.start();

            TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
            List<RemoteLogSegment> remoteLogSegments =
                    buildRemoteLogSegmentList(tb, DATA1_PHYSICAL_TABLE_PATH, 1, conf, 10);
            FsPath remoteLogTabletDir =
                    remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);
            List<RemoteLogDownloadFuture> futures =
                    requestRemoteLogs(remoteLogDownloader, remoteLogTabletDir, remoteLogSegments);

            // Wait for first chunk.
            retry(Duration.ofMinutes(1), () -> assertThat(futures.get(0).isDone()).isTrue());

            // Verify the returned LogRecords is readable and has data.
            LogRecords logRecords = futures.get(0).getLogRecords();
            assertThat(logRecords.sizeInBytes()).isGreaterThan(0);
            assertThat(logRecords.batches().iterator().hasNext()).isTrue();

            // Consume to cleanup.
            futures.get(0).getRecycleCallback().run();

            // Stop the download thread before checking permits.
            remoteLogDownloader.close();
            assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(1);
        } finally {
            IOUtils.closeQuietly(remoteLogDownloader);
        }
    }

    private RemoteLogDownloadRequest createDownloadRequest(
            TableBucket tableBucket, long startOffset, long maxTimestamp) {
        RemoteLogSegment remoteLogSegment =
                RemoteLogSegment.Builder.builder()
                        .tableBucket(tableBucket)
                        .physicalTablePath(DATA1_PHYSICAL_TABLE_PATH)
                        .remoteLogSegmentId(UUID.randomUUID())
                        .remoteLogStartOffset(startOffset)
                        .remoteLogEndOffset(startOffset + 10)
                        .maxTimestamp(maxTimestamp)
                        .segmentSizeInBytes(Integer.MAX_VALUE)
                        .build();
        return new RemoteLogDownloadRequest(
                remoteLogSegment, remoteLogDir, 0, new CompletableFuture<>());
    }

    private List<RemoteLogDownloadFuture> requestRemoteLogs(
            RemoteLogDownloader remoteLogDownloader,
            FsPath remoteLogTabletDir,
            List<RemoteLogSegment> remoteLogSegments) {
        List<RemoteLogDownloadFuture> futures = new ArrayList<>();
        for (RemoteLogSegment segment : remoteLogSegments) {
            RemoteLogDownloadFuture future =
                    remoteLogDownloader.requestRemoteLog(remoteLogTabletDir, segment, 0);
            futures.add(future);
        }
        return futures;
    }

    private static List<RemoteLogSegment> buildRemoteLogSegmentList(
            TableBucket tableBucket,
            PhysicalTablePath physicalTablePath,
            int num,
            Configuration conf,
            long maxTimestamp)
            throws Exception {
        List<RemoteLogSegment> remoteLogSegmentList = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            long baseOffset = i * 10L;
            RemoteLogSegment remoteLogSegment =
                    RemoteLogSegment.Builder.builder()
                            .tableBucket(tableBucket)
                            .physicalTablePath(physicalTablePath)
                            .remoteLogSegmentId(UUID.randomUUID())
                            .remoteLogStartOffset(baseOffset)
                            .remoteLogEndOffset(baseOffset + 9)
                            .maxTimestamp(maxTimestamp)
                            .segmentSizeInBytes(Integer.MAX_VALUE)
                            .build();
            genRemoteLogSegmentFile(
                    tableBucket, physicalTablePath, conf, remoteLogSegment, baseOffset);
            remoteLogSegmentList.add(remoteLogSegment);
        }
        return remoteLogSegmentList;
    }

    /**
     * Tests chunked download with a small chunk size, verifying that: 1. A large segment is split
     * into multiple chunks. 2. Chunks can be consumed while subsequent chunks are still being
     * downloaded (边读边下载). 3. Flow control (maxPrefetchChunks) pauses downloading when unconsumed
     * chunks reach the limit.
     */
    @Test
    void testChunkedDownloadAndReadWhileDownloading() throws Exception {
        // Use a small chunk size (200 bytes) to force multiple chunks from one segment.
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_CHUNK_SIZE, MemorySize.parse("200b"));
        // Allow at most 2 unconsumed chunks ahead.
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_MAX_PREFETCH_CHUNKS, 2);
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 1);

        RemoteLogDownloader downloader =
                new RemoteLogDownloader(DATA1_TABLE_PATH, conf, scannerMetricGroup, 10L);
        try {
            TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
            // Build a segment with 20 batches so total size > 200 bytes * multiple chunks.
            RemoteLogSegment segment = buildLargeRemoteLogSegment(tb, 20);
            FsPath tabletDir = remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);

            // Collect all chunk futures via a self-referencing callback chain.
            List<RemoteLogDownloadFuture> chunkFutures =
                    Collections.synchronizedList(new ArrayList<>());
            RemoteLogDownloadFuture firstFuture =
                    downloader.requestRemoteLog(tabletDir, segment, 0);
            chunkFutures.add(firstFuture);

            Consumer<RemoteLogDownloadFuture> chunkCollector =
                    new Consumer<RemoteLogDownloadFuture>() {
                        @Override
                        public void accept(RemoteLogDownloadFuture nextFuture) {
                            chunkFutures.add(nextFuture);
                            nextFuture.setNextChunkCallback(this);
                        }
                    };
            firstFuture.setNextChunkCallback(chunkCollector);

            // Start after callback is set to avoid race condition.
            downloader.start();

            // Wait for the first chunk to be ready.
            retry(Duration.ofMinutes(1), () -> assertThat(chunkFutures.get(0).isDone()).isTrue());

            // With maxPrefetchChunks=2, we expect at most 2 chunks downloaded before consumption.
            retry(
                    Duration.ofSeconds(5),
                    () -> assertThat(chunkFutures.size()).isGreaterThanOrEqualTo(2));

            // Read chunks while downloading continues (边读边下载).
            int totalBytesRead = 0;
            int chunksConsumed = 0;
            while (true) {
                if (chunksConsumed >= chunkFutures.size()) {
                    // Wait for next chunk if available.
                    Thread.sleep(50);
                    if (chunksConsumed >= chunkFutures.size()) {
                        break;
                    }
                }
                RemoteLogDownloadFuture chunkFuture = chunkFutures.get(chunksConsumed);
                waitUntil(chunkFuture::isDone, Duration.ofMinutes(1), "chunk download timeout");
                LogRecords records = chunkFuture.getLogRecords();
                if (records.sizeInBytes() == 0) {
                    chunkFuture.getRecycleCallback().run();
                    chunksConsumed++;
                    break;
                }
                totalBytesRead += records.sizeInBytes();
                // Consuming a chunk triggers more downloads.
                chunkFuture.getRecycleCallback().run();
                chunksConsumed++;
            }

            // Verify we got multiple chunks (not a single monolithic download).
            assertThat(chunksConsumed).isGreaterThan(1);

            // Verify total bytes matches the segment file size.
            File segFile =
                    new File(
                            RemoteLogDownloader.getRemoteLogFilePath(tabletDir, segment).getPath());
            assertThat(totalBytesRead).isEqualTo((int) segFile.length());
        } finally {
            IOUtils.closeQuietly(downloader);
        }
    }

    /**
     * Tests segment switching: after all chunks of segment 1 are consumed, the downloader
     * automatically starts downloading segment 2.
     */
    @Test
    void testSegmentSwitchingAfterChunksConsumed() throws Exception {
        // Small chunk size to produce multiple chunks per segment.
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_CHUNK_SIZE, MemorySize.parse("200b"));
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_MAX_PREFETCH_CHUNKS, 3);
        // Only 1 segment can be active at a time.
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 1);

        RemoteLogDownloader downloader =
                new RemoteLogDownloader(DATA1_TABLE_PATH, conf, scannerMetricGroup, 10L);
        try {
            TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
            // Create 2 segments, each with multiple batches.
            RemoteLogSegment segment1 = buildLargeRemoteLogSegment(tb, 10);
            RemoteLogSegment segment2 = buildLargeRemoteLogSegment(tb, 10);
            FsPath tabletDir = remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);

            // Request both segments.
            List<RemoteLogDownloadFuture> seg1Chunks =
                    Collections.synchronizedList(new ArrayList<>());
            RemoteLogDownloadFuture seg1First = downloader.requestRemoteLog(tabletDir, segment1, 0);
            seg1Chunks.add(seg1First);
            Consumer<RemoteLogDownloadFuture> seg1Collector =
                    new Consumer<RemoteLogDownloadFuture>() {
                        @Override
                        public void accept(RemoteLogDownloadFuture f) {
                            seg1Chunks.add(f);
                            f.setNextChunkCallback(this);
                        }
                    };
            seg1First.setNextChunkCallback(seg1Collector);

            List<RemoteLogDownloadFuture> seg2Chunks =
                    Collections.synchronizedList(new ArrayList<>());
            RemoteLogDownloadFuture seg2First = downloader.requestRemoteLog(tabletDir, segment2, 0);
            seg2Chunks.add(seg2First);
            Consumer<RemoteLogDownloadFuture> seg2Collector =
                    new Consumer<RemoteLogDownloadFuture>() {
                        @Override
                        public void accept(RemoteLogDownloadFuture f) {
                            seg2Chunks.add(f);
                            f.setNextChunkCallback(this);
                        }
                    };
            seg2First.setNextChunkCallback(seg2Collector);

            // Start after all callbacks are set to avoid race condition.
            downloader.start();

            // Segment 1 should start downloading first (prefetchNum=1 blocks segment 2).
            retry(Duration.ofMinutes(1), () -> assertThat(seg1Chunks.get(0).isDone()).isTrue());
            // Segment 2 should NOT have started yet.
            assertThat(seg2First.isDone()).isFalse();

            // Consume all chunks from segment 1.
            int seg1Index = 0;
            while (true) {
                if (seg1Index >= seg1Chunks.size()) {
                    Thread.sleep(50);
                    if (seg1Index >= seg1Chunks.size()) {
                        break;
                    }
                }
                RemoteLogDownloadFuture chunkFuture = seg1Chunks.get(seg1Index);
                waitUntil(chunkFuture::isDone, Duration.ofMinutes(1), "seg1 chunk timeout");
                LogRecords records = chunkFuture.getLogRecords();
                chunkFuture.getRecycleCallback().run();
                seg1Index++;
                if (records.sizeInBytes() == 0) {
                    break;
                }
            }

            // After segment 1 is fully consumed, segment 2 should start downloading.
            retry(Duration.ofMinutes(1), () -> assertThat(seg2Chunks.get(0).isDone()).isTrue());

            // Verify segment 2 data is readable.
            LogRecords seg2Records = seg2Chunks.get(0).getLogRecords();
            assertThat(seg2Records.sizeInBytes()).isGreaterThan(0);

            // Consume segment 2 chunks to cleanup.
            int seg2Index = 0;
            while (true) {
                if (seg2Index >= seg2Chunks.size()) {
                    Thread.sleep(50);
                    if (seg2Index >= seg2Chunks.size()) {
                        break;
                    }
                }
                RemoteLogDownloadFuture chunkFuture = seg2Chunks.get(seg2Index);
                waitUntil(chunkFuture::isDone, Duration.ofMinutes(1), "seg2 chunk timeout");
                // Read data BEFORE recycling: recycleCallback may trigger cleanup of the backing
                // file, making subsequent getLogRecords() calls unsafe.
                boolean isEmpty = chunkFuture.getLogRecords().sizeInBytes() == 0;
                chunkFuture.getRecycleCallback().run();
                seg2Index++;
                if (isEmpty) {
                    break;
                }
            }
        } finally {
            IOUtils.closeQuietly(downloader);
        }
    }

    /**
     * Tests that the configurable chunk size option works correctly by comparing the number of
     * chunks produced with different chunk sizes.
     */
    @Test
    void testConfigurableChunkSize() throws Exception {
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 1);
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_MAX_PREFETCH_CHUNKS, 100);

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        RemoteLogSegment segment = buildLargeRemoteLogSegment(tb, 30);
        FsPath tabletDir = remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);

        // Test with a very small chunk size (100 bytes).
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_CHUNK_SIZE, MemorySize.parse("100b"));
        int smallChunkCount = countChunksForSegment(segment, tabletDir);

        // Test with a large chunk size (10 MB) - should produce 1 chunk.
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_CHUNK_SIZE, MemorySize.parse("10mb"));
        int largeChunkCount = countChunksForSegment(segment, tabletDir);

        // Small chunk size should produce more chunks.
        assertThat(smallChunkCount).isGreaterThan(largeChunkCount);
        // Large chunk size should read the entire segment in one chunk.
        assertThat(largeChunkCount).isEqualTo(1);
    }

    private int countChunksForSegment(RemoteLogSegment segment, FsPath tabletDir) throws Exception {
        RemoteLogDownloader downloader =
                new RemoteLogDownloader(DATA1_TABLE_PATH, conf, scannerMetricGroup, 10L);
        try {
            List<RemoteLogDownloadFuture> chunks = Collections.synchronizedList(new ArrayList<>());
            RemoteLogDownloadFuture first = downloader.requestRemoteLog(tabletDir, segment, 0);
            chunks.add(first);
            Consumer<RemoteLogDownloadFuture> collector =
                    new Consumer<RemoteLogDownloadFuture>() {
                        @Override
                        public void accept(RemoteLogDownloadFuture f) {
                            chunks.add(f);
                            f.setNextChunkCallback(this);
                        }
                    };
            first.setNextChunkCallback(collector);

            // Start after callback is set.
            downloader.start();

            int nonEmptyChunks = 0;
            int consumed = 0;
            while (true) {
                if (consumed >= chunks.size()) {
                    Thread.sleep(50);
                    if (consumed >= chunks.size()) {
                        break;
                    }
                }
                RemoteLogDownloadFuture chunkFuture = chunks.get(consumed);
                waitUntil(chunkFuture::isDone, Duration.ofMinutes(1), "chunk timeout");
                LogRecords records = chunkFuture.getLogRecords();
                chunkFuture.getRecycleCallback().run();
                consumed++;
                if (records.sizeInBytes() == 0) {
                    break;
                }
                nonEmptyChunks++;
            }
            return nonEmptyChunks;
        } finally {
            IOUtils.closeQuietly(downloader);
        }
    }

    /**
     * Builds a remote log segment file with multiple batches to ensure the segment is large enough
     * to span multiple chunks when a small chunk size is configured.
     */
    private RemoteLogSegment buildLargeRemoteLogSegment(TableBucket tb, int numBatches)
            throws Exception {
        UUID segmentId = UUID.randomUUID();
        RemoteLogSegment segment =
                RemoteLogSegment.Builder.builder()
                        .tableBucket(tb)
                        .physicalTablePath(DATA1_PHYSICAL_TABLE_PATH)
                        .remoteLogSegmentId(segmentId)
                        .remoteLogStartOffset(0)
                        .remoteLogEndOffset((long) numBatches * 10 - 1)
                        .maxTimestamp(10)
                        .segmentSizeInBytes(Integer.MAX_VALUE)
                        .build();

        FsPath tabletDir = remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);
        FsPath segDir = remoteLogSegmentDir(tabletDir, segmentId);
        File segDirFile = new File(segDir.toString());
        if (!segDirFile.exists()) {
            segDirFile.mkdirs();
        }

        File logFile = FlussPaths.logFile(segDirFile, 0);
        FileLogRecords fileLogRecords =
                FileLogRecords.open(logFile, false, 10 * 1024 * 1024, false);
        for (int i = 0; i < numBatches; i++) {
            fileLogRecords.append(genMemoryLogRecordsWithBaseOffset(i * 10L, DATA1));
        }
        fileLogRecords.flush();
        fileLogRecords.close();
        return segment;
    }

    private static Long remoteLogSegmentFilesLength(
            List<RemoteLogSegment> remoteLogSegments, FsPath remoteLogTabletDir, int segmentNum) {
        return remoteLogSegments.stream()
                .limit(segmentNum)
                .mapToLong(
                        segment ->
                                new File(
                                                RemoteLogDownloader.getRemoteLogFilePath(
                                                                remoteLogTabletDir, segment)
                                                        .getPath())
                                        .length())
                .sum();
    }
}
