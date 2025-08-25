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

import com.alibaba.fluss.client.metrics.ScannerMetricGroup;
import com.alibaba.fluss.client.metrics.TestingScannerMetricGroup;
import com.alibaba.fluss.client.table.scanner.RemoteFileDownloader;
import com.alibaba.fluss.client.table.scanner.log.RemoteLogDownloader.RemoteLogDownloadRequest;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.IOUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.testutils.DataTestUtils.genRemoteLogSegmentFile;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitUntil;
import static com.alibaba.fluss.utils.FlussPaths.remoteLogDir;
import static com.alibaba.fluss.utils.FlussPaths.remoteLogTabletDir;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteLogDownloader}. */
class RemoteLogDownloaderTest {

    private @TempDir File remoteDataDir;
    private @TempDir File localDir;
    private FsPath remoteLogDir;
    private Configuration conf;
    private ScannerMetricGroup scannerMetricGroup;

    @BeforeEach
    void beforeEach() {
        conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir.getAbsolutePath());
        conf.set(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR, localDir.getAbsolutePath());
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 4);
        remoteLogDir = remoteLogDir(conf);
        scannerMetricGroup = TestingScannerMetricGroup.newInstance();
    }

    @Test
    void testPrefetchNum() throws Exception {
        RemoteFileDownloader remoteFileDownloader = new RemoteFileDownloader(1);
        RemoteLogDownloader remoteLogDownloader =
                new RemoteLogDownloader(
                        DATA1_TABLE_PATH, conf, remoteFileDownloader, scannerMetricGroup, 10L);
        try {
            // trigger auto download.
            remoteLogDownloader.start();

            Path localLogDir = remoteLogDownloader.getLocalLogDir();
            TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
            List<RemoteLogSegment> remoteLogSegments =
                    buildRemoteLogSegmentList(tb, DATA1_PHYSICAL_TABLE_PATH, 5, conf, 10);
            FsPath remoteLogTabletDir =
                    remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);
            List<RemoteLogDownloadFuture> futures =
                    requestRemoteLogs(remoteLogDownloader, remoteLogTabletDir, remoteLogSegments);

            // the first 4 segments should success.
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        for (int i = 0; i < 4; i++) {
                            assertThat(futures.get(i).isDone()).isTrue();
                        }
                    });

            assertThat(FileUtils.listDirectory(localLogDir).length).isEqualTo(4);
            assertThat(scannerMetricGroup.remoteFetchRequestCount().getCount()).isEqualTo(4);
            assertThat(scannerMetricGroup.remoteFetchBytes().getCount())
                    .isEqualTo(
                            remoteLogSegmentFilesLength(remoteLogSegments, remoteLogTabletDir, 4));
            assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(0);

            futures.get(0).getRecycleCallback().run();
            // the 5th segment should success.
            retry(Duration.ofMinutes(1), () -> assertThat(futures.get(4).isDone()).isTrue());
            assertThat(FileUtils.listDirectory(localLogDir).length).isEqualTo(4);
            assertThat(scannerMetricGroup.remoteFetchRequestCount().getCount()).isEqualTo(5);
            assertThat(scannerMetricGroup.remoteFetchBytes().getCount())
                    .isEqualTo(
                            remoteLogSegmentFilesLength(remoteLogSegments, remoteLogTabletDir, 5));
            assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(0);

            futures.get(1).getRecycleCallback().run();
            futures.get(2).getRecycleCallback().run();
            assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(2);
            // the removal of log files are async, so we need to wait for the removal.
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(FileUtils.listDirectory(localLogDir).length).isEqualTo(2));

            // test cleanup
            remoteLogDownloader.close();
            assertThat(localLogDir.toFile().exists()).isFalse();
        } finally {
            IOUtils.closeQuietly(remoteLogDownloader);
            IOUtils.closeQuietly(remoteFileDownloader);
        }
    }

    @Test
    void testDownloadLogInParallelAndInPriority() throws Exception {
        class TestRemoteFileDownloader extends RemoteFileDownloader {
            final Set<String> threadNames = Collections.synchronizedSet(new HashSet<>());

            private TestRemoteFileDownloader(int threadNum) {
                super(threadNum);
            }

            @Override
            protected long downloadFile(Path targetFilePath, FsPath remoteFilePath)
                    throws IOException {
                threadNames.add(Thread.currentThread().getName());
                return super.downloadFile(targetFilePath, remoteFilePath);
            }
        }

        // prepare the environment, 4 download threads, pre-fetch 4 segments, 10 segments to fetch.
        TestRemoteFileDownloader fileDownloader = new TestRemoteFileDownloader(4);
        RemoteLogDownloader remoteLogDownloader =
                new RemoteLogDownloader(
                        DATA1_TABLE_PATH,
                        conf, // max 4 pre-fetch num
                        fileDownloader,
                        scannerMetricGroup,
                        10L);
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
                        remoteLogDownloader.requestRemoteLog(remoteLogTabletDir, segment);
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
            // make sure 4 threads are used.
            assertThat(fileDownloader.threadNames.size()).isEqualTo(4);
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
            IOUtils.closeQuietly(fileDownloader);
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
        return new RemoteLogDownloadRequest(remoteLogSegment, remoteLogDir);
    }

    private List<RemoteLogDownloadFuture> requestRemoteLogs(
            RemoteLogDownloader remoteLogDownloader,
            FsPath remoteLogTabletDir,
            List<RemoteLogSegment> remoteLogSegments) {
        List<RemoteLogDownloadFuture> futures = new ArrayList<>();
        for (RemoteLogSegment segment : remoteLogSegments) {
            RemoteLogDownloadFuture future =
                    remoteLogDownloader.requestRemoteLog(remoteLogTabletDir, segment);
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
            UUID segmentId = UUID.randomUUID();
            RemoteLogSegment remoteLogSegment =
                    RemoteLogSegment.Builder.builder()
                            .tableBucket(tableBucket)
                            .physicalTablePath(physicalTablePath)
                            .remoteLogSegmentId(segmentId)
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

    private static Long remoteLogSegmentFilesLength(
            List<RemoteLogSegment> remoteLogSegments, FsPath remoteLogTabletDir, int segmentNum) {
        return remoteLogSegments.stream()
                .limit(segmentNum)
                .mapToLong(
                        segment ->
                                new File(
                                                RemoteLogDownloader.getFsPathAndFileName(
                                                                remoteLogTabletDir, segment)
                                                        .getPath()
                                                        .getPath())
                                        .length())
                .sum();
    }
}
