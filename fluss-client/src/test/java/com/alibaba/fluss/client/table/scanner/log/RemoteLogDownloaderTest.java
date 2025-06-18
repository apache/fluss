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
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.testutils.DataTestUtils.genRemoteLogSegmentFile;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.utils.FlussPaths.remoteLogDir;
import static com.alibaba.fluss.utils.FlussPaths.remoteLogTabletDir;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteLogDownloader}. */
class RemoteLogDownloaderTest {

    private @TempDir File remoteDataDir;
    private @TempDir File localDir;
    private FsPath remoteLogDir;
    private Configuration conf;
    private RemoteFileDownloader remoteFileDownloader;
    private ScannerMetricGroup scannerMetricGroup;
    private RemoteLogDownloader remoteLogDownloader;

    @BeforeEach
    void beforeEach() {
        conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDataDir.getAbsolutePath());
        conf.set(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR, localDir.getAbsolutePath());
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 4);
        remoteLogDir = remoteLogDir(conf);
        remoteFileDownloader = new RemoteFileDownloader(1);
        scannerMetricGroup = TestingScannerMetricGroup.newInstance();
        remoteLogDownloader =
                new RemoteLogDownloader(
                        DATA1_TABLE_PATH,
                        conf,
                        remoteFileDownloader,
                        scannerMetricGroup,
                        // use a short timout for faster testing
                        10L);
    }

    @AfterEach
    void afterEach() {
        if (remoteLogDownloader != null) {
            IOUtils.closeQuietly(remoteLogDownloader);
        }
        if (remoteFileDownloader != null) {
            IOUtils.closeQuietly(remoteFileDownloader);
        }
    }

    @Test
    void testPrefetchNum() throws Exception {
        Path localLogDir = remoteLogDownloader.getLocalLogDir();
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        List<RemoteLogSegment> remoteLogSegments =
                buildRemoteLogSegmentList(tb, DATA1_PHYSICAL_TABLE_PATH, 5, conf);
        FsPath remoteLogTabletDir = remoteLogTabletDir(remoteLogDir, DATA1_PHYSICAL_TABLE_PATH, tb);
        List<RemoteLogDownloadFuture> futures =
                requestRemoteLogs(remoteLogTabletDir, remoteLogSegments);

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
                .isEqualTo(remoteLogSegmentFilesLength(remoteLogSegments, remoteLogTabletDir, 4));
        assertThat(remoteLogDownloader.getPrefetchSemaphore().availablePermits()).isEqualTo(0);

        futures.get(0).getRecycleCallback().run();
        // the 5th segment should success.
        retry(Duration.ofMinutes(1), () -> assertThat(futures.get(4).isDone()).isTrue());
        assertThat(FileUtils.listDirectory(localLogDir).length).isEqualTo(4);
        assertThat(scannerMetricGroup.remoteFetchRequestCount().getCount()).isEqualTo(5);
        assertThat(scannerMetricGroup.remoteFetchBytes().getCount())
                .isEqualTo(remoteLogSegmentFilesLength(remoteLogSegments, remoteLogTabletDir, 5));
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
        assertThat(FileUtils.listDirectory(localLogDir).length).isEqualTo(0);
    }

    private List<RemoteLogDownloadFuture> requestRemoteLogs(
            FsPath remoteLogTabletDir, List<RemoteLogSegment> remoteLogSegments) {
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
            Configuration conf)
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
