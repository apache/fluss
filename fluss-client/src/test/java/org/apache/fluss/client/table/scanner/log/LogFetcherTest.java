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

import org.apache.fluss.client.metadata.ClientSchemaGetter;
import org.apache.fluss.client.metadata.TestingClientSchemaGetter;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.FetchLogReadPreference;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE2;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE3;
import static org.apache.fluss.record.MemoryLogRecords.EMPTY;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** UT Test for {@link LogFetcher}. */
public class LogFetcherTest {
    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);

    private TestingMetadataUpdater metadataUpdater;
    private LogFetcher logFetcher = null;

    // TODO Add more ut tests like kafka.

    @BeforeEach
    public void setup() {
        metadataUpdater = initializeMetadataUpdater();
        logFetcher = createLogFetcher(new Configuration());
    }

    @AfterEach
    public void tearDown() {
        IOUtils.closeQuietly(logFetcher);
    }

    private LogFetcher createLogFetcher(Configuration conf) {
        return createLogFetcher(conf, metadataUpdater, new RemoteFileDownloader(1));
    }

    private LogFetcher createLogFetcher(
            Configuration conf,
            TestingMetadataUpdater updater,
            RemoteFileDownloader remoteFileDownloader) {
        LogFetcher fetcher =
                new LogFetcher(
                        "default-fetcher",
                        initializeLogScannerStatus(),
                        conf,
                        updater,
                        TestingScannerMetricGroup.newInstance(),
                        remoteFileDownloader,
                        LogRecordReadContext.SchemaResolution.TARGET);
        fetcher.registerTable(
                new TableScanSpec(DATA1_TABLE_INFO, null, null),
                new TestingClientSchemaGetter(
                        DATA1_TABLE_PATH, new SchemaInfo(DATA1_SCHEMA, 0), updater, conf));
        return fetcher;
    }

    private ClientSchemaGetter createSchemaGetter(Configuration conf) {
        return new TestingClientSchemaGetter(
                DATA1_TABLE_PATH, new SchemaInfo(DATA1_SCHEMA, 0), metadataUpdater, conf);
    }

    @Test
    void sendFetchRequestWithNotLeaderOrFollowerException() {
        List<TableBucket> fetchable = Collections.singletonList(tb1);
        Map<Integer, FetchLogRequest> requestMap = logFetcher.prepareFetchLogRequests(fetchable);
        Set<Integer> serverSet = requestMap.keySet();
        assertThat(serverSet).containsExactlyInAnyOrder(1);

        assertThat(metadataUpdater.getBucketLocation(tb1))
                .hasValue(
                        new BucketLocation(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb1,
                                1,
                                new int[] {1, 2, 3}));

        // send fetchLogRequest to serverId 1, which will respond with NotLeaderOrFollowerException
        // as responseLogicId=1 do.
        logFetcher.sendFetchRequest(1, requestMap.get(1));

        // When NotLeaderOrFollowerException is received, the bucketLocation will be removed from
        // metadata updater to trigger get the latest bucketLocation in next fetch round.
        assertThat(metadataUpdater.getBucketLocation(tb1)).isNotPresent();
    }

    @Test
    void testStaleLeaderErrorInvalidatesMetadataAfterResubscription() {
        IOUtils.closeQuietly(logFetcher);
        DelayedTabletServerGateway delayedGateway = new DelayedTabletServerGateway();
        metadataUpdater = initializeMetadataUpdater(delayedGateway);
        logFetcher = createLogFetcher(new Configuration());
        LogScannerStatus status = logFetcher.getLogScannerStatus();
        status.assignScanBuckets(Collections.singletonMap(tb1, 10L));

        Map<Integer, FetchLogRequest> requestMap =
                logFetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
        assertThat(getFetchLogData(requestMap.get(1)).get(tb1).getFetchOffset()).isEqualTo(10L);
        logFetcher.sendFetchRequest(1, requestMap.get(1));

        // Change the subscription while the request for offset 10 is still in flight.
        status.assignScanBuckets(Collections.singletonMap(tb1, 100L));
        assertThat(delayedGateway.getRequestCount()).isEqualTo(1);
        assertThat(metadataUpdater.getBucketLocation(tb1)).isPresent();
        assertThat(logFetcher.getCompletedFetchesSize()).isZero();

        // The stale NOT_LEADER_OR_FOLLOWER response must still invalidate the cached route.
        delayedGateway.completeResponse();

        assertThat(metadataUpdater.getBucketLocation(tb1)).isNotPresent();
        assertThat(logFetcher.getCompletedFetchesSize()).isZero();
        assertThat(status.getBucketOffset(tb1)).isEqualTo(100L);
    }

    @Test
    void testUnregisterTableDiscardsBufferedFetches() {
        Map<Integer, FetchLogRequest> requestMap =
                logFetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
        logFetcher.sendFetchRequest(1, requestMap.get(1));
        assertThat(logFetcher.getCompletedFetchesSize()).isEqualTo(1);

        logFetcher.getLogScannerStatus().unassignScanBuckets(Collections.singletonList(tb1));
        logFetcher.unregisterTable(DATA1_TABLE_ID);

        assertThat(logFetcher.getCompletedFetchesSize()).isZero();
        assertThat(logFetcher.getRegisteredTableCount()).isZero();
    }

    @ParameterizedTest
    @ValueSource(longs = {100L, -1L})
    void testBoundedEmptyEarliestResponse(long resolvedOffset) {
        IOUtils.closeQuietly(logFetcher);

        EarliestTabletServerGateway gateway =
                new EarliestTabletServerGateway(resolvedOffset, false);

        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());

        logFetcher.getLogScannerStatus().assignScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 50L);

        Map<Integer, FetchLogRequest> requestMap =
                logFetcher.prepareFetchLogRequests(Collections.singletonList(tb1));

        logFetcher.sendFetchRequest(1, requestMap.get(1));

        assertThat(logFetcher.hasAvailableFetches()).isTrue();

        if (resolvedOffset == FetchLogResultForBucket.NO_RESOLVED_EARLIEST_OFFSET) {
            assertThatThrownBy(logFetcher::collectFetch)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("resolved_earliest_offset");

            assertThat(logFetcher.hasAvailableFetches()).isFalse();
            assertThat(logFetcher.getLogScannerStatus().getBucketOffset(tb1))
                    .isEqualTo(LogScanner.EARLIEST_OFFSET);
            return;
        }

        ScanRecords records = logFetcher.collectFetch();

        assertThat(records.count()).isZero();
        assertThat(records.hasProgress()).isTrue();
        assertThat(records.consumedUpToOffset(tb1)).isEqualTo(50L);
        assertThat(records.finishedBuckets()).containsExactly(tb1);

        assertThat(logFetcher.getLogScannerStatus().getBucketOffset(tb1)).isEqualTo(50L);
    }

    @Test
    void testDiscardResponseAfterStartingOffsetChanges() {
        IOUtils.closeQuietly(logFetcher);
        DelayedTabletServerGateway gateway = new DelayedTabletServerGateway();
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());
        logFetcher.getLogScannerStatus().assignScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 50L);
        logFetcher.sendFetchRequest(
                1, logFetcher.prepareFetchLogRequests(Collections.singletonList(tb1)).get(1));
        logFetcher.getLogScannerStatus().assignScanBucket(tb1, 10L, 50L);

        gateway.completeResponse();

        assertThat(logFetcher.getCompletedFetchesSize()).isZero();
        assertThat(logFetcher.getLogScannerStatus().getBucketOffset(tb1)).isEqualTo(10L);
    }

    @Test
    void testBoundedEarliestBeyondStoppingOffsetSkipsRemoteDownload() throws Exception {
        IOUtils.closeQuietly(logFetcher);
        EarliestTabletServerGateway gateway = new EarliestTabletServerGateway(100L, true);
        metadataUpdater = initializeMetadataUpdater(gateway);

        try (RemoteFileDownloader failingDownloader =
                        new RemoteFileDownloader(1) {
                            @Override
                            protected long downloadFile(Path targetFilePath, FsPath remoteFilePath)
                                    throws IOException {
                                throw new IOException("Remote download must not happen");
                            }
                        };
                LogFetcher fetcher =
                        createLogFetcher(new Configuration(), metadataUpdater, failingDownloader)) {

            fetcher.getLogScannerStatus().assignScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 50L);
            Map<Integer, FetchLogRequest> requestMap =
                    fetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
            fetcher.sendFetchRequest(1, requestMap.get(1));

            // The remote response becomes a metadata-only CompletedFetch.
            assertThat(fetcher.hasAvailableFetches()).isTrue();

            ScanRecords records = fetcher.collectFetch();

            assertThat(records.isEmpty()).isTrue();
            assertThat(records.consumedUpToOffset(tb1)).isEqualTo(50L);
            assertThat(records.finishedBuckets()).containsExactly(tb1);
        }
    }

    @Test
    void testBoundedEarliestBeforeStoppingOffsetDownloadsRemoteRecords() throws Exception {
        IOUtils.closeQuietly(logFetcher);
        EarliestTabletServerGateway gateway = new EarliestTabletServerGateway(20L, true);
        metadataUpdater = initializeMetadataUpdater(gateway);

        try (RemoteFileDownloader failingDownloader =
                        new RemoteFileDownloader(1) {
                            @Override
                            protected long downloadFile(Path targetFilePath, FsPath remoteFilePath)
                                    throws IOException {
                                throw new IOException("Expected remote download");
                            }
                        };
                LogFetcher fetcher =
                        createLogFetcher(new Configuration(), metadataUpdater, failingDownloader)) {

            fetcher.getLogScannerStatus().assignScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 50L);
            Map<Integer, FetchLogRequest> requestMap =
                    fetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
            fetcher.sendFetchRequest(1, requestMap.get(1));

            retry(Duration.ofSeconds(30), () -> assertThat(fetcher.hasAvailableFetches()).isTrue());

            assertThatThrownBy(fetcher::collectFetch)
                    .isInstanceOf(FetchException.class)
                    .rootCause()
                    .hasMessageContaining("Expected remote download");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBoundedRemoteDoesNotScheduleSegmentsAtOrBeyondStoppingOffset(boolean earliest)
            throws Exception {
        IOUtils.closeQuietly(logFetcher);

        long physicalStartingOffset = 20L;
        long stoppingOffset = 25L;

        TwoSegmentRemoteTabletServerGateway gateway =
                new TwoSegmentRemoteTabletServerGateway(physicalStartingOffset);

        metadataUpdater = initializeMetadataUpdater(gateway);

        Configuration conf = new Configuration();

        // Prevent the RemoteLogDownloader worker from consuming the queue.
        // We want to inspect exactly how many segments LogFetcher scheduled.
        conf.setInt(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 0);

        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogFetcher fetcher = createLogFetcher(conf, metadataUpdater, downloader)) {
            long requestedStartingOffset =
                    earliest ? LogScanner.EARLIEST_OFFSET : physicalStartingOffset;
            fetcher.getLogScannerStatus()
                    .assignScanBucket(tb1, requestedStartingOffset, stoppingOffset);
            Map<Integer, FetchLogRequest> requestMap =
                    fetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
            fetcher.sendFetchRequest(1, requestMap.get(1));

            // Remote response contains:
            //   segment 0: [20, 100)
            //   segment 1: [100, 200)
            //
            // Since stop=25, only segment 0 is relevant.
            assertThat(fetcher.getPendingRemoteLogFetchCount()).isEqualTo(1);
        }
    }

    @Test
    void testDiscardStaleResponseAfterTableReregistered() {
        IOUtils.closeQuietly(logFetcher);
        DelayedTabletServerGateway delayedGateway = new DelayedTabletServerGateway();
        metadataUpdater = initializeMetadataUpdater(delayedGateway);
        Configuration conf = new Configuration();
        logFetcher = createLogFetcher(conf);

        Map<Integer, FetchLogRequest> requestMap =
                logFetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
        logFetcher.sendFetchRequest(1, requestMap.get(1));

        logFetcher.getLogScannerStatus().unassignScanBuckets(Collections.singletonList(tb1));
        logFetcher.unregisterTable(DATA1_TABLE_ID);
        logFetcher.registerTable(
                new TableScanSpec(DATA1_TABLE_INFO, null, null), createSchemaGetter(conf));
        logFetcher.getLogScannerStatus().assignScanBuckets(Collections.singletonMap(tb1, 0L));

        delayedGateway.completeResponse();

        assertThat(logFetcher.getCompletedFetchesSize()).isZero();
        assertThat(logFetcher.getRegisteredTableCount()).isEqualTo(1);
    }

    @Test
    void testSendFetchesRechecksFetchableBucketsAfterMetadataUpdate() throws Exception {
        IOUtils.closeQuietly(logFetcher);
        DelayedTabletServerGateway delayedGateway = new DelayedTabletServerGateway();
        BlockingMetadataUpdater blockingMetadataUpdater =
                new BlockingMetadataUpdater(delayedGateway);
        metadataUpdater = blockingMetadataUpdater;
        logFetcher = createLogFetcher(new Configuration());

        logFetcher.sendFetches();
        assertThat(delayedGateway.getRequestCount()).isOne();

        blockingMetadataUpdater.invalidateTableMetadata();
        CompletableFuture<Void> secondSend = CompletableFuture.runAsync(logFetcher::sendFetches);
        try {
            assertThat(blockingMetadataUpdater.awaitMetadataUpdateStarted()).isTrue();
            delayedGateway.completeResponse();
            assertThat(logFetcher.getCompletedFetchesSize()).isOne();
        } finally {
            blockingMetadataUpdater.continueMetadataUpdate();
            delayedGateway.completeResponse();
        }

        secondSend.get(30, TimeUnit.SECONDS);
        assertThat(delayedGateway.getRequestCount()).isOne();
    }

    @Test
    void testPrepareFetchLogRequestWithReadPreference() throws Exception {
        Map<Integer, FetchLogRequest> defaultRequestMap =
                logFetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
        FetchLogRequest defaultRequest = defaultRequestMap.get(1);
        assertThat(defaultRequest.hasReadPreference()).isTrue();
        assertThat(defaultRequest.getReadPreference())
                .isEqualTo(FetchLogReadPreference.LOCAL_FIRST.value());

        Configuration remoteFirstConf = new Configuration();
        remoteFirstConf.setString(
                ConfigOptions.CLIENT_SCANNER_LOG_READ_PREFERENCE.key(),
                FetchLogReadPreference.REMOTE_FIRST.toString());
        LogFetcher remoteFirstFetcher = createLogFetcher(remoteFirstConf);
        try {
            Map<Integer, FetchLogRequest> remoteFirstRequestMap =
                    remoteFirstFetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
            FetchLogRequest remoteFirstRequest = remoteFirstRequestMap.get(1);
            assertThat(remoteFirstRequest.hasReadPreference()).isTrue();
            assertThat(remoteFirstRequest.getReadPreference())
                    .isEqualTo(FetchLogReadPreference.REMOTE_FIRST.value());
        } finally {
            remoteFirstFetcher.close();
        }
    }

    @Test
    void throwExceptionWhenRemoteDownloadFails() throws Exception {
        TestingMetadataUpdater localMetadataUpdater =
                initializeMetadataUpdater(new RemoteFetchTabletServerGateway());

        try (RemoteFileDownloader failingDownloader =
                        new RemoteFileDownloader(1) {
                            @Override
                            protected long downloadFile(Path targetFilePath, FsPath remoteFilePath)
                                    throws IOException {
                                throw new IOException("Simulated remote download failure");
                            }
                        };
                LogFetcher fetcher =
                        createLogFetcher(
                                new Configuration(), localMetadataUpdater, failingDownloader)) {

            Map<Integer, FetchLogRequest> requestMap =
                    fetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
            fetcher.sendFetchRequest(1, requestMap.get(1));
            retry(Duration.ofSeconds(30), () -> assertThat(fetcher.hasAvailableFetches()).isTrue());
            // collectFetch should throw FetchException due to download failure
            assertThatThrownBy(fetcher::collectFetch)
                    .isInstanceOf(FetchException.class)
                    .rootCause()
                    .hasMessageContaining("Simulated remote download failure");
        }
    }

    private LogScannerStatus initializeLogScannerStatus() {
        Map<TableBucket, Long> scanBucketAndOffsets = new HashMap<>();
        scanBucketAndOffsets.put(tb1, 0L);
        LogScannerStatus status = new LogScannerStatus();
        status.assignScanBuckets(scanBucketAndOffsets);
        return status;
    }

    private static class RemoteFetchTabletServerGateway extends TestTabletServerGateway {

        public RemoteFetchTabletServerGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(request);
            Map<TableBucket, FetchLogResultForBucket> resultForBucketMap = new HashMap<>();
            fetchLogData.forEach(
                    (tableBucket, fetchReqInfo) -> {
                        RemoteLogSegment segment =
                                RemoteLogSegment.Builder.builder()
                                        .tableBucket(tableBucket)
                                        .physicalTablePath(PhysicalTablePath.of(DATA1_TABLE_PATH))
                                        .remoteLogSegmentId(UUID.randomUUID())
                                        .remoteLogStartOffset(fetchReqInfo.getFetchOffset())
                                        .remoteLogEndOffset(fetchReqInfo.getFetchOffset() + 100)
                                        .maxTimestamp(1000L)
                                        .segmentSizeInBytes(1024)
                                        .build();
                        RemoteLogFetchInfo remoteLogFetchInfo =
                                new RemoteLogFetchInfo(
                                        "/tmp/test-tablet-dir",
                                        null,
                                        Collections.singletonList(segment),
                                        0);
                        resultForBucketMap.put(
                                tableBucket,
                                FetchLogResultForBucket.remote(
                                        tableBucket, remoteLogFetchInfo, 100L));
                    });
            return CompletableFuture.completedFuture(makeFetchLogResponse(resultForBucketMap));
        }
    }

    private static class TestingTabletServerGateway extends TestTabletServerGateway {

        public TestingTabletServerGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(request);
            Map<TableBucket, FetchLogResultForBucket> resultForBucketMap = new HashMap<>();
            // return with NotLeaderOrFollowerException.
            fetchLogData.forEach(
                    (tableBucket, fetchData) -> {
                        FetchLogResultForBucket fetchLogResultForBucket =
                                FetchLogResultForBucket.error(
                                        tableBucket,
                                        ApiError.fromThrowable(
                                                new NotLeaderOrFollowerException(
                                                        "mock fetchLog fail for not leader or follower exception.")));
                        resultForBucketMap.put(tableBucket, fetchLogResultForBucket);
                    });
            return CompletableFuture.completedFuture(makeFetchLogResponse(resultForBucketMap));
        }
    }

    private static class DelayedTabletServerGateway extends TestingTabletServerGateway {
        private final CompletableFuture<FetchLogResponse> responseFuture =
                new CompletableFuture<>();
        private final AtomicInteger requestCount = new AtomicInteger();
        private FetchLogResponse response;

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            requestCount.incrementAndGet();
            response = super.fetchLog(request).join();
            return responseFuture;
        }

        private int getRequestCount() {
            return requestCount.get();
        }

        private void completeResponse() {
            responseFuture.complete(response);
        }
    }

    private static class EarliestTabletServerGateway extends TestTabletServerGateway {
        private final long resolvedEarliestOffset;
        private final boolean remote;

        private EarliestTabletServerGateway(long resolvedEarliestOffset, boolean remote) {
            super(false, Collections.emptySet());
            this.resolvedEarliestOffset = resolvedEarliestOffset;
            this.remote = remote;
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(request);
            Map<TableBucket, FetchLogResultForBucket> results = new HashMap<>();

            fetchLogData.forEach(
                    (tableBucket, fetchReqInfo) -> {
                        FetchLogResultForBucket result;
                        if (remote) {
                            RemoteLogSegment segment =
                                    RemoteLogSegment.Builder.builder()
                                            .tableBucket(tableBucket)
                                            .physicalTablePath(
                                                    PhysicalTablePath.of(DATA1_TABLE_PATH))
                                            .remoteLogSegmentId(UUID.randomUUID())
                                            .remoteLogStartOffset(resolvedEarliestOffset)
                                            .remoteLogEndOffset(resolvedEarliestOffset + 100L)
                                            .maxTimestamp(1000L)
                                            .segmentSizeInBytes(1024)
                                            .build();

                            RemoteLogFetchInfo remoteLogFetchInfo =
                                    new RemoteLogFetchInfo(
                                            "/tmp/test-tablet-dir",
                                            null,
                                            Collections.singletonList(segment),
                                            0);

                            result =
                                    FetchLogResultForBucket.remote(
                                            tableBucket,
                                            remoteLogFetchInfo,
                                            100L,
                                            resolvedEarliestOffset);

                        } else if (resolvedEarliestOffset
                                == FetchLogResultForBucket.NO_RESOLVED_EARLIEST_OFFSET) {

                            // Simulates an old server: successful empty response without
                            // resolved_earliest_offset.
                            result =
                                    FetchLogResultForBucket.records(
                                            tableBucket, EMPTY, 100L, -1L, -1L);

                        } else {
                            result =
                                    FetchLogResultForBucket.records(
                                            tableBucket,
                                            EMPTY,
                                            100L,
                                            -1L,
                                            -1L,
                                            resolvedEarliestOffset);
                        }

                        results.put(tableBucket, result);
                    });

            return CompletableFuture.completedFuture(makeFetchLogResponse(results));
        }
    }

    private static class TwoSegmentRemoteTabletServerGateway extends TestTabletServerGateway {
        private final long resolvedEarliestOffset;

        private TwoSegmentRemoteTabletServerGateway(long resolvedEarliestOffset) {
            super(false, Collections.emptySet());
            this.resolvedEarliestOffset = resolvedEarliestOffset;
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(request);
            Map<TableBucket, FetchLogResultForBucket> results = new HashMap<>();
            fetchLogData.forEach(
                    (tableBucket, fetchReqInfo) -> {
                        RemoteLogSegment firstSegment =
                                RemoteLogSegment.Builder.builder()
                                        .tableBucket(tableBucket)
                                        .physicalTablePath(PhysicalTablePath.of(DATA1_TABLE_PATH))
                                        .remoteLogSegmentId(UUID.randomUUID())
                                        .remoteLogStartOffset(resolvedEarliestOffset)
                                        .remoteLogEndOffset(100L)
                                        .maxTimestamp(1000L)
                                        .segmentSizeInBytes(1024)
                                        .build();

                        RemoteLogSegment secondSegment =
                                RemoteLogSegment.Builder.builder()
                                        .tableBucket(tableBucket)
                                        .physicalTablePath(PhysicalTablePath.of(DATA1_TABLE_PATH))
                                        .remoteLogSegmentId(UUID.randomUUID())
                                        .remoteLogStartOffset(100L)
                                        .remoteLogEndOffset(200L)
                                        .maxTimestamp(2000L)
                                        .segmentSizeInBytes(1024)
                                        .build();

                        RemoteLogFetchInfo remoteLogFetchInfo =
                                new RemoteLogFetchInfo(
                                        "/tmp/test-tablet-dir",
                                        null,
                                        Arrays.asList(firstSegment, secondSegment),
                                        0);

                        FetchLogResultForBucket result;

                        if (fetchReqInfo.getFetchOffset() == LogScanner.EARLIEST_OFFSET) {

                            result =
                                    FetchLogResultForBucket.remote(
                                            tableBucket,
                                            remoteLogFetchInfo,
                                            200L,
                                            resolvedEarliestOffset);
                        } else {
                            // Explicit offset request must not carry
                            // resolved_earliest_offset.
                            result =
                                    FetchLogResultForBucket.remote(
                                            tableBucket, remoteLogFetchInfo, 200L);
                        }

                        results.put(tableBucket, result);
                    });

            return CompletableFuture.completedFuture(makeFetchLogResponse(results));
        }
    }

    private static class BlockingMetadataUpdater extends TestingMetadataUpdater {
        private final CountDownLatch metadataUpdateStarted = new CountDownLatch(1);
        private final CountDownLatch continueMetadataUpdate = new CountDownLatch(1);
        private final Cluster refreshedCluster;

        private BlockingMetadataUpdater(TestTabletServerGateway gateway) {
            super(
                    COORDINATOR,
                    Arrays.asList(NODE1, NODE2, NODE3),
                    Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO),
                    Collections.singletonMap(1, gateway),
                    new Configuration());
            refreshedCluster = getCluster();
        }

        @Override
        public void updateTableOrPartitionMetadata(TablePath tablePath, Long partitionId) {
            metadataUpdateStarted.countDown();
            try {
                if (!continueMetadataUpdate.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("Timed out waiting to continue metadata update");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while blocking metadata update", e);
            }
            updateCluster(refreshedCluster);
        }

        private void invalidateTableMetadata() {
            updateCluster(
                    getCluster()
                            .invalidPhysicalTableBucketMeta(
                                    Collections.singleton(PhysicalTablePath.of(DATA1_TABLE_PATH))));
        }

        private boolean awaitMetadataUpdateStarted() throws InterruptedException {
            return metadataUpdateStarted.await(30, TimeUnit.SECONDS);
        }

        private void continueMetadataUpdate() {
            continueMetadataUpdate.countDown();
        }
    }

    private TestingMetadataUpdater initializeMetadataUpdater() {
        return initializeMetadataUpdater(new TestingTabletServerGateway());
    }

    private TestingMetadataUpdater initializeMetadataUpdater(TestTabletServerGateway gateway) {
        return new TestingMetadataUpdater(
                TestingMetadataUpdater.COORDINATOR,
                Arrays.asList(NODE1, NODE2, NODE3),
                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO),
                Collections.singletonMap(1, gateway),
                new Configuration());
    }
}
