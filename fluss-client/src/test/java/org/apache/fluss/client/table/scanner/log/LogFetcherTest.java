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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.RequiredApiVersionNotSupportedException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForTable;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.FetchLogReadPreference;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
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
import java.util.stream.Collectors;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE2;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE3;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.rpc.protocol.Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION;
import static org.apache.fluss.rpc.protocol.Errors.NONE;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.concurrent.FutureUtils.completedExceptionally;
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
                initializeMetadataUpdater(new RemoteFetchTabletServerGateway(false, 0L));

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

    @Test
    void testBoundedExplicitOffsetKeepsUsingV0AfterEmptyFetch() {
        IOUtils.closeQuietly(logFetcher);

        // An empty response without filtered progress leaves the scan at offset 100.
        // This verifies protocol fallback persistence independently of completion semantics.
        LegacyFetchGateway gateway = new LegacyFetchGateway(-1L);
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());

        LogScannerStatus status = logFetcher.getLogScannerStatus();
        status.assignBoundedScanBucket(tb1, 100L, 105L);

        // Initial bounded request requires FETCH_LOG v1, which this gateway rejects.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(1);
        assertThat(gateway.onlyBucketRequest(0).hasStoppingOffset()).isTrue();
        assertThat(gateway.onlyBucketRequest(0).getFetchOffset()).isEqualTo(100L);

        // Fall back to v0. The simulated response contains no records or progress.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(2);
        assertThat(gateway.onlyBucketRequest(1).hasStoppingOffset()).isFalse();
        assertThat(gateway.onlyBucketRequest(1).getFetchOffset()).isEqualTo(100L);
        assertThat(logFetcher.collectFetch().hasProgress()).isFalse();
        assertThat(status.getBucketOffset(tb1)).isEqualTo(100L);

        // The next request must still be v0, without a second v1 probe.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(3);
        assertThat(gateway.onlyBucketRequest(2).hasStoppingOffset()).isFalse();
        assertThat(gateway.onlyBucketRequest(2).getFetchOffset()).isEqualTo(100L);
        assertThat(gateway.listOffsetRequests).isZero();
    }

    @Test
    void testMixedBoundedAndUnboundedBucketsFallBackToV0() throws Exception {
        IOUtils.closeQuietly(logFetcher);
        TableBucket bounded = tb1;
        TableBucket unbounded = new TableBucket(DATA1_TABLE_ID, 1);
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        LegacyFetchGateway gateway = new LegacyFetchGateway(-1L, records);
        metadataUpdater = initializeMetadataUpdater(gateway);
        // Both buckets must share the same leader to be included in one RPC.
        Cluster original = metadataUpdater.getCluster();
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(DATA1_TABLE_PATH);
        int[] replicas = {NODE1.id(), NODE2.id(), NODE3.id()};
        List<BucketLocation> locations =
                Arrays.asList(
                        new BucketLocation(
                                physicalTablePath, DATA1_TABLE_ID, 0, NODE1.id(), replicas),
                        new BucketLocation(
                                physicalTablePath, DATA1_TABLE_ID, 1, NODE1.id(), replicas),
                        new BucketLocation(
                                physicalTablePath, DATA1_TABLE_ID, 2, NODE3.id(), replicas));
        metadataUpdater.updateCluster(
                new Cluster(
                        original.getAliveTabletServers(),
                        original.getCoordinatorServer(),
                        Collections.singletonMap(physicalTablePath, locations),
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_ID),
                        Collections.emptyMap(),
                        Collections.emptyMap()));
        logFetcher = createLogFetcher(new Configuration());
        LogScannerStatus status = logFetcher.getLogScannerStatus();
        status.assignBoundedScanBucket(bounded, 2L, 6L);
        status.assignScanBuckets(Collections.singletonMap(unbounded, 0L));
        // First request contains both bounded and unbounded buckets.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(1);
        List<PbFetchLogReqForBucket> firstBuckets =
                gateway.fetchRequests.get(0).getTablesReqAt(0).getBucketsReqsList();
        assertThat(firstBuckets).hasSize(2);
        assertThat(firstBuckets)
                .extracting(PbFetchLogReqForBucket::getBucketId)
                .containsExactlyInAnyOrder(bounded.getBucket(), unbounded.getBucket());
        assertThat(firstBuckets).filteredOn(PbFetchLogReqForBucket::hasStoppingOffset).hasSize(1);
        // The bounded bucket requires v1, so the entire RPC is rejected.
        // The second request must fall back to the v0 protocol shape.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(2);
        List<PbFetchLogReqForBucket> secondBuckets =
                gateway.fetchRequests.get(1).getTablesReqAt(0).getBucketsReqsList();
        assertThat(secondBuckets).hasSize(2);
        assertThat(secondBuckets)
                .allSatisfy(bucket -> assertThat(bucket.hasStoppingOffset()).isFalse());
        Map<Integer, Long> fetchOffsets =
                secondBuckets.stream()
                        .collect(
                                Collectors.toMap(
                                        PbFetchLogReqForBucket::getBucketId,
                                        PbFetchLogReqForBucket::getFetchOffset));
        assertThat(fetchOffsets)
                .containsEntry(bounded.getBucket(), 2L)
                .containsEntry(unbounded.getBucket(), 0L);
        // Explicit bounded offsets do not require ListOffsets.
        // Unbounded requests must also never enter this fallback resolution.
        assertThat(gateway.listOffsetRequests).isZero();
        ScanRecords result = logFetcher.collectFetch();
        // The bounded bucket must be truncated inside the returned batch.
        assertThat(result.records(bounded))
                .extracting(ScanRecord::logOffset)
                .containsExactly(2L, 3L, 4L, 5L);
        assertThat(result.consumedUpToOffset(bounded)).isEqualTo(6L);
        assertThat(result.finishedBuckets()).containsExactly(bounded);
        assertThat(status.getBucketOffset(bounded)).isEqualTo(6L);
        // The unbounded bucket must continue consuming normally.
        assertThat(result.records(unbounded)).hasSize(DATA1.size());
        assertThat(result.records(unbounded).get(0).logOffset()).isZero();
        assertThat(status.getBucketOffset(unbounded)).isEqualTo(DATA1.size());
        assertThat(result.finishedBuckets()).doesNotContain(unbounded);
        assertThat(status.fetchableBuckets(bucket -> true))
                .contains(unbounded)
                .doesNotContain(bounded);
        // No duplicate completion event.
        assertThat(logFetcher.collectFetch().finishedBuckets()).isEmpty();
    }

    @Test
    void testBoundedEarliestFallsBackToListOffsets() {
        IOUtils.closeQuietly(logFetcher);

        LegacyFetchGateway gateway = new LegacyFetchGateway(105L);
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());

        logFetcher
                .getLogScannerStatus()
                .assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 105L);

        // First try FETCH_LOG v1 and receive a version error.
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(1);
        assertThat(gateway.onlyBucketRequest(0).hasStoppingOffset()).isTrue();
        assertThat(gateway.onlyBucketRequest(0).getFetchOffset())
                .isEqualTo(LogScanner.EARLIEST_OFFSET);

        // A legacy EARLIEST request must resolve the starting offset before sending FETCH_LOG v0.
        logFetcher.sendFetches();

        assertThat(gateway.listOffsetRequests).isEqualTo(1);
        assertThat(gateway.fetchRequests).hasSize(1);

        // The old server resolves EARLIEST to offset 100.
        gateway.completeListOffsets(0, 100L);

        // Consume the synthetic start-resolution result.
        ScanRecords resolution = logFetcher.collectFetch();

        assertThat(resolution.consumedUpToOffset(tb1)).isEqualTo(100L);
        assertThat(logFetcher.getLogScannerStatus().getBucketOffset(tb1)).isEqualTo(100L);
        assertThat(resolution.finishedBuckets()).isEmpty();

        // Subsequent FETCH_LOG v0 must use an explicit offset.
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(2);
        assertThat(gateway.onlyBucketRequest(1).hasStoppingOffset()).isFalse();
        assertThat(gateway.onlyBucketRequest(1).getFetchOffset()).isEqualTo(100L);

        // The start offset must not be resolved a second time.
        assertThat(gateway.listOffsetRequests).isEqualTo(1);

        ScanRecords result = logFetcher.collectFetch();

        assertThat(result.consumedUpToOffset(tb1)).isEqualTo(105L);
        assertThat(result.finishedBuckets()).containsExactly(tb1);
    }

    @Test
    void testBoundedLegacyEarliestRetentionRaceDoesNotSkipData() {
        IOUtils.closeQuietly(logFetcher);
        LegacyFetchGateway gateway = new LegacyFetchGateway(-1L);
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());
        LogScannerStatus status = logFetcher.getLogScannerStatus();
        status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 150L);

        // The server only supports FETCH_LOG v0.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(1);
        assertThat(gateway.onlyBucketRequest(0).hasStoppingOffset()).isTrue();

        // Resolve EARLIEST through ListOffsets.
        logFetcher.sendFetches();
        assertThat(gateway.listOffsetRequests).isEqualTo(1);
        gateway.completeListOffsets(0, 100L);

        ScanRecords resolution = logFetcher.collectFetch();
        assertThat(resolution.consumedUpToOffset(tb1)).isEqualTo(100L);
        assertThat(resolution.finishedBuckets()).isEmpty();
        assertThat(status.getBucketOffset(tb1)).isEqualTo(100L);

        // Retention advances the earliest available offset from 100 to 120
        // before FETCH_LOG v0 executes.
        gateway.failLegacyFetchWithOutOfRange();
        logFetcher.sendFetches();
        // The legacy request must use the previously resolved explicit offset.
        assertThat(gateway.fetchRequests).hasSize(2);
        assertThat(gateway.onlyBucketRequest(1).hasStoppingOffset()).isFalse();
        assertThat(gateway.onlyBucketRequest(1).getFetchOffset()).isEqualTo(100L);
        // The out-of-range error must be propagated rather than silently
        // resetting the subscription to the new earliest offset.
        assertThatThrownBy(logFetcher::collectFetch)
                .isInstanceOf(FetchException.class)
                .hasMessageContaining("out of range");
        assertThat(status.getBucketOffset(tb1)).isEqualTo(100L);
        assertThat(status.drainFinishedBuckets()).isEmpty();
        // The failed fetch must not be reported as a completed bounded scan.
        assertThat(status.allSubscribedBucketsFinished()).isFalse();
    }

    @Test
    void testBoundedLegacyListOffsetsRetry() throws Exception {
        IOUtils.closeQuietly(logFetcher);
        LegacyFetchGateway gateway = new LegacyFetchGateway(105L);
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());
        logFetcher
                .getLogScannerStatus()
                .assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 105L);
        // FETCH_LOG v1 is rejected.
        logFetcher.sendFetches();
        // Consume the retry signal caused by the version rejection.
        // It must not be mistaken for the later ListOffsets failure signal.
        assertThat(logFetcher.awaitNotEmpty(System.nanoTime())).isTrue();
        // Start the first ListOffsets request.
        logFetcher.sendFetches();
        assertThat(gateway.listOffsetRequests).isEqualTo(1);
        // No retry signal or completed fetch should be available yet.
        assertThat(logFetcher.awaitNotEmpty(System.nanoTime())).isFalse();
        CompletableFuture<Boolean> waitResult = new CompletableFuture<>();
        Thread waitingThread =
                new Thread(
                        () -> {
                            try {
                                waitResult.complete(
                                        logFetcher.awaitNotEmpty(
                                                System.nanoTime() + TimeUnit.SECONDS.toNanos(30)));
                            } catch (Throwable t) {
                                waitResult.completeExceptionally(t);
                            }
                        },
                        "list-offsets-retry-waiter");

        waitingThread.start();
        try {
            // Ensure the scanner has actually entered the timed wait before completing the
            // ListOffsets request exceptionally.
            retry(
                    Duration.ofSeconds(5),
                    () -> {
                        assertThat(waitingThread.getState()).isEqualTo(Thread.State.TIMED_WAITING);
                        assertThat(waitResult.isDone()).isFalse();
                    });
            gateway.failListOffsets(0, new IOException("temporary ListOffsets failure"));
            // The waiter started while ListOffsets was still resolving.
            // Its wait deadline is 30 seconds, so the 100ms retry deadline cannot explain this
            // immediate wakeup.
            assertThat(waitResult.get(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            waitingThread.interrupt();
            waitingThread.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(waitingThread.isAlive()).isFalse();
        }
        // Retry is subject to the legacy backoff.
        retry(
                Duration.ofSeconds(5),
                () -> {
                    logFetcher.sendFetches();
                    assertThat(gateway.listOffsetRequests).isEqualTo(2);
                });
        // A successful retry resolves the starting offset.
        gateway.completeListOffsets(1, 100L);
        ScanRecords resolution = logFetcher.collectFetch();
        assertThat(resolution.consumedUpToOffset(tb1)).isEqualTo(100L);
        assertThat(gateway.listOffsetRequests).isEqualTo(2);
        // The scanner can now resume ordinary legacy fetching.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(2);
        assertThat(gateway.onlyBucketRequest(1).getFetchOffset()).isEqualTo(100L);
        assertThat(gateway.onlyBucketRequest(1).hasStoppingOffset()).isFalse();
        ScanRecords result = logFetcher.collectFetch();
        assertThat(result.finishedBuckets()).containsExactly(tb1);
    }

    @Test
    void testBoundedLegacyEarliestListOffsetsTerminalFailure() {
        IOUtils.closeQuietly(logFetcher);
        LegacyFetchGateway gateway = new LegacyFetchGateway(-1L);
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());
        LogScannerStatus status = logFetcher.getLogScannerStatus();
        status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 105L);
        // FETCH_LOG v1 is rejected.
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(1);
        assertThat(gateway.onlyBucketRequest(0).hasStoppingOffset()).isTrue();

        // Start legacy EARLIEST resolution.
        logFetcher.sendFetches();
        assertThat(gateway.listOffsetRequests).isEqualTo(1);

        // The legacy server rejects ListOffsets with a non-retriable error.
        gateway.completeListOffsetsWithError(0, Errors.AUTHORIZATION_EXCEPTION);
        // The error must be delivered to the caller rather than
        // converted into an endless retry loop.
        assertThatThrownBy(logFetcher::collectFetch).isInstanceOf(AuthorizationException.class);

        // A failed start resolution must not change the scan position.
        assertThat(status.getBucketOffset(tb1)).isEqualTo(LogScanner.EARLIEST_OFFSET);
        assertThat(status.drainFinishedBuckets()).isEmpty();

        // A terminal failure must not keep issuing ListOffsets requests.
        logFetcher.sendFetches();
        assertThat(gateway.listOffsetRequests).isEqualTo(1);
        assertThat(gateway.fetchRequests).hasSize(1);
    }

    @Test
    void testBoundedLegacyEarliestBeyondStopCompletesWithoutFetch() {
        IOUtils.closeQuietly(logFetcher);
        LegacyFetchGateway gateway = new LegacyFetchGateway(-1L);
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());
        logFetcher
                .getLogScannerStatus()
                .assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 100L);
        // First FETCH_LOG v1 attempt is rejected.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(1);
        assertThat(gateway.onlyBucketRequest(0).hasStoppingOffset()).isTrue();
        // Resolve EARLIEST using ListOffsets on the legacy server.
        logFetcher.sendFetches();
        assertThat(gateway.listOffsetRequests).isEqualTo(1);
        // The earliest available offset is already beyond stop.
        gateway.completeListOffsets(0, 150L);
        ScanRecords result = logFetcher.collectFetch();
        assertThat(result.count()).isZero();
        assertThat(result.consumedUpToOffset(tb1)).isEqualTo(100L);
        assertThat(result.finishedBuckets()).containsExactly(tb1);

        // No legacy FETCH_LOG request is needed for the empty range.
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(1);
        assertThat(gateway.listOffsetRequests).isEqualTo(1);
        assertThat(logFetcher.collectFetch().finishedBuckets()).isEmpty();
    }

    @Test
    void testObsoleteLegacyListOffsetsResultIsIgnored() {
        IOUtils.closeQuietly(logFetcher);
        LegacyFetchGateway gateway = new LegacyFetchGateway(205L);
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());
        LogScannerStatus status = logFetcher.getLogScannerStatus();
        // Subscription A: EARLIEST -> 105.
        status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 105L);
        // FETCH_LOG v1 rejected, then ListOffsets A starts.
        logFetcher.sendFetches();
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(1);
        assertThat(gateway.listOffsetRequests).isEqualTo(1);

        // Replace A with subscription B before ListOffsets A completes.
        status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 205L);
        logFetcher.onSubscriptionChanged(tb1, true);
        // B independently enters legacy fallback and starts ListOffsets B.
        logFetcher.sendFetches();
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(2);
        assertThat(gateway.listOffsetRequests).isEqualTo(2);

        // The obsolete ListOffsets A completes after B is subscribed.
        gateway.completeListOffsets(0, 100L);
        ScanRecords obsolete = logFetcher.collectFetch();
        // A's response must not resolve B's EARLIEST offset.
        assertThat(obsolete.hasProgress()).isFalse();
        assertThat(obsolete.finishedBuckets()).isEmpty();
        assertThat(status.getBucketOffset(tb1)).isEqualTo(LogScanner.EARLIEST_OFFSET);
        // Now ListOffsets B completes with its own starting offset.
        gateway.completeListOffsets(1, 200L);
        ScanRecords resolution = logFetcher.collectFetch();
        assertThat(resolution.consumedUpToOffset(tb1)).isEqualTo(200L);
        assertThat(resolution.finishedBuckets()).isEmpty();
        assertThat(status.getBucketOffset(tb1)).isEqualTo(200L);
        // B can continue with a normal FETCH_LOG v0 request.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(3);
        assertThat(gateway.onlyBucketRequest(2).hasStoppingOffset()).isFalse();
        assertThat(gateway.onlyBucketRequest(2).getFetchOffset()).isEqualTo(200L);

        ScanRecords completed = logFetcher.collectFetch();

        assertThat(completed.consumedUpToOffset(tb1)).isEqualTo(205L);
        assertThat(completed.finishedBuckets()).containsExactly(tb1);
        assertThat(status.getBucketOffset(tb1)).isEqualTo(205L);
        assertThat(logFetcher.collectFetch().finishedBuckets()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRemoteSegmentRequestsRespectStoppingOffset(boolean bounded) throws Exception {
        TestingMetadataUpdater updater =
                initializeMetadataUpdater(new RemoteFetchTabletServerGateway(true, 0L));
        Configuration conf = new Configuration();
        // Prevent the download thread from consuming queued requests. This lets us inspect all
        // requests submitted by the fetch callback.
        conf.setInt(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 0);
        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogFetcher fetcher = createLogFetcher(conf, updater, downloader)) {
            LogScannerStatus status = fetcher.getLogScannerStatus();
            if (bounded) {
                status.assignBoundedScanBucket(tb1, 20L, 100L);
            } else {
                status.assignScanBuckets(Collections.singletonMap(tb1, 20L));
            }
            Map<Integer, FetchLogRequest> requests =
                    fetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
            assertThat(requests).containsKey(1);
            FetchLogRequest request = requests.get(1);
            assertThat(request.getTablesReqAt(0).getBucketsReqAt(0).hasStoppingOffset())
                    .isEqualTo(bounded);
            // The gateway returns three remote segments:
            // [0, 100), [100, 200), [200, 300).
            fetcher.sendFetchRequest(1, request);
            assertThat(fetcher.getQueuedRemoteSegmentCount()).isEqualTo(bounded ? 1 : 3);
        }
    }

    @Test
    void testBoundedEarliestResolvesStartBeforeRemoteFetch() throws Exception {
        TestingMetadataUpdater updater =
                initializeMetadataUpdater(new RemoteFetchTabletServerGateway(true, 100L));
        Configuration conf = new Configuration();
        // Prevent background downloads from consuming queued requests.
        conf.setInt(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, 0);
        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogFetcher fetcher = createLogFetcher(conf, updater, downloader)) {
            LogScannerStatus status = fetcher.getLogScannerStatus();
            status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 150L);
            Map<Integer, FetchLogRequest> requests =
                    fetcher.prepareFetchLogRequests(Collections.singletonList(tb1));
            assertThat(requests).containsKey(1);
            FetchLogRequest request = requests.get(1);
            PbFetchLogReqForBucket bucketRequest = request.getTablesReqAt(0).getBucketsReqAt(0);
            assertThat(bucketRequest.hasStoppingOffset()).isTrue();
            assertThat(bucketRequest.getStoppingOffset()).isEqualTo(150L);
            assertThat(bucketRequest.getFetchOffset()).isEqualTo(LogScanner.EARLIEST_OFFSET);
            // Simulate a v1 remote response:
            // log_start_offset = 100
            // segments = [100,200), [200,300), [300,400)
            fetcher.sendFetchRequest(1, request);
            // Only the first segment overlaps the bounded range [100,150).
            assertThat(fetcher.getQueuedRemoteSegmentCount()).isEqualTo(1);
            // Consume the synthetic EARLIEST resolution result.
            // Remote files remain pending because downloading is disabled.
            ScanRecords resolution = fetcher.collectFetch();
            assertThat(resolution.count()).isZero();
            assertThat(resolution.consumedUpToOffset(tb1)).isEqualTo(100L);
            assertThat(resolution.finishedBuckets()).isEmpty();
            // The scanner must use the resolved offset, not EARLIEST_OFFSET (-2).
            assertThat(status.getBucketOffset(tb1)).isEqualTo(100L);
            assertThat(status.allSubscribedBucketsFinished()).isFalse();
            // The pending remote request must remain associated with this bucket. Resolving
            // EARLIEST must not discard the remote download.
            assertThat(fetcher.getQueuedRemoteSegmentCount()).isEqualTo(1);
            // The fetcher must not submit an extra request while the remote segment is pending.
            fetcher.sendFetches();
            assertThat(fetcher.getQueuedRemoteSegmentCount()).isEqualTo(1);
        }
    }

    @Test
    void testBoundedEarliestUsesV1LogStartOffset() {
        IOUtils.closeQuietly(logFetcher);
        ControlledFetchLogGateway gateway = new ControlledFetchLogGateway();
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());
        LogScannerStatus status = logFetcher.getLogScannerStatus();
        status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 150L);
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(1);

        PbFetchLogReqForBucket request =
                gateway.fetchRequests.get(0).getTablesReqAt(0).getBucketsReqAt(0);

        assertThat(request.getFetchOffset()).isEqualTo(LogScanner.EARLIEST_OFFSET);
        assertThat(request.hasStoppingOffset()).isTrue();
        assertThat(request.getStoppingOffset()).isEqualTo(150L);

        // A successful FETCH_LOG v1 response resolves EARLIEST to 100.
        FetchLogResponse response =
                makeFetchLogResponse(
                        Collections.singletonMap(
                                tb1,
                                FetchLogResultForBucket.empty(tb1, 180L, -1L)
                                        .withLogStartOffset(100L)));
        // Successful responses do not need to set the optional error_code field.
        assertThat(response.getTablesRespsList().get(0).getBucketsRespsList().get(0).hasErrorCode())
                .isFalse();
        gateway.completeFetch(0, response);
        ScanRecords resolution = logFetcher.collectFetch();

        assertThat(resolution.count()).isZero();
        assertThat(resolution.consumedUpToOffset(tb1)).isEqualTo(100L);
        assertThat(resolution.finishedBuckets()).isEmpty();
        assertThat(status.getBucketOffset(tb1)).isEqualTo(100L);

        // After resolving EARLIEST, the next request uses an explicit offset.
        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(2);
        PbFetchLogReqForBucket nextRequest =
                gateway.fetchRequests.get(1).getTablesReqAt(0).getBucketsReqAt(0);

        assertThat(nextRequest.getFetchOffset()).isEqualTo(100L);
        assertThat(nextRequest.hasStoppingOffset()).isTrue();
        assertThat(nextRequest.getStoppingOffset()).isEqualTo(150L);
        assertThat(gateway.listOffsetRequests).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBoundedEarliestRejectsInvalidV1LogStartOffset(boolean negativeStartOffset) {
        IOUtils.closeQuietly(logFetcher);
        ControlledFetchLogGateway gateway = new ControlledFetchLogGateway();
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());

        LogScannerStatus status = logFetcher.getLogScannerStatus();
        status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 150L);

        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(1);

        PbFetchLogRespForBucket bucketResponse =
                new PbFetchLogRespForBucket().setBucketId(tb1.getBucket()).setHighWatermark(200L);

        if (negativeStartOffset) {
            bucketResponse.setLogStartOffset(-1L);
        }

        // The response is successful but has no valid log_start_offset.
        // Do not set error_code: it is optional on successful responses.
        assertThat(bucketResponse.hasErrorCode()).isFalse();

        PbFetchLogRespForTable tableResponse =
                new PbFetchLogRespForTable().setTableId(tb1.getTableId());
        tableResponse.addAllBucketsResps(Collections.singletonList(bucketResponse));

        FetchLogResponse response = new FetchLogResponse();
        response.addAllTablesResps(Collections.singletonList(tableResponse));

        gateway.completeFetch(0, response);

        assertThatThrownBy(logFetcher::collectFetch)
                .isInstanceOf(FetchException.class)
                .hasRootCauseMessage("Missing or invalid bounded log start offset for " + tb1);

        // Invalid metadata must not silently resolve EARLIEST or finish the scan.
        assertThat(status.getBucketOffset(tb1)).isEqualTo(LogScanner.EARLIEST_OFFSET);
        assertThat(status.drainFinishedBuckets()).isEmpty();
        assertThat(status.allSubscribedBucketsFinished()).isFalse();
    }

    @Test
    void testObsoleteBoundedV1FetchResponseIsIgnored() {
        IOUtils.closeQuietly(logFetcher);

        ControlledFetchLogGateway gateway = new ControlledFetchLogGateway();
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());

        LogScannerStatus status = logFetcher.getLogScannerStatus();

        // Subscription A.
        status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 150L);

        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(1);

        // Replace A with B before the first response completes.
        status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 250L);
        logFetcher.onSubscriptionChanged(tb1, true);

        // The obsolete response from A arrives after B has been subscribed.
        gateway.completeFetch(
                0,
                makeFetchLogResponse(
                        Collections.singletonMap(
                                tb1,
                                FetchLogResultForBucket.empty(tb1, 180L, -1L)
                                        .withLogStartOffset(100L))));

        ScanRecords obsolete = logFetcher.collectFetch();

        assertThat(obsolete.hasProgress()).isFalse();
        assertThat(obsolete.finishedBuckets()).isEmpty();
        assertThat(status.getBucketOffset(tb1)).isEqualTo(LogScanner.EARLIEST_OFFSET);
        assertThat(logFetcher.getCompletedFetchesSize()).isZero();

        // B must still be able to issue its own FETCH_LOG v1 request.
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(2);

        PbFetchLogReqForBucket requestB =
                gateway.fetchRequests.get(1).getTablesReqAt(0).getBucketsReqAt(0);

        assertThat(requestB.getFetchOffset()).isEqualTo(LogScanner.EARLIEST_OFFSET);
        assertThat(requestB.hasStoppingOffset()).isTrue();
        assertThat(requestB.getStoppingOffset()).isEqualTo(250L);

        // B resolves EARLIEST independently.
        gateway.completeFetch(
                1,
                makeFetchLogResponse(
                        Collections.singletonMap(
                                tb1,
                                FetchLogResultForBucket.empty(tb1, 280L, -1L)
                                        .withLogStartOffset(200L))));

        ScanRecords resolution = logFetcher.collectFetch();

        assertThat(resolution.consumedUpToOffset(tb1)).isEqualTo(200L);
        assertThat(resolution.finishedBuckets()).isEmpty();
        assertThat(status.getBucketOffset(tb1)).isEqualTo(200L);
    }

    @Test
    void testBoundedLegacyEarliestRejectsMalformedListOffsetsResponse() {
        IOUtils.closeQuietly(logFetcher);

        LegacyFetchGateway gateway = new LegacyFetchGateway(-1L);
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());

        LogScannerStatus status = logFetcher.getLogScannerStatus();
        status.assignBoundedScanBucket(tb1, LogScanner.EARLIEST_OFFSET, 105L);

        // Reject FETCH_LOG v1, then start ListOffsets.
        logFetcher.sendFetches();
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(1);
        assertThat(gateway.listOffsetRequests).isEqualTo(1);

        // The response contains no bucket result.
        gateway.completeMalformedListOffsets(0);

        assertThatThrownBy(logFetcher::collectFetch)
                .isInstanceOf(FetchException.class)
                .hasMessageContaining("Missing or duplicate ListOffsets result");

        assertThat(status.getBucketOffset(tb1)).isEqualTo(LogScanner.EARLIEST_OFFSET);
        assertThat(status.drainFinishedBuckets()).isEmpty();

        // A malformed response is terminal, not an endless retry.
        logFetcher.sendFetches();
        assertThat(gateway.listOffsetRequests).isEqualTo(1);
    }

    private LogScannerStatus initializeLogScannerStatus() {
        Map<TableBucket, Long> scanBucketAndOffsets = new HashMap<>();
        scanBucketAndOffsets.put(tb1, 0L);
        LogScannerStatus status = new LogScannerStatus();
        status.assignScanBuckets(scanBucketAndOffsets);
        return status;
    }

    @Test
    void testObsoleteBoundedV1FetchFailureIsIgnored() {
        IOUtils.closeQuietly(logFetcher);

        ControlledFetchLogGateway gateway = new ControlledFetchLogGateway();
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());

        LogScannerStatus status = logFetcher.getLogScannerStatus();

        // Subscription A.
        status.assignBoundedScanBucket(tb1, 0L, 5L);
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(1);

        // Replace A with B, keeping the same starting offset.
        status.assignBoundedScanBucket(tb1, 0L, 10L);
        logFetcher.onSubscriptionChanged(tb1, true);

        // The old FETCH_LOG request fails after replacement.
        gateway.failFetch(0, new RequiredApiVersionNotSupportedException("obsolete"));

        ScanRecords result = logFetcher.collectFetch();

        assertThat(result.hasProgress()).isFalse();
        assertThat(result.finishedBuckets()).isEmpty();
        assertThat(status.getBucketOffset(tb1)).isZero();
        assertThat(status.allSubscribedBucketsFinished()).isFalse();

        // A stale failure must not downgrade B to FETCH_LOG v0.
        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(2);

        PbFetchLogReqForBucket request =
                gateway.fetchRequests.get(1).getTablesReqAt(0).getBucketsReqAt(0);

        assertThat(request.getFetchOffset()).isZero();
        assertThat(request.hasStoppingOffset()).isTrue();
        assertThat(request.getStoppingOffset()).isEqualTo(10L);
        assertThat(gateway.listOffsetRequests).isZero();
    }

    @Test
    void testFetchLogAuthorizationFailureDoesNotTriggerFallback() {
        IOUtils.closeQuietly(logFetcher);

        ControlledFetchLogGateway gateway = new ControlledFetchLogGateway();
        metadataUpdater = initializeMetadataUpdater(gateway);
        logFetcher = createLogFetcher(new Configuration());

        LogScannerStatus status = logFetcher.getLogScannerStatus();
        status.assignBoundedScanBucket(tb1, 0L, 5L);

        logFetcher.sendFetches();
        assertThat(gateway.fetchRequests).hasSize(1);

        gateway.failFetch(0, new AuthorizationException("denied"));

        // A generic FETCH_LOG RPC failure follows the existing fetch error handling path; only a
        // required API version failure triggers fallback.
        assertThat(logFetcher.collectFetch().hasProgress()).isFalse();
        assertThat(status.getBucketOffset(tb1)).isZero();
        assertThat(status.allSubscribedBucketsFinished()).isFalse();

        logFetcher.sendFetches();

        assertThat(gateway.fetchRequests).hasSize(2);

        PbFetchLogReqForBucket request =
                gateway.fetchRequests.get(1).getTablesReqAt(0).getBucketsReqAt(0);

        assertThat(request.hasStoppingOffset()).isTrue();
        assertThat(request.getFetchOffset()).isZero();
        assertThat(request.getStoppingOffset()).isEqualTo(5L);
        assertThat(gateway.listOffsetRequests).isZero();
    }

    private static class RemoteFetchTabletServerGateway extends TestTabletServerGateway {

        private final boolean multipleSegments;
        private final long logStartOffset;

        private RemoteFetchTabletServerGateway(boolean multipleSegments, long logStartOffset) {
            super(false, Collections.emptySet());
            this.multipleSegments = multipleSegments;
            this.logStartOffset = logStartOffset;
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(request);
            Map<TableBucket, FetchLogResultForBucket> resultForBucketMap = new HashMap<>();
            fetchLogData.forEach(
                    (tableBucket, fetchReqInfo) -> {
                        PhysicalTablePath physicalTablePath =
                                PhysicalTablePath.of(DATA1_TABLE_PATH);
                        long[] segmentStarts =
                                multipleSegments
                                        ? new long[] {
                                            logStartOffset,
                                            logStartOffset + 100L,
                                            logStartOffset + 200L
                                        }
                                        : new long[] {
                                            Math.max(fetchReqInfo.getFetchOffset(), logStartOffset)
                                        };

                        List<RemoteLogSegment> segments = new ArrayList<>();
                        for (long start : segmentStarts) {
                            segments.add(
                                    RemoteLogSegment.Builder.builder()
                                            .tableBucket(tableBucket)
                                            .physicalTablePath(physicalTablePath)
                                            .remoteLogSegmentId(UUID.randomUUID())
                                            .remoteLogStartOffset(start)
                                            .remoteLogEndOffset(start + 100L)
                                            .maxTimestamp(1000L)
                                            .segmentSizeInBytes(1024)
                                            .build());
                        }

                        RemoteLogFetchInfo remoteLogFetchInfo =
                                new RemoteLogFetchInfo(
                                        FlussPaths.remoteLogTabletDir(
                                                        new FsPath("/tmp/remote/log"),
                                                        physicalTablePath,
                                                        tableBucket)
                                                .toString(),
                                        null,
                                        segments,
                                        0);
                        resultForBucketMap.put(
                                tableBucket,
                                FetchLogResultForBucket.remote(
                                                tableBucket, remoteLogFetchInfo, 100L)
                                        .withLogStartOffset(logStartOffset));
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

    private static class LegacyFetchGateway extends TestTabletServerGateway {

        private final long filteredEndOffset;

        private final List<FetchLogRequest> fetchRequests = new ArrayList<>();

        private final List<CompletableFuture<ListOffsetsResponse>> listOffsetFutures =
                new ArrayList<>();

        @Nullable private final MemoryLogRecords responseRecords;

        private int listOffsetRequests;
        private boolean legacyFetchOutOfRange;

        private LegacyFetchGateway(long filteredEndOffset) {
            this(filteredEndOffset, null);
        }

        private LegacyFetchGateway(
                long filteredEndOffset, @Nullable MemoryLogRecords responseRecords) {
            super(false, Collections.emptySet());
            this.filteredEndOffset = filteredEndOffset;
            this.responseRecords = responseRecords;
        }

        private void failLegacyFetchWithOutOfRange() {
            legacyFetchOutOfRange = true;
        }

        private void completeMalformedListOffsets(int index) {
            listOffsetFutures.get(index).complete(new ListOffsetsResponse());
        }

        private PbFetchLogReqForBucket onlyBucketRequest(int index) {
            FetchLogRequest request = fetchRequests.get(index);

            assertThat(request.getTablesReqsList()).hasSize(1);
            assertThat(request.getTablesReqAt(0).getBucketsReqsList()).hasSize(1);

            return request.getTablesReqAt(0).getBucketsReqAt(0);
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            fetchRequests.add(request);
            boolean requiresV1 =
                    request.getTablesReqsList().stream()
                            .flatMap(table -> table.getBucketsReqsList().stream())
                            .anyMatch(PbFetchLogReqForBucket::hasStoppingOffset);
            if (requiresV1) {
                return completedExceptionally(
                        new RequiredApiVersionNotSupportedException(
                                "FETCH_LOG version 1 is not supported"));
            }
            Map<TableBucket, FetchLogResultForBucket> results = new HashMap<>();
            for (TableBucket bucket : getFetchLogData(request).keySet()) {
                FetchLogResultForBucket result;
                if (legacyFetchOutOfRange) {
                    result =
                            FetchLogResultForBucket.error(
                                    bucket,
                                    new ApiError(
                                            LOG_OFFSET_OUT_OF_RANGE_EXCEPTION,
                                            "Retention advanced the earliest offset to 120"));
                } else if (responseRecords == null) {
                    result = FetchLogResultForBucket.empty(bucket, 120L, filteredEndOffset);
                } else {
                    result =
                            FetchLogResultForBucket.records(
                                    bucket, responseRecords, 120L, -1L, -1L);
                }
                results.put(bucket, result);
            }
            return CompletableFuture.completedFuture(makeFetchLogResponse(results));
        }

        @Override
        public CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request) {
            listOffsetRequests++;
            CompletableFuture<ListOffsetsResponse> future = new CompletableFuture<>();
            listOffsetFutures.add(future);
            return future;
        }

        private void completeListOffsets(int index, long offset) {
            ListOffsetsResponse response = new ListOffsetsResponse();
            response.addBucketsResp().setBucketId(0).setErrorCode(NONE.code()).setOffset(offset);
            listOffsetFutures.get(index).complete(response);
        }

        private void completeListOffsetsWithError(int index, Errors error) {
            ListOffsetsResponse response = new ListOffsetsResponse();
            response.addBucketsResp().setBucketId(0).setErrorCode(error.code());
            listOffsetFutures.get(index).complete(response);
        }

        private void failListOffsets(int index, Throwable failure) {
            listOffsetFutures.get(index).completeExceptionally(failure);
        }
    }

    private static class ControlledFetchLogGateway extends TestTabletServerGateway {

        private final List<FetchLogRequest> fetchRequests = new ArrayList<>();
        private final List<CompletableFuture<FetchLogResponse>> fetchResponses = new ArrayList<>();

        private int listOffsetRequests;

        private ControlledFetchLogGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request) {
            listOffsetRequests++;
            return completedExceptionally(new AssertionError("Unexpected ListOffsets request"));
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            fetchRequests.add(request);
            CompletableFuture<FetchLogResponse> response = new CompletableFuture<>();
            fetchResponses.add(response);
            return response;
        }

        private void failFetch(int index, Throwable failure) {
            fetchResponses.get(index).completeExceptionally(failure);
        }

        private void completeFetch(int index, FetchLogResponse response) {
            fetchResponses.get(index).complete(response);
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
