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

import org.apache.fluss.client.metadata.TestingClientSchemaGetter;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE2;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithBaseOffset;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the timeout budget across all blocking stages of a log scanner poll. */
class AbstractLogScannerPollTest {

    @Test
    void testCommittedOffsetLookupSharesTimeoutWithFetchWait() throws Exception {
        AtomicLong offsetLookupNanos = new AtomicLong();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), "group");
        conf.setBoolean(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key(), false);

        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<FindConsumerGroupOwnerResponse> findConsumerGroupOwner(
                            FindConsumerGroupOwnerRequest request) {
                        return CompletableFuture.completedFuture(
                                new FindConsumerGroupOwnerResponse()
                                        .setOwner(
                                                new PbServerNode()
                                                        .setNodeId(NODE1.id())
                                                        .setHost(NODE1.host())
                                                        .setPort(NODE1.port())));
                    }

                    @Override
                    public CompletableFuture<GetConsumerOffsetsResponse> getConsumerOffsets(
                            GetConsumerOffsetsRequest request) {
                        offsetLookupNanos.set(System.nanoTime());
                        return CompletableFuture.completedFuture(new GetConsumerOffsetsResponse());
                    }
                };
        TestingMetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        TestingMetadataUpdater.COORDINATOR,
                        Collections.singletonList(NODE1),
                        Collections.emptyMap(),
                        Collections.singletonMap(NODE1.id(), gateway),
                        conf);
        LogScannerStatus status = new LogScannerStatus();
        status.assignFromCommittedOffset(new TableBucket(1L, 0));
        TestingScannerMetricGroup metrics = TestingScannerMetricGroup.newInstance();
        TrackingLogFetcher fetcher = new TrackingLogFetcher(status, conf, metadataUpdater, metrics);

        try (TestScanner scanner =
                new TestScanner(status, fetcher, metrics, conf, metadataUpdater)) {
            Duration timeout = Duration.ofSeconds(30);
            long beforePoll = System.nanoTime();
            assertThat(scanner.poll(timeout)).isSameAs(ScanRecords.EMPTY);
            // Starting a fresh timeout after the offset lookup would exceed this upper bound.
            assertThat(fetcher.deadlineNanos)
                    .isBetween(
                            beforePoll + timeout.toNanos(),
                            offsetLookupNanos.get() + timeout.toNanos());
        }
    }

    @Test
    void testZeroTimeoutPollResolvesEachLeaderIndependently() throws Exception {
        LatestOffsetGateway slow = new LatestOffsetGateway();
        LatestOffsetGateway fast = new LatestOffsetGateway();
        Configuration conf = new Configuration();
        TestingMetadataUpdater updater =
                TestingMetadataUpdater.builder(
                                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO))
                        .withTabletServerGateway(NODE1.id(), slow)
                        .withTabletServerGateway(NODE2.id(), fast)
                        .build();
        LogScannerStatus status = new LogScannerStatus();
        TableBucket first = new TableBucket(DATA1_TABLE_ID, 0);
        TableBucket second = new TableBucket(DATA1_TABLE_ID, 1);
        status.assignScanBuckets(Collections.singletonMap(first, LogScanner.LATEST_OFFSET));
        status.assignScanBuckets(Collections.singletonMap(second, LogScanner.LATEST_OFFSET));
        TestingScannerMetricGroup metrics = TestingScannerMetricGroup.newInstance();
        LogFetcher fetcher =
                new LogFetcher(
                        "test",
                        status,
                        conf,
                        updater,
                        metrics,
                        new RemoteFileDownloader(1),
                        LogRecordReadContext.SchemaResolution.TARGET);
        try (TestScanner scanner = new TestScanner(status, fetcher, metrics, conf, updater)) {
            fetcher.registerTable(
                    new TableScanSpec(DATA1_TABLE_INFO, null, null),
                    new TestingClientSchemaGetter(
                            DATA1_TABLE_PATH, new SchemaInfo(DATA1_SCHEMA, 0), updater, conf));
            assertThat(scanner.poll(Duration.ZERO).isEmpty()).isTrue();
            assertThat(scanner.poll(Duration.ZERO).isEmpty()).isTrue();
            assertThat(slow.requests.get()).isOne();
            assertThat(fast.requests.get()).isOne();

            fast.completeOffset(second, 7L);
            ScanRecords records = scanner.poll(Duration.ZERO);
            assertThat(records.records(first)).isEmpty();
            assertThat(records.records(second)).hasSize(DATA1.size());
            assertThat(records.records(second).get(0).logOffset()).isEqualTo(7L);
            assertThat(status.allConsumed()).doesNotContainKey(first);

            status.unassignScanBuckets(Collections.singletonList(first));
            status.assignScanBuckets(Collections.singletonMap(first, 42L));
            slow.completeOffset(first, 100L);
            assertThat(status.getBucketOffset(first)).isEqualTo(42L);
        }
    }

    @Test
    void testLateOffsetResponseAfterScannerClose() throws Exception {
        LatestOffsetGateway gateway = new LatestOffsetGateway();
        TestingMetadataUpdater updater =
                TestingMetadataUpdater.builder(
                                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO))
                        .withTabletServerGateway(NODE1.id(), gateway)
                        .build();
        Configuration conf = new Configuration();
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID, 0);
        LogScannerStatus status = new LogScannerStatus();
        status.assignScanBuckets(Collections.singletonMap(bucket, LogScanner.LATEST_OFFSET));
        TestingScannerMetricGroup metrics = TestingScannerMetricGroup.newInstance();
        LogFetcher fetcher = new TrackingLogFetcher(status, conf, updater, metrics);
        try (TestScanner scanner = new TestScanner(status, fetcher, metrics, conf, updater)) {
            scanner.poll(Duration.ZERO);
            assertThat(gateway.requests.get()).isOne();
        }

        gateway.completeOffset(bucket, 7L);
        assertThat(status.getBucketOffset(bucket)).isNull();
    }

    private static class LatestOffsetGateway extends TestTabletServerGateway {
        private final AtomicInteger requests = new AtomicInteger();
        private final CompletableFuture<ListOffsetsResponse> response = new CompletableFuture<>();

        private final MemoryLogRecords records;

        private LatestOffsetGateway() throws Exception {
            super(false, Collections.emptySet());
            records = genMemoryLogRecordsWithBaseOffset(7L, DATA1);
        }

        @Override
        public CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request) {
            requests.incrementAndGet();
            return response;
        }

        void completeOffset(TableBucket bucket, long offset) {
            ListOffsetsResponse result = new ListOffsetsResponse();
            result.addBucketsResp().setBucketId(bucket.getBucket()).setOffset(offset);
            response.complete(result);
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchLogResultForBucket> result = new HashMap<>();
            getFetchLogData(request)
                    .forEach(
                            (bucket, info) -> {
                                long offset = info.getFetchOffset();
                                result.put(
                                        bucket,
                                        FetchLogResultForBucket.records(
                                                bucket,
                                                offset == 7L ? records : MemoryLogRecords.EMPTY,
                                                7L + DATA1.size(),
                                                -1L,
                                                -1L));
                            });
            return CompletableFuture.completedFuture(makeFetchLogResponse(result));
        }
    }

    private static class TestScanner extends AbstractLogScanner<ScanRecords> {
        private TestScanner(
                LogScannerStatus status,
                LogFetcher fetcher,
                TestingScannerMetricGroup metrics,
                Configuration conf,
                TestingMetadataUpdater metadataUpdater) {
            super("test", status, fetcher, metrics, conf, metadataUpdater);
        }

        @Override
        protected ScanRecords emptyResult() {
            return ScanRecords.EMPTY;
        }

        @Override
        protected ScanRecords toResult(ScanRecords records) {
            return records;
        }

        @Override
        protected String notSubscribedMessage() {
            return "not subscribed";
        }
    }

    private static class TrackingLogFetcher extends LogFetcher {
        private Long deadlineNanos;

        private TrackingLogFetcher(
                LogScannerStatus status,
                Configuration conf,
                TestingMetadataUpdater metadataUpdater,
                TestingScannerMetricGroup metrics) {
            super(
                    "test",
                    status,
                    conf,
                    metadataUpdater,
                    metrics,
                    new RemoteFileDownloader(1),
                    LogRecordReadContext.SchemaResolution.TARGET);
        }

        @Override
        public void sendFetches() {}

        @Override
        public boolean awaitNotEmpty(long deadlineNanos) {
            this.deadlineNanos = deadlineNanos;
            return false;
        }
    }
}
