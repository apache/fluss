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

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.GetConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the timeout budget across all blocking stages of a log scanner poll. */
class AbstractLogScannerPollTest {

    @Test
    void testCommittedOffsetLookupSharesTimeoutWithFetchWait() throws Exception {
        long timeoutNanos = TimeUnit.SECONDS.toNanos(1);
        long beforePoll = System.nanoTime();
        Long fetchDeadlineNanos = pollFetchDeadlineNanos();
        long afterPoll = System.nanoTime();
        // The committed-offset lookup and the fetch wait share the deadline set when poll began,
        // so the fetch wait must not extend the deadline by its own timeout.
        assertThat(fetchDeadlineNanos)
                .isBetween(beforePoll + timeoutNanos, afterPoll + timeoutNanos);
    }

    private static Long pollFetchDeadlineNanos() throws Exception {
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
            assertThat(scanner.poll(Duration.ofSeconds(1))).isSameAs(ScanRecords.EMPTY);
            return fetcher.deadlineNanos;
        }
    }

    private static class TestScanner extends AbstractLogScanner<ScanRecords> {
        private TestScanner(
                LogScannerStatus status,
                TrackingLogFetcher fetcher,
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
