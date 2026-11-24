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
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitConsumerOffsetsResponse;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests commit callbacks and resource cleanup during scanner close. */
class AbstractLogScannerCloseTest {

    @Test
    void testInvalidGroupConfigurationReleasesScannerResources() throws IOException {
        Configuration conf =
                new Configuration().set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED, true);
        TestingMetadataUpdater updater =
                TestingMetadataUpdater.builder(Collections.emptyMap()).build();
        LogScannerStatus status = new LogScannerStatus();
        TestingScannerMetricGroup metrics = TestingScannerMetricGroup.newInstance();
        try (TrackingLogFetcher fetcher =
                new TrackingLogFetcher(status, conf, updater, metrics, false)) {
            assertThatThrownBy(() -> new TestScanner(status, fetcher, metrics, conf, updater))
                    .isInstanceOf(InvalidConfigException.class);
            assertThat(fetcher.closeCalls).isOne();
            assertThat(metrics.isClosed()).isTrue();
        }
    }

    @Test
    void testCloseDeliversCommitCallbackBeforeReleasingResources() {
        CompletableFuture<CommitConsumerOffsetsResponse> response = new CompletableFuture<>();
        TestScanner scanner = newScanner(response, false, Duration.ofSeconds(10));
        CompletableFuture<Thread> callbackThread = new CompletableFuture<>();
        scanner.commitAsync(
                (offsets, error) -> {
                    assertThat(error).isNull();
                    assertThat(scanner.scannerMetricGroup.isClosed()).isFalse();
                    assertThat(((TrackingLogFetcher) scanner.logFetcher).closeCalls).isZero();
                    callbackThread.complete(Thread.currentThread());
                });
        response.complete(new CommitConsumerOffsetsResponse());

        scanner.close();

        assertThat(callbackThread).isCompletedWithValue(Thread.currentThread());
        assertThat(scanner.scannerMetricGroup.isClosed()).isTrue();
        assertThat(((TrackingLogFetcher) scanner.logFetcher).closeCalls).isOne();
    }

    @Test
    void testCloseReleasesResourcesWithAnUnansweredCommit() {
        CompletableFuture<CommitConsumerOffsetsResponse> pending = new CompletableFuture<>();
        TestScanner scanner = newScanner(pending, false, Duration.ofMillis(50));
        scanner.commitAsync();

        scanner.close();

        assertThat(pending).isNotDone();
        assertThat(scanner.scannerMetricGroup.isClosed()).isTrue();
        assertThat(((TrackingLogFetcher) scanner.logFetcher).closeCalls).isOne();

        scanner.close();
        assertThat(((TrackingLogFetcher) scanner.logFetcher).closeCalls).isOne();
    }

    @Test
    void testCloseDeliversCommitFailureAndReleasesResourcesWhenFetcherCloseFails() {
        IllegalStateException commitFailure = new IllegalStateException("commit failed");
        CompletableFuture<CommitConsumerOffsetsResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(commitFailure);
        TestScanner scanner = newScanner(failed, true, Duration.ofSeconds(10));
        CompletableFuture<Exception> callbackError = new CompletableFuture<>();
        scanner.commitAsync((offsets, error) -> callbackError.complete(error));

        scanner.close();

        assertThat(callbackError).isCompletedWithValue(commitFailure);
        assertThat(scanner.scannerMetricGroup.isClosed()).isTrue();
        assertThat(((TrackingLogFetcher) scanner.logFetcher).closeCalls).isOne();

        scanner.close();
        assertThat(((TrackingLogFetcher) scanner.logFetcher).closeCalls).isOne();
    }

    private static TestScanner newScanner(
            CompletableFuture<CommitConsumerOffsetsResponse> commitResponse,
            boolean failFetcherClose,
            Duration requestTimeout) {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), "group");
        conf.setBoolean(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key(), true);
        conf.set(ConfigOptions.CLIENT_REQUEST_TIMEOUT, requestTimeout);

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
                    public CompletableFuture<CommitConsumerOffsetsResponse> commitConsumerOffsets(
                            CommitConsumerOffsetsRequest request) {
                        return commitResponse;
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
        status.assignScanBuckets(Collections.singletonMap(new TableBucket(1L, 0), 7L));
        TestingScannerMetricGroup metrics = TestingScannerMetricGroup.newInstance();
        TrackingLogFetcher fetcher =
                new TrackingLogFetcher(status, conf, metadataUpdater, metrics, failFetcherClose);
        return new TestScanner(status, fetcher, metrics, conf, metadataUpdater);
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
        private final boolean failClose;
        private int closeCalls;

        private TrackingLogFetcher(
                LogScannerStatus status,
                Configuration conf,
                TestingMetadataUpdater metadataUpdater,
                TestingScannerMetricGroup metrics,
                boolean failClose) {
            super(
                    "test",
                    status,
                    conf,
                    metadataUpdater,
                    metrics,
                    new RemoteFileDownloader(1),
                    LogRecordReadContext.SchemaResolution.TARGET);
            this.failClose = failClose;
        }

        @Override
        public synchronized void close() throws IOException {
            closeCalls++;
            super.close();
            if (failClose) {
                throw new IOException("fetcher close failed");
            }
        }
    }
}
