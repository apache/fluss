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
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_INFO;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests compatibility with implementations of the original {@link LogScanner} interface. */
class LogScannerTest {

    @Test
    void testLegacySubscriptionWithIntegerArguments() {
        LegacyLogScanner scanner = new LegacyLogScanner();
        LogScanner api = scanner;
        api.subscribe(0, 2, 5L);
        assertThat(scanner.partitionId).isZero();
        assertThat(scanner.bucket).isEqualTo(2);
        assertThat(scanner.offset).isEqualTo(5L);
    }

    @Test
    void testLegacyScannerRejectsBoundedSubscriptions() {
        LogScanner scanner = new LegacyLogScanner();
        assertThatThrownBy(() -> scanner.subscribeBounded(0, 2, 5))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> scanner.subscribeBounded(1L, 0, 2, 5))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testInvalidOffsetsDoNotReplaceSubscription() throws Exception {
        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogScanner scanner = createScanner(downloader)) {
            scanner.subscribeBounded(0, 0, 0);
            assertThatThrownBy(() -> scanner.subscribeBounded(0, 0, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stopping offset");
            assertThatThrownBy(() -> scanner.subscribeBounded(0, -1, 5))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("starting offset");
            assertThatThrownBy(() -> scanner.subscribeBounded(0, 0, LogScanner.NO_STOPPING_OFFSET))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stopping offset");
            assertThat(scanner.poll(Duration.ZERO).finishedBuckets()).hasSize(1);
        }
    }

    @ParameterizedTest
    @CsvSource({"0, 0", "5, 5", "5, 3", "-2, 0"})
    void testEmptyBoundedSubscription(long startingOffset, long stoppingOffset) throws Exception {
        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogScannerImpl scanner = createScanner(downloader)) {
            scanner.subscribeBounded(0, startingOffset, stoppingOffset);
            ScanRecords records = scanner.poll(Duration.ZERO);
            assertThat(records.isEmpty()).isTrue();
            assertThat(records.finishedBuckets())
                    .containsExactly(new TableBucket(DATA1_TABLE_ID, 0));
            assertThat(scanner.poll(Duration.ZERO).hasProgress()).isFalse();
        }
    }

    @Test
    void testUnboundedSubscriptionClearsPendingCompletion() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID, 0);
        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogScannerImpl scanner = createScanner(downloader)) {
            scanner.subscribeBounded(0, 0, 0);
            scanner.subscribe(0, LogScanner.EARLIEST_OFFSET);
            assertThat(scanner.logScannerStatus.getBucketStoppingOffset(bucket))
                    .isEqualTo(LogScanner.NO_STOPPING_OFFSET);
            assertThat(scanner.logScannerStatus.hasReachedStoppingOffset(bucket)).isFalse();
            assertThat(scanner.logScannerStatus.hasPendingFinishedBuckets()).isFalse();

            // Preserve the original API's deferred validation of out-of-range offsets.
            scanner.subscribe(0, Long.MIN_VALUE);
            assertThat(scanner.logScannerStatus.getBucketOffset(bucket)).isEqualTo(Long.MIN_VALUE);
        }
    }

    @Test
    void testPartitionedSubscriptionRejectsInvalidOffsets() throws Exception {
        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogScannerImpl scanner = createScanner(downloader, PARTITION_TABLE_INFO)) {
            assertThatThrownBy(() -> scanner.subscribeBounded(1L, 0, 0, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stopping offset");
            assertThatThrownBy(
                            () -> scanner.subscribeBounded(1L, 0, 0, LogScanner.NO_STOPPING_OFFSET))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stopping offset");
            assertThatThrownBy(() -> scanner.subscribeBounded(1L, 0, -1, 5))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("starting offset");
            assertThat(scanner.logScannerStatus.prepareToPoll()).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSubscriptionRejectsWrongTableType(boolean bounded) throws Exception {
        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogScanner scanner = createScanner(downloader);
                LogScanner partitionedScanner = createScanner(downloader, PARTITION_TABLE_INFO)) {
            String nonPartitionedMethod =
                    bounded
                            ? "subscribeBounded(int bucket, long startingOffset, long stoppingOffset)"
                            : "subscribe(int bucket, long offset)";
            String partitionedMethod =
                    bounded
                            ? "subscribeBounded(long partitionId, int bucket, long startingOffset, long stoppingOffset)"
                            : "subscribe(long partitionId, int bucket, long offset)";
            assertThatThrownBy(
                            () -> {
                                if (bounded) {
                                    scanner.subscribeBounded(1L, 0, 0, 5);
                                } else {
                                    scanner.subscribe(1L, 0, 0);
                                }
                            })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(
                            "The table is not a partitioned table, please use \"%s\" to subscribe a non-partitioned bucket instead.",
                            nonPartitionedMethod);
            assertThatThrownBy(
                            () -> {
                                if (bounded) {
                                    partitionedScanner.subscribeBounded(0, 0, 5);
                                } else {
                                    partitionedScanner.subscribe(0, 0);
                                }
                            })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(
                            "The table is a partitioned table, please use \"%s\" to subscribe a partitioned bucket instead.",
                            partitionedMethod);
        }
    }

    private static LogScannerImpl createScanner(RemoteFileDownloader downloader) {
        return createScanner(downloader, DATA1_TABLE_INFO);
    }

    private static LogScannerImpl createScanner(
            RemoteFileDownloader downloader, TableInfo tableInfo) {
        return new LogScannerImpl(
                new Configuration(),
                tableInfo,
                new TestingMetadataUpdater(
                        Collections.singletonMap(tableInfo.getTablePath(), tableInfo)),
                TestingClientMetricGroup.newInstance(),
                downloader,
                null,
                TEST_SCHEMA_GETTER,
                null);
    }

    private static class LegacyLogScanner implements LogScanner {
        private long partitionId;
        private int bucket;
        private long offset;

        @Override
        public ScanRecords poll(Duration timeout) {
            return ScanRecords.EMPTY;
        }

        @Override
        public void subscribe(int bucket, long offset) {
            this.bucket = bucket;
            this.offset = offset;
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            this.partitionId = partitionId;
            subscribe(bucket, offset);
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {}

        @Override
        public void unsubscribe(int bucket) {}

        @Override
        public void wakeup() {}

        @Override
        public void close() {}
    }
}
