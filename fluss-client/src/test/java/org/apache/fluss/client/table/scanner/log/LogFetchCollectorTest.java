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

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.UnsupportedBoundedEarliestException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LogFetchCollector}. */
public class LogFetchCollectorTest {
    private LogScannerStatus logScannerStatus;
    private LogFetchBuffer logFetchBuffer;
    private LogFetchCollector logFetchCollector;
    private LogRecordReadContext readContext;

    @BeforeEach
    void setup() {
        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(new TableBucket(DATA1_TABLE_ID, 0), 0L);
        scanBuckets.put(new TableBucket(DATA1_TABLE_ID, 1), 0L);
        scanBuckets.put(new TableBucket(DATA1_TABLE_ID, 2), 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
        logFetchBuffer = new LogFetchBuffer();
        logFetchCollector =
                new LogFetchCollector(logScannerStatus, new Configuration(), metadataUpdater);
        readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER);
    }

    @AfterEach
    void afterEach() {
        if (readContext != null) {
            readContext.close();
            readContext = null;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBoundedEarliestRequiresResolvedOffset(boolean arrow) {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(tb, EARLIEST_OFFSET, 50L);
        logFetchBuffer.add(
                makeCompletedFetch(
                        tb, FetchLogResultForBucket.empty(tb, 100L, -1L), EARLIEST_OFFSET));
        AbstractLogFetchCollector<?, ?> collector =
                arrow
                        ? new ArrowLogFetchCollector(
                                logScannerStatus,
                                new Configuration(),
                                new TestingMetadataUpdater(
                                        Collections.singletonMap(
                                                DATA1_TABLE_PATH, DATA1_TABLE_INFO)))
                        : logFetchCollector;

        assertThatThrownBy(() -> collector.collectFetch(logFetchBuffer))
                .isInstanceOf(UnsupportedBoundedEarliestException.class)
                .hasMessageContaining("resolved_earliest_offset");
        assertThat(logFetchBuffer.peek()).isNull();
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(EARLIEST_OFFSET);
    }

    @ParameterizedTest
    @CsvSource({
        // arrow, startingOffset, stoppingOffset, resolvedOffset, expectedOffset, finished
        "false, -2,  50, 100,  50, true",
        "true,  -2,  50, 100,  50, true",
        "false, -2, 100, 100, 100, true",
        "true,  -2, 100, 100, 100, true",
        "false, -2, 150, 100, 100, false",
        "true,  -2, 150, 100, 100, false",
        "false, -2,  50,   0,   0, false",
        "true,  -2,  50,   0,   0, false",
        "false, 10,  50,  -1,  10, false",
        "true,   10,  50,  -1,  10, false"
    })
    void testResolvedBoundedStartingOffset(
            boolean arrow,
            long startingOffset,
            long stoppingOffset,
            long resolvedOffset,
            long expectedOffset,
            boolean finished) {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(tb, startingOffset, stoppingOffset);
        CompletedFetch fetch =
                makeCompletedFetch(
                        tb,
                        FetchLogResultForBucket.records(
                                tb, MemoryLogRecords.EMPTY, 100L, -1L, -1L, resolvedOffset),
                        startingOffset);

        logFetchBuffer.add(fetch);
        boolean expectedProgress = startingOffset != expectedOffset;

        if (arrow) {
            ArrowLogFetchCollector collector =
                    new ArrowLogFetchCollector(
                            logScannerStatus,
                            new Configuration(),
                            new TestingMetadataUpdater(
                                    Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO)));

            try (ArrowScanRecords result = collector.collectFetch(logFetchBuffer)) {
                assertThat(result.count()).isZero();
                assertThat(result.records(tb)).isEmpty();
                assertThat(result.hasProgress()).isEqualTo(expectedProgress);
                if (finished) {
                    assertThat(result.finishedBuckets().contains(tb)).isTrue();
                } else {
                    assertThat(result.finishedBuckets().isEmpty()).isTrue();
                }
                if (expectedProgress) {
                    assertThat(result.consumedUpToOffset(tb)).isEqualTo(expectedOffset);
                } else {
                    assertThat(result.consumedUpToOffset(tb)).isNull();
                }
            }

            // Completion/progress from this response must be one-shot.
            try (ArrowScanRecords result = collector.collectFetch(logFetchBuffer)) {
                assertThat(result.hasProgress()).isFalse();
            }
        } else {
            ScanRecords result = logFetchCollector.collectFetch(logFetchBuffer);
            assertThat(result.isEmpty()).isTrue();
            assertThat(result.records(tb)).isEmpty();
            assertThat(result.hasProgress()).isEqualTo(expectedProgress);
            if (finished) {
                assertThat(result.finishedBuckets().contains(tb)).isTrue();
            } else {
                assertThat(result.finishedBuckets().isEmpty()).isTrue();
            }
            if (expectedProgress) {
                assertThat(result.consumedUpToOffset(tb)).isEqualTo(expectedOffset);
            } else {
                assertThat(result.consumedUpToOffset(tb)).isNull();
            }

            // Completion/progress from this response must be one-shot.
            assertThat(logFetchCollector.collectFetch(logFetchBuffer).hasProgress()).isFalse();
        }

        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(expectedOffset);
        assertThat(logScannerStatus.hasReachedStoppingOffset(tb)).isEqualTo(finished);
        assertThat(fetch.isConsumed()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(longs = {-1L, 100L})
    void testUnboundedEarliestKeepsEmptyFetchBehavior(long resolvedOffset) {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBuckets(Collections.singletonMap(tb, EARLIEST_OFFSET));
        logFetchBuffer.add(
                makeCompletedFetch(
                        tb,
                        FetchLogResultForBucket.records(
                                tb, MemoryLogRecords.EMPTY, 100L, -1L, -1L, resolvedOffset),
                        EARLIEST_OFFSET));

        assertThat(logFetchCollector.collectFetch(logFetchBuffer).hasProgress()).isTrue();
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(EARLIEST_OFFSET);
    }

    @Test
    void testDiscardStaleResolvedStartingOffset() {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(tb, 10L, 50L);
        CompletedFetch fetch =
                makeCompletedFetch(
                        tb,
                        FetchLogResultForBucket.records(
                                tb, MemoryLogRecords.EMPTY, 100L, -1L, -1L, 100L),
                        EARLIEST_OFFSET);
        logFetchBuffer.add(fetch);

        assertThat(logFetchCollector.collectFetch(logFetchBuffer).hasProgress()).isFalse();
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(10L);
        assertThat(fetch.isConsumed()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testResolvedStartingOffsetInsideBatch(boolean arrow) throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);

        // The logical starting offset is EARLIEST_OFFSET, while the server resolves it to 100.
        // The bounded subscription should stop before offset 103.
        logScannerStatus.assignScanBucket(tb, EARLIEST_OFFSET, 103L);

        // The physical batch starts from offset 95, which means records before the resolved
        // starting offset 100 must be skipped by the collector.
        MemoryLogRecords records =
                createBasicMemoryLogRecords(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        95L,
                        0L,
                        LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Collections.nCopies(DATA1.size(), ChangeType.APPEND_ONLY),
                        DATA1,
                        LogFormat.ARROW,
                        DEFAULT_COMPRESSION,
                        true);

        FetchLogResultForBucket result =
                FetchLogResultForBucket.records(tb, records, 105L, -1L, -1L, 100L);

        CompletedFetch fetch = makeCompletedFetch(tb, result, EARLIEST_OFFSET);

        logFetchBuffer.add(fetch);

        MetadataUpdater metadata =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));

        if (arrow) {
            ArrowLogFetchCollector collector =
                    new ArrowLogFetchCollector(logScannerStatus, new Configuration(), metadata);

            try (ArrowScanRecords scanRecords = collector.collectFetch(logFetchBuffer)) {

                assertThat(scanRecords.count()).isEqualTo(3);
                assertThat(scanRecords.records(tb)).hasSize(1);

                ArrowBatchData batch = scanRecords.records(tb).get(0);
                assertThat(batch.getBaseLogOffset()).isEqualTo(100L);
                assertThat(batch.getRecordCount()).isEqualTo(3);
                assertThat(batch.getVectorSchemaRoot().getRowCount()).isEqualTo(3);

                assertThat(scanRecords.consumedUpToOffset(tb)).isEqualTo(103L);
                assertThat(scanRecords.finishedBuckets()).containsExactly(tb);
            }
        } else {
            LogFetchCollector collector =
                    new LogFetchCollector(logScannerStatus, new Configuration(), metadata);

            ScanRecords scanRecords = collector.collectFetch(logFetchBuffer);

            assertThat(scanRecords.records(tb))
                    .extracting(ScanRecord::logOffset)
                    .containsExactly(100L, 101L, 102L);

            assertThat(scanRecords.consumedUpToOffset(tb)).isEqualTo(103L);
            assertThat(scanRecords.finishedBuckets()).containsExactly(tb);
        }

        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(103L);
        assertThat(fetch.isConsumed()).isTrue();
    }

    @Test
    void testResolvedStartingOffsetInsideBatchWithMaxPollRecords() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);

        logScannerStatus.assignScanBucket(tb, EARLIEST_OFFSET, 103L);

        MemoryLogRecords records =
                createBasicMemoryLogRecords(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        95L,
                        0L,
                        LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Collections.nCopies(DATA1.size(), ChangeType.APPEND_ONLY),
                        DATA1,
                        LogFormat.ARROW,
                        DEFAULT_COMPRESSION,
                        true);

        FetchLogResultForBucket result =
                FetchLogResultForBucket.records(tb, records, 105L, -1L, -1L, 100L);

        CompletedFetch fetch = makeCompletedFetch(tb, result, EARLIEST_OFFSET);

        logFetchBuffer.add(fetch);

        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, 2);

        MetadataUpdater metadata =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));

        LogFetchCollector collector = new LogFetchCollector(logScannerStatus, conf, metadata);

        // Only two records can be returned in the first poll. Although the physical batch
        // starts at 95, records before the resolved starting offset 100 must not count
        // towards max.poll.records.
        ScanRecords first = collector.collectFetch(logFetchBuffer);

        assertThat(first.records(tb)).extracting(ScanRecord::logOffset).containsExactly(100L, 101L);
        assertThat(first.consumedUpToOffset(tb)).isEqualTo(102L);
        assertThat(first.finishedBuckets()).isEmpty();

        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(102L);
        assertThat(fetch.isConsumed()).isFalse();

        // The remaining record before the bounded end offset is returned by the next poll.
        ScanRecords second = collector.collectFetch(logFetchBuffer);

        assertThat(second.records(tb)).extracting(ScanRecord::logOffset).containsExactly(102L);
        assertThat(second.consumedUpToOffset(tb)).isEqualTo(103L);
        assertThat(second.finishedBuckets()).containsExactly(tb);

        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(103L);
        assertThat(fetch.isConsumed()).isTrue();
    }

    @Test
    void testBoundedEarliestWithoutResolutionDrainsNonEmptyFetch() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);

        logScannerStatus.assignScanBucket(tb, LogScanner.EARLIEST_OFFSET, 200L);

        // Intentionally use the old overload to simulate a response from an old server:
        // records are present, but resolved_earliest_offset is absent.
        FetchLogResultForBucket result =
                FetchLogResultForBucket.records(
                        tb, genMemoryLogRecordsByObject(DATA1), 200L, -1L, -1L);

        CompletedFetch fetch = makeCompletedFetch(tb, result, LogScanner.EARLIEST_OFFSET);

        logFetchBuffer.add(fetch);

        assertThatThrownBy(() -> logFetchCollector.collectFetch(logFetchBuffer))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("resolved_earliest_offset");

        assertThat(fetch.isConsumed()).isTrue();
        assertThat(logFetchBuffer.peek()).isNull();
        assertThat(logFetchBuffer.isEmpty()).isTrue();

        // Failed resolution must not mutate the scanner position.
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(LogScanner.EARLIEST_OFFSET);
    }

    @Test
    void testNormal() throws Exception {
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket0 =
                FetchLogResultForBucket.records(
                        tb, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L);
        CompletedFetch completedFetch = makeCompletedFetch(tb, resultForBucket0, fetchOffset);

        // Validate that the buffer is empty until after we add the fetch data.
        assertThat(logFetchBuffer.isEmpty()).isTrue();
        logFetchBuffer.add(completedFetch);
        assertThat(logFetchBuffer.isEmpty()).isFalse();

        // Validate that the completed fetch isn't initialized just because we add it to the buffer
        assertThat(completedFetch.isInitialized()).isFalse();

        // Fetch the data and validate that we get all the records we want back.
        ScanRecords bucketAndRecords = logFetchCollector.collectFetch(logFetchBuffer);
        assertThat(bucketAndRecords.buckets().size()).isEqualTo(1);
        assertThat(bucketAndRecords.records(tb).size()).isEqualTo(10);

        // When we collected the data from the buffer, this will cause the completed fetch to get
        // initialized.
        assertThat(completedFetch.isInitialized()).isTrue();

        assertThat(completedFetch.isConsumed()).isTrue();

        assertThat(logFetchBuffer.isEmpty()).isTrue();
        assertThat(logFetchBuffer.peek()).isNull();
        assertThat(logFetchBuffer.poll()).isNull();

        // However, while the queue is "empty", the next-in-line fetch is actually still in the
        // buffer.
        assertThat(logFetchBuffer.nextInLineFetch()).isNotNull();

        // Validate that the next fetch position has been updated to point to the record after our
        // last fetched record.
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(10L);

        // Now attempt to collect more records from the fetch buffer.
        bucketAndRecords = logFetchCollector.collectFetch(logFetchBuffer);
        assertThat(bucketAndRecords.buckets().size()).isEqualTo(0);
    }

    @Test
    void testCollectAfterUnassign() throws Exception {
        TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 1L, 1);
        TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 1L, 2);
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(tb1, 0L);
        scanBuckets.put(tb2, 0L);
        logScannerStatus.assignScanBuckets(scanBuckets);

        FetchLogResultForBucket resultForBucket1 =
                FetchLogResultForBucket.records(
                        tb1, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L);
        FetchLogResultForBucket resultForBucket2 =
                FetchLogResultForBucket.records(
                        tb2, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L);
        CompletedFetch completedFetch1 = makeCompletedFetch(tb1, resultForBucket1, 0L);
        CompletedFetch completedFetch2 = makeCompletedFetch(tb2, resultForBucket2, 0L);

        logFetchBuffer.add(completedFetch1);
        logFetchBuffer.add(completedFetch2);

        // unassign bucket 2
        logScannerStatus.unassignScanBuckets(Collections.singletonList(tb2));

        ScanRecords bucketAndRecords = logFetchCollector.collectFetch(logFetchBuffer);
        // should only contain records for bucket 1
        assertThat(bucketAndRecords.buckets()).containsExactly(tb1);

        // collect again, should be empty
        bucketAndRecords = logFetchCollector.collectFetch(logFetchBuffer);
        assertThat(bucketAndRecords.buckets().size()).isEqualTo(0);
    }

    @Test
    void testTotalBytesRead() throws Exception {
        TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 1L, 1);
        TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 1L, 2);
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(tb1, 0L);
        scanBuckets.put(tb2, 0L);
        logScannerStatus.assignScanBuckets(scanBuckets);

        CompletedFetch completedFetch1 =
                makeCompletedFetch(
                        tb1,
                        FetchLogResultForBucket.records(
                                tb1, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L),
                        0L);
        CompletedFetch completedFetch2 =
                makeCompletedFetch(
                        tb2,
                        FetchLogResultForBucket.records(
                                tb2, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L),
                        0L);

        logFetchBuffer.add(completedFetch1);
        logFetchBuffer.add(completedFetch2);

        ScanRecords scanRecords = logFetchCollector.collectFetch(logFetchBuffer);

        // Both fetches should be fully consumed
        assertThat(completedFetch1.isConsumed()).isTrue();
        assertThat(completedFetch2.isConsumed()).isTrue();

        // Compute the expected per-record size from the batch-level average
        // (Arrow format records use batch.sizeInBytes() / recordCount as fallback)
        MemoryLogRecords expectedData = genMemoryLogRecordsByObject(DATA1);
        int expectedPerRecordSize = 0;
        int expectedRecordCount = 0;
        for (LogRecordBatch batch : expectedData.batches()) {
            expectedPerRecordSize = batch.sizeInBytes() / batch.getRecordCount();
            expectedRecordCount += batch.getRecordCount();
        }
        // Two fetches with the same data
        long expectedTotal = (long) expectedPerRecordSize * expectedRecordCount * 2;

        long totalBytesRead = 0;
        for (ScanRecord record : scanRecords) {
            assertThat(record.getSizeInBytes()).isEqualTo(expectedPerRecordSize);
            totalBytesRead += record.getSizeInBytes();
        }
        assertThat(totalBytesRead).isEqualTo(expectedTotal);
    }

    @Test
    void testShouldContinueConsumeSameCompletedFetchAcrossPolls() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, 2);
        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        LogFetchCollector collector =
                new LogFetchCollector(logScannerStatus, conf, metadataUpdater);

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        FetchLogResultForBucket result =
                FetchLogResultForBucket.records(
                        tb, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L);
        CompletedFetch completedFetch = makeCompletedFetch(tb, result, 0L);
        logFetchBuffer.add(completedFetch);

        ScanRecords firstPoll = collector.collectFetch(logFetchBuffer);
        assertThat(firstPoll.records(tb).size()).isEqualTo(2);
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(2L);
        assertThat(logScannerStatus.recordsLag()).isEqualTo(8L);
        assertThat(completedFetch.isConsumed()).isFalse();

        ScanRecords secondPoll = collector.collectFetch(logFetchBuffer);
        assertThat(secondPoll.records(tb).size()).isEqualTo(2);
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(4L);
        assertThat(logScannerStatus.recordsLag()).isEqualTo(6L);
        assertThat(completedFetch.isConsumed()).isFalse();
    }

    @Test
    void testFilteredEmptyResponseAdvancesOffset() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, 2);
        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        LogFetchCollector collector =
                new LogFetchCollector(logScannerStatus, conf, metadataUpdater);

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        FetchLogResultForBucket filteredEmpty = FetchLogResultForBucket.empty(tb, 10L, 20L);
        CompletedFetch completedFetch = makeCompletedFetch(tb, filteredEmpty, 0L);
        logFetchBuffer.add(completedFetch);

        ScanRecords scanRecords = collector.collectFetch(logFetchBuffer);
        assertThat(scanRecords.records(tb)).isEmpty();
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(20L);
        assertThat(completedFetch.isConsumed()).isTrue();
        // Empty record list, but bucket exposed via buckets() with an advanced consumedUpToOffset.
        assertThat(scanRecords.buckets()).contains(tb);
        assertThat(scanRecords.consumedUpToOffset(tb)).isEqualTo(20L);
    }

    private DefaultCompletedFetch makeCompletedFetch(
            TableBucket tableBucket, FetchLogResultForBucket resultForBucket, long offset) {
        return new DefaultCompletedFetch(
                tableBucket,
                DATA1_TABLE_PATH,
                resultForBucket,
                readContext,
                logScannerStatus,
                true,
                offset,
                null);
    }

    @Test
    void testCollectDrainsDiscardedFetch() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        CompletedFetch completedFetch =
                makeCompletedFetch(
                        tb,
                        FetchLogResultForBucket.records(
                                tb, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L),
                        0L);
        logFetchBuffer.add(completedFetch);
        logScannerStatus.unassignScanBuckets(Collections.singletonList(tb));

        ScanRecords records = logFetchCollector.collectFetch(logFetchBuffer);

        assertThat(records.buckets()).isEmpty();
        assertThat(completedFetch.isConsumed()).isTrue();
    }

    @Test
    void testUpdateBeforeAndAfterNeverSplitAcrossPolls() throws Exception {
        // Create records: INSERT, UPDATE_BEFORE, UPDATE_AFTER, INSERT
        // With maxPollRecords=1, the fix should still return -U/+U together.
        List<ChangeType> changeTypes =
                Arrays.asList(
                        ChangeType.INSERT,
                        ChangeType.UPDATE_BEFORE,
                        ChangeType.UPDATE_AFTER,
                        ChangeType.INSERT);
        List<Object[]> objects = DATA1.subList(0, 4);
        MemoryLogRecords records =
                createBasicMemoryLogRecords(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0L,
                        System.currentTimeMillis(),
                        LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        changeTypes,
                        objects,
                        LogFormat.ARROW,
                        DEFAULT_COMPRESSION);

        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, 1);
        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        LogFetchCollector collector =
                new LogFetchCollector(logScannerStatus, conf, metadataUpdater);

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        FetchLogResultForBucket result = FetchLogResultForBucket.records(tb, records, 4L, -1L, -1L);
        CompletedFetch completedFetch = makeCompletedFetch(tb, result, 0L);
        logFetchBuffer.add(completedFetch);

        // Poll 1: should get 1 INSERT record (maxPollRecords=1)
        ScanRecords poll1 = collector.collectFetch(logFetchBuffer);
        List<ScanRecord> records1 = poll1.records(tb);
        assertThat(records1).hasSize(1);
        assertThat(records1.get(0).getChangeType()).isEqualTo(ChangeType.INSERT);

        // Poll 2: should get 2 records (-U and +U together) even though maxPollRecords=1,
        // because -U/+U must never be split.
        ScanRecords poll2 = collector.collectFetch(logFetchBuffer);
        List<ScanRecord> records2 = poll2.records(tb);
        assertThat(records2).hasSize(2);
        assertThat(records2.get(0).getChangeType()).isEqualTo(ChangeType.UPDATE_BEFORE);
        assertThat(records2.get(1).getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);

        // Poll 3: should get the last INSERT record
        ScanRecords poll3 = collector.collectFetch(logFetchBuffer);
        List<ScanRecord> records3 = poll3.records(tb);
        assertThat(records3).hasSize(1);
        assertThat(records3.get(0).getChangeType()).isEqualTo(ChangeType.INSERT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEmptyRangeFromBeginningReportsCompletion(boolean arrow) {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(bucket, EARLIEST_OFFSET, 0L);

        MetadataUpdater metadata =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));

        if (arrow) {
            ArrowLogFetchCollector collector =
                    new ArrowLogFetchCollector(logScannerStatus, new Configuration(), metadata);
            try (ArrowScanRecords records = collector.collectFetch(logFetchBuffer)) {
                assertThat(records.count()).isZero();
                assertThat(records.hasProgress()).isTrue();
                assertThat(records.finishedBuckets()).containsExactly(bucket);
            }
            try (ArrowScanRecords records = collector.collectFetch(logFetchBuffer)) {
                assertThat(records.hasProgress()).isFalse();
            }
        } else {
            ScanRecords records = logFetchCollector.collectFetch(logFetchBuffer);
            assertThat(records.isEmpty()).isTrue();
            assertThat(records.hasProgress()).isTrue();
            assertThat(records.finishedBuckets()).containsExactly(bucket);
            assertThat(logFetchCollector.collectFetch(logFetchBuffer).hasProgress()).isFalse();
        }
    }

    @ParameterizedTest
    @CsvSource({
        // arrow, stoppingOffset, filteredEndOffset, expectedOffset, finished
        "false, 5, 20, 5, true",
        "true,  5, 20, 5, true",
        "false, 10, 5, 5, false",
        "true,  10, 5, 5, false"
    })
    void testBoundedFilteredEmptyResponse(
            boolean arrow,
            long stoppingOffset,
            long filteredEndOffset,
            long expectedOffset,
            boolean finished) {

        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(tb, 0L, stoppingOffset);

        FetchLogResultForBucket filteredEmpty =
                FetchLogResultForBucket.empty(tb, 10L, filteredEndOffset);
        CompletedFetch completedFetch = makeCompletedFetch(tb, filteredEmpty, 0L);
        logFetchBuffer.add(completedFetch);

        if (arrow) {
            ArrowLogFetchCollector collector =
                    new ArrowLogFetchCollector(
                            logScannerStatus, new Configuration(), metadataUpdater);

            try (ArrowScanRecords records = collector.collectFetch(logFetchBuffer)) {
                assertThat(records.count()).isZero();
                assertThat(records.hasProgress()).isTrue();
                assertThat(records.records(tb)).isEmpty();
                assertThat(records.buckets()).containsExactly(tb);
                assertThat(records.consumedUpToOffset(tb)).isEqualTo(expectedOffset);

                if (finished) {
                    assertThat(records.finishedBuckets()).containsExactly(tb);
                } else {
                    assertThat(records.finishedBuckets()).isEmpty();
                }
            }
        } else {
            LogFetchCollector collector =
                    new LogFetchCollector(logScannerStatus, new Configuration(), metadataUpdater);

            ScanRecords records = collector.collectFetch(logFetchBuffer);

            assertThat(records.isEmpty()).isTrue();
            assertThat(records.hasProgress()).isTrue();
            assertThat(records.records(tb)).isEmpty();
            assertThat(records.consumedUpToOffset(tb)).isEqualTo(expectedOffset);

            if (finished) {
                assertThat(records.finishedBuckets()).containsExactly(tb);
            } else {
                assertThat(records.finishedBuckets()).isEmpty();
            }
        }

        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(expectedOffset);
        assertThat(logScannerStatus.hasReachedStoppingOffset(tb)).isEqualTo(finished);
        assertThat(completedFetch.isConsumed()).isTrue();
    }

    @Test
    void testBoundedFetchStopsAtStoppingOffset() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, 2);

        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        LogFetchCollector collector =
                new LogFetchCollector(logScannerStatus, conf, metadataUpdater);

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(tb, 0L, 5L);

        FetchLogResultForBucket result =
                FetchLogResultForBucket.records(
                        tb, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L);
        CompletedFetch completedFetch = makeCompletedFetch(tb, result, 0L);
        logFetchBuffer.add(completedFetch);

        ScanRecords firstPoll = collector.collectFetch(logFetchBuffer);
        assertThat(firstPoll.records(tb)).extracting(ScanRecord::logOffset).containsExactly(0L, 1L);
        assertThat(firstPoll.consumedUpToOffset(tb)).isEqualTo(2L);
        assertThat(firstPoll.finishedBuckets()).isEmpty();
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(2L);
        assertThat(completedFetch.isConsumed()).isFalse();

        ScanRecords secondPoll = collector.collectFetch(logFetchBuffer);
        assertThat(secondPoll.records(tb))
                .extracting(ScanRecord::logOffset)
                .containsExactly(2L, 3L);
        assertThat(secondPoll.consumedUpToOffset(tb)).isEqualTo(4L);
        assertThat(secondPoll.finishedBuckets()).isEmpty();
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(4L);
        assertThat(completedFetch.isConsumed()).isFalse();

        ScanRecords thirdPoll = collector.collectFetch(logFetchBuffer);
        assertThat(thirdPoll.records(tb)).extracting(ScanRecord::logOffset).containsExactly(4L);
        assertThat(thirdPoll.consumedUpToOffset(tb)).isEqualTo(5L);
        assertThat(thirdPoll.finishedBuckets()).containsExactly(tb);
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(5L);
        assertThat(completedFetch.isConsumed()).isTrue();
    }

    @Test
    void testStoppingOffsetTakesPrecedenceOverUpdatePairGrouping() throws Exception {
        List<ChangeType> changeTypes =
                Arrays.asList(
                        ChangeType.INSERT,
                        ChangeType.UPDATE_BEFORE,
                        ChangeType.UPDATE_AFTER,
                        ChangeType.INSERT);

        List<Object[]> objects = DATA1.subList(0, 4);

        MemoryLogRecords records =
                createBasicMemoryLogRecords(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0L,
                        System.currentTimeMillis(),
                        LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        changeTypes,
                        objects,
                        LogFormat.ARROW,
                        DEFAULT_COMPRESSION);

        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, 1);

        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        LogFetchCollector collector =
                new LogFetchCollector(logScannerStatus, conf, metadataUpdater);

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(tb, 0L, 2L);

        FetchLogResultForBucket result = FetchLogResultForBucket.records(tb, records, 4L, -1L, -1L);
        CompletedFetch completedFetch = makeCompletedFetch(tb, result, 0L);
        logFetchBuffer.add(completedFetch);

        ScanRecords firstPoll = collector.collectFetch(logFetchBuffer);
        assertThat(firstPoll.records(tb)).hasSize(1);
        assertThat(firstPoll.records(tb).get(0).getChangeType()).isEqualTo(ChangeType.INSERT);
        assertThat(firstPoll.finishedBuckets()).isEmpty();

        ScanRecords secondPoll = collector.collectFetch(logFetchBuffer);

        assertThat(secondPoll.records(tb)).hasSize(1);
        assertThat(secondPoll.records(tb).get(0).getChangeType())
                .isEqualTo(ChangeType.UPDATE_BEFORE);
        assertThat(secondPoll.records(tb).get(0).logOffset()).isEqualTo(1L);

        assertThat(secondPoll.consumedUpToOffset(tb)).isEqualTo(2L);
        assertThat(secondPoll.finishedBuckets()).containsExactly(tb);
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(2L);
        assertThat(completedFetch.isConsumed()).isTrue();
    }

    @Test
    void testDiscardBufferedFetchForFinishedBucket() throws Exception {
        Configuration conf = new Configuration();
        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        LogFetchCollector collector =
                new LogFetchCollector(logScannerStatus, conf, metadataUpdater);

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(tb, 0L, 5L);

        FetchLogResultForBucket firstResult =
                FetchLogResultForBucket.records(
                        tb, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L);
        CompletedFetch firstFetch = makeCompletedFetch(tb, firstResult, 0L);

        FetchLogResultForBucket staleResult =
                FetchLogResultForBucket.records(
                        tb, genMemoryLogRecordsByObject(DATA1), 20L, -1L, -1L);
        CompletedFetch staleFetch = makeCompletedFetch(tb, staleResult, 10L);

        logFetchBuffer.add(firstFetch);
        logFetchBuffer.add(staleFetch);

        ScanRecords firstPoll = collector.collectFetch(logFetchBuffer);

        assertThat(firstPoll.finishedBuckets()).containsExactly(tb);
        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(5L);
        assertThat(firstFetch.isConsumed()).isTrue();

        ScanRecords secondPoll = collector.collectFetch(logFetchBuffer);

        assertThat(secondPoll.hasProgress()).isFalse();
        assertThat(staleFetch.isConsumed()).isTrue();
    }

    @Test
    void testUnboundedArrowProgressOnlyPreservesLegacyResultShape() {
        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));

        ArrowLogFetchCollector collector =
                new ArrowLogFetchCollector(logScannerStatus, new Configuration(), metadataUpdater);

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(tb, 0L, LogScanner.NO_STOPPING_OFFSET);

        FetchLogResultForBucket filteredEmpty = FetchLogResultForBucket.empty(tb, 10L, 5L);
        CompletedFetch completedFetch = makeCompletedFetch(tb, filteredEmpty, 0L);
        logFetchBuffer.add(completedFetch);

        try (ArrowScanRecords records = collector.collectFetch(logFetchBuffer)) {
            assertThat(records.count()).isZero();

            // Preserve the legacy Arrow collector result shape for an unbounded
            // progress-only fetch.
            assertThat(records.isEmpty()).isFalse();
            assertThat(records.buckets()).containsExactly(tb);
            assertThat(records.records(tb)).isEmpty();

            assertThat(records.hasProgress()).isTrue();
            assertThat(records.consumedUpToOffset(tb)).isEqualTo(5L);
            assertThat(records.finishedBuckets()).isEmpty();
        }

        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(5L);
        assertThat(completedFetch.isConsumed()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(longs = {5L, 10L})
    void testBoundedArrowFetchRespectsStoppingOffset(long stoppingOffset) throws Exception {
        MetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));

        ArrowLogFetchCollector collector =
                new ArrowLogFetchCollector(logScannerStatus, new Configuration(), metadataUpdater);

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        logScannerStatus.assignScanBucket(tb, 0L, stoppingOffset);

        MemoryLogRecords records =
                createBasicMemoryLogRecords(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0L,
                        0L,
                        LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Collections.nCopies(DATA1.size(), ChangeType.APPEND_ONLY),
                        DATA1,
                        LogFormat.ARROW,
                        DEFAULT_COMPRESSION,
                        true);

        FetchLogResultForBucket result =
                FetchLogResultForBucket.records(tb, records, DATA1.size(), -1L, -1L);

        CompletedFetch completedFetch = makeCompletedFetch(tb, result, 0L);
        logFetchBuffer.add(completedFetch);

        try (ArrowScanRecords scanRecords = collector.collectFetch(logFetchBuffer)) {
            int expectedRecordCount = (int) stoppingOffset;

            assertThat(scanRecords.count()).isEqualTo(expectedRecordCount);
            assertThat(scanRecords.records(tb)).hasSize(1);

            ArrowBatchData batch = scanRecords.records(tb).get(0);
            assertThat(batch.getBaseLogOffset()).isZero();
            assertThat(batch.getRecordCount()).isEqualTo(expectedRecordCount);
            assertThat(batch.getVectorSchemaRoot().getRowCount()).isEqualTo(expectedRecordCount);

            assertThat(scanRecords.consumedUpToOffset(tb)).isEqualTo(stoppingOffset);
            assertThat(scanRecords.finishedBuckets()).containsExactly(tb);
            assertThat(scanRecords.hasProgress()).isTrue();
        }

        assertThat(logScannerStatus.getBucketOffset(tb)).isEqualTo(stoppingOffset);
        assertThat(completedFetch.isConsumed()).isTrue();
    }

    @Test
    void testBoundedArrowTruncationReleasesAllBatchMemory() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID, 0);
        int batchSize = DATA1.size();
        long stoppingOffset = batchSize + batchSize / 2;
        logScannerStatus.assignScanBucket(bucket, 0L, stoppingOffset);
        ArrowLogFetchCollector collector =
                new ArrowLogFetchCollector(
                        logScannerStatus,
                        new Configuration(),
                        new TestingMetadataUpdater(
                                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO)));

        List<MemoryLogRecords> batches = new ArrayList<>();
        int totalBytes = 0;
        for (int i = 0; i < 3; i++) {
            MemoryLogRecords records =
                    createBasicMemoryLogRecords(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            (long) i * batchSize,
                            System.currentTimeMillis(),
                            LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                            NO_WRITER_ID,
                            NO_BATCH_SEQUENCE,
                            Collections.nCopies(batchSize, ChangeType.APPEND_ONLY),
                            DATA1,
                            LogFormat.ARROW,
                            DEFAULT_COMPRESSION,
                            true);
            batches.add(records);
            totalBytes += records.sizeInBytes();
        }
        ByteBuffer buffer = ByteBuffer.allocate(totalBytes);
        for (MemoryLogRecords batch : batches) {
            buffer.put(batch.getMemorySegment().wrap(0, batch.sizeInBytes()));
        }
        buffer.flip();
        CompletedFetch fetch =
                makeCompletedFetch(
                        bucket,
                        FetchLogResultForBucket.records(
                                bucket,
                                MemoryLogRecords.pointToByteBuffer(buffer),
                                3L * batchSize,
                                -1L,
                                -1L),
                        0L);
        logFetchBuffer.add(fetch);

        LongSupplier allocatedBytes;
        try (ArrowScanRecords records = collector.collectFetch(logFetchBuffer)) {
            assertThat(records.records(bucket)).hasSize(2);
            assertThat(records.count()).isEqualTo((int) stoppingOffset);
            assertThat(records.finishedBuckets()).containsExactly(bucket);
            assertThat(fetch.isConsumed()).isTrue();
            ArrowBatchData first = records.records(bucket).get(0);
            ArrowBatchData truncated = records.records(bucket).get(1);
            assertThat(first.getRecordCount()).isEqualTo(batchSize);
            assertThat(truncated.getBaseLogOffset()).isEqualTo(batchSize);
            assertThat(truncated.getRecordCount()).isEqualTo(batchSize / 2);
            // Both batches remain readable after draining the fetch and transferring ownership.
            assertThat(first.getVectorSchemaRoot().getVector(0).getObject(0))
                    .isEqualTo(DATA1.get(0)[0]);
            assertThat(truncated.getVectorSchemaRoot().getVector(0).getObject(batchSize / 2 - 1))
                    .isEqualTo(DATA1.get(batchSize / 2 - 1)[0]);
            allocatedBytes =
                    first.getVectorSchemaRoot().getVector(0).getAllocator()::getAllocatedMemory;
            assertThat(allocatedBytes.getAsLong()).isPositive();
        }
        // Check the read-side allocator before closing readContext: closing the result must
        // release retained slices as well as the discarded third batch and the original second
        // batch.
        assertThat(allocatedBytes.getAsLong()).isZero();
    }
}
