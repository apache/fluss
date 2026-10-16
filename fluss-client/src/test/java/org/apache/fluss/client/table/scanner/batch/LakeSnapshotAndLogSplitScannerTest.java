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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.exception.UnsupportedBoundedEarliestException;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE2;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE3;
import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.metadata.LogFormat.ARROW;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests bounded log consumption before merging a lake snapshot. */
class LakeSnapshotAndLogSplitScannerTest {
    private static final Duration TIMEOUT = Duration.ZERO;
    private static final TableBucket BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 0);

    private Table table;
    private LogScanner logScanner;
    private LakeSource<LakeSplit> lakeSource;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        table = mock(Table.class);
        Scan scan = mock(Scan.class);
        logScanner = mock(LogScanner.class);
        lakeSource = mock(LakeSource.class);
        SortedRecordReader reader = mock(SortedRecordReader.class);
        when(table.getTableInfo()).thenReturn(DATA1_TABLE_INFO_PK);
        when(table.newScan()).thenReturn(scan);
        when(scan.project(any(int[].class))).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);
        when(lakeSource.createRecordReader(any())).thenReturn(reader);
        when(reader.order()).thenReturn(Comparator.comparingInt(row -> row.getInt(0)));
        when(reader.read())
                .thenReturn(
                        CloseableIterator.wrap(
                                Collections.<LogRecord>singletonList(
                                                new ScanRecord(GenericRow.of(1, null)))
                                        .iterator()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConsumesFinalRecordsBeforeMergingSnapshot(boolean partitioned) throws Exception {
        TableBucket bucket = partitioned ? new TableBucket(DATA1_TABLE_ID_PK, 7L, 0) : BUCKET;
        when(logScanner.poll(TIMEOUT))
                .thenReturn(
                        finishedRecords(
                                bucket,
                                new ScanRecord(10, 0, ChangeType.DELETE, GenericRow.of(1, null)),
                                new ScanRecord(11, 0, ChangeType.INSERT, GenericRow.of(2, null))));

        try (LakeSnapshotAndLogSplitScanner scanner = createScanner(bucket, 10L, 20L)) {
            if (partitioned) {
                verify(logScanner).subscribeBounded(7L, 0, 10L, 20L);
            } else {
                verify(logScanner).subscribeBounded(0, 10L, 20L);
            }
            // Completion can accompany records even when the final visible offset is below stop-1.
            try (CloseableIterator<InternalRow> pending = scanner.pollBatch(TIMEOUT)) {
                assertThat(pending.hasNext()).isFalse();
            }
            try (CloseableIterator<InternalRow> rows = scanner.pollBatch(TIMEOUT)) {
                assertThat(rows).isNotNull();
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getInt(0)).isEqualTo(2);
                assertThat(rows.hasNext()).isFalse();
            }
            assertThat(scanner.pollBatch(TIMEOUT)).isNull();
            verify(logScanner, times(1)).poll(TIMEOUT);
        }
        verify(logScanner).close();
    }

    @Test
    void testCompletionWithoutRecords() throws Exception {
        when(logScanner.poll(TIMEOUT)).thenReturn(finishedRecords(BUCKET));
        try (LakeSnapshotAndLogSplitScanner scanner = createScanner(BUCKET, 10L, 20L)) {
            verify(logScanner).subscribeBounded(0, 10L, 20L);
            // The bounded scan can finish without returning any records.
            try (CloseableIterator<InternalRow> pending = scanner.pollBatch(TIMEOUT)) {
                assertThat(pending.hasNext()).isFalse();
            }
            // The next poll starts reading the merged lake snapshot.
            try (CloseableIterator<InternalRow> rows = scanner.pollBatch(TIMEOUT)) {
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getInt(0)).isEqualTo(1);
                assertThat(rows.hasNext()).isFalse();
            }
            assertThat(scanner.pollBatch(TIMEOUT)).isNull();
            verify(logScanner, times(1)).poll(TIMEOUT);
        }
    }

    @ParameterizedTest
    @CsvSource({"10, 10", "10, 5", "-2, 0"})
    void testEmptyLogRangeDoesNotPollLog(long startingOffset, long stoppingOffset)
            throws Exception {
        try (LakeSnapshotAndLogSplitScanner scanner =
                createScanner(BUCKET, startingOffset, stoppingOffset)) {
            try (CloseableIterator<InternalRow> rows = scanner.pollBatch(TIMEOUT)) {
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getInt(0)).isEqualTo(1);
            }
            verify(logScanner, times(0)).poll(any());
        }
    }

    @Test
    void testProgressWithoutCompletionKeepsReading() throws Exception {
        ScanRecords progress =
                new ScanRecords(Collections.emptyMap(), Collections.singletonMap(BUCKET, 15L));
        when(logScanner.poll(TIMEOUT)).thenReturn(progress, finishedRecords(BUCKET));
        try (LakeSnapshotAndLogSplitScanner scanner = createScanner(BUCKET, 10L, 20L)) {
            for (int i = 0; i < 2; i++) {
                try (CloseableIterator<InternalRow> pending = scanner.pollBatch(TIMEOUT)) {
                    assertThat(pending.hasNext()).isFalse();
                }
            }
            try (CloseableIterator<InternalRow> rows = scanner.pollBatch(TIMEOUT)) {
                assertThat(rows.hasNext()).isTrue();
            }
            verify(logScanner, times(2)).poll(TIMEOUT);
        }
    }

    @Test
    void testMissingLogRangeErrorIsPropagated() throws Exception {
        FetchException failure =
                new FetchException("Requested snapshot log offset is out of range");
        when(logScanner.poll(TIMEOUT)).thenThrow(failure);
        try (LakeSnapshotAndLogSplitScanner scanner = createScanner(BUCKET, 10L, 20L)) {
            assertThatThrownBy(() -> scanner.pollBatch(TIMEOUT)).isSameAs(failure);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFallbackToLegacyScanForOldServer(boolean partitioned) throws Exception {
        TableBucket bucket = partitioned ? new TableBucket(DATA1_TABLE_ID_PK, 7L, 0) : BUCKET;
        UnsupportedBoundedEarliestException failure =
                new UnsupportedBoundedEarliestException(
                        "Server does not report resolved_earliest_offset");
        ScanRecords legacyRecords =
                new ScanRecords(
                        Collections.singletonMap(
                                bucket,
                                Arrays.asList(
                                        new ScanRecord(
                                                10, 0, ChangeType.DELETE, GenericRow.of(1, null)),
                                        new ScanRecord(
                                                11, 0, ChangeType.INSERT, GenericRow.of(2, null)),
                                        new ScanRecord(
                                                12, 0, ChangeType.INSERT, GenericRow.of(3, null)))),
                        Collections.singletonMap(bucket, 13L));
        when(logScanner.poll(TIMEOUT)).thenThrow(failure).thenReturn(legacyRecords);

        try (LakeSnapshotAndLogSplitScanner scanner =
                createScanner(bucket, LogScanner.EARLIEST_OFFSET, 12L)) {
            if (partitioned) {
                verify(logScanner).subscribeBounded(7L, 0, LogScanner.EARLIEST_OFFSET, 12L);
            } else {
                verify(logScanner).subscribeBounded(0, LogScanner.EARLIEST_OFFSET, 12L);
            }
            // The first poll detects the old server and switches subscription.
            try (CloseableIterator<InternalRow> pending = scanner.pollBatch(TIMEOUT)) {
                assertThat(pending.hasNext()).isFalse();
            }
            if (partitioned) {
                verify(logScanner).subscribe(7L, 0, LogScanner.EARLIEST_OFFSET);
            } else {
                verify(logScanner).subscribe(0, LogScanner.EARLIEST_OFFSET);
            }
            // The second poll consumes records from the legacy subscription.
            try (CloseableIterator<InternalRow> pending = scanner.pollBatch(TIMEOUT)) {
                assertThat(pending.hasNext()).isFalse();
            }
            // The third poll returns the merged snapshot.
            try (CloseableIterator<InternalRow> rows = scanner.pollBatch(TIMEOUT)) {
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getInt(0)).isEqualTo(2);
                assertThat(rows.hasNext()).isFalse();
            }
            assertThat(scanner.pollBatch(TIMEOUT)).isNull();
            verify(logScanner, times(2)).poll(TIMEOUT);
        }
    }

    @Test
    void testLegacyFallbackExcludesRecordsAtStoppingOffset() throws Exception {
        ScanRecords legacyRecords =
                new ScanRecords(
                        Collections.singletonMap(
                                BUCKET,
                                Arrays.asList(
                                        new ScanRecord(
                                                10, 0, ChangeType.INSERT, GenericRow.of(2, null)),
                                        new ScanRecord(
                                                12, 0, ChangeType.INSERT, GenericRow.of(3, null)))),
                        Collections.singletonMap(BUCKET, 13L));
        when(logScanner.poll(TIMEOUT))
                .thenThrow(
                        new UnsupportedBoundedEarliestException("Missing resolved_earliest_offset"))
                .thenReturn(legacyRecords);
        try (LakeSnapshotAndLogSplitScanner scanner =
                createScanner(BUCKET, LogScanner.EARLIEST_OFFSET, 12L)) {
            // Switch from bounded to legacy subscription.
            try (CloseableIterator<InternalRow> pending = scanner.pollBatch(TIMEOUT)) {
                assertThat(pending.hasNext()).isFalse();
            }
            // Read log records.
            try (CloseableIterator<InternalRow> pending = scanner.pollBatch(TIMEOUT)) {
                assertThat(pending.hasNext()).isFalse();
            }
            // Lake contains id=1, and the valid log contains id=2.
            // Offset 12 / id=3 must be excluded.
            try (CloseableIterator<InternalRow> rows = scanner.pollBatch(TIMEOUT)) {
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getInt(0)).isEqualTo(1);
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getInt(0)).isEqualTo(2);
                assertThat(rows.hasNext()).isFalse();
            }
            assertThat(scanner.pollBatch(TIMEOUT)).isNull();
        }
    }

    @Test
    void testLegacyFallbackCompletesOnOffsetProgress() throws Exception {
        ScanRecords progress =
                new ScanRecords(Collections.emptyMap(), Collections.singletonMap(BUCKET, 25L));
        when(logScanner.poll(TIMEOUT))
                .thenThrow(
                        new UnsupportedBoundedEarliestException("Missing resolved_earliest_offset"))
                .thenReturn(progress);
        try (LakeSnapshotAndLogSplitScanner scanner =
                createScanner(BUCKET, LogScanner.EARLIEST_OFFSET, 20L)) {
            // First poll switches to the legacy subscription.
            try (CloseableIterator<InternalRow> pending = scanner.pollBatch(TIMEOUT)) {
                assertThat(pending.hasNext()).isFalse();
            }
            verify(logScanner).subscribe(0, LogScanner.EARLIEST_OFFSET);
            // Second poll sees progress beyond the stopping offset.
            // There are no actual log records.
            try (CloseableIterator<InternalRow> pending = scanner.pollBatch(TIMEOUT)) {
                assertThat(pending.hasNext()).isFalse();
            }
            // The scanner must now start merging the lake snapshot.
            try (CloseableIterator<InternalRow> rows = scanner.pollBatch(TIMEOUT)) {
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getInt(0)).isEqualTo(1);
                assertThat(rows.hasNext()).isFalse();
            }
            assertThat(scanner.pollBatch(TIMEOUT)).isNull();
            verify(logScanner, times(2)).poll(TIMEOUT);
        }
    }

    @Test
    void testOtherUnsupportedOperationDoesNotTriggerFallback() throws Exception {
        UnsupportedOperationException failure =
                new UnsupportedOperationException("Unrelated scanner error");
        when(logScanner.poll(TIMEOUT)).thenThrow(failure);
        try (LakeSnapshotAndLogSplitScanner scanner =
                createScanner(BUCKET, LogScanner.EARLIEST_OFFSET, 20L)) {
            assertThatThrownBy(() -> scanner.pollBatch(TIMEOUT)).isSameAs(failure);
            verify(logScanner, never()).subscribe(0, LogScanner.EARLIEST_OFFSET);
        }
    }

    @Test
    void testOldServerFallbackWithRealLogScanner() throws Exception {
        // Log records:
        // 10: DELETE id=1
        // 11: INSERT id=2
        // 12: INSERT id=3 (exclusive boundary, must be ignored)
        MemoryLogRecords logRecords =
                createBasicMemoryLogRecords(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        10L,
                        0L,
                        CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Arrays.asList(ChangeType.DELETE, ChangeType.INSERT, ChangeType.INSERT),
                        Arrays.<Object[]>asList(
                                new Object[] {1, null},
                                new Object[] {2, null},
                                new Object[] {3, null}),
                        ARROW,
                        DEFAULT_COMPRESSION);

        LegacyTabletServerGateway gateway = new LegacyTabletServerGateway(logRecords);

        Configuration conf = new Configuration();
        TestingMetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        TestingMetadataUpdater.COORDINATOR,
                        Arrays.asList(NODE1, NODE2, NODE3),
                        Collections.singletonMap(
                                DATA1_TABLE_INFO_PK.getTablePath(), DATA1_TABLE_INFO_PK),
                        Collections.singletonMap(1, gateway),
                        conf);

        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogScannerImpl realLogScanner =
                        new LogScannerImpl(
                                conf,
                                DATA1_TABLE_INFO_PK,
                                metadataUpdater,
                                TestingClientMetricGroup.newInstance(),
                                downloader,
                                null,
                                TEST_SCHEMA_GETTER,
                                null)) {

            // Replace only the mocked scanner with the real implementation.
            Scan scan = table.newScan();
            when(scan.createLogScanner()).thenReturn(realLogScanner);

            try (LakeSnapshotAndLogSplitScanner scanner =
                    createScanner(BUCKET, LogScanner.EARLIEST_OFFSET, 12L)) {
                List<Integer> mergedKeys = new ArrayList<>();
                boolean completed = false;
                for (int attempt = 0; attempt < 20; attempt++) {
                    CloseableIterator<InternalRow> rows = scanner.pollBatch(Duration.ofMillis(200));
                    if (rows == null) {
                        completed = true;
                        break;
                    }
                    try (CloseableIterator<InternalRow> result = rows) {
                        while (result.hasNext()) {
                            mergedKeys.add(result.next().getInt(0));
                        }
                    }
                }
                assertThat(completed).isTrue();
                // The lake snapshot contains id=1, but offset 10 deletes it.
                // Offset 11 inserts id=2.
                // Offset 12 is excluded by the exclusive stopping offset.
                assertThat(mergedKeys).containsExactly(2);
                // The first request uses bounded EARLIEST.
                // After the missing field is detected, the legacy subscription
                // issues another EARLIEST request.
                assertThat(gateway.requestedOffsets).hasSizeGreaterThanOrEqualTo(2);
                assertThat(gateway.requestedOffsets.subList(0, 2))
                        .containsExactly(LogScanner.EARLIEST_OFFSET, LogScanner.EARLIEST_OFFSET);
            }
        }
    }

    private LakeSnapshotAndLogSplitScanner createScanner(
            TableBucket bucket, long startingOffset, long stoppingOffset) {
        return new LakeSnapshotAndLogSplitScanner(
                table, lakeSource, null, bucket, startingOffset, stoppingOffset, null);
    }

    private static ScanRecords finishedRecords(TableBucket bucket, ScanRecord... records) {
        return new ScanRecords(
                Collections.singletonMap(bucket, Arrays.asList(records)),
                Collections.emptyMap(),
                Collections.singleton(bucket));
    }

    private static final class LegacyTabletServerGateway extends TestTabletServerGateway {
        private final MemoryLogRecords logRecords;
        private final List<Long> requestedOffsets = new ArrayList<>();

        private LegacyTabletServerGateway(MemoryLogRecords logRecords) {
            super(false, Collections.emptySet());
            this.logRecords = logRecords;
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchReqInfo> requests = getFetchLogData(request);
            assertThat(requests).hasSize(1);
            Map.Entry<TableBucket, FetchReqInfo> entry = requests.entrySet().iterator().next();
            TableBucket bucket = entry.getKey();
            long offset = entry.getValue().getFetchOffset();
            requestedOffsets.add(offset);
            FetchLogResultForBucket result;
            if (requestedOffsets.size() == 1) {
                // Simulate an old server response:
                // no resolved_earliest_offset field.
                result =
                        FetchLogResultForBucket.records(
                                bucket, MemoryLogRecords.EMPTY, 13L, -1L, -1L);
            } else if (requestedOffsets.size() == 2) {
                // The legacy unbounded subscription should resume fetching.
                result = FetchLogResultForBucket.records(bucket, logRecords, 13L, -1L, -1L);
            } else {
                result =
                        FetchLogResultForBucket.records(
                                bucket, MemoryLogRecords.EMPTY, 13L, -1L, -1L);
            }
            return CompletableFuture.completedFuture(
                    makeFetchLogResponse(Collections.singletonMap(bucket, result)));
        }
    }
}
