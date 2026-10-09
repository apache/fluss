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

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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

    private LakeSnapshotAndLogSplitScanner createScanner(
            TableBucket bucket, long startingOffset, long stoppingOffset) {
        return new LakeSnapshotAndLogSplitScanner(
                table, lakeSource, null, bucket, startingOffset, stoppingOffset, null);
    }

    private static ScanRecords finishedRecords(TableBucket bucket, ScanRecord... records) {
        return new ScanRecords(Collections.singletonMap(bucket, Arrays.asList(records))) {
            @Override
            public Set<TableBucket> finishedBuckets() {
                return Collections.singleton(bucket);
            }
        };
    }
}
