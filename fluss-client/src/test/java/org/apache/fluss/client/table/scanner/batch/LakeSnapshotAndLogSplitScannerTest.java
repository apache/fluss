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
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.SortedRecordReader;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests exclusive log bounds and materialization-free progress in the shared lake merge reader. */
class LakeSnapshotAndLogSplitScannerTest {
    private static final TableBucket BUCKET = new TableBucket(1L, 0);
    private final LogScanner log = mock(LogScanner.class);

    @Test
    void testExclusiveUpperBoundBeforeUpdatingState() throws Exception {
        when(log.poll(any()))
                .thenReturn(
                        new ScanRecords(
                                Collections.singletonMap(
                                        BUCKET,
                                        Arrays.asList(
                                                record(2, 1, 11),
                                                record(3, 2, 22),
                                                record(4, 1, 99))),
                                Collections.singletonMap(BUCKET, 5L)));
        try (LakeSnapshotAndLogSplitScanner scanner = scanner(2, 4)) {
            assertThat(read(scanner)).containsExactlyInAnyOrder("1:11", "2:22");
        }
        verify(log).close();
    }

    @Test
    void testProgressOnlyTailCompletes() throws Exception {
        when(log.poll(any()))
                .thenReturn(
                        new ScanRecords(
                                Collections.emptyMap(), Collections.singletonMap(BUCKET, 4L)));
        try (LakeSnapshotAndLogSplitScanner scanner = scanner(2, 4)) {
            assertThat(read(scanner)).containsExactly("1:10");
        }
    }

    @Test
    void testEmptyReplayDoesNotSubscribeOrPoll() throws Exception {
        try (LakeSnapshotAndLogSplitScanner scanner = scanner(4, 4)) {
            assertThat(read(scanner)).containsExactly("1:10");
        }
        verify(log, never()).subscribe(0, 4);
        verify(log, never()).poll(any());
    }

    private static ScanRecord record(long offset, int id, int value) {
        return new ScanRecord(offset, 10L, ChangeType.INSERT, GenericRow.of(id, value));
    }

    @SuppressWarnings("unchecked")
    private LakeSnapshotAndLogSplitScanner scanner(long start, long stop) throws Exception {
        Table table = mock(Table.class);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        when(table.getTableInfo())
                .thenReturn(
                        TableInfo.of(TablePath.of("db", "t"), 1, 0, descriptor, "unused", 0, 0));
        Scan scan = mock(Scan.class);
        when(table.newScan()).thenReturn(scan);
        when(scan.project(any(int[].class))).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(log);
        LakeSource<LakeSplit> source = mock(LakeSource.class);
        SortedRecordReader reader = mock(SortedRecordReader.class);
        when(source.createRecordReader(any())).thenReturn(reader);
        when(reader.order()).thenReturn(Comparator.comparingInt(row -> row.getInt(0)));
        when(reader.read())
                .thenReturn(
                        CloseableIterator.wrap(
                                Collections.<LogRecord>singletonList(record(0, 1, 10)).iterator()));
        return new LakeSnapshotAndLogSplitScanner(table, source, null, BUCKET, start, stop, null);
    }

    private static List<String> read(LakeSnapshotAndLogSplitScanner scanner) throws Exception {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            CloseableIterator<InternalRow> batch = scanner.pollBatch(Duration.ofMillis(10));
            if (batch == null) {
                return result;
            }
            try (CloseableIterator<InternalRow> rows = batch) {
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    result.add(row.getInt(0) + ":" + row.getInt(1));
                }
            }
        }
        throw new AssertionError("Reader did not finish after consuming the stopping offset");
    }
}
