/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.FileLogProjection;
import com.alibaba.fluss.record.FileLogRecords;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.Projection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static com.alibaba.fluss.record.TestData.DATA2;
import static com.alibaba.fluss.record.TestData.DATA2_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA2_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA2_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA2_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.rpc.CommonRpcMessageUtils.toByteBuffer;
import static com.alibaba.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.client.table.scanner.log.DefaultCompletedFetch}. */
public class DefaultCompletedFetchTest {
    private LogScannerStatus logScannerStatus;
    private @TempDir File tempDir;
    private TableInfo tableInfo;
    private RowType rowType;

    @BeforeEach
    void beforeEach() {
        tableInfo = DATA2_TABLE_INFO;
        rowType = DATA2_ROW_TYPE;
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 0), 0L);
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 1), 0L);
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 2), 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
    }

    @Test
    void testSimple() throws Exception {
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket0 =
                new FetchLogResultForBucket(
                        tb, createMemoryLogRecords(DATA2, LogFormat.ARROW), 10L);
        DefaultCompletedFetch defaultCompletedFetch =
                makeCompletedFetch(tb, resultForBucket0, fetchOffset);
        List<ScanRecord> scanRecords = defaultCompletedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(8);
        assertThat(scanRecords.get(0).logOffset()).isEqualTo(0L);

        scanRecords = defaultCompletedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(2);
        assertThat(scanRecords.get(0).logOffset()).isEqualTo(8L);

        scanRecords = defaultCompletedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(0);
    }

    @Test
    void testNegativeFetchCount() throws Exception {
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket0 =
                new FetchLogResultForBucket(
                        tb, createMemoryLogRecords(DATA2, LogFormat.ARROW), 10L);
        DefaultCompletedFetch defaultCompletedFetch =
                makeCompletedFetch(tb, resultForBucket0, fetchOffset);
        List<ScanRecord> scanRecords = defaultCompletedFetch.fetchRecords(-10);
        assertThat(scanRecords.size()).isEqualTo(0);
    }

    @Test
    void testNoRecordsInFetch() {
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket0 =
                new FetchLogResultForBucket(tb, MemoryLogRecords.EMPTY, 0L);
        DefaultCompletedFetch defaultCompletedFetch =
                makeCompletedFetch(tb, resultForBucket0, fetchOffset);
        List<ScanRecord> scanRecords = defaultCompletedFetch.fetchRecords(10);
        assertThat(scanRecords.size()).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEXED", "ARROW"})
    void testProjection(String format) throws Exception {
        LogFormat logFormat = LogFormat.fromString(format);
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.STRING())
                        .withComment("b is second column")
                        .column("c", DataTypes.STRING())
                        .withComment("c is adding column")
                        .build();
        tableInfo =
                TableInfo.of(
                        DATA2_TABLE_PATH,
                        DATA2_TABLE_ID,
                        DEFAULT_SCHEMA_ID,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(3)
                                .logFormat(logFormat)
                                .build(),
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        Projection projection = Projection.of(new int[] {0, 2});
        MemoryLogRecords memoryLogRecords;
        if (logFormat == LogFormat.ARROW) {
            memoryLogRecords = genRecordsWithProjection(DATA2, projection);
        } else {
            memoryLogRecords = createMemoryLogRecords(DATA2, LogFormat.INDEXED);
        }
        FetchLogResultForBucket resultForBucket0 =
                new FetchLogResultForBucket(tb, memoryLogRecords, 10L);
        DefaultCompletedFetch defaultCompletedFetch =
                makeCompletedFetch(tb, resultForBucket0, fetchOffset, projection);
        List<ScanRecord> scanRecords = defaultCompletedFetch.fetchRecords(8);
        List<Object[]> expectedObjects =
                Arrays.asList(
                        new Object[] {1, "hello"},
                        new Object[] {2, "hi"},
                        new Object[] {3, "nihao"},
                        new Object[] {4, "hello world"},
                        new Object[] {5, "hi world"},
                        new Object[] {6, "nihao world"},
                        new Object[] {7, "hello world2"},
                        new Object[] {8, "hi world2"});
        assertThat(scanRecords.size()).isEqualTo(8);
        for (int i = 0; i < scanRecords.size(); i++) {
            Object[] expectObject = expectedObjects.get(i);
            ScanRecord actualRecord = scanRecords.get(i);
            assertThat(actualRecord.logOffset()).isEqualTo(i);
            assertThat(actualRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
            InternalRow row = actualRecord.getRow();
            assertThat(row.getInt(0)).isEqualTo(expectObject[0]);
            assertThat(row.getString(1).toString()).isEqualTo(expectObject[1]);
        }

        // test projection reorder.
        defaultCompletedFetch =
                makeCompletedFetch(
                        tb, resultForBucket0, fetchOffset, Projection.of(new int[] {2, 0}));
        scanRecords = defaultCompletedFetch.fetchRecords(8);
        assertThat(scanRecords.size()).isEqualTo(8);
        for (int i = 0; i < scanRecords.size(); i++) {
            Object[] expectObject = expectedObjects.get(i);
            ScanRecord actualRecord = scanRecords.get(i);
            assertThat(actualRecord.logOffset()).isEqualTo(i);
            assertThat(actualRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
            InternalRow row = actualRecord.getRow();
            assertThat(row.getString(0).toString()).isEqualTo(expectObject[1]);
            assertThat(row.getInt(1)).isEqualTo(expectObject[0]);
        }
    }

    private DefaultCompletedFetch makeCompletedFetch(
            TableBucket tableBucket, FetchLogResultForBucket resultForBucket, long offset) {
        return makeCompletedFetch(tableBucket, resultForBucket, offset, null);
    }

    private DefaultCompletedFetch makeCompletedFetch(
            TableBucket tableBucket,
            FetchLogResultForBucket resultForBucket,
            long offset,
            Projection projection) {
        return new DefaultCompletedFetch(
                tableBucket,
                resultForBucket,
                LogRecordReadContext.createReadContext(tableInfo, false, projection),
                logScannerStatus,
                true,
                offset);
    }

    private MemoryLogRecords createMemoryLogRecords(List<Object[]> objects, LogFormat logFormat)
            throws Exception {
        return createRecordsWithoutBaseLogOffset(
                rowType, DEFAULT_SCHEMA_ID, 0L, 1000L, objects, logFormat);
    }

    private MemoryLogRecords genRecordsWithProjection(List<Object[]> objects, Projection projection)
            throws Exception {
        File logFile = FlussPaths.logFile(tempDir, 0L);
        FileLogRecords fileLogRecords = FileLogRecords.open(logFile, false, 1024 * 1024, false);
        fileLogRecords.append(
                createRecordsWithoutBaseLogOffset(
                        rowType, DEFAULT_SCHEMA_ID, 0L, 1000L, objects, LogFormat.ARROW));
        fileLogRecords.flush();

        FileLogProjection fileLogProjection = new FileLogProjection();
        fileLogProjection.setCurrentProjection(
                DATA2_TABLE_ID, rowType, DEFAULT_COMPRESSION, projection.getProjectionInOrder());
        ByteBuffer buffer =
                toByteBuffer(
                        fileLogProjection
                                .project(
                                        fileLogRecords.channel(),
                                        0,
                                        fileLogRecords.sizeInBytes(),
                                        Integer.MAX_VALUE)
                                .getBytesView()
                                .getByteBuf());
        return MemoryLogRecords.pointToByteBuffer(buffer);
    }
}
