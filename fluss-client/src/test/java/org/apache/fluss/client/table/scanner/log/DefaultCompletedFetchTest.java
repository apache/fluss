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

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.LogDecodeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.FileLogProjection;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.ProjectionPushdownCache;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;
import org.apache.fluss.testutils.InternalRowAssert;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.Projection;
import org.apache.fluss.utils.crc.Crc32C;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.crcOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.lastOffsetDeltaOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.TestData.DATA2;
import static org.apache.fluss.record.TestData.DATA2_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA2_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA2_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toByteBuffer;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.client.table.scanner.log.DefaultCompletedFetch}. */
public class DefaultCompletedFetchTest {
    /**
     * Every log batch format: V1 appends a statistics length to the header, and V2 inserts a leader
     * epoch before the CRC, moving the CRC and the offset delta.
     */
    private static final byte[] MAGICS = {
        LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2
    };

    private LogScannerStatus logScannerStatus;
    private @TempDir File tempDir;
    private TableInfo tableInfo;
    private RowType rowType;
    private TestingSchemaGetter testingSchemaGetter;

    @BeforeEach
    void beforeEach() {
        testingSchemaGetter =
                new TestingSchemaGetter(
                        new SchemaInfo(DATA2_TABLE_INFO.getSchema(), DEFAULT_SCHEMA_ID));
        tableInfo = DATA2_TABLE_INFO;
        rowType = DATA2_ROW_TYPE;
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 0), 0L);
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 1), 0L);
        scanBuckets.put(new TableBucket(DATA2_TABLE_ID, 2), 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
    }

    @ParameterizedTest
    @MethodSource("typeAndMagic")
    void testFilteredRecordsKeepSelectedRowsAfterFetchIsDrained(LogFormat format, byte magic)
            throws Exception {
        tableInfo =
                TableInfo.of(
                        DATA2_TABLE_PATH,
                        DATA2_TABLE_ID,
                        DEFAULT_SCHEMA_ID,
                        TableDescriptor.builder()
                                .schema(DATA2_TABLE_INFO.getSchema())
                                .distributedBy(3)
                                .logFormat(format)
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        0L,
                        0L);
        TableBucket bucket = new TableBucket(DATA2_TABLE_ID, 0);
        DefaultCompletedFetch fetch =
                makeCompletedFetch(
                        bucket,
                        FetchLogResultForBucket.records(
                                bucket,
                                createMemoryLogRecords(DATA2, format, magic),
                                10L,
                                -1L,
                                -1L),
                        0L);
        List<ScanRecord> selected = fetch.fetchRecords(10, record -> record.logOffset() % 2 == 0);
        assertThat(selected).extracting(ScanRecord::logOffset).containsExactly(0L, 2L, 4L, 6L, 8L);
        List<ScanRecord> expected =
                makeCompletedFetch(
                                bucket,
                                FetchLogResultForBucket.records(
                                        bucket,
                                        createMemoryLogRecords(DATA2, format, magic),
                                        10L,
                                        -1L,
                                        -1L),
                                0L)
                        .fetchRecords(10);
        fetch.drain();
        for (int i = 0; i < selected.size(); i++) {
            assertThat(selected.get(i)).isEqualTo(expected.get(i * 2));
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testSimple(byte recordBatchMagic) throws Exception {
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket0 =
                FetchLogResultForBucket.records(
                        tb,
                        createMemoryLogRecords(DATA2, LogFormat.ARROW, recordBatchMagic),
                        10L,
                        -1L,
                        -1L);
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

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testNegativeFetchCount(byte recordBatchMagic) throws Exception {
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket0 =
                FetchLogResultForBucket.records(
                        tb,
                        createMemoryLogRecords(DATA2, LogFormat.ARROW, recordBatchMagic),
                        10L,
                        -1L,
                        -1L);
        DefaultCompletedFetch defaultCompletedFetch =
                makeCompletedFetch(tb, resultForBucket0, fetchOffset);
        List<ScanRecord> scanRecords = defaultCompletedFetch.fetchRecords(-10);
        assertThat(scanRecords.size()).isEqualTo(0);
    }

    @ParameterizedTest
    @MethodSource("decodeFailures")
    void testDecodeFailureReportsTheOffsetToSkipTo(
            byte magic,
            Corruption corruption,
            boolean checkCrcs,
            @Nullable Projection projection,
            boolean arrowBatches,
            OptionalLong expectedNextOffset,
            @Nullable Class<? extends Exception> expectedCause)
            throws Exception {
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, 0);
        DefaultCompletedFetch fetch =
                makeCompletedFetch(
                        tb,
                        FetchLogResultForBucket.records(
                                tb, corruptBetweenHealthyBatches(corruption, magic), 30L, -1L, -1L),
                        0L,
                        projection,
                        checkCrcs);
        try {
            assertThat(poll(fetch, arrowBatches)).isEqualTo(10);
            for (int i = 0; i < 3; i++) {
                assertThatThrownBy(() -> poll(fetch, arrowBatches))
                        .isInstanceOfSatisfying(
                                LogDecodeException.class,
                                e -> {
                                    assertThat(e.getTableBucket()).isEqualTo(tb);
                                    assertThat(e.getOffset()).isEqualTo(10L);
                                    assertThat(e.getNextOffset()).isEqualTo(expectedNextOffset);
                                    if (expectedCause != null) {
                                        assertThat(e).hasCauseInstanceOf(expectedCause);
                                    }
                                })
                        .hasMessageContaining(
                                expectedNextOffset.isPresent() ? "offset 20" : "unknown");
            }
        } finally {
            fetch.drain();
        }
    }

    @ParameterizedTest
    @MethodSource("untrustedBatchEnds")
    void testDecodeFailureDoesNotReportAnInvalidOffsetToSkipTo(
            byte magic, long baseOffset, int lastOffsetDelta, boolean arrowBatches)
            throws Exception {
        // Garbage records with a valid CRC, so only the offsets make the batch end untrusted. The
        // CRC covers the offset delta but not the base offset.
        byte[] corrupt = arrowBatch(0L, magic);
        Arrays.fill(corrupt, recordBatchHeaderSize(magic), corrupt.length, (byte) 0xff);
        MemorySegment segment = MemorySegment.wrap(corrupt);
        segment.putInt(lastOffsetDeltaOffset(magic), lastOffsetDelta);
        updateCrc(corrupt, magic);
        segment.putLong(BASE_OFFSET_OFFSET, baseOffset);

        TableBucket tb = new TableBucket(DATA2_TABLE_ID, 0);
        DefaultCompletedFetch fetch =
                makeCompletedFetch(
                        tb,
                        FetchLogResultForBucket.records(
                                tb, MemoryLogRecords.pointToBytes(corrupt), 30L, -1L, -1L),
                        0L,
                        null,
                        true);
        try {
            assertThatThrownBy(() -> poll(fetch, arrowBatches))
                    .isInstanceOfSatisfying(
                            LogDecodeException.class,
                            e -> {
                                assertThat(e.getOffset()).isEqualTo(baseOffset);
                                assertThat(e.getNextOffset()).isEmpty();
                            })
                    .hasMessageContaining("unknown");
        } finally {
            fetch.drain();
        }
    }

    @Test
    void testDecodeFailureAfterPartOfTheBatchWasReturned() throws Exception {
        byte magic = LOG_MAGIC_VALUE_V0;
        // A changelog batch at offsets 0-9 whose second change type is invalid, with a valid CRC.
        MemoryLogRecords records =
                createRecordsWithoutBaseLogOffset(
                        rowType,
                        DEFAULT_SCHEMA_ID,
                        0L,
                        1000L,
                        magic,
                        DATA2,
                        LogFormat.ARROW,
                        false);
        byte[] bytes = toBytes(records);
        bytes[recordBatchHeaderSize(magic) + 1] = Byte.MAX_VALUE;
        updateCrc(bytes, magic);

        TableBucket tb = new TableBucket(DATA2_TABLE_ID, 0);
        DefaultCompletedFetch fetch =
                makeCompletedFetch(
                        tb,
                        FetchLogResultForBucket.records(
                                tb, MemoryLogRecords.pointToBytes(bytes), 10L, -1L, -1L),
                        0L);
        try {
            List<ScanRecord> returned = fetch.fetchRecords(100);
            assertThat(returned).hasSize(1);
            assertThat(returned.get(0).logOffset()).isEqualTo(0L);
            // The failure is attributed to the whole batch: skipping it gives up offsets 1-9.
            assertThatThrownBy(() -> fetch.fetchRecords(100))
                    .isInstanceOfSatisfying(
                            LogDecodeException.class,
                            e -> {
                                assertThat(e.getOffset()).isEqualTo(0L);
                                assertThat(e.getNextOffset()).hasValue(10L);
                            });
        } finally {
            fetch.drain();
        }
    }

    @Test
    void testNoRecordsInFetch() {
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket0 =
                FetchLogResultForBucket.records(tb, MemoryLogRecords.EMPTY, 0L, -1L, -1L);
        DefaultCompletedFetch defaultCompletedFetch =
                makeCompletedFetch(tb, resultForBucket0, fetchOffset);
        List<ScanRecord> scanRecords = defaultCompletedFetch.fetchRecords(10);
        assertThat(scanRecords.size()).isEqualTo(0);
    }

    @ParameterizedTest
    @MethodSource("typeAndMagic")
    void testProjection(LogFormat logFormat, byte magic) throws Exception {
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
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        Projection projection = Projection.of(new int[] {0, 2});
        MemoryLogRecords memoryLogRecords;
        if (logFormat == LogFormat.ARROW) {
            memoryLogRecords = genRecordsWithProjection(DATA2, projection, magic);
        } else {
            memoryLogRecords = createMemoryLogRecords(DATA2, LogFormat.INDEXED, magic);
        }
        FetchLogResultForBucket resultForBucket0 =
                FetchLogResultForBucket.records(tb, memoryLogRecords, 10L, -1L, -1L);
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

    @Test
    void testComplexTypeFetch() throws Exception {
        List<Object[]> complexData =
                Arrays.asList(
                        new Object[] {
                            1,
                            new Object[] {fromString("a"), fromString("b")},
                            new Object[] {
                                new GenericArray(new int[] {1, 2}),
                                new GenericArray(new int[] {3, 4})
                            },
                            new Object[] {
                                10, new Object[] {20, fromString("nested")}, fromString("row1")
                            },
                            GenericMap.of(1, fromString("one"), 2, fromString("two")),
                            GenericMap.of(
                                    fromString("k1"),
                                    GenericMap.of(10, fromString("v1"), 20, fromString("v2"))),
                            GenericMap.of(
                                    fromString("arr1"),
                                    new GenericArray(new int[] {1, 2}),
                                    fromString("arr2"),
                                    new GenericArray(new int[] {3, 4, 5}))
                        },
                        new Object[] {
                            2,
                            new Object[] {fromString("c"), null},
                            new Object[] {null, new GenericArray(new int[] {3, 4})},
                            new Object[] {
                                30, new Object[] {40, fromString("test")}, fromString("row2")
                            },
                            GenericMap.of(3, null, 4, fromString("four")),
                            GenericMap.of(fromString("k2"), GenericMap.of(30, fromString("v3"))),
                            GenericMap.of(fromString("arr3"), new GenericArray(new int[] {6}))
                        },
                        new Object[] {
                            3,
                            new Object[] {fromString("e"), fromString("f")},
                            new Object[] {
                                new GenericArray(new int[] {5, 6, 7}),
                                new GenericArray(new int[] {8})
                            },
                            new Object[] {
                                50, new Object[] {60, fromString("value")}, fromString("row3")
                            },
                            GenericMap.of(5, fromString("five")),
                            GenericMap.of(
                                    fromString("k3"),
                                    GenericMap.of(50, fromString("v5"), 60, fromString("v6"))),
                            GenericMap.of(
                                    fromString("arr4"),
                                    new GenericArray(new int[] {7, 8}),
                                    fromString("arr5"),
                                    new GenericArray(new int[] {9}))
                        });
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.ARRAY(DataTypes.STRING()))
                        .column("c", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))
                        .column(
                                "d",
                                DataTypes.ROW(
                                        DataTypes.INT(),
                                        DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()),
                                        DataTypes.STRING()))
                        .column("e", DataTypes.MAP(DataTypes.INT().copy(false), DataTypes.STRING()))
                        .column(
                                "f",
                                DataTypes.MAP(
                                        DataTypes.STRING().copy(false),
                                        DataTypes.MAP(
                                                DataTypes.INT().copy(false), DataTypes.STRING())))
                        .column(
                                "g",
                                DataTypes.MAP(
                                        DataTypes.STRING().copy(false),
                                        DataTypes.ARRAY(DataTypes.INT())))
                        .build();
        TableInfo tableInfo =
                TableInfo.of(
                        DATA2_TABLE_PATH,
                        DATA2_TABLE_ID,
                        DEFAULT_SCHEMA_ID,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(3)
                                .logFormat(LogFormat.ARROW)
                                .build(),
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        long fetchOffset = 0L;
        int bucketId = 0;
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket =
                FetchLogResultForBucket.records(
                        tb,
                        createRecordsWithoutBaseLogOffset(
                                schema.getRowType(),
                                DEFAULT_SCHEMA_ID,
                                0L,
                                1000L,
                                LOG_MAGIC_VALUE_V0,
                                complexData,
                                LogFormat.ARROW),
                        3L,
                        -1L,
                        -1L);
        DefaultCompletedFetch defaultCompletedFetch =
                new DefaultCompletedFetch(
                        tb,
                        DATA2_TABLE_PATH,
                        resultForBucket,
                        LogRecordReadContext.createReadContext(
                                tableInfo,
                                false,
                                LogRecordReadContext.SchemaResolution.DYNAMIC,
                                null,
                                new TestingSchemaGetter(
                                        tableInfo.getSchemaId(), tableInfo.getSchema()),
                                new ChunkedAllocationManager.ChunkedFactory()),
                        logScannerStatus,
                        true,
                        fetchOffset,
                        null);
        List<ScanRecord> scanRecords = defaultCompletedFetch.fetchRecords(3);
        // close the read context to release arrow root resource,
        // this is important to test complex types
        defaultCompletedFetch.readContext.close();
        assertThat(scanRecords.size()).isEqualTo(3);

        List<GenericRow> expectedRows = new ArrayList<>();
        for (Object[] data : complexData) {
            Object[] rowData = (Object[]) data[3];
            Object[] nestedRowData = (Object[]) rowData[1];
            GenericRow nestedRow = GenericRow.of(nestedRowData[0], nestedRowData[1]);
            GenericRow row = GenericRow.of(rowData[0], nestedRow, rowData[2]);
            expectedRows.add(
                    GenericRow.of(
                            data[0],
                            new GenericArray((Object[]) data[1]),
                            new GenericArray((Object[]) data[2]),
                            row,
                            data[4],
                            data[5],
                            data[6]));
        }

        for (int i = 0; i < scanRecords.size(); i++) {
            ScanRecord record = scanRecords.get(i);
            assertThat(record.logOffset()).isEqualTo(i);
            InternalRowAssert.assertThatRow(record.getRow())
                    .withSchema(schema.getRowType())
                    .isEqualTo(expectedRows.get(i));
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
        return makeCompletedFetch(tableBucket, resultForBucket, offset, projection, true);
    }

    private DefaultCompletedFetch makeCompletedFetch(
            TableBucket tableBucket,
            FetchLogResultForBucket resultForBucket,
            long offset,
            Projection projection,
            boolean checkCrcs) {
        return new DefaultCompletedFetch(
                tableBucket,
                DATA2_TABLE_PATH,
                resultForBucket,
                LogRecordReadContext.createReadContext(
                        tableInfo,
                        false,
                        LogRecordReadContext.SchemaResolution.DYNAMIC,
                        projection,
                        new TestingSchemaGetter(tableInfo.getSchemaId(), tableInfo.getSchema()),
                        new ChunkedAllocationManager.ChunkedFactory()),
                logScannerStatus,
                checkCrcs,
                offset,
                null);
    }

    private static Collection<Arguments> decodeFailures() {
        Projection allColumns = Projection.of(new int[] {0, 1, 2});
        OptionalLong skipTo20 = OptionalLong.of(20L);
        OptionalLong unknown = OptionalLong.empty();
        Class<CorruptMessageException> corrupt = CorruptMessageException.class;
        List<Arguments> params = new ArrayList<>();
        for (byte magic : MAGICS) {
            for (boolean arrowBatches : new boolean[] {false, true}) {
                // The header is trusted unverified when the CRC check is disabled or the batch is
                // projected, and only after the CRC check otherwise.
                params.add(
                        Arguments.arguments(
                                magic,
                                Corruption.RECORDS,
                                false,
                                null,
                                arrowBatches,
                                skipTo20,
                                null));
                params.add(
                        Arguments.arguments(
                                magic,
                                Corruption.RECORDS,
                                true,
                                allColumns,
                                arrowBatches,
                                skipTo20,
                                null));
                params.add(
                        Arguments.arguments(
                                magic,
                                Corruption.RECORDS,
                                true,
                                null,
                                arrowBatches,
                                unknown,
                                corrupt));
                params.add(
                        Arguments.arguments(
                                magic,
                                Corruption.RECORDS_WITH_VALID_CRC,
                                true,
                                null,
                                arrowBatches,
                                skipTo20,
                                null));
                params.add(
                        Arguments.arguments(
                                magic,
                                Corruption.OFFSET_DELTA,
                                false,
                                null,
                                arrowBatches,
                                unknown,
                                null));
                params.add(
                        Arguments.arguments(
                                magic, Corruption.MAGIC, false, null, arrowBatches, unknown, null));
                // An incomplete header is rejected however the CRC check is configured.
                params.add(
                        Arguments.arguments(
                                magic,
                                Corruption.INCOMPLETE_HEADER,
                                false,
                                null,
                                arrowBatches,
                                unknown,
                                corrupt));
                params.add(
                        Arguments.arguments(
                                magic,
                                Corruption.INCOMPLETE_HEADER,
                                true,
                                allColumns,
                                arrowBatches,
                                unknown,
                                corrupt));
                params.add(
                        Arguments.arguments(
                                magic,
                                Corruption.INCOMPLETE_HEADER,
                                true,
                                null,
                                arrowBatches,
                                unknown,
                                corrupt));
            }
        }
        return params;
    }

    private static Collection<Arguments> untrustedBatchEnds() {
        List<Arguments> params = new ArrayList<>();
        for (byte magic : MAGICS) {
            for (boolean arrowBatches : new boolean[] {false, true}) {
                // A negative base offset: resuming at the end -2 would restart from the earliest
                // offset instead of skipping the batch.
                params.add(Arguments.arguments(magic, -3L, 0, arrowBatches));
                params.add(Arguments.arguments(magic, Long.MIN_VALUE, 2, arrowBatches));
                // The end underflows and wraps to Long.MAX_VALUE.
                params.add(Arguments.arguments(magic, Long.MIN_VALUE, -2, arrowBatches));
                // The end overflows and wraps to Long.MIN_VALUE.
                params.add(Arguments.arguments(magic, Long.MAX_VALUE, 0, arrowBatches));
            }
        }
        return params;
    }

    /** How {@link #corruptBetweenHealthyBatches} corrupts the batch at offset 10. */
    private enum Corruption {
        /** Garbage records behind an intact header, with a stale CRC. */
        RECORDS,
        /** Garbage records behind an intact header, with a matching CRC. */
        RECORDS_WITH_VALID_CRC,
        /** Garbage records behind an offset delta of -1, putting the batch end at its start. */
        OFFSET_DELTA,
        /** An unsupported magic. */
        MAGIC,
        /** One byte short of its header, whose last byte would be read from the next batch. */
        INCOMPLETE_HEADER
    }

    /**
     * Returns Arrow batches at offsets 0-9 and 20-29 with a corrupt batch at offset 10 between
     * them.
     */
    private MemoryLogRecords corruptBetweenHealthyBatches(Corruption corruption, byte magic)
            throws Exception {
        byte[] corrupt;
        if (corruption == Corruption.INCOMPLETE_HEADER) {
            corrupt = new byte[recordBatchHeaderSize(magic) - 1];
            MemorySegment segment = MemorySegment.wrap(corrupt);
            segment.putLong(BASE_OFFSET_OFFSET, 10L);
            segment.putInt(LENGTH_OFFSET, corrupt.length - LOG_OVERHEAD);
            corrupt[MAGIC_OFFSET] = magic;
        } else {
            corrupt = arrowBatch(10L, magic);
            if (corruption == Corruption.MAGIC) {
                corrupt[MAGIC_OFFSET] = Byte.MAX_VALUE;
            } else {
                Arrays.fill(corrupt, recordBatchHeaderSize(magic), corrupt.length, (byte) 0xff);
            }
            if (corruption == Corruption.OFFSET_DELTA) {
                // An all-ones int is -1 in either byte order.
                int deltaStart = lastOffsetDeltaOffset(magic);
                Arrays.fill(corrupt, deltaStart, deltaStart + Integer.BYTES, (byte) 0xff);
            }
            if (corruption == Corruption.RECORDS_WITH_VALID_CRC) {
                updateCrc(corrupt, magic);
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] batch : new byte[][] {arrowBatch(0L, magic), corrupt, arrowBatch(20L, magic)}) {
            out.write(batch, 0, batch.length);
        }
        return MemoryLogRecords.pointToBytes(out.toByteArray());
    }

    /** Returns an append-only Arrow batch, as the Arrow batch path only loads those. */
    private byte[] arrowBatch(long baseOffset, byte magic) throws Exception {
        return toBytes(
                createRecordsWithoutBaseLogOffset(
                        rowType,
                        DEFAULT_SCHEMA_ID,
                        baseOffset,
                        1000L,
                        magic,
                        DATA2,
                        LogFormat.ARROW,
                        true));
    }

    /** Recomputes the CRC of a single batch so it passes its CRC check. */
    private static void updateCrc(byte[] batch, byte magic) {
        int crcStart = schemaIdOffset(magic);
        long crc = Crc32C.compute(batch, crcStart, batch.length - crcStart);
        MemorySegment.wrap(batch).putInt(crcOffset(magic), (int) crc);
    }

    private static byte[] toBytes(MemoryLogRecords records) {
        byte[] bytes = new byte[records.sizeInBytes()];
        records.getMemorySegment().get(records.getPosition(), bytes, 0, bytes.length);
        return bytes;
    }

    /** Polls the fetch through the row or Arrow path and returns the number of rows read. */
    private static int poll(DefaultCompletedFetch fetch, boolean arrowBatches) {
        if (!arrowBatches) {
            return fetch.fetchRecords(100).size();
        }
        int rows = 0;
        for (ArrowBatchData batch : fetch.fetchArrowBatches(100)) {
            rows += batch.getRecordCount();
            batch.close();
        }
        return rows;
    }

    private static Collection<Arguments> typeAndMagic() {
        List<Arguments> params = new ArrayList<>();
        params.add(Arguments.arguments(LogFormat.ARROW, LOG_MAGIC_VALUE_V1));
        params.add(Arguments.arguments(LogFormat.INDEXED, LOG_MAGIC_VALUE_V1));
        params.add(Arguments.arguments(LogFormat.ARROW, LOG_MAGIC_VALUE_V0));
        params.add(Arguments.arguments(LogFormat.INDEXED, LOG_MAGIC_VALUE_V0));
        return params;
    }

    private MemoryLogRecords createMemoryLogRecords(
            List<Object[]> objects, LogFormat logFormat, byte magic) throws Exception {
        return createRecordsWithoutBaseLogOffset(
                rowType, DEFAULT_SCHEMA_ID, 0L, 1000L, magic, objects, logFormat);
    }

    private MemoryLogRecords genRecordsWithProjection(
            List<Object[]> objects, Projection projection, byte magic) throws Exception {
        File logFile = FlussPaths.logFile(tempDir, 0L);
        FileLogRecords fileLogRecords = FileLogRecords.open(logFile, false, 1024 * 1024, false);
        fileLogRecords.append(
                createRecordsWithoutBaseLogOffset(
                        rowType, DEFAULT_SCHEMA_ID, 0L, 1000L, magic, objects, LogFormat.ARROW));
        fileLogRecords.flush();

        FileLogProjection fileLogProjection = new FileLogProjection(new ProjectionPushdownCache());
        fileLogProjection.setCurrentProjection(
                DATA2_TABLE_ID,
                testingSchemaGetter,
                DEFAULT_COMPRESSION,
                projection.getProjectionInOrder());
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
