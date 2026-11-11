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
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.exception.LogDecodeException;
import org.apache.fluss.exception.TableNotExistException;
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
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.OutOfMemoryException;
import org.apache.fluss.testutils.InternalRowAssert;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.Projection;
import org.apache.fluss.utils.crc.Crc32C;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

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
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
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
    private final List<DefaultCompletedFetch> fetches = new ArrayList<>();
    private final ChunkedAllocationManager.ChunkedFactory chunkedFactory =
            new ChunkedAllocationManager.ChunkedFactory();

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

    @AfterEach
    void closeFetches() {
        try {
            for (DefaultCompletedFetch fetch : fetches) {
                fetch.drain();
                fetch.readContext.close();
            }
        } finally {
            chunkedFactory.close();
        }
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
    @CsvSource({
        "0, RECORDS, false, false, 10, false, 10, 20, ",
        "1, RECORDS_WITH_VALID_CRC, false, false, 10, false, 10, 20, 20",
        "2, RECORDS_WITH_VALID_CRC, true, false, 10, false, 10, 20, 20",
        "2, ARROW_ROW_COUNT, true, false, 10, false, 10, 20, 20",
        "2, RECORDS_WITH_VALID_CRC, true, false, 10, false, 10, 19, ",
        "2, RECORDS, true, false, 10, false, 10, 20, ",
        "1, RECORDS, true, true, 10, false, 10, 20, 20",
        "1, RECORDS, true, true, 10, false, 10, 30, ",
        "1, RECORDS_WITH_VALID_CRC, true, true, 10, false, 10, 19, ",
        "1, RECORDS, true, true, 0, true, 10, , ",
        "1, RECORDS_WITH_VALID_CRC, true, true, 0, true, 10, , 20",
        "1, RECORDS_WITH_VALID_CRC, false, false, 0, true, 10, , 20",
        "1, RECORDS_WITH_VALID_CRC, true, false, 10, false, 10, , ",
        "1, RECORDS_WITH_VALID_CRC, true, false, 10, false, 100, , ",
        "1, RECORDS_WITH_VALID_CRC, true, false, 0, true, 11, , ",
        "1, RECORDS_WITH_VALID_CRC, true, false, 20, false, 10, 20, ",
        "0, INCOMPLETE_HEADER, false, false, 10, false, 10, , ",
        "2, INCOMPLETE_HEADER, false, true, 10, false, 10, 20, ",
        "0, MAGIC, false, false, 10, false, 10, 20, "
    })
    void testDecodeFailureReportsVerifiedNextOffset(
            byte magic,
            Corruption corruption,
            boolean checkCrcs,
            boolean projected,
            long fetchOffset,
            boolean hasPrecedingBatch,
            long failedBaseOffset,
            @Nullable Long followingOffset,
            @Nullable Long expectedNextOffset)
            throws Exception {
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, 0);
        Projection projection = projected ? Projection.of(new int[] {0, 1, 2}) : null;
        MemoryLogRecords data = corruptBatch(corruption, magic, failedBaseOffset, followingOffset);
        if (hasPrecedingBatch) {
            data = concatBatches(arrowBatch(0L, magic), toBytes(data));
        }
        for (boolean arrowBatches : new boolean[] {false, true}) {
            DefaultCompletedFetch fetch =
                    makeCompletedFetch(
                            tb,
                            FetchLogResultForBucket.records(tb, data, 40L, -1L, -1L),
                            fetchOffset,
                            projection,
                            checkCrcs);
            if (hasPrecedingBatch) {
                readFetch(fetch, arrowBatches);
            }
            assertThatThrownBy(() -> readFetch(fetch, arrowBatches))
                    .isInstanceOfSatisfying(
                            LogDecodeException.class,
                            e -> {
                                assertThat(e.getTableBucket()).isEqualTo(tb);
                                assertThat(e.getOffset()).isEqualTo(failedBaseOffset);
                                if (expectedNextOffset == null) {
                                    assertThat(e.getNextOffset()).isEmpty();
                                } else {
                                    assertThat(e.getNextOffset()).hasValue(expectedNextOffset);
                                }
                            });
        }
    }

    @ParameterizedTest
    @MethodSource("invalidBatchOffsetRanges")
    void testInvalidBatchOffsetRangeFailsBeforeReadingRecords(
            long baseOffset, int lastOffsetDelta, int recordCount, boolean arrowBatches)
            throws Exception {
        byte magic = LOG_MAGIC_VALUE_V2;
        byte[] batch = arrowBatch(0L, magic);
        MemorySegment segment = MemorySegment.wrap(batch);
        segment.putLong(BASE_OFFSET_OFFSET, baseOffset);
        segment.putInt(lastOffsetDeltaOffset(magic), lastOffsetDelta);
        segment.putInt(recordsCountOffset(magic), recordCount);
        updateCrc(batch, magic);

        TableBucket tb = new TableBucket(DATA2_TABLE_ID, 0);
        DefaultCompletedFetch fetch =
                makeCompletedFetch(
                        tb,
                        FetchLogResultForBucket.records(
                                tb, MemoryLogRecords.pointToBytes(batch), 30L, -1L, -1L),
                        0L,
                        null,
                        false);
        assertThatThrownBy(() -> readFetch(fetch, arrowBatches))
                .isInstanceOfSatisfying(
                        LogDecodeException.class,
                        e -> {
                            assertThat(e.getOffset()).isEqualTo(baseOffset);
                            assertThat(e.getNextOffset()).isEmpty();
                        })
                .hasRootCauseMessage("Invalid log batch offset range at " + baseOffset);
    }

    @Test
    void testDecodeFailureAfterPartOfTheBatchWasReturned() throws Exception {
        byte magic = LOG_MAGIC_VALUE_V0;
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
                                tb, concatBatches(bytes, arrowBatch(10L, magic)), 20L, -1L, -1L),
                        0L);
        assertThat(fetch.fetchRecords(100)).extracting(ScanRecord::logOffset).containsExactly(0L);
        assertThatThrownBy(() -> fetch.fetchRecords(100))
                .isInstanceOfSatisfying(
                        LogDecodeException.class,
                        e -> {
                            assertThat(e.getOffset()).isEqualTo(0L);
                            assertThat(e.getNextOffset()).hasValue(10L);
                        });
    }

    @ParameterizedTest
    @MethodSource("schemaLookupFailures")
    void testSchemaLookupFailureRemainsAFetchException(
            RuntimeException failure, boolean arrowBatches) throws Exception {
        int missingSchema = DEFAULT_SCHEMA_ID + 1;
        testingSchemaGetter =
                new TestingSchemaGetter(DEFAULT_SCHEMA_ID, tableInfo.getSchema()) {
                    @Override
                    public Schema getSchema(int schemaId) {
                        if (schemaId == missingSchema) {
                            throw failure;
                        }
                        return super.getSchema(schemaId);
                    }
                };
        byte magic = LOG_MAGIC_VALUE_V0;
        byte[] batch = arrowBatch(0L, magic);
        MemorySegment.wrap(batch).putShort(schemaIdOffset(magic), (short) missingSchema);
        updateCrc(batch, magic);

        TableBucket bucket = new TableBucket(DATA2_TABLE_ID, 0);
        DefaultCompletedFetch fetch =
                makeCompletedFetch(
                        bucket,
                        FetchLogResultForBucket.records(
                                bucket, MemoryLogRecords.pointToBytes(batch), 10L, -1L, -1L),
                        0L);
        assertThatThrownBy(() -> readFetch(fetch, arrowBatches))
                .isExactlyInstanceOf(FetchException.class)
                .hasCause(failure);
        assertThat(fetch.nextFetchOffset()).isZero();
        assertThatThrownBy(() -> readFetch(fetch, arrowBatches))
                .isExactlyInstanceOf(FetchException.class)
                .hasCause(failure);
    }

    private static Collection<Arguments> schemaLookupFailures() {
        RuntimeException timeout = new RuntimeException(new TimeoutException("schema lookup"));
        return Arrays.asList(
                Arguments.of(timeout, false),
                Arguments.of(timeout, true),
                Arguments.of(new RuntimeException(new InterruptedException("schema lookup")), true),
                Arguments.of(new TableNotExistException("table was dropped"), false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testArrowAllocationFailureRemainsAFetchException(boolean arrowBatches) throws Exception {
        byte magic = LOG_MAGIC_VALUE_V0;
        int batchSize = DATA2.size();
        MemoryLogRecords records =
                concatBatches(arrowBatch(0L, magic), arrowBatch(batchSize, magic));
        TableBucket bucket = new TableBucket(DATA2_TABLE_ID, 0);
        DefaultCompletedFetch fetch =
                makeCompletedFetch(
                        bucket,
                        FetchLogResultForBucket.records(bucket, records, 2L * batchSize, -1L, -1L),
                        0L);

        if (arrowBatches) {
            try (ArrowBatchData firstBatch = fetch.fetchArrowBatches(batchSize).get(0)) {
                assertThat(firstBatch.getRecordCount()).isEqualTo(batchSize);
                firstBatch.getVectorSchemaRoot().getVector(0).getAllocator().setLimit(0L);
            }
        } else {
            assertThat(fetch.fetchRecords(batchSize)).hasSize(batchSize);
            fetch.readContext.getBufferAllocator().setLimit(0L);
        }

        assertThatThrownBy(() -> readFetch(fetch, arrowBatches))
                .isExactlyInstanceOf(FetchException.class)
                .hasRootCauseInstanceOf(
                        arrowBatches
                                ? org.apache.arrow.memory.OutOfMemoryException.class
                                : OutOfMemoryException.class);
        assertThat(fetch.nextFetchOffset()).isEqualTo(batchSize);
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
        DefaultCompletedFetch fetch =
                new DefaultCompletedFetch(
                        tableBucket,
                        DATA2_TABLE_PATH,
                        resultForBucket,
                        LogRecordReadContext.createReadContext(
                                tableInfo,
                                false,
                                LogRecordReadContext.SchemaResolution.DYNAMIC,
                                projection,
                                testingSchemaGetter,
                                chunkedFactory),
                        logScannerStatus,
                        checkCrcs,
                        offset,
                        null);
        fetches.add(fetch);
        return fetch;
    }

    private static Collection<Arguments> invalidBatchOffsetRanges() {
        int recordCount = DATA2.size();
        return Arrays.asList(
                Arguments.of(-3L, 0, recordCount, false),
                Arguments.of(1L, -1, recordCount, true),
                Arguments.of(Long.MAX_VALUE - 1, 0, recordCount, false),
                Arguments.of(Long.MAX_VALUE, 0, recordCount, true),
                Arguments.of(1L, 0, recordCount, false),
                Arguments.of(1L, 0, recordCount, true),
                Arguments.of(1L, 0, -1, true));
    }

    private enum Corruption {
        RECORDS,
        RECORDS_WITH_VALID_CRC,
        ARROW_ROW_COUNT,
        MAGIC,
        INCOMPLETE_HEADER
    }

    private MemoryLogRecords corruptBatch(
            Corruption corruption, byte magic, long baseOffset, @Nullable Long followingOffset)
            throws Exception {
        byte[] corrupt;
        if (corruption == Corruption.INCOMPLETE_HEADER) {
            corrupt = new byte[recordBatchHeaderSize(magic) - 1];
            MemorySegment segment = MemorySegment.wrap(corrupt);
            segment.putInt(LENGTH_OFFSET, corrupt.length - LOG_OVERHEAD);
            corrupt[MAGIC_OFFSET] = magic;
        } else {
            corrupt = arrowBatch(10L, magic);
            if (corruption == Corruption.MAGIC) {
                corrupt[MAGIC_OFFSET] = Byte.MAX_VALUE;
            } else if (corruption == Corruption.ARROW_ROW_COUNT) {
                MemorySegment.wrap(corrupt).putInt(recordsCountOffset(magic), DATA2.size() - 1);
                updateCrc(corrupt, magic);
            } else {
                Arrays.fill(corrupt, recordBatchHeaderSize(magic), corrupt.length, (byte) 0xff);
                if (corruption == Corruption.RECORDS_WITH_VALID_CRC) {
                    updateCrc(corrupt, magic);
                }
            }
        }
        MemorySegment.wrap(corrupt).putLong(BASE_OFFSET_OFFSET, baseOffset);
        if (followingOffset == null) {
            return MemoryLogRecords.pointToBytes(corrupt);
        }
        return concatBatches(corrupt, arrowBatch(followingOffset, magic));
    }

    private static MemoryLogRecords concatBatches(byte[] first, byte[] second) {
        byte[] data = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, data, first.length, second.length);
        return MemoryLogRecords.pointToBytes(data);
    }

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

    private static void readFetch(DefaultCompletedFetch fetch, boolean arrowBatches) {
        if (arrowBatches) {
            fetch.fetchArrowBatches(100).forEach(ArrowBatchData::close);
        } else {
            fetch.fetchRecords(100);
        }
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
