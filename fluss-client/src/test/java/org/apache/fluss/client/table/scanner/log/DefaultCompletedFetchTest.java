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
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
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
import org.apache.fluss.testutils.InternalRowAssert;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.Projection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.TestData.DATA2;
import static org.apache.fluss.record.TestData.DATA2_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA2_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA2_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toByteBuffer;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.client.table.scanner.log.DefaultCompletedFetch}. */
public class DefaultCompletedFetchTest {
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
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testSimple(byte recordBatchMagic) throws Exception {
        long fetchOffset = 0L;
        int bucketId = 0; // records for 0-10.
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket0 =
                new FetchLogResultForBucket(
                        tb, createMemoryLogRecords(DATA2, LogFormat.ARROW, recordBatchMagic), 10L);
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
                new FetchLogResultForBucket(
                        tb, createMemoryLogRecords(DATA2, LogFormat.ARROW, recordBatchMagic), 10L);
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
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        long fetchOffset = 0L;
        int bucketId = 0;
        TableBucket tb = new TableBucket(DATA2_TABLE_ID, bucketId);
        FetchLogResultForBucket resultForBucket =
                new FetchLogResultForBucket(
                        tb,
                        createRecordsWithoutBaseLogOffset(
                                schema.getRowType(),
                                DEFAULT_SCHEMA_ID,
                                0L,
                                1000L,
                                LOG_MAGIC_VALUE_V0,
                                complexData,
                                LogFormat.ARROW),
                        3L);
        DefaultCompletedFetch defaultCompletedFetch =
                new DefaultCompletedFetch(
                        tb,
                        resultForBucket,
                        LogRecordReadContext.createReadContext(
                                tableInfo,
                                false,
                                null,
                                new TestingSchemaGetter(
                                        tableInfo.getSchemaId(), tableInfo.getSchema())),
                        logScannerStatus,
                        true,
                        fetchOffset);
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
        return new DefaultCompletedFetch(
                tableBucket,
                resultForBucket,
                LogRecordReadContext.createReadContext(
                        tableInfo,
                        false,
                        projection,
                        new TestingSchemaGetter(tableInfo.getSchemaId(), tableInfo.getSchema())),
                logScannerStatus,
                true,
                offset);
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
