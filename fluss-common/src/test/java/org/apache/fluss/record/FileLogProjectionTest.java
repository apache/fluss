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

package org.apache.fluss.record;

import org.apache.fluss.exception.InvalidColumnProjectionException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.V1_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordReadContext.createArrowReadContext;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link FileLogProjection}. */
class FileLogProjectionTest {

    private static final Logger log = LoggerFactory.getLogger(FileLogProjectionTest.class);
    private @TempDir File tempDir;
    private TestingSchemaGetter testingSchemaGetter;

    @BeforeEach
    void beforeEach() {
        testingSchemaGetter = new TestingSchemaGetter(new SchemaInfo(TestData.DATA1_SCHEMA, 1));
        testingSchemaGetter.updateLatestSchemaInfo(new SchemaInfo(TestData.DATA2_SCHEMA, 2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSetCurrentProjection(boolean isProjectedByIds) throws Exception {
        short schemaId = (short) 2;
        FileLogRecords recordsOfData2RowType =
                createFileLogRecords(
                        schemaId,
                        LOG_MAGIC_VALUE_V1,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2,
                        TestData.DATA2);

        ProjectionPushdownCache cache = new ProjectionPushdownCache();
        FileLogProjection projection = new FileLogProjection(cache);
        // get schema during running.
        doProjection(
                1L,
                schemaId,
                projection,
                recordsOfData2RowType,
                new int[] {0, 2},
                isProjectedByIds,
                recordsOfData2RowType.sizeInBytes());
        assertThat(cache.projectionCache.size()).isEqualTo(1);
        FileLogProjection.ProjectionInfo info1 =
                cache.getProjectionInfo(1L, schemaId, new int[] {0, 2}, isProjectedByIds);
        assertThat(info1).isNotNull();
        assertThat(info1.nodesProjection.stream().toArray()).isEqualTo(new int[] {0, 2});
        // a int: [0,1] ; b string: [2,3,4] ; c string: [5,6,7]
        assertThat(info1.buffersProjection.stream().toArray()).isEqualTo(new int[] {0, 1, 5, 6, 7});

        doProjection(
                2L,
                schemaId,
                projection,
                recordsOfData2RowType,
                new int[] {1},
                isProjectedByIds,
                recordsOfData2RowType.sizeInBytes());
        assertThat(cache.projectionCache.size()).isEqualTo(2);
        FileLogProjection.ProjectionInfo info2 =
                cache.getProjectionInfo(2L, schemaId, new int[] {1}, isProjectedByIds);
        assertThat(info2).isNotNull();
        assertThat(info2.nodesProjection.stream().toArray()).isEqualTo(new int[] {1});
        // a int: [0,1] ; b string: [2,3,4] ; c string: [5,6,7]
        assertThat(info2.buffersProjection.stream().toArray()).isEqualTo(new int[] {2, 3, 4});
    }

    @Test
    void testIllegalSetCurrentProjection() throws Exception {
        long tableId = 1L;
        short schemaId = (short) 2;
        FileLogRecords recordsOfData2RowType =
                createFileLogRecords(
                        schemaId,
                        LOG_MAGIC_VALUE_V1,
                        TestData.DATA2_ROW_TYPE,
                        TestData.DATA2,
                        TestData.DATA2);
        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        assertThatThrownBy(
                        () ->
                                doProjection(
                                        tableId,
                                        schemaId,
                                        projection,
                                        recordsOfData2RowType,
                                        new int[] {3},
                                        recordsOfData2RowType.sizeInBytes()))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("Projected fields [3] is out of bound for schema with 3 fields.");

        assertThatThrownBy(
                        () ->
                                doProjection(
                                        tableId,
                                        schemaId,
                                        projection,
                                        recordsOfData2RowType,
                                        new int[] {1, 0},
                                        recordsOfData2RowType.sizeInBytes()))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("The projection indexes should be in field order, but is [1, 0]");

        assertThatThrownBy(
                        () ->
                                doProjection(
                                        tableId,
                                        schemaId,
                                        projection,
                                        recordsOfData2RowType,
                                        new int[] {0, 0, 0},
                                        recordsOfData2RowType.sizeInBytes()))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage(
                        "The projection indexes should not contain duplicated fields, but is [0, 0, 0]");
    }

    @Test
    void testProjectionOldDataWithNewSchema() throws Exception {
        // Currently, we only support add column at last.
        short schemaId = 1;
        try (FileLogRecords records =
                createFileLogRecords(
                        schemaId, LOG_MAGIC_VALUE_V1, TestData.DATA1_ROW_TYPE, TestData.DATA1)) {

            ProjectionPushdownCache cache = new ProjectionPushdownCache();
            FileLogProjection projection = new FileLogProjection(cache);
            assertThat(
                            doProjection(
                                    2L,
                                    2,
                                    projection,
                                    records,
                                    new int[] {1},
                                    records.sizeInBytes()))
                    .containsExactly(
                            new Object[] {"a"},
                            new Object[] {"b"},
                            new Object[] {"c"},
                            new Object[] {"d"},
                            new Object[] {"e"},
                            new Object[] {"f"},
                            new Object[] {"g"},
                            new Object[] {"h"},
                            new Object[] {"i"},
                            new Object[] {"j"});

            assertThatThrownBy(
                            () ->
                                    doProjection(
                                            1L,
                                            2,
                                            projection,
                                            records,
                                            new int[] {0, 2},
                                            records.sizeInBytes()))
                    .isInstanceOf(InvalidColumnProjectionException.class)
                    .hasMessage(
                            "Projected fields [0, 2] is out of bound for schema with 2 fields.");
        }
    }

    static Stream<Arguments> projectedFieldsArgs() {
        return Stream.of(
                Arguments.of(new int[] {0}, LOG_MAGIC_VALUE_V0, (short) 1, true),
                Arguments.arguments(new int[] {1}, LOG_MAGIC_VALUE_V0, (short) 1, true),
                Arguments.arguments(new int[] {0, 1}, LOG_MAGIC_VALUE_V0, (short) 1, true),
                Arguments.of(new int[] {0}, LOG_MAGIC_VALUE_V1, (short) 1, false),
                Arguments.arguments(new int[] {1}, LOG_MAGIC_VALUE_V1, (short) 1, false),
                Arguments.arguments(new int[] {0, 1}, LOG_MAGIC_VALUE_V1, (short) 1, false),
                Arguments.of(new int[] {0}, LOG_MAGIC_VALUE_V0, (short) 2, false),
                Arguments.arguments(new int[] {1}, LOG_MAGIC_VALUE_V0, (short) 2, false),
                Arguments.arguments(new int[] {0, 1}, LOG_MAGIC_VALUE_V0, (short) 2, false),
                Arguments.of(new int[] {0}, LOG_MAGIC_VALUE_V1, (short) 2, true),
                Arguments.arguments(new int[] {1}, LOG_MAGIC_VALUE_V1, (short) 2, true),
                Arguments.arguments(new int[] {0, 1}, LOG_MAGIC_VALUE_V1, (short) 2, true));
    }

    @ParameterizedTest
    @MethodSource("projectedFieldsArgs")
    void testProject(
            int[] projectedFields, byte recordBatchMagic, short schemaId, boolean isProjectedByIds)
            throws Exception {
        testingSchemaGetter.updateLatestSchemaInfo(new SchemaInfo(TestData.DATA1_SCHEMA, schemaId));
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        schemaId,
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        List<Object[]> results =
                doProjection(
                        1L,
                        schemaId,
                        new FileLogProjection(new ProjectionPushdownCache()),
                        fileLogRecords,
                        projectedFields,
                        isProjectedByIds,
                        Integer.MAX_VALUE);
        List<Object[]> allData = new ArrayList<>();
        allData.addAll(TestData.DATA1);
        allData.addAll(TestData.ANOTHER_DATA1);
        List<Object[]> expected = new ArrayList<>();
        assertThat(results.size()).isEqualTo(allData.size());
        for (Object[] data : allData) {
            Object[] objs = new Object[projectedFields.length];
            for (int j = 0; j < projectedFields.length; j++) {
                objs[j] = data[projectedFields[j]];
            }
            expected.add(objs);
        }
        assertEquals(results, expected);
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testIllegalByteOrder(byte recordBatchMagic) throws Exception {
        final FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        // overwrite the wrong decoding byte order endian
        projection.getLogHeaderBuffer().order(ByteOrder.BIG_ENDIAN);
        assertThatThrownBy(
                        () ->
                                doProjection(
                                        projection,
                                        fileLogRecords,
                                        new int[] {0, 1},
                                        Integer.MAX_VALUE))
                .isInstanceOf(SchemaNotExistException.class);

        // The BIG_ENDIAN of schema will be read as 256 wronly. In this case, we use 256 to skip it.
        short schemaId = 256;
        testingSchemaGetter.updateLatestSchemaInfo(new SchemaInfo(TestData.DATA2_SCHEMA, schemaId));
        final FileLogRecords fileLogRecords2 =
                createFileLogRecords(
                        schemaId,
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);

        assertThatThrownBy(
                        () ->
                                doProjection(
                                        1L,
                                        schemaId,
                                        projection,
                                        fileLogRecords2,
                                        new int[] {0, 1},
                                        Integer.MAX_VALUE))
                .isInstanceOf(EOFException.class)
                .hasMessageContaining("Failed to read `arrow header` from file channel");
        fileLogRecords2.close();
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testProjectSizeLimited(byte recordBatchMagic) throws Exception {
        List<Object[]> allData = new ArrayList<>();
        allData.addAll(TestData.DATA1);
        allData.addAll(TestData.ANOTHER_DATA1);
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        int totalSize = fileLogRecords.sizeInBytes();
        boolean hasEmpty = false;
        boolean hasHalf = false;
        boolean hasFull = false;
        for (int i = 4; i >= 1; i--) {
            int maxBytes = totalSize / i;
            List<Object[]> results =
                    doProjection(
                            new FileLogProjection(new ProjectionPushdownCache()),
                            fileLogRecords,
                            new int[] {0, 1},
                            maxBytes);
            if (results.isEmpty()) {
                hasEmpty = true;
            } else if (results.size() == TestData.DATA1.size()) {
                hasHalf = true;
                assertEquals(results, TestData.DATA1);
            } else if (results.size() == allData.size()) {
                hasFull = true;
                assertEquals(results, allData);
            } else {
                fail("Unexpected result size: " + results.size());
            }
        }
        assertThat(hasEmpty).isTrue();
        assertThat(hasHalf).isTrue();
        assertThat(hasFull).isTrue();
    }

    @Test
    void testReadLogHeaderFullyOrFail() throws Exception {
        ByteBuffer logHeaderBuffer = ByteBuffer.allocate(V1_RECORD_BATCH_HEADER_SIZE);

        // only V1 log header, should read fully
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V1, V1_RECORD_BATCH_HEADER_SIZE)) {
            FileLogProjection.readLogHeaderFullyOrFail(
                    fileLogRecords.channel(), logHeaderBuffer, 0);
            assertThat(logHeaderBuffer.hasRemaining()).isFalse();
        }

        // V1 log header with data, should read fully
        try (FileLogRecords fileLogRecords = createFileWithLogHeader(LOG_MAGIC_VALUE_V1, 100)) {
            logHeaderBuffer.rewind();
            FileLogProjection.readLogHeaderFullyOrFail(
                    fileLogRecords.channel(), logHeaderBuffer, 0);
            assertThat(logHeaderBuffer.hasRemaining()).isFalse();
        }

        // only v0 log header, should only read 48 bytes
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V0, V0_RECORD_BATCH_HEADER_SIZE)) {
            logHeaderBuffer.rewind();
            FileLogProjection.readLogHeaderFullyOrFail(
                    fileLogRecords.channel(), logHeaderBuffer, 0);
            assertThat(logHeaderBuffer.hasRemaining()).isTrue();
            assertThat(logHeaderBuffer.position()).isEqualTo(V0_RECORD_BATCH_HEADER_SIZE);
        }

        // v0 log header with data, should read fully
        try (FileLogRecords fileLogRecords = createFileWithLogHeader(LOG_MAGIC_VALUE_V0, 100)) {
            logHeaderBuffer.rewind();
            FileLogProjection.readLogHeaderFullyOrFail(
                    fileLogRecords.channel(), logHeaderBuffer, 0);
            assertThat(logHeaderBuffer.hasRemaining()).isFalse();
        }

        // v1 log header incomplete, should throw exception
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V1, V0_RECORD_BATCH_HEADER_SIZE)) {
            logHeaderBuffer.rewind();
            assertThatThrownBy(
                            () ->
                                    FileLogProjection.readLogHeaderFullyOrFail(
                                            fileLogRecords.channel(), logHeaderBuffer, 0),
                            "Should throw exception if the log header is incomplete")
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining(
                            "Expected to read 52 bytes, but reached end of file after reading 48 bytes.");
        }

        // v0 log header incomplete, should throw exception
        try (FileLogRecords fileLogRecords =
                createFileWithLogHeader(LOG_MAGIC_VALUE_V0, V0_RECORD_BATCH_HEADER_SIZE - 1)) {
            logHeaderBuffer.rewind();
            assertThatThrownBy(
                            () ->
                                    FileLogProjection.readLogHeaderFullyOrFail(
                                            fileLogRecords.channel(), logHeaderBuffer, 0),
                            "Should throw exception if the log header is incomplete")
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining(
                            "Expected to read 48 bytes, but reached end of file after reading 47 bytes.");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNestedRowProjection(boolean isProjectedByIds) throws Exception {
        int schemaId = 1;
        final Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column(
                                "c",
                                DataTypes.ROW(
                                        DataTypes.INT(),
                                        DataTypes.ROW(DataTypes.INT(), DataTypes.STRING())))
                        .column("d", DataTypes.INT())
                        .column("e", DataTypes.STRING())
                        .build();
        List<Object[]> data =
                Arrays.asList(
                        new Object[] {
                            1, "a", new Object[] {11, new Object[] {111, "aa"}}, 1111, "aaa"
                        },
                        new Object[] {
                            2, "b", new Object[] {22, new Object[] {222, "bb"}}, 2222, "bbb"
                        },
                        new Object[] {
                            3, "c", new Object[] {33, new Object[] {333, "cc"}}, 3333, "ccc"
                        },
                        new Object[] {
                            4, "d", new Object[] {44, new Object[] {444, "dd"}}, 4444, "ddd"
                        },
                        new Object[] {
                            5, "e", new Object[] {55, new Object[] {555, "ee"}}, 5555, "eee"
                        });

        int[] projectedFields = new int[] {0, 3, 4};
        List<Object[]> expected =
                Arrays.asList(
                        new Object[] {
                            1,
                            isProjectedByIds ? 11 : 1111,
                            isProjectedByIds ? new Object[] {111, "aa"} : "aaa"
                        },
                        new Object[] {
                            2,
                            isProjectedByIds ? 22 : 2222,
                            isProjectedByIds ? new Object[] {222, "bb"} : "bbb"
                        },
                        new Object[] {
                            3,
                            isProjectedByIds ? 33 : 3333,
                            isProjectedByIds ? new Object[] {333, "cc"} : "ccc"
                        },
                        new Object[] {
                            4,
                            isProjectedByIds ? 44 : 4444,
                            isProjectedByIds ? new Object[] {444, "dd"} : "ddd"
                        },
                        new Object[] {
                            5,
                            isProjectedByIds ? 55 : 5555,
                            isProjectedByIds ? new Object[] {555, "ee"} : "eee"
                        });

        testingSchemaGetter.updateLatestSchemaInfo(new SchemaInfo(schema, 1));
        FileLogRecords fileLogRecords =
                createFileLogRecords(schemaId, (byte) 1, schema.getRowType(), data);
        List<Object[]> results =
                doProjection(
                        1L,
                        schemaId,
                        new FileLogProjection(new ProjectionPushdownCache()),
                        fileLogRecords,
                        projectedFields,
                        isProjectedByIds,
                        Integer.MAX_VALUE);
        assertEquals(results, expected);
    }

    private FileLogRecords createFileWithLogHeader(byte magic, int length) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(MAGIC_OFFSET);
        buffer.put(magic);
        buffer.position(length);
        buffer.flip();
        File file = new File(tempDir, UUID.randomUUID() + ".log");
        FileLogRecords fileLogRecords = FileLogRecords.open(file);
        fileLogRecords.channel().write(buffer);
        fileLogRecords.flush();
        return fileLogRecords;
    }

    @SafeVarargs
    final FileLogRecords createFileLogRecords(
            byte recordBatchMagic, RowType rowType, List<Object[]>... inputs) throws Exception {
        return createFileLogRecords(DEFAULT_SCHEMA_ID, recordBatchMagic, rowType, inputs);
    }

    @SafeVarargs
    final FileLogRecords createFileLogRecords(
            int schemaId, byte recordBatchMagic, RowType rowType, List<Object[]>... inputs)
            throws Exception {
        FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"));
        long offsetBase = 0L;
        for (List<Object[]> input : inputs) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            rowType,
                            schemaId,
                            offsetBase,
                            System.currentTimeMillis(),
                            recordBatchMagic,
                            input,
                            LogFormat.ARROW));
            offsetBase += input.size();
        }
        fileLogRecords.flush();
        return fileLogRecords;
    }

    private List<Object[]> doProjection(
            FileLogProjection projection,
            FileLogRecords fileLogRecords,
            int[] projectedFields,
            int fetchMaxBytes)
            throws Exception {
        return doProjection(
                1L, DEFAULT_SCHEMA_ID, projection, fileLogRecords, projectedFields, fetchMaxBytes);
    }

    private List<Object[]> doProjection(
            long tableId,
            int schemaId,
            FileLogProjection projection,
            FileLogRecords fileLogRecords,
            int[] projectedFields,
            int fetchMaxBytes)
            throws Exception {
        return doProjection(
                tableId,
                schemaId,
                projection,
                fileLogRecords,
                projectedFields,
                false,
                fetchMaxBytes);
    }

    private List<Object[]> doProjection(
            long tableId,
            int schemaId,
            FileLogProjection projection,
            FileLogRecords fileLogRecords,
            int[] projectedFields,
            boolean isProjectedByIds,
            int fetchMaxBytes)
            throws Exception {
        projection.setCurrentProjection(
                tableId,
                testingSchemaGetter,
                DEFAULT_COMPRESSION,
                projectedFields,
                isProjectedByIds);
        LogRecords project =
                projection.project(
                        fileLogRecords.channel(), 0, fileLogRecords.sizeInBytes(), fetchMaxBytes);
        assertThat(project.sizeInBytes()).isLessThanOrEqualTo(fetchMaxBytes);
        Schema schema = testingSchemaGetter.getSchema(schemaId);
        RowType rowType = schema.getRowType();
        RowType projectedType =
                isProjectedByIds
                        ? FileLogProjection.projectByIds(
                                rowType, schema.getHighestFieldId(), projectedFields)
                        : rowType.project(projectedFields);
        List<Object[]> results = new ArrayList<>();
        long expectedOffset = 0L;
        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, schemaId, testingSchemaGetter, true)) {
            for (LogRecordBatch batch : project.batches()) {
                try (CloseableIterator<LogRecord> records = batch.records(context)) {
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        assertThat(record.logOffset()).isEqualTo(expectedOffset);
                        assertThat(record.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                        InternalRow row = record.getRow();
                        assertThat(row.getFieldCount()).isEqualTo(projectedFields.length);
                        Object[] objs = getRow(projectedType, row);
                        results.add(objs);
                        expectedOffset++;
                    }
                }
            }
        }
        return results;
    }

    private static void assertEquals(List<Object[]> actual, List<Object[]> expected) {
        assertThat(actual.size()).isEqualTo(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i)).isEqualTo(expected.get(i));
        }
    }

    private static Object[] getRow(RowType projectedType, InternalRow row) {
        Object[] results = new Object[projectedType.getFieldCount()];
        for (int i = 0; i < projectedType.getFieldCount(); i++) {
            if (row.isNullAt(i)) {
                results[i] = null;
                continue;
            }
            switch (projectedType.getTypeAt(i).getTypeRoot()) {
                case INTEGER:
                    results[i] = row.getInt(i);
                    break;
                case STRING:
                    results[i] = row.getString(i).toString();
                    break;
                case ROW:
                    RowType rowType = (RowType) projectedType.getTypeAt(i);
                    results[i] = getRow(rowType, row.getRow(i, rowType.getFieldCount()));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported type: " + projectedType.getTypeAt(i));
            }
        }
        return results;
    }
}
