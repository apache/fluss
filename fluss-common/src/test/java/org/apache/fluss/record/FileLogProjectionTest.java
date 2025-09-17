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
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
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

    private @TempDir File tempDir;

    // TODO: add tests for nested types
    @Test
    void testSetCurrentProjection() {
        FileLogProjection projection = new FileLogProjection();
        projection.setCurrentProjection(
                1L, TestData.DATA2_ROW_TYPE, DEFAULT_COMPRESSION, new int[] {0, 2});
        FileLogProjection.ProjectionInfo info1 = projection.currentProjection;
        assertThat(info1).isNotNull();
        assertThat(info1.nodesProjection.stream().toArray()).isEqualTo(new int[] {0, 2});
        // a int: [0,1] ; b string: [2,3,4] ; c string: [5,6,7]
        assertThat(info1.buffersProjection.stream().toArray()).isEqualTo(new int[] {0, 1, 5, 6, 7});
        assertThat(projection.projectionsCache).hasSize(1);
        assertThat(projection.projectionsCache.get(1L)).isSameAs(info1);

        projection.setCurrentProjection(
                2L, TestData.DATA2_ROW_TYPE, DEFAULT_COMPRESSION, new int[] {1});
        FileLogProjection.ProjectionInfo info2 = projection.currentProjection;
        assertThat(info2).isNotNull();
        assertThat(info2.nodesProjection.stream().toArray()).isEqualTo(new int[] {1});
        // a int: [0,1] ; b string: [2,3,4] ; c string: [5,6,7]
        assertThat(info2.buffersProjection.stream().toArray()).isEqualTo(new int[] {2, 3, 4});
        assertThat(projection.projectionsCache).hasSize(2);
        assertThat(projection.projectionsCache.get(2L)).isSameAs(info2);

        projection.setCurrentProjection(
                1L, TestData.DATA2_ROW_TYPE, DEFAULT_COMPRESSION, new int[] {0, 2});
        assertThat(projection.currentProjection).isNotNull().isSameAs(info1);

        assertThatThrownBy(
                        () ->
                                projection.setCurrentProjection(
                                        1L,
                                        TestData.DATA1_ROW_TYPE,
                                        DEFAULT_COMPRESSION,
                                        new int[] {1}))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("The schema and projection should be identical for the same table id.");
    }

    @Test
    void testIllegalSetCurrentProjection() {
        FileLogProjection projection = new FileLogProjection();

        assertThatThrownBy(
                        () ->
                                projection.setCurrentProjection(
                                        1L,
                                        TestData.DATA2_ROW_TYPE,
                                        DEFAULT_COMPRESSION,
                                        new int[] {3}))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("Projected fields [3] is out of bound for schema with 3 fields.");

        assertThatThrownBy(
                        () ->
                                projection.setCurrentProjection(
                                        1L,
                                        TestData.DATA2_ROW_TYPE,
                                        DEFAULT_COMPRESSION,
                                        new int[] {1, 0}))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage("The projection indexes should be in field order, but is [1, 0]");

        assertThatThrownBy(
                        () ->
                                projection.setCurrentProjection(
                                        1L,
                                        TestData.DATA2_ROW_TYPE,
                                        DEFAULT_COMPRESSION,
                                        new int[] {0, 0, 0}))
                .isInstanceOf(InvalidColumnProjectionException.class)
                .hasMessage(
                        "The projection indexes should not contain duplicated fields, but is [0, 0, 0]");
    }

    static Stream<Arguments> projectedFieldsArgs() {
        return Stream.of(
                Arguments.of((Object) new int[] {0}, LOG_MAGIC_VALUE_V0),
                Arguments.arguments((Object) new int[] {1}, LOG_MAGIC_VALUE_V0),
                Arguments.arguments((Object) new int[] {0, 1}, LOG_MAGIC_VALUE_V0),
                Arguments.of((Object) new int[] {0}, LOG_MAGIC_VALUE_V1),
                Arguments.arguments((Object) new int[] {1}, LOG_MAGIC_VALUE_V1),
                Arguments.arguments((Object) new int[] {0, 1}, LOG_MAGIC_VALUE_V1));
    }

    @ParameterizedTest
    @MethodSource("projectedFieldsArgs")
    void testProject(int[] projectedFields, byte recordBatchMagic) throws Exception {
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        List<Object[]> results =
                doProjection(
                        new FileLogProjection(),
                        fileLogRecords,
                        TestData.DATA1_ROW_TYPE,
                        projectedFields,
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
        FileLogRecords fileLogRecords =
                createFileLogRecords(
                        recordBatchMagic,
                        TestData.DATA1_ROW_TYPE,
                        TestData.DATA1,
                        TestData.ANOTHER_DATA1);
        FileLogProjection projection = new FileLogProjection();
        // overwrite the wrong decoding byte order endian
        projection.getLogHeaderBuffer().order(ByteOrder.BIG_ENDIAN);
        // should throw exception.
        assertThatThrownBy(
                        () ->
                                doProjection(
                                        projection,
                                        fileLogRecords,
                                        TestData.DATA1_ROW_TYPE,
                                        new int[] {0, 1},
                                        Integer.MAX_VALUE))
                .isInstanceOf(EOFException.class)
                .hasMessageContaining("Failed to read `arrow header` from file channel");
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
                            new FileLogProjection(),
                            fileLogRecords,
                            TestData.DATA1_ROW_TYPE,
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
        FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"));
        long offsetBase = 0L;
        for (List<Object[]> input : inputs) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            rowType,
                            DEFAULT_SCHEMA_ID,
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
            RowType rowType,
            int[] projectedFields,
            int fetchMaxBytes)
            throws Exception {
        projection.setCurrentProjection(1L, rowType, DEFAULT_COMPRESSION, projectedFields);
        RowType projectedType = rowType.project(projectedFields);
        LogRecords project =
                projection.project(
                        fileLogRecords.channel(), 0, fileLogRecords.sizeInBytes(), fetchMaxBytes);
        assertThat(project.sizeInBytes()).isLessThanOrEqualTo(fetchMaxBytes);
        List<Object[]> results = new ArrayList<>();
        long expectedOffset = 0L;
        try (LogRecordReadContext context =
                createArrowReadContext(projectedType, DEFAULT_SCHEMA_ID)) {
            for (LogRecordBatch batch : project.batches()) {
                try (CloseableIterator<LogRecord> records = batch.records(context)) {
                    while (records.hasNext()) {
                        LogRecord record = records.next();
                        assertThat(record.logOffset()).isEqualTo(expectedOffset);
                        assertThat(record.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                        InternalRow row = record.getRow();
                        assertThat(row.getFieldCount()).isEqualTo(projectedFields.length);
                        Object[] objs = new Object[projectedFields.length];
                        for (int i = 0; i < projectedFields.length; i++) {
                            if (row.isNullAt(i)) {
                                objs[i] = null;
                                continue;
                            }
                            switch (projectedType.getTypeAt(i).getTypeRoot()) {
                                case INTEGER:
                                    objs[i] = row.getInt(i);
                                    break;
                                case STRING:
                                    objs[i] = row.getString(i).toString();
                                    break;
                                default:
                                    throw new IllegalArgumentException(
                                            "Unsupported type: " + projectedType.getTypeAt(i));
                            }
                        }
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
}
