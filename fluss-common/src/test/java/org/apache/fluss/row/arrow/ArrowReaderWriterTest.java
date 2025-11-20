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

package org.apache.fluss.row.arrow;

import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.arrowChangeTypeOffset;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ArrowReader} and {@link ArrowWriter}. */
class ArrowReaderWriterTest {

    private static final DataType NESTED_DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("ri", DataTypes.INT()),
                    DataTypes.FIELD("rs", DataTypes.STRING()),
                    DataTypes.FIELD("rb", DataTypes.BIGINT()));

    private static final List<DataType> ALL_TYPES =
            Arrays.asList(
                    DataTypes.BOOLEAN(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT().copy(false),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DECIMAL(10, 3),
                    DataTypes.CHAR(3),
                    DataTypes.STRING(),
                    DataTypes.BINARY(5),
                    DataTypes.BYTES(),
                    DataTypes.TIME(),
                    DataTypes.DATE(),
                    DataTypes.TIMESTAMP(0),
                    DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.TIMESTAMP(9),
                    DataTypes.TIMESTAMP_LTZ(0),
                    DataTypes.TIMESTAMP_LTZ(3),
                    DataTypes.TIMESTAMP_LTZ(6),
                    DataTypes.TIMESTAMP_LTZ(9),
                    DataTypes.ARRAY(DataTypes.INT()),
                    DataTypes.ARRAY(DataTypes.FLOAT().copy(false)), // vector embedding type
                    DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()))) // nested array
            // TODO: Add Map and Row types in Issue #1973 and #1974
            // DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
            // DataTypes.ROW(...)
            ;

    private static final List<InternalRow> TEST_DATA =
            Arrays.asList(
                    GenericRow.of(
                            true,
                            (byte) 1,
                            (short) 2,
                            3,
                            4L,
                            5.0f,
                            6.0,
                            Decimal.fromUnscaledLong(1234, 10, 3),
                            fromString("abc"),
                            fromString("Hello World!"),
                            new byte[] {1, 2, 3, 4, 5},
                            new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
                            3600000,
                            100,
                            TimestampNtz.fromMillis(3600000),
                            TimestampNtz.fromMillis(3600123),
                            TimestampNtz.fromMillis(3600123, 456000),
                            TimestampNtz.fromMillis(3600123, 456789),
                            TimestampLtz.fromEpochMillis(3600000),
                            TimestampLtz.fromEpochMillis(3600123),
                            TimestampLtz.fromEpochMillis(3600123, 456000),
                            TimestampLtz.fromEpochMillis(3600123, 456789),
                            GenericArray.of(1, 2, 3, 4, 5, -11, 222, 444, 102234),
                            GenericArray.of(0.1f, 1.1f, 2.2f, 3.3f, 4.4f, -0.5f, 6.6f),
                            GenericArray.of(
                                    GenericArray.of(fromString("a"), fromString("b")),
                                    GenericArray.of(fromString("c"), fromString("d")))),
                    // TODO: Add Map and Row test data in Issue #1973 and #1974
                    // GenericMap.of(...),
                    // GenericRow.of(...)),
                    GenericRow.of(
                            false,
                            (byte) 1,
                            (short) 2,
                            null,
                            4L,
                            5.0f,
                            6.0,
                            Decimal.fromUnscaledLong(1234, 10, 3),
                            fromString("abc"),
                            null,
                            new byte[] {1, 2, 3, 4, 5},
                            new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
                            3600000,
                            123,
                            null,
                            TimestampNtz.fromMillis(3600120),
                            TimestampNtz.fromMillis(3600120, 120000),
                            TimestampNtz.fromMillis(3600120, 123450),
                            null,
                            TimestampLtz.fromEpochMillis(3600120),
                            TimestampLtz.fromEpochMillis(3600120, 120000),
                            TimestampLtz.fromEpochMillis(3600120, 123450),
                            GenericArray.of(1, 2, 3, null, Integer.MAX_VALUE, Integer.MIN_VALUE),
                            GenericArray.of(
                                    0.0f,
                                    -0.1f,
                                    1.1f,
                                    2.2f,
                                    3.3f,
                                    Float.MAX_VALUE,
                                    Float.MIN_VALUE),
                            GenericArray.of(
                                    GenericArray.of(fromString("a"), null, fromString("c")),
                                    null,
                                    GenericArray.of(fromString("hello"), fromString("world")))));
    // TODO: Add Map and Row test data in Issue #1973 and #1974
    // GenericMap.of(...),
    // GenericRow.of(...)));

    @Test
    void testReaderWriter() throws IOException {
        RowType rowType = DataTypes.ROW(ALL_TYPES.toArray(new DataType[0]));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
                ArrowWriterPool provider = new ArrowWriterPool(allocator);
                ArrowWriter writer =
                        provider.getOrCreateWriter(
                                1L, 1, Integer.MAX_VALUE, rowType, NO_COMPRESSION)) {
            for (InternalRow row : TEST_DATA) {
                writer.writeRow(row);
            }

            AbstractPagedOutputView pagedOutputView =
                    new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024));

            // skip arrow batch header.
            int size =
                    writer.serializeToOutputView(
                            pagedOutputView, arrowChangeTypeOffset(CURRENT_LOG_MAGIC_VALUE));
            int heapMemorySize = Math.max(size, writer.estimatedSizeInBytes());
            MemorySegment segment = MemorySegment.allocateHeapMemory(heapMemorySize);

            assertThat(pagedOutputView.getWrittenSegments().size()).isEqualTo(1);
            MemorySegment firstSegment = pagedOutputView.getCurrentSegment();
            firstSegment.copyTo(arrowChangeTypeOffset(CURRENT_LOG_MAGIC_VALUE), segment, 0, size);

            ArrowReader reader =
                    ArrowUtils.createArrowReader(segment, 0, size, root, allocator, rowType);
            int rowCount = reader.getRowCount();
            for (int i = 0; i < rowCount; i++) {
                ColumnarRow row = reader.read(i);
                row.setRowId(i);
                assertThatRow(row).withSchema(rowType).isEqualTo(TEST_DATA.get(i));

                InternalRow rowData = TEST_DATA.get(i);
                assertThat(row.getBoolean(0)).isEqualTo(rowData.getBoolean(0));
                assertThat(row.getByte(1)).isEqualTo(rowData.getByte(1));
                assertThat(row.getShort(2)).isEqualTo(rowData.getShort(2));
                if (!row.isNullAt(3)) {
                    assertThat(row.getInt(3)).isEqualTo(rowData.getInt(3));
                }
                assertThat(row.getLong(4)).isEqualTo(rowData.getLong(4));
                assertThat(row.getFloat(5)).isEqualTo(rowData.getFloat(5));
                assertThat(row.getDouble(6)).isEqualTo(rowData.getDouble(6));
                assertThat(row.getDecimal(7, 10, 3)).isEqualTo(rowData.getDecimal(7, 10, 3));
                assertThat(row.getChar(8, 3)).isEqualTo(rowData.getChar(8, 3));
                if (!row.isNullAt(9)) {
                    assertThat(row.getString(9)).isEqualTo(rowData.getString(9));
                }
                assertThat(row.getBinary(10, 5)).isEqualTo(rowData.getBinary(10, 5));
                assertThat(row.getBytes(11)).isEqualTo(rowData.getBytes(11));
                assertThat(row.getInt(12)).isEqualTo(rowData.getInt(12));
                assertThat(row.getInt(13)).isEqualTo(rowData.getInt(13));
                if (!row.isNullAt(14)) {
                    assertThat(row.getTimestampNtz(14, 0))
                            .isEqualTo(rowData.getTimestampNtz(14, 0));
                }
                assertThat(row.getTimestampNtz(15, 3)).isEqualTo(rowData.getTimestampNtz(15, 3));
                assertThat(row.getTimestampNtz(16, 6)).isEqualTo(rowData.getTimestampNtz(16, 6));
                assertThat(row.getTimestampNtz(17, 9)).isEqualTo(rowData.getTimestampNtz(17, 9));
                if (!row.isNullAt(18)) {
                    assertThat(row.getTimestampLtz(18, 0))
                            .isEqualTo(rowData.getTimestampLtz(18, 0));
                }
                assertThat(row.getTimestampLtz(19, 3)).isEqualTo(rowData.getTimestampLtz(19, 3));
                assertThat(row.getTimestampLtz(20, 6)).isEqualTo(rowData.getTimestampLtz(20, 6));
                assertThat(row.getTimestampLtz(21, 9)).isEqualTo(rowData.getTimestampLtz(21, 9));
            }
            reader.close();
        }
    }

    @Test
    void testWriterExceedMaxSizeInBytes() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowWriterPool provider = new ArrowWriterPool(allocator);
                ArrowWriter writer =
                        provider.getOrCreateWriter(
                                1L, 1, 1024, DATA1_ROW_TYPE, DEFAULT_COMPRESSION)) {
            while (!writer.isFull()) {
                writer.writeRow(row(DATA1.get(0)));
            }

            // exceed max size
            assertThatThrownBy(() -> writer.writeRow(row(DATA1.get(0))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(
                            "The arrow batch size is full and it shouldn't accept writing new rows, it's a bug.");
        }
    }
}
