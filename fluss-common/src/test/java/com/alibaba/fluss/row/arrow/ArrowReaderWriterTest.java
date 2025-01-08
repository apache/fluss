/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.row.arrow;

import com.alibaba.fluss.compression.ArrowCompressionType;
import com.alibaba.fluss.memory.AbstractPagedOutputView;
import com.alibaba.fluss.memory.ManagedPagedOutputView;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.TestingMemorySegmentPool;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.columnar.ColumnarRow;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.fluss.record.DefaultLogRecordBatch.ARROW_ROWKIND_OFFSET;
import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ArrowReader} and {@link ArrowWriter}. */
class ArrowReaderWriterTest {

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
                    DataTypes.TIMESTAMP_LTZ(9));

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
                            BinaryString.fromString("abc"),
                            BinaryString.fromString("Hello World!"),
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
                            TimestampLtz.fromEpochMillis(3600123, 456789)),
                    GenericRow.of(
                            false,
                            (byte) 1,
                            (short) 2,
                            null,
                            4L,
                            5.0f,
                            6.0,
                            Decimal.fromUnscaledLong(1234, 10, 3),
                            BinaryString.fromString("abc"),
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
                            TimestampLtz.fromEpochMillis(3600120, 123450)));

    @Test
    void testReaderWriter() throws IOException {
        RowType rowType = DataTypes.ROW(ALL_TYPES.toArray(new DataType[0]));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
                ArrowWriterPool provider = new ArrowWriterPool(allocator);
                ArrowWriter writer =
                        provider.getOrCreateWriter(
                                1L, 1, Integer.MAX_VALUE, rowType, ArrowCompressionType.NO)) {
            for (InternalRow row : TEST_DATA) {
                writer.writeRow(row);
            }

            AbstractPagedOutputView pagedOutputView =
                    new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024));

            // skip arrow batch header.
            int size = writer.serializeToOutputView(pagedOutputView, ARROW_ROWKIND_OFFSET);
            MemorySegment segment = MemorySegment.allocateHeapMemory(writer.sizeInBytes());

            assertThat(pagedOutputView.getWrittenSegments().size()).isEqualTo(1);
            MemorySegment firstSegment = pagedOutputView.getCurrentSegment();
            firstSegment.copyTo(ARROW_ROWKIND_OFFSET, segment, 0, size);

            ArrowReader reader =
                    ArrowUtils.createArrowReader(segment, 0, size, root, allocator, rowType);
            int rowCount = reader.getRowCount();
            ColumnarRow row = reader.read(0);
            for (int i = 0; i < rowCount; i++) {
                row.setRowId(i);
                assertThatRow(row).withSchema(rowType).isEqualTo(TEST_DATA.get(i));
            }
        }
    }

    @Test
    void testWriterExceedMaxSizeInBytes() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowWriterPool provider = new ArrowWriterPool(allocator);
                ArrowWriter writer =
                        provider.getOrCreateWriter(
                                1L, 1, 1024, DATA1_ROW_TYPE, ArrowCompressionType.NO)) {
            while (!writer.isFull()) {
                writer.writeRow(row(DATA1_ROW_TYPE, DATA1.get(0)));
            }

            // exceed max size
            assertThatThrownBy(() -> writer.writeRow(row(DATA1_ROW_TYPE, DATA1.get(0))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(
                            "The arrow batch size is full and it shouldn't accept writing new rows, it's a bug.");
        }
    }
}
