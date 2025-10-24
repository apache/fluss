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

package org.apache.fluss.row;

import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.compacted.CompactedRowReader;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.row.indexed.IndexedRowReader;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.apache.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.arrowChangeTypeOffset;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test reader and writer consistency for Arrow, Compacted and Indexed row. */
class RowReaderWriterConsistencyTest {

    @Test
    void testChar() throws Exception {
        BinaryString str = BinaryString.fromString("abc");

        Object compactedResult = writeAndReadCompactedRow(DataTypes.CHAR(3), str);
        Object indexedResult = writeAndReadIndexRow(DataTypes.CHAR(3), str);
        Object arrowResult = writeAndReadArrowRow(DataTypes.CHAR(3), str);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testString() throws Exception {
        BinaryString str = BinaryString.fromString("abc");

        Object compactedResult = writeAndReadCompactedRow(DataTypes.STRING(), str);
        Object indexedResult = writeAndReadIndexRow(DataTypes.STRING(), str);
        Object arrowResult = writeAndReadArrowRow(DataTypes.STRING(), str);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testBoolean() throws Exception {
        boolean[] values = new boolean[] {true, false};

        for (boolean value : values) {
            Object compactedResult = writeAndReadCompactedRow(DataTypes.BOOLEAN(), value);
            Object indexedResult = writeAndReadIndexRow(DataTypes.BOOLEAN(), value);
            Object arrowResult = writeAndReadArrowRow(DataTypes.BOOLEAN(), value);

            assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
        }
    }

    @Test
    void testBinary() throws Exception {
        byte[] bytes = new byte[] {0, 1, 2, 3, 0, 0};

        Object compactedResult = writeAndReadCompactedRow(DataTypes.BINARY(6), bytes);
        Object indexedResult = writeAndReadIndexRow(DataTypes.BINARY(6), bytes);
        Object arrowResult = writeAndReadArrowRow(DataTypes.BINARY(6), bytes);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testBytes() throws Exception {
        byte[] bytes = new byte[] {0, 1, 2, 3, 0, 0};

        Object compactedResult = writeAndReadCompactedRow(DataTypes.BYTES(), bytes);
        Object indexedResult = writeAndReadIndexRow(DataTypes.BYTES(), bytes);
        Object arrowResult = writeAndReadArrowRow(DataTypes.BYTES(), bytes);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testDecimal() throws Exception {
        BigDecimal bigDecimal = new BigDecimal("1234567890.0987654321");

        for (int p = 1; p <= 38; p++) {
            for (int s = 1; s <= p; s++) {
                Decimal decimal = Decimal.fromBigDecimal(bigDecimal, p, s);

                Object compactedResult = writeAndReadCompactedRow(DataTypes.DECIMAL(p, s), decimal);
                Object indexedResult = writeAndReadIndexRow(DataTypes.DECIMAL(p, s), decimal);
                Object arrowResult = writeAndReadArrowRow(DataTypes.DECIMAL(p, s), decimal);

                assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
            }
        }
    }

    @Test
    void testTinyInt() throws Exception {
        byte value = (byte) 123;

        Object compactedResult = writeAndReadCompactedRow(DataTypes.TINYINT(), value);
        Object indexedResult = writeAndReadIndexRow(DataTypes.TINYINT(), value);
        Object arrowResult = writeAndReadArrowRow(DataTypes.TINYINT(), value);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testSmallInt() throws Exception {
        short value = (short) 123;

        Object compactedResult = writeAndReadCompactedRow(DataTypes.SMALLINT(), value);
        Object indexedResult = writeAndReadIndexRow(DataTypes.SMALLINT(), value);
        Object arrowResult = writeAndReadArrowRow(DataTypes.SMALLINT(), value);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testInt() throws Exception {
        int value = 123;

        Object compactedResult = writeAndReadCompactedRow(DataTypes.INT(), value);
        Object indexedResult = writeAndReadIndexRow(DataTypes.INT(), value);
        Object arrowResult = writeAndReadArrowRow(DataTypes.INT(), value);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testDate() throws Exception {
        LocalDate date = LocalDate.now();
        int days = (int) date.toEpochDay();

        Object compactedResult = writeAndReadCompactedRow(DataTypes.DATE(), days);
        Object indexedResult = writeAndReadIndexRow(DataTypes.DATE(), days);
        Object arrowResult = writeAndReadArrowRow(DataTypes.DATE(), days);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testTime() throws Exception {
        LocalTime time = LocalTime.now();
        int mill = (int) (time.toNanoOfDay() / 1_000_000);

        for (int i = 0; i <= 9; i++) {
            Object compactedResult = writeAndReadCompactedRow(DataTypes.TIME(i), mill);
            Object indexedResult = writeAndReadIndexRow(DataTypes.TIME(i), mill);
            Object arrowResult = writeAndReadArrowRow(DataTypes.TIME(i), mill);

            assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
        }
    }

    @Test
    void testBigint() throws Exception {
        long value = 123;

        Object compactedResult = writeAndReadCompactedRow(DataTypes.BIGINT(), value);
        Object indexedResult = writeAndReadIndexRow(DataTypes.BIGINT(), value);
        Object arrowResult = writeAndReadArrowRow(DataTypes.BIGINT(), value);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testFloat() throws Exception {
        float value = 123.456f;

        Object compactedResult = writeAndReadCompactedRow(DataTypes.FLOAT(), value);
        Object indexedResult = writeAndReadIndexRow(DataTypes.FLOAT(), value);
        Object arrowResult = writeAndReadArrowRow(DataTypes.FLOAT(), value);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testDouble() throws Exception {
        double value = 123.456;

        Object compactedResult = writeAndReadCompactedRow(DataTypes.DOUBLE(), value);
        Object indexedResult = writeAndReadIndexRow(DataTypes.DOUBLE(), value);
        Object arrowResult = writeAndReadArrowRow(DataTypes.DOUBLE(), value);

        assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
    }

    @Test
    void testTimestampNtz() throws Exception {
        TimestampNtz timestampNtz = TimestampNtz.fromLocalDateTime(LocalDateTime.now());

        for (int i = 0; i <= 9; i++) {
            Object compactedResult = writeAndReadCompactedRow(DataTypes.TIMESTAMP(i), timestampNtz);
            Object indexedResult = writeAndReadIndexRow(DataTypes.TIMESTAMP(i), timestampNtz);
            Object arrowResult = writeAndReadArrowRow(DataTypes.TIMESTAMP(i), timestampNtz);

            assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
        }
    }

    @Test
    void testTimestampLtz() throws Exception {
        TimestampLtz timestampLtz = TimestampLtz.fromInstant(Instant.now());

        for (int i = 0; i <= 9; i++) {
            Object compactedResult =
                    writeAndReadCompactedRow(DataTypes.TIMESTAMP_LTZ(i), timestampLtz);
            Object indexedResult = writeAndReadIndexRow(DataTypes.TIMESTAMP_LTZ(i), timestampLtz);
            Object arrowResult = writeAndReadArrowRow(DataTypes.TIMESTAMP_LTZ(i), timestampLtz);

            assertThat(compactedResult).isEqualTo(indexedResult).isEqualTo(arrowResult);
        }
    }

    private Object writeAndReadCompactedRow(DataType dataType, Object value) {
        CompactedRowWriter crw = new CompactedRowWriter(1);
        CompactedRowWriter.FieldWriter cfw = CompactedRowWriter.createFieldWriter(dataType);
        cfw.writeField(crw, 0, value);

        CompactedRowReader crr = new CompactedRowReader(1);
        CompactedRowReader.FieldReader cfr = CompactedRowReader.createFieldReader(dataType);
        crr.pointTo(crw.segment(), 0, crw.position());

        return cfr.readField(crr, 0);
    }

    private Object writeAndReadIndexRow(DataType dataType, Object value) {
        IndexedRowWriter irw = new IndexedRowWriter(new DataType[] {dataType});
        IndexedRowWriter.FieldWriter ifw = IndexedRowWriter.createFieldWriter(dataType);
        ifw.writeField(irw, 0, value);

        IndexedRowReader irr = new IndexedRowReader(new DataType[] {dataType});
        IndexedRowReader.FieldReader ifr = IndexedRowReader.createFieldReader(dataType);
        irr.pointTo(irw.segment(), 0);

        return ifr.readField(irr, 0);
    }

    private Object writeAndReadArrowRow(DataType dataType, Object value) throws Exception {
        RowType rowType = DataTypes.ROW(dataType);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root =
                VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
        ArrowWriterPool provider = new ArrowWriterPool(allocator);
        ArrowWriter arrowWriter =
                provider.getOrCreateWriter(1L, 1, Integer.MAX_VALUE, rowType, NO_COMPRESSION);
        arrowWriter.writeRow(row(value));

        AbstractPagedOutputView pagedOutputView =
                new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024));

        // skip arrow batch header.
        int size =
                arrowWriter.serializeToOutputView(
                        pagedOutputView, arrowChangeTypeOffset(CURRENT_LOG_MAGIC_VALUE));
        MemorySegment segment =
                MemorySegment.allocateHeapMemory(arrowWriter.estimatedSizeInBytes());

        assertThat(pagedOutputView.getWrittenSegments().size()).isEqualTo(1);
        MemorySegment firstSegment = pagedOutputView.getCurrentSegment();
        firstSegment.copyTo(arrowChangeTypeOffset(CURRENT_LOG_MAGIC_VALUE), segment, 0, size);

        ArrowReader reader =
                ArrowUtils.createArrowReader(segment, 0, size, root, allocator, rowType);

        InternalRow.FieldGetter fieldGetter = InternalRow.createFieldGetter(dataType, 0);
        return fieldGetter.getFieldOrNull(reader.read(0));
    }
}
