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

package org.apache.fluss.row.arrow.writers;

import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ArrowVectorWriter} and {@link
 * org.apache.fluss.row.arrow.vectors.ArrowVectorColumnVector}.
 */
class ArrowVectorWriterTest {

    private static final int DIMENSION = 4;

    private static final RowType ROW_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                    DataTypes.FIELD("embedding", DataTypes.VECTOR(DIMENSION)));

    // ---------------------------------------------------------------------------
    // Helper: write rows and read them back, calling consumer inside resource scope
    // ---------------------------------------------------------------------------

    @FunctionalInterface
    private interface RowConsumer {
        void accept(int rowIndex, ColumnarRow row) throws Exception;
    }

    private void writeAndVerify(InternalRow[] rows, RowConsumer consumer) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(ROW_TYPE), allocator);
                ArrowWriterPool pool = new ArrowWriterPool(allocator);
                ArrowWriter writer =
                        pool.getOrCreateWriter(
                                1L, 1, Integer.MAX_VALUE, ROW_TYPE, NO_COMPRESSION)) {

            for (InternalRow row : rows) {
                writer.writeRow(row);
            }

            AbstractPagedOutputView outputView =
                    new ManagedPagedOutputView(new TestingMemorySegmentPool(64 * 1024));
            int size =
                    writer.serializeToOutputView(
                            outputView, recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE));
            int heapSize = Math.max(size, writer.estimatedSizeInBytes());
            MemorySegment segment = MemorySegment.allocateHeapMemory(heapSize);
            outputView
                    .getCurrentSegment()
                    .copyTo(recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE), segment, 0, size);

            ArrowReader reader =
                    ArrowUtils.createArrowReader(segment, 0, size, root, allocator, ROW_TYPE);
            for (int i = 0; i < rows.length; i++) {
                ColumnarRow row = reader.read(i);
                row.setRowId(i);
                try {
                    consumer.accept(i, row);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    /** Write 3 non-null VECTOR(4) rows, read back and assert all float values. */
    @Test
    void testWriteAndReadVectorRows() throws Exception {
        float[] v0 = {1.0f, 2.0f, 3.0f, 4.0f};
        float[] v1 = {-1.0f, 0.5f, 100.0f, Float.MIN_VALUE};
        float[] v2 = {Float.MAX_VALUE, -Float.MAX_VALUE, 0.0f, -0.0f};

        InternalRow[] rows = {
            GenericRow.of(0L, new GenericArray(new Float[] {v0[0], v0[1], v0[2], v0[3]})),
            GenericRow.of(1L, new GenericArray(new Float[] {v1[0], v1[1], v1[2], v1[3]})),
            GenericRow.of(2L, new GenericArray(new Float[] {v2[0], v2[1], v2[2], v2[3]}))
        };

        float[][] expected = {v0, v1, v2};
        writeAndVerify(
                rows,
                (i, row) -> {
                    assertThat(row.isNullAt(1)).isFalse();
                    InternalArray vec = row.getArray(1);
                    assertThat(vec.size()).isEqualTo(DIMENSION);
                    for (int j = 0; j < DIMENSION; j++) {
                        assertThat(vec.getFloat(j)).isEqualTo(expected[i][j]);
                    }
                });
    }

    /** Write a null VECTOR row and verify isNullAt returns true for that row. */
    @Test
    void testWriteNullVector() throws Exception {
        InternalRow[] rows = {
            GenericRow.of(0L, new GenericArray(new Float[] {1.0f, 2.0f, 3.0f, 4.0f})),
            GenericRow.of(1L, (Object) null) // null embedding
        };

        writeAndVerify(
                rows,
                (i, row) -> {
                    if (i == 0) {
                        assertThat(row.isNullAt(1)).isFalse();
                        InternalArray vec = row.getArray(1);
                        assertThat(vec.size()).isEqualTo(DIMENSION);
                        assertThat(vec.getFloat(0)).isEqualTo(1.0f);
                        assertThat(vec.getFloat(1)).isEqualTo(2.0f);
                        assertThat(vec.getFloat(2)).isEqualTo(3.0f);
                        assertThat(vec.getFloat(3)).isEqualTo(4.0f);
                    } else {
                        // Row 1 has null embedding
                        assertThat(row.isNullAt(1)).isTrue();
                    }
                });
    }

    /**
     * Write more than INITIAL_CAPACITY (1024) rows with VECTOR(4). This exercises safe-mode element
     * writes for child element indices beyond INITIAL_CAPACITY, analogous to the fix for
     * ArrowArrayWriter.
     */
    @Test
    void testWriteBeyondInitialCapacity() throws Exception {
        // 300 rows * 4 elements = 1200 child elements > INITIAL_CAPACITY (1024)
        int numRows = 300;
        InternalRow[] rows = new InternalRow[numRows];
        for (int i = 0; i < numRows; i++) {
            float base = (float) i;
            rows[i] =
                    GenericRow.of(
                            (long) i,
                            new GenericArray(
                                    new Float[] {base, base + 0.1f, base + 0.2f, base + 0.3f}));
        }

        writeAndVerify(
                rows,
                (i, row) -> {
                    assertThat(row.getLong(0)).isEqualTo((long) i);
                    assertThat(row.isNullAt(1)).isFalse();
                    InternalArray vec = row.getArray(1);
                    assertThat(vec.size()).isEqualTo(DIMENSION);
                    float base = (float) i;
                    assertThat(vec.getFloat(0)).isEqualTo(base);
                    assertThat(vec.getFloat(1)).isEqualTo(base + 0.1f);
                    assertThat(vec.getFloat(2)).isEqualTo(base + 0.2f);
                    assertThat(vec.getFloat(3)).isEqualTo(base + 0.3f);
                });
    }

    /**
     * Write a batch, serialize it, then write a second batch and verify that the writer's internal
     * offset counter is correctly reset between batches.
     */
    @Test
    void testResetAndRewrite() throws IOException {
        float[] firstBatch = {9.0f, 8.0f, 7.0f, 6.0f};
        float[] secondBatch = {1.0f, 2.0f, 3.0f, 4.0f};

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(ROW_TYPE), allocator);
                ArrowWriterPool pool = new ArrowWriterPool(allocator);
                ArrowWriter writer =
                        pool.getOrCreateWriter(
                                1L, 1, Integer.MAX_VALUE, ROW_TYPE, NO_COMPRESSION)) {

            // ---- First batch ----
            writer.writeRow(
                    GenericRow.of(
                            0L,
                            new GenericArray(
                                    new Float[] {
                                        firstBatch[0], firstBatch[1], firstBatch[2], firstBatch[3]
                                    })));

            AbstractPagedOutputView outputView1 =
                    new ManagedPagedOutputView(new TestingMemorySegmentPool(64 * 1024));
            int size1 =
                    writer.serializeToOutputView(
                            outputView1, recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE));
            int heapSize1 = Math.max(size1, writer.estimatedSizeInBytes());
            MemorySegment segment1 = MemorySegment.allocateHeapMemory(heapSize1);
            outputView1
                    .getCurrentSegment()
                    .copyTo(recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE), segment1, 0, size1);

            ArrowReader reader1 =
                    ArrowUtils.createArrowReader(segment1, 0, size1, root, allocator, ROW_TYPE);
            assertThat(reader1.getRowCount()).isEqualTo(1);
            ColumnarRow row1 = reader1.read(0);
            row1.setRowId(0);
            InternalArray vec1 = row1.getArray(1);
            assertThat(vec1.size()).isEqualTo(DIMENSION);
            for (int i = 0; i < DIMENSION; i++) {
                assertThat(vec1.getFloat(i)).isEqualTo(firstBatch[i]);
            }

            // ---- Reset and second batch ----
            writer.reset(Integer.MAX_VALUE);
            writer.writeRow(
                    GenericRow.of(
                            1L,
                            new GenericArray(
                                    new Float[] {
                                        secondBatch[0],
                                        secondBatch[1],
                                        secondBatch[2],
                                        secondBatch[3]
                                    })));

            AbstractPagedOutputView outputView2 =
                    new ManagedPagedOutputView(new TestingMemorySegmentPool(64 * 1024));
            int size2 =
                    writer.serializeToOutputView(
                            outputView2, recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE));
            int heapSize2 = Math.max(size2, writer.estimatedSizeInBytes());
            MemorySegment segment2 = MemorySegment.allocateHeapMemory(heapSize2);
            outputView2
                    .getCurrentSegment()
                    .copyTo(recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE), segment2, 0, size2);

            ArrowReader reader2 =
                    ArrowUtils.createArrowReader(segment2, 0, size2, root, allocator, ROW_TYPE);
            assertThat(reader2.getRowCount()).isEqualTo(1);
            ColumnarRow row2 = reader2.read(0);
            row2.setRowId(0);
            InternalArray vec2 = row2.getArray(1);
            assertThat(vec2.size()).isEqualTo(DIMENSION);
            for (int i = 0; i < DIMENSION; i++) {
                assertThat(vec2.getFloat(i)).isEqualTo(secondBatch[i]);
            }
        }
    }

    /**
     * Writing an array with wrong number of elements into a VECTOR(4) writer must throw
     * IllegalArgumentException immediately (before any child vector writes).
     */
    @Test
    void testMismatchedArraySize() {
        // Write a 5-element array into a VECTOR(4) column — dimension mismatch
        InternalRow[] rows = {
            GenericRow.of(0L, new GenericArray(new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}))
        };
        assertThatThrownBy(() -> writeAndVerify(rows, (i, row) -> {}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("VECTOR dimension mismatch");
    }
}
