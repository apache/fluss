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

import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for pruned (sub-batch) Arrow log record batch write and read roundtrip. Verifies that
 * pruned batches with target columns can be written and read back using the full table schema, with
 * absent columns appearing as null.
 */
public class PrunedArrowLogRecordBatchTest {

    private static final short SCHEMA_ID = 1;
    private static final int PAGE_SIZE = 1024;

    // Full table schema: 4 columns (a INT, b STRING, c INT, d STRING)
    private static final RowType FULL_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("a", DataTypes.INT()),
                    new DataField("b", DataTypes.STRING()),
                    new DataField("c", DataTypes.INT()),
                    new DataField("d", DataTypes.STRING()));

    private static final Schema FULL_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("c", DataTypes.INT())
                    .column("d", DataTypes.STRING())
                    .build();

    private BufferAllocator allocator;
    private ArrowWriterPool writerPool;

    @BeforeEach
    void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        writerPool = new ArrowWriterPool(allocator);
    }

    @AfterEach
    void tearDown() {
        writerPool.close();
        allocator.close();
    }

    @Test
    void testWriteAndReadPrunedBatch() throws Exception {
        // Target columns: [0, 1] -> (a INT, b STRING)
        int[] targetColumns = {0, 1};
        RowType prunedRowType = FULL_ROW_TYPE.project(targetColumns);

        // Create ArrowWriter for the pruned schema
        ArrowWriter writer =
                writerPool.getOrCreateWriter(
                        1L, SCHEMA_ID, PAGE_SIZE, prunedRowType, NO_COMPRESSION);

        // Build a pruned batch with targetColumns
        MemoryLogRecordsArrowBuilder builder =
                MemoryLogRecordsArrowBuilder.builder(
                        SCHEMA_ID,
                        writer,
                        new ManagedPagedOutputView(new TestingMemorySegmentPool(PAGE_SIZE)),
                        true,
                        null,
                        targetColumns);

        // Write pruned rows (only 2 columns)
        builder.append(ChangeType.APPEND_ONLY, row(new Object[] {1, "hello"}));
        builder.append(ChangeType.APPEND_ONLY, row(new Object[] {2, "world"}));
        builder.append(ChangeType.APPEND_ONLY, row(new Object[] {3, "test"}));
        builder.close();

        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());

        // Read back using the FULL schema
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID, FULL_SCHEMA);
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        FULL_ROW_TYPE, SCHEMA_ID, schemaGetter)) {
            Iterator<LogRecordBatch> batchIter = records.batches().iterator();
            assertThat(batchIter.hasNext()).isTrue();

            LogRecordBatch batch = batchIter.next();
            assertThat(batch.getRecordCount()).isEqualTo(3);

            // Verify batch attributes
            batch.ensureValid();

            // Verify records inline (pruned batches use a shared ProjectedRow and
            // temporary VectorSchemaRoot that is freed when the iterator closes).
            try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                // row 0: a=1, b="hello", c=null, d=null
                assertThat(iter.hasNext()).isTrue();
                InternalRow row0 = iter.next().getRow();
                assertThat(row0.getInt(0)).isEqualTo(1);
                assertThat(row0.getString(1).toString()).isEqualTo("hello");
                assertThat(row0.isNullAt(2)).isTrue();
                assertThat(row0.isNullAt(3)).isTrue();

                // row 1: a=2, b="world", c=null, d=null
                assertThat(iter.hasNext()).isTrue();
                InternalRow row1 = iter.next().getRow();
                assertThat(row1.getInt(0)).isEqualTo(2);
                assertThat(row1.getString(1).toString()).isEqualTo("world");
                assertThat(row1.isNullAt(2)).isTrue();
                assertThat(row1.isNullAt(3)).isTrue();

                // row 2: a=3, b="test", c=null, d=null
                assertThat(iter.hasNext()).isTrue();
                InternalRow row2 = iter.next().getRow();
                assertThat(row2.getInt(0)).isEqualTo(3);
                assertThat(row2.getString(1).toString()).isEqualTo("test");
                assertThat(row2.isNullAt(2)).isTrue();
                assertThat(row2.isNullAt(3)).isTrue();

                assertThat(iter.hasNext()).isFalse();
            }

            assertThat(batchIter.hasNext()).isFalse();
        }
    }

    @Test
    void testPrunedBatchNonContiguousColumns() throws Exception {
        // Target columns: [0, 2] -> (a INT, c INT) - non-contiguous
        int[] targetColumns = {0, 2};
        RowType prunedRowType = FULL_ROW_TYPE.project(targetColumns);

        ArrowWriter writer =
                writerPool.getOrCreateWriter(
                        1L, SCHEMA_ID, PAGE_SIZE, prunedRowType, NO_COMPRESSION);

        MemoryLogRecordsArrowBuilder builder =
                MemoryLogRecordsArrowBuilder.builder(
                        SCHEMA_ID,
                        writer,
                        new ManagedPagedOutputView(new TestingMemorySegmentPool(PAGE_SIZE)),
                        true,
                        null,
                        targetColumns);

        builder.append(ChangeType.APPEND_ONLY, row(new Object[] {10, 20}));
        builder.append(ChangeType.APPEND_ONLY, row(new Object[] {30, 40}));
        builder.close();

        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());

        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID, FULL_SCHEMA);
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        FULL_ROW_TYPE, SCHEMA_ID, schemaGetter)) {
            LogRecordBatch batch = records.batches().iterator().next();
            batch.ensureValid();

            // Verify records inline (same reason as testWriteAndReadPrunedBatch)
            try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                // row 0: a=10, b=null, c=20, d=null
                assertThat(iter.hasNext()).isTrue();
                InternalRow row0 = iter.next().getRow();
                assertThat(row0.getInt(0)).isEqualTo(10);
                assertThat(row0.isNullAt(1)).isTrue();
                assertThat(row0.getInt(2)).isEqualTo(20);
                assertThat(row0.isNullAt(3)).isTrue();

                // row 1: a=30, b=null, c=40, d=null
                assertThat(iter.hasNext()).isTrue();
                InternalRow row1 = iter.next().getRow();
                assertThat(row1.getInt(0)).isEqualTo(30);
                assertThat(row1.isNullAt(1)).isTrue();
                assertThat(row1.getInt(2)).isEqualTo(40);
                assertThat(row1.isNullAt(3)).isTrue();

                assertThat(iter.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testMixedFullAndPrunedBatches() throws Exception {
        // Build a full batch first
        ArrowWriter fullWriter =
                writerPool.getOrCreateWriter(
                        1L, SCHEMA_ID, PAGE_SIZE, FULL_ROW_TYPE, NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder fullBuilder =
                MemoryLogRecordsArrowBuilder.builder(
                        SCHEMA_ID,
                        fullWriter,
                        new ManagedPagedOutputView(new TestingMemorySegmentPool(PAGE_SIZE)),
                        true,
                        null,
                        null);
        fullBuilder.append(ChangeType.APPEND_ONLY, row(new Object[] {1, "a", 10, "x"}));
        fullBuilder.append(ChangeType.APPEND_ONLY, row(new Object[] {2, "b", 20, "y"}));
        fullBuilder.close();
        MemoryLogRecords fullRecords = MemoryLogRecords.pointToBytesView(fullBuilder.build());

        // Build a pruned batch
        int[] targetColumns = {0, 1};
        RowType prunedRowType = FULL_ROW_TYPE.project(targetColumns);
        ArrowWriter prunedWriter =
                writerPool.getOrCreateWriter(
                        2L, SCHEMA_ID, PAGE_SIZE, prunedRowType, NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder prunedBuilder =
                MemoryLogRecordsArrowBuilder.builder(
                        SCHEMA_ID,
                        prunedWriter,
                        new ManagedPagedOutputView(new TestingMemorySegmentPool(PAGE_SIZE)),
                        true,
                        null,
                        targetColumns);
        prunedBuilder.append(ChangeType.APPEND_ONLY, row(new Object[] {3, "c"}));
        prunedBuilder.append(ChangeType.APPEND_ONLY, row(new Object[] {4, "d"}));
        prunedBuilder.close();
        MemoryLogRecords prunedRecords = MemoryLogRecords.pointToBytesView(prunedBuilder.build());

        // Read the full batch - should have all 4 columns with values
        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID, FULL_SCHEMA);
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        FULL_ROW_TYPE, SCHEMA_ID, schemaGetter)) {

            // Verify full batch
            LogRecordBatch fullBatch = fullRecords.batches().iterator().next();
            fullBatch.ensureValid();
            try (CloseableIterator<LogRecord> iter = fullBatch.records(readContext)) {
                LogRecord r0 = iter.next();
                assertThat(r0.getRow().getInt(0)).isEqualTo(1);
                assertThat(r0.getRow().getString(1).toString()).isEqualTo("a");
                assertThat(r0.getRow().getInt(2)).isEqualTo(10);
                assertThat(r0.getRow().getString(3).toString()).isEqualTo("x");

                LogRecord r1 = iter.next();
                assertThat(r1.getRow().getInt(0)).isEqualTo(2);
                assertThat(r1.getRow().getString(1).toString()).isEqualTo("b");
                assertThat(r1.getRow().getInt(2)).isEqualTo(20);
                assertThat(r1.getRow().getString(3).toString()).isEqualTo("y");
            }

            // Verify pruned batch - absent columns are null
            LogRecordBatch prunedBatch = prunedRecords.batches().iterator().next();
            prunedBatch.ensureValid();
            try (CloseableIterator<LogRecord> iter = prunedBatch.records(readContext)) {
                LogRecord r0 = iter.next();
                assertThat(r0.getRow().getInt(0)).isEqualTo(3);
                assertThat(r0.getRow().getString(1).toString()).isEqualTo("c");
                assertThat(r0.getRow().isNullAt(2)).isTrue();
                assertThat(r0.getRow().isNullAt(3)).isTrue();

                LogRecord r1 = iter.next();
                assertThat(r1.getRow().getInt(0)).isEqualTo(4);
                assertThat(r1.getRow().getString(1).toString()).isEqualTo("d");
                assertThat(r1.getRow().isNullAt(2)).isTrue();
                assertThat(r1.getRow().isNullAt(3)).isTrue();
            }
        }
    }

    @Test
    void testPrunedBatchCrcValidity() throws Exception {
        int[] targetColumns = {1, 3};
        RowType prunedRowType = FULL_ROW_TYPE.project(targetColumns);

        ArrowWriter writer =
                writerPool.getOrCreateWriter(
                        1L, SCHEMA_ID, PAGE_SIZE, prunedRowType, NO_COMPRESSION);

        MemoryLogRecordsArrowBuilder builder =
                MemoryLogRecordsArrowBuilder.builder(
                        SCHEMA_ID,
                        writer,
                        new ManagedPagedOutputView(new TestingMemorySegmentPool(PAGE_SIZE)),
                        true,
                        null,
                        targetColumns);

        builder.append(ChangeType.APPEND_ONLY, row(new Object[] {"hello", "world"}));
        builder.close();

        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
        LogRecordBatch batch = records.batches().iterator().next();

        // CRC should be valid
        assertThat(batch.isValid()).isTrue();
        batch.ensureValid();
    }

    @Test
    void testNonPrunedBatchUnchanged() throws Exception {
        // Verify that non-pruned batches (targetColumns=null) still work correctly
        ArrowWriter writer =
                writerPool.getOrCreateWriter(
                        1L, SCHEMA_ID, PAGE_SIZE, FULL_ROW_TYPE, NO_COMPRESSION);

        MemoryLogRecordsArrowBuilder builder =
                MemoryLogRecordsArrowBuilder.builder(
                        SCHEMA_ID,
                        writer,
                        new ManagedPagedOutputView(new TestingMemorySegmentPool(PAGE_SIZE)),
                        true,
                        null,
                        null);

        builder.append(ChangeType.APPEND_ONLY, row(new Object[] {1, "a", 10, "x"}));
        builder.append(ChangeType.APPEND_ONLY, row(new Object[] {2, "b", 20, "y"}));
        builder.close();

        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());

        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID, FULL_SCHEMA);
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        FULL_ROW_TYPE, SCHEMA_ID, schemaGetter)) {
            LogRecordBatch batch = records.batches().iterator().next();
            batch.ensureValid();

            List<LogRecord> recordList = new ArrayList<>();
            try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                while (iter.hasNext()) {
                    recordList.add(iter.next());
                }
            }

            assertThat(recordList).hasSize(2);

            InternalRow row0 = recordList.get(0).getRow();
            assertThat(row0.getInt(0)).isEqualTo(1);
            assertThat(row0.getString(1).toString()).isEqualTo("a");
            assertThat(row0.getInt(2)).isEqualTo(10);
            assertThat(row0.getString(3).toString()).isEqualTo("x");

            InternalRow row1 = recordList.get(1).getRow();
            assertThat(row1.getInt(0)).isEqualTo(2);
            assertThat(row1.getString(1).toString()).isEqualTo("b");
            assertThat(row1.getInt(2)).isEqualTo(20);
            assertThat(row1.getString(3).toString()).isEqualTo("y");
        }
    }
}
