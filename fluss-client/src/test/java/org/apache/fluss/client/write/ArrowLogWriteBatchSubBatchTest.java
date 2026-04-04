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

package org.apache.fluss.client.write;

import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
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

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordReadContext.createArrowReadContext;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for sub record batch behavior of {@link ArrowLogWriteBatch} with column pruning. */
public class ArrowLogWriteBatchSubBatchTest {

    private static final long TABLE_ID = 200001L;
    private static final int SCHEMA_ID = 1;

    // 4-column schema: (int a, String b, int c, String d)
    private static final RowType FULL_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("a", DataTypes.INT()),
                    new DataField("b", DataTypes.STRING()),
                    new DataField("c", DataTypes.INT()),
                    new DataField("d", DataTypes.STRING()));

    private static final int[] TARGET_COLUMNS = new int[] {0, 1};

    // Pruned schema: (int a, String b)
    private static final RowType PRUNED_ROW_TYPE = FULL_ROW_TYPE.project(TARGET_COLUMNS);

    private static final Schema FULL_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("c", DataTypes.INT())
                    .column("d", DataTypes.STRING())
                    .build();

    private static final Schema PRUNED_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .build();

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_sub_batch_table");
    private static final PhysicalTablePath PHYSICAL_TABLE_PATH = PhysicalTablePath.of(TABLE_PATH);

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(FULL_SCHEMA).distributedBy(3).build();

    private static final TableInfo TABLE_INFO =
            TableInfo.of(
                    TABLE_PATH,
                    TABLE_ID,
                    SCHEMA_ID,
                    TABLE_DESCRIPTOR,
                    "fluss-data",
                    System.currentTimeMillis(),
                    System.currentTimeMillis());

    private BufferAllocator allocator;
    private ArrowWriterPool writerPool;

    @BeforeEach
    void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        writerPool = new ArrowWriterPool(allocator);
    }

    @AfterEach
    void teardown() {
        writerPool.close();
        allocator.close();
    }

    @Test
    void testSchemaSwitchClosesFullBatchForPrunedRow() throws Exception {
        int bucketId = 0;
        int maxSizeInBytes = 4096;

        // Create a full-schema batch (targetColumns=null)
        ArrowLogWriteBatch fullBatch = createBatch(bucketId, maxSizeInBytes, FULL_ROW_TYPE, null);

        // Append 3 full rows
        assertThat(fullBatch.tryAppend(fullWriteRecord(row(1, "x1", 10, "y1")), noopCallback()))
                .isTrue();
        assertThat(fullBatch.tryAppend(fullWriteRecord(row(2, "x2", 20, "y2")), noopCallback()))
                .isTrue();
        assertThat(fullBatch.tryAppend(fullWriteRecord(row(3, "x3", 30, "y3")), noopCallback()))
                .isTrue();

        // Attempt to append a pruned row -> should return false (targetColumns mismatch)
        assertThat(
                        fullBatch.tryAppend(
                                prunedWriteRecord(row(4, "x4", 40, "y4"), TARGET_COLUMNS),
                                noopCallback()))
                .isFalse();

        // Close and verify the full batch
        fullBatch.close();
        BytesView fullBytesView = fullBatch.build();
        MemoryLogRecords fullRecords = MemoryLogRecords.pointToBytesView(fullBytesView);

        TestingSchemaGetter fullSchemaGetter = new TestingSchemaGetter(SCHEMA_ID, FULL_SCHEMA);
        LogRecordBatch batch = fullRecords.batches().iterator().next();
        assertThat(batch.getRecordCount()).isEqualTo(3);

        try (LogRecordReadContext readContext =
                        createArrowReadContext(FULL_ROW_TYPE, SCHEMA_ID, fullSchemaGetter);
                CloseableIterator<LogRecord> recordIter = batch.records(readContext)) {
            LogRecord r1 = recordIter.next();
            assertThat(r1.getRow().getInt(0)).isEqualTo(1);
            assertThat(r1.getRow().getString(1).toString()).isEqualTo("x1");
            assertThat(r1.getRow().getInt(2)).isEqualTo(10);
            assertThat(r1.getRow().getString(3).toString()).isEqualTo("y1");

            LogRecord r2 = recordIter.next();
            assertThat(r2.getRow().getInt(0)).isEqualTo(2);
            assertThat(r2.getRow().getString(1).toString()).isEqualTo("x2");

            LogRecord r3 = recordIter.next();
            assertThat(r3.getRow().getInt(0)).isEqualTo(3);
            assertThat(r3.getRow().getString(1).toString()).isEqualTo("x3");

            assertThat(recordIter.hasNext()).isFalse();
        }
    }

    @Test
    void testPrunedBatchClosedByFullRow() throws Exception {
        int bucketId = 0;
        int maxSizeInBytes = 4096;

        // Create a pruned-schema batch (targetColumns=[0,1])
        ArrowLogWriteBatch prunedBatch =
                createBatch(bucketId, maxSizeInBytes, PRUNED_ROW_TYPE, TARGET_COLUMNS);

        // Append 2 pruned rows (full rows projected via ProjectedRow inside tryAppend)
        assertThat(
                        prunedBatch.tryAppend(
                                prunedWriteRecord(row(4, "x4", 40, "y4"), TARGET_COLUMNS),
                                noopCallback()))
                .isTrue();
        assertThat(
                        prunedBatch.tryAppend(
                                prunedWriteRecord(row(5, "x5", 50, "y5"), TARGET_COLUMNS),
                                noopCallback()))
                .isTrue();

        // Attempt to append a full row -> should return false
        assertThat(prunedBatch.tryAppend(fullWriteRecord(row(6, "x6", 60, "y6")), noopCallback()))
                .isFalse();

        // Close and verify the pruned batch
        prunedBatch.close();
        BytesView prunedBytesView = prunedBatch.build();
        MemoryLogRecords prunedRecords = MemoryLogRecords.pointToBytesView(prunedBytesView);

        TestingSchemaGetter prunedSchemaGetter = new TestingSchemaGetter(SCHEMA_ID, PRUNED_SCHEMA);
        LogRecordBatch batch = prunedRecords.batches().iterator().next();
        assertThat(batch.getRecordCount()).isEqualTo(2);

        try (LogRecordReadContext readContext =
                        createArrowReadContext(PRUNED_ROW_TYPE, SCHEMA_ID, prunedSchemaGetter);
                CloseableIterator<LogRecord> recordIter = batch.records(readContext)) {
            // Pruned rows should contain only columns a and b
            LogRecord r1 = recordIter.next();
            assertThat(r1.getRow().getFieldCount()).isEqualTo(2);
            assertThat(r1.getRow().getInt(0)).isEqualTo(4);
            assertThat(r1.getRow().getString(1).toString()).isEqualTo("x4");

            LogRecord r2 = recordIter.next();
            assertThat(r2.getRow().getFieldCount()).isEqualTo(2);
            assertThat(r2.getRow().getInt(0)).isEqualTo(5);
            assertThat(r2.getRow().getString(1).toString()).isEqualTo("x5");

            assertThat(recordIter.hasNext()).isFalse();
        }

        // Also verify reading with the FULL schema expands absent columns to null
        TestingSchemaGetter fullSchemaGetter = new TestingSchemaGetter(SCHEMA_ID, FULL_SCHEMA);
        LogRecordBatch fullBatch = prunedRecords.batches().iterator().next();
        try (LogRecordReadContext fullReadContext =
                        createArrowReadContext(FULL_ROW_TYPE, SCHEMA_ID, fullSchemaGetter);
                CloseableIterator<LogRecord> recordIter = fullBatch.records(fullReadContext)) {
            LogRecord r1 = recordIter.next();
            assertThat(r1.getRow().getInt(0)).isEqualTo(4);
            assertThat(r1.getRow().getString(1).toString()).isEqualTo("x4");
            assertThat(r1.getRow().isNullAt(2)).isTrue();
            assertThat(r1.getRow().isNullAt(3)).isTrue();

            LogRecord r2 = recordIter.next();
            assertThat(r2.getRow().getInt(0)).isEqualTo(5);
            assertThat(r2.getRow().getString(1).toString()).isEqualTo("x5");
            assertThat(r2.getRow().isNullAt(2)).isTrue();
            assertThat(r2.getRow().isNullAt(3)).isTrue();

            assertThat(recordIter.hasNext()).isFalse();
        }
    }

    @Test
    void testOnlyFullRows() throws Exception {
        int bucketId = 0;
        int maxSizeInBytes = 4096;

        ArrowLogWriteBatch batch = createBatch(bucketId, maxSizeInBytes, FULL_ROW_TYPE, null);

        assertThat(batch.tryAppend(fullWriteRecord(row(1, "a1", 10, "b1")), noopCallback()))
                .isTrue();
        assertThat(batch.tryAppend(fullWriteRecord(row(2, "a2", 20, "b2")), noopCallback()))
                .isTrue();

        batch.close();
        BytesView bytesView = batch.build();
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(bytesView);

        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID, FULL_SCHEMA);
        LogRecordBatch recordBatch = records.batches().iterator().next();
        assertThat(recordBatch.getRecordCount()).isEqualTo(2);

        try (LogRecordReadContext readContext =
                        createArrowReadContext(FULL_ROW_TYPE, SCHEMA_ID, schemaGetter);
                CloseableIterator<LogRecord> recordIter = recordBatch.records(readContext)) {
            LogRecord r1 = recordIter.next();
            assertThat(r1.getRow().getInt(0)).isEqualTo(1);
            assertThat(r1.getRow().getString(1).toString()).isEqualTo("a1");
            assertThat(r1.getRow().getInt(2)).isEqualTo(10);
            assertThat(r1.getRow().getString(3).toString()).isEqualTo("b1");

            LogRecord r2 = recordIter.next();
            assertThat(r2.getRow().getInt(0)).isEqualTo(2);

            assertThat(recordIter.hasNext()).isFalse();
        }
    }

    @Test
    void testOnlyPrunedRows() throws Exception {
        int bucketId = 0;
        int maxSizeInBytes = 4096;

        ArrowLogWriteBatch batch =
                createBatch(bucketId, maxSizeInBytes, PRUNED_ROW_TYPE, TARGET_COLUMNS);

        assertThat(
                        batch.tryAppend(
                                prunedWriteRecord(row(1, "a1", 10, "b1"), TARGET_COLUMNS),
                                noopCallback()))
                .isTrue();
        assertThat(
                        batch.tryAppend(
                                prunedWriteRecord(row(2, "a2", 20, "b2"), TARGET_COLUMNS),
                                noopCallback()))
                .isTrue();
        assertThat(
                        batch.tryAppend(
                                prunedWriteRecord(row(3, "a3", 30, "b3"), TARGET_COLUMNS),
                                noopCallback()))
                .isTrue();

        batch.close();
        BytesView bytesView = batch.build();
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(bytesView);

        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID, PRUNED_SCHEMA);
        LogRecordBatch recordBatch = records.batches().iterator().next();
        assertThat(recordBatch.getRecordCount()).isEqualTo(3);

        try (LogRecordReadContext readContext =
                        createArrowReadContext(PRUNED_ROW_TYPE, SCHEMA_ID, schemaGetter);
                CloseableIterator<LogRecord> recordIter = recordBatch.records(readContext)) {
            LogRecord r1 = recordIter.next();
            assertThat(r1.getRow().getFieldCount()).isEqualTo(2);
            assertThat(r1.getRow().getInt(0)).isEqualTo(1);
            assertThat(r1.getRow().getString(1).toString()).isEqualTo("a1");

            LogRecord r2 = recordIter.next();
            assertThat(r2.getRow().getInt(0)).isEqualTo(2);
            assertThat(r2.getRow().getString(1).toString()).isEqualTo("a2");

            LogRecord r3 = recordIter.next();
            assertThat(r3.getRow().getInt(0)).isEqualTo(3);
            assertThat(r3.getRow().getString(1).toString()).isEqualTo("a3");

            assertThat(recordIter.hasNext()).isFalse();
        }

        // Also verify reading with the FULL schema expands absent columns to null
        TestingSchemaGetter fullSchemaGetter = new TestingSchemaGetter(SCHEMA_ID, FULL_SCHEMA);
        LogRecordBatch fullBatch = records.batches().iterator().next();
        try (LogRecordReadContext fullReadContext =
                        createArrowReadContext(FULL_ROW_TYPE, SCHEMA_ID, fullSchemaGetter);
                CloseableIterator<LogRecord> recordIter = fullBatch.records(fullReadContext)) {
            LogRecord r1 = recordIter.next();
            assertThat(r1.getRow().getInt(0)).isEqualTo(1);
            assertThat(r1.getRow().getString(1).toString()).isEqualTo("a1");
            assertThat(r1.getRow().isNullAt(2)).isTrue();
            assertThat(r1.getRow().isNullAt(3)).isTrue();

            LogRecord r2 = recordIter.next();
            assertThat(r2.getRow().getInt(0)).isEqualTo(2);
            assertThat(r2.getRow().getString(1).toString()).isEqualTo("a2");
            assertThat(r2.getRow().isNullAt(2)).isTrue();
            assertThat(r2.getRow().isNullAt(3)).isTrue();

            LogRecord r3 = recordIter.next();
            assertThat(r3.getRow().getInt(0)).isEqualTo(3);
            assertThat(r3.getRow().getString(1).toString()).isEqualTo("a3");
            assertThat(r3.getRow().isNullAt(2)).isTrue();
            assertThat(r3.getRow().isNullAt(3)).isTrue();

            assertThat(recordIter.hasNext()).isFalse();
        }
    }

    @Test
    void testProjectedRowCorrectness() throws Exception {
        int bucketId = 0;
        int maxSizeInBytes = 4096;

        // Create pruned batch with targetColumns=[0,1]
        ArrowLogWriteBatch batch =
                createBatch(bucketId, maxSizeInBytes, PRUNED_ROW_TYPE, TARGET_COLUMNS);

        // Append a full row - the batch should project it to only (a, b)
        GenericRow fullRow = row(42, "hello", 99, "world");
        assertThat(batch.tryAppend(prunedWriteRecord(fullRow, TARGET_COLUMNS), noopCallback()))
                .isTrue();

        batch.close();
        BytesView bytesView = batch.build();
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(bytesView);

        TestingSchemaGetter schemaGetter = new TestingSchemaGetter(SCHEMA_ID, PRUNED_SCHEMA);
        LogRecordBatch recordBatch = records.batches().iterator().next();
        assertThat(recordBatch.getRecordCount()).isEqualTo(1);

        try (LogRecordReadContext readContext =
                        createArrowReadContext(PRUNED_ROW_TYPE, SCHEMA_ID, schemaGetter);
                CloseableIterator<LogRecord> recordIter = recordBatch.records(readContext)) {
            LogRecord r = recordIter.next();
            // Should only have 2 fields: a=42, b="hello"
            assertThat(r.getRow().getFieldCount()).isEqualTo(2);
            assertThat(r.getRow().getInt(0)).isEqualTo(42);
            assertThat(r.getRow().getString(1).toString()).isEqualTo("hello");

            assertThat(recordIter.hasNext()).isFalse();
        }

        // Also verify reading with the FULL schema expands absent columns to null
        TestingSchemaGetter fullSchemaGetter = new TestingSchemaGetter(SCHEMA_ID, FULL_SCHEMA);
        LogRecordBatch fullBatch = records.batches().iterator().next();
        try (LogRecordReadContext fullReadContext =
                        createArrowReadContext(FULL_ROW_TYPE, SCHEMA_ID, fullSchemaGetter);
                CloseableIterator<LogRecord> recordIter = fullBatch.records(fullReadContext)) {
            LogRecord r = recordIter.next();
            assertThat(r.getRow().getInt(0)).isEqualTo(42);
            assertThat(r.getRow().getString(1).toString()).isEqualTo("hello");
            assertThat(r.getRow().isNullAt(2)).isTrue();
            assertThat(r.getRow().isNullAt(3)).isTrue();

            assertThat(recordIter.hasNext()).isFalse();
        }
    }

    // ----------------------------- Helper methods -----------------------------

    private ArrowLogWriteBatch createBatch(
            int bucketId, int maxSizeInBytes, RowType rowType, int[] targetColumns) {
        String keySuffix =
                targetColumns != null ? "-pruned-" + java.util.Arrays.toString(targetColumns) : "";
        ArrowWriter arrowWriter =
                writerPool.getOrCreateWriter(
                        TABLE_ID,
                        SCHEMA_ID,
                        maxSizeInBytes,
                        rowType,
                        DEFAULT_COMPRESSION,
                        keySuffix);
        return new ArrowLogWriteBatch(
                bucketId,
                PHYSICAL_TABLE_PATH,
                SCHEMA_ID,
                arrowWriter,
                new UnmanagedPagedOutputView(256),
                System.currentTimeMillis(),
                null,
                targetColumns);
    }

    private WriteRecord fullWriteRecord(GenericRow row) {
        return WriteRecord.forArrowAppend(TABLE_INFO, PHYSICAL_TABLE_PATH, row, null);
    }

    private WriteRecord prunedWriteRecord(GenericRow row, int[] targetColumns) {
        return WriteRecord.forArrowAppend(
                TABLE_INFO, PHYSICAL_TABLE_PATH, row, null, targetColumns);
    }

    private WriteCallback noopCallback() {
        return (bucket, offset, exception) -> {};
    }
}
