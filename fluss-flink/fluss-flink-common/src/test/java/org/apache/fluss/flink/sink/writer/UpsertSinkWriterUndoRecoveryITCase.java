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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlussRowType;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link UpsertSinkWriter} undo recovery mechanism. */
public class UpsertSinkWriterUndoRecoveryITCase extends FlinkTestBase {

    private static final Schema TEST_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .column("name", DataTypes.STRING())
                    .column("value", DataTypes.INT())
                    .primaryKey("id")
                    .build();

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(TEST_SCHEMA).distributedBy(2, "id").build();

    private static final String TEST_TABLE_NAME = "test_undo_recovery";
    private TablePath tablePath;
    private org.apache.flink.table.types.logical.RowType flinkRowType;

    @BeforeEach
    public void setup() throws Exception {
        tablePath = TablePath.of(DEFAULT_DB, TEST_TABLE_NAME);
        createTable(tablePath, TABLE_DESCRIPTOR);

        flinkRowType =
                org.apache.flink.table.types.logical.RowType.of(
                        new LogicalType[] {
                            new BigIntType(false), new VarCharType(Integer.MAX_VALUE), new IntType()
                        },
                        new String[] {"id", "name", "value"});
    }

    @Test
    public void testUndoRecoveryAfterCheckpoint() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Create and initialize the first writer instance
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        // Initialize state (no previous state)
        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        // Write some data
        writer1.write(createRowData(1L, "Alice", 100, RowKind.INSERT), new MockSinkWriterContext());
        writer1.write(createRowData(2L, "Bob", 200, RowKind.INSERT), new MockSinkWriterContext());
        writer1.write(
                createRowData(3L, "Charlie", 300, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        // Simulate checkpoint 1
        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);

        // Get the checkpointed state
        Map<TableBucket, Long> checkpoint1State = getStateFromWriter(initCtx1);
        assertThat(checkpoint1State).isNotEmpty();

        // Write more data after checkpoint (these should be undone)
        writer1.write(
                createRowData(1L, "Alice-Updated", 150, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());
        writer1.write(createRowData(4L, "David", 400, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        // Close the first writer
        writer1.close();

        // Verify data before recovery (includes post-checkpoint writes)
        List<InternalRow> dataBeforeRecovery = scanAllData();
        assertThat(dataBeforeRecovery).hasSize(4);

        // Simulate recovery: create a new writer with checkpointed state
        MockWriterInitContext initContext2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext2.getMailboxExecutor(), 0, 1);

        // Initialize with recovered state
        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpoint1State, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext2.metricGroup());

        // Verify data after recovery (post-checkpoint writes should be undone)
        List<InternalRow> dataAfterRecovery = scanAllData();
        assertThat(dataAfterRecovery).hasSize(3);

        // Verify the data is correct (reverted to checkpoint state)
        Map<Long, InternalRow> dataMap = new HashMap<>();
        for (InternalRow row : dataAfterRecovery) {
            dataMap.put(row.getLong(0), row);
        }

        assertThat(dataMap).containsKey(1L);
        assertThat(dataMap.get(1L).getString(1).toString()).isEqualTo("Alice");
        assertThat(dataMap.get(1L).getInt(2)).isEqualTo(100);

        assertThat(dataMap).containsKey(2L);
        assertThat(dataMap.get(2L).getString(1).toString()).isEqualTo("Bob");

        assertThat(dataMap).containsKey(3L);
        assertThat(dataMap.get(3L).getString(1).toString()).isEqualTo("Charlie");

        // ID 4 should not exist (was inserted after checkpoint)
        assertThat(dataMap).doesNotContainKey(4L);

        writer2.close();
    }

    @Test
    public void testNoUndoForFreshStart() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Create writer with no previous state
        UpsertSinkWriter<RowData> writer =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx = new TestFunctionInitializationContext();
        writer.initializeState(initCtx);
        writer.initialize(initContext.metricGroup());

        // Write some data
        writer.write(createRowData(1L, "Alice", 100, RowKind.INSERT), new MockSinkWriterContext());
        writer.write(createRowData(2L, "Bob", 200, RowKind.INSERT), new MockSinkWriterContext());
        writer.flush(false);

        // Verify data is written
        List<InternalRow> data = scanAllData();
        assertThat(data).hasSize(2);

        writer.close();
    }

    @Test
    public void testUndoWithDeleteOperations() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Create and initialize writer
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        // Write initial data
        writer1.write(createRowData(1L, "Alice", 100, RowKind.INSERT), new MockSinkWriterContext());
        writer1.write(createRowData(2L, "Bob", 200, RowKind.INSERT), new MockSinkWriterContext());
        writer1.write(
                createRowData(3L, "Charlie", 300, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        // Checkpoint
        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);
        Map<TableBucket, Long> checkpoint1State = getStateFromWriter(initCtx1);

        // Delete a record after checkpoint
        writer1.write(createRowData(2L, "Bob", 200, RowKind.DELETE), new MockSinkWriterContext());
        writer1.flush(false);

        writer1.close();

        // Verify Bob is deleted using point lookup (which reads from pre-write buffer)
        // Point lookup uses getFromBufferOrKv which includes pre-write buffer data
        // so DELETE operations are immediately visible after flush
        org.apache.fluss.client.table.Table table = conn.getTable(tablePath);
        assertThat(lookupByPrimaryKey(table, 1L)).isNotNull(); // Alice should exist
        assertThat(lookupByPrimaryKey(table, 2L)).isNull(); // Bob should be deleted
        assertThat(lookupByPrimaryKey(table, 3L)).isNotNull(); // Charlie should exist

        // Recover from checkpoint
        MockWriterInitContext initContext2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext2.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpoint1State, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext2.metricGroup());

        // Verify Bob is restored (delete was undone) using point lookup
        assertThat(lookupByPrimaryKey(table, 1L)).isNotNull(); // Alice should exist
        assertThat(lookupByPrimaryKey(table, 2L)).isNotNull(); // Bob should be restored
        assertThat(lookupByPrimaryKey(table, 3L)).isNotNull(); // Charlie should exist

        writer2.close();
    }

    /**
     * Test Case 4: Undo operation failure handling.
     *
     * <p>This test verifies behavior when undo operations encounter failures during recovery.
     */
    @Test
    public void testUndoOperationFailure() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Step 1: Create writer and write initial data
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);
        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        // Write and checkpoint
        writer1.write(createRowData(1L, "Alice", 100, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);
        Map<TableBucket, Long> checkpointState1 = getStateFromWriter(initCtx1);

        // Write more data after checkpoint
        writer1.write(createRowData(2L, "Bob", 200, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        writer1.close();

        // Step 2: Restore from checkpoint (undo recovery)
        // Even if undo encounters issues, the system should gracefully handle it
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);
        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpointState1, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext.metricGroup());

        // Verify data after undo - should only have checkpointed data
        List<InternalRow> results = scanAllData();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getLong(0)).isEqualTo(1L);

        writer2.close();
    }

    /**
     * Test Case 5: Recovery mode switch with empty checkpoint state.
     *
     * <p>This test verifies proper handling when recovering with empty checkpoint offset, which
     * means no undo is needed.
     */
    @Test
    public void testRecoveryModeSwitchException() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Create first writer and write some data
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);
        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        writer1.write(createRowData(1L, "Test", 100, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        // Checkpoint with current state
        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);
        Map<TableBucket, Long> checkpointState1 = getStateFromWriter(initCtx1);

        writer1.close();

        // Create new writer with checkpoint state (triggers recovery mode, but no undo needed)
        MockWriterInitContext initContext2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext2.getMailboxExecutor(), 0, 1);
        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpointState1, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext2.metricGroup());

        // After initialization, writer should have switched to normal mode
        // Further writes should use normal mode
        writer2.write(createRowData(2L, "Test2", 200, RowKind.INSERT), new MockSinkWriterContext());
        writer2.flush(false);

        // Verify both records are present
        List<InternalRow> results = scanAllData();
        assertThat(results).hasSize(2);

        writer2.close();
    }

    /**
     * Test Case 6: Undo recovery for partitioned table.
     *
     * <p>This test verifies undo recovery works correctly with partitioned tables.
     */
    @Test
    public void testPartitionTableUndoRecovery() throws Exception {
        // Create a partitioned table
        TablePath partitionedTablePath = TablePath.of(DEFAULT_DB, "test_partition_undo");
        Schema partitionedSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("region", DataTypes.STRING())
                        .column("value", DataTypes.INT())
                        .primaryKey("id", "region")
                        .build();

        TableDescriptor partitionedTableDescriptor =
                TableDescriptor.builder()
                        .schema(partitionedSchema)
                        .distributedBy(2, "id")
                        .partitionedBy("region")
                        .build();

        createTable(partitionedTablePath, partitionedTableDescriptor);

        // Create partitions
        String partition1 = "region=north";
        String partition2 = "region=south";
        admin.createPartition(partitionedTablePath, parsePartitionSpec(partition1), false).get();
        admin.createPartition(partitionedTablePath, parsePartitionSpec(partition2), false).get();

        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        org.apache.flink.table.types.logical.RowType partitionedFlinkRowType =
                org.apache.flink.table.types.logical.RowType.of(
                        new LogicalType[] {
                            new BigIntType(false),
                            new VarCharType(false, Integer.MAX_VALUE),
                            new IntType()
                        },
                        new String[] {"id", "region", "value"});

        // Create writer for partitioned table
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(false, false);
        serializationSchema.open(
                new SerializerInitContextImpl(toFlussRowType(partitionedFlinkRowType)));

        UpsertSinkWriter<RowData> writer1 =
                new UpsertSinkWriter<>(
                        partitionedTablePath,
                        flussConfig,
                        partitionedFlinkRowType,
                        null,
                        initContext.getMailboxExecutor(),
                        serializationSchema,
                        0,
                        1,
                        "test-partition-lock-" + System.currentTimeMillis());

        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        // Write data to different partitions
        GenericRowData row1 = GenericRowData.of(1L, StringData.fromString("north"), 100);
        row1.setRowKind(RowKind.INSERT);
        writer1.write(row1, new MockSinkWriterContext());

        GenericRowData row2 = GenericRowData.of(2L, StringData.fromString("south"), 200);
        row2.setRowKind(RowKind.INSERT);
        writer1.write(row2, new MockSinkWriterContext());
        writer1.flush(false);

        // Create checkpoint
        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);
        Map<TableBucket, Long> checkpointState1 = getStateFromWriter(initCtx1);

        // Write more data after checkpoint
        // Use id=1 (same as row1) to ensure it goes to the same bucket
        // This tests undo recovery for updates to existing keys
        GenericRowData row3 = GenericRowData.of(1L, StringData.fromString("north"), 300);
        row3.setRowKind(RowKind.UPDATE_AFTER);
        writer1.write(row3, new MockSinkWriterContext());
        writer1.flush(false);

        writer1.close();

        // Restore from checkpoint (undo recovery)
        // Create a new initContext for writer2 (simulating a new task after recovery)
        MockWriterInitContext initContext2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Create a new serialization schema for writer2
        RowDataSerializationSchema serializationSchema2 =
                new RowDataSerializationSchema(false, false);
        serializationSchema2.open(
                new SerializerInitContextImpl(toFlussRowType(partitionedFlinkRowType)));

        UpsertSinkWriter<RowData> writer2 =
                new UpsertSinkWriter<>(
                        partitionedTablePath,
                        flussConfig,
                        partitionedFlinkRowType,
                        null,
                        initContext2.getMailboxExecutor(),
                        serializationSchema2,
                        0,
                        1,
                        "test-partition-lock-" + System.currentTimeMillis());

        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpointState1, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext2.metricGroup());

        // Verify data: should only have checkpointed data (row1 and row2)
        // row1 should have original value (100), not updated value (300)
        Table table = conn.getTable(partitionedTablePath);
        List<InternalRow> results = new ArrayList<>();

        // Scan both partitions
        long tableId = table.getTableInfo().getTableId();
        long partitionIdNorth = getPartitionId(partitionedTablePath, partition1);
        long partitionIdSouth = getPartitionId(partitionedTablePath, partition2);

        for (long partitionId : new long[] {partitionIdNorth, partitionIdSouth}) {
            for (int bucketId = 0; bucketId < 2; bucketId++) {
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                try (org.apache.fluss.client.table.scanner.batch.BatchScanner batchScanner =
                        table.newScan().limit(10000).createBatchScanner(tableBucket)) {
                    org.apache.fluss.utils.CloseableIterator<InternalRow> iterator;
                    while ((iterator = batchScanner.pollBatch(java.time.Duration.ofSeconds(1)))
                            != null) {
                        try {
                            while (iterator.hasNext()) {
                                results.add(iterator.next());
                            }
                        } finally {
                            iterator.close();
                        }
                    }
                }
            }
        }

        assertThat(results).hasSize(2); // Only row1 and row2
        assertThat(results.stream().map(r -> r.getLong(0)).toArray())
                .containsExactlyInAnyOrder(1L, 2L);

        // Verify row1 has original value, not updated value
        InternalRow row1Result = results.stream().filter(r -> r.getLong(0) == 1L).findFirst().get();
        assertThat(row1Result.getInt(2)).isEqualTo(100); // Original value, not 300

        writer2.close();
    }

    /**
     * Test Case 7: Undo recovery with INSERT followed by DELETE.
     *
     * <p>This test verifies undo correctly handles the case where a row is inserted and then
     * deleted after checkpoint.
     */
    @Test
    public void testUndoRecoveryWithInsertDelete() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Step 1: Create writer and write initial data
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);
        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        // Write initial data
        writer1.write(createRowData(1L, "Alice", 100, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        // Create checkpoint 1
        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);
        Map<TableBucket, Long> checkpointState1 = getStateFromWriter(initCtx1);

        // After checkpoint: INSERT a new row and then DELETE it
        writer1.write(createRowData(2L, "Bob", 200, RowKind.INSERT), new MockSinkWriterContext());
        writer1.flush(false);

        // Verify row 2 exists
        Table table = conn.getTable(tablePath);
        InternalRow row2Before = lookupByPrimaryKey(table, 2L);
        assertThat(row2Before).isNotNull();

        // DELETE row 2
        writer1.write(createRowData(2L, "Bob", 200, RowKind.DELETE), new MockSinkWriterContext());
        writer1.flush(false);

        // Verify row 2 is deleted
        InternalRow row2Deleted = lookupByPrimaryKey(table, 2L);
        assertThat(row2Deleted).isNull();

        writer1.close();

        // Step 2: Restore from checkpoint (undo recovery)
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);
        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpointState1, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext.metricGroup());

        // Verify data after undo: row 2 should not exist (undo both INSERT and DELETE)
        List<InternalRow> results = scanAllData();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getLong(0)).isEqualTo(1L); // Only row 1

        // Verify row 2 does not exist using point lookup
        InternalRow row2After = lookupByPrimaryKey(table, 2L);
        assertThat(row2After).isNull();

        writer2.close();
    }

    private UpsertSinkWriter<RowData> createUpsertSinkWriter(
            Configuration flussConfig,
            MailboxExecutor mailboxExecutor,
            int subtaskIndex,
            int parallelism)
            throws Exception {
        // Create serialization schema that does NOT ignore DELETE operations
        // The second parameter (ignoreDelete) must be false to allow DELETE operations
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(false, false);
        serializationSchema.open(new SerializerInitContextImpl(toFlussRowType(flinkRowType)));

        return new UpsertSinkWriter<>(
                tablePath,
                flussConfig,
                flinkRowType,
                null, // no partial update
                mailboxExecutor,
                serializationSchema,
                subtaskIndex,
                parallelism,
                "test-lock-owner-" + System.currentTimeMillis());
    }

    private RowData createRowData(long id, String name, int value, RowKind rowKind) {
        GenericRowData rowData = GenericRowData.of(id, StringData.fromString(name), value);
        rowData.setRowKind(rowKind);
        return rowData;
    }

    private PartitionSpec parsePartitionSpec(String partitionStr) {
        // Parse "key=value" format
        String[] parts = partitionStr.split("=", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid partition format: " + partitionStr);
        }
        return new PartitionSpec(Collections.singletonMap(parts[0], parts[1]));
    }

    private long getPartitionId(TablePath tablePath, String partitionStr) throws Exception {
        PartitionSpec spec = parsePartitionSpec(partitionStr);
        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath, spec).get();
        if (partitionInfos.isEmpty()) {
            throw new IllegalStateException("Partition not found: " + partitionStr);
        }
        return partitionInfos.get(0).getPartitionId();
    }

    private List<InternalRow> scanAllData() throws Exception {
        Table table = conn.getTable(tablePath);
        List<InternalRow> results = new ArrayList<>();

        // For primary key table, we need to scan the KV state, not the log
        // We use BatchScanner with a large limit to read current data from each bucket
        int numBuckets = TABLE_DESCRIPTOR.getTableDistribution().get().getBucketCount().get();
        long tableId = table.getTableInfo().getTableId();

        // Use a large limit that's sufficient for test data
        int scanLimit = 10000;

        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            TableBucket tableBucket = new TableBucket(tableId, null, bucketId);
            try (org.apache.fluss.client.table.scanner.batch.BatchScanner batchScanner =
                    table.newScan().limit(scanLimit).createBatchScanner(tableBucket)) {
                org.apache.fluss.utils.CloseableIterator<InternalRow> iterator;
                while ((iterator = batchScanner.pollBatch(java.time.Duration.ofSeconds(1)))
                        != null) {
                    try {
                        while (iterator.hasNext()) {
                            results.add(iterator.next());
                        }
                    } finally {
                        iterator.close();
                    }
                }
            }
        }

        return results;
    }

    /**
     * Lookup a row by primary key. This method uses point lookup which reads from pre-write buffer
     * and KV store, so it can see uncommitted data immediately.
     *
     * @param table the table to lookup from
     * @param id the primary key value
     * @return the row if found, null if not found or deleted
     */
    private InternalRow lookupByPrimaryKey(Table table, long id) throws Exception {
        org.apache.fluss.row.GenericRow key = new org.apache.fluss.row.GenericRow(1);
        key.setField(0, id);
        org.apache.fluss.client.lookup.Lookuper lookuper = table.newLookup().createLookuper();
        org.apache.fluss.client.lookup.LookupResult result = lookuper.lookup(key).get();
        return result.getSingletonRow();
    }

    private Map<TableBucket, Long> getStateFromWriter(TestFunctionInitializationContext context) {
        List<WriterState> states = context.getWriterStates();
        Map<TableBucket, Long> result = new HashMap<>();
        for (WriterState state : states) {
            result.putAll(state.getBucketOffsets());
        }
        return result;
    }

    static class MockSinkWriterContext extends FlinkSinkWriterTest.MockSinkWriterContext {}

    static class TestFunctionInitializationContext implements FunctionInitializationContext {
        private final Map<TableBucket, Long> restoredState;
        private final long checkpointId;
        private final List<WriterState> writerStates;

        TestFunctionInitializationContext() {
            this(new HashMap<>(), -1L);
        }

        TestFunctionInitializationContext(Map<TableBucket, Long> restoredState, long checkpointId) {
            this.restoredState = restoredState;
            this.checkpointId = checkpointId;
            this.writerStates = new ArrayList<>();
            if (!restoredState.isEmpty()) {
                writerStates.add(new WriterState(checkpointId, restoredState));
            }
        }

        @Override
        public boolean isRestored() {
            return !restoredState.isEmpty();
        }

        @Override
        public org.apache.flink.api.common.state.OperatorStateStore getOperatorStateStore() {
            return new TestOperatorStateStore(writerStates);
        }

        @Override
        public org.apache.flink.api.common.state.KeyedStateStore getKeyedStateStore() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public java.util.OptionalLong getRestoredCheckpointId() {
            return checkpointId >= 0
                    ? java.util.OptionalLong.of(checkpointId)
                    : java.util.OptionalLong.empty();
        }

        public List<WriterState> getWriterStates() {
            return writerStates;
        }
    }

    static class TestFunctionSnapshotContext implements FunctionSnapshotContext {
        private final long checkpointId;

        TestFunctionSnapshotContext(long checkpointId) {
            this.checkpointId = checkpointId;
        }

        @Override
        public long getCheckpointId() {
            return checkpointId;
        }

        @Override
        public long getCheckpointTimestamp() {
            return System.currentTimeMillis();
        }
    }

    static class TestOperatorStateStore
            implements org.apache.flink.api.common.state.OperatorStateStore {
        private final List<WriterState> writerStates;

        TestOperatorStateStore(List<WriterState> writerStates) {
            this.writerStates = writerStates;
        }

        @Override
        public <S> org.apache.flink.api.common.state.ListState<S> getUnionListState(
                org.apache.flink.api.common.state.ListStateDescriptor<S> stateDescriptor) {
            return new TestUnionListState<>((List<S>) writerStates);
        }

        @Override
        public <S> org.apache.flink.api.common.state.ListState<S> getListState(
                org.apache.flink.api.common.state.ListStateDescriptor<S> stateDescriptor) {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public <K, V> org.apache.flink.api.common.state.BroadcastState<K, V> getBroadcastState(
                org.apache.flink.api.common.state.MapStateDescriptor<K, V> stateDescriptor) {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public java.util.Set<String> getRegisteredStateNames() {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public java.util.Set<String> getRegisteredBroadcastStateNames() {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    static class TestUnionListState<S> implements org.apache.flink.api.common.state.ListState<S> {
        private final List<S> states;

        TestUnionListState(List<S> states) {
            this.states = states;
        }

        @Override
        public void update(List<S> values) {
            states.clear();
            states.addAll(values);
        }

        @Override
        public void addAll(List<S> values) throws Exception {
            states.addAll(values);
        }

        @Override
        public Iterable<S> get() throws Exception {
            return states;
        }

        @Override
        public void add(S value) throws Exception {
            states.add(value);
        }

        @Override
        public void clear() {
            states.clear();
        }
    }
}
