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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ColumnLockConflictException;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlussRowType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for Aggregation Merge Engine with concurrent Flink jobs.
 *
 * <p>This test verifies that multiple concurrent Flink jobs writing to the same aggregation table
 * maintain consistency under various scenarios:
 *
 * <ul>
 *   <li>Multiple jobs updating the same columns are blocked by column lock (consistency protection)
 *   <li>Sequential job writes maintain proper aggregation
 *   <li>Concurrent writes from different subtasks of the same job work correctly
 * </ul>
 */
public class AggregationConcurrentJobsITCase extends FlinkTestBase {

    // Schema: product_id, total_sales, max_price, min_price, last_update_time
    private static final Schema TEST_SCHEMA =
            Schema.newBuilder()
                    .column("product_id", DataTypes.BIGINT())
                    .column("total_sales", DataTypes.BIGINT())
                    .column("max_price", DataTypes.DECIMAL(10, 2))
                    .column("min_price", DataTypes.DECIMAL(10, 2))
                    .column("last_update_time", DataTypes.TIMESTAMP(3))
                    .primaryKey("product_id")
                    .build();

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(TEST_SCHEMA)
                    .distributedBy(2, "product_id")
                    .property(
                            ConfigOptions.TABLE_MERGE_ENGINE.key(),
                            MergeEngineType.AGGREGATE.name())
                    .property(ConfigOptions.TABLE_COLUMN_LOCK_ENABLED.key(), "true")
                    .property("table.merge-engine.aggregate.total_sales", "sum")
                    .property("table.merge-engine.aggregate.max_price", "max")
                    .property("table.merge-engine.aggregate.min_price", "min")
                    .property("table.merge-engine.aggregate.last_update_time", "last_value")
                    .build();

    private static final String TEST_TABLE_NAME = "test_concurrent_jobs";
    private TablePath tablePath;
    private org.apache.flink.table.types.logical.RowType flinkRowType;

    @BeforeEach
    public void setup() throws Exception {
        tablePath = TablePath.of(DEFAULT_DB, TEST_TABLE_NAME);
        createTable(tablePath, TABLE_DESCRIPTOR);

        flinkRowType =
                org.apache.flink.table.types.logical.RowType.of(
                        new LogicalType[] {
                            new BigIntType(false), // product_id
                            new BigIntType(), // total_sales
                            new DecimalType(10, 2), // max_price
                            new DecimalType(10, 2), // min_price
                            new TimestampType(3) // last_update_time
                        },
                        new String[] {
                            "product_id",
                            "total_sales",
                            "max_price",
                            "min_price",
                            "last_update_time"
                        });
    }

    /**
     * Test Case 1: Column lock prevents concurrent jobs from updating the same columns.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Job A acquires lock and starts writing (all columns)
     *   <li>Job B tries to acquire lock for same columns - should fail with
     *       ColumnLockConflictException
     *   <li>This verifies that consistency is protected by column lock mechanism
     * </ul>
     */
    @Test
    public void testColumnLock_PreventsConcurrentJobsOnSameColumns() throws Exception {
        Configuration flussConfig = clientConf;

        // Job A: successfully acquires lock for all columns
        String lockOwnerA = "job-a-" + System.currentTimeMillis();
        UpsertSinkWriter<RowData> writerA =
                createUpsertSinkWriter(flussConfig, lockOwnerA, null, 0, 1);
        TestFunctionInitializationContext initCtxA = new TestFunctionInitializationContext();
        writerA.initializeState(initCtxA);
        MockWriterInitContext initContextA =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        writerA.initialize(initContextA.metricGroup());

        LocalDateTime time = LocalDateTime.of(2024, 1, 1, 10, 0, 0);

        // Job A writes successfully
        writerA.write(
                createRowData(1L, 100L, "50.00", "50.00", time, RowKind.INSERT),
                new MockSinkWriterContext());
        writerA.flush(false);

        // Job B: tries to acquire lock for same columns - should fail
        String lockOwnerB = "job-b-" + System.currentTimeMillis();
        UpsertSinkWriter<RowData> writerB =
                createUpsertSinkWriter(flussConfig, lockOwnerB, null, 0, 1);
        TestFunctionInitializationContext initCtxB = new TestFunctionInitializationContext();
        writerB.initializeState(initCtxB);
        MockWriterInitContext initContextB =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Verify that Job B fails to initialize due to column lock conflict
        assertThatThrownBy(() -> writerB.initialize(initContextB.metricGroup()))
                .hasMessageContaining("Column lock conflict")
                .cause()
                .isInstanceOf(ColumnLockConflictException.class);

        writerA.close();
        writerB.close();
    }

    /**
     * Test Case 2: Sequential job writes (one job completes before another starts).
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Job A writes data and completes (releases lock)
     *   <li>Job B starts and writes more data (acquires lock successfully)
     *   <li>Verify aggregation is correct after both jobs
     * </ul>
     */
    @Test
    public void testSequentialJobs_MaintainAggregationConsistency() throws Exception {
        Configuration flussConfig = clientConf;

        // Job A writes data
        String lockOwnerA = "job-a-" + System.currentTimeMillis();
        UpsertSinkWriter<RowData> writerA =
                createUpsertSinkWriter(flussConfig, lockOwnerA, null, 0, 1);
        TestFunctionInitializationContext initCtxA = new TestFunctionInitializationContext();
        writerA.initializeState(initCtxA);
        writerA.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        LocalDateTime time1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0);
        writerA.write(
                createRowData(1L, 100L, "50.00", "50.00", time1, RowKind.INSERT),
                new MockSinkWriterContext());
        writerA.flush(false);

        // Job A completes and releases lock
        writerA.close();

        // Job B starts after Job A completes
        // Lock is released synchronously on writer close, no additional wait needed
        String lockOwnerB = "job-b-" + System.currentTimeMillis();
        UpsertSinkWriter<RowData> writerB =
                createUpsertSinkWriter(flussConfig, lockOwnerB, null, 0, 1);
        TestFunctionInitializationContext initCtxB = new TestFunctionInitializationContext();
        writerB.initializeState(initCtxB);
        writerB.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        LocalDateTime time2 = LocalDateTime.of(2024, 1, 1, 11, 0, 0);
        writerB.write(
                createRowData(1L, 200L, "75.00", "40.00", time2, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());
        writerB.flush(false);

        writerB.close();

        // Verify aggregation results
        List<InternalRow> results = scanAllData();
        assertThat(results).hasSize(1);

        InternalRow row = results.get(0);
        assertThat(row.getLong(0)).isEqualTo(1L); // product_id

        // total_sales: 100 + 200 = 300
        assertThat(row.getLong(1)).isEqualTo(300L);

        // max_price: max(50.00, 75.00) = 75.00
        assertThat(row.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("75.00"));

        // min_price: min(50.00, 40.00) = 40.00
        assertThat(row.getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("40.00"));

        // last_update_time: time2
        assertThat(row.getTimestampNtz(4, 3).toLocalDateTime()).isEqualTo(time2);
    }

    /**
     * Test Case 3: Multiple subtasks of the same job write concurrently.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Single job with parallelism=2 (two subtasks)
     *   <li>Both subtasks share the same lock owner ID
     *   <li>Both subtasks write to different rows
     *   <li>Verify all data is written correctly
     * </ul>
     */
    @Test
    public void testSameJob_MultipleSubtasks_WriteConcurrently() throws Exception {
        Configuration flussConfig = clientConf;

        // Same job, same lock owner, but different subtask indexes
        String sharedLockOwner = "shared-job-" + System.currentTimeMillis();

        // Subtask 0
        UpsertSinkWriter<RowData> writer0 =
                createUpsertSinkWriter(flussConfig, sharedLockOwner, null, 0, 2);
        TestFunctionInitializationContext initCtx0 = new TestFunctionInitializationContext();
        writer0.initializeState(initCtx0);
        writer0.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        // Subtask 1
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, sharedLockOwner, null, 1, 2);
        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        LocalDateTime time = LocalDateTime.of(2024, 1, 1, 10, 0, 0);

        // Subtask 0 writes to product 1, 3
        writer0.write(
                createRowData(1L, 100L, "50.00", "50.00", time, RowKind.INSERT),
                new MockSinkWriterContext());
        writer0.write(
                createRowData(3L, 300L, "70.00", "70.00", time, RowKind.INSERT),
                new MockSinkWriterContext());
        writer0.flush(false);

        // Subtask 1 writes to product 2, 4
        writer1.write(
                createRowData(2L, 200L, "60.00", "60.00", time, RowKind.INSERT),
                new MockSinkWriterContext());
        writer1.write(
                createRowData(4L, 400L, "80.00", "80.00", time, RowKind.INSERT),
                new MockSinkWriterContext());
        writer1.flush(false);

        writer0.close();
        writer1.close();

        // Verify all products are written
        List<InternalRow> results = scanAllData();
        assertThat(results).hasSize(4);

        Map<Long, InternalRow> resultMap = buildDataMap(results);
        assertThat(resultMap).containsKeys(1L, 2L, 3L, 4L);

        // Verify each product
        assertThat(resultMap.get(1L).getLong(1)).isEqualTo(100L);
        assertThat(resultMap.get(2L).getLong(1)).isEqualTo(200L);
        assertThat(resultMap.get(3L).getLong(1)).isEqualTo(300L);
        assertThat(resultMap.get(4L).getLong(1)).isEqualTo(400L);
    }

    /**
     * Test Case 4: Multiple sequential aggregations on same primary key.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Job A writes multiple updates to the same key
     *   <li>Job B (after Job A completes) writes more updates to the same key
     *   <li>Verify final aggregation is correct
     * </ul>
     */
    @Test
    public void testSequentialJobs_MultipleAggregations_SameKey() throws Exception {
        Configuration flussConfig = clientConf;

        // Job A: multiple writes
        String lockOwnerA = "job-a-" + System.currentTimeMillis();
        UpsertSinkWriter<RowData> writerA =
                createUpsertSinkWriter(flussConfig, lockOwnerA, null, 0, 1);
        TestFunctionInitializationContext initCtxA = new TestFunctionInitializationContext();
        writerA.initializeState(initCtxA);
        writerA.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        LocalDateTime time1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0);
        LocalDateTime time2 = LocalDateTime.of(2024, 1, 1, 11, 0, 0);

        // Job A: first write
        writerA.write(
                createRowData(1L, 100L, "50.00", "50.00", time1, RowKind.INSERT),
                new MockSinkWriterContext());

        // Job A: second write to same key
        writerA.write(
                createRowData(1L, 50L, "60.00", "45.00", time2, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writerA.flush(false);
        writerA.close();

        // Verify intermediate result
        List<InternalRow> intermediateResults = scanAllData();
        assertThat(intermediateResults).hasSize(1);
        InternalRow intermediateRow = intermediateResults.get(0);
        // total_sales: 100 + 50 = 150
        assertThat(intermediateRow.getLong(1)).isEqualTo(150L);
        // max_price: max(50, 60) = 60
        assertThat(intermediateRow.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("60.00"));
        // min_price: min(50, 45) = 45
        assertThat(intermediateRow.getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("45.00"));

        // Job B: more writes to same key
        // writerA has already closed and released lock, can proceed immediately
        String lockOwnerB = "job-b-" + System.currentTimeMillis();
        UpsertSinkWriter<RowData> writerB =
                createUpsertSinkWriter(flussConfig, lockOwnerB, null, 0, 1);
        TestFunctionInitializationContext initCtxB = new TestFunctionInitializationContext();
        writerB.initializeState(initCtxB);
        writerB.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        LocalDateTime time3 = LocalDateTime.of(2024, 1, 1, 12, 0, 0);
        LocalDateTime time4 = LocalDateTime.of(2024, 1, 1, 13, 0, 0);

        // Job B: first write
        writerB.write(
                createRowData(1L, 75L, "70.00", "48.00", time3, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        // Job B: second write
        writerB.write(
                createRowData(1L, 25L, "55.00", "55.00", time4, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writerB.flush(false);
        writerB.close();

        // Verify final result
        List<InternalRow> finalResults = scanAllData();
        assertThat(finalResults).hasSize(1);

        InternalRow finalRow = finalResults.get(0);
        assertThat(finalRow.getLong(0)).isEqualTo(1L); // product_id

        // total_sales: 100 + 50 + 75 + 25 = 250
        assertThat(finalRow.getLong(1)).isEqualTo(250L);

        // max_price: max(50, 60, 70, 55) = 70.00
        assertThat(finalRow.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("70.00"));

        // min_price: min(50, 45, 48, 55) = 45.00
        assertThat(finalRow.getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("45.00"));

        // last_update_time: time4 (last value)
        assertThat(finalRow.getTimestampNtz(4, 3).toLocalDateTime()).isEqualTo(time4);
    }

    /**
     * Test Case 5: Multiple concurrent jobs writing to different columns have no conflict.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Job A writes to column subset [total_sales, last_update_time]
     *   <li>Job B writes to column subset [max_price, min_price]
     *   <li>Both jobs run concurrently (no column lock conflict)
     *   <li>Verify both jobs write successfully and aggregation is correct
     * </ul>
     */
    @Test
    public void testConcurrentJobs_DifferentColumns_NoConflict() throws Exception {
        Configuration flussConfig = clientConf;

        // Job A: writes to [product_id, total_sales, last_update_time]
        String lockOwnerA = "job-a-" + System.currentTimeMillis();
        int[] columnsA = new int[] {0, 1, 4}; // product_id (PK), total_sales, last_update_time
        UpsertSinkWriter<RowData> writerA =
                createUpsertSinkWriter(flussConfig, lockOwnerA, columnsA, 0, 1);
        TestFunctionInitializationContext initCtxA = new TestFunctionInitializationContext();
        writerA.initializeState(initCtxA);
        writerA.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        // Job B: writes to [product_id, max_price, min_price]
        String lockOwnerB = "job-b-" + System.currentTimeMillis();
        int[] columnsB = new int[] {0, 2, 3}; // product_id (PK), max_price, min_price
        UpsertSinkWriter<RowData> writerB =
                createUpsertSinkWriter(flussConfig, lockOwnerB, columnsB, 0, 1);
        TestFunctionInitializationContext initCtxB = new TestFunctionInitializationContext();
        writerB.initializeState(initCtxB);
        // Job B should successfully initialize (no column conflict with Job A)
        writerB.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        LocalDateTime time1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0);
        LocalDateTime time2 = LocalDateTime.of(2024, 1, 1, 11, 0, 0);

        // Job A writes total_sales=100, last_update_time=time1
        // Note: max_price and min_price are null in this write
        RowData rowA = createRowData(1L, 100L, null, null, time1, RowKind.INSERT);
        writerA.write(rowA, new MockSinkWriterContext());
        writerA.flush(false);

        // Job B writes max_price=50.00, min_price=50.00
        // Note: total_sales is null in this write, last_update_time=time2
        RowData rowB = createRowData(1L, null, "50.00", "50.00", time2, RowKind.UPDATE_AFTER);
        writerB.write(rowB, new MockSinkWriterContext());
        writerB.flush(false);

        // Both jobs write more updates
        writerA.write(
                createRowData(1L, 200L, null, null, time2, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());
        writerA.flush(false);

        writerB.write(
                createRowData(1L, null, "75.00", "40.00", time2, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());
        writerB.flush(false);

        writerA.close();
        writerB.close();

        // Verify aggregation results
        List<InternalRow> results = scanAllData();
        assertThat(results).hasSize(1);

        InternalRow row = results.get(0);
        assertThat(row.getLong(0)).isEqualTo(1L); // product_id

        // total_sales: 100 + 200 = 300 (from Job A only)
        assertThat(row.getLong(1)).isEqualTo(300L);

        // max_price: max(50.00, 75.00) = 75.00 (from Job B only)
        assertThat(row.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("75.00"));

        // min_price: min(50.00, 40.00) = 40.00 (from Job B only)
        assertThat(row.getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("40.00"));

        // last_update_time: time2 (last value, could be from either job)
        assertThat(row.getTimestampNtz(4, 3).toLocalDateTime()).isEqualTo(time2);
    }

    /**
     * Test Case 6: Partition table with concurrent jobs and column lock.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Create a partitioned table with column lock enabled
     *   <li>Job A writes to partition p1 and acquires table-level lock
     *   <li>Job B tries to write to same table (different partition) - should fail due to
     *       table-level lock
     *   <li>After Job A completes, Job C can write successfully
     *   <li>Note: Current implementation uses table-level lock for partitioned tables
     * </ul>
     */
    @Test
    public void testPartitionTable_ConcurrentJobs_ColumnLock() throws Exception {
        // Create partitioned table
        TablePath partitionedTablePath = TablePath.of(DEFAULT_DB, "test_partition_lock");
        Schema partitionedSchema =
                Schema.newBuilder()
                        .column("region", DataTypes.STRING())
                        .column("product_id", DataTypes.BIGINT())
                        .column("total_sales", DataTypes.BIGINT())
                        .column("max_price", DataTypes.DECIMAL(10, 2))
                        .primaryKey("region", "product_id")
                        .build();

        TableDescriptor partitionedTableDescriptor =
                TableDescriptor.builder()
                        .schema(partitionedSchema)
                        .distributedBy(2, "product_id")
                        .partitionedBy("region")
                        .property(
                                ConfigOptions.TABLE_MERGE_ENGINE.key(),
                                MergeEngineType.AGGREGATE.name())
                        .property(ConfigOptions.TABLE_COLUMN_LOCK_ENABLED.key(), "true")
                        .property("table.merge-engine.aggregate.total_sales", "sum")
                        .property("table.merge-engine.aggregate.max_price", "max")
                        .build();

        createTable(partitionedTablePath, partitionedTableDescriptor);

        // Create partitions
        String partition1 = "region=north";
        String partition2 = "region=south";
        admin.createPartition(partitionedTablePath, parsePartitionSpec(partition1), false).get();
        admin.createPartition(partitionedTablePath, parsePartitionSpec(partition2), false).get();

        Configuration flussConfig = clientConf;

        org.apache.flink.table.types.logical.RowType partitionedFlinkRowType =
                org.apache.flink.table.types.logical.RowType.of(
                        new LogicalType[] {
                            new org.apache.flink.table.types.logical.VarCharType(
                                    false, Integer.MAX_VALUE), // region (NOT NULL, primary key)
                            new BigIntType(false), // product_id (NOT NULL, primary key)
                            new BigIntType(), // total_sales
                            new DecimalType(10, 2) // max_price
                        },
                        new String[] {"region", "product_id", "total_sales", "max_price"});

        // Job A: writes to partition p1 (north)
        String lockOwnerA = "job-a-partition-" + System.currentTimeMillis();
        UpsertSinkWriter<RowData> writerA =
                createPartitionedUpsertSinkWriter(
                        partitionedTablePath,
                        flussConfig,
                        lockOwnerA,
                        null,
                        0,
                        1,
                        partitionedFlinkRowType);
        TestFunctionInitializationContext initCtxA = new TestFunctionInitializationContext();
        writerA.initializeState(initCtxA);
        writerA.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        // Job A writes to north
        GenericRowData rowA =
                GenericRowData.of(
                        org.apache.flink.table.data.StringData.fromString("north"),
                        1L,
                        100L,
                        DecimalData.fromBigDecimal(new BigDecimal("50.00"), 10, 2));
        rowA.setRowKind(RowKind.INSERT);
        writerA.write(rowA, new MockSinkWriterContext());
        writerA.flush(false);

        // Job B: tries to write to partition p2 (south) - should fail due to table-level lock
        String lockOwnerB = "job-b-partition-" + System.currentTimeMillis();
        UpsertSinkWriter<RowData> writerB =
                createPartitionedUpsertSinkWriter(
                        partitionedTablePath,
                        flussConfig,
                        lockOwnerB,
                        null,
                        0,
                        1,
                        partitionedFlinkRowType);
        TestFunctionInitializationContext initCtxB = new TestFunctionInitializationContext();
        writerB.initializeState(initCtxB);

        // Job B should fail to initialize due to table-level column lock conflict
        assertThatThrownBy(
                        () ->
                                writerB.initialize(
                                        new MockWriterInitContext(
                                                        new InterceptingOperatorMetricGroup())
                                                .metricGroup()))
                .hasMessageContaining("Column lock conflict")
                .cause()
                .isInstanceOf(ColumnLockConflictException.class);

        writerB.close();

        // Job A completes and releases lock
        writerA.close();

        // Job C: writes to partition north after Job A completes - should succeed
        String lockOwnerC = "job-c-partition-" + System.currentTimeMillis();
        UpsertSinkWriter<RowData> writerC =
                createPartitionedUpsertSinkWriter(
                        partitionedTablePath,
                        flussConfig,
                        lockOwnerC,
                        null,
                        0,
                        1,
                        partitionedFlinkRowType);
        TestFunctionInitializationContext initCtxC = new TestFunctionInitializationContext();
        writerC.initializeState(initCtxC);
        writerC.initialize(
                new MockWriterInitContext(new InterceptingOperatorMetricGroup()).metricGroup());

        // Job C writes to north
        GenericRowData rowC =
                GenericRowData.of(
                        org.apache.flink.table.data.StringData.fromString("north"),
                        2L,
                        200L,
                        DecimalData.fromBigDecimal(new BigDecimal("60.00"), 10, 2));
        rowC.setRowKind(RowKind.INSERT);
        writerC.write(rowC, new MockSinkWriterContext());
        writerC.flush(false);

        writerC.close();

        // Verify data in north partition
        Table table = conn.getTable(partitionedTablePath);
        List<InternalRow> resultsNorth = new ArrayList<>();

        // Scan partition north
        long partitionIdNorth = getPartitionId(partitionedTablePath, partition1);
        scanPartitionData(table, partitionIdNorth, resultsNorth);
        assertThat(resultsNorth).hasSize(2);

        Map<Long, InternalRow> resultMap = new HashMap<>();
        for (InternalRow row : resultsNorth) {
            resultMap.put(row.getLong(1), row);
        }

        assertThat(resultMap).containsKeys(1L, 2L);
        assertThat(resultMap.get(1L).getString(0).toString()).isEqualTo("north");
        assertThat(resultMap.get(1L).getLong(2)).isEqualTo(100L);
        assertThat(resultMap.get(2L).getString(0).toString()).isEqualTo("north");
        assertThat(resultMap.get(2L).getLong(2)).isEqualTo(200L);
    }

    // ==================== Helper Methods ====================

    private UpsertSinkWriter<RowData> createPartitionedUpsertSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            String lockOwnerId,
            int[] targetColumnIndexes,
            int subtaskIndex,
            int parallelism,
            org.apache.flink.table.types.logical.RowType rowType)
            throws Exception {
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(false, true);
        serializationSchema.open(new SerializerInitContextImpl(toFlussRowType(rowType)));

        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        return new UpsertSinkWriter<>(
                tablePath,
                flussConfig,
                rowType,
                targetColumnIndexes,
                initContext.getMailboxExecutor(),
                serializationSchema,
                subtaskIndex,
                parallelism,
                lockOwnerId);
    }

    private void scanPartitionData(Table table, long partitionId, List<InternalRow> results)
            throws Exception {
        int numBuckets = TABLE_DESCRIPTOR.getTableDistribution().get().getBucketCount().get();
        long tableId = table.getTableInfo().getTableId();
        int scanLimit = 10000;

        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
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
    }

    private UpsertSinkWriter<RowData> createUpsertSinkWriter(
            Configuration flussConfig,
            String lockOwnerId,
            int[] targetColumnIndexes,
            int subtaskIndex,
            int parallelism)
            throws Exception {
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(false, true);
        serializationSchema.open(new SerializerInitContextImpl(toFlussRowType(flinkRowType)));

        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        return new UpsertSinkWriter<>(
                tablePath,
                flussConfig,
                flinkRowType,
                targetColumnIndexes,
                initContext.getMailboxExecutor(),
                serializationSchema,
                subtaskIndex,
                parallelism,
                lockOwnerId);
    }

    private RowData createRowData(
            long productId,
            long totalSales,
            String maxPrice,
            String minPrice,
            LocalDateTime lastUpdateTime,
            RowKind rowKind) {
        return createRowData(
                productId, Long.valueOf(totalSales), maxPrice, minPrice, lastUpdateTime, rowKind);
    }

    private RowData createRowData(
            long productId,
            Long totalSales,
            String maxPrice,
            String minPrice,
            LocalDateTime lastUpdateTime,
            RowKind rowKind) {
        GenericRowData rowData =
                GenericRowData.of(
                        productId,
                        totalSales,
                        maxPrice != null
                                ? DecimalData.fromBigDecimal(new BigDecimal(maxPrice), 10, 2)
                                : null,
                        minPrice != null
                                ? DecimalData.fromBigDecimal(new BigDecimal(minPrice), 10, 2)
                                : null,
                        TimestampData.fromLocalDateTime(lastUpdateTime));
        rowData.setRowKind(rowKind);
        return rowData;
    }

    private List<InternalRow> scanAllData() throws Exception {
        Table table = conn.getTable(tablePath);
        List<InternalRow> results = new ArrayList<>();

        int numBuckets = TABLE_DESCRIPTOR.getTableDistribution().get().getBucketCount().get();
        long tableId = table.getTableInfo().getTableId();

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

    private Map<Long, InternalRow> buildDataMap(List<InternalRow> rows) {
        Map<Long, InternalRow> map = new HashMap<>();
        for (InternalRow row : rows) {
            map.put(row.getLong(0), row);
        }
        return map;
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

    // ==================== Test Helper Classes ====================

    static class MockSinkWriterContext extends FlinkSinkWriterTest.MockSinkWriterContext {}

    static class TestFunctionInitializationContext
            implements org.apache.flink.runtime.state.FunctionInitializationContext {
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
