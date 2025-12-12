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
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.sink.serializer.SerializerInitContextImpl;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.MergeEngineType;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlussRowType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for Aggregation Merge Engine with failover scenarios.
 *
 * <p>This test verifies:
 *
 * <ul>
 *   <li>Aggregation functions work correctly (sum, max, min, last_value, etc.)
 *   <li>Exactly-Once semantics after failover with Undo Recovery mechanism
 *   <li>Multiple updates to the same primary key aggregate correctly
 *   <li>Data replay after failover produces correct aggregation results
 * </ul>
 */
public class AggregationMergeEngineFailoverITCase extends FlinkTestBase {

    // Schema for testing: product_id, total_sales, max_price, min_price, last_update_time
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
                    // Configure aggregation functions for each field
                    .property("table.merge-engine.aggregate.total_sales", "sum")
                    .property("table.merge-engine.aggregate.max_price", "max")
                    .property("table.merge-engine.aggregate.min_price", "min")
                    .property("table.merge-engine.aggregate.last_update_time", "last_value")
                    .build();

    private static final String TEST_TABLE_NAME = "test_agg_merge_engine";
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
     * Test Case 1: Verify basic aggregation functions work correctly.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Write initial data: product_id=1, sales=100, price=50.00
     *   <li>Write update: product_id=1, sales=200, price=75.00
     *   <li>Write another update: product_id=1, sales=150, price=60.00
     *   <li>Expected result: total_sales=450 (sum), max_price=75.00, min_price=50.00,
     *       last_update_time=latest
     * </ul>
     */
    @Test
    public void testBasicAggregationFunctions() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        UpsertSinkWriter<RowData> writer =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx = new TestFunctionInitializationContext();
        writer.initializeState(initCtx);
        writer.initialize(initContext.metricGroup());

        // Write initial data
        LocalDateTime time1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0);
        writer.write(
                createRowData(1L, 100L, "50.00", "50.00", time1, RowKind.INSERT),
                new MockSinkWriterContext());

        // Write first update - should aggregate
        LocalDateTime time2 = LocalDateTime.of(2024, 1, 1, 11, 0, 0);
        writer.write(
                createRowData(1L, 200L, "75.00", "75.00", time2, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        // Write second update
        LocalDateTime time3 = LocalDateTime.of(2024, 1, 1, 12, 0, 0);
        writer.write(
                createRowData(1L, 150L, "60.00", "60.00", time3, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writer.flush(false);

        // Verify aggregation results
        List<InternalRow> results = scanAllData();
        assertThat(results).hasSize(1);

        InternalRow row = results.get(0);
        assertThat(row.getLong(0)).isEqualTo(1L); // product_id

        // total_sales: 100 + 200 + 150 = 450
        assertThat(row.getLong(1)).isEqualTo(450L);

        // max_price: max(50.00, 75.00, 60.00) = 75.00
        assertThat(row.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("75.00"));

        // min_price: min(50.00, 75.00, 60.00) = 50.00
        assertThat(row.getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("50.00"));

        // last_update_time should be time3
        assertThat(row.getTimestampNtz(4, 3).toLocalDateTime()).isEqualTo(time3);

        writer.close();
    }

    /**
     * Test Case 2: Verify Exactly-Once semantics with Undo Recovery after failover.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Write data: product_id=1, sales=100
     *   <li>Checkpoint
     *   <li>Write more data: product_id=1, sales=200 (should be undone after recovery)
     *   <li>Simulate failover
     *   <li>Verify: Undo recovery rolls back to checkpoint state (sales=100)
     *   <li>Replay data: product_id=1, sales=200
     *   <li>Final result: total_sales=300
     * </ul>
     */
    @Test
    public void testAggregationWithUndoRecoveryAfterFailover() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Phase 1: Initial writes before checkpoint
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        // Write initial data
        LocalDateTime time1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0);
        writer1.write(
                createRowData(1L, 100L, "50.00", "50.00", time1, RowKind.INSERT),
                new MockSinkWriterContext());

        writer1.write(
                createRowData(2L, 200L, "60.00", "60.00", time1, RowKind.INSERT),
                new MockSinkWriterContext());

        writer1.flush(false);

        // Checkpoint 1
        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);

        Map<TableBucket, Long> checkpoint1State = getStateFromWriter(initCtx1);
        assertThat(checkpoint1State).isNotEmpty();

        // Phase 2: Write more data after checkpoint (should be undone)
        LocalDateTime time2 = LocalDateTime.of(2024, 1, 1, 11, 0, 0);
        writer1.write(
                createRowData(1L, 200L, "75.00", "45.00", time2, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writer1.write(
                createRowData(2L, 300L, "80.00", "55.00", time2, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writer1.flush(false);
        writer1.close();

        // Verify data before recovery (includes post-checkpoint writes)
        List<InternalRow> dataBeforeRecovery = scanAllData();
        assertThat(dataBeforeRecovery).hasSize(2);

        Map<Long, InternalRow> beforeMap = buildDataMap(dataBeforeRecovery);

        // product_id=1: total_sales=100+200=300, max=75.00, min=45.00
        assertThat(beforeMap.get(1L).getLong(1)).isEqualTo(300L);
        assertThat(beforeMap.get(1L).getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("75.00"));
        assertThat(beforeMap.get(1L).getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("45.00"));

        // Phase 3: Simulate failover - create new writer with checkpoint state
        MockWriterInitContext initContext2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext2.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpoint1State, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext2.metricGroup());

        // Verify undo recovery rolled back to checkpoint state
        List<InternalRow> dataAfterRecovery = scanAllData();
        assertThat(dataAfterRecovery).hasSize(2);

        Map<Long, InternalRow> afterMap = buildDataMap(dataAfterRecovery);

        // product_id=1: should be rolled back to (100, 50.00, 50.00)
        assertThat(afterMap.get(1L).getLong(1)).isEqualTo(100L);
        assertThat(afterMap.get(1L).getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("50.00"));
        assertThat(afterMap.get(1L).getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("50.00"));

        // product_id=2: should be rolled back to (200, 60.00, 60.00)
        assertThat(afterMap.get(2L).getLong(1)).isEqualTo(200L);

        // Phase 4: Replay data after recovery
        LocalDateTime time3 = LocalDateTime.of(2024, 1, 1, 12, 0, 0);
        writer2.write(
                createRowData(1L, 200L, "75.00", "45.00", time3, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writer2.write(
                createRowData(2L, 300L, "80.00", "55.00", time3, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writer2.flush(false);

        // Verify final aggregation results after replay
        List<InternalRow> finalData = scanAllData();
        assertThat(finalData).hasSize(2);

        Map<Long, InternalRow> finalMap = buildDataMap(finalData);

        // product_id=1: total_sales=100+200=300, max=75.00, min=45.00
        assertThat(finalMap.get(1L).getLong(1)).isEqualTo(300L);
        assertThat(finalMap.get(1L).getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("75.00"));
        assertThat(finalMap.get(1L).getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("45.00"));

        // product_id=2: total_sales=200+300=500, max=80.00, min=55.00
        assertThat(finalMap.get(2L).getLong(1)).isEqualTo(500L);
        assertThat(finalMap.get(2L).getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("80.00"));
        assertThat(finalMap.get(2L).getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("55.00"));

        writer2.close();
    }

    /**
     * Test Case 3: Verify undo recovery with multiple updates to the same primary key.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Write data: product_id=1, sales=100, price=50.00
     *   <li>Checkpoint
     *   <li>Write multiple updates after checkpoint: sales=50, 30, 20, prices vary
     *   <li>Simulate failover
     *   <li>Verify undo recovery correctly rolls back all post-checkpoint updates
     *   <li>Replay all updates
     *   <li>Final result should match expected aggregation
     * </ul>
     */
    @Test
    public void testUndoRecoveryWithMultipleUpdatesToSamePK() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        // Phase 1: Initial write and checkpoint
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        LocalDateTime time1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0);
        writer1.write(
                createRowData(1L, 100L, "50.00", "50.00", time1, RowKind.INSERT),
                new MockSinkWriterContext());

        writer1.flush(false);

        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);
        Map<TableBucket, Long> checkpoint1State = getStateFromWriter(initCtx1);

        // Phase 2: Multiple updates after checkpoint
        LocalDateTime time2 = LocalDateTime.of(2024, 1, 1, 11, 0, 0);
        writer1.write(
                createRowData(1L, 50L, "60.00", "40.00", time2, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        LocalDateTime time3 = LocalDateTime.of(2024, 1, 1, 12, 0, 0);
        writer1.write(
                createRowData(1L, 30L, "70.00", "35.00", time3, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        LocalDateTime time4 = LocalDateTime.of(2024, 1, 1, 13, 0, 0);
        writer1.write(
                createRowData(1L, 20L, "55.00", "55.00", time4, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writer1.flush(false);
        writer1.close();

        // Verify data before recovery
        List<InternalRow> dataBeforeRecovery = scanAllData();
        assertThat(dataBeforeRecovery).hasSize(1);

        InternalRow beforeRow = dataBeforeRecovery.get(0);
        // total_sales: 100+50+30+20=200
        assertThat(beforeRow.getLong(1)).isEqualTo(200L);
        // max_price: max(50, 60, 70, 55) = 70.00
        assertThat(beforeRow.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("70.00"));
        // min_price: min(50, 40, 35, 55) = 35.00
        assertThat(beforeRow.getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("35.00"));

        // Phase 3: Failover recovery
        MockWriterInitContext initContext2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext2.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpoint1State, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext2.metricGroup());

        // Verify undo recovery rolled back
        List<InternalRow> dataAfterRecovery = scanAllData();
        assertThat(dataAfterRecovery).hasSize(1);

        InternalRow afterRow = dataAfterRecovery.get(0);
        // Should be rolled back to checkpoint state: (100, 50.00, 50.00)
        assertThat(afterRow.getLong(1)).isEqualTo(100L);
        assertThat(afterRow.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("50.00"));
        assertThat(afterRow.getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("50.00"));

        // Phase 4: Replay all updates
        LocalDateTime time5 = LocalDateTime.of(2024, 1, 1, 14, 0, 0);
        writer2.write(
                createRowData(1L, 50L, "60.00", "40.00", time5, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());
        writer2.write(
                createRowData(1L, 30L, "70.00", "35.00", time5, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());
        writer2.write(
                createRowData(1L, 20L, "55.00", "55.00", time5, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writer2.flush(false);

        // Verify final result matches expected aggregation
        List<InternalRow> finalData = scanAllData();
        assertThat(finalData).hasSize(1);

        InternalRow finalRow = finalData.get(0);
        // total_sales: 100+50+30+20=200
        assertThat(finalRow.getLong(1)).isEqualTo(200L);
        // max_price: max(50, 60, 70, 55) = 70.00
        assertThat(finalRow.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("70.00"));
        // min_price: min(50, 40, 35, 55) = 35.00
        assertThat(finalRow.getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("35.00"));

        writer2.close();
    }

    /**
     * Test Case 4: Verify no undo recovery when checkpoint state matches current state.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Write data and checkpoint
     *   <li>Close writer
     *   <li>Create new writer with checkpoint state (no new writes between checkpoint and recovery)
     *   <li>Verify data remains unchanged (no undo needed)
     * </ul>
     */
    @Test
    public void testNoUndoWhenNoNewWritesAfterCheckpoint() throws Exception {
        Configuration flussConfig = clientConf;
        MockWriterInitContext initContext =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());

        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(flussConfig, initContext.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(initContext.metricGroup());

        // Write data
        LocalDateTime time1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0);
        writer1.write(
                createRowData(1L, 100L, "50.00", "50.00", time1, RowKind.INSERT),
                new MockSinkWriterContext());
        writer1.write(
                createRowData(1L, 200L, "75.00", "40.00", time1, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());

        writer1.flush(false);

        // Checkpoint
        TestFunctionSnapshotContext snapshotCtx1 = new TestFunctionSnapshotContext(1L);
        writer1.snapshotState(snapshotCtx1);
        Map<TableBucket, Long> checkpoint1State = getStateFromWriter(initCtx1);

        writer1.close();

        // Get data before recovery
        List<InternalRow> dataBeforeRecovery = scanAllData();
        assertThat(dataBeforeRecovery).hasSize(1);

        InternalRow beforeRow = dataBeforeRecovery.get(0);
        long beforeSales = beforeRow.getLong(1);
        BigDecimal beforeMaxPrice = beforeRow.getDecimal(2, 10, 2).toBigDecimal();
        BigDecimal beforeMinPrice = beforeRow.getDecimal(3, 10, 2).toBigDecimal();

        // Create new writer with checkpoint state (no new writes)
        MockWriterInitContext initContext2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writer2 =
                createUpsertSinkWriter(flussConfig, initContext2.getMailboxExecutor(), 0, 1);

        TestFunctionInitializationContext initCtx2 =
                new TestFunctionInitializationContext(checkpoint1State, 1L);
        writer2.initializeState(initCtx2);
        writer2.initialize(initContext2.metricGroup());

        // Verify data remains unchanged
        List<InternalRow> dataAfterRecovery = scanAllData();
        assertThat(dataAfterRecovery).hasSize(1);

        InternalRow afterRow = dataAfterRecovery.get(0);
        assertThat(afterRow.getLong(1)).isEqualTo(beforeSales);
        assertThat(afterRow.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(beforeMaxPrice);
        assertThat(afterRow.getDecimal(3, 10, 2).toBigDecimal())
                .isEqualByComparingTo(beforeMinPrice);

        writer2.close();
    }

    /**
     * Test Case 5: Column lock protection between concurrent writers.
     *
     * <p>This test verifies that column locks prevent concurrent writes from different jobs.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Job A acquires lock and writes data
     *   <li>Job B (different owner) tries to acquire lock while Job A holds it - should fail
     *   <li>After Job A releases lock, Job B can successfully acquire it
     * </ul>
     */
    @Test
    public void testUndoRecovery_WithColumnLockProtection() throws Exception {
        Configuration flussConfig = clientConf;
        String lockOwnerId = "test-agg-lock-owner-" + System.currentTimeMillis();

        // Step 1: Job A creates writer and acquires lock
        MockWriterInitContext mockInitContext1 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writer1 =
                createUpsertSinkWriter(
                        flussConfig, mockInitContext1.getMailboxExecutor(), 0, 1, lockOwnerId);
        TestFunctionInitializationContext initCtx1 = new TestFunctionInitializationContext();
        writer1.initializeState(initCtx1);
        writer1.initialize(mockInitContext1.metricGroup());

        // Job A writes some data
        LocalDateTime time1 = LocalDateTime.of(2024, 1, 1, 10, 0, 0);
        writer1.write(
                createRowData(1L, 100L, "50.00", "50.00", time1, RowKind.INSERT),
                new MockSinkWriterContext());
        writer1.flush(false);

        // Step 2: While Job A holds lock, Job B tries to acquire lock - should fail
        String lockOwnerB = "test-agg-lock-owner-b-" + System.currentTimeMillis();
        MockWriterInitContext mockInitContextB =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writerB =
                createUpsertSinkWriter(
                        flussConfig, mockInitContextB.getMailboxExecutor(), 0, 1, lockOwnerB);
        TestFunctionInitializationContext initCtxB = new TestFunctionInitializationContext();
        writerB.initializeState(initCtxB);

        // This should throw ColumnLockConflictException because writer1 holds the lock
        try {
            writerB.initialize(mockInitContextB.metricGroup());
            assertThat(false)
                    .withFailMessage(
                            "Expected ColumnLockConflictException but initialization succeeded")
                    .isTrue();
        } catch (Exception e) {
            assertThat(e.getCause())
                    .isInstanceOf(org.apache.fluss.exception.ColumnLockConflictException.class);
        }

        // Step 3: Close writer1 to release lock
        writer1.close();

        // Step 4: Now Job B should be able to acquire lock
        MockWriterInitContext mockInitContextB2 =
                new MockWriterInitContext(new InterceptingOperatorMetricGroup());
        UpsertSinkWriter<RowData> writerB2 =
                createUpsertSinkWriter(
                        flussConfig, mockInitContextB2.getMailboxExecutor(), 0, 1, lockOwnerB);
        TestFunctionInitializationContext initCtxB2 = new TestFunctionInitializationContext();
        writerB2.initializeState(initCtxB2);
        writerB2.initialize(mockInitContextB2.metricGroup());

        // Job B writes successfully
        LocalDateTime time3 = LocalDateTime.of(2024, 1, 1, 12, 0, 0);
        writerB2.write(
                createRowData(1L, 50L, "80.00", "45.00", time3, RowKind.UPDATE_AFTER),
                new MockSinkWriterContext());
        writerB2.flush(false);

        writerB2.close();
        writerB.close();

        // Verify final result
        List<InternalRow> finalResults = scanAllData();
        assertThat(finalResults).hasSize(1);
        InternalRow finalRow = finalResults.get(0);
        assertThat(finalRow.getLong(0)).isEqualTo(1L);
        assertThat(finalRow.getLong(1)).isEqualTo(150L); // 100 (checkpoint) + 50 (Job B)
        assertThat(finalRow.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("80.00")); // max(50, 80) = 80
    }

    // ==================== Helper Methods ====================

    private UpsertSinkWriter<RowData> createUpsertSinkWriter(
            Configuration flussConfig,
            MailboxExecutor mailboxExecutor,
            int subtaskIndex,
            int parallelism)
            throws Exception {
        return createUpsertSinkWriter(
                flussConfig,
                mailboxExecutor,
                subtaskIndex,
                parallelism,
                "test-agg-lock-owner-" + System.currentTimeMillis());
    }

    private UpsertSinkWriter<RowData> createUpsertSinkWriter(
            Configuration flussConfig,
            MailboxExecutor mailboxExecutor,
            int subtaskIndex,
            int parallelism,
            String lockOwnerId)
            throws Exception {
        // Create serialization schema for aggregation table
        // For aggregation tables, UPDATE_BEFORE should be ignored (automatic optimization)
        // because the aggregation merge engine does not support retraction semantics
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(
                        false, // isAppendOnly = false (this is a primary key table)
                        false, // ignoreDelete = false (allow DELETE operations)
                        true); // ignoreUpdateBefore = true (ignore UPDATE_BEFORE for aggregation)
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
                lockOwnerId);
    }

    private RowData createRowData(
            long productId,
            long totalSales,
            String maxPrice,
            String minPrice,
            LocalDateTime lastUpdateTime,
            RowKind rowKind) {
        GenericRowData rowData =
                GenericRowData.of(
                        productId,
                        totalSales,
                        DecimalData.fromBigDecimal(new BigDecimal(maxPrice), 10, 2),
                        DecimalData.fromBigDecimal(new BigDecimal(minPrice), 10, 2),
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

    private Map<TableBucket, Long> getStateFromWriter(TestFunctionInitializationContext context) {
        List<WriterState> states = context.getWriterStates();
        Map<TableBucket, Long> result = new HashMap<>();
        for (WriterState state : states) {
            result.putAll(state.getBucketOffsets());
        }
        return result;
    }

    private Map<Long, InternalRow> buildDataMap(List<InternalRow> rows) {
        Map<Long, InternalRow> map = new HashMap<>();
        for (InternalRow row : rows) {
            map.put(row.getLong(0), row);
        }
        return map;
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
        public void update(List<S> values) throws Exception {
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
