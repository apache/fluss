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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.testutils.MockDataUtils;
import org.apache.fluss.flink.source.testutils.Order;
import org.apache.fluss.flink.source.testutils.OrderPartial;
import org.apache.fluss.flink.source.testutils.OrderPartialDeserializationSchema;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.flink.utils.PojoToRowConverter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.MockDataUtils.binaryRowToGenericRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the {@link FlussSource} class with PK tables. */
public class FlussSourceITCase extends FlinkTestBase {
    private static final List<Order> ORDERS = MockDataUtils.ORDERS;

    private static final Schema PK_SCHEMA = MockDataUtils.getOrdersSchemaPK();
    private static final Schema LOG_SCHEMA = MockDataUtils.getOrdersSchemaLog();

    private final String pkTableName = "orders_test_pk";
    private final String logTableName = "orders_test_log";

    private final TablePath ordersLogTablePath = new TablePath(DEFAULT_DB, logTableName);
    private final TablePath ordersPKTablePath = new TablePath(DEFAULT_DB, pkTableName);

    private final TableDescriptor logTableDescriptor =
            TableDescriptor.builder().schema(LOG_SCHEMA).distributedBy(1, "orderId").build();
    private final TableDescriptor pkTableDescriptor =
            TableDescriptor.builder().schema(PK_SCHEMA).distributedBy(1, "orderId").build();

    protected StreamExecutionEnvironment env;

    @BeforeEach
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
    }

    @Test
    public void testTablePKSource() throws Exception {
        createTable(ordersPKTablePath, pkTableDescriptor);
        writeRowsToTable(ordersPKTablePath);
        // Create a DataStream from the FlussSource
        FlussSource<Order> flussSource =
                FlussSource.<Order>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new MockDataUtils.OrderDeserializationSchema())
                        .build();

        DataStreamSource<Order> stream =
                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");

        List<Order> collectedElements = stream.executeAndCollect(ORDERS.size());

        // Assert result size and elements match
        assertThat(collectedElements).hasSameElementsAs(ORDERS);
    }

    @Test
    public void testTablePKSourceWithProjectionPushdown() throws Exception {
        createTable(ordersPKTablePath, pkTableDescriptor);
        writeRowsToTable(ordersPKTablePath);
        List<OrderPartial> expectedOutput =
                Arrays.asList(
                        new OrderPartial(600, 600),
                        new OrderPartial(700, 601),
                        new OrderPartial(800, 602),
                        new OrderPartial(900, 603),
                        new OrderPartial(1000, 604));

        // Create a DataStream from the FlussSource
        FlussSource<OrderPartial> flussSource =
                FlussSource.<OrderPartial>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new OrderPartialDeserializationSchema())
                        .setProjectedFields("orderId", "amount")
                        .build();

        DataStreamSource<OrderPartial> stream =
                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");

        List<OrderPartial> collectedElements = stream.executeAndCollect(ORDERS.size());

        // Assert result size and elements match
        assertThat(collectedElements).hasSameElementsAs(expectedOutput);
    }

    @Test
    public void testRowDataPKTableSource() throws Exception {
        createTable(ordersPKTablePath, pkTableDescriptor);
        writeRowsToTable(ordersPKTablePath);
        Table table = conn.getTable(ordersPKTablePath);
        RowType rowType = table.getTableInfo().getRowType();

        // Create a DataStream from the FlussSource
        FlussSource<RowData> flussSource =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new RowDataDeserializationSchema())
                        .build();

        DataStreamSource<RowData> stream =
                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");

        List<InternalRow> updatedRows =
                Arrays.asList(row(600L, 20L, 800, "addr1"), row(700L, 22L, 801, "addr2"));

        // send some row updates
        writeRows(conn, ordersPKTablePath, updatedRows, false);

        List<RowData> expectedResult =
                Arrays.asList(
                        createRowData(600L, 20L, 600, "addr1", RowKind.INSERT),
                        createRowData(700L, 22L, 601, "addr2", RowKind.INSERT),
                        createRowData(800L, 23L, 602, "addr3", RowKind.INSERT),
                        createRowData(900L, 24L, 603, "addr4", RowKind.INSERT),
                        createRowData(1000L, 25L, 604, "addr5", RowKind.INSERT),
                        createRowData(600L, 20L, 600, "addr1", RowKind.UPDATE_BEFORE),
                        createRowData(600L, 20L, 800, "addr1", RowKind.UPDATE_AFTER),
                        createRowData(700L, 22L, 601, "addr2", RowKind.UPDATE_BEFORE),
                        createRowData(700L, 22L, 801, "addr2", RowKind.UPDATE_AFTER));

        List<RowData> rawRows = stream.executeAndCollect(expectedResult.size());
        List<RowData> collectedRows =
                rawRows.stream()
                        .map(row -> binaryRowToGenericRow(row, rowType))
                        .collect(Collectors.toList());

        // Assert result size and elements match
        assertThat(expectedResult).hasSameElementsAs(collectedRows);
    }

    @Test
    public void testRowDataLogTableSource() throws Exception {
        createTable(ordersLogTablePath, logTableDescriptor);
        writeRowsToTable(ordersLogTablePath);
        Table table = conn.getTable(ordersLogTablePath);
        RowType rowType = table.getTableInfo().getRowType();

        // Create a DataStream from the FlussSource
        FlussSource<RowData> flussSource =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(logTableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new RowDataDeserializationSchema())
                        .build();

        DataStreamSource<RowData> stream =
                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");

        // these rows should be interpreted as Inserts
        List<InternalRow> updatedRows =
                Arrays.asList(row(600L, 20L, 600, "addr1"), row(700L, 22L, 601, "addr2"));

        // send some row updates
        writeRows(conn, ordersLogTablePath, updatedRows, true);

        List<RowData> expectedResult =
                Arrays.asList(
                        createRowData(600L, 20L, 600, "addr1", RowKind.INSERT),
                        createRowData(700L, 22L, 601, "addr2", RowKind.INSERT),
                        createRowData(800L, 23L, 602, "addr3", RowKind.INSERT),
                        createRowData(900L, 24L, 603, "addr4", RowKind.INSERT),
                        createRowData(1000L, 25L, 604, "addr5", RowKind.INSERT),
                        createRowData(600L, 20L, 600, "addr1", RowKind.INSERT),
                        createRowData(700L, 22L, 601, "addr2", RowKind.INSERT));

        List<RowData> rawRows = stream.executeAndCollect(expectedResult.size());
        List<RowData> collectedRows =
                rawRows.stream()
                        .map(row -> binaryRowToGenericRow(row, rowType))
                        .collect(Collectors.toList());

        // Assert result size and elements match
        assertThat(expectedResult).hasSameElementsAs(collectedRows);
    }

    @Test
    public void testTableLogSourceWithProjectionPushdown() throws Exception {
        createTable(ordersLogTablePath, logTableDescriptor);
        writeRowsToTable(ordersLogTablePath);
        List<OrderPartial> expectedOutput =
                Arrays.asList(
                        new OrderPartial(600, 600),
                        new OrderPartial(700, 601),
                        new OrderPartial(800, 602),
                        new OrderPartial(900, 603),
                        new OrderPartial(1000, 604));

        // Create a DataStream from the FlussSource
        FlussSource<OrderPartial> flussSource =
                FlussSource.<OrderPartial>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(logTableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new OrderPartialDeserializationSchema())
                        .setProjectedFields("orderId", "amount")
                        .build();

        DataStreamSource<OrderPartial> stream =
                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");

        List<OrderPartial> collectedElements = stream.executeAndCollect(ORDERS.size());

        // Assert result size and elements match
        assertThat(collectedElements).hasSameElementsAs(expectedOutput);
    }

    /** Verifies that event-time timestamps are correctly assigned via WatermarkStrategy. */
    @Test
    void testTimestamp() throws Exception {
        // 1. Create Fluss log table
        String tableName = "wm_timestamp_test";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("event_time", DataTypes.BIGINT())
                        .build();
        createTable(tablePath, TableDescriptor.builder().schema(schema).distributedBy(1).build());

        // 2. Write 3 records with known event_time values
        final long currentTimestamp = System.currentTimeMillis();
        List<InternalRow> rows =
                Arrays.asList(
                        row(1, "name1", currentTimestamp + 1L),
                        row(2, "name2", currentTimestamp + 2L),
                        row(3, "name3", currentTimestamp + 3L));
        writeRows(conn, tablePath, rows, true);

        // 3. Build FlussSource and apply WatermarkStrategy with TimestampAssigner
        FlussSource<RowData> source =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializationSchema(new RowDataDeserializationSchema())
                        .build();

        env.setParallelism(1);
        DataStreamSource<RowData> stream =
                env.fromSource(
                        source,
                        WatermarkStrategy.<RowData>noWatermarks()
                                .withTimestampAssigner(
                                        (rowData, ts) -> rowData.getLong(2)), // event_time column
                        "testTimestamp");

        // Verify that the timestamp and watermark are working fine.
        List<Long> result =
                stream.transform(
                                "timestampVerifier",
                                TypeInformation.of(Long.class),
                                new WatermarkVerifyingOperator(v -> v.getLong(2)))
                        .executeAndCollect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        currentTimestamp + 1L, currentTimestamp + 2L, currentTimestamp + 3L);
    }

    /** Verifies per-bucket (per-split) watermark multiplexing correctness. */
    @Test
    void testPerBucketWatermark() throws Exception {
        // 1. Create 2-bucket Fluss log table
        String tableName = "wm_per_bucket_test";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("ts", DataTypes.BIGINT())
                        .build();
        createTable(tablePath, TableDescriptor.builder().schema(schema).distributedBy(2).build());

        // 2. Write 6 records with interleaved timestamps
        List<InternalRow> rows =
                Arrays.asList(
                        row(1, "a", 100L),
                        row(2, "b", 150L),
                        row(3, "c", 200L),
                        row(4, "d", 250L),
                        row(5, "e", 300L),
                        row(6, "f", 350L));
        writeRows(conn, tablePath, rows, true);

        // 3. Build FlussSource and apply per-split WatermarkStrategy
        FlussSource<RowData> source =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializationSchema(new RowDataDeserializationSchema())
                        .build();

        env.setParallelism(1);

        // 4. Assert per-split watermark ordering via ProcessFunction
        env.fromSource(
                        source,
                        WatermarkStrategy.forGenerator(ctx -> new OnEventWatermarkGenerator())
                                .withTimestampAssigner(
                                        (rowData, ts) -> rowData.getLong(2)), // ts column
                        "testPerPartitionWatermark")
                .process(
                        new ProcessFunction<RowData, Object>() {
                            @Override
                            public void processElement(
                                    RowData value,
                                    ProcessFunction<RowData, Object>.Context ctx,
                                    Collector<Object> out) {
                                assertThat(ctx.timestamp())
                                        .as(
                                                "Event time should never behind watermark "
                                                        + "because of per-split watermark multiplexing logic")
                                        .isGreaterThanOrEqualTo(
                                                ctx.timerService().currentWatermark());
                                out.collect(ctx.timestamp());
                            }
                        })
                .executeAndCollect(6);
    }

    /** A StreamMap that verifies the watermark logic. */
    private static class WatermarkVerifyingOperator extends StreamMap<RowData, Long> {

        private static final long serialVersionUID = 1L;

        public WatermarkVerifyingOperator(MapFunction<RowData, Long> mapper) {
            super(mapper);
        }

        @Override
        public void processElement(StreamRecord<RowData> element) {
            output.collect(new StreamRecord<>(element.getTimestamp()));
        }
    }

    /** A WatermarkGenerator that emits a watermark equal to the event timestamp on each event. */
    private static class OnEventWatermarkGenerator implements WatermarkGenerator<RowData> {
        @Override
        public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new Watermark(eventTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    private static RowData createRowData(
            Long orderId, Long itemId, Integer amount, String address, RowKind rowKind) {
        GenericRowData row = new GenericRowData(4);
        row.setField(0, orderId);
        row.setField(1, itemId);
        row.setField(2, amount);
        row.setField(3, StringData.fromString(address));

        row.setRowKind(rowKind);
        return row;
    }

    private void writeRowsToTable(TablePath tablePath) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            RowType rowType = table.getTableInfo().getRowType();
            boolean isLogTable = !table.getTableInfo().hasPrimaryKey();

            PojoToRowConverter<Order> converter = new PojoToRowConverter<>(Order.class, rowType);

            List<GenericRow> rows =
                    ORDERS.stream().map(converter::convert).collect(Collectors.toList());

            if (isLogTable) {
                AppendWriter writer = table.newAppend().createWriter();
                rows.forEach(writer::append);
                writer.flush();
            } else {
                UpsertWriter writer = table.newUpsert().createWriter();
                rows.forEach(writer::upsert);
                writer.flush();
            }
        }
    }
}
