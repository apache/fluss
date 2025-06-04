/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.flink.source;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.testutils.MockDataUtils;
import com.alibaba.fluss.flink.source.testutils.Order;
import com.alibaba.fluss.flink.source.testutils.OrderPartial;
import com.alibaba.fluss.flink.source.testutils.OrderPartialDeserializationSchema;
import com.alibaba.fluss.flink.utils.FlinkTestBase;
import com.alibaba.fluss.flink.utils.PojoToRowConverter;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.flink.source.testutils.MockDataUtils.binaryRowToGenericRow;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
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
        writeRows(ordersPKTablePath, updatedRows, false);

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
        writeRows(ordersLogTablePath, updatedRows, true);

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
