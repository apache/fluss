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

package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.flink.row.OperationType;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.flink.sink.serializer.RowDataSerializationSchema;
import com.alibaba.fluss.flink.utils.FlinkTestBase;
import com.alibaba.fluss.flink.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.alibaba.fluss.flink.utils.FlinkConversions.toFlinkRowKind;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the Fluss sink connector in Flink. */
public class FlussSinkITCase extends FlinkTestBase {
    private static final Schema pkSchema =
            Schema.newBuilder()
                    .column("orderId", DataTypes.BIGINT())
                    .column("itemId", DataTypes.BIGINT())
                    .column("amount", DataTypes.INT())
                    .column("address", DataTypes.STRING())
                    .primaryKey("orderId")
                    .build();

    private static final Schema logSchema =
            Schema.newBuilder()
                    .column("orderId", DataTypes.BIGINT())
                    .column("itemId", DataTypes.BIGINT())
                    .column("amount", DataTypes.INT())
                    .column("address", DataTypes.STRING())
                    .build();

    private static final TableDescriptor pkTableDescriptor =
            TableDescriptor.builder().schema(pkSchema).distributedBy(1, "orderId").build();

    private static final TableDescriptor logTableDescriptor =
            TableDescriptor.builder().schema(logSchema).build();

    private static final String pkTableName = "orders_test_pk";
    private static final String logTableName = "orders_test_log";

    private StreamExecutionEnvironment env;
    private String bootstrapServers;

    @BeforeEach
    public void setup() throws Exception {
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
    }

    @AfterAll
    protected static void afterAll() throws Exception {
        conn.close();
    }

    @Test
    public void testRowDataTablePKSink() throws Exception {
        createTable(TablePath.of(DEFAULT_DB, pkTableName), pkTableDescriptor);
        FlussRowToFlinkRowConverter converter =
                new FlussRowToFlinkRowConverter(pkSchema.getRowType());

        RowData row1 = converter.toFlinkRowData(row(600L, 20L, 600, "addr1"));
        row1.setRowKind(RowKind.INSERT);
        RowData row2 = converter.toFlinkRowData(row(700L, 22L, 601, "addr2"));
        row1.setRowKind(RowKind.INSERT);
        RowData row3 = converter.toFlinkRowData(row(800L, 23L, 602, "addr3"));
        row1.setRowKind(RowKind.INSERT);
        RowData row4 = converter.toFlinkRowData(row(900L, 24L, 603, "addr4"));
        row1.setRowKind(RowKind.INSERT);
        RowData row5 = converter.toFlinkRowData(row(1000L, 25L, 604, "addr5"));
        row1.setRowKind(RowKind.INSERT);

        // Updates
        RowData row6 = converter.toFlinkRowData(row(800L, 230L, 602, "addr30"));
        row6.setRowKind(RowKind.UPDATE_AFTER);

        RowData row7 = converter.toFlinkRowData(row(900L, 240L, 603, "addr40"));
        row7.setRowKind(RowKind.UPDATE_AFTER);

        List<RowData> inputRows = new ArrayList<>();
        inputRows.add(row1);
        inputRows.add(row2);
        inputRows.add(row3);
        inputRows.add(row4);
        inputRows.add(row5);
        inputRows.add(row6);
        inputRows.add(row7);

        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(false, true);

        DataStream<RowData> stream = env.fromData(inputRows);

        FlinkSink<RowData> flussSink =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setSerializationSchema(serializationSchema)
                        .build();

        stream.sinkTo(flussSink).name("Fluss Sink");

        env.executeAsync("Test RowData Fluss Sink");

        Table table = conn.getTable(new TablePath(DEFAULT_DB, pkTableName));
        LogScanner logScanner = table.newScan().createLogScanner();

        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }

        List<RowData> rows = new ArrayList<>();

        while (rows.size() < inputRows.size()) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    RowData row = converter.toFlinkRowData(record.getRow());
                    row.setRowKind(toFlinkRowKind(record.getChangeType()));
                    rows.add(row);
                }
            }
        }

        // Add UPDATE_BEFORE rows to validate output.
        RowData row8 = converter.toFlinkRowData(row(800L, 23L, 602, "addr3"));
        row8.setRowKind(RowKind.UPDATE_BEFORE);

        RowData row9 = converter.toFlinkRowData(row(900L, 24L, 603, "addr4"));
        row9.setRowKind(RowKind.UPDATE_BEFORE);

        inputRows.add(row8);
        inputRows.add(row9);

        // Assert result size and elements match
        assertThat(rows.size()).isEqualTo(inputRows.size());
        assertThat(rows).containsAll(inputRows);

        logScanner.close();
    }

    @Test
    public void testRowDataTableLogSink() throws Exception {
        createTable(TablePath.of(DEFAULT_DB, logTableName), logTableDescriptor);
        FlussRowToFlinkRowConverter converter =
                new FlussRowToFlinkRowConverter(logSchema.getRowType());

        RowData row1 = converter.toFlinkRowData(row(600L, 20L, 600, "addr1"));
        RowData row2 = converter.toFlinkRowData(row(700L, 22L, 601, "addr2"));
        RowData row3 = converter.toFlinkRowData(row(800L, 23L, 602, "addr3"));
        RowData row4 = converter.toFlinkRowData(row(900L, 24L, 603, "addr4"));
        RowData row5 = converter.toFlinkRowData(row(1000L, 25L, 604, "addr5"));

        List<RowData> inputRows = new ArrayList<>();
        inputRows.add(row1);
        inputRows.add(row2);
        inputRows.add(row3);
        inputRows.add(row4);
        inputRows.add(row5);

        RowDataSerializationSchema serializationSchema = new RowDataSerializationSchema(true, true);

        DataStream<RowData> stream = env.fromData(inputRows);

        FlinkSink<RowData> flussSink =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(logTableName)
                        .setSerializationSchema(serializationSchema)
                        .build();

        stream.sinkTo(flussSink).name("Fluss Sink");

        env.executeAsync("Test RowData Fluss Sink");

        Table table = conn.getTable(new TablePath(DEFAULT_DB, logTableName));
        LogScanner logScanner = table.newScan().createLogScanner();

        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }

        List<RowData> rows = new ArrayList<>();

        while (rows.size() < inputRows.size()) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    RowData row = converter.toFlinkRowData(record.getRow());
                    row.setRowKind(toFlinkRowKind(record.getChangeType()));
                    rows.add(row);
                }
            }
        }

        // Assert result size and elements match
        assertThat(rows.size()).isEqualTo(inputRows.size());
        assertThat(rows).containsAll(inputRows);

        logScanner.close();
    }

    @Test
    public void testOrdersTablePKSink() throws Exception {
        createTable(TablePath.of(DEFAULT_DB, pkTableName), pkTableDescriptor);
        ArrayList<TestOrder> orders = new ArrayList<>();
        orders.add(new TestOrder(600, 20, 600, "addr1", RowKind.INSERT));
        orders.add(new TestOrder(700, 22, 601, "addr2", RowKind.INSERT));
        orders.add(new TestOrder(800, 23, 602, "addr3", RowKind.INSERT));
        orders.add(new TestOrder(900, 24, 603, "addr4", RowKind.INSERT));
        orders.add(new TestOrder(1000, 25, 604, "addr5", RowKind.INSERT));
        orders.add(new TestOrder(800, 230, 602, "addr3", RowKind.UPDATE_AFTER));
        orders.add(new TestOrder(900, 240, 603, "addr4", RowKind.UPDATE_AFTER));

        // Create a DataStream from the FlussSource
        DataStream<TestOrder> stream = env.fromData(orders);

        FlinkSink<TestOrder> flussSink =
                FlussSink.<TestOrder>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setSerializationSchema(new TestOrderSerializationSchema())
                        .build();

        stream.sinkTo(flussSink).name("Fluss Sink");
        env.executeAsync("Test Order Fluss Sink");

        Table table = conn.getTable(new TablePath(DEFAULT_DB, pkTableName));
        LogScanner logScanner = table.newScan().createLogScanner();

        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }

        orders.add(new TestOrder(800, 23, 602, "addr3", RowKind.UPDATE_BEFORE));
        orders.add(new TestOrder(900, 24, 603, "addr4", RowKind.UPDATE_BEFORE));

        List<TestOrder> rows = new ArrayList<>();
        while (rows.size() < orders.size()) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    InternalRow row = record.getRow();
                    TestOrder order =
                            new TestOrder(
                                    row.getLong(0),
                                    row.getLong(1),
                                    row.getInt(2),
                                    row.getString(3).toString(),
                                    toFlinkRowKind(record.getChangeType()));
                    rows.add(order);
                }
            }
        }

        assertThat(rows.size()).isEqualTo(orders.size());
        assertThat(rows.containsAll(orders)).isTrue();

        logScanner.close();
    }

    private static class TestOrder implements Serializable {
        private static final long serialVersionUID = 1L;
        private final long orderId;
        private final long itemId;
        private final int amount;
        private final String address;
        private final RowKind rowKind;

        public TestOrder(long orderId, long itemId, int amount, String address, RowKind rowKind) {
            this.orderId = orderId;
            this.itemId = itemId;
            this.amount = amount;
            this.address = address;
            this.rowKind = rowKind;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestOrder testOrder = (TestOrder) o;
            return orderId == testOrder.orderId
                    && itemId == testOrder.itemId
                    && amount == testOrder.amount
                    && Objects.equals(address, testOrder.address)
                    && rowKind == testOrder.rowKind;
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderId, itemId, amount, address, rowKind);
        }

        @Override
        public String toString() {
            return "TestOrder{"
                    + "orderId="
                    + orderId
                    + ", itemId="
                    + itemId
                    + ", amount="
                    + amount
                    + ", address='"
                    + address
                    + '\''
                    + ", rowKind="
                    + rowKind
                    + '}';
        }
    }

    private static class TestOrderSerializationSchema
            implements FlussSerializationSchema<TestOrder> {
        private static final long serialVersionUID = 1L;

        @Override
        public void open(InitializationContext context) throws Exception {}

        @Override
        public RowWithOp serialize(TestOrder value) throws Exception {
            GenericRow row = new GenericRow(4);
            row.setField(0, value.orderId);
            row.setField(1, value.itemId);
            row.setField(2, value.amount);
            row.setField(3, BinaryString.fromString(value.address));

            RowKind rowKind = value.rowKind;
            switch (rowKind) {
                case INSERT:
                case UPDATE_AFTER:
                    return new RowWithOp(row, OperationType.UPSERT);
                case UPDATE_BEFORE:
                case DELETE:
                    return new RowWithOp(row, OperationType.DELETE);
                default:
                    throw new IllegalArgumentException("Unsupported row kind: " + rowKind);
            }
        }
    }
}
