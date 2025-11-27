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

package org.apache.fluss.flink.sink;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

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

import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkRowKind;
import static org.apache.fluss.testutils.DataTestUtils.row;
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

    @Test
    public void testPartialUpdateWithTwoWriters() throws Exception {
        createTable(TablePath.of(DEFAULT_DB, "partial_update_two_writers_test"), pkTableDescriptor);

        // Initial inserts
        ArrayList<TestOrder> initialOrders = new ArrayList<>();
        initialOrders.add(new TestOrder(2001, 3001, -1, null, RowKind.INSERT));
        initialOrders.add(new TestOrder(2002, 3002, -1, null, RowKind.INSERT));
        initialOrders.add(new TestOrder(2003, 3003, -1, null, RowKind.INSERT));

        DataStream<TestOrder> initialStream = env.fromData(initialOrders);

        FlinkSink<TestOrder> initialSink =
                FlussSink.<TestOrder>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable("partial_update_two_writers_test")
                        .setPartialUpdateColumns("orderId", "itemId")
                        .setSerializationSchema(new TestOrderSerializationSchema())
                        .build();

        initialStream.sinkTo(initialSink).name("Fluss Initial Data Sink");
        env.execute("First Stream Updates");

        ArrayList<TestOrder> itemIdUpdates = new ArrayList<>();
        itemIdUpdates.add(new TestOrder(2001, -1, 100, "addr1", RowKind.UPDATE_AFTER));
        itemIdUpdates.add(new TestOrder(2003, -1, 300, "addr3", RowKind.UPDATE_AFTER));

        DataStream<TestOrder> updateStream = env.fromData(itemIdUpdates);

        FlinkSink<TestOrder> updateSink =
                FlussSink.<TestOrder>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable("partial_update_two_writers_test")
                        .setPartialUpdateColumns("orderId", "amount", "address")
                        .setSerializationSchema(new TestOrderSerializationSchema())
                        .build();

        updateStream.sinkTo(updateSink).name("Fluss Amount/Address Update Sink");
        env.execute("Test Amount/Address Updates");

        Table table = conn.getTable(new TablePath(DEFAULT_DB, "partial_update_two_writers_test"));
        LogScanner logScanner = table.newScan().createLogScanner();

        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }

        // Build expected change log: 3 inserts, then before/after for 2001 and 2003
        List<TestOrder> expected = new ArrayList<>();
        expected.add(new TestOrder(2001, 3001, -1, null, RowKind.INSERT));
        expected.add(new TestOrder(2002, 3002, -1, null, RowKind.INSERT));
        expected.add(new TestOrder(2003, 3003, -1, null, RowKind.INSERT));
        // update for 2001: before and after (itemId stays, amount/address updated)
        expected.add(new TestOrder(2001, 3001, -1, null, RowKind.UPDATE_BEFORE));
        expected.add(new TestOrder(2001, 3001, 100, "addr1", RowKind.UPDATE_AFTER));
        // update for 2003
        expected.add(new TestOrder(2003, 3003, -1, null, RowKind.UPDATE_BEFORE));
        expected.add(new TestOrder(2003, 3003, 300, "addr3", RowKind.UPDATE_AFTER));

        // Poll actual changelog until we have all expected records or timeout
        List<TestOrder> actual = new ArrayList<>();
        int idlePolls = 0;
        int maxIdlePolls = 60; // ~60s max wait
        while (actual.size() < expected.size() && idlePolls < maxIdlePolls) {
            int sizeBefore = actual.size();
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    InternalRow row = record.getRow();
                    String address = row.getString(3) != null ? row.getString(3).toString() : null;
                    TestOrder order =
                            new TestOrder(
                                    row.getLong(0),
                                    row.getLong(1),
                                    row.getInt(2),
                                    address,
                                    toFlinkRowKind(record.getChangeType()));
                    actual.add(order);
                }
            }
            idlePolls = (actual.size() == sizeBefore) ? idlePolls + 1 : 0;
        }

        assertThat(actual.size()).isEqualTo(expected.size());
        assertThat(actual).containsAll(expected);

        logScanner.close();
    }

    @Test
    public void testPartialUpdateWithTwoWritersWithRD() throws Exception {
        // Create PK table
        createTable(TablePath.of(DEFAULT_DB, pkTableName), pkTableDescriptor);

        // Helper converter for building RowData
        FlussRowToFlinkRowConverter converter =
                new FlussRowToFlinkRowConverter(pkSchema.getRowType());

        // Initial inserts
        List<RowData> inserts = new ArrayList<>();
        RowData i1 = converter.toFlinkRowData(row(101L, 1001L, 10, "a1"));
        i1.setRowKind(RowKind.INSERT);
        RowData i2 = converter.toFlinkRowData(row(102L, 1002L, 20, "a2"));
        i2.setRowKind(RowKind.INSERT);
        RowData i3 = converter.toFlinkRowData(row(103L, 1003L, 30, "a3"));
        i3.setRowKind(RowKind.INSERT);
        RowData i4 = converter.toFlinkRowData(row(104L, 1004L, 40, "a4"));
        i4.setRowKind(RowKind.INSERT);
        inserts.add(i1);
        inserts.add(i2);
        inserts.add(i3);
        inserts.add(i4);

        // Writer 1: updates only address for keys 101, 102 (partial columns: orderId, address)
        List<RowData> updatesWriter1 = new ArrayList<>();
        // Set other columns to null to emphasize partial update behavior
        RowData u1a = converter.toFlinkRowData(row(101L, null, null, "a1_new"));
        u1a.setRowKind(RowKind.UPDATE_AFTER);
        RowData u2a = converter.toFlinkRowData(row(102L, null, null, "a2_new"));
        u2a.setRowKind(RowKind.UPDATE_AFTER);
        updatesWriter1.add(u1a);
        updatesWriter1.add(u2a);

        // Writer 2: updates only amount for keys 103, 104 (partial columns: orderId, amount)
        List<RowData> updatesWriter2 = new ArrayList<>();
        RowData u3b = converter.toFlinkRowData(row(103L, null, 31, null));
        u3b.setRowKind(RowKind.UPDATE_AFTER);
        RowData u4b = converter.toFlinkRowData(row(104L, null, 41, null));
        u4b.setRowKind(RowKind.UPDATE_AFTER);
        updatesWriter2.add(u3b);
        updatesWriter2.add(u4b);

        // Execute initial inserts in a dedicated, synchronous Flink job to ensure they land first
        StreamExecutionEnvironment insertEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> insertStream = insertEnv.fromData(inserts);
        RowDataSerializationSchema insertSerializationSchema =
                new RowDataSerializationSchema(false, true);
        FlinkSink<RowData> insertSink =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setSerializationSchema(insertSerializationSchema)
                        .build();

        insertStream.sinkTo(insertSink).name("Fluss Insert Sink");
        // finish inserts before starting partial update writers
        insertEnv.execute("Test Inserts for Partial Updates Two Writers");

        // Now start partial updates in a separate async job to avoid racing with inserts
        StreamExecutionEnvironment updatesEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> streamWriter1 = updatesEnv.fromData(updatesWriter1);
        DataStream<RowData> streamWriter2 = updatesEnv.fromData(updatesWriter2);

        RowDataSerializationSchema updatesSerializationSchema =
                new RowDataSerializationSchema(false, true);

        // Partial update sink 1: orderId + address
        FlinkSink<RowData> partialSink1 =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setSerializationSchema(updatesSerializationSchema)
                        .setPartialUpdateColumns("orderId", "address")
                        .build();

        // Partial update sink 2: orderId + amount
        FlinkSink<RowData> partialSink2 =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setSerializationSchema(updatesSerializationSchema)
                        .setPartialUpdateColumns("orderId", "amount")
                        .build();

        streamWriter1.sinkTo(partialSink1).name("Fluss Partial Sink 1");
        streamWriter2.sinkTo(partialSink2).name("Fluss Partial Sink 2");

        updatesEnv.executeAsync("Test Partial Updates Two Writers");

        // Consume change-log and assert contents
        Table table = conn.getTable(new TablePath(DEFAULT_DB, pkTableName));
        LogScanner logScanner = table.newScan().createLogScanner();
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }

        List<RowData> expected = new ArrayList<>();
        expected.addAll(inserts);
        // Add UPDATE_BEFORE for each partial update (based on initial state of those rows)
        RowData b1 = converter.toFlinkRowData(row(101L, 1001L, 10, "a1"));
        b1.setRowKind(RowKind.UPDATE_BEFORE);
        RowData b2 = converter.toFlinkRowData(row(102L, 1002L, 20, "a2"));
        b2.setRowKind(RowKind.UPDATE_BEFORE);
        RowData b3 = converter.toFlinkRowData(row(103L, 1003L, 30, "a3"));
        b3.setRowKind(RowKind.UPDATE_BEFORE);
        RowData b4 = converter.toFlinkRowData(row(104L, 1004L, 40, "a4"));
        b4.setRowKind(RowKind.UPDATE_BEFORE);
        expected.add(b1);
        expected.add(b2);
        expected.add(b3);
        expected.add(b4);
        // And the UPDATE_AFTER rows as provided in the update streams, but with full after-image
        // Writer1 updates address only; amount/itemId should remain as before
        RowData a1 = converter.toFlinkRowData(row(101L, 1001L, 10, "a1_new"));
        a1.setRowKind(RowKind.UPDATE_AFTER);
        RowData a2 = converter.toFlinkRowData(row(102L, 1002L, 20, "a2_new"));
        a2.setRowKind(RowKind.UPDATE_AFTER);
        // Writer2 updates amount only
        RowData a3 = converter.toFlinkRowData(row(103L, 1003L, 31, "a3"));
        a3.setRowKind(RowKind.UPDATE_AFTER);
        RowData a4 = converter.toFlinkRowData(row(104L, 1004L, 41, "a4"));
        a4.setRowKind(RowKind.UPDATE_AFTER);
        expected.add(a1);
        expected.add(a2);
        expected.add(a3);
        expected.add(a4);

        List<RowData> actual = new ArrayList<>();
        int idlePolls = 0;
        int maxIdlePolls = 60; // ~60s max wait to avoid indefinite hang
        while (actual.size() < expected.size() && idlePolls < maxIdlePolls) {
            int sizeBefore = actual.size();
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    RowData row = converter.toFlinkRowData(record.getRow());
                    row.setRowKind(toFlinkRowKind(record.getChangeType()));
                    actual.add(row);
                }
            }
            idlePolls = (actual.size() == sizeBefore) ? idlePolls + 1 : 0;
        }

        assertThat(actual.size()).isEqualTo(expected.size());
        assertThat(actual).containsAll(expected);

        logScanner.close();
    }

    @Test
    public void testPartialUpdateSingleWriterNullRemainder() throws Exception {
        // Create PK table
        createTable(TablePath.of(DEFAULT_DB, pkTableName), pkTableDescriptor);

        FlussRowToFlinkRowConverter converter =
                new FlussRowToFlinkRowConverter(pkSchema.getRowType());

        // Initial insert full row
        List<RowData> inserts = new ArrayList<>();
        RowData i1 = converter.toFlinkRowData(row(201L, 2001L, 100, "x1"));
        i1.setRowKind(RowKind.INSERT);
        inserts.add(i1);

        // Partial update only address (orderId, address). Other columns are explicitly null in
        // input
        List<RowData> updates = new ArrayList<>();
        RowData u1 = converter.toFlinkRowData(row(201L, null, null, "x1_new"));
        u1.setRowKind(RowKind.UPDATE_AFTER);
        updates.add(u1);

        // Stage 1: run inserts in a dedicated synchronous job to avoid race with partial updates
        StreamExecutionEnvironment insertEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> insertStream = insertEnv.fromData(inserts);
        RowDataSerializationSchema insertSerializationSchema =
                new RowDataSerializationSchema(false, true);
        FlinkSink<RowData> insertSink =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setSerializationSchema(insertSerializationSchema)
                        .build();
        insertStream.sinkTo(insertSink).name("Fluss Insert Sink");
        insertEnv.execute("Test Partial Update Single Writer - Inserts");

        // Stage 2: start partial updates in a separate async job
        StreamExecutionEnvironment updateEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> updateStream = updateEnv.fromData(updates);
        RowDataSerializationSchema updateSerializationSchema =
                new RowDataSerializationSchema(false, true);
        FlinkSink<RowData> partialSink =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setSerializationSchema(updateSerializationSchema)
                        .setPartialUpdateColumns("orderId", "address")
                        .build();
        updateStream.sinkTo(partialSink).name("Fluss Partial Sink");
        updateEnv.executeAsync("Test Partial Update Single Writer - Updates");

        // Read changelog
        Table table = conn.getTable(new TablePath(DEFAULT_DB, pkTableName));
        LogScanner logScanner = table.newScan().createLogScanner();
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }

        List<RowData> expected = new ArrayList<>();
        expected.addAll(inserts);
        // Before image prior to the address update
        RowData before = converter.toFlinkRowData(row(201L, 2001L, 100, "x1"));
        before.setRowKind(RowKind.UPDATE_BEFORE);
        expected.add(before);
        // After image: only address changed; other fields remain unchanged (not null)
        RowData after = converter.toFlinkRowData(row(201L, 2001L, 100, "x1_new"));
        after.setRowKind(RowKind.UPDATE_AFTER);
        expected.add(after);

        List<RowData> actual = new ArrayList<>();
        int idlePolls = 0;
        int maxIdlePolls = 60; // ~60s max wait to avoid indefinite hang
        while (actual.size() < expected.size() && idlePolls < maxIdlePolls) {
            int sizeBefore = actual.size();
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    RowData row = converter.toFlinkRowData(record.getRow());
                    row.setRowKind(toFlinkRowKind(record.getChangeType()));
                    actual.add(row);
                }
            }
            idlePolls = (actual.size() == sizeBefore) ? idlePolls + 1 : 0;
        }

        assertThat(actual.size()).isEqualTo(expected.size());
        assertThat(actual).containsAll(expected);

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
