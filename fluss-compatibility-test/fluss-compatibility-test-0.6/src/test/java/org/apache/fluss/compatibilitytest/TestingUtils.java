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

package org.apache.fluss.compatibilitytest;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.BIGINT_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.BOOLEAN_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.INT_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.STRING_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.TINYINT_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.commons.util.Preconditions.condition;

/** Utils for compatibility test for fluss-0.6 client. */
public class TestingUtils {
    public static boolean serverReady(int port, int serverId, boolean isTabletServer)
            throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:" + port);
        Connection connection = ConnectionFactory.createConnection(conf);
        connection.close();

        RpcClient rpcClient =
                RpcClient.create(
                        conf, new ClientMetricGroup(MetricRegistry.create(conf, null), "1"));
        MetadataUpdater metadataUpdater = new MetadataUpdater(conf, rpcClient);
        Cluster cluster =
                sendMetadataRequestAndRebuildCluster(
                        GatewayClientProxy.createGatewayProxy(
                                () ->
                                        new ServerNode(
                                                serverId,
                                                "localhost",
                                                port,
                                                isTabletServer
                                                        ? ServerType.TABLET_SERVER
                                                        : ServerType.COORDINATOR),
                                rpcClient,
                                AdminGateway.class),
                        false,
                        metadataUpdater.getCluster(),
                        null,
                        null,
                        null);
        boolean isReady =
                cluster.getCoordinatorServer() != null
                        && cluster.getAliveTabletServers().size() == 1;
        rpcClient.close();
        return isReady;
    }

    public static void createTable(Admin admin, TestingTableDescriptor testingTd) throws Exception {
        TablePath tablePath = TablePath.of(testingTd.getDbName(), testingTd.getTableName());
        Schema schema =
                buildSchema(
                        testingTd.getColumns(),
                        testingTd.getColumnTypes(),
                        testingTd.getPrimaryKeys());
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy(testingTd.getPartitionKeys())
                        .properties(testingTd.getProperties())
                        .build();
        admin.createTable(tablePath, descriptor, false).get();
    }

    public static void produceLog(
            Connection flussConnection, TablePath tablePath, List<Object[]> records)
            throws Exception {
        try (Table table = flussConnection.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            records.forEach(record -> appendWriter.append(row(record)));
        }
    }

    public static List<Object[]> poll(
            Connection flussConnection, TablePath tablePath, LogScanner scanner, Duration timeout)
            throws Exception {
        try (Table table = flussConnection.getTable(tablePath)) {
            ScanRecords records = scanner.poll(timeout);
            return recordsToObjects(records, table.getTableInfo().getRowType());
        }
    }

    public static void putKv(
            Connection flussConnection, TablePath tablePath, List<Object[]> records)
            throws Exception {
        try (Table table = flussConnection.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            records.forEach(record -> upsertWriter.upsert(row(record)));
        }
    }

    public static @Nullable Object[] lookup(
            Connection flussConnection, TablePath tablePath, Object[] key) throws Exception {
        try (Table table = flussConnection.getTable(tablePath)) {
            RowType rowType = table.getTableInfo().getRowType();
            InternalRow.FieldGetter[] fieldGetters =
                    new InternalRow.FieldGetter[rowType.getFieldCount()];
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
            }

            InternalRow row =
                    table.newLookup().createLookuper().lookup(row(key)).get().getSingletonRow();
            return row == null ? null : rowToObjects(row, fieldGetters);
        }
    }

    public static OffsetSpec toOffsetSpec(TestingOffsetSpec offsetSpec) {
        if (offsetSpec.type == OffsetSpec.LIST_EARLIEST_OFFSET) {
            return new OffsetSpec.EarliestSpec();
        } else if (offsetSpec.type == OffsetSpec.LIST_LATEST_OFFSET) {
            return new OffsetSpec.LatestSpec();
        } else if (offsetSpec.type == OffsetSpec.LIST_OFFSET_FROM_TIMESTAMP) {
            assertThat(offsetSpec.timestamp).isNotNull();
            return new OffsetSpec.TimestampSpec(offsetSpec.timestamp);
        } else {
            throw new IllegalArgumentException("Unsupported offset spec type: " + offsetSpec.type);
        }
    }

    private static GenericRow row(Object... objects) {
        GenericRow row = new GenericRow(objects.length);
        for (int i = 0; i < objects.length; i++) {
            if (objects[i] instanceof String) {
                row.setField(i, BinaryString.fromString((String) objects[i]));
            } else {
                row.setField(i, objects[i]);
            }
        }
        return row;
    }

    private static List<Object[]> recordsToObjects(ScanRecords records, RowType rowType) {
        InternalRow.FieldGetter[] fieldGetters =
                new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
        List<Object[]> result = new ArrayList<>();
        for (ScanRecord record : records) {
            InternalRow row = record.getRow();
            result.add(rowToObjects(row, fieldGetters));
        }
        return result;
    }

    private static Object[] rowToObjects(InternalRow row, InternalRow.FieldGetter[] fieldGetters) {
        Object[] object = new Object[fieldGetters.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            Object filedVar = fieldGetters[i].getFieldOrNull(row);
            if (filedVar instanceof BinaryString) {
                object[i] = ((BinaryString) filedVar).toString();
            } else {
                object[i] = filedVar;
            }
        }
        return object;
    }

    private static Schema buildSchema(
            List<String> columns, List<String> columnTypes, List<String> primaryKeys) {
        condition(
                columns.size() == columnTypes.size(),
                "The size of columns and columnTypes should be the same.");
        Schema.Builder builder = Schema.newBuilder();
        for (int i = 0; i < columns.size(); i++) {
            builder.column(columns.get(i), getDataType(columnTypes.get(i)));
        }

        if (!primaryKeys.isEmpty()) {
            builder.primaryKey(primaryKeys);
        }

        return builder.build();
    }

    private static DataType getDataType(String type) {
        switch (type) {
            case BOOLEAN_TYPE:
                return DataTypes.BOOLEAN();
            case INT_TYPE:
                return DataTypes.INT();
            case BIGINT_TYPE:
                return DataTypes.BIGINT();
            case STRING_TYPE:
                return DataTypes.STRING();
            case TINYINT_TYPE:
                return DataTypes.TINYINT();
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }
}
