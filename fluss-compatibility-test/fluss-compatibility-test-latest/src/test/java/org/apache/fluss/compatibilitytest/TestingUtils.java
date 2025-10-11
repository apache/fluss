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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.BIGINT_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.BOOLEAN_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.INT_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.STRING_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.TINYINT_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.commons.util.Preconditions.condition;

/** Utils for compatibility test for fluss latest version client. */
public class TestingUtils {

    public static final Map<String, String> CLIENT_SALS_PROPERTIES = new HashMap<>();

    static {
        CLIENT_SALS_PROPERTIES.put("client.security.protocol", "SASL");
        CLIENT_SALS_PROPERTIES.put("client.security.sasl.mechanism", "PLAIN");
        CLIENT_SALS_PROPERTIES.put("client.security.sasl.username", "admin");
        CLIENT_SALS_PROPERTIES.put("client.security.sasl.password", "admin-pass");
    }

    public static boolean serverReady(
            int port, int serverId, boolean isTabletServer, boolean serverSupportAuth)
            throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:" + port);
        if (serverSupportAuth) {
            CLIENT_SALS_PROPERTIES.forEach(conf::setString);
        }
        Connection connection = ConnectionFactory.createConnection(conf);
        connection.close();

        RpcClient rpcClient =
                RpcClient.create(
                        conf, new ClientMetricGroup(MetricRegistry.create(conf, null), "1"), false);
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

    public static TestingAclBinding fromAclBinding(AclBinding aclBinding) {
        Resource resource = aclBinding.getResource();
        AccessControlEntry accessControlEntry = aclBinding.getAccessControlEntry();
        TestingAclBinding.Resource testingResource =
                new TestingAclBinding.Resource(
                        TestingAclBinding.ResourceType.valueOf(resource.getType().name()),
                        resource.getName());
        FlussPrincipal principal = accessControlEntry.getPrincipal();
        TestingAclBinding.AccessControlEntry testingAccessControlEntry =
                new TestingAclBinding.AccessControlEntry(
                        principal.getName(),
                        principal.getType(),
                        TestingAclBinding.PermissionType.valueOf(
                                accessControlEntry.getPermissionType().name()),
                        accessControlEntry.getHost(),
                        TestingAclBinding.OperationType.valueOf(
                                accessControlEntry.getOperationType().name()));
        return new TestingAclBinding(testingResource, testingAccessControlEntry);
    }

    public static AclBinding toAclBinding(TestingAclBinding testingAclBinding) {
        TestingAclBinding.Resource testingResource = testingAclBinding.resource;
        TestingAclBinding.AccessControlEntry testingAccessControlEntry =
                testingAclBinding.accessControlEntry;
        Resource resource =
                new Resource(
                        ResourceType.fromName(testingResource.type.name()), testingResource.name);
        AccessControlEntry accessControlEntry =
                new AccessControlEntry(
                        new FlussPrincipal(
                                testingAccessControlEntry.principalName,
                                testingAccessControlEntry.principalType),
                        testingAccessControlEntry.host,
                        OperationType.valueOf(testingAccessControlEntry.operationType.name()),
                        PermissionType.valueOf(testingAccessControlEntry.permissionType.name()));
        return new AclBinding(resource, accessControlEntry);
    }

    public static TestingConfigEntry fromConfigEntry(ConfigEntry configEntry) {
        return new TestingConfigEntry(
                configEntry.key(),
                configEntry.value(),
                TestingConfigEntry.ConfigSource.valueOf(configEntry.source().name()));
    }

    public static AlterConfig toAlterConfig(TestingAlterConfig testingAlterConfig) {
        return new AlterConfig(
                testingAlterConfig.key,
                testingAlterConfig.value,
                AlterConfigOpType.valueOf(testingAlterConfig.opType.name()));
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
