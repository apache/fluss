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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.compatibilitytest.CompatEnvironment.CLIENT_SALS_PROPERTIES;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_06_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_07_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_LATEST_VERSION_MAGIC;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Basic class for fluss client connect to server compatibility test. */
@Testcontainers
public class ClientToServerCompatTest extends CompatTest {
    public static final Schema DATA1_LOG_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .build();

    public static final Schema DATA1_KV_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .primaryKey("a")
                    .build();

    public static final Schema DATA1_PARTITIONED_LOG_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .column("c", DataTypes.STRING())
                    .withComment("c is third column")
                    .build();

    private @Nullable Connection flussConnection;
    private @Nullable Admin admin;

    @Test
    void testConnectTo06Server() throws Exception {
        clientToServerTestPipeline(FLUSS_06_VERSION_MAGIC);
    }

    @Test
    void testConnectTo07Server() throws Exception {
        clientToServerTestPipeline(FLUSS_07_VERSION_MAGIC);
    }

    @Test
    void testConnectToLatestServer() throws Exception {
        clientToServerTestPipeline(FLUSS_LATEST_VERSION_MAGIC);
    }

    private void clientToServerTestPipeline(int serverVersionMagic) throws Exception {
        // start the server.
        initAndStartFlussServer(serverVersionMagic);
        initFlussConnection(serverVersionMagic);

        // test admin compact.
        verifyAdmin(serverVersionMagic);

        // verify produceLog and fetchLog.
        verifyProduceAndFetchLog();

        // verify putKv and Lookup.
        verifyPutKvAndLookup();

        // TODO test lake part.

        stopServer();
    }

    private void verifyAdmin(int serverVersionMagic) throws Exception {
        verifyGetServerNodes();
        verifyTable(serverVersionMagic);
        verifySingleFiledPartition(serverVersionMagic);
        verifyMultiFiledPartition(serverVersionMagic);

        // listOffsets has been verified in verifyProduceAndFetchLog().
        // LatestKvSnapshots has been verified in verifyPutKvAndLookup().

        // TODO LatestLakeSnapshots will be verified in lake part.

        // for server version lower than 0.7, the acl is not supported.
        if (serverVersionMagic > FLUSS_06_VERSION_MAGIC) {
            verifyAcls();
        }

        // for server version lower than 0.8, alter/describe cluster configs is not supported.
        if (serverVersionMagic > FLUSS_07_VERSION_MAGIC) {
            verifyClusterConfig();
        }
    }

    void initFlussConnection(int serverVersion) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:" + coordinatorServerPort);
        if (serverVersion >= FLUSS_07_VERSION_MAGIC) {
            CLIENT_SALS_PROPERTIES.forEach(conf::setString);
        }
        flussConnection = ConnectionFactory.createConnection(conf);
        admin = flussConnection.getAdmin();
    }

    void verifyGetServerNodes() throws Exception {
        Admin admin = getAdmin();
        List<ServerNode> serverNodeList = admin.getServerNodes().get();
        assertThat(serverNodeList).hasSize(2);
        ServerNode csNode =
                serverNodeList.get(0).serverType() == ServerType.COORDINATOR
                        ? serverNodeList.get(0)
                        : serverNodeList.get(1);
        ServerNode tsNode =
                serverNodeList.get(0).serverType() == ServerType.TABLET_SERVER
                        ? serverNodeList.get(0)
                        : serverNodeList.get(1);
        assertThat(csNode.serverType()).isEqualTo(ServerType.COORDINATOR);
        assertThat(csNode.id()).isEqualTo(0);
        assertThat(csNode.uid()).isEqualTo("cs-0");

        assertThat(tsNode.serverType()).isEqualTo(ServerType.TABLET_SERVER);
        assertThat(tsNode.id()).isEqualTo(0);
        assertThat(tsNode.uid()).isEqualTo("ts-0");
    }

    /** Verify table operations. */
    private void verifyTable(int serverVersionMagic) throws Exception {
        Admin admin = getAdmin();
        String dbName = "db-for-table-1";
        createDatabase(admin, dbName);
        assertThat(admin.databaseExists(dbName).get()).isTrue();
        assertThat(admin.listTables(dbName).get()).isEmpty();

        // verify create log table.
        String logTableName = "log-table-1";
        TablePath logTablePath = TablePath.of(dbName, logTableName);
        assertThat(admin.tableExists(logTablePath).get()).isFalse();

        TableDescriptor logTableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_LOG_SCHEMA)
                        .properties(Collections.singletonMap("table.datalake.enabled", "false"))
                        .build();
        createTable(admin, logTablePath, logTableDescriptor);
        verifyGetTableSchema(logTablePath, logTableDescriptor);
        verifyGetTableInfo(logTablePath, logTableDescriptor);
        assertThat(admin.tableExists(logTablePath).get()).isTrue();
        assertThat(admin.listTables(dbName).get()).containsExactlyInAnyOrder(logTableName);

        // verify create primary key table.
        String kvTableName = "kv-table-1";
        TablePath kvTablePath = TablePath.of(dbName, kvTableName);
        assertThat(admin.tableExists(kvTablePath).get()).isFalse();
        TableDescriptor kvTableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_KV_SCHEMA)
                        .properties(Collections.singletonMap("table.datalake.enabled", "false"))
                        .build();
        createTable(admin, kvTablePath, kvTableDescriptor);
        verifyGetTableSchema(kvTablePath, kvTableDescriptor);
        verifyGetTableInfo(kvTablePath, kvTableDescriptor);
        assertThat(admin.tableExists(kvTablePath).get()).isTrue();
        assertThat(admin.listTables(dbName).get())
                .containsExactlyInAnyOrder(logTableName, kvTableName);

        // test alter table, verify reset and set option.
        // for server version lower than 0.8, the alterTable is not supported.
        if (serverVersionMagic > FLUSS_07_VERSION_MAGIC) {
            verifyAlterTable(
                    kvTablePath,
                    kvTableDescriptor,
                    Collections.singletonMap("table.datalake.enabled", "true"));
        }

        dropTable(kvTablePath);
        assertThat(admin.tableExists(kvTablePath).get()).isFalse();
        assertThat(admin.listTables(dbName).get()).containsExactlyInAnyOrder(logTableName);

        dropTable(logTablePath);
        assertThat(admin.listTables(dbName).get()).isEmpty();
        dropDatabase(dbName);
    }

    private void dropDatabase(String dbName) throws Exception {
        Admin admin = getAdmin();
        admin.dropDatabase(dbName, false, false).get();
    }

    private void dropTable(TablePath tablePath) throws Exception {
        Admin admin = getAdmin();
        admin.dropTable(tablePath, false).get();
    }

    private void verifyGetTableSchema(TablePath tablePath, TableDescriptor expectedTableDescriptor)
            throws Exception {
        Admin admin = getAdmin();
        SchemaInfo schemaInfo = admin.getTableSchema(tablePath).get();
        assertThat(schemaInfo.getSchemaId()).isEqualTo(1);
        Schema schema = schemaInfo.getSchema();
        assertThat(expectedTableDescriptor.getSchema()).isEqualTo(schema);

        // test get schema by id.
        schemaInfo = admin.getTableSchema(tablePath, 1).get();
        assertThat(schemaInfo.getSchemaId()).isEqualTo(1);
        schema = schemaInfo.getSchema();
        assertThat(expectedTableDescriptor.getSchema()).isEqualTo(schema);
    }

    private void verifyGetTableInfo(TablePath tablePath, TableDescriptor expectedTableDescriptor)
            throws Exception {
        Admin admin = getAdmin();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        TableDescriptor descriptor = tableInfo.toTableDescriptor();
        assertThat(descriptor.getSchema()).isEqualTo(expectedTableDescriptor.getSchema());
        assertThat(descriptor.getPartitionKeys())
                .isEqualTo(expectedTableDescriptor.getPartitionKeys());
        Map<String, String> actualProperties = descriptor.getProperties();
        expectedTableDescriptor
                .getProperties()
                .forEach((k, v) -> assertThat(actualProperties).containsEntry(k, v));
    }

    private void verifyAlterTable(
            TablePath tablePath, TableDescriptor tableDescriptor, Map<String, String> alterOptions)
            throws Exception {
        Admin admin = getAdmin();
        verifyGetTableInfo(tablePath, tableDescriptor);

        List<TableChange> tableChanges = new ArrayList<>();
        alterOptions.forEach(
                (k, v) -> {
                    if (v == null) {
                        tableChanges.add(TableChange.reset(k));
                    } else {
                        tableChanges.add(TableChange.set(k, v));
                    }
                });

        admin.alterTable(tablePath, tableChanges, false).get();
        // verify changes.
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        Map<String, String> properties = tableInfo.getProperties().toMap();
        alterOptions.forEach(
                (k, v) -> {
                    if (v == null) {
                        assertThat(properties).doesNotContainKey(k);
                    } else {
                        assertThat(properties).containsEntry(k, v);
                    }
                });
    }

    private void verifySingleFiledPartition(int serverVersionMagic) throws Exception {
        Admin admin = getAdmin();
        String dbName = "db-for-partition-1";
        createDatabase(admin, dbName);

        // create a partitioned log table with auto partition open.
        String logTableName = "partitioned-log-table-1";
        TablePath logTablePath = TablePath.of(dbName, logTableName);
        Map<String, String> properties = new HashMap<>();
        properties.put("table.auto-partition.enabled", "true");
        properties.put("table.auto-partition.num-precreate", "2");
        properties.put("table.auto-partition.time-unit", "YEAR");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_PARTITIONED_LOG_SCHEMA)
                        .partitionedBy("c")
                        .properties(properties)
                        .build();
        createTable(admin, logTablePath, tableDescriptor);
        assertThat(admin.tableExists(logTablePath).get()).isTrue();
        int currentYear = LocalDate.now().getYear();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    List<String> partitions = listPartitions(logTablePath, null);
                    assertThat(partitions)
                            .containsExactlyInAnyOrder(
                                    String.valueOf(currentYear), String.valueOf(currentYear + 1));
                });

        // for server version lower than 0.7, the list partitions by is not supported.
        if (serverVersionMagic > FLUSS_06_VERSION_MAGIC) {
            // verify listPartitions by spec.
            List<String> partitions =
                    listPartitions(
                            logTablePath,
                            new PartitionSpec(
                                    Collections.singletonMap("c", String.valueOf(currentYear))));
            assertThat(partitions).containsExactlyInAnyOrder(String.valueOf(currentYear));
        }

        // verify create partition.
        admin.createPartition(
                        logTablePath,
                        new PartitionSpec(Collections.singletonMap("c", "1997")),
                        false)
                .get();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    List<String> partitions = listPartitions(logTablePath, null);
                    assertThat(partitions)
                            .containsExactlyInAnyOrder(
                                    String.valueOf(currentYear),
                                    String.valueOf(currentYear + 1),
                                    "1997");
                });

        // verify drop partition.
        admin.dropPartition(
                        logTablePath,
                        new PartitionSpec(Collections.singletonMap("c", "1997")),
                        false)
                .get();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    List<String> partitions = listPartitions(logTablePath, null);
                    assertThat(partitions)
                            .containsExactlyInAnyOrder(
                                    String.valueOf(currentYear), String.valueOf(currentYear + 1));
                });

        dropTable(logTablePath);
        dropDatabase(dbName);
    }

    private void verifyMultiFiledPartition(int serverVersionMagic) throws Exception {
        // for server version lower than 0.7, multi filed partition is not supported.
        if (serverVersionMagic < FLUSS_07_VERSION_MAGIC) {
            return;
        }

        Admin admin = getAdmin();
        String dbName = "db-for-partition-2";
        createDatabase(admin, dbName);

        // create a multi filed partitioned log table with auto partition close.
        String logTableName = "partitioned-log-table-2";
        TablePath logTablePath = TablePath.of(dbName, logTableName);
        Map<String, String> properties = new HashMap<>();
        properties.put("table.auto-partition.enabled", "false");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_PARTITIONED_LOG_SCHEMA)
                        .partitionedBy("b", "c")
                        .properties(properties)
                        .build();
        createTable(admin, logTablePath, tableDescriptor);

        assertThat(listPartitions(logTablePath, null)).isEmpty();
        // create partition.
        Map<String, String> spec = new HashMap<>();
        spec.put("b", "b1");
        spec.put("c", "c1");
        admin.createPartition(logTablePath, new PartitionSpec(spec), false).get();
        spec = new HashMap<>();
        spec.put("b", "b1");
        spec.put("c", "c2");
        admin.createPartition(logTablePath, new PartitionSpec(spec), false).get();

        Map<String, String> spec2 = new HashMap<>();
        spec2.put("b", "b1");
        retry(
                Duration.ofMinutes(1),
                () -> {
                    List<String> partitions =
                            listPartitions(logTablePath, new PartitionSpec(spec2));
                    assertThat(partitions).containsExactlyInAnyOrder("b1$c1", "b1$c2");
                });

        admin.dropPartition(logTablePath, new PartitionSpec(spec), false).get();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    List<String> partitions = listPartitions(logTablePath, null);
                    assertThat(partitions).containsExactlyInAnyOrder("b1$c1");
                });

        dropTable(logTablePath);
        dropDatabase(dbName);
    }

    List<String> listPartitions(TablePath tablePath, @Nullable PartitionSpec partitionSpec)
            throws Exception {
        Admin admin = getAdmin();
        List<PartitionInfo> partitionInfos;
        if (partitionSpec == null) {
            partitionInfos = admin.listPartitionInfos(tablePath).get();
        } else {
            partitionInfos = admin.listPartitionInfos(tablePath, partitionSpec).get();
        }

        return partitionInfos.stream()
                .map(PartitionInfo::getPartitionName)
                .collect(Collectors.toList());
    }

    private void verifyAcls() throws Exception {
        Admin admin = getAdmin();
        List<AclBinding> testingAclBindings =
                new ArrayList<>(admin.listAcls(AclBindingFilter.ANY).get());
        assertThat(testingAclBindings).isEmpty();

        AclBinding aclBinding =
                new AclBinding(
                        new Resource(ResourceType.CLUSTER, "fluss-cluster"),
                        new AccessControlEntry(
                                new FlussPrincipal("guest", "User"),
                                "127.0.0.1",
                                OperationType.DESCRIBE,
                                PermissionType.ALLOW));
        admin.createAcls(Collections.singleton(aclBinding)).all().get();

        testingAclBindings = new ArrayList<>(admin.listAcls(AclBindingFilter.ANY).get());
        assertThat(testingAclBindings).containsExactly(aclBinding);

        // drop all acls.
        admin.dropAcls(Collections.singletonList(AclBindingFilter.ANY)).all().get();
        testingAclBindings = new ArrayList<>(admin.listAcls(AclBindingFilter.ANY).get());
        assertThat(testingAclBindings).isEmpty();
    }

    private void verifyClusterConfig() throws Exception {
        Admin admin = getAdmin();
        List<ConfigEntry> configEntries = new ArrayList<>(admin.describeClusterConfigs().get());
        Optional<ConfigEntry> first =
                configEntries.stream().filter(e -> e.key().equals("datalake.format")).findFirst();
        assertThat(first).isPresent();
        assertThat(first.get().value()).isEqualTo("paimon");
        assertThat(first.get().source()).isEqualTo(ConfigEntry.ConfigSource.INITIAL_SERVER_CONFIG);

        AlterConfig alterConfig = new AlterConfig("datalake.format", null, AlterConfigOpType.SET);
        admin.alterClusterConfigs(Collections.singleton(alterConfig)).get();

        configEntries = new ArrayList<>(admin.describeClusterConfigs().get());
        first = configEntries.stream().filter(e -> e.key().equals("datalake.format")).findFirst();
        assertThat(first).isPresent();
        assertThat(first.get().value()).isNull();
        assertThat(first.get().source()).isEqualTo(ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG);
    }

    private void verifyProduceAndFetchLog() throws Exception {
        Admin admin = getAdmin();
        Connection flussConnection = getFlussConnection();

        String dbForLogTest = "db-for-log-1";
        createDatabase(admin, dbForLogTest);
        String logTableName = "produce-log-table-1";
        TablePath logTablePath = TablePath.of(dbForLogTest, logTableName);
        TableDescriptor logTableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_LOG_SCHEMA)
                        .properties(Collections.singletonMap("table.datalake.enabled", "false"))
                        .build();
        createTable(admin, logTablePath, logTableDescriptor);

        long timeBeforeInput = System.currentTimeMillis();
        int recordSize = 10;
        List<GenericRow> expectedRows = new ArrayList<>();
        try (Table table = flussConnection.getTable(logTablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < recordSize; i++) {
                GenericRow row = row(i, "a");
                expectedRows.add(row);
                appendWriter.append(row).get();
            }

            LogScanner logScanner = table.newScan().createLogScanner();
            logScanner.subscribe(0, LogScanner.EARLIEST_OFFSET);

            List<GenericRow> rowList = new ArrayList<>();
            while (rowList.size() < recordSize) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    InternalRow row = scanRecord.getRow();
                    rowList.add(row(row.getInt(0), row.getString(1)));
                }
            }
            assertThat(rowList).hasSize(recordSize);
            assertThat(rowList).containsExactlyInAnyOrderElementsOf(expectedRows);
            logScanner.close();
        }

        // test list offset here.
        assertThat(
                        admin.listOffsets(
                                        logTablePath,
                                        Collections.singletonList(0),
                                        new OffsetSpec.EarliestSpec())
                                .all()
                                .get()
                                .get(0))
                .isEqualTo(0L);
        assertThat(
                        admin.listOffsets(
                                        logTablePath,
                                        Collections.singletonList(0),
                                        new OffsetSpec.LatestSpec())
                                .all()
                                .get()
                                .get(0))
                .isEqualTo(10L);
        assertThat(
                        admin.listOffsets(
                                        logTablePath,
                                        Collections.singletonList(0),
                                        new OffsetSpec.TimestampSpec(timeBeforeInput))
                                .all()
                                .get()
                                .get(0))
                .isEqualTo(0L);

        dropTable(logTablePath);
        dropDatabase(dbForLogTest);
    }

    private void verifyPutKvAndLookup() throws Exception {
        Admin admin = getAdmin();
        Connection flussConnection = getFlussConnection();
        String dbForKvTest = "db-for-kv-1";
        createDatabase(admin, dbForKvTest);

        String kvTableName = "put-kv-and-lookup-table-1";
        TablePath kvTablePath = TablePath.of(dbForKvTest, kvTableName);
        TableDescriptor kvTableDescriptor =
                TableDescriptor.builder().schema(DATA1_KV_SCHEMA).build();
        createTable(admin, kvTablePath, kvTableDescriptor);

        List<Object[]> inputData =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"}, new Object[] {3, "c"});
        try (Table table = flussConnection.getTable(kvTablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            inputData.forEach(
                    record -> {
                        upsertWriter.upsert(row(record));
                        upsertWriter.flush();
                    });
        }

        // test lookup.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    Object[] lookup = lookup(flussConnection, kvTablePath, new Object[] {1});
                    assertThat(lookup).isEqualTo(new Object[] {1, "a"});
                });
        assertThat(lookup(flussConnection, kvTablePath, new Object[] {4})).isNull();

        dropTable(kvTablePath);
        dropDatabase(dbForKvTest);
    }

    private Admin getAdmin() {
        if (admin == null) {
            throw new RuntimeException("admin is null, please init it first.");
        }
        return admin;
    }

    private Connection getFlussConnection() {
        if (flussConnection == null) {
            throw new RuntimeException("flussConnection is null, please init it first.");
        }
        return flussConnection;
    }

    private static @Nullable Object[] lookup(
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
}
