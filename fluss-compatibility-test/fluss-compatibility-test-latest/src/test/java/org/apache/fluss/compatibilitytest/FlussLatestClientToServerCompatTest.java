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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.AclBindingFilter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_06_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_07_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_LATEST_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.TestingUtils.toAclBinding;
import static org.apache.fluss.compatibilitytest.TestingUtils.toAlterConfig;
import static org.apache.fluss.compatibilitytest.TestingUtils.toOffsetSpec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Compatibility test for fluss latest client to connect with different fluss server to do admin,
 * read, write operations.
 */
@Testcontainers
public class FlussLatestClientToServerCompatTest extends ClientToServerCompatTest {

    private @Nullable Connection flussConnection;
    private @Nullable Admin admin;
    private final Map<TablePath, LogScanner> logScannerMap = new HashMap<>();

    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
        if (!logScannerMap.isEmpty()) {
            Collection<LogScanner> logScanners = logScannerMap.values();
            for (LogScanner scanner : logScanners) {
                scanner.close();
            }
        }
    }

    @Test
    void testToFluss06Server() throws Exception {
        clientToServerTestPipeline(FLUSS_LATEST_VERSION_MAGIC, FLUSS_06_VERSION_MAGIC);
    }

    @Test
    void testToFluss07Server() throws Exception {
        clientToServerTestPipeline(FLUSS_LATEST_VERSION_MAGIC, FLUSS_07_VERSION_MAGIC);
    }

    @Test
    void testToFlussLatestServer() throws Exception {
        // This test is for easy testing new future before release.
        clientToServerTestPipeline(FLUSS_LATEST_VERSION_MAGIC, FLUSS_LATEST_VERSION_MAGIC);
    }

    @Override
    boolean verifyServerReady(int serverVersion) throws Exception {
        boolean serverSupportAuth = serverVersion >= FLUSS_07_VERSION_MAGIC;
        return TestingUtils.serverReady(coordinatorServerPort, 0, false, serverSupportAuth)
                && TestingUtils.serverReady(tabletServerPort, 0, true, serverSupportAuth);
    }

    @Override
    void initFlussConnection(int serverVersion) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", "localhost:" + coordinatorServerPort);
        if (serverVersion >= FLUSS_07_VERSION_MAGIC) {
            TestingUtils.CLIENT_SALS_PROPERTIES.forEach(conf::setString);
        }
        flussConnection = ConnectionFactory.createConnection(conf);
    }

    @Override
    void initFlussAdmin() {
        if (flussConnection == null) {
            throw new RuntimeException("flussConnection is null, please init it first.");
        }
        admin = flussConnection.getAdmin();
    }

    @Override
    void createDatabase(String dbName) throws Exception {
        Admin admin = getAdmin();
        admin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();
    }

    @Override
    boolean tableExists(String dbName, String tableName) throws Exception {
        Admin admin = getAdmin();
        return admin.tableExists(TablePath.of(dbName, tableName)).get();
    }

    @Override
    void createTable(TestingTableDescriptor tableDescriptor) throws Exception {
        Admin admin = getAdmin();
        TestingUtils.createTable(admin, tableDescriptor);
    }

    @Override
    void produceLog(String dbName, String tableName, List<Object[]> records) throws Exception {
        Connection flussConnection = getFlussConnection();
        TestingUtils.produceLog(flussConnection, TablePath.of(dbName, tableName), records);
    }

    @Override
    void subscribe(
            String dbName,
            String tableName,
            @Nullable Integer partitionId,
            int bucketId,
            long offset) {
        TablePath tablePath = TablePath.of(dbName, tableName);
        Connection flussConnection = getFlussConnection();
        LogScanner logScanner;
        if (logScannerMap.containsKey(tablePath)) {
            logScanner = logScannerMap.get(tablePath);
        } else {
            Table table = flussConnection.getTable(TablePath.of(dbName, tableName));
            logScanner = table.newScan().createLogScanner();
            logScannerMap.put(tablePath, logScanner);
        }

        if (partitionId != null) {
            logScanner.subscribe(partitionId, bucketId, offset);
        } else {
            logScanner.subscribe(bucketId, offset);
        }
    }

    @Override
    List<Object[]> poll(String dbName, String tableName, Duration timeout) throws Exception {
        TablePath tablePath = TablePath.of(dbName, tableName);
        if (!logScannerMap.containsKey(tablePath)) {
            throw new RuntimeException("Please subscribe this table first.");
        }
        LogScanner scanner = logScannerMap.get(tablePath);
        return TestingUtils.poll(flussConnection, tablePath, scanner, timeout);
    }

    @Override
    void putKv(String dbName, String tableName, List<Object[]> records) throws Exception {
        Connection flussConnection = getFlussConnection();
        TestingUtils.putKv(flussConnection, TablePath.of(dbName, tableName), records);
    }

    @Override
    @Nullable
    Object[] lookup(String dbName, String tableName, Object[] key) throws Exception {
        Connection flussConnection = getFlussConnection();
        return TestingUtils.lookup(flussConnection, TablePath.of(dbName, tableName), key);
    }

    @Override
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

    @Override
    void verifyGetDatabaseInfo(String dbName) throws Exception {
        Admin admin = getAdmin();
        DatabaseInfo databaseInfo = admin.getDatabaseInfo(dbName).get();
        assertThat(databaseInfo.getDatabaseName()).isEqualTo(dbName);
        assertThat(databaseInfo.getDatabaseDescriptor().getCustomProperties()).isEmpty();
    }

    @Override
    boolean databaseExists(String dbName) throws Exception {
        Admin admin = getAdmin();
        return admin.databaseExists(dbName).get();
    }

    @Override
    List<String> listDatabases() throws Exception {
        Admin admin = getAdmin();
        return admin.listDatabases().get();
    }

    @Override
    void dropDatabase(String dbName) throws Exception {
        Admin admin = getAdmin();
        admin.dropDatabase(dbName, false, false).get();
    }

    @Override
    List<String> listTables(String dbName) throws Exception {
        Admin admin = getAdmin();
        return admin.listTables(dbName).get();
    }

    @Override
    void verifyGetTableSchema(TestingTableDescriptor tableDescriptor) throws Exception {
        Admin admin = getAdmin();
        String dbName = tableDescriptor.getDbName();
        String tableName = tableDescriptor.getTableName();
        SchemaInfo schemaInfo = admin.getTableSchema(TablePath.of(dbName, tableName)).get();
        assertThat(schemaInfo.getSchemaId()).isEqualTo(1);
        Schema schema = schemaInfo.getSchema();
        assertThat(schema.getColumnNames())
                .containsExactlyInAnyOrderElementsOf(tableDescriptor.getColumns());
        assertThat(schema.getPrimaryKeyColumnNames())
                .containsExactlyInAnyOrderElementsOf(tableDescriptor.getPrimaryKeys());

        // test get schema by id.
        schemaInfo = admin.getTableSchema(TablePath.of(dbName, tableName), 1).get();
        assertThat(schemaInfo.getSchemaId()).isEqualTo(1);
        schema = schemaInfo.getSchema();
        assertThat(schema.getColumnNames())
                .containsExactlyInAnyOrderElementsOf(tableDescriptor.getColumns());
        assertThat(schema.getPrimaryKeyColumnNames())
                .containsExactlyInAnyOrderElementsOf(tableDescriptor.getPrimaryKeys());
    }

    @Override
    void verifyGetTableInfo(TestingTableDescriptor tableDescriptor) throws Exception {
        Admin admin = getAdmin();
        String dbName = tableDescriptor.getDbName();
        String tableName = tableDescriptor.getTableName();
        TablePath tablePath = TablePath.of(dbName, tableName);
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getTablePath()).isEqualTo(tablePath);
        assertThat(tableInfo.getSchemaId()).isEqualTo(1);

        List<String> primaryKeys = tableInfo.getPrimaryKeys();
        assertThat(primaryKeys)
                .containsExactlyInAnyOrderElementsOf(tableDescriptor.getPrimaryKeys());

        List<String> partitionKeys = tableInfo.getPartitionKeys();
        assertThat(partitionKeys)
                .containsExactlyInAnyOrderElementsOf(tableDescriptor.getPartitionKeys());

        Map<String, String> properties = tableInfo.getProperties().toMap();
        tableDescriptor
                .getProperties()
                .forEach((k, v) -> assertThat(properties).containsEntry(k, v));
    }

    @Override
    void dropTable(String dbName, String tableName) throws Exception {
        Admin admin = getAdmin();
        admin.dropTable(TablePath.of(dbName, tableName), false).get();
    }

    @Override
    void verifyAlterTable(
            TestingTableDescriptor tableDescriptor,
            Map<String, String> alterOptions,
            int serverVersionMagic)
            throws Exception {
        Admin admin = getAdmin();
        verifyGetTableInfo(tableDescriptor);

        List<TableChange> tableChanges = new ArrayList<>();
        alterOptions.forEach(
                (k, v) -> {
                    if (v == null) {
                        tableChanges.add(TableChange.reset(k));
                    } else {
                        tableChanges.add(TableChange.set(k, v));
                    }
                });

        String dbName = tableDescriptor.getDbName();
        String tableName = tableDescriptor.getTableName();

        // for server version lower than 0.8, the alterTable is not supported.
        if (serverVersionMagic <= FLUSS_07_VERSION_MAGIC) {
            assertThatThrownBy(
                            () ->
                                    admin.alterTable(
                                                    TablePath.of(dbName, tableName),
                                                    tableChanges,
                                                    false)
                                            .get())
                    .rootCause()
                    .isInstanceOf(UnsupportedVersionException.class)
                    .hasMessage("The server does not support ALTER_TABLE(1044)");
        } else {
            admin.alterTable(TablePath.of(dbName, tableName), tableChanges, false).get();

            // verify changes.
            TableInfo tableInfo = admin.getTableInfo(TablePath.of(dbName, tableName)).get();
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
    }

    @Override
    List<String> listPartitions(String dbName, String tableName) throws Exception {
        Admin admin = getAdmin();
        List<PartitionInfo> partitionInfos =
                admin.listPartitionInfos(TablePath.of(dbName, tableName)).get();
        return partitionInfos.stream()
                .map(PartitionInfo::getPartitionName)
                .collect(Collectors.toList());
    }

    @Override
    List<String> listPartitions(String dbName, String tableName, Map<String, String> partitionSpec)
            throws Exception {
        Admin admin = getAdmin();
        List<PartitionInfo> partitionInfos =
                admin.listPartitionInfos(
                                TablePath.of(dbName, tableName), new PartitionSpec(partitionSpec))
                        .get();
        return partitionInfos.stream()
                .map(PartitionInfo::getPartitionName)
                .collect(Collectors.toList());
    }

    @Override
    void createPartition(String dbName, String tableName, Map<String, String> partitionSpec)
            throws Exception {
        Admin admin = getAdmin();
        admin.createPartition(
                        TablePath.of(dbName, tableName), new PartitionSpec(partitionSpec), false)
                .get();
    }

    @Override
    void dropPartition(String dbName, String tableName, Map<String, String> partitionSpec)
            throws Exception {
        Admin admin = getAdmin();
        admin.dropPartition(
                        TablePath.of(dbName, tableName), new PartitionSpec(partitionSpec), false)
                .get();
    }

    @Override
    List<TestingAclBinding> listAnyAcls() throws Exception {
        Admin admin = getAdmin();
        return admin.listAcls(AclBindingFilter.ANY).get().stream()
                .map(TestingUtils::fromAclBinding)
                .collect(Collectors.toList());
    }

    @Override
    void createAcl(TestingAclBinding aclBinding) throws Exception {
        Admin admin = getAdmin();
        admin.createAcls(Collections.singleton(toAclBinding(aclBinding))).all().get();
    }

    @Override
    void dropAllAcls() throws Exception {
        Admin admin = getAdmin();
        admin.dropAcls(Collections.singletonList(AclBindingFilter.ANY)).all().get();
    }

    @Override
    List<TestingConfigEntry> describeClusterConfigs() throws Exception {
        Admin admin = getAdmin();
        return admin.describeClusterConfigs().get().stream()
                .map(TestingUtils::fromConfigEntry)
                .collect(Collectors.toList());
    }

    @Override
    void alterClusterConfig(TestingAlterConfig alterConfig) throws Exception {
        Admin admin = getAdmin();
        admin.alterClusterConfigs(Collections.singleton(toAlterConfig(alterConfig))).get();
    }

    @Override
    Long listOffsets(
            String dbName,
            String tableName,
            @Nullable String partitionName,
            int bucket,
            TestingOffsetSpec offsetSpec)
            throws Exception {
        Admin admin = getAdmin();
        if (partitionName != null) {
            return admin.listOffsets(
                            TablePath.of(dbName, tableName),
                            partitionName,
                            Collections.singletonList(bucket),
                            toOffsetSpec(offsetSpec))
                    .all()
                    .get()
                    .get(bucket);
        } else {
            return admin.listOffsets(
                            TablePath.of(dbName, tableName),
                            Collections.singletonList(bucket),
                            toOffsetSpec(offsetSpec))
                    .all()
                    .get()
                    .get(bucket);
        }
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
}
