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

import org.apache.fluss.compatibilitytest.TestingAclBinding.AccessControlEntry;
import org.apache.fluss.compatibilitytest.TestingAclBinding.OperationType;
import org.apache.fluss.compatibilitytest.TestingAclBinding.PermissionType;
import org.apache.fluss.compatibilitytest.TestingAclBinding.Resource;
import org.apache.fluss.compatibilitytest.TestingAlterConfig.AlterConfigOpType;
import org.apache.fluss.compatibilitytest.TestingConfigEntry.ConfigSource;

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

import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_06_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.CompatEnvironment.FLUSS_07_VERSION_MAGIC;
import static org.apache.fluss.compatibilitytest.TestingAclBinding.ResourceType.CLUSTER;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.INT_TYPE;
import static org.apache.fluss.compatibilitytest.TestingTableDescriptor.STRING_TYPE;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Basic abstract class for fluss client connect to server compatibility test. */
@Testcontainers
public abstract class ClientToServerCompatTest extends CompatTest {

    protected void clientToServerTestPipeline(int clientVersionMagic, int serverVersionMagic)
            throws Exception {
        // start the server.
        initAndStartFlussServer(clientVersionMagic, serverVersionMagic);

        // init fluss connection and admin.
        initFlussConnection(serverVersionMagic);
        initFlussAdmin();

        // test admin compact.
        verifyAdminOperations(clientVersionMagic, serverVersionMagic);

        // verify produceLog and fetchLog.
        verifyProduceAndFetchLog();

        // verify putKv and Lookup.
        verifyPutKvAndLookup();

        // TODO test lake part.

        stopServer();
    }

    abstract void verifyGetServerNodes() throws Exception;

    abstract void verifyGetDatabaseInfo(String dbName) throws Exception;

    abstract boolean databaseExists(String dbName) throws Exception;

    abstract List<String> listDatabases() throws Exception;

    abstract void dropDatabase(String dbName) throws Exception;

    abstract List<String> listTables(String dbName) throws Exception;

    abstract void verifyGetTableSchema(TestingTableDescriptor tableDescriptor) throws Exception;

    abstract void verifyGetTableInfo(TestingTableDescriptor tableDescriptor) throws Exception;

    abstract void dropTable(String dbName, String tableName) throws Exception;

    /**
     * For reset option, the value is null. Since versions Fluss-0.6/Fluss-0.7 do not support
     * alterTable, the validation logic for ALTER TABLE is defined by the implementer themselves.
     */
    abstract void verifyAlterTable(
            TestingTableDescriptor tableDescriptor,
            Map<String, String> alterOptions,
            int serverVersionMagic)
            throws Exception;

    abstract List<String> listPartitions(String dbName, String tableName) throws Exception;

    abstract List<String> listPartitions(
            String dbName, String tableName, Map<String, String> partitionSpec) throws Exception;

    abstract void createPartition(
            String dbName, String tableName, Map<String, String> partitionSpec) throws Exception;

    abstract void dropPartition(String dbName, String tableName, Map<String, String> partitionSpec)
            throws Exception;

    abstract List<TestingAclBinding> listAnyAcls() throws Exception;

    abstract void createAcl(TestingAclBinding aclBinding) throws Exception;

    abstract void dropAllAcls() throws Exception;

    abstract List<TestingConfigEntry> describeClusterConfigs() throws Exception;

    abstract void alterClusterConfig(TestingAlterConfig alterConfig) throws Exception;

    abstract Long listOffsets(
            String dbName,
            String tableName,
            @Nullable String partitionName,
            int bucket,
            TestingOffsetSpec offsetSpec)
            throws Exception;

    private void verifyAdminOperations(int clientVersionMagic, int serverVersionMagic)
            throws Exception {
        verifyGetServerNodes();
        verifyDatabaseOperations();
        verifyTableOperations(clientVersionMagic, serverVersionMagic);
        verifySingleFiledPartitionOperations(clientVersionMagic, serverVersionMagic);
        // TODO verify multi-field partition operations.

        // listOffsets has been verified in verifyProduceAndFetchLog().

        // LatestKvSnapshots has been verified in verifyPutKvAndLookup().

        // TODO LatestLakeSnapshots will be verified in lake part.

        verifyAcls(clientVersionMagic, serverVersionMagic);
        verifyClusterConfigOperations(clientVersionMagic, serverVersionMagic);
    }

    private void verifyDatabaseOperations() throws Exception {
        // default database "fluss" will be created automatically.
        assertThat(listDatabases()).containsExactlyInAnyOrder("fluss");

        createDatabase("db-1");
        createDatabase("db-2");
        verifyGetDatabaseInfo("db-1");
        verifyGetDatabaseInfo("db-2");
        assertThat(databaseExists("db-1")).isTrue();
        assertThat(databaseExists("db-2")).isTrue();
        assertThat(databaseExists("db-3")).isFalse();
        assertThat(listDatabases()).containsExactlyInAnyOrder("db-1", "db-2", "fluss");
        dropDatabase("db-1");
        assertThat(databaseExists("db-1")).isFalse();
        assertThat(listDatabases()).containsExactlyInAnyOrder("db-2", "fluss");
        dropDatabase("db-2");
    }

    private void verifyTableOperations(int clientVersionMagic, int serverVersionMagic)
            throws Exception {
        String dbName = "db-for-table-1";
        createDatabase(dbName);
        assertThat(databaseExists(dbName)).isTrue();
        assertThat(listTables(dbName)).isEmpty();

        // verify create log table.
        String logTableName = "log-table-1";
        assertThat(tableExists(dbName, logTableName)).isFalse();
        TestingTableDescriptor logTableDescriptor =
                new TestingTableDescriptor(
                        dbName,
                        logTableName,
                        List.of("a", "b"),
                        List.of(INT_TYPE, STRING_TYPE),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap("table.log.ttl", "1d"));
        createTable(logTableDescriptor);
        verifyGetTableSchema(logTableDescriptor);
        verifyGetTableInfo(logTableDescriptor);
        assertThat(tableExists(dbName, logTableName)).isTrue();
        assertThat(listTables(dbName)).containsExactlyInAnyOrder(logTableName);

        // verify create primary key table.
        String kvTableName = "kv-table-1";
        assertThat(tableExists(dbName, kvTableName)).isFalse();
        TestingTableDescriptor kvTableDescriptor =
                new TestingTableDescriptor(
                        dbName,
                        kvTableName,
                        List.of("a", "b"),
                        List.of(INT_TYPE, STRING_TYPE),
                        Collections.singletonList("a"),
                        Collections.emptyList(),
                        Collections.singletonMap("table.datalake.enabled", "false"));
        createTable(kvTableDescriptor);
        verifyGetTableSchema(kvTableDescriptor);
        verifyGetTableInfo(kvTableDescriptor);
        assertThat(tableExists(dbName, kvTableName)).isTrue();
        assertThat(listTables(dbName)).containsExactlyInAnyOrder(logTableName, kvTableName);

        // test alter table, verify reset and set option.
        if (clientVersionMagic > FLUSS_07_VERSION_MAGIC) {
            verifyAlterTable(
                    kvTableDescriptor,
                    Collections.singletonMap("table.datalake.enabled", "true"),
                    serverVersionMagic);
        }

        dropTable(dbName, kvTableName);
        assertThat(tableExists(dbName, kvTableName)).isFalse();
        assertThat(listTables(dbName)).containsExactlyInAnyOrder(logTableName);

        dropTable(dbName, logTableName);
        assertThat(listTables(dbName)).isEmpty();
        dropDatabase(dbName);
    }

    private void verifySingleFiledPartitionOperations(int clientVersion, int serverVersionMagic)
            throws Exception {
        String dbName = "db-for-partition-1";
        createDatabase(dbName);

        // create a partitioned log table with auto partition closed.
        String logTableName = "partitioned-log-table-1";
        Map<String, String> properties = new HashMap<>();
        properties.put("table.auto-partition.enabled", "true");
        properties.put("table.auto-partition.num-precreate", "2");
        properties.put("table.auto-partition.time-unit", "YEAR");
        TestingTableDescriptor logTableDescriptor =
                new TestingTableDescriptor(
                        dbName,
                        logTableName,
                        List.of("a", "b", "c"),
                        List.of(INT_TYPE, STRING_TYPE, STRING_TYPE),
                        Collections.emptyList(),
                        Collections.singletonList("c"),
                        properties);
        createTable(logTableDescriptor);
        assertThat(tableExists(dbName, logTableName)).isTrue();
        int currentYear = LocalDate.now().getYear();
        retry(
                Duration.ofMinutes(1),
                () -> {
                    List<String> partitions = listPartitions(dbName, logTableName);
                    assertThat(partitions)
                            .containsExactlyInAnyOrder(
                                    String.valueOf(currentYear), String.valueOf(currentYear + 1));
                });

        // verify listPartitions by spec.
        if (clientVersion > FLUSS_06_VERSION_MAGIC) {
            List<String> partitions =
                    listPartitions(
                            dbName,
                            logTableName,
                            Collections.singletonMap("c", String.valueOf(currentYear)));
            if (serverVersionMagic < FLUSS_07_VERSION_MAGIC) {
                // 0.6 server does not support listPartitions by spec. So the result is the same as
                // listPartitions.
                assertThat(partitions)
                        .containsExactlyInAnyOrder(
                                String.valueOf(currentYear), String.valueOf(currentYear + 1));
            } else {
                assertThat(partitions).containsExactlyInAnyOrder(String.valueOf(currentYear));
            }
        }

        // verify create partition.
        if (clientVersion > FLUSS_06_VERSION_MAGIC) {
            createPartition(dbName, logTableName, Collections.singletonMap("c", "1997"));
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        List<String> partitions = listPartitions(dbName, logTableName);
                        assertThat(partitions)
                                .containsExactlyInAnyOrder(
                                        String.valueOf(currentYear),
                                        String.valueOf(currentYear + 1),
                                        "1997");
                    });
        }

        // verify drop partition.
        if (clientVersion > FLUSS_06_VERSION_MAGIC) {
            dropPartition(dbName, logTableName, Collections.singletonMap("c", "1997"));
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        List<String> partitions = listPartitions(dbName, logTableName);
                        assertThat(partitions)
                                .containsExactlyInAnyOrder(
                                        String.valueOf(currentYear),
                                        String.valueOf(currentYear + 1));
                    });
        }

        dropTable(dbName, logTableName);
        dropDatabase(dbName);
    }

    private void verifyAcls(int clientVersionMagic, int serverVersionMagic) throws Exception {
        if (clientVersionMagic < FLUSS_07_VERSION_MAGIC) {
            return;
        }

        // add acl binding.
        TestingAclBinding aclBinding =
                new TestingAclBinding(
                        new Resource(CLUSTER, "fluss-cluster"),
                        new AccessControlEntry(
                                "guest",
                                "User",
                                PermissionType.ALLOW,
                                "127.0.0.1",
                                OperationType.DESCRIBE));

        if (serverVersionMagic < FLUSS_07_VERSION_MAGIC) {
            assertThatThrownBy(() -> createAcl(aclBinding))
                    .rootCause()
                    .hasMessage("The server does not support CREATE_ACLS(1039)");
            assertThatThrownBy(this::listAnyAcls)
                    .rootCause()
                    .hasMessage("The server does not support LIST_ACLS(1040)");
            assertThatThrownBy(this::dropAllAcls)
                    .rootCause()
                    .hasMessage("The server does not support DROP_ACLS(1041)");
            return;
        }

        List<TestingAclBinding> testingAclBindings = listAnyAcls();
        assertThat(testingAclBindings).isEmpty();

        createAcl(aclBinding);

        testingAclBindings = listAnyAcls();
        assertThat(testingAclBindings).containsExactly(aclBinding);

        dropAllAcls();
        testingAclBindings = listAnyAcls();
        assertThat(testingAclBindings).isEmpty();
    }

    private void verifyClusterConfigOperations(int clientVersionMagic, int serverVersionMagic)
            throws Exception {
        if (clientVersionMagic <= FLUSS_07_VERSION_MAGIC) {
            return;
        }

        TestingAlterConfig alterConfig =
                new TestingAlterConfig("datalake.format", null, AlterConfigOpType.SET);
        if (serverVersionMagic <= FLUSS_07_VERSION_MAGIC) {
            assertThatThrownBy(this::describeClusterConfigs)
                    .rootCause()
                    .hasMessage("The server does not support DESCRIBE_CLUSTER_CONFIGS(1045)");
            assertThatThrownBy(() -> alterClusterConfig(alterConfig))
                    .rootCause()
                    .hasMessage("The server does not support ALTER_CLUSTER_CONFIGS(1046)");
            return;
        }

        List<TestingConfigEntry> configEntries = describeClusterConfigs();
        Optional<TestingConfigEntry> first =
                configEntries.stream().filter(e -> e.key.equals("datalake.format")).findFirst();
        assertThat(first).isPresent();
        assertThat(first.get().value).isEqualTo("paimon");
        assertThat(first.get().source).isEqualTo(ConfigSource.INITIAL_SERVER_CONFIG);

        alterClusterConfig(alterConfig);

        configEntries = describeClusterConfigs();
        first = configEntries.stream().filter(e -> e.key.equals("datalake.format")).findFirst();
        assertThat(first).isPresent();
        assertThat(first.get().value).isNull();
        assertThat(first.get().source).isEqualTo(ConfigSource.DYNAMIC_SERVER_CONFIG);
    }

    private void verifyProduceAndFetchLog() throws Exception {
        String dbForLogTest = "db-for-log-1";
        createDatabase(dbForLogTest);

        String logTableName = "produce-log-table-1";
        TestingTableDescriptor logTableDescriptor =
                new TestingTableDescriptor(
                        dbForLogTest,
                        logTableName,
                        List.of("a", "b"),
                        List.of(INT_TYPE, STRING_TYPE),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap());
        createTable(logTableDescriptor);

        long timeBeforeInput = System.currentTimeMillis();

        List<Object[]> inputData =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"}, new Object[] {3, "c"});
        produceLog(dbForLogTest, logTableName, inputData);

        subscribe(dbForLogTest, logTableName, null, 0, 0);
        List<Object[]> results = new ArrayList<>();
        while (results.size() < inputData.size()) {
            List<Object[]> poll = poll(dbForLogTest, logTableName, Duration.ofMillis(300));
            results.addAll(poll);
        }
        assertThat(results).containsExactlyInAnyOrderElementsOf(inputData);

        // test list offset here.
        assertThat(listOffsets(dbForLogTest, logTableName, null, 0, new TestingOffsetSpec(0, null)))
                .isEqualTo(0L);
        assertThat(listOffsets(dbForLogTest, logTableName, null, 0, new TestingOffsetSpec(1, null)))
                .isEqualTo(3L);
        assertThat(
                        listOffsets(
                                dbForLogTest,
                                logTableName,
                                null,
                                0,
                                new TestingOffsetSpec(2, timeBeforeInput)))
                .isEqualTo(0L);

        dropTable(dbForLogTest, logTableName);
        dropDatabase(dbForLogTest);
    }

    private void verifyPutKvAndLookup() throws Exception {
        String dbForKvTest = "db-for-kv-1";
        createDatabase(dbForKvTest);

        String kvTableName = "put-kv-and-lookup-table-1";
        TestingTableDescriptor logTableDescriptor =
                new TestingTableDescriptor(
                        dbForKvTest,
                        kvTableName,
                        List.of("a", "b"),
                        List.of(INT_TYPE, STRING_TYPE),
                        Collections.singletonList("a"),
                        Collections.emptyList(),
                        Collections.emptyMap());
        createTable(logTableDescriptor);

        List<Object[]> inputData =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"}, new Object[] {3, "c"});
        putKv(dbForKvTest, kvTableName, inputData);

        // test lookup.
        Object[] lookup = lookup(dbForKvTest, kvTableName, new Object[] {1});
        assertThat(lookup).isEqualTo(new Object[] {1, "a"});
        assertThat(lookup(dbForKvTest, kvTableName, new Object[] {4})).isNull();

        dropTable(dbForKvTest, kvTableName);
        dropDatabase(dbForKvTest);
    }
}
