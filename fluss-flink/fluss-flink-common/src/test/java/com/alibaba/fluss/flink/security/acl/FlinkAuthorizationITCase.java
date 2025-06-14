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

package com.alibaba.fluss.flink.security.acl;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.exception.AuthorizationException;
import com.alibaba.fluss.flink.catalog.FlinkCatalogOptions;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static com.alibaba.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertQueryResultExactOrder;
import static com.alibaba.fluss.flink.utils.FlinkTestBase.waitUntilPartitions;
import static com.alibaba.fluss.security.acl.OperationType.CREATE;
import static com.alibaba.fluss.security.acl.OperationType.DESCRIBE;
import static com.alibaba.fluss.security.acl.OperationType.DROP;
import static com.alibaba.fluss.security.acl.OperationType.READ;
import static com.alibaba.fluss.security.acl.OperationType.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for flink authorization. */
abstract class FlinkAuthorizationITCase extends AbstractTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(initConfig())
                    .build();

    static final String CATALOG_NAME = "testcatalog";
    static final String ADMIN_CATALOG_NAME = "test_admin_catalog";
    static final String DEFAULT_DB = FlinkCatalogOptions.DEFAULT_DATABASE.defaultValue();
    static FlussPrincipal guest = new FlussPrincipal("guest", "User");
    static Configuration clientConf;

    private TableEnvironment tEnv;
    private TableEnvironment tBatchEnv;

    @BeforeAll
    static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig("CLIENT");
    }

    @BeforeEach
    void before() throws ExecutionException, InterruptedException {
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        // create table environment
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tBatchEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        // crate catalog using sql
        String createCatalogDDL =
                String.format(
                        "create catalog %s with ( \n"
                                + "'type' = 'fluss', \n"
                                + "'bootstrap.servers' = '%s', \n"
                                + "'client.security.protocol' = 'sasl', \n"
                                + "'client.security.sasl.mechanism' = 'PLAIN', \n"
                                + "'client.security.sasl.username' = 'guest', \n"
                                + "'client.security.sasl.password' = 'password2' \n"
                                + ")",
                        CATALOG_NAME, bootstrapServers);
        tEnv.executeSql(createCatalogDDL);
        tBatchEnv.executeSql(createCatalogDDL);
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tBatchEnv.executeSql("use catalog " + CATALOG_NAME);
        String createAminCatalogDDL =
                String.format(
                        "create catalog %s with ( \n"
                                + "'type' = 'fluss', \n"
                                + "'bootstrap.servers' = '%s', \n"
                                + "'client.security.protocol' = 'sasl', \n"
                                + "'client.security.sasl.mechanism' = 'PLAIN', \n"
                                + "'client.security.sasl.username' = 'root', \n"
                                + "'client.security.sasl.password' = 'password' \n"
                                + ")",
                        ADMIN_CATALOG_NAME, bootstrapServers);
        tEnv.executeSql(createAminCatalogDDL).await();
    }

    @AfterEach
    void after() throws ExecutionException, InterruptedException {
        dropAcl(Resource.any(), OperationType.ANY);
    }

    @Test
    void testShowDatabases() throws Exception {
        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show databases").collect()))
                .isEmpty();
        addAcl(Resource.database(DEFAULT_DB), DESCRIBE);
        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show databases").collect()))
                .containsExactly(Row.of(DEFAULT_DB));
        dropAcl(Resource.database(DEFAULT_DB), DESCRIBE);
        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show databases").collect()))
                .isEmpty();
    }

    @Test
    void testCreateAndDropDatabase() throws Exception {
        String databaseName = String.format("test_show_db_%s", RandomUtils.nextInt());
        String createDatabaseDDL = "CREATE DATABASE " + databaseName;
        // test create database
        assertThatThrownBy(() -> tEnv.executeSql(createDatabaseDDL).await())
                .hasRootCauseInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "Principal %s have no authorization to operate CREATE on resource %s",
                                guest, Resource.cluster()));
        addAcl(Resource.cluster(), CREATE);
        tEnv.executeSql(createDatabaseDDL).await();
        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show databases").collect()))
                .containsExactlyInAnyOrder(Row.of(DEFAULT_DB), Row.of(databaseName));

        // test drop database
        String dropDatabaseDDL = "drop database " + databaseName;
        assertThatThrownBy(() -> tEnv.executeSql(dropDatabaseDDL).await())
                .hasRootCauseInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "Principal %s have no authorization to operate DROP on resource %s",
                                guest, Resource.database(databaseName)));
        addAcl(Resource.database(databaseName), DROP);
        tEnv.executeSql(dropDatabaseDDL).await();
        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show databases").collect()))
                .containsExactlyInAnyOrder(Row.of(DEFAULT_DB));
    }

    @Test
    void testShowTables() throws Exception {
        TablePath testTable1 =
                new TablePath(
                        DEFAULT_DB, String.format("test_show_db_1_%s", RandomUtils.nextInt()));
        TablePath testTable2 =
                new TablePath(
                        DEFAULT_DB, String.format("test_show_db_2_%s", RandomUtils.nextInt()));
        String createTableDDLFormat = "CREATE TABLE %s ( a int not null primary key not enforced);";
        addAcl(Resource.database(DEFAULT_DB), CREATE);
        tEnv.executeSql(String.format(createTableDDLFormat, testTable1.getTableName())).await();
        tEnv.executeSql(String.format(createTableDDLFormat, testTable2.getTableName())).await();
        dropAcl(Resource.database(DEFAULT_DB), CREATE);

        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show tables").collect()))
                .isEmpty();
        addAcl(Resource.table(testTable1), DESCRIBE);
        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show tables").collect()))
                .containsExactlyInAnyOrder(Row.of(testTable1.getTableName()));
        addAcl(Resource.database(DEFAULT_DB), CREATE);
        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show tables").collect()))
                .containsExactlyInAnyOrder(
                        Row.of(testTable1.getTableName()), Row.of(testTable2.getTableName()));
    }

    @Test
    void tesCreateAndDropTable() throws Exception {
        String tableName = String.format("test_create_db_%s", RandomUtils.nextInt());
        TablePath testTable = new TablePath(DEFAULT_DB, tableName);
        String createTableDDL =
                String.format(
                        "CREATE TABLE %s ( a int not null primary key not enforced);",
                        testTable.getTableName());
        // test create database
        assertThatThrownBy(() -> tEnv.executeSql(createTableDDL).await())
                .hasRootCauseInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "Principal %s have no authorization to operate CREATE on resource %s",
                                guest, Resource.database(testTable.getDatabaseName())));
        addAcl(Resource.database(DEFAULT_DB), CREATE);
        tEnv.executeSql(String.format(createTableDDL)).await();
        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show tables").collect()))
                .containsExactlyInAnyOrder(Row.of(testTable.getTableName()));

        // test drop database
        String dropTableDDL = "drop table " + testTable.getTableName();
        assertThatThrownBy(() -> tEnv.executeSql(dropTableDDL).await())
                .hasRootCauseInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "Principal %s have no authorization to operate DROP on resource %s",
                                guest, Resource.table(testTable)));
        addAcl(Resource.table(testTable), DROP);
        tEnv.executeSql(dropTableDDL).await();
        assertThat(CollectionUtil.iteratorToList(tEnv.executeSql("show tables").collect()))
                .isEmpty();
    }

    @Test
    void testAlterPartitionTable() throws Exception {
        addAcl(Resource.cluster(), CREATE);
        String tableName = String.format("test_partitioned_log_table_%s", RandomUtils.nextInt());
        TablePath testTable = new TablePath(DEFAULT_DB, tableName);
        tEnv.executeSql(
                        String.format(
                                "CREATE TABLE %s (dt varchar) partitioned by (dt)  ;", tableName))
                .await();
        String addPartitionDDL =
                String.format("alter table %s add partition (dt='2022-01-01');", tableName);

        // test add partition
        assertThatThrownBy(() -> tEnv.executeSql(addPartitionDDL).await())
                .hasRootCauseInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "Principal %s have no authorization to operate WRITE on resource %s",
                                guest, Resource.table(testTable)));
        addAcl(Resource.database(DEFAULT_DB), WRITE);
        tEnv.executeSql(String.format(addPartitionDDL)).await();

        // test drop partition
        dropAcl(Resource.database(DEFAULT_DB), WRITE);
        String dropPartitionDDL =
                String.format("alter table %s drop partition (dt='2022-01-01');", tableName);
        assertThatThrownBy(() -> tEnv.executeSql(dropPartitionDDL).await())
                .hasRootCauseInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "Principal %s have no authorization to operate WRITE on resource %s",
                                guest, Resource.table(testTable)));
        addAcl(Resource.table(testTable), WRITE);
        tEnv.executeSql(dropPartitionDDL).await();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testProduceAndConsumeLogTable(boolean isPartitionTable) throws Exception {
        addAcl(Resource.cluster(), CREATE);
        String tableName = String.format("test_log_table_%s", RandomUtils.nextInt());
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String ddl =
                String.format(
                        "create table %s ("
                                + "  id int not null,"
                                + "  address varchar,"
                                + "  name varchar,"
                                + "  dt varchar)"
                                + (isPartitionTable ? "  partitioned by (dt)" : ""),
                        tableName);
        tEnv.executeSql(String.format(ddl)).await();
        if (isPartitionTable) {
            // prepare partition in advance.
            addAcl(Resource.database(tablePath.getDatabaseName()), WRITE);
            tEnv.executeSql(
                    String.format(
                            "alter table %s add partition (dt='2022-01-01');",
                            tablePath.getTableName()));
            waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 1);
            dropAcl(Resource.database(tablePath.getDatabaseName()), WRITE);
        }

        // test produce
        String insertDML =
                String.format(
                        "insert into %s values (1, 'beijing', 'zhangsan', '2022-01-01');",
                        tablePath.getTableName());
        assertThatThrownBy(() -> tEnv.executeSql(insertDML).await())
                .hasRootCauseInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "No WRITE permission among all the tables: %s",
                                Collections.singletonList(tablePath)));
        addAcl(Resource.table(tablePath), WRITE);
        tEnv.executeSql(insertDML).await();

        // test consume
        String selectDML = String.format("select * from %s;", tablePath.getTableName());
        assertThatThrownBy(() -> tEnv.executeSql(selectDML).await())
                .hasRootCauseExactlyInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "No permission to READ table %s in database %s",
                                tablePath.getTableName(), tablePath.getDatabaseName()));
        addAcl(Resource.table(tablePath), READ);
        assertQueryResultExactOrder(
                tEnv, selectDML, Collections.singletonList("+I[1, beijing, zhangsan, 2022-01-01]"));
    }

    @Test
    void testPutAndLookupKvTable() throws Exception {
        addAcl(Resource.cluster(), CREATE);
        String tableName = String.format("test_pk_table_%s", RandomUtils.nextInt());
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String ddl =
                String.format(
                        "create table %s ("
                                + "  id int not null,"
                                + "  address varchar,"
                                + "  name varchar,"
                                + "  primary key (id) NOT ENFORCED)",
                        tableName);
        tBatchEnv.executeSql(String.format(ddl)).await();

        // test put kv
        String insertDML =
                String.format(
                        "insert into %s values (1, 'beijing', 'zhangsan'),(2, 'shanghai', 'lisi');",
                        tablePath.getTableName());
        assertThatThrownBy(() -> tBatchEnv.executeSql(insertDML).await())
                .hasRootCauseInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "No WRITE permission among all the tables: %s",
                                Collections.singletonList(tablePath)));
        addAcl(Resource.table(tablePath), WRITE);
        tBatchEnv.executeSql(insertDML).await();

        // test lookup
        String lookupSql =
                String.format("select * from %s where id = 2;", tablePath.getTableName());
        assertThatThrownBy(() -> tBatchEnv.executeSql(lookupSql).await())
                .hasRootCauseExactlyInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "No permission to READ table %s in database %s",
                                tablePath.getTableName(), tablePath.getDatabaseName()));
        addAcl(Resource.table(tablePath), READ);
        assertQueryResultExactOrder(
                tBatchEnv, lookupSql, Collections.singletonList("+I[2, shanghai, lisi]"));

        // test limit scan
        dropAcl(Resource.table(tablePath), READ);
        String limitScanSql = String.format("select * from %s limit 2;", tablePath.getTableName());
        assertThatThrownBy(() -> tBatchEnv.executeSql(limitScanSql).await())
                .hasRootCauseExactlyInstanceOf(AuthorizationException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                "No permission to READ table %s in database %s",
                                tablePath.getTableName(), tablePath.getDatabaseName()));
        addAcl(Resource.database(tablePath.getDatabaseName()), READ);
        assertQueryResultExactOrder(
                tBatchEnv,
                limitScanSql,
                Arrays.asList("+I[1, beijing, zhangsan]", "+I[2, shanghai, lisi]"));
    }

    void addAcl(Resource resource, OperationType operationType)
            throws ExecutionException, InterruptedException {
        tEnv.executeSql(
                        String.format(
                                "CALL %s.sys.add_acl('%s', 'ALLOW', '%s' , '%s', '*')",
                                ADMIN_CATALOG_NAME,
                                getProcedureResourceString(resource),
                                String.format("%s:%s", guest.getType(), guest.getName()),
                                operationType.name()))
                .await();
    }

    void dropAcl(Resource resource, OperationType operationType)
            throws ExecutionException, InterruptedException {
        tEnv.executeSql(
                        String.format(
                                "CALL %s.sys.drop_acl('%s', 'ANY', '%s', '%s', 'ANY')",
                                ADMIN_CATALOG_NAME,
                                getProcedureResourceString(resource),
                                String.format("%s:%s", guest.getType(), guest.getName()),
                                operationType.name()))
                .await();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set a shorter max lag time to make tests in FlussFailServerTableITCase faster
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));

        // set security information.
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
        conf.setString("security.sasl.enabled.mechanisms", "plain");
        conf.setString(
                "security.sasl.plain.jaas.config",
                "com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_root=\"password\" "
                        + "    user_guest=\"password2\";");
        conf.set(ConfigOptions.SUPER_USERS, "User:root");
        conf.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        return conf;
    }

    private String getProcedureResourceString(Resource resource) {
        switch (resource.getType()) {
            case ANY:
                return "ANY";
            case CLUSTER:
                return "cluster";
            case DATABASE:
            case TABLE:
                return String.format("cluster.%s", resource.getName());
            default:
                return "";
        }
    }
}
