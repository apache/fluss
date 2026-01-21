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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.NoRebalanceInProgressException;
import org.apache.fluss.exception.SecurityDisabledException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ServerTags;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.fluss.cluster.rebalance.ServerTag.PERMANENT_OFFLINE;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for Flink Procedure. */
public abstract class FlinkProcedureITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(4)
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(initConfig())
                    .build();

    static final String CATALOG_NAME = "testcatalog";
    static final String DEFAULT_DB = "defaultdb";
    static Configuration clientConf;
    static String bootstrapServers;
    static Connection conn;
    static Admin admin;

    TableEnvironment tEnv;

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    void before() throws ExecutionException, InterruptedException {
        String bootstrapServers =
                String.join(
                        ",",
                        FLUSS_CLUSTER_EXTENSION
                                .getClientConfig("CLIENT")
                                .get(ConfigOptions.BOOTSTRAP_SERVERS));
        // create table environment
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        String catalogDDL =
                String.format(
                        "create catalog %s with ( \n"
                                + "'type' = 'fluss', \n"
                                + "'bootstrap.servers' = '%s', \n"
                                + "'client.security.protocol' = 'sasl', \n"
                                + "'client.security.sasl.mechanism' = 'PLAIN', \n"
                                + "'client.security.sasl.username' = 'root', \n"
                                + "'client.security.sasl.password' = 'password' \n"
                                + ")",
                        CATALOG_NAME, bootstrapServers);
        tEnv.executeSql(catalogDDL).await();
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @Test
    void testShowProcedures() throws Exception {
        try (CloseableIterator<Row> showProceduresIterator =
                tEnv.executeSql("show procedures").collect()) {
            List<String> expectedShowProceduresResult =
                    Arrays.asList(
                            "+I[sys.add_acl]",
                            "+I[sys.drop_acl]",
                            "+I[sys.get_cluster_configs]",
                            "+I[sys.list_acl]",
                            "+I[sys.set_cluster_configs]",
                            "+I[sys.reset_cluster_configs]",
                            "+I[sys.add_server_tag]",
                            "+I[sys.remove_server_tag]",
                            "+I[sys.rebalance]",
                            "+I[sys.cancel_rebalance]",
                            "+I[sys.list_rebalance]");
            // make sure no more results is unread.
            assertResultsIgnoreOrder(showProceduresIterator, expectedShowProceduresResult, true);
        }

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "show procedures from  %s.non_db",
                                                        CATALOG_NAME))
                                        .await())
                .hasMessage(
                        String.format(
                                "Fail to show procedures because the Database `non_db` to show from/in does not exist in Catalog `%s`.",
                                CATALOG_NAME));
    }

    @Test
    void testCallNonExistProcedure() {
        assertThatThrownBy(() -> tEnv.executeSql("CALL `system`.generate_n(4)").collect())
                .rootCause()
                .hasMessageContaining("No match found for function signature generate_n");
    }

    @Test
    void testIndexArgument() throws Exception {
        tEnv.executeSql(
                        String.format(
                                "Call %s.sys.add_acl( resource => 'CLUSTER', permission => 'ALLOW', principal =>  'User:Alice', operation  =>  'READ', host => '*' )",
                                CATALOG_NAME))
                .await();
        // index argument supports part of index argument(if it is optional)
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(
                                String.format(
                                        "Call %s.sys.list_acl( resource => 'ANY')", CATALOG_NAME))
                        .collect()) {
            assertCallResult(
                    listProceduresIterator,
                    new String[] {
                        "+I[resource=\"cluster\";permission=\"ALLOW\";principal=\"User:Alice\";operation=\"READ\";host=\"*\"]"
                    });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAclProcedureIgnoreCase(boolean upperCase) throws Exception {
        // Only type operation and permission is case-ignored , while principal and resource name
        // must be case-insensitive
        String addAcl =
                String.format(
                        upperCase
                                ? "Call %s.sys.add_acl('CLUSTER', 'ALLOW', 'User:Alice', 'READ', '127.0.0.1' )"
                                : "Call %s.sys.add_acl('cluster', 'allow', 'User:Alice', 'read', '127.0.0.1' )",
                        CATALOG_NAME);
        String listAcl =
                String.format(
                        upperCase
                                ? "Call %s.sys.list_acl('ANY', 'ANY', 'ANY', 'ANY', 'ANY' )"
                                : "Call %s.sys.list_acl('any', 'any', 'any', 'any', 'any' )",
                        CATALOG_NAME);
        String dropAcl =
                String.format(
                        upperCase
                                ? "Call %s.sys.drop_acl('CLUSTER', 'ALLOW', 'User:Alice', 'READ', '127.0.0.1' )"
                                : "Call %s.sys.drop_acl('cluster', 'allow', 'User:Alice', 'read', '127.0.0.1' )",
                        CATALOG_NAME);
        tEnv.executeSql(addAcl).await();
        try (CloseableIterator<Row> listProceduresIterator = tEnv.executeSql(listAcl).collect()) {
            assertCallResult(
                    listProceduresIterator,
                    new String[] {
                        "+I[resource=\"cluster\";permission=\"ALLOW\";principal=\"User:Alice\";operation=\"READ\";host=\"127.0.0.1\"]"
                    });
        }
        tEnv.executeSql(dropAcl).await();
        try (CloseableIterator<Row> listProceduresIterator = tEnv.executeSql(listAcl).collect()) {
            assertCallResult(listProceduresIterator, new String[0]);
        }
    }

    @Test
    void testNotAllowFilterWhenAddAcl() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.add_acl('CLUSTER', 'ALLOW', 'User:Alice', 'READ', 'ANY' )",
                                                        CATALOG_NAME))
                                        .wait())
                .hasMessageContaining(
                        "Wildcard 'ANY' can only be used for filtering, not for adding ACL entries.");
    }

    @Test
    void testUnsupportedOperation() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.list_acl('CLUSTER', 'ALLOW', 'User:Alice', 'UNKNOWN', 'ANY' )",
                                                        CATALOG_NAME))
                                        .wait())
                .hasMessageContaining(
                        "No enum constant org.apache.fluss.security.acl.OperationType.UNKNOWN");
    }

    @Test
    void testUnsupportedPermissionType() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.add_acl('CLUSTER', 'UNKNOWN', 'User:Alice', 'ANY', 'ANY' )",
                                                        CATALOG_NAME))
                                        .wait())
                .hasMessageContaining(
                        "No enum constant org.apache.fluss.security.acl.PermissionType.UNKNOWN");
    }

    @Test
    void testDisableAuthorization() throws Exception {
        String catalogName = "disable_acl_catalog";
        FlussClusterExtension flussClusterExtension = FlussClusterExtension.builder().build();
        try {
            flussClusterExtension.start();
            // prepare table
            Configuration clientConfig = flussClusterExtension.getClientConfig();
            clientConfig.set(ConfigOptions.CLIENT_WRITER_RETRIES, 0);
            clientConfig.set(ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE, false);
            String catalogDDL =
                    String.format(
                            "create catalog %s with ( \n"
                                    + "'type' = 'fluss', \n"
                                    + "'bootstrap.servers' = '%s' \n"
                                    + ")",
                            catalogName,
                            String.join(
                                    ",",
                                    flussClusterExtension
                                            .getClientConfig()
                                            .get(ConfigOptions.BOOTSTRAP_SERVERS)));
            tEnv.executeSql(catalogDDL).await();
            assertThatThrownBy(
                            () ->
                                    tEnv.executeSql(
                                                    String.format(
                                                            "Call %s.sys.add_acl('CLUSTER', 'ALLOW', 'User:Alice', 'READ', '*' )",
                                                            catalogName))
                                            .await())
                    .rootCause()
                    .isExactlyInstanceOf(SecurityDisabledException.class)
                    .hasMessageContaining("No Authorizer is configured");
        } finally {
            flussClusterExtension.close();
        }
    }

    @Test
    void testGetClusterConfigs() throws Exception {
        // Get specific config
        try (CloseableIterator<Row> resultIterator =
                tEnv.executeSql(
                                String.format(
                                        "Call %s.sys.get_cluster_configs('%s')",
                                        CATALOG_NAME,
                                        ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()))
                        .collect()) {
            List<Row> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(results).hasSize(1);
            Row row = results.get(0);
            assertThat(row.getField(0))
                    .isEqualTo(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key());
            assertThat(row.getField(1)).isEqualTo("100 mb");
            assertThat(row.getField(2)).isNotNull(); // config_source
        }

        // Get multiple config
        try (CloseableIterator<Row> resultIterator =
                tEnv.executeSql(
                                String.format(
                                        "Call %s.sys.get_cluster_configs('%s', '%s')",
                                        CATALOG_NAME,
                                        ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(),
                                        ConfigOptions.DATALAKE_FORMAT.key()))
                        .collect()) {
            List<Row> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(results).hasSize(2);
            // the first row
            Row row0 = results.get(0);
            assertThat(row0.getField(0))
                    .isEqualTo(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key());
            assertThat(row0.getField(1)).isEqualTo("100 mb");
            assertThat(row0.getField(2)).isNotNull(); // config_source

            // the second row
            Row row1 = results.get(1);
            assertThat(row1.getField(0)).isEqualTo(ConfigOptions.DATALAKE_FORMAT.key());
            assertThat(row1.getField(1)).isEqualTo("paimon");
            assertThat(row1.getField(2)).isNotNull(); // config_source
        }

        // Get all configs
        try (CloseableIterator<Row> resultIterator =
                tEnv.executeSql(String.format("Call %s.sys.get_cluster_configs()", CATALOG_NAME))
                        .collect()) {
            List<Row> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(results).isNotEmpty();
        }

        // Get non-existent config
        try (CloseableIterator<Row> resultIterator =
                tEnv.executeSql(
                                String.format(
                                        "Call %s.sys.get_cluster_configs('non.existent.config')",
                                        CATALOG_NAME))
                        .collect()) {
            List<Row> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(results).hasSize(0);
        }

        // reset cluster configs.
        tEnv.executeSql(
                        String.format(
                                "Call %s.sys.reset_cluster_configs('%s')",
                                CATALOG_NAME,
                                ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()))
                .await();
    }

    @Test
    void testSetClusterConfigs() throws Exception {
        // Test setting a valid config
        try (CloseableIterator<Row> resultIterator =
                tEnv.executeSql(
                                String.format(
                                        "Call %s.sys.set_cluster_configs('%s', '200MB')",
                                        CATALOG_NAME,
                                        ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()))
                        .collect()) {
            List<Row> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(results).hasSize(1);
            assertThat(results.get(0).getField(0))
                    .asString()
                    .contains("Successfully set to '200MB'")
                    .contains(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key());
        }

        // Test setting multiple config
        try (CloseableIterator<Row> resultIterator =
                tEnv.executeSql(
                                String.format(
                                        "Call %s.sys.set_cluster_configs('%s', '300MB', '%s', 'paimon')",
                                        CATALOG_NAME,
                                        ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(),
                                        ConfigOptions.DATALAKE_FORMAT.key()))
                        .collect()) {
            List<Row> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(results).hasSize(2);
            assertThat(results.get(0).getField(0))
                    .asString()
                    .contains("Successfully set to '300MB'")
                    .contains(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key());

            assertThat(results.get(1).getField(0))
                    .asString()
                    .contains("Successfully set to 'paimon'")
                    .contains(ConfigOptions.DATALAKE_FORMAT.key());
        }

        // Verify the config was actually set
        try (CloseableIterator<Row> resultIterator =
                tEnv.executeSql(
                                String.format(
                                        "Call %s.sys.get_cluster_configs('%s')",
                                        CATALOG_NAME,
                                        ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()))
                        .collect()) {
            List<Row> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(results).hasSize(1);
            assertThat(results.get(0).getField(1)).isEqualTo("300MB");
        }

        // reset cluster configs.
        tEnv.executeSql(
                        String.format(
                                "Call %s.sys.reset_cluster_configs('%s', '%s')",
                                CATALOG_NAME,
                                ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(),
                                ConfigOptions.DATALAKE_FORMAT.key()))
                .await();
    }

    @Test
    void testResetClusterConfigs() throws Exception {
        // First set a config
        tEnv.executeSql(
                        String.format(
                                "Call %s.sys.set_cluster_configs('%s', 'paimon', '%s', '200MB')",
                                CATALOG_NAME,
                                ConfigOptions.DATALAKE_FORMAT.key(),
                                ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()))
                .await();

        // Delete the config (reset to default)
        try (CloseableIterator<Row> resultIterator =
                tEnv.executeSql(
                                String.format(
                                        "Call %s.sys.reset_cluster_configs('%s', '%s')",
                                        CATALOG_NAME,
                                        ConfigOptions.DATALAKE_FORMAT.key(),
                                        ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()))
                        .collect()) {
            List<Row> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(results).hasSize(2);
            assertThat(results.get(0).getField(0))
                    .asString()
                    .contains("Successfully deleted")
                    .contains(ConfigOptions.DATALAKE_FORMAT.key());

            assertThat(results.get(1).getField(0))
                    .asString()
                    .contains("Successfully deleted")
                    .contains(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key());
        }
    }

    @Test
    void testSetClusterConfigsValidation() throws Exception {
        // Try to set an invalid config (not allowed for dynamic change)
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.set_cluster_configs('invalid.config.key', 'value')",
                                                        CATALOG_NAME))
                                        .await())
                .rootCause()
                // TODO: Fix misleading error: non-existent key reported as not allowed.
                .hasMessageContaining(
                        "The config key invalid.config.key is not allowed to be changed dynamically");

        // validation to ensure an even number of arguments are passed
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.set_cluster_configs('%s')",
                                                        CATALOG_NAME,
                                                        ConfigOptions
                                                                .KV_SHARED_RATE_LIMITER_BYTES_PER_SEC
                                                                .key()))
                                        .await())
                .rootCause()
                .hasMessageContaining(
                        "config_pairs must be set in pairs. Please specify a valid configuration pairs");

        // Try to no parameters passed
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.set_cluster_configs()",
                                                        CATALOG_NAME))
                                        .await())
                .rootCause()
                .hasMessageContaining(
                        "config_pairs cannot be null or empty. Please specify a valid configuration pairs");

        // Try to mismatched key-value pairs in the input parameters.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.set_cluster_configs('%s', 'paimon')",
                                                        CATALOG_NAME,
                                                        ConfigOptions
                                                                .KV_SHARED_RATE_LIMITER_BYTES_PER_SEC
                                                                .key()))
                                        .await())
                .rootCause()
                .hasMessageContaining(
                        "Could not parse value 'paimon' for key 'kv.rocksdb.shared-rate-limiter.bytes-per-sec'");
    }

    @Test
    void testResetClusterConfigsValidation() throws Exception {
        // Try to reset an invalid config
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.reset_cluster_configs('invalid.config.key')",
                                                        CATALOG_NAME))
                                        .await())
                .rootCause()
                // TODO: Fix misleading error: non-existent key reported as not allowed.
                .hasMessageContaining(
                        "The config key invalid.config.key is not allowed to be changed dynamically");

        // Try to no parameters passed
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.reset_cluster_configs()",
                                                        CATALOG_NAME))
                                        .await())
                .rootCause()
                .hasMessageContaining(
                        "config_keys cannot be null or empty. Please specify valid configuration keys");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAddAndRemoveServerTag(boolean upperCase) throws Exception {
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        assertThat(zkClient.getServerTags()).isNotPresent();

        String addServerTag =
                String.format(
                        upperCase
                                ? "Call %s.sys.add_server_tag('0', 'PERMANENT_OFFLINE')"
                                : "Call %s.sys.add_server_tag('0', 'permanent_offline')",
                        CATALOG_NAME);
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(addServerTag).collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }
        Optional<ServerTags> serverTags = zkClient.getServerTags();
        assertThat(serverTags).isPresent();
        Map<Integer, ServerTag> tags = serverTags.get().getServerTags();
        assertThat(tags.size()).isEqualTo(1);
        assertThat(tags.get(0)).isEqualTo(PERMANENT_OFFLINE);

        // test add duplicated server tag. no error will be thrown.
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(addServerTag).collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }
        serverTags = zkClient.getServerTags();
        assertThat(serverTags).isPresent();
        tags = serverTags.get().getServerTags();
        assertThat(tags.size()).isEqualTo(1);
        assertThat(tags.get(0)).isEqualTo(PERMANENT_OFFLINE);

        // test add multi.
        String addMultiServerTag =
                String.format(
                        upperCase
                                ? "Call %s.sys.add_server_tag('1,2', 'PERMANENT_OFFLINE')"
                                : "Call %s.sys.add_server_tag('1,2', 'permanent_offline')",
                        CATALOG_NAME);
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(addMultiServerTag).collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }
        serverTags = zkClient.getServerTags();
        assertThat(serverTags).isPresent();
        tags = serverTags.get().getServerTags();
        assertThat(tags.keySet()).containsExactlyInAnyOrder(0, 1, 2);

        String removeServerTag =
                String.format(
                        upperCase
                                ? "Call %s.sys.remove_server_tag('0,1,2', 'PERMANENT_OFFLINE')"
                                : "Call %s.sys.remove_server_tag('0,1,2', 'permanent_offline')",
                        CATALOG_NAME);
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(removeServerTag).collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }
        serverTags = zkClient.getServerTags();
        assertThat(serverTags).isNotPresent();

        // test remove non-exist server tag, no error will be thrown.
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(removeServerTag).collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRebalance(boolean upperCase) throws Exception {
        // add server tag PERMANENT_OFFLINE for server 3, this will avoid to generate bucket
        // assignment on server 3 when create table.
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(
                                "Call "
                                        + CATALOG_NAME
                                        + ".sys.add_server_tag('3', 'PERMANENT_OFFLINE')")
                        .collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }

        // first create some unbalance assignment table.
        for (int i = 0; i < 2; i++) {
            String tableName = "reblance_test_tab_" + i;
            tEnv.executeSql(
                    String.format(
                            "create table %s (a int, b varchar, c bigint, d int ) "
                                    + "with ('connector' = 'fluss')",
                            tableName));
            long tableId =
                    admin.getTableInfo(TablePath.of(DEFAULT_DB, tableName)).get().getTableId();
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        }

        // remove tag after crated table.
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(
                                "Call "
                                        + CATALOG_NAME
                                        + ".sys.remove_server_tag('3', 'PERMANENT_OFFLINE')")
                        .collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }

        String rebalance =
                String.format(
                        upperCase
                                ? "Call %s.sys.rebalance('REPLICA_DISTRIBUTION,LEADER_DISTRIBUTION')"
                                : "Call %s.sys.rebalance('replica_distribution,leader_distribution')",
                        CATALOG_NAME);
        try (CloseableIterator<Row> rows = tEnv.executeSql(rebalance).collect()) {
            List<String> actual =
                    CollectionUtil.iteratorToList(rows).stream()
                            .map(Row::toString)
                            .collect(Collectors.toList());
            assertThat(actual.size()).isEqualTo(1);
        }

        // test cancel an un-existed rebalance.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "Call %s.sys.cancel_rebalance('not-exist-id')",
                                                        CATALOG_NAME))
                                        .await())
                .rootCause()
                .isInstanceOf(NoRebalanceInProgressException.class)
                .hasMessageContaining(
                        "Rebalance task id not-exist-id to cancel is not the current rebalance task id");

        // To compatibility with old version, that the call procedure input cannot be null.
        Optional<RebalanceProgress> progressOpt = admin.listRebalanceProgress(null).get();
        assertThat(progressOpt).isPresent();
        RebalanceProgress progress = progressOpt.get();
        // test cancel rebalance.
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(
                                String.format(
                                        "Call %s.sys.cancel_rebalance('%s')",
                                        CATALOG_NAME, progress.rebalanceId()))
                        .collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }

        // delete rebalance plan to avoid conflict with other tests.
        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().deleteRebalanceTask();
    }

    @Test
    void testListRebalanceProgress() throws Exception {
        // add server tag PERMANENT_OFFLINE for server 3, this will avoid to generate bucket
        // assignment on server 3 when create table.
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(
                                "Call "
                                        + CATALOG_NAME
                                        + ".sys.add_server_tag('3', 'PERMANENT_OFFLINE')")
                        .collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }

        // first create some unbalance assignment table.
        for (int i = 0; i < 10; i++) {
            String tableName = "reblance_test_tab_" + i;
            tEnv.executeSql(
                    String.format(
                            "create table %s (a int, b varchar, c bigint, d int ) "
                                    + "with ('connector' = 'fluss')",
                            tableName));
            long tableId =
                    admin.getTableInfo(TablePath.of(DEFAULT_DB, tableName)).get().getTableId();
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        }

        // remove tag after crated table.
        try (CloseableIterator<Row> listProceduresIterator =
                tEnv.executeSql(
                                "Call "
                                        + CATALOG_NAME
                                        + ".sys.remove_server_tag('3', 'PERMANENT_OFFLINE')")
                        .collect()) {
            assertCallResult(listProceduresIterator, new String[] {"+I[success]"});
        }

        String rebalance =
                String.format(
                        "Call %s.sys.rebalance('REPLICA_DISTRIBUTION,LEADER_DISTRIBUTION')",
                        CATALOG_NAME);
        List<String> plan;
        try (CloseableIterator<Row> rows = tEnv.executeSql(rebalance).collect()) {
            plan =
                    CollectionUtil.iteratorToList(rows).stream()
                            .map(Row::toString)
                            .collect(Collectors.toList());
            assertThat(plan.size()).isEqualTo(1);
        }

        // To compatibility with old version, that the call procedure input cannot be null.
        Optional<RebalanceProgress> progressOpt = admin.listRebalanceProgress(null).get();
        assertThat(progressOpt).isPresent();
        RebalanceProgress progress = progressOpt.get();
        retry(
                Duration.ofMinutes(2),
                () -> {
                    try (CloseableIterator<Row> rows =
                            tEnv.executeSql(
                                            String.format(
                                                    "Call %s.sys.list_rebalance('%s')",
                                                    CATALOG_NAME, progress.rebalanceId()))
                                    .collect()) {
                        List<Row> listProgressResult = CollectionUtil.iteratorToList(rows);
                        Row row = listProgressResult.get(0);
                        assertThat(row.getArity()).isEqualTo(4);
                        assertThat(row.getField(0)).isEqualTo(progress.rebalanceId());
                        assertThat(row.getField(1)).isEqualTo(RebalanceStatus.COMPLETED);
                        assertThat((String) row.getField(2)).endsWith("%");
                        assertThat((String) row.getField(3)).startsWith("{\"rebalance_id\":");
                    }
                });
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter max lag time to make tests in FlussFailServerTableITCase faster
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));

        // Enable shared RocksDB rate limiter for testing
        conf.set(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC, MemorySize.parse("100mb"));

        // set security information.
        conf.setString(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:sasl");
        conf.setString("security.sasl.enabled.mechanisms", "plain");
        conf.setString(
                "security.sasl.plain.jaas.config",
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_root=\"password\" "
                        + "    user_guest=\"password2\";");
        conf.set(ConfigOptions.SUPER_USERS, "User:root");
        conf.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        return conf;
    }

    private static void assertCallResult(CloseableIterator<Row> rows, String[] expected) {
        List<String> actual =
                CollectionUtil.iteratorToList(rows).stream()
                        .map(Row::toString)
                        .collect(Collectors.toList());
        if (expected.length == 0) {
            assertThat(actual).isEmpty();
        } else {
            assertThat(actual).containsExactlyInAnyOrder(expected);
        }
    }
}
