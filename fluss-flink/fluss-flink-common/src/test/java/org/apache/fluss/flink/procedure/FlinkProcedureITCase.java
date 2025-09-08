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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.SecurityDisabledException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for Flink Procedure. */
public abstract class FlinkProcedureITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(initConfig())
                    .build();

    static final String CATALOG_NAME = "testcatalog";

    TableEnvironment tEnv;

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
    }

    @Test
    void testShowProcedures() throws Exception {
        try (CloseableIterator<Row> showProceduresIterator =
                tEnv.executeSql("show procedures").collect()) {
            List<String> expectedShowProceduresResult =
                    Arrays.asList("+I[sys.add_acl]", "+I[sys.drop_acl]", "+I[sys.list_acl]");
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
            List<String> acls =
                    CollectionUtil.iteratorToList(listProceduresIterator).stream()
                            .map(Row::toString)
                            .collect(Collectors.toList());
            assertThat(acls)
                    .containsExactlyInAnyOrder(
                            "+I[resource=\"cluster\";permission=\"ALLOW\";principal=\"User:Alice\";operation=\"READ\";host=\"*\"]");
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
            List<String> acls =
                    CollectionUtil.iteratorToList(listProceduresIterator).stream()
                            .map(Row::toString)
                            .collect(Collectors.toList());
            assertThat(acls)
                    .containsExactlyInAnyOrder(
                            "+I[resource=\"cluster\";permission=\"ALLOW\";principal=\"User:Alice\";operation=\"READ\";host=\"127.0.0.1\"]");
        }
        tEnv.executeSql(dropAcl).await();
        try (CloseableIterator<Row> listProceduresIterator = tEnv.executeSql(listAcl).collect()) {
            List<String> acls =
                    CollectionUtil.iteratorToList(listProceduresIterator).stream()
                            .map(Row::toString)
                            .collect(Collectors.toList());
            assertThat(acls).isEmpty();
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
                "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                        + "    user_root=\"password\" "
                        + "    user_guest=\"password2\";");
        conf.set(ConfigOptions.SUPER_USERS, "User:root");
        conf.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        return conf;
    }
}
