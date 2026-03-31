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

import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;

/**
 * Integration tests for Variant type support in Flink 2.2 connector.
 *
 * <p>Tests the end-to-end flow of writing and reading Variant data through the Flink connector,
 * including type conversion between Flink's BinaryVariant and Fluss's Variant.
 */
public class Flink22VariantTypeITCase extends AbstractTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    static final String CATALOG_NAME = "testcatalog";
    static final String DEFAULT_DB = "defaultdb";

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    protected TableEnvironment tBatchEnv;

    @BeforeEach
    void before() {
        String bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        tBatchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tBatchEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tBatchEnv.executeSql("use catalog " + CATALOG_NAME);
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
        tBatchEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @Test
    void testVariantInLogTable() throws Exception {
        tEnv.executeSql(
                "create table variant_log_test ("
                        + "id int, "
                        + "data variant"
                        + ") with ('bucket.num' = '3')");

        tEnv.executeSql(
                        "INSERT INTO variant_log_test "
                                + "SELECT 1, PARSE_JSON('{\"name\": \"Alice\", \"age\": 30}')")
                .await();
        tEnv.executeSql("INSERT INTO variant_log_test " + "SELECT 2, PARSE_JSON('[1, 2, 3]')")
                .await();
        tEnv.executeSql("INSERT INTO variant_log_test " + "SELECT 3, PARSE_JSON('\"hello\"')")
                .await();
        tEnv.executeSql("INSERT INTO variant_log_test " + "SELECT 4, PARSE_JSON('42')").await();
        tEnv.executeSql("INSERT INTO variant_log_test " + "SELECT 5, PARSE_JSON('true')").await();
        tEnv.executeSql("INSERT INTO variant_log_test " + "SELECT 6, PARSE_JSON('null')").await();
        tEnv.executeSql("INSERT INTO variant_log_test " + "SELECT 7, CAST(NULL AS VARIANT)")
                .await();

        CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from variant_log_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, {\"age\":30,\"name\":\"Alice\"}]",
                        "+I[2, [1,2,3]]",
                        "+I[3, \"hello\"]",
                        "+I[4, 42]",
                        "+I[5, true]",
                        "+I[6, null]",
                        "+I[7, null]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testVariantInPrimaryKeyTable() throws Exception {
        tEnv.executeSql(
                "create table variant_pk_test ("
                        + "id int not null, "
                        + "data variant, "
                        + "primary key (id) not enforced"
                        + ")");

        // Insert initial data using streaming mode
        tEnv.executeSql(
                        "INSERT INTO variant_pk_test "
                                + "SELECT 1, PARSE_JSON('{\"key\": \"value1\"}')")
                .await();
        tEnv.executeSql(
                        "INSERT INTO variant_pk_test "
                                + "SELECT 2, PARSE_JSON('{\"key\": \"value2\"}')")
                .await();
        tEnv.executeSql("INSERT INTO variant_pk_test " + "SELECT 3, PARSE_JSON('100')").await();

        // Verify initial data using streaming mode
        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from variant_pk_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, {\"key\":\"value1\"}]", "+I[2, {\"key\":\"value2\"}]", "+I[3, 100]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // Update data
        tEnv.executeSql(
                        "INSERT INTO variant_pk_test "
                                + "SELECT 1, PARSE_JSON('{\"key\": \"updated\"}')")
                .await();
        tEnv.executeSql("INSERT INTO variant_pk_test " + "SELECT 3, CAST(NULL AS VARIANT)").await();

        // Verify updated data (changelog semantics: -U for old, +U for new)
        expectedRows =
                Arrays.asList(
                        "-U[1, {\"key\":\"value1\"}]",
                        "+U[1, {\"key\":\"updated\"}]",
                        "-U[3, 100]",
                        "+U[3, null]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testVariantWithMixedColumns() throws Exception {
        tEnv.executeSql(
                "create table variant_mixed_test ("
                        + "id int, "
                        + "name string, "
                        + "metadata variant, "
                        + "tags array<string>"
                        + ") with ('bucket.num' = '3')");

        tEnv.executeSql(
                        "INSERT INTO variant_mixed_test "
                                + "SELECT 1, 'Alice', PARSE_JSON('{\"role\": \"admin\"}'), "
                                + "ARRAY['tag1', 'tag2']")
                .await();
        tEnv.executeSql(
                        "INSERT INTO variant_mixed_test "
                                + "SELECT 2, 'Bob', PARSE_JSON('{\"role\": \"user\", \"level\": 5}'), "
                                + "ARRAY['tag3']")
                .await();
        tEnv.executeSql(
                        "INSERT INTO variant_mixed_test "
                                + "SELECT 3, 'Charlie', CAST(NULL AS VARIANT), CAST(NULL AS ARRAY<STRING>)")
                .await();

        CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from variant_mixed_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, Alice, {\"role\":\"admin\"}, [tag1, tag2]]",
                        "+I[2, Bob, {\"level\":5,\"role\":\"user\"}, [tag3]]",
                        "+I[3, Charlie, null, null]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testVariantWithNestedJson() throws Exception {
        tEnv.executeSql(
                "create table variant_nested_test ("
                        + "id int, "
                        + "doc variant"
                        + ") with ('bucket.num' = '3')");

        tEnv.executeSql(
                        "INSERT INTO variant_nested_test "
                                + "SELECT 1, PARSE_JSON('"
                                + "{\"user\": {\"name\": \"Alice\", \"address\": "
                                + "{\"city\": \"NYC\", \"zip\": \"10001\"}}, "
                                + "\"scores\": [95, 87, 92]}"
                                + "')")
                .await();
        tEnv.executeSql(
                        "INSERT INTO variant_nested_test "
                                + "SELECT 2, PARSE_JSON('"
                                + "{\"items\": [{\"id\": 1, \"name\": \"a\"}, "
                                + "{\"id\": 2, \"name\": \"b\"}]}"
                                + "')")
                .await();

        CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from variant_nested_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, {\"scores\":[95,87,92],\"user\":{\"address\":"
                                + "{\"city\":\"NYC\",\"zip\":\"10001\"},\"name\":\"Alice\"}}]",
                        "+I[2, {\"items\":[{\"id\":1,\"name\":\"a\"},"
                                + "{\"id\":2,\"name\":\"b\"}]}]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }
}
