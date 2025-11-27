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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for Array type support in Flink connector. */
abstract class FlinkComplexTypeITCase extends AbstractTestBase {

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
    void testArrayTypesInLogTable() throws Exception {
        tEnv.executeSql(
                "create table array_log_test ("
                        + "id int, "
                        + "int_array array<int>, "
                        + "bigint_array array<bigint>, "
                        + "float_array array<float>, "
                        + "double_array array<double>, "
                        + "string_array array<string>, "
                        + "boolean_array array<boolean>, "
                        + "nested_int_array array<array<int>>, "
                        + "nested_string_array array<array<string>>, "
                        + "deeply_nested_array array<array<array<int>>>"
                        + ") with ('bucket.num' = '3')");

        tEnv.executeSql(
                        "INSERT INTO array_log_test VALUES "
                                + "(1, ARRAY[1, 2, CAST(NULL AS INT)], ARRAY[100, CAST(NULL AS BIGINT), 300], "
                                + "ARRAY[CAST(1.1 AS FLOAT), CAST(NULL AS FLOAT)], ARRAY[2.2, 3.3, CAST(NULL AS DOUBLE)], "
                                + "ARRAY['a', CAST(NULL AS STRING), 'c'], ARRAY[true, CAST(NULL AS BOOLEAN), false], "
                                + "ARRAY[ARRAY[1, 2], CAST(NULL AS ARRAY<INT>), ARRAY[3]], "
                                + "ARRAY[ARRAY['x'], ARRAY[CAST(NULL AS STRING), 'y']], "
                                + "ARRAY[ARRAY[ARRAY[1, 2]], ARRAY[ARRAY[3, 4, 5]]]), "
                                + "(2, CAST(NULL AS ARRAY<INT>), ARRAY[400, 500], "
                                + "ARRAY[CAST(4.4 AS FLOAT)], ARRAY[5.5], "
                                + "ARRAY['d', 'e'], ARRAY[true], "
                                + "ARRAY[ARRAY[6, 7, 8]], ARRAY[ARRAY['z']], "
                                + "ARRAY[ARRAY[ARRAY[9]]])")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from array_log_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, [1, 2, null], [100, null, 300], [1.1, null], [2.2, 3.3, null], [a, null, c], [true, null, false], [[1, 2], null, [3]], [[x], [null, y]], [[[1, 2]], [[3, 4, 5]]]]",
                        "+I[2, null, [400, 500], [4.4], [5.5], [d, e], [true], [[6, 7, 8]], [[z]], [[[9]]]]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testArrayTypesInPrimaryKeyTable() throws Exception {
        tEnv.executeSql(
                "create table array_pk_test ("
                        + "id int, "
                        + "int_array array<int>, "
                        + "bigint_array array<bigint>, "
                        + "float_array array<float>, "
                        + "double_array array<double>, "
                        + "string_array array<string>, "
                        + "boolean_array array<boolean>, "
                        + "nested_int_array array<array<int>>, "
                        + "nested_string_array array<array<string>>, "
                        + "primary key(id) not enforced"
                        + ") with ('bucket.num' = '3')");

        tEnv.executeSql(
                        "INSERT INTO array_pk_test VALUES "
                                + "(1, ARRAY[1, 2], ARRAY[100, 300], ARRAY[CAST(1.1 AS FLOAT)], ARRAY[2.2, 3.3], "
                                + "ARRAY['a', CAST(NULL AS STRING), 'c'], ARRAY[true, false], "
                                + "ARRAY[ARRAY[1, 2], CAST(NULL AS ARRAY<INT>), ARRAY[3]], "
                                + "ARRAY[ARRAY['x'], ARRAY[CAST(NULL AS STRING), 'y']]), "
                                + "(2, CAST(NULL AS ARRAY<INT>), ARRAY[400, 500], ARRAY[CAST(4.4 AS FLOAT)], ARRAY[5.5], "
                                + "ARRAY['d', 'e'], ARRAY[true], ARRAY[ARRAY[6, 7, 8]], ARRAY[ARRAY['z']]), "
                                + "(3, ARRAY[10], ARRAY[600], ARRAY[CAST(7.7 AS FLOAT)], ARRAY[8.8], "
                                + "ARRAY['f'], ARRAY[false], ARRAY[ARRAY[9]], ARRAY[ARRAY['w']])")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from array_pk_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, [1, 2], [100, 300], [1.1], [2.2, 3.3], [a, null, c], [true, false], [[1, 2], null, [3]], [[x], [null, y]]]",
                        "+I[2, null, [400, 500], [4.4], [5.5], [d, e], [true], [[6, 7, 8]], [[z]]]",
                        "+I[3, [10], [600], [7.7], [8.8], [f], [false], [[9]], [[w]]]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        tEnv.executeSql(
                        "INSERT INTO array_pk_test VALUES "
                                + "(1, ARRAY[100, 200], ARRAY[1000], ARRAY[CAST(10.1 AS FLOAT)], ARRAY[11.1], "
                                + "ARRAY['updated'], ARRAY[false], ARRAY[ARRAY[100]], ARRAY[ARRAY['updated']]), "
                                + "(4, ARRAY[20, 30], ARRAY[2000, 3000], ARRAY[CAST(20.2 AS FLOAT)], ARRAY[30.3], "
                                + "ARRAY['new'], ARRAY[true], ARRAY[ARRAY[200], ARRAY[300]], ARRAY[ARRAY['new1'], ARRAY['new2']])")
                .await();

        expectedRows =
                Arrays.asList(
                        "-U[1, [1, 2], [100, 300], [1.1], [2.2, 3.3], [a, null, c], [true, false], [[1, 2], null, [3]], [[x], [null, y]]]",
                        "+U[1, [100, 200], [1000], [10.1], [11.1], [updated], [false], [[100]], [[updated]]]",
                        "+I[4, [20, 30], [2000, 3000], [20.2], [30.3], [new], [true], [[200], [300]], [[new1], [new2]]]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testArrayTypeAsPartitionKeyThrowsException() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table array_partition_test ("
                                                + "id int, "
                                                + "data string, "
                                                + "tags array<string>, "
                                                + "primary key(id) not enforced"
                                                + ") partitioned by (tags)"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("is not supported");
    }

    @Test
    void testArrayTypeAsPrimaryKeyThrowsException() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table array_pk_invalid ("
                                                + "id int, "
                                                + "data array<string>, "
                                                + "primary key(data) not enforced"
                                                + ")"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("is not supported");
    }

    @Test
    void testArrayTypeAsBucketKeyThrowsException() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table array_bucket_test ("
                                                + "id int, "
                                                + "data array<string>, "
                                                + "primary key(id) not enforced"
                                                + ") with ('bucket.key' = 'data', 'bucket.num' = '3')"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("is not supported");
    }
}
