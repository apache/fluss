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

import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
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
    void testComplexTypesInLogTable() throws Exception {
        tEnv.executeSql(
                "create table complex_log_test ("
                        + "id int, "
                        // Array types
                        + "int_array array<int>, "
                        + "bigint_array array<bigint>, "
                        + "float_array array<float>, "
                        + "double_array array<double>, "
                        + "string_array array<string>, "
                        + "boolean_array array<boolean>, "
                        + "nested_int_array array<array<int>>, "
                        + "nested_string_array array<array<string>>, "
                        + "deeply_nested_array array<array<array<int>>>, "
                        // Map types
                        + "simple_map map<int, string>, "
                        + "nested_map map<string, map<int, string>>, "
                        + "map_with_array_value map<string, array<int>>, "
                        + "map_with_row_value map<int, row<a int, b string>>, "
                        // Row types
                        + "simple_row row<a int, b string>, "
                        + "nested_row row<x int, y row<z int, w string>, v string>, "
                        + "array_of_rows array<row<a int, b string>>, "
                        // Advanced nested types
                        + "array_of_maps array<map<string, double>>, "
                        + "row_with_map_and_arrays row<data map<int, array<float>>>, "
                        + "map_with_complex_row map<bigint, row<name string, tags array<string>, ids array<int>>>"
                        + ") with ('bucket.num' = '3')");

        tEnv.executeSql(
                        "INSERT INTO complex_log_test VALUES "
                                + "(1, "
                                // Arrays
                                + "ARRAY[1, 2, CAST(NULL AS INT)], ARRAY[100, CAST(NULL AS BIGINT), 300], "
                                + "ARRAY[CAST(1.1 AS FLOAT), CAST(NULL AS FLOAT)], ARRAY[2.2, 3.3, CAST(NULL AS DOUBLE)], "
                                + "ARRAY['a', CAST(NULL AS STRING), 'c'], ARRAY[true, CAST(NULL AS BOOLEAN), false], "
                                + "ARRAY[ARRAY[1, 2], CAST(NULL AS ARRAY<INT>), ARRAY[3]], "
                                + "ARRAY[ARRAY['x'], ARRAY[CAST(NULL AS STRING), 'y']], "
                                + "ARRAY[ARRAY[ARRAY[1, 2]], ARRAY[ARRAY[3, 4, 5]]], "
                                // Maps
                                + "MAP[1, 'one', 2, 'two'], "
                                + "MAP['k1', MAP[10, 'v1', 20, 'v2']], "
                                + "MAP['arr1', ARRAY[1, 2, 3]], "
                                + "MAP[1, ROW(100, 'row1')], "
                                // Rows
                                + "ROW(10, 'hello'), "
                                + "ROW(20, ROW(30, 'nested'), 'row1'), "
                                + "ARRAY[ROW(1, 'a'), ROW(2, 'b')], "
                                // Advanced nested types - using CAST for complex constructors
                                + "CAST(NULL AS ARRAY<MAP<STRING, DOUBLE>>), "
                                + "CAST(NULL AS ROW<data MAP<INT, ARRAY<FLOAT>>>), "
                                + "CAST(NULL AS MAP<BIGINT, ROW<name STRING, tags ARRAY<STRING>, ids ARRAY<INT>>>)"
                                + "), "
                                + "(2, "
                                // Arrays
                                + "CAST(NULL AS ARRAY<INT>), ARRAY[400, 500], "
                                + "ARRAY[CAST(4.4 AS FLOAT)], ARRAY[5.5], "
                                + "ARRAY['d', 'e'], ARRAY[true], "
                                + "ARRAY[ARRAY[6, 7, 8]], ARRAY[ARRAY['z']], "
                                + "ARRAY[ARRAY[ARRAY[9]]], "
                                // Maps
                                + "MAP[3, 'three'], "
                                + "MAP['k3', MAP[40, 'v4']], "
                                + "MAP['arr3', ARRAY[6]], "
                                + "MAP[3, ROW(300, 'row3')], "
                                // Rows
                                + "ROW(40, 'world'), "
                                + "ROW(50, ROW(60, 'test'), 'row2'), "
                                + "ARRAY[ROW(3, 'c')], "
                                // Advanced nested types - using CAST for complex constructors
                                + "CAST(NULL AS ARRAY<MAP<STRING, DOUBLE>>), "
                                + "CAST(NULL AS ROW<data MAP<INT, ARRAY<FLOAT>>>), "
                                + "CAST(NULL AS MAP<BIGINT, ROW<name STRING, tags ARRAY<STRING>, ids ARRAY<INT>>>)"
                                + "), "
                                + "(3, "
                                + "CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<BIGINT>), "
                                + "CAST(NULL AS ARRAY<FLOAT>), CAST(NULL AS ARRAY<DOUBLE>), "
                                + "CAST(NULL AS ARRAY<STRING>), CAST(NULL AS ARRAY<BOOLEAN>), "
                                + "CAST(NULL AS ARRAY<ARRAY<INT>>), CAST(NULL AS ARRAY<ARRAY<STRING>>), "
                                + "CAST(NULL AS ARRAY<ARRAY<ARRAY<INT>>>), "
                                + "MAP[1, CAST(NULL AS STRING)], "
                                + "MAP['k1', MAP[10, CAST(NULL AS STRING)]], "
                                + "MAP['arr1', CAST(NULL AS ARRAY<INT>)], "
                                + "CAST(NULL AS MAP<INT, ROW<a INT, b STRING>>), "
                                + "CAST(NULL AS ROW<a INT, b STRING>), "
                                + "CAST(NULL AS ROW<x INT, y ROW<z INT, w STRING>, v STRING>), "
                                + "CAST(NULL AS ARRAY<ROW<a INT, b STRING>>), "
                                + "CAST(NULL AS ARRAY<MAP<STRING, DOUBLE>>), "
                                + "CAST(NULL AS ROW<data MAP<INT, ARRAY<FLOAT>>>), "
                                + "CAST(NULL AS MAP<BIGINT, ROW<name STRING, tags ARRAY<STRING>, ids ARRAY<INT>>>)"
                                + "), "
                                + "(4, "
                                + "CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<BIGINT>), "
                                + "CAST(NULL AS ARRAY<FLOAT>), CAST(NULL AS ARRAY<DOUBLE>), "
                                + "CAST(NULL AS ARRAY<STRING>), CAST(NULL AS ARRAY<BOOLEAN>), "
                                + "CAST(NULL AS ARRAY<ARRAY<INT>>), CAST(NULL AS ARRAY<ARRAY<STRING>>), "
                                + "CAST(NULL AS ARRAY<ARRAY<ARRAY<INT>>>), "
                                + "MAP[2, 'two', 3, CAST(NULL AS STRING)], "
                                + "MAP['k2', CAST(NULL AS MAP<INT, STRING>)], "
                                + "MAP['arr2', ARRAY[1, 2], 'arr3', CAST(NULL AS ARRAY<INT>)], "
                                + "CAST(NULL AS MAP<INT, ROW<a INT, b STRING>>), "
                                + "CAST(NULL AS ROW<a INT, b STRING>), "
                                + "CAST(NULL AS ROW<x INT, y ROW<z INT, w STRING>, v STRING>), "
                                + "CAST(NULL AS ARRAY<ROW<a INT, b STRING>>), "
                                + "CAST(NULL AS ARRAY<MAP<STRING, DOUBLE>>), "
                                + "CAST(NULL AS ROW<data MAP<INT, ARRAY<FLOAT>>>), "
                                + "CAST(NULL AS MAP<BIGINT, ROW<name STRING, tags ARRAY<STRING>, ids ARRAY<INT>>>)"
                                + ")")
                .await();

        CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from complex_log_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, [1, 2, null], [100, null, 300], [1.1, null], [2.2, 3.3, null], [a, null, c], [true, null, false], [[1, 2], null, [3]], [[x], [null, y]], [[[1, 2]], [[3, 4, 5]]], "
                                + "{1=one, 2=two}, {k1={20=v2, 10=v1}}, {arr1=[1, 2, 3]}, {1=+I[100, row1]}, " // map
                                + "+I[10, hello], +I[20, +I[30, nested], row1], [+I[1, a], +I[2, b]], " // row
                                + "null, null, null]", // complex nested types
                        "+I[2, null, [400, 500], [4.4], [5.5], [d, e], [true], [[6, 7, 8]], [[z]], [[[9]]], "
                                + "{3=three}, {k3={40=v4}}, {arr3=[6]}, {3=+I[300, row3]}, "
                                + "+I[40, world], +I[50, +I[60, test], row2], [+I[3, c]], "
                                + "null, null, null]",
                        "+I[3, null, null, null, null, null, null, null, null, null, "
                                + "{1=null}, {k1={10=null}}, {arr1=null}, null, "
                                + "null, null, null, null, null, null]",
                        "+I[4, null, null, null, null, null, null, null, null, null, "
                                + "{2=two, 3=null}, {k2=null}, {arr2=[1, 2], arr3=null}, null, "
                                + "null, null, null, null, null, null]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testComplexTypesInPartitionedLogTable() throws Exception {
        tEnv.executeSql(
                "create table complex_log_test ("
                        + "id int, "
                        + "dt string, "
                        // Array types
                        + "int_array array<int>, "
                        + "bigint_array array<bigint>, "
                        + "float_array array<float>, "
                        + "double_array array<double>, "
                        + "string_array array<string>, "
                        + "boolean_array array<boolean>, "
                        + "nested_int_array array<array<int>>, "
                        + "nested_string_array array<array<string>>, "
                        + "deeply_nested_array array<array<array<int>>>, "
                        // Map types
                        + "simple_map map<int, string>, "
                        + "nested_map map<string, map<int, string>>, "
                        + "map_with_array_value map<string, array<int>>, "
                        // Row types
                        + "simple_row row<a int, b string>, "
                        + "nested_row row<x int, y row<z int, w string>>, "
                        // Advanced nested types
                        + "array_of_maps array<map<string, double>>, "
                        + "row_with_map_and_arrays row<data map<int, array<float>>>, "
                        + "map_with_complex_row map<bigint, row<name string, tags array<string>, ids array<int>>>"
                        + ") PARTITIONED BY (dt) "
                        + "with ('bucket.num' = '3')");

        tEnv.executeSql(
                        "INSERT INTO complex_log_test VALUES "
                                + "(1, '2024', "
                                // Arrays
                                + "ARRAY[1, 2, CAST(NULL AS INT)], ARRAY[100, CAST(NULL AS BIGINT), 300], "
                                + "ARRAY[CAST(1.1 AS FLOAT), CAST(NULL AS FLOAT)], ARRAY[2.2, 3.3, CAST(NULL AS DOUBLE)], "
                                + "ARRAY['a', CAST(NULL AS STRING), 'c'], ARRAY[true, CAST(NULL AS BOOLEAN), false], "
                                + "ARRAY[ARRAY[1, 2], CAST(NULL AS ARRAY<INT>), ARRAY[3]], "
                                + "ARRAY[ARRAY['x'], ARRAY[CAST(NULL AS STRING), 'y']], "
                                + "ARRAY[ARRAY[ARRAY[1, 2]], ARRAY[ARRAY[3, 4, 5]]], "
                                // Maps
                                + "MAP[1, 'one'], "
                                + "MAP['k1', MAP[10, 'v1']], "
                                + "MAP['arr1', ARRAY[1, 2]], "
                                // Rows
                                + "ROW(10, 'hello'), "
                                + "ROW(20, ROW(30, 'nested')), "
                                // Advanced nested types
                                + "CAST(NULL AS ARRAY<MAP<STRING, DOUBLE>>), "
                                + "CAST(NULL AS ROW<data MAP<INT, ARRAY<FLOAT>>>), "
                                + "CAST(NULL AS MAP<BIGINT, ROW<name STRING, tags ARRAY<STRING>, ids ARRAY<INT>>>)"
                                + "), "
                                + "(2, '2023', "
                                // Arrays
                                + "CAST(NULL AS ARRAY<INT>), ARRAY[400, 500], "
                                + "ARRAY[CAST(4.4 AS FLOAT)], ARRAY[5.5], "
                                + "ARRAY['d', 'e'], ARRAY[true], "
                                + "ARRAY[ARRAY[6, 7, 8]], ARRAY[ARRAY['z']], "
                                + "ARRAY[ARRAY[ARRAY[9]]], "
                                // Maps
                                + "MAP[3, 'three'], "
                                + "MAP['k2', MAP[20, 'v2']], "
                                + "MAP['arr2', ARRAY[3, 4, 5]], "
                                // Rows
                                + "ROW(40, 'world'), "
                                + "ROW(50, ROW(60, 'test')), "
                                // Advanced nested types
                                + "CAST(NULL AS ARRAY<MAP<STRING, DOUBLE>>), "
                                + "CAST(NULL AS ROW<data MAP<INT, ARRAY<FLOAT>>>), "
                                + "CAST(NULL AS MAP<BIGINT, ROW<name STRING, tags ARRAY<STRING>, ids ARRAY<INT>>>)"
                                + "), "
                                + "(3, '2024', "
                                + "CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<BIGINT>), "
                                + "CAST(NULL AS ARRAY<FLOAT>), CAST(NULL AS ARRAY<DOUBLE>), "
                                + "CAST(NULL AS ARRAY<STRING>), CAST(NULL AS ARRAY<BOOLEAN>), "
                                + "CAST(NULL AS ARRAY<ARRAY<INT>>), CAST(NULL AS ARRAY<ARRAY<STRING>>), "
                                + "CAST(NULL AS ARRAY<ARRAY<ARRAY<INT>>>), "
                                + "MAP[1, CAST(NULL AS STRING)], "
                                + "MAP['k1', MAP[10, CAST(NULL AS STRING)]], "
                                + "MAP['arr1', CAST(NULL AS ARRAY<INT>)], "
                                + "CAST(NULL AS ROW<a INT, b STRING>), "
                                + "CAST(NULL AS ROW<x INT, y ROW<z INT, w STRING>>), "
                                + "CAST(NULL AS ARRAY<MAP<STRING, DOUBLE>>), "
                                + "CAST(NULL AS ROW<data MAP<INT, ARRAY<FLOAT>>>), "
                                + "CAST(NULL AS MAP<BIGINT, ROW<name STRING, tags ARRAY<STRING>, ids ARRAY<INT>>>)"
                                + "), "
                                + "(4, '2023', "
                                + "CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<BIGINT>), "
                                + "CAST(NULL AS ARRAY<FLOAT>), CAST(NULL AS ARRAY<DOUBLE>), "
                                + "CAST(NULL AS ARRAY<STRING>), CAST(NULL AS ARRAY<BOOLEAN>), "
                                + "CAST(NULL AS ARRAY<ARRAY<INT>>), CAST(NULL AS ARRAY<ARRAY<STRING>>), "
                                + "CAST(NULL AS ARRAY<ARRAY<ARRAY<INT>>>), "
                                + "MAP[2, 'two', 3, CAST(NULL AS STRING)], "
                                + "MAP['k2', CAST(NULL AS MAP<INT, STRING>)], "
                                + "MAP['arr2', ARRAY[1, 2], 'arr3', CAST(NULL AS ARRAY<INT>)], "
                                + "CAST(NULL AS ROW<a INT, b STRING>), "
                                + "CAST(NULL AS ROW<x INT, y ROW<z INT, w STRING>>), "
                                + "CAST(NULL AS ARRAY<MAP<STRING, DOUBLE>>), "
                                + "CAST(NULL AS ROW<data MAP<INT, ARRAY<FLOAT>>>), "
                                + "CAST(NULL AS MAP<BIGINT, ROW<name STRING, tags ARRAY<STRING>, ids ARRAY<INT>>>)"
                                + ")")
                .await();

        CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from complex_log_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 2024, [1, 2, null], [100, null, 300], [1.1, null], [2.2, 3.3, null], [a, null, c], [true, null, false], [[1, 2], null, [3]], [[x], [null, y]], [[[1, 2]], [[3, 4, 5]]], "
                                + "{1=one}, {k1={10=v1}}, {arr1=[1, 2]}, +I[10, hello], +I[20, +I[30, nested]], null, null, null]",
                        "+I[2, 2023, null, [400, 500], [4.4], [5.5], [d, e], [true], [[6, 7, 8]], [[z]], [[[9]]], "
                                + "{3=three}, {k2={20=v2}}, {arr2=[3, 4, 5]}, +I[40, world], +I[50, +I[60, test]], null, null, null]",
                        "+I[3, 2024, null, null, null, null, null, null, null, null, null, "
                                + "{1=null}, {k1={10=null}}, {arr1=null}, null, null, null, null, null]",
                        "+I[4, 2023, null, null, null, null, null, null, null, null, null, "
                                + "{2=two, 3=null}, {k2=null}, {arr2=[1, 2], arr3=null}, null, null, null, null, null]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testComplexTypesInPrimaryKeyTable() throws Exception {
        tEnv.executeSql(
                "create table complex_pk_test ("
                        + "id int, "
                        + "name string, "
                        + "int_array array<int>, "
                        + "bigint_array array<bigint>, "
                        + "float_array array<float>, "
                        + "double_array array<double>, "
                        + "string_array array<string>, "
                        + "boolean_array array<boolean>, "
                        + "nested_int_array array<array<int>>, "
                        + "nested_string_array array<array<string>>, "
                        + "simple_map map<string, int>, "
                        + "nested_map map<string, map<int, string>>, "
                        + "address row<city string, zipcode int>, "
                        + "nested_row row<x int, y row<z int, w string>>, "
                        + "primary key(id) not enforced"
                        + ") with ('bucket.num' = '3')");

        tEnv.executeSql(
                        "INSERT INTO complex_pk_test VALUES "
                                + "(1, 'user1', "
                                + "ARRAY[1, 2], ARRAY[100, 300], ARRAY[CAST(1.1 AS FLOAT)], ARRAY[2.2, 3.3], "
                                + "ARRAY['a', CAST(NULL AS STRING), 'c'], ARRAY[true, false], "
                                + "ARRAY[ARRAY[1, 2], CAST(NULL AS ARRAY<INT>), ARRAY[3]], "
                                + "ARRAY[ARRAY['x'], ARRAY[CAST(NULL AS STRING), 'y']], "
                                + "MAP['age', 25, 'score', 90], "
                                + "MAP['k1', MAP[10, 'v1', 20, 'v2']], "
                                + "ROW('Beijing', 100000), "
                                + "ROW(10, ROW(20, 'nested1'))"
                                + "), "
                                + "(2, 'user2', "
                                + "CAST(NULL AS ARRAY<INT>), ARRAY[400, 500], ARRAY[CAST(4.4 AS FLOAT)], ARRAY[5.5], "
                                + "ARRAY['d', 'e'], ARRAY[true], ARRAY[ARRAY[6, 7, 8]], ARRAY[ARRAY['z']], "
                                + "MAP['age', 30, 'score', 85], "
                                + "MAP['k2', MAP[30, 'v3']], "
                                + "ROW('Shanghai', 200000), "
                                + "ROW(30, ROW(40, 'nested2'))"
                                + "), "
                                + "(3, 'user3', "
                                + "ARRAY[10], ARRAY[600], ARRAY[CAST(7.7 AS FLOAT)], ARRAY[8.8], "
                                + "ARRAY['f'], ARRAY[false], ARRAY[ARRAY[9]], ARRAY[ARRAY['w']], "
                                + "CAST(NULL AS MAP<STRING, INT>), "
                                + "CAST(NULL AS MAP<STRING, MAP<INT, STRING>>), "
                                + "CAST(NULL AS ROW<city STRING, zipcode INT>), "
                                + "CAST(NULL AS ROW<x INT, y ROW<z INT, w STRING>>)"
                                + ")")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from complex_pk_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, user1, [1, 2], [100, 300], [1.1], [2.2, 3.3], [a, null, c], [true, false], [[1, 2], null, [3]], [[x], [null, y]], {score=90, age=25}, {k1={20=v2, 10=v1}}, +I[Beijing, 100000], +I[10, +I[20, nested1]]]",
                        "+I[2, user2, null, [400, 500], [4.4], [5.5], [d, e], [true], [[6, 7, 8]], [[z]], {score=85, age=30}, {k2={30=v3}}, +I[Shanghai, 200000], +I[30, +I[40, nested2]]]",
                        "+I[3, user3, [10], [600], [7.7], [8.8], [f], [false], [[9]], [[w]], null, null, null, null]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        tEnv.executeSql(
                        "INSERT INTO complex_pk_test VALUES "
                                + "(1, 'user1_updated', "
                                + "ARRAY[100, 200], ARRAY[1000], ARRAY[CAST(10.1 AS FLOAT)], ARRAY[11.1], "
                                + "ARRAY['updated'], ARRAY[false], ARRAY[ARRAY[100]], ARRAY[ARRAY['updated']], "
                                + "MAP['age', 26, 'score', 95], "
                                + "MAP['k1', MAP[50, 'v5']], "
                                + "ROW('Shenzhen', 300000), "
                                + "ROW(50, ROW(60, 'updated_nested'))"
                                + "), "
                                + "(4, 'user4', "
                                + "ARRAY[20, 30], ARRAY[2000, 3000], ARRAY[CAST(20.2 AS FLOAT)], ARRAY[30.3], "
                                + "ARRAY['new'], ARRAY[true], ARRAY[ARRAY[200], ARRAY[300]], ARRAY[ARRAY['new1'], ARRAY['new2']], "
                                + "MAP['age', 40], "
                                + "MAP['k4', MAP[70, 'v7']], "
                                + "ROW('Hangzhou', 500000), "
                                + "ROW(70, ROW(80, 'new_nested'))"
                                + ")")
                .await();

        expectedRows =
                Arrays.asList(
                        "-U[1, user1, [1, 2], [100, 300], [1.1], [2.2, 3.3], [a, null, c], [true, false], [[1, 2], null, [3]], [[x], [null, y]], {score=90, age=25}, {k1={20=v2, 10=v1}}, +I[Beijing, 100000], +I[10, +I[20, nested1]]]",
                        "+U[1, user1_updated, [100, 200], [1000], [10.1], [11.1], [updated], [false], [[100]], [[updated]], {score=95, age=26}, {k1={50=v5}}, +I[Shenzhen, 300000], +I[50, +I[60, updated_nested]]]",
                        "+I[4, user4, [20, 30], [2000, 3000], [20.2], [30.3], [new], [true], [[200], [300]], [[new1], [new2]], {age=40}, {k4={70=v7}}, +I[Hangzhou, 500000], +I[70, +I[80, new_nested]]]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        tEnv.executeSql(
                        "INSERT INTO complex_pk_test VALUES "
                                + "(5, 'user5', "
                                + "ARRAY[50, 60], CAST(NULL AS ARRAY<BIGINT>), CAST(NULL AS ARRAY<FLOAT>), CAST(NULL AS ARRAY<DOUBLE>), "
                                + "CAST(NULL AS ARRAY<STRING>), CAST(NULL AS ARRAY<BOOLEAN>), "
                                + "CAST(NULL AS ARRAY<ARRAY<INT>>), CAST(NULL AS ARRAY<ARRAY<STRING>>), "
                                + "MAP['age', 50], "
                                + "CAST(NULL AS MAP<STRING, MAP<INT, STRING>>), "
                                + "CAST(NULL AS ROW<city STRING, zipcode INT>), "
                                + "CAST(NULL AS ROW<x INT, y ROW<z INT, w STRING>>)"
                                + "), "
                                + "(6, 'user6', "
                                + "CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<BIGINT>), CAST(NULL AS ARRAY<FLOAT>), CAST(NULL AS ARRAY<DOUBLE>), "
                                + "ARRAY['g', 'h'], CAST(NULL AS ARRAY<BOOLEAN>), "
                                + "CAST(NULL AS ARRAY<ARRAY<INT>>), CAST(NULL AS ARRAY<ARRAY<STRING>>), "
                                + "CAST(NULL AS MAP<STRING, INT>), "
                                + "CAST(NULL AS MAP<STRING, MAP<INT, STRING>>), "
                                + "ROW('Guangzhou', 600000), "
                                + "CAST(NULL AS ROW<x INT, y ROW<z INT, w STRING>>)"
                                + "), "
                                + "(7, 'user7', "
                                + "CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<BIGINT>), CAST(NULL AS ARRAY<FLOAT>), CAST(NULL AS ARRAY<DOUBLE>), "
                                + "CAST(NULL AS ARRAY<STRING>), CAST(NULL AS ARRAY<BOOLEAN>), "
                                + "CAST(NULL AS ARRAY<ARRAY<INT>>), CAST(NULL AS ARRAY<ARRAY<STRING>>), "
                                + "CAST(NULL AS MAP<STRING, INT>), "
                                + "CAST(NULL AS MAP<STRING, MAP<INT, STRING>>), "
                                + "CAST(NULL AS ROW<city STRING, zipcode INT>), "
                                + "CAST(NULL AS ROW<x INT, y ROW<z INT, w STRING>>)"
                                + ")")
                .await();

        List<String> expectedPartialRows =
                Arrays.asList(
                        "+I[5, user5, [50, 60], null, null, null, null, null, null, null, {age=50}, null, null, null]",
                        "+I[6, user6, null, null, null, null, [g, h], null, null, null, null, null, +I[Guangzhou, 600000], null]",
                        "+I[7, user7, null, null, null, null, null, null, null, null, null, null, null, null]");
        assertResultsIgnoreOrder(rowIter, expectedPartialRows, true);

        Schema srcSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("query_name", DataTypes.STRING())
                        .column("c", DataTypes.INT())
                        .columnByExpression("proc", "PROCTIME()")
                        .build();
        RowTypeInfo srcTestTypeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {Types.INT, Types.STRING, Types.INT},
                        new String[] {"a", "query_name", "c"});
        List<Row> testData =
                Arrays.asList(
                        Row.of(1, "name1", 11),
                        Row.of(2, "name2", 2),
                        Row.of(3, "name33", 33),
                        Row.of(10, "name0", 44));
        DataStream<Row> srcDs = env.fromCollection(testData).returns(srcTestTypeInfo);
        tEnv.dropTemporaryView("src");
        tEnv.createTemporaryView("src", tEnv.fromDataStream(srcDs, srcSchema));
        CloseableIterator<Row> collected =
                tEnv.executeSql(
                                "SELECT a, query_name, complex_pk_test.* FROM src "
                                        + "LEFT JOIN complex_pk_test FOR SYSTEM_TIME AS OF src.proc "
                                        + "ON src.a = complex_pk_test.id")
                        .collect();
        List<String> expected =
                Arrays.asList(
                        "+I[1, name1, 1, user1_updated, [100, 200], [1000], [10.1], [11.1], [updated], [false], [[100]], [[updated]], {score=95, age=26}, {k1={50=v5}}, +I[Shenzhen, 300000], +I[50, +I[60, updated_nested]]]",
                        "+I[2, name2, 2, user2, null, [400, 500], [4.4], [5.5], [d, e], [true], [[6, 7, 8]], [[z]], {score=85, age=30}, {k2={30=v3}}, +I[Shanghai, 200000], +I[30, +I[40, nested2]]]",
                        "+I[3, name33, 3, user3, [10], [600], [7.7], [8.8], [f], [false], [[9]], [[w]], null, null, null, null]",
                        "+I[10, name0, null, null, null, null, null, null, null, null, null, null, null, null, null, null]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testExceptionsForComplexTypesUsage() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table array_partition_test ("
                                                + "id int, "
                                                + "data string, "
                                                + "tags array<string>, "
                                                + "primary key(id, tags) not enforced"
                                                + ") partitioned by (tags)"))
                .hasRootCauseInstanceOf(InvalidTableException.class)
                .hasRootCauseMessage(
                        "Primary key column 'tags' has unsupported data type ARRAY<STRING> NOT NULL. "
                                + "Currently, primary key column does not support types: [ARRAY, MAP, ROW].");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table array_bucket_test ("
                                                + "id int, "
                                                + "data string, "
                                                + "tags array<string> "
                                                + ") with ('bucket.key' = 'tags')"))
                .hasRootCauseInstanceOf(InvalidTableException.class)
                .hasRootCauseMessage(
                        "Bucket key column 'tags' has unsupported data type ARRAY<STRING>. "
                                + "Currently, bucket key column does not support types: [ARRAY, MAP, ROW].");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table map_partition_test ("
                                                + "id int, "
                                                + "data string, "
                                                + "metadata map<string, string>, "
                                                + "primary key(id, metadata) not enforced"
                                                + ") partitioned by (metadata)"))
                .hasRootCauseInstanceOf(InvalidTableException.class)
                .hasRootCauseMessage(
                        "Primary key column 'metadata' has unsupported data type MAP<STRING NOT NULL, STRING> NOT NULL. "
                                + "Currently, primary key column does not support types: [ARRAY, MAP, ROW].");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table map_bucket_test ("
                                                + "id int, "
                                                + "data string, "
                                                + "metadata map<string, string> "
                                                + ") with ('bucket.key' = 'metadata')"))
                .hasRootCauseInstanceOf(InvalidTableException.class)
                .hasRootCauseMessage(
                        "Bucket key column 'metadata' has unsupported data type MAP<STRING NOT NULL, STRING>. "
                                + "Currently, bucket key column does not support types: [ARRAY, MAP, ROW].");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table row_partition_test ("
                                                + "id int, "
                                                + "data string, "
                                                + "info row<name string, age int>"
                                                + ") partitioned by (info)"))
                .hasRootCauseInstanceOf(InvalidTableException.class)
                .hasRootCauseMessage(
                        "Currently, partitioned table supported partition key type are [CHAR, STRING, "
                                + "BOOLEAN, BINARY, BYTES, TINYINT, SMALLINT, INTEGER, DATE, TIME_WITHOUT_TIME_ZONE, "
                                + "BIGINT, FLOAT, DOUBLE, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE], "
                                + "but got partition key 'info' with data type ROW<`name` STRING, `age` INT>.");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table row_bucket_test ("
                                                + "id int, "
                                                + "data string, "
                                                + "info row<name string, age int> "
                                                + ") with ('bucket.key' = 'info')"))
                .hasRootCauseInstanceOf(InvalidTableException.class)
                .hasRootCauseMessage(
                        "Bucket key column 'info' has unsupported data type ROW<`name` STRING, `age` INT>. "
                                + "Currently, bucket key column does not support types: [ARRAY, MAP, ROW].");
    }
}
