/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.spark.catalog;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link com.alibaba.fluss.connector.spark.catalog.SparkCatalog}. */
public class SparkCatalogITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCatalogITCase.class);

    private static final String DB = "my_db";
    private static final String TABLE = "my_table";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    private static SparkSession spark;
    private static Admin admin;

    @BeforeAll
    public static void beforeAll() {
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        Map<String, String> configs = getSparkConfigs(flussConf);
        SparkConf sparkConf =
                new SparkConf().setAppName("bss-spark-unit-tests").setMaster("local[*]");
        configs.forEach(sparkConf::set);
        spark = SparkSession.builder().config(sparkConf).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Connection connection = ConnectionFactory.createConnection(flussConf);
        admin = connection.getAdmin();
    }

    @AfterAll
    public static void afterAll() {
        try {
            spark.close();
            admin.close();
        } catch (Exception e) {
            // ignore
        }
    }

    @AfterEach
    public void afterEach() {
        sql("DROP TABLE IF EXISTS fluss_catalog." + DB + "." + TABLE);
        sql("DROP DATABASE IF EXISTS fluss_catalog." + DB);
    }

    @Test
    public void createDatabaseTest() {
        sql("CREATE DATABASE fluss_catalog." + DB);
        assertThatThrownBy(() -> sql("CREATE DATABASE fluss_catalog." + DB))
                .isInstanceOf(NamespaceAlreadyExistsException.class)
                .hasMessageContaining(
                        "[SCHEMA_ALREADY_EXISTS] Cannot create schema `my_db` because it already exists.");
        sql("CREATE DATABASE IF NOT EXISTS fluss_catalog." + DB);
        List<String> databases =
                sql("SHOW DATABASES IN fluss_catalog").collectAsList().stream()
                        .map(row -> row.getString(0))
                        .collect(Collectors.toList());
        assertThat(databases.size()).isEqualTo(2);
        assertThat("fluss").isIn(databases);
        assertThat(DB).isIn(databases);
    }

    @Test
    public void dropDatabaseTest() {
        sql("CREATE DATABASE fluss_catalog." + DB);
        sql("DROP DATABASE fluss_catalog." + DB);
        assertThatThrownBy(() -> sql("DROP DATABASE fluss_catalog." + DB))
                .isInstanceOf(NoSuchNamespaceException.class)
                .hasMessageContaining("[SCHEMA_NOT_FOUND] The schema `my_db` cannot be found.");
        sql("DROP DATABASE IF EXISTS fluss_catalog." + DB);
        List<String> databases =
                sql("SHOW DATABASES IN fluss_catalog").collectAsList().stream()
                        .map(row -> row.getString(0))
                        .collect(Collectors.toList());
        assertThat(databases.size()).isEqualTo(1);
        assertThat(databases.get(0)).isEqualTo("fluss");
    }

    @Test
    public void dropDatabaseWithCascadeTest() {
        sql("CREATE DATABASE fluss_catalog." + DB);
        sql("CREATE TABLE fluss_catalog." + DB + "." + TABLE + " (id INT, name STRING)");
        assertThatThrownBy(() -> sql("DROP DATABASE fluss_catalog." + DB))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining(
                        "[SCHEMA_NOT_EMPTY] Cannot drop a schema `my_db` because it contains objects.");
        List<String> tables =
                sql("SHOW TABLES IN fluss_catalog." + DB).collectAsList().stream()
                        .map(row -> row.getString(1))
                        .collect(Collectors.toList());
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.get(0)).isEqualTo(TABLE);
        List<String> databases =
                sql("SHOW DATABASES IN fluss_catalog").collectAsList().stream()
                        .map(row -> row.getString(0))
                        .collect(Collectors.toList());
        assertThat(databases.size()).isEqualTo(2);

        sql("DROP DATABASE fluss_catalog." + DB + " CASCADE");
        databases =
                sql("SHOW DATABASES IN fluss_catalog").collectAsList().stream()
                        .map(row -> row.getString(0))
                        .collect(Collectors.toList());
        assertThat(databases.size()).isEqualTo(1);
        assertThat(databases.get(0)).isEqualTo("fluss");
    }

    @Test
    public void createTableTest() {
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE fluss_catalog."
                                                + DB
                                                + "."
                                                + TABLE
                                                + " (id INT, name STRING)"))
                .isInstanceOf(NoSuchNamespaceException.class)
                .hasMessageContaining("[SCHEMA_NOT_FOUND] The schema `my_db` cannot be found.");
        sql("CREATE DATABASE fluss_catalog." + DB);
        sql("CREATE TABLE fluss_catalog." + DB + "." + TABLE + " (id INT, name STRING)");
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE fluss_catalog."
                                                + DB
                                                + "."
                                                + TABLE
                                                + " (id INT, name STRING)"))
                .isInstanceOf(TableAlreadyExistsException.class)
                .hasMessageContaining(
                        "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `my_db`.`my_table` because it already exists.");
        sql(
                "CREATE TABLE IF NOT EXISTS fluss_catalog."
                        + DB
                        + "."
                        + TABLE
                        + " (id INT, name STRING)");
        List<String> tables =
                sql("SHOW TABLES IN fluss_catalog." + DB).collectAsList().stream()
                        .map(row -> row.getString(1))
                        .collect(Collectors.toList());
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.get(0)).isEqualTo(TABLE);
    }

    @Test
    public void dropTableTest() {
        sql("CREATE DATABASE fluss_catalog." + DB);
        sql("CREATE TABLE fluss_catalog." + DB + "." + TABLE + " (id INT, name STRING)");
        List<String> tables =
                sql("SHOW TABLES IN fluss_catalog." + DB).collectAsList().stream()
                        .map(row -> row.getString(1))
                        .collect(Collectors.toList());
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.get(0)).isEqualTo(TABLE);
        sql("DROP TABLE fluss_catalog." + DB + "." + TABLE);
        assertThatThrownBy(() -> sql("DROP TABLE fluss_catalog." + DB + "." + TABLE))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining(
                        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `fluss_catalog`.`my_db`.`my_table` cannot be found.");
        sql("DROP TABLE IF EXISTS fluss_catalog." + DB + "." + TABLE);
        tables =
                sql("SHOW TABLES IN fluss_catalog." + DB).collectAsList().stream()
                        .map(row -> row.getString(1))
                        .collect(Collectors.toList());
        assertThat(tables).isEmpty();
    }

    @Test
    public void fieldTypeTest() {
        sql("CREATE DATABASE fluss_catalog." + DB);
        sql(
                "CREATE TABLE fluss_catalog."
                        + DB
                        + "."
                        + TABLE
                        + " ("
                        + " int_field INT,"
                        + " short_field SHORT,"
                        + " byte_field BYTE,"
                        + " string_field STRING,"
                        + " boolean_field BOOLEAN,"
                        + " long_field LONG,"
                        + " float_field FLOAT,"
                        + " double_field DOUBLE,"
                        + " char_field CHAR(3),"
                        + " binary_field BINARY,"
                        + " date_field DATE,"
                        + " timestamp_field TIMESTAMP,"
                        + " timestamp_ntz_field TIMESTAMP_NTZ,"
                        + " decimal_field DECIMAL(10, 5)"
                        + ")");
        List<String> tables =
                sql("SHOW TABLES IN fluss_catalog." + DB).collectAsList().stream()
                        .map(row -> row.getString(1))
                        .collect(Collectors.toList());
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.get(0)).isEqualTo(TABLE);

        // check spark datatype
        sql("DESCRIBE TABLE fluss_catalog." + DB + "." + TABLE);

        // check fluss datatype
        TableInfo tableInfo = admin.getTable(TablePath.of(DB, TABLE)).join();
        assertThat(tableInfo.getTableDescriptor().hasPrimaryKey()).isFalse();
        assertThat(tableInfo.getTableDescriptor().getPartitionKeys()).isEmpty();
        List<Schema.Column> columns = tableInfo.getTableDescriptor().getSchema().getColumns();
        assertThat(columns.size()).isEqualTo(14);

        assertThat(columns.get(0).getName()).isEqualTo("int_field");
        assertThat(columns.get(0).getDataType()).isInstanceOf(IntType.class);
        assertThat(columns.get(1).getName()).isEqualTo("short_field");
        assertThat(columns.get(1).getDataType()).isInstanceOf(SmallIntType.class);
        assertThat(columns.get(2).getName()).isEqualTo("byte_field");
        assertThat(columns.get(2).getDataType()).isInstanceOf(TinyIntType.class);
        assertThat(columns.get(3).getName()).isEqualTo("string_field");
        assertThat(columns.get(3).getDataType()).isInstanceOf(StringType.class);
        assertThat(columns.get(4).getName()).isEqualTo("boolean_field");
        assertThat(columns.get(4).getDataType()).isInstanceOf(BooleanType.class);
        assertThat(columns.get(5).getName()).isEqualTo("long_field");
        assertThat(columns.get(5).getDataType()).isInstanceOf(BigIntType.class);
        assertThat(columns.get(6).getName()).isEqualTo("float_field");
        assertThat(columns.get(6).getDataType()).isInstanceOf(FloatType.class);
        assertThat(columns.get(7).getName()).isEqualTo("double_field");
        assertThat(columns.get(7).getDataType()).isInstanceOf(DoubleType.class);
        assertThat(columns.get(8).getName()).isEqualTo("char_field");
        assertThat(columns.get(8).getDataType()).isInstanceOf(CharType.class);
        assertThat(((CharType) columns.get(8).getDataType()).getLength()).isEqualTo(3);
        assertThat(columns.get(9).getName()).isEqualTo("binary_field");
        assertThat(columns.get(9).getDataType()).isInstanceOf(BytesType.class);
        assertThat(columns.get(10).getName()).isEqualTo("date_field");
        assertThat(columns.get(10).getDataType()).isInstanceOf(DateType.class);
        assertThat(columns.get(11).getName()).isEqualTo("timestamp_field");
        assertThat(columns.get(11).getDataType()).isInstanceOf(LocalZonedTimestampType.class);
        assertThat(((LocalZonedTimestampType) columns.get(11).getDataType()).getPrecision())
                .isEqualTo(6);
        assertThat(columns.get(12).getName()).isEqualTo("timestamp_ntz_field");
        assertThat(columns.get(12).getDataType()).isInstanceOf(TimestampType.class);
        assertThat(((TimestampType) columns.get(12).getDataType()).getPrecision()).isEqualTo(6);
        assertThat(columns.get(13).getName()).isEqualTo("decimal_field");
        assertThat(columns.get(13).getDataType()).isInstanceOf(DecimalType.class);
        assertThat(((DecimalType) columns.get(13).getDataType()).getPrecision()).isEqualTo(10);
        assertThat(((DecimalType) columns.get(13).getDataType()).getScale()).isEqualTo(5);
    }

    @Test
    public void primaryKeyAndPartitionKeyTest() {
        sql("CREATE DATABASE fluss_catalog." + DB);
        sql(
                "CREATE TABLE fluss_catalog."
                        + DB
                        + "."
                        + TABLE
                        + " ("
                        + " int_field INT,"
                        + " short_field SHORT,"
                        + " byte_field BYTE,"
                        + " string_field STRING,"
                        + " boolean_field BOOLEAN,"
                        + " long_field LONG,"
                        + " float_field FLOAT,"
                        + " double_field DOUBLE,"
                        + " char_field CHAR(3),"
                        + " binary_field BINARY,"
                        + " date_field DATE,"
                        + " timestamp_field TIMESTAMP,"
                        + " timestamp_ntz_field TIMESTAMP_NTZ,"
                        + " decimal_field DECIMAL(10, 2)"
                        + ") PARTITIONED BY (string_field) OPTIONS ("
                        + " 'primary.key' = 'int_field, string_field',"
                        + " 'table.auto-partition.enabled' = 'true',"
                        + " 'table.auto-partition.time-unit' = 'HOUR'"
                        + ")");
        List<String> tables =
                sql("SHOW TABLES IN fluss_catalog." + DB).collectAsList().stream()
                        .map(row -> row.getString(1))
                        .collect(Collectors.toList());
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.get(0)).isEqualTo(TABLE);

        sql("DESCRIBE TABLE fluss_catalog." + DB + "." + TABLE);

        TableInfo tableInfo = admin.getTable(TablePath.of(DB, TABLE)).join();
        // check primary key
        assertThat(tableInfo.getTableDescriptor().hasPrimaryKey()).isTrue();
        List<String> primaryKey =
                tableInfo.getTableDescriptor().getSchema().getPrimaryKey().get().getColumnNames();
        assertThat(primaryKey.size()).isEqualTo(2);
        assertThat(primaryKey.get(0)).isEqualTo("int_field");
        assertThat(primaryKey.get(1)).isEqualTo("string_field");
        // check partition key
        List<String> partitionKeys = tableInfo.getTableDescriptor().getPartitionKeys();
        assertThat(partitionKeys.size()).isEqualTo(1);
        assertThat(partitionKeys.get(0)).isEqualTo("string_field");
        List<Schema.Column> columns = tableInfo.getTableDescriptor().getSchema().getColumns();
        assertThat(columns.size()).isEqualTo(14);
    }

    @Test
    public void commentTest() {
        sql("CREATE DATABASE fluss_catalog." + DB);
        sql(
                "CREATE TABLE fluss_catalog."
                        + DB
                        + "."
                        + TABLE
                        + " ("
                        + " id INT COMMENT 'id comment test',"
                        + " first_name STRING COMMENT 'first name comment test',"
                        + " last_name STRING"
                        + ") COMMENT 'table comment test'");

        List<String> tables =
                sql("SHOW TABLES IN fluss_catalog." + DB).collectAsList().stream()
                        .map(row -> row.getString(1))
                        .collect(Collectors.toList());
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables.get(0)).isEqualTo(TABLE);

        List<String> comments =
                sql("DESCRIBE TABLE fluss_catalog." + DB + "." + TABLE).collectAsList().stream()
                        .map(row -> row.getString(2))
                        .collect(Collectors.toList());
        assertThat(comments.size()).isEqualTo(3);
        assertThat(comments.get(0)).isEqualTo("id comment test");
        assertThat(comments.get(1)).isEqualTo("first name comment test");
        assertThat(comments.get(2)).isNull();
    }

    private static Map<String, String> getSparkConfigs(Configuration flussConf) {
        Map<String, String> configs = new HashMap<>();
        configs.put("spark.sql.catalog.fluss_catalog", SparkCatalog.class.getName());
        configs.put(
                "spark.sql.catalog.fluss_catalog.bootstrap.servers",
                String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS)));
        return configs;
    }

    public static Dataset<Row> sql(String sqlText) {
        Dataset<Row> ds = spark.sql(sqlText);
        if (ds.columns().length == 0) {
            LOG.info("+----------------+");
            LOG.info("|  Empty Result  |");
            LOG.info("+----------------+");
        } else {
            ds.show(20, 50);
        }
        return ds;
    }
}
