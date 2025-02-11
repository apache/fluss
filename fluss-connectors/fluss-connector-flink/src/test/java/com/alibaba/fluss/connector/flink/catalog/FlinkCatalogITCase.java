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

package com.alibaba.fluss.connector.flink.catalog;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.InvalidConfigException;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BUCKET_KEY;
import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BUCKET_NUMBER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link com.alibaba.fluss.connector.flink.catalog.FlinkCatalog}. */
class FlinkCatalogITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    private static final String CATALOG_NAME = "testcatalog";
    private static final String DEFAULT_DB = FlinkCatalogOptions.DEFAULT_DATABASE.defaultValue();
    static Catalog catalog;
    static TableEnvironment tEnv;

    @BeforeAll
    static void beforeAll() {
        // open a catalog so that we can get table from the catalog
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        String bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        catalog =
                new FlinkCatalog(
                        CATALOG_NAME,
                        DEFAULT_DB,
                        bootstrapServers,
                        Thread.currentThread().getContextClassLoader());
        catalog.open();
        // create table environment
        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
    }

    @AfterAll
    static void afterAll() {
        tEnv.executeSql("use catalog " + TableConfigOptions.TABLE_CATALOG_NAME.defaultValue());
        tEnv.executeSql("DROP CATALOG IF EXISTS " + CATALOG_NAME);
        if (catalog != null) {
            catalog.close();
        }
    }

    @BeforeEach
    void before() {
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        // we don't need to "USE fluss" explicitly as it is the default database
    }

    @Test
    void testCreateTable() throws Exception {
        // create a table will all supported data types
        tEnv.executeSql(
                "create table test_table "
                        + "(a int not null primary key not enforced,"
                        + " b CHAR(3),"
                        + " c STRING not null COMMENT 'STRING COMMENT',"
                        + " d STRING,"
                        + " e BOOLEAN,"
                        + " f BINARY(2),"
                        + " g BYTES COMMENT 'BYTES',"
                        + " h BYTES,"
                        + " i DECIMAL(12, 2),"
                        + " j TINYINT,"
                        + " k SMALLINT,"
                        + " l BIGINT,"
                        + " m FLOAT,"
                        + " n DOUBLE,"
                        + " o DATE,"
                        + " p TIME,"
                        + " q TIMESTAMP,"
                        + " r TIMESTAMP_LTZ,"
                        + " s ROW<a INT>) COMMENT 'a test table'");
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder
                .column("a", DataTypes.INT().notNull())
                .column("b", DataTypes.CHAR(3))
                .column("c", DataTypes.STRING().notNull())
                .withComment("STRING COMMENT")
                .column("d", DataTypes.STRING())
                .column("e", DataTypes.BOOLEAN())
                .column("f", DataTypes.BINARY(2))
                .column("g", DataTypes.BYTES())
                .withComment("BYTES")
                .column("h", DataTypes.BYTES())
                .column("i", DataTypes.DECIMAL(12, 2))
                .column("j", DataTypes.TINYINT())
                .column("k", DataTypes.SMALLINT())
                .column("l", DataTypes.BIGINT())
                .column("m", DataTypes.FLOAT())
                .column("n", DataTypes.DOUBLE())
                .column("o", DataTypes.DATE())
                .column("p", DataTypes.TIME())
                .column("q", DataTypes.TIMESTAMP())
                .column("r", DataTypes.TIMESTAMP_LTZ())
                .column("s", DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())))
                .primaryKey("a");
        Schema expectedSchema = schemaBuilder.build();
        CatalogTable table =
                (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, "test_table"));
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
    }

    @Test
    void testCreateUnSupportedTable() {
        // test unsupported table, partitioned table without auto partitioned enabled
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_table_unsupported"
                                                + " (a int, b int) partitioned by (b)"))
                .cause()
                .isInstanceOf(CatalogException.class)
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Currently, partitioned table must enable auto partition");

        // test invalid property
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_arrow_compression"
                                                + " (a int, b int) with ("
                                                + " 'table.log.format' = 'arrow',"
                                                + " 'table.log.arrow.compression.type' = 'zstd',"
                                                + " 'table.log.arrow.compression.zstd.level' = '0')"))
                .cause()
                .hasRootCauseMessage(
                        "Invalid ZSTD compression level: 0. Expected a value between 1 and 22.");
    }

    @Test
    void testCreateNoPkTable() throws Exception {
        tEnv.executeSql("create table append_only_table(a int, b int) with ('bucket.num' = '10')");
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("a", DataTypes.INT()).column("b", DataTypes.INT());
        Schema expectedSchema = schemaBuilder.build();
        CatalogTable table =
                (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, "append_only_table"));
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("bucket.num", "10");
        assertOptionsEqual(table.getOptions(), expectedOptions);
    }

    @Test
    void testCreatePartitionedTable() throws Exception {
        tEnv.executeSql(
                "create table test_partitioned_table (a int, b string) partitioned by (b) "
                        + "with ('table.auto-partition.enabled' = 'true',"
                        + " 'table.auto-partition.time-unit' = 'day')");
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("a", DataTypes.INT()).column("b", DataTypes.STRING());
        Schema expectedSchema = schemaBuilder.build();
        CatalogTable table =
                (CatalogTable)
                        catalog.getTable(new ObjectPath(DEFAULT_DB, "test_partitioned_table"));
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
        assertThat(table.getPartitionKeys()).isEqualTo(Collections.singletonList("b"));
    }

    @Test
    void testTableWithExpression() throws Exception {
        // create a table with watermark and computed column
        tEnv.executeSql(
                "CREATE TABLE expression_test (\n"
                        + "    `user` BIGINT not null primary key not enforced,\n"
                        + "    product STRING COMMENT 'comment1',\n"
                        + "    price DOUBLE,\n"
                        + "    quantity DOUBLE,\n"
                        + "    cost AS price * quantity,\n"
                        + "    order_time TIMESTAMP(3),\n"
                        + "    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND\n"
                        + ") with ('k1' = 'v1')");
        CatalogTable table =
                (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, "expression_test"));
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder
                .column("user", DataTypes.BIGINT().notNull())
                .column("product", DataTypes.STRING())
                .withComment("comment1")
                .column("price", DataTypes.DOUBLE())
                .column("quantity", DataTypes.DOUBLE())
                .columnByExpression("cost", "`price` * `quantity`")
                .column("order_time", DataTypes.TIMESTAMP(3))
                .watermark("order_time", "`order_time` - INTERVAL '5' SECOND")
                .primaryKey("user");
        Schema expectedSchema = schemaBuilder.build();
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("k1", "v1");
        expectedOptions.put(BUCKET_KEY.key(), "user");
        expectedOptions.put(BUCKET_NUMBER.key(), "1");
        assertOptionsEqual(table.getOptions(), expectedOptions);
    }

    @Test
    void testCreateWithUnSupportDataType() {
        // create a table with varchar datatype
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_table_unsupported (a varchar(10))"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported data type: VARCHAR(10)");

        // create a table with varbinary datatype
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_table_unsupported (a varbinary(10))"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported data type: VARBINARY(10)");

        // create a table with multiset datatype
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table test_table_unsupported (a multiset<int>)"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported data type: MULTISET<INT>");
    }

    @Test
    void testCreateDatabase() {
        tEnv.executeSql("create database test_db");
        List<Row> databases =
                CollectionUtil.iteratorToList(tEnv.executeSql("show databases").collect());
        assertThat(databases.toString())
                .isEqualTo(String.format("[+I[%s], +I[test_db]]", DEFAULT_DB));
        tEnv.executeSql("drop database test_db");
        databases = CollectionUtil.iteratorToList(tEnv.executeSql("show databases").collect());
        assertThat(databases.toString()).isEqualTo(String.format("[+I[%s]]", DEFAULT_DB));
    }

    @Test
    void testFactoryCannotFindForCreateTemporaryTable() {
        // create fluss temporary table is not supported
        tEnv.executeSql(
                "create temporary table test_temp_table (a int, b int)"
                        + " with ('connector' = 'fluss', 'bootstrap.servers' = 'localhost:9092')");
        assertThatThrownBy(() -> tEnv.executeSql("insert into test_temp_table values (1, 2)"))
                .cause()
                .isInstanceOf(ValidationException.class)
                .hasMessage("Cannot discover a connector using option: 'connector'='fluss'");
    }

    @Test
    void testFactoryCannotFindForCreateCatalogTable() {
        // create fluss table under non-fluss catalog is not supported
        tEnv.executeSql("use catalog " + TableConfigOptions.TABLE_CATALOG_NAME.defaultValue());
        tEnv.executeSql(
                "create table test_catalog_table (a int, b int)"
                        + " with ('connector' = 'fluss', 'bootstrap.servers' = 'localhost:9092')");
        assertThatThrownBy(() -> tEnv.executeSql("insert into test_catalog_table values (1, 2)"))
                .cause()
                .isInstanceOf(ValidationException.class)
                .hasMessage("Cannot discover a connector using option: 'connector'='fluss'");
    }

    private static void assertOptionsEqual(
            Map<String, String> actualOptions, Map<String, String> expectedOptions) {
        actualOptions.remove(ConfigOptions.BOOTSTRAP_SERVERS.key());
        actualOptions.remove(ConfigOptions.TABLE_REPLICATION_FACTOR.key());
        assertThat(actualOptions.size()).isEqualTo(expectedOptions.size());
        assertThat(actualOptions).isEqualTo(expectedOptions);
    }
}
