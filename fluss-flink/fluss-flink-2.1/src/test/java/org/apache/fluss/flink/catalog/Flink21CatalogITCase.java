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

package org.apache.fluss.flink.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.flink.FlinkConnectorOptions.BUCKET_KEY;
import static org.apache.fluss.flink.FlinkConnectorOptions.BUCKET_NUMBER;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for catalog in Flink 2.1. */
public class Flink21CatalogITCase extends FlinkCatalogITCase {

    @BeforeAll
    static void beforeAll() {
        FlinkCatalogITCase.beforeAll();

        // close the old one and open a new one later
        catalog.close();

        catalog =
                new Flink21Catalog(
                        catalog.catalogName,
                        catalog.defaultDatabase,
                        catalog.bootstrapServers,
                        catalog.classLoader,
                        catalog.securityConfigs);
        catalog.open();
    }

    @Test
    @Override
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
                .primaryKey("a")
                .index("a");
        Schema expectedSchema = schemaBuilder.build();
        CatalogTable table =
                (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, "test_table"));
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
    }

    @Test
    @Override
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
                .primaryKey("user")
                .index("user");
        Schema expectedSchema = schemaBuilder.build();
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("k1", "v1");
        expectedOptions.put(BUCKET_KEY.key(), "user");
        expectedOptions.put(BUCKET_NUMBER.key(), "1");
        assertOptionsEqual(table.getOptions(), expectedOptions);
    }

    @Test
    void testGetTableWithIndex() throws Exception {
        String tableName = "table_with_pk_only";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a int, "
                                + " b varchar, "
                                + " c bigint, "
                                + " primary key (a, b) NOT ENFORCED"
                                + ") with ( "
                                + " 'connector' = 'fluss' "
                                + ")",
                        tableName));
        CatalogTable table = (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, tableName));
        Schema expectedSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.STRING().notNull())
                        .column("c", DataTypes.BIGINT())
                        .primaryKey("a", "b")
                        .index("a", "b")
                        .build();
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);

        tableName = "table_with_prefix_bucket_key";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a int, "
                                + " b varchar, "
                                + " c bigint, "
                                + " primary key (a, b) NOT ENFORCED"
                                + ") with ( "
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'a'"
                                + ")",
                        tableName));

        table = (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, tableName));
        expectedSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.STRING().notNull())
                        .column("c", DataTypes.BIGINT())
                        .primaryKey("a", "b")
                        .index("a", "b")
                        .index("a")
                        .build();
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);

        tableName = "table_with_bucket_key_is_not_prefix_pk";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a int, "
                                + " b varchar, "
                                + " c bigint, "
                                + " primary key (a, b) NOT ENFORCED"
                                + ") with ( "
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'b'"
                                + ")",
                        tableName));

        table = (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, tableName));
        expectedSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.STRING().notNull())
                        .column("c", DataTypes.BIGINT())
                        .primaryKey("a", "b")
                        .index("a", "b")
                        .build();
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);

        tableName = "table_with_partition_1";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a int, "
                                + " b varchar, "
                                + " c bigint, "
                                + " dt varchar, "
                                + " primary key (a, b, dt) NOT ENFORCED "
                                + ") "
                                + " partitioned by (dt) "
                                + " with ( "
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'a'"
                                + ")",
                        tableName));

        table = (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, tableName));
        expectedSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.STRING().notNull())
                        .column("c", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING().notNull())
                        .primaryKey("a", "b", "dt")
                        .index("a", "b", "dt")
                        .index("a", "dt")
                        .build();
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);

        tableName = "table_with_partition_2";
        tEnv.executeSql(
                String.format(
                        "create table %s ( "
                                + " a int, "
                                + " b varchar, "
                                + " c bigint, "
                                + " dt varchar, "
                                + " primary key (dt, a, b) NOT ENFORCED "
                                + ") "
                                + " partitioned by (dt) "
                                + " with ( "
                                + " 'connector' = 'fluss', "
                                + " 'bucket.key' = 'a'"
                                + ")",
                        tableName));

        table = (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, tableName));
        expectedSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.STRING().notNull())
                        .column("c", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING().notNull())
                        .primaryKey("dt", "a", "b")
                        .index("dt", "a", "b")
                        .index("a", "dt")
                        .build();
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);
    }
}
