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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.testutils.common.MultiVersionTest;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for catalog in Flink 2.2. */
public class Flink22CatalogITCase extends FlinkCatalogITCase {

    @Test
    @MultiVersionTest
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

    @Override
    protected void addDefaultIndexKey(Schema.Builder schemaBuilder) {
        super.addDefaultIndexKey(schemaBuilder);

        Schema currentSchema = schemaBuilder.build();
        currentSchema.getPrimaryKey().ifPresent(pk -> schemaBuilder.index(pk.getColumnNames()));
    }

    @Test
    @MultiVersionTest
    void testCreateTableWithNestedNotNull() throws Exception {
        // Flink before 2.2 drops nested NOT NULL constraints during DDL resolution (FLINK-20539).
        String tableName = "nested_not_null_table";
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " headers ARRAY<ROW<name STRING NOT NULL, header_value BYTES>>,"
                                + " address ROW<city STRING NOT NULL, zip STRING>,"
                                + " scores MAP<STRING, ROW<score INT NOT NULL>>"
                                + ")",
                        tableName));

        Schema expectedSchema =
                Schema.newBuilder()
                        .column(
                                "headers",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "name", DataTypes.STRING().notNull()),
                                                DataTypes.FIELD(
                                                        "header_value", DataTypes.BYTES()))))
                        .column(
                                "address",
                                DataTypes.ROW(
                                        DataTypes.FIELD("city", DataTypes.STRING().notNull()),
                                        DataTypes.FIELD("zip", DataTypes.STRING())))
                        .column(
                                "scores",
                                // map keys are always non-nullable in Fluss
                                DataTypes.MAP(
                                        DataTypes.STRING().notNull(),
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "score", DataTypes.INT().notNull()))))
                        .build();
        CatalogTable table = (CatalogTable) catalog.getTable(new ObjectPath(DEFAULT_DB, tableName));
        assertThat(table.getUnresolvedSchema()).isEqualTo(expectedSchema);

        RowType expectedRowType =
                org.apache.fluss.types.DataTypes.ROW(
                        org.apache.fluss.types.DataTypes.FIELD(
                                "headers",
                                org.apache.fluss.types.DataTypes.ARRAY(
                                        org.apache.fluss.types.DataTypes.ROW(
                                                org.apache.fluss.types.DataTypes.FIELD(
                                                        "name",
                                                        org.apache.fluss.types.DataTypes.STRING()
                                                                .copy(false)),
                                                org.apache.fluss.types.DataTypes.FIELD(
                                                        "header_value",
                                                        org.apache.fluss.types.DataTypes
                                                                .BYTES())))),
                        org.apache.fluss.types.DataTypes.FIELD(
                                "address",
                                org.apache.fluss.types.DataTypes.ROW(
                                        org.apache.fluss.types.DataTypes.FIELD(
                                                "city",
                                                org.apache.fluss.types.DataTypes.STRING()
                                                        .copy(false)),
                                        org.apache.fluss.types.DataTypes.FIELD(
                                                "zip", org.apache.fluss.types.DataTypes.STRING()))),
                        org.apache.fluss.types.DataTypes.FIELD(
                                "scores",
                                org.apache.fluss.types.DataTypes.MAP(
                                        org.apache.fluss.types.DataTypes.STRING().copy(false),
                                        org.apache.fluss.types.DataTypes.ROW(
                                                org.apache.fluss.types.DataTypes.FIELD(
                                                        "score",
                                                        org.apache.fluss.types.DataTypes.INT()
                                                                .copy(false))))));
        try (Connection conn =
                ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
            TableInfo tableInfo =
                    conn.getAdmin().getTableInfo(TablePath.of(DEFAULT_DB, tableName)).get();
            assertThat(tableInfo.getRowType()).isEqualTo(expectedRowType);
        }
    }
}
