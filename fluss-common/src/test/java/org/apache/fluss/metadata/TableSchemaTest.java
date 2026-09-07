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

package org.apache.fluss.metadata;

import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.fluss.metadata.Schema}. */
class TableSchemaTest {

    @Test
    void testAutoIncrementColumnSchema() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .primaryKey("f0")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multiple primary keys are not supported.");

        assertThat(
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.BIGINT())
                                .column("f3", DataTypes.STRING())
                                .enableAutoIncrement("f1")
                                .primaryKey("f0")
                                .build()
                                .getAutoIncrementColumnNames())
                .isEqualTo(Collections.singletonList("f1"));
        assertThat(
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.BIGINT())
                                .column("f3", DataTypes.STRING())
                                .primaryKey("f0")
                                .build()
                                .getAutoIncrementColumnNames())
                .isEmpty();

        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f0")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Auto increment column can not be used as the primary key.");

        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f1")
                                        .enableAutoIncrement("f1")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multiple auto increment columns are not supported yet.");
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f3")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The data type of auto increment column must be INT or BIGINT.");
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f4")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Auto increment column f4 does not exist in table columns [f0, f1, f3].");
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f1")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Auto increment column can only be used in primary-key table.");
    }

    @Test
    void testReassignFieldId() {
        // Schema.Builder.column will reassign field id in flatten order.
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING().copy(false))
                        .column(
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("n0", DataTypes.TINYINT(), 0),
                                        DataTypes.FIELD("n1", DataTypes.STRING(), 1),
                                        DataTypes.FIELD(
                                                "n2",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "m1", DataTypes.TINYINT(), 0)),
                                                2)))
                        .column(
                                "f2",
                                DataTypes.ROW(
                                        DataTypes.FIELD("n0", DataTypes.TINYINT(), 0),
                                        DataTypes.FIELD("n1", DataTypes.STRING(), 1)))
                        .column("f3", DataTypes.STRING())
                        .primaryKey("f0")
                        .build();
        assertThat(schema.getColumnIds()).containsExactly(0, 1, 6, 9);
        RowType expectedType =
                new RowType(
                        true,
                        Arrays.asList(
                                DataTypes.FIELD("f0", DataTypes.STRING().copy(false), 0),
                                DataTypes.FIELD(
                                        "f1",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("n0", DataTypes.TINYINT(), 2),
                                                DataTypes.FIELD("n1", DataTypes.STRING(), 3),
                                                DataTypes.FIELD(
                                                        "n2",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "m1",
                                                                        DataTypes.TINYINT(),
                                                                        5)),
                                                        4)),
                                        1),
                                DataTypes.FIELD(
                                        "f2",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("n0", DataTypes.TINYINT(), 7),
                                                DataTypes.FIELD("n1", DataTypes.STRING(), 8)),
                                        6),
                                DataTypes.FIELD("f3", DataTypes.STRING(), 9)));
        assertThat(schema.getRowType().equalsWithFieldId(expectedType)).isTrue();

        // Schema.Builder.fromColumns won't reassign field id.
        List<Schema.Column> columns =
                Arrays.asList(
                        new Schema.Column("f0", DataTypes.STRING().copy(false), null, 0),
                        new Schema.Column(
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("n0", DataTypes.TINYINT(), 0),
                                        DataTypes.FIELD("n1", DataTypes.STRING(), 1),
                                        DataTypes.FIELD(
                                                "n2",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "m1", DataTypes.TINYINT(), 1)),
                                                2)),
                                null,
                                1),
                        new Schema.Column(
                                "f2",
                                DataTypes.ROW(
                                        DataTypes.FIELD("n0", DataTypes.TINYINT(), 0),
                                        DataTypes.FIELD("n1", DataTypes.STRING(), 1)),
                                null,
                                2));
        assertThatThrownBy(() -> Schema.newBuilder().fromColumns(columns).build())
                .hasMessageContaining(
                        "All field IDs (including nested fields) must be unique. Found 3 unique IDs but expected 9");
        List<Schema.Column> columns2 =
                Arrays.asList(
                        new Schema.Column("f0", DataTypes.STRING().copy(false), null, 0),
                        new Schema.Column(
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("n0", DataTypes.TINYINT(), 6),
                                        DataTypes.FIELD("n1", DataTypes.STRING(), 7),
                                        DataTypes.FIELD(
                                                "n2",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "m1", DataTypes.TINYINT(), 11)),
                                                8)),
                                null,
                                1),
                        new Schema.Column(
                                "f2",
                                DataTypes.ROW(
                                        DataTypes.FIELD("n0", DataTypes.TINYINT(), 9),
                                        DataTypes.FIELD("n1", DataTypes.STRING(), 10)),
                                null,
                                2));
        schema = Schema.newBuilder().fromColumns(columns2).build();
        assertThat(schema.getColumnIds()).containsExactly(0, 1, 2);
        expectedType =
                new RowType(
                        true,
                        Arrays.asList(
                                DataTypes.FIELD("f0", DataTypes.STRING().copy(false), 0),
                                DataTypes.FIELD(
                                        "f1",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("n0", DataTypes.TINYINT(), 6),
                                                DataTypes.FIELD("n1", DataTypes.STRING(), 7),
                                                DataTypes.FIELD(
                                                        "n2",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "m1",
                                                                        DataTypes.TINYINT(),
                                                                        11)),
                                                        8)),
                                        1),
                                DataTypes.FIELD(
                                        "f2",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("n0", DataTypes.TINYINT(), 9),
                                                DataTypes.FIELD("n1", DataTypes.STRING(), 10)),
                                        2)));
        assertThat(schema.getRowType().equalsWithFieldId(expectedType)).isTrue();
    }

    @Test
    void testSchemaBuilderColumnWithAggFunction() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("sum_val", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("max_val", DataTypes.INT(), AggFunctions.MAX())
                        .column("min_val", DataTypes.INT(), AggFunctions.MIN())
                        .column("last_val", DataTypes.STRING(), AggFunctions.LAST_VALUE())
                        .column(
                                "last_non_null",
                                DataTypes.STRING(),
                                AggFunctions.LAST_VALUE_IGNORE_NULLS())
                        .column("first_val", DataTypes.STRING(), AggFunctions.FIRST_VALUE())
                        .column(
                                "first_non_null",
                                DataTypes.STRING(),
                                AggFunctions.FIRST_VALUE_IGNORE_NULLS())
                        .column("listagg_val", DataTypes.STRING(), AggFunctions.LISTAGG())
                        .column("string_agg_val", DataTypes.STRING(), AggFunctions.STRING_AGG())
                        .column("bool_and_val", DataTypes.BOOLEAN(), AggFunctions.BOOL_AND())
                        .column("bool_or_val", DataTypes.BOOLEAN(), AggFunctions.BOOL_OR())
                        .primaryKey("id")
                        .build();

        assertThat(schema.getAggFunction("sum_val").get()).isEqualTo(AggFunctions.SUM());
        assertThat(schema.getAggFunction("max_val").get()).isEqualTo(AggFunctions.MAX());
        assertThat(schema.getAggFunction("min_val").get()).isEqualTo(AggFunctions.MIN());
        assertThat(schema.getAggFunction("last_val").get()).isEqualTo(AggFunctions.LAST_VALUE());
        assertThat(schema.getAggFunction("last_non_null").get())
                .isEqualTo(AggFunctions.LAST_VALUE_IGNORE_NULLS());
        assertThat(schema.getAggFunction("first_val").get()).isEqualTo(AggFunctions.FIRST_VALUE());
        assertThat(schema.getAggFunction("first_non_null").get())
                .isEqualTo(AggFunctions.FIRST_VALUE_IGNORE_NULLS());
        assertThat(schema.getAggFunction("listagg_val").get()).isEqualTo(AggFunctions.LISTAGG());
        assertThat(schema.getAggFunction("string_agg_val").get())
                .isEqualTo(AggFunctions.STRING_AGG());
        assertThat(schema.getAggFunction("bool_and_val").get()).isEqualTo(AggFunctions.BOOL_AND());
        assertThat(schema.getAggFunction("bool_or_val").get()).isEqualTo(AggFunctions.BOOL_OR());
    }

    @Test
    void testSchemaBuilderColumnWithAggFunctionThrowsExceptionForPrimaryKey() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT(), AggFunctions.SUM())
                                        .column("value", DataTypes.BIGINT())
                                        .primaryKey("id")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set aggregation function for primary key column");
    }

    @Test
    void testSchemaEqualityWithAggFunction() {
        Schema schema1 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        Schema schema2 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        Schema schema3 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunctions.MAX())
                        .primaryKey("id")
                        .build();

        assertThat(schema1).isEqualTo(schema2);
        assertThat(schema1).isNotEqualTo(schema3);
    }

    @Test
    void testSchemaFromSchemaPreservesAggFunction() {
        Schema original =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("max_val", DataTypes.INT(), AggFunctions.MAX())
                        .primaryKey("id")
                        .build();

        Schema copied = Schema.newBuilder().fromSchema(original).build();

        assertThat(copied.getAggFunction("value")).isPresent();
        assertThat(copied.getAggFunction("value").get()).isEqualTo(AggFunctions.SUM());
        assertThat(copied.getAggFunction("max_val")).isPresent();
        assertThat(copied.getAggFunction("max_val").get()).isEqualTo(AggFunctions.MAX());
    }

    @Test
    void testSchemaFromSchemaPreservesAutoIncrement() {
        Schema original =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id")
                        .enableAutoIncrement("value")
                        .build();

        Schema copied = Schema.newBuilder().fromSchema(original).build();
        assertThat(copied).isEqualTo(original);
    }

    @Test
    void testListaggWithCustomDelimiter() {
        // Test LISTAGG with default delimiter (comma)
        Schema schemaDefault =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("tags", DataTypes.STRING(), AggFunctions.LISTAGG())
                        .primaryKey("id")
                        .build();

        assertThat(schemaDefault.getAggFunction("tags")).isPresent();
        assertThat(schemaDefault.getAggFunction("tags").get()).isEqualTo(AggFunctions.LISTAGG());
        assertThat(schemaDefault.getAggFunction("tags").get().hasParameters()).isFalse();

        // Test LISTAGG with custom delimiter
        Schema schemaCustom =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("tags", DataTypes.STRING(), AggFunctions.LISTAGG(";"))
                        .column("values", DataTypes.STRING(), AggFunctions.LISTAGG("|"))
                        .column("paths", DataTypes.STRING(), AggFunctions.LISTAGG("/"))
                        .primaryKey("id")
                        .build();

        assertThat(schemaCustom.getAggFunction("tags")).isPresent();
        assertThat(schemaCustom.getAggFunction("tags").get()).isEqualTo(AggFunctions.LISTAGG(";"));
        assertThat(schemaCustom.getAggFunction("tags").get().getParameter("delimiter"))
                .isEqualTo(";");

        assertThat(schemaCustom.getAggFunction("values")).isPresent();
        assertThat(schemaCustom.getAggFunction("values").get())
                .isEqualTo(AggFunctions.LISTAGG("|"));
        assertThat(schemaCustom.getAggFunction("values").get().getParameter("delimiter"))
                .isEqualTo("|");

        assertThat(schemaCustom.getAggFunction("paths")).isPresent();
        assertThat(schemaCustom.getAggFunction("paths").get()).isEqualTo(AggFunctions.LISTAGG("/"));
        assertThat(schemaCustom.getAggFunction("paths").get().getParameter("delimiter"))
                .isEqualTo("/");

        // Test STRING_AGG with custom delimiter
        Schema schemaStringAgg =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("items", DataTypes.STRING(), AggFunctions.STRING_AGG(", "))
                        .primaryKey("id")
                        .build();

        assertThat(schemaStringAgg.getAggFunction("items")).isPresent();
        assertThat(schemaStringAgg.getAggFunction("items").get())
                .isEqualTo(AggFunctions.STRING_AGG(", "));
        assertThat(schemaStringAgg.getAggFunction("items").get().getParameter("delimiter"))
                .isEqualTo(", ");
    }

    @Test
    void testColumnGroupBuilderForms() {
        // Three forms of declaring the same two-group device_logs schema must produce equal
        // schemas: retroactive .columnGroup(name), inline .column(name, type, group), and the
        // .columnGroup(name, block) lambda form.
        Schema retroactive =
                Schema.newBuilder()
                        .column("device_id", DataTypes.STRING())
                        .column("ip", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .column("geo_region", DataTypes.STRING())
                        .columnGroup("enriched_geo")
                        .column("risk_score", DataTypes.DOUBLE())
                        .columnGroup("enriched_risk")
                        .column("risk_classification", DataTypes.STRING())
                        .columnGroup("enriched_risk")
                        .build();

        Schema inline =
                Schema.newBuilder()
                        .column("device_id", DataTypes.STRING())
                        .column("ip", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .column("geo_region", DataTypes.STRING(), "enriched_geo")
                        .column("risk_score", DataTypes.DOUBLE(), "enriched_risk")
                        .column("risk_classification", DataTypes.STRING(), "enriched_risk")
                        .build();

        Schema block =
                Schema.newBuilder()
                        .column("device_id", DataTypes.STRING())
                        .column("ip", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .columnGroup(
                                "enriched_geo", g -> g.column("geo_region", DataTypes.STRING()))
                        .columnGroup(
                                "enriched_risk",
                                g ->
                                        g.column("risk_score", DataTypes.DOUBLE())
                                                .column("risk_classification", DataTypes.STRING()))
                        .build();

        assertThat(inline).isEqualTo(retroactive);
        assertThat(block).isEqualTo(retroactive);
        assertThat(retroactive.getColumnGroupNames())
                .containsExactlyInAnyOrder("enriched_geo", "enriched_risk");
        assertThat(retroactive.getColumnGroups().get("enriched_geo")).containsExactly(3);
        assertThat(retroactive.getColumnGroups().get("enriched_risk")).containsExactly(4, 5);
        assertThat(retroactive.getDefaultGroupColumnIndices()).containsExactly(0, 1, 2);
    }

    @Test
    void testColumnGroupBlockPreservesExplicitGroup() {
        // An explicit .column(name, type, "other") inside a block keeps its explicit group.
        Schema schema =
                Schema.newBuilder()
                        .column("device_id", DataTypes.STRING())
                        .columnGroup(
                                "enriched_geo",
                                g ->
                                        g.column("geo_region", DataTypes.STRING())
                                                .column(
                                                        "risk_score",
                                                        DataTypes.DOUBLE(),
                                                        "enriched_risk"))
                        .build();

        assertThat(schema.getColumns().get(1).getColumnGroup()).hasValue("enriched_geo");
        assertThat(schema.getColumns().get(2).getColumnGroup()).hasValue("enriched_risk");
    }

    @Test
    void testColumnGroupBlockDoesNotAffectColumnsAddedBefore() {
        Schema schema =
                Schema.newBuilder()
                        .column("device_id", DataTypes.STRING())
                        .column("ip", DataTypes.STRING())
                        .columnGroup(
                                "enriched_geo", g -> g.column("geo_region", DataTypes.STRING()))
                        .build();

        assertThat(schema.getColumns().get(0).getColumnGroup()).isEmpty();
        assertThat(schema.getColumns().get(1).getColumnGroup()).isEmpty();
        assertThat(schema.getColumns().get(2).getColumnGroup()).hasValue("enriched_geo");
    }

    @Test
    void testColumnGroupInlineAndBlockNullChecks() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING(), (String) null)
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Column group name must not be null.");

        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .columnGroup(null, g -> g.column("f0", DataTypes.STRING()))
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Column group name must not be null.");

        assertThatThrownBy(() -> Schema.newBuilder().columnGroup("g", null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Column group block must not be null.");
    }
}
