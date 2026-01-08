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

package org.apache.fluss.utils.json;

import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SchemaJsonSerde}. */
public class SchemaJsonSerdeTest extends JsonSerdeTestBase<Schema> {

    static final Schema SCHEMA_0 =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .column("c", DataTypes.CHAR(10))
                    .withComment("c is third column")
                    .primaryKey("a", "c")
                    .build();

    static final Schema SCHEMA_1 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("a", DataTypes.INT()),
                                    new Schema.Column(
                                            "b", DataTypes.STRING(), "b is second column"),
                                    new Schema.Column(
                                            "c", DataTypes.TIMESTAMP(), "c is third column")))
                    .primaryKey(Collections.singletonList("a"))
                    .build();

    static final Schema SCHEMA_2 = Schema.newBuilder().fromSchema(SCHEMA_1).build();

    static final Schema SCHEMA_3 =
            Schema.newBuilder()
                    .column("a", DataTypes.BIGINT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .column("c", DataTypes.TIMESTAMP(6))
                    .withComment("c is third column")
                    .build();

    static final Schema SCHEMA_4 =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.INT())
                    .withComment("b is second column")
                    .column("c", DataTypes.CHAR(10))
                    .withComment("c is third column")
                    .primaryKey("a", "c")
                    .enableAutoIncrement("b")
                    .build();

    static final Schema SCHEMA_5 =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column(
                            "b",
                            DataTypes.ROW(
                                    DataTypes.FIELD("c", DataTypes.INT(), "a is first column", 0),
                                    DataTypes.FIELD("d", DataTypes.INT(), "a is first column", 1)))
                    .withComment("b is second column")
                    .build();

    static final String SCHEMA_JSON_0 =
            "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"a is first column\",\"id\":0},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\",\"id\":1},{\"name\":\"c\",\"data_type\":{\"type\":\"CHAR\",\"nullable\":false,\"length\":10},\"comment\":\"c is third column\",\"id\":2}],\"primary_key\":[\"a\",\"c\"],\"highest_field_id\":2}";
    static final String SCHEMA_JSON_1 =
            "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"id\":0},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\",\"id\":1},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\",\"id\":2}],\"primary_key\":[\"a\"],\"highest_field_id\":2}";
    // shouldn't contain primary_key fields
    static final String SCHEMA_JSON_3 =
            "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"BIGINT\"},\"comment\":\"a is first column\",\"id\":0},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\",\"id\":1},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\",\"id\":2}],\"highest_field_id\":2}";

    static final String SCHEMA_JSON_4 =
            "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"a is first column\",\"id\":0},{\"name\":\"b\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"b is second column\",\"id\":1},{\"name\":\"c\",\"data_type\":{\"type\":\"CHAR\",\"nullable\":false,\"length\":10},\"comment\":\"c is third column\",\"id\":2}],\"primary_key\":[\"a\",\"c\"],\"auto_increment_column\":[\"b\"],\"highest_field_id\":2}";

    static final String SCHEMA_JSON_5 =
            "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\"},\"comment\":\"a is first column\",\"id\":0},{\"name\":\"b\",\"data_type\":{\"type\":\"ROW\",\"fields\":[{\"name\":\"c\",\"field_type\":{\"type\":\"INTEGER\"},\"description\":\"a is first column\",\"field_id\":2},{\"name\":\"d\",\"field_type\":{\"type\":\"INTEGER\"},\"description\":\"a is first column\",\"field_id\":3}]},\"comment\":\"b is second column\",\"id\":1}],\"highest_field_id\":3}";

    static final Schema SCHEMA_WITH_AGG =
            Schema.newBuilder()
                    .column("product_id", DataTypes.BIGINT().copy(false))
                    .column("total_sales", DataTypes.BIGINT(), AggFunctions.SUM())
                    .column("max_price", DataTypes.DECIMAL(10, 2), AggFunctions.MAX())
                    .column(
                            "last_update_time",
                            DataTypes.TIMESTAMP(),
                            AggFunctions.LAST_VALUE_IGNORE_NULLS())
                    .column("tags", DataTypes.STRING(), AggFunctions.LISTAGG(";"))
                    .column("categories", DataTypes.STRING(), AggFunctions.STRING_AGG("|"))
                    .column("labels", DataTypes.STRING(), AggFunctions.LISTAGG())
                    .primaryKey("product_id")
                    .build();

    static final String SCHEMA_JSON_WITH_AGG =
            "{\"version\":1,\"columns\":[{\"name\":\"product_id\",\"data_type\":{\"type\":\"BIGINT\",\"nullable\":false},\"id\":0},{\"name\":\"total_sales\",\"data_type\":{\"type\":\"BIGINT\"},\"agg_function\":{\"type\":\"sum\"},\"id\":1},{\"name\":\"max_price\",\"data_type\":{\"type\":\"DECIMAL\",\"precision\":10,\"scale\":2},\"agg_function\":{\"type\":\"max\"},\"id\":2},{\"name\":\"last_update_time\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"agg_function\":{\"type\":\"last_value_ignore_nulls\"},\"id\":3},{\"name\":\"tags\",\"data_type\":{\"type\":\"STRING\"},\"agg_function\":{\"type\":\"listagg\",\"parameters\":{\"delimiter\":\";\"}},\"id\":4},{\"name\":\"categories\",\"data_type\":{\"type\":\"STRING\"},\"agg_function\":{\"type\":\"string_agg\",\"parameters\":{\"delimiter\":\"|\"}},\"id\":5},{\"name\":\"labels\",\"data_type\":{\"type\":\"STRING\"},\"agg_function\":{\"type\":\"listagg\"},\"id\":6}],\"primary_key\":[\"product_id\"],\"highest_field_id\":6}";

    SchemaJsonSerdeTest() {
        super(SchemaJsonSerde.INSTANCE);
    }

    @Override
    protected void assertEquals(Schema actual, Schema expected) {
        assertThat(actual).isEqualTo(expected);
        // compare field ids.
        assertThat(actual.getRowType().equalsWithFieldId(expected.getRowType())).isTrue();
    }

    @Override
    protected Schema[] createObjects() {
        return new Schema[] {
            SCHEMA_0, SCHEMA_1, SCHEMA_2, SCHEMA_3, SCHEMA_4, SCHEMA_5, SCHEMA_WITH_AGG
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            SCHEMA_JSON_0,
            SCHEMA_JSON_1,
            SCHEMA_JSON_1,
            SCHEMA_JSON_3,
            SCHEMA_JSON_4,
            SCHEMA_JSON_5,
            SCHEMA_JSON_WITH_AGG
        };
    }

    @Test
    void testCompatibilityFromJsonLackOfColumnId() {
        String[] jsons = jsonLackOfColumnId();
        Schema[] expectedSchema = new Schema[] {SCHEMA_0, SCHEMA_1, SCHEMA_2, SCHEMA_3, SCHEMA_5};
        for (int i = 0; i < jsons.length; i++) {
            assertEquals(
                    JsonSerdeUtils.readValue(
                            jsons[i].getBytes(StandardCharsets.UTF_8), SchemaJsonSerde.INSTANCE),
                    expectedSchema[i]);
        }
    }

    protected String[] jsonLackOfColumnId() {
        String oldSchemaJson1 =
                "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"a is first column\"},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\"},{\"name\":\"c\",\"data_type\":{\"type\":\"CHAR\",\"nullable\":false,\"length\":10},\"comment\":\"c is third column\"}],\"primary_key\":[\"a\",\"c\"]}";

        String oldSchemaJson2 =
                "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false}},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\"},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\"}],\"primary_key\":[\"a\"]}";

        // shouldn't contain primary_key fields
        String oldSchemaJson3 =
                "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"BIGINT\"},\"comment\":\"a is first column\"},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\"},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\"}]}";

        return new String[] {oldSchemaJson1, oldSchemaJson2, oldSchemaJson2, oldSchemaJson3};
    }
}
