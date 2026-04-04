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

package org.apache.fluss.types.variant;

import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.StringType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ShreddingSchema}. */
class ShreddingSchemaTest {

    @Test
    void testShreddedColumnName() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "event",
                        Arrays.asList(
                                new ShreddedField("age", new IntType(), 5),
                                new ShreddedField("name", new StringType(), 6)));

        assertThat(schema.shreddedColumnName("age")).isEqualTo("$event.age");
        assertThat(schema.shreddedColumnName("name")).isEqualTo("$event.name");
        assertThat(schema.shreddedColumnName("address.city")).isEqualTo("$event.address.city");
    }

    @Test
    void testExtractFieldPath() {
        ShreddingSchema schema = new ShreddingSchema("event", Collections.emptyList());

        assertThat(schema.extractFieldPath("$event.age")).isEqualTo("age");
        assertThat(schema.extractFieldPath("$event.address.city")).isEqualTo("address.city");
        assertThat(schema.extractFieldPath("$other.age")).isNull();
        assertThat(schema.extractFieldPath("invalid")).isNull();
    }

    @Test
    void testPropertyKey() {
        ShreddingSchema schema = new ShreddingSchema("event", Collections.emptyList());
        assertThat(schema.propertyKey()).isEqualTo("variant.shredding.schema.event");
    }

    @Test
    void testGetters() {
        ShreddedField field1 = new ShreddedField("age", new IntType(), 5);
        ShreddedField field2 = new ShreddedField("name", new StringType(), 6);
        ShreddingSchema schema = new ShreddingSchema("data", Arrays.asList(field1, field2));

        assertThat(schema.getVariantColumnName()).isEqualTo("data");
        assertThat(schema.getFields()).hasSize(2);
        assertThat(schema.getFields().get(0).getFieldPath()).isEqualTo("age");
        assertThat(schema.getFields().get(1).getFieldPath()).isEqualTo("name");
    }

    @Test
    void testToJsonFromJsonRoundtrip() {
        ShreddingSchema original =
                new ShreddingSchema(
                        "event",
                        Arrays.asList(
                                new ShreddedField("age", DataTypes.INT(), 5),
                                new ShreddedField("name", DataTypes.STRING(), 6),
                                new ShreddedField("active", DataTypes.BOOLEAN(), 7)));

        String json = original.toJson();
        assertThat(json).contains("\"variant_column\":\"event\"");
        assertThat(json).contains("\"path\":\"age\"");
        assertThat(json).contains("\"path\":\"name\"");
        assertThat(json).contains("\"path\":\"active\"");

        ShreddingSchema deserialized = ShreddingSchema.fromJson(json);
        assertThat(deserialized.getVariantColumnName()).isEqualTo("event");
        assertThat(deserialized.getFields()).hasSize(3);

        assertThat(deserialized.getFields().get(0).getFieldPath()).isEqualTo("age");
        assertThat(deserialized.getFields().get(0).getColumnId()).isEqualTo(5);
        assertThat(deserialized.getFields().get(1).getFieldPath()).isEqualTo("name");
        assertThat(deserialized.getFields().get(2).getFieldPath()).isEqualTo("active");
    }

    @Test
    void testToJsonFromJsonEmptyFields() {
        ShreddingSchema original = new ShreddingSchema("data", Collections.emptyList());

        String json = original.toJson();
        ShreddingSchema deserialized = ShreddingSchema.fromJson(json);

        assertThat(deserialized.getVariantColumnName()).isEqualTo("data");
        assertThat(deserialized.getFields()).isEmpty();
    }

    @Test
    void testEqualsAndHashCode() {
        ShreddingSchema s1 =
                new ShreddingSchema(
                        "event", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 5)));
        ShreddingSchema s2 =
                new ShreddingSchema(
                        "event", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 5)));
        ShreddingSchema s3 =
                new ShreddingSchema(
                        "other", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 5)));

        assertThat(s1).isEqualTo(s2);
        assertThat(s1.hashCode()).isEqualTo(s2.hashCode());
        assertThat(s1).isNotEqualTo(s3);
    }

    @Test
    void testFieldsListIsUnmodifiable() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "event", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 5)));

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> schema.getFields().add(new ShreddedField("x", DataTypes.INT(), 9)))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
