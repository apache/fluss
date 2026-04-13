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

package org.apache.fluss.row.arrow;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.arrow.vectors.ShreddedVariantColumnVector;
import org.apache.fluss.row.arrow.writers.ArrowShreddedVariantWriter;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.variant.ShreddedField;
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.types.variant.Variant;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip tests for {@link ArrowShreddedVariantWriter} and {@link ShreddedVariantColumnVector}.
 * Verifies that writing a Variant through shredding and reading it back produces identical results.
 *
 * <p>The new design uses StructVector layout: Struct{metadata, value, typed_value: Struct{
 * fieldName: Struct{value, typed_value}, ...}}.
 */
class ShreddedVariantRoundTripTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    /**
     * Creates a StructVector with the full shredding layout and returns the VectorSchemaRoot for
     * lifecycle management.
     */
    private VectorSchemaRoot createVariantRoot(ShreddingSchema shreddingSchema) {
        Field variantField = ArrowUtils.toVariantFieldWithShredding("data", shreddingSchema);
        Schema schema = new Schema(Collections.singletonList(variantField));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        // Allocate all vectors recursively
        for (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector fv :
                root.getFieldVectors()) {
            fv.allocateNew();
        }
        return root;
    }

    // --------------------------------------------------------------------------------------------
    // All fields shredded (null residual)
    // --------------------------------------------------------------------------------------------

    @Test
    void testAllFieldsShredded() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data",
                        Arrays.asList(
                                new ShreddedField("age", DataTypes.INT()),
                                new ShreddedField("name", DataTypes.STRING())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            Variant input = Variant.fromJson("{\"age\":25,\"name\":\"Alice\"}");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            assertThat(reader.isNullAt(0)).isFalse();

            Variant result = reader.getVariant(0);
            assertThat(result).isNotNull();
            assertThat(result.isObject()).isTrue();
            assertThat(result.getFieldByName("age").getInt()).isEqualTo(25);
            assertThat(result.getFieldByName("name").getString()).isEqualTo("Alice");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Partial shredding (some fields in residual)
    // --------------------------------------------------------------------------------------------

    @Test
    void testPartialShredding() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            Variant input = Variant.fromJson("{\"age\":30,\"name\":\"Bob\",\"active\":true}");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            Variant result = reader.getVariant(0);
            assertThat(result.getFieldByName("age").getInt()).isEqualTo(30);
            assertThat(result.getFieldByName("name").getString()).isEqualTo("Bob");
            assertThat(result.getFieldByName("active").getBoolean()).isTrue();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Nested objects in residual
    // --------------------------------------------------------------------------------------------

    @Test
    void testNestedObjectInResidual() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            Variant input =
                    Variant.fromJson(
                            "{\"age\":28,\"address\":{\"city\":\"Beijing\",\"zip\":\"100000\"}}");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            Variant result = reader.getVariant(0);
            assertThat(result.getFieldByName("age").getInt()).isEqualTo(28);
            Variant address = result.getFieldByName("address");
            assertThat(address).isNotNull();
            assertThat(address.isObject()).isTrue();
            assertThat(address.getFieldByName("city").getString()).isEqualTo("Beijing");
            assertThat(address.getFieldByName("zip").getString()).isEqualTo("100000");
        }
    }

    @Test
    void testDeeplyNestedObjectInResidual() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("name", DataTypes.STRING())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            Variant input =
                    Variant.fromJson(
                            "{\"name\":\"Charlie\",\"info\":{\"contact\":{\"email\":\"a@b.com\"}}}");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            Variant result = reader.getVariant(0);
            assertThat(result.getFieldByName("name").getString()).isEqualTo("Charlie");
            Variant info = result.getFieldByName("info");
            assertThat(info.isObject()).isTrue();
            Variant contact = info.getFieldByName("contact");
            assertThat(contact.isObject()).isTrue();
            assertThat(contact.getFieldByName("email").getString()).isEqualTo("a@b.com");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Non-object variant (scalar value)
    // --------------------------------------------------------------------------------------------

    @Test
    void testScalarVariantGoesToResidual() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            Variant input = Variant.ofString("just a string");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            Variant result = reader.getVariant(0);
            assertThat(result.getString()).isEqualTo("just a string");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Null variant
    // --------------------------------------------------------------------------------------------

    @Test
    void testNullVariant() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            writer.write(0, GenericRow.of((Variant) null), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            assertThat(reader.isNullAt(0)).isTrue();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Type mismatch — field goes to per-field value fallback
    // --------------------------------------------------------------------------------------------

    @Test
    void testTypeMismatchKeepsInPerFieldValue() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            // "age" is a STRING — type mismatch for INT shredded column
            Variant input = Variant.fromJson("{\"age\":\"not-a-number\",\"name\":\"Dave\"}");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            Variant result = reader.getVariant(0);
            assertThat(result.getFieldByName("age").getString()).isEqualTo("not-a-number");
            assertThat(result.getFieldByName("name").getString()).isEqualTo("Dave");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Multiple rows with mixed data
    // --------------------------------------------------------------------------------------------

    @Test
    void testMultipleRows() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data",
                        Arrays.asList(
                                new ShreddedField("id", DataTypes.BIGINT()),
                                new ShreddedField("name", DataTypes.STRING())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);

            // Row 0: all fields present
            Variant v0 = Variant.fromJson("{\"id\":1,\"name\":\"Alice\",\"extra\":\"x\"}");
            writer.write(0, GenericRow.of(v0), 0, true);

            // Row 1: missing "name"
            Variant v1 = Variant.fromJson("{\"id\":2,\"extra\":\"y\"}");
            writer.write(1, GenericRow.of(v1), 0, true);

            // Row 2: null variant
            writer.write(2, GenericRow.of((Variant) null), 0, true);

            // Row 3: nested object with all shredded fields
            Variant v3 =
                    Variant.fromJson(
                            "{\"id\":3,\"name\":\"Carol\",\"meta\":{\"tag\":\"important\"}}");
            writer.write(3, GenericRow.of(v3), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            // Row 0
            Variant r0 = reader.getVariant(0);
            assertThat(r0.getFieldByName("id").getLong()).isEqualTo(1L);
            assertThat(r0.getFieldByName("name").getString()).isEqualTo("Alice");
            assertThat(r0.getFieldByName("extra").getString()).isEqualTo("x");

            // Row 1
            Variant r1 = reader.getVariant(1);
            assertThat(r1.getFieldByName("id").getLong()).isEqualTo(2L);
            assertThat(r1.getFieldByName("extra").getString()).isEqualTo("y");
            assertThat(r1.getFieldByName("name")).isNull();

            // Row 2
            assertThat(reader.isNullAt(2)).isTrue();

            // Row 3
            Variant r3 = reader.getVariant(3);
            assertThat(r3.getFieldByName("id").getLong()).isEqualTo(3L);
            assertThat(r3.getFieldByName("name").getString()).isEqualTo("Carol");
            Variant meta = r3.getFieldByName("meta");
            assertThat(meta.isObject()).isTrue();
            assertThat(meta.getFieldByName("tag").getString()).isEqualTo("important");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Explicit null values in fields
    // --------------------------------------------------------------------------------------------

    @Test
    void testExplicitNullFieldValueAllShredded() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data",
                        Arrays.asList(
                                new ShreddedField("age", DataTypes.BIGINT()),
                                new ShreddedField("name", DataTypes.STRING())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            Variant input = Variant.fromJson("{\"age\":null,\"name\":\"Alice\"}");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            Variant result = reader.getVariant(0);
            assertThat(result).isNotNull();
            assertThat(result.isObject()).isTrue();

            // "age" should still exist as an explicit null
            Variant ageField = result.getFieldByName("age");
            assertThat(ageField).isNotNull();
            assertThat(ageField.isNull()).isTrue();

            assertThat(result.getFieldByName("name").getString()).isEqualTo("Alice");
        }
    }

    @Test
    void testExplicitNullFieldValuePartialShredding() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.BIGINT())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            Variant input = Variant.fromJson("{\"age\":null,\"name\":\"Bob\",\"extra\":\"x\"}");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            Variant result = reader.getVariant(0);
            assertThat(result).isNotNull();

            Variant ageField = result.getFieldByName("age");
            assertThat(ageField).isNotNull();
            assertThat(ageField.isNull()).isTrue();
            assertThat(result.getFieldByName("name").getString()).isEqualTo("Bob");
            assertThat(result.getFieldByName("extra").getString()).isEqualTo("x");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Empty object {}
    // --------------------------------------------------------------------------------------------

    @Test
    void testEmptyObjectRoundTrip() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            Variant input = Variant.fromJson("{}");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            assertThat(reader.isNullAt(0)).isFalse();

            Variant result = reader.getVariant(0);
            assertThat(result).isNotNull();
            assertThat(result.isObject()).isTrue();
            assertThat(result.getFieldByName("age")).isNull();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Field missing in input
    // --------------------------------------------------------------------------------------------

    @Test
    void testFieldNotPresentInInput() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data",
                        Arrays.asList(
                                new ShreddedField("age", DataTypes.INT()),
                                new ShreddedField("name", DataTypes.STRING())));

        try (VectorSchemaRoot root = createVariantRoot(schema)) {
            StructVector variantVec = (StructVector) root.getVector(0);

            ArrowShreddedVariantWriter writer = new ArrowShreddedVariantWriter(variantVec, schema);
            Variant input = Variant.fromJson("{\"age\":42}");
            writer.write(0, GenericRow.of(input), 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(variantVec, schema);

            Variant result = reader.getVariant(0);
            assertThat(result.getFieldByName("age").getInt()).isEqualTo(42);
            assertThat(result.getFieldByName("name")).isNull();
        }
    }
}
