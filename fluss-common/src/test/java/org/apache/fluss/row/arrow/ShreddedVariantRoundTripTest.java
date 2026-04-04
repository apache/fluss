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
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.BigIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ValueVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.variant.ShreddedField;
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.types.variant.Variant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip tests for {@link ArrowShreddedVariantWriter} and {@link ShreddedVariantColumnVector}.
 * Verifies that writing a Variant through shredding and reading it back produces identical JSON
 * output.
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

    // --------------------------------------------------------------------------------------------
    // All fields shredded (null residual)
    // --------------------------------------------------------------------------------------------

    @Test
    void testAllFieldsShredded() {
        // Schema: shred "age" (INT) and "name" (STRING)
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data",
                        Arrays.asList(
                                new ShreddedField("age", DataTypes.INT(), 1),
                                new ShreddedField("name", DataTypes.STRING(), 2)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                IntVector ageVec = new IntVector("$data.age", allocator);
                VarCharVector nameVec = new VarCharVector("$data.name", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();
            nameVec.allocateNew();

            DataType[] types = {DataTypes.INT(), DataTypes.STRING()};
            FieldVector[] shreddedVecs = {ageVec, nameVec};

            // Write
            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            Variant input = Variant.fromJson("{\"age\":25,\"name\":\"Alice\"}");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            // Read
            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec, nameVec}, schema, types);

            // Residual should be null since all fields are shredded
            assertThat(residualVec.isNull(0)).isTrue();
            assertThat(reader.isNullAt(0)).isFalse();

            Variant result = reader.getVariant(0);
            assertThat(result).isNotNull();
            assertThat(result.isObject()).isTrue();

            // Verify field values - use result directly (not re-parsed through JSON
            // which may change compact encoding, e.g. INT8 for small numbers)
            assertThat(result.getFieldByName("age").getInt()).isEqualTo(25);
            assertThat(result.getFieldByName("name").getString()).isEqualTo("Alice");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Partial shredding (some fields in residual)
    // --------------------------------------------------------------------------------------------

    @Test
    void testPartialShredding() {
        // Schema: shred "age" (INT) only; "name" and "active" stay in residual
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 1)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                IntVector ageVec = new IntVector("$data.age", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();

            DataType[] types = {DataTypes.INT()};
            FieldVector[] shreddedVecs = {ageVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            Variant input = Variant.fromJson("{\"age\":30,\"name\":\"Bob\",\"active\":true}");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec}, schema, types);

            // Residual should NOT be null (name and active are in residual)
            assertThat(residualVec.isNull(0)).isFalse();

            Variant result = reader.getVariant(0);
            assertThat(result.getFieldByName("age").getInt()).isEqualTo(30);
            assertThat(result.getFieldByName("name").getString()).isEqualTo("Bob");
            assertThat(result.getFieldByName("active").getBoolean()).isTrue();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Nested objects in residual (Bug1 fix: metadata must preserve nested field names)
    // --------------------------------------------------------------------------------------------

    @Test
    void testNestedObjectInResidual() {
        // Schema: shred "age" (INT); "address" (nested object) stays in residual
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 1)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                IntVector ageVec = new IntVector("$data.age", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();

            DataType[] types = {DataTypes.INT()};
            FieldVector[] shreddedVecs = {ageVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            // Input has a nested object "address" with sub-fields "city" and "zip"
            Variant input =
                    Variant.fromJson(
                            "{\"age\":28,\"address\":{\"city\":\"Beijing\",\"zip\":\"100000\"}}");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec}, schema, types);

            Variant result = reader.getVariant(0);
            assertThat(result).isNotNull();

            // Verify the nested object is preserved correctly
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
        // Schema: shred "name" (STRING); deeply nested object stays in residual
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("name", DataTypes.STRING(), 1)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                VarCharVector nameVec = new VarCharVector("$data.name", allocator)) {

            residualVec.allocateNew();
            nameVec.allocateNew();

            DataType[] types = {DataTypes.STRING()};
            FieldVector[] shreddedVecs = {nameVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            // Input has deeply nested: info.contact.email
            Variant input =
                    Variant.fromJson(
                            "{\"name\":\"Charlie\",\"info\":{\"contact\":{\"email\":\"a@b.com\"}}}");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {nameVec}, schema, types);

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
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 1)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                IntVector ageVec = new IntVector("$data.age", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();

            DataType[] types = {DataTypes.INT()};
            FieldVector[] shreddedVecs = {ageVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            // Write a scalar (non-object) Variant
            Variant input = Variant.ofString("just a string");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec}, schema, types);

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
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 1)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                IntVector ageVec = new IntVector("$data.age", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();

            DataType[] types = {DataTypes.INT()};
            FieldVector[] shreddedVecs = {ageVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            // Write a null Variant
            GenericRow nullRow = GenericRow.of((Variant) null);
            writer.write(0, nullRow, 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec}, schema, types);

            assertThat(reader.isNullAt(0)).isTrue();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Type mismatch — field stays in residual
    // --------------------------------------------------------------------------------------------

    @Test
    void testTypeMismatchKeepsInResidual() {
        // Schema: shred "age" as INT
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 1)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                IntVector ageVec = new IntVector("$data.age", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();

            DataType[] types = {DataTypes.INT()};
            FieldVector[] shreddedVecs = {ageVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            // "age" is a STRING in this row — type mismatch, should stay in residual
            Variant input = Variant.fromJson("{\"age\":\"not-a-number\",\"name\":\"Dave\"}");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec}, schema, types);

            // Shredded column should be null (type mismatch)
            assertThat(ageVec.isNull(0)).isTrue();
            // Residual should contain both "age" and "name"
            assertThat(residualVec.isNull(0)).isFalse();

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
        // Use BIGINT and STRING types which JSON parser encodes compatibly
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data",
                        Arrays.asList(
                                new ShreddedField("id", DataTypes.BIGINT(), 1),
                                new ShreddedField("name", DataTypes.STRING(), 2)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                BigIntVector idVec = new BigIntVector("$data.id", allocator);
                VarCharVector nameVec = new VarCharVector("$data.name", allocator)) {

            residualVec.allocateNew();
            idVec.allocateNew();
            nameVec.allocateNew();

            DataType[] types = {DataTypes.BIGINT(), DataTypes.STRING()};
            FieldVector[] shreddedVecs = {idVec, nameVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

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
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {idVec, nameVec}, schema, types);

            // Row 0: all fields present
            Variant r0 = reader.getVariant(0);
            assertThat(r0.getFieldByName("id").getLong()).isEqualTo(1L);
            assertThat(r0.getFieldByName("name").getString()).isEqualTo("Alice");
            assertThat(r0.getFieldByName("extra").getString()).isEqualTo("x");

            // Row 1: missing "name"
            Variant r1 = reader.getVariant(1);
            assertThat(r1.getFieldByName("id").getLong()).isEqualTo(2L);
            assertThat(r1.getFieldByName("extra").getString()).isEqualTo("y");
            assertThat(r1.getFieldByName("name")).isNull();

            // Row 2: null
            assertThat(reader.isNullAt(2)).isTrue();

            // Row 3: nested object preserved
            Variant r3 = reader.getVariant(3);
            assertThat(r3.getFieldByName("id").getLong()).isEqualTo(3L);
            assertThat(r3.getFieldByName("name").getString()).isEqualTo("Carol");
            Variant meta = r3.getFieldByName("meta");
            assertThat(meta.isObject()).isTrue();
            assertThat(meta.getFieldByName("tag").getString()).isEqualTo("important");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Explicit null values in fields (Bug6: must NOT be silently dropped)
    // --------------------------------------------------------------------------------------------

    @Test
    void testExplicitNullFieldValueAllShredded() {
        // Schema: shred "age" (BIGINT) and "name" (STRING)
        // Input: {"age": null, "name": "Alice"} — "age" is explicitly null
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data",
                        Arrays.asList(
                                new ShreddedField("age", DataTypes.BIGINT(), 1),
                                new ShreddedField("name", DataTypes.STRING(), 2)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                BigIntVector ageVec = new BigIntVector("$data.age", allocator);
                VarCharVector nameVec = new VarCharVector("$data.name", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();
            nameVec.allocateNew();

            DataType[] types = {DataTypes.BIGINT(), DataTypes.STRING()};
            FieldVector[] shreddedVecs = {ageVec, nameVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            Variant input = Variant.fromJson("{\"age\":null,\"name\":\"Alice\"}");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec, nameVec}, schema, types);

            Variant result = reader.getVariant(0);
            assertThat(result).isNotNull();
            assertThat(result.isObject()).isTrue();

            // "age" should still exist as an explicit null — NOT be silently dropped
            Variant ageField = result.getFieldByName("age");
            assertThat(ageField).isNotNull();
            assertThat(ageField.isNull()).isTrue();

            // "name" should be properly shredded and read back
            assertThat(result.getFieldByName("name").getString()).isEqualTo("Alice");
        }
    }

    @Test
    void testExplicitNullFieldValuePartialShredding() {
        // Schema: shred "age" (BIGINT); "name" stays in residual
        // Input: {"age": null, "name": "Bob", "extra": "x"}
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.BIGINT(), 1)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                BigIntVector ageVec = new BigIntVector("$data.age", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();

            DataType[] types = {DataTypes.BIGINT()};
            FieldVector[] shreddedVecs = {ageVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            Variant input = Variant.fromJson("{\"age\":null,\"name\":\"Bob\",\"extra\":\"x\"}");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            // "age" is null → must NOT be shredded, stays in residual
            assertThat(ageVec.isNull(0)).isTrue();
            // Residual should contain age (null), name, extra
            assertThat(residualVec.isNull(0)).isFalse();

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec}, schema, types);

            Variant result = reader.getVariant(0);
            assertThat(result).isNotNull();

            // All fields preserved including the explicit null
            Variant ageField = result.getFieldByName("age");
            assertThat(ageField).isNotNull();
            assertThat(ageField.isNull()).isTrue();
            assertThat(result.getFieldByName("name").getString()).isEqualTo("Bob");
            assertThat(result.getFieldByName("extra").getString()).isEqualTo("x");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Empty object {} — must NOT be treated as SQL NULL (data corruption regression test)
    // --------------------------------------------------------------------------------------------

    @Test
    void testEmptyObjectRoundTrip() {
        // Before the fix: numFields == 0 caused the "all shredded" branch to fire vacuously,
        // writing null to the residual vector.  The reader then returned isNullAt()==true,
        // silently turning {} into SQL NULL — a data corruption bug.
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data", Arrays.asList(new ShreddedField("age", DataTypes.INT(), 1)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                IntVector ageVec = new IntVector("$data.age", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();

            DataType[] types = {DataTypes.INT()};
            FieldVector[] shreddedVecs = {ageVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            // Write an empty object {}
            Variant input = Variant.fromJson("{}");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec}, schema, types);

            // {} is NOT SQL NULL — isNullAt() must return false
            assertThat(reader.isNullAt(0)).isFalse();

            Variant result = reader.getVariant(0);
            assertThat(result).isNotNull();
            assertThat(result.isObject()).isTrue();
            // Empty object has no fields
            assertThat(result.getFieldByName("age")).isNull();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Field missing in input (shredded field not present in variant)
    // --------------------------------------------------------------------------------------------

    @Test
    void testFieldNotPresentInInput() {
        ShreddingSchema schema =
                new ShreddingSchema(
                        "data",
                        Arrays.asList(
                                new ShreddedField("age", DataTypes.INT(), 1),
                                new ShreddedField("name", DataTypes.STRING(), 2)));

        try (VarBinaryVector residualVec = new VarBinaryVector("residual", allocator);
                IntVector ageVec = new IntVector("$data.age", allocator);
                VarCharVector nameVec = new VarCharVector("$data.name", allocator)) {

            residualVec.allocateNew();
            ageVec.allocateNew();
            nameVec.allocateNew();

            DataType[] types = {DataTypes.INT(), DataTypes.STRING()};
            FieldVector[] shreddedVecs = {ageVec, nameVec};

            ArrowShreddedVariantWriter writer =
                    new ArrowShreddedVariantWriter(residualVec, schema, shreddedVecs, types);

            // Only "age" is present, "name" is missing
            Variant input = Variant.fromJson("{\"age\":42}");
            GenericRow row = GenericRow.of(input);
            writer.write(0, row, 0, true);

            ShreddedVariantColumnVector reader =
                    new ShreddedVariantColumnVector(
                            residualVec, new ValueVector[] {ageVec, nameVec}, schema, types);

            Variant result = reader.getVariant(0);
            assertThat(result.getFieldByName("age").getInt()).isEqualTo(42);
            assertThat(result.getFieldByName("name")).isNull();
        }
    }
}
