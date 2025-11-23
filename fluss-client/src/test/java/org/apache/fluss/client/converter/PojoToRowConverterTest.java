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

package org.apache.fluss.client.converter;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link PojoToRowConverter}. */
public class PojoToRowConverterTest {

    @Test
    public void testNullHandlingToRow() {
        RowType table = ConvertersTestFixtures.fullSchema();
        RowType projection = table;
        PojoToRowConverter<ConvertersTestFixtures.TestPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.TestPojo.class, table, projection);
        assertThat(writer.toRow(null)).isNull();
    }

    @Test
    public void testProjectionSubsetWrites() {
        RowType table = ConvertersTestFixtures.fullSchema();
        RowType projection =
                RowType.builder()
                        .field("booleanField", DataTypes.BOOLEAN())
                        .field("intField", DataTypes.INT())
                        .field("stringField", DataTypes.STRING())
                        .build();

        PojoToRowConverter<ConvertersTestFixtures.TestPojo> writer =
                PojoToRowConverter.of(ConvertersTestFixtures.TestPojo.class, table, projection);
        ConvertersTestFixtures.TestPojo pojo = ConvertersTestFixtures.TestPojo.sample();
        GenericRow row = writer.toRow(pojo);
        assertThat(row.getFieldCount()).isEqualTo(3);
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getInt(1)).isEqualTo(123456);
        assertThat(row.getString(2).toString()).isEqualTo("Hello, World!");
    }

    @Test
    public void testPojoMustExactlyMatchTableSchema() {
        RowType table =
                RowType.builder()
                        .field("booleanField", DataTypes.BOOLEAN())
                        .field("intField", DataTypes.INT())
                        .field("stringField", DataTypes.STRING())
                        .field("extraField", DataTypes.DOUBLE())
                        .build();
        RowType projection = table;
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.PartialTestPojo.class,
                                        table,
                                        projection))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must exactly match table schema");
    }

    @Test
    public void testPojoNoDefaultCtorFails() {
        RowType table = RowType.builder().field("intField", DataTypes.INT()).build();
        RowType proj = table;
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.NoDefaultConstructorPojo.class,
                                        table,
                                        proj))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("public default constructor");
    }

    @Test
    public void testDecimalTypeValidationAtCreation() {
        RowType table = RowType.builder().field("decimalField", DataTypes.DECIMAL(10, 2)).build();
        RowType proj = table;
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.DecimalWrongTypePojo.class,
                                        table,
                                        proj))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("decimalField");
    }

    @Test
    public void testDateTimeTypeValidationAtCreation() {
        RowType dateSchema = RowType.builder().field("dateField", DataTypes.DATE()).build();
        RowType timeSchema = RowType.builder().field("timeField", DataTypes.TIME()).build();
        RowType tsSchema = RowType.builder().field("timestampField", DataTypes.TIMESTAMP()).build();
        RowType ltzSchema =
                RowType.builder().field("timestampLtzField", DataTypes.TIMESTAMP_LTZ()).build();
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.DateWrongTypePojo.class,
                                        dateSchema,
                                        dateSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("dateField");
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.TimeWrongTypePojo.class,
                                        timeSchema,
                                        timeSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("timeField");
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.TimestampWrongTypePojo.class,
                                        tsSchema,
                                        tsSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("timestampField");
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.TimestampLtzWrongTypePojo.class,
                                        ltzSchema,
                                        ltzSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type")
                .hasMessageContaining("timestampLtzField");
    }

    @Test
    public void testUnsupportedSchemaFieldTypeThrows() {
        RowType table =
                RowType.builder()
                        .field("mapField", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();
        RowType proj = table;
        assertThatThrownBy(
                        () ->
                                PojoToRowConverter.of(
                                        ConvertersTestFixtures.MapPojo.class, table, proj))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported field type")
                .hasMessageContaining("MAP")
                .hasMessageContaining("mapField");
    }


    @Test
    void testByteToSmallIntWidening() {
        RowType table = RowType.builder().field("value", DataTypes.SMALLINT()).build();
        RowType projection = table;

        PojoToRowConverter<ByteFieldPojo> converter =
                PojoToRowConverter.of(ByteFieldPojo.class, table, projection);

        ByteFieldPojo pojo = new ByteFieldPojo();
        pojo.value = (byte) 42;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getShort(0)).isEqualTo((short) 42);
    }

    @Test
    public void testIntToBigIntWidening() {
        RowType table = RowType.builder().field("orderId", DataTypes.BIGINT()).build();
        RowType projection = table;

        PojoToRowConverter<IntFieldPojo> converter =
                PojoToRowConverter.of(IntFieldPojo.class, table, projection);

        IntFieldPojo pojo = new IntFieldPojo();
        pojo.orderId = 123456;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getLong(0)).isEqualTo(123456L);
    }

    @Test
    public void testShortToBigIntWidening() {
        RowType table = RowType.builder().field("quantity", DataTypes.BIGINT()).build();
        RowType projection = table;

        PojoToRowConverter<ShortFieldPojo> converter =
                PojoToRowConverter.of(ShortFieldPojo.class, table, projection);

        ShortFieldPojo pojo = new ShortFieldPojo();
        pojo.quantity = (short) 999;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getLong(0)).isEqualTo(999L);
    }

    @Test
    public void testIntToFloatWidening() {
        RowType table = RowType.builder().field("orderId", DataTypes.FLOAT()).build();
        RowType projection = table;

        PojoToRowConverter<IntFieldPojo> converter =
                PojoToRowConverter.of(IntFieldPojo.class, table, projection);

        IntFieldPojo pojo = new IntFieldPojo();
        pojo.orderId = 1000;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getFloat(0)).isEqualTo(1000.0f);
    }

    @Test
    public void testLongToDoubleWidening() {
        RowType table = RowType.builder().field("value", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<LongFieldPojo> converter =
                PojoToRowConverter.of(LongFieldPojo.class, table, projection);

        LongFieldPojo pojo = new LongFieldPojo();
        pojo.value = 123456789L;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getDouble(0)).isEqualTo(123456789.0);
    }

    @Test
    public void testFloatToDoubleWidening() {
        RowType table = RowType.builder().field("value", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<FloatFieldPojo> converter =
                PojoToRowConverter.of(FloatFieldPojo.class, table, projection);

        FloatFieldPojo pojo = new FloatFieldPojo();
        pojo.value = 99.99f;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getDouble(0)).isCloseTo(99.99, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    public void testMultipleWideningInSamePojo() {
        RowType table =
                RowType.builder()
                        .field("byteVal", DataTypes.BIGINT())
                        .field("shortVal", DataTypes.BIGINT())
                        .field("intVal", DataTypes.BIGINT())
                        .field("floatVal", DataTypes.DOUBLE())
                        .build();
        RowType projection = table;

        PojoToRowConverter<MixedNumericPojo> converter =
                PojoToRowConverter.of(MixedNumericPojo.class, table, projection);

        MixedNumericPojo pojo = new MixedNumericPojo();
        pojo.byteVal = (byte) 10;
        pojo.shortVal = (short) 100;
        pojo.intVal = 1000;
        pojo.floatVal = 10.5f;

        GenericRow row = converter.toRow(pojo);

        assertThat(row.getLong(0)).isEqualTo(10L);
        assertThat(row.getLong(1)).isEqualTo(100L);
        assertThat(row.getLong(2)).isEqualTo(1000L);
        assertThat(row.getDouble(3)).isCloseTo(10.5, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    public void testNarrowingConversionStillFails() {
        RowType table = RowType.builder().field("value", DataTypes.SMALLINT()).build();
        RowType projection = table;

        // Long to Short is narrowing - should still fail
        assertThatThrownBy(() -> PojoToRowConverter.of(LongFieldPojo.class, table, projection))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type");
    }

    @Test
    public void testByteToIntWidening() {
        RowType table = RowType.builder().field("value", DataTypes.INT()).build();
        RowType projection = table;

        PojoToRowConverter<ByteFieldPojo> converter =
                PojoToRowConverter.of(ByteFieldPojo.class, table, projection);

        ByteFieldPojo pojo = new ByteFieldPojo();
        pojo.value = (byte) 100;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getInt(0)).isEqualTo(100);
    }

    @Test
    public void testShortToIntWidening() {
        RowType table = RowType.builder().field("quantity", DataTypes.INT()).build();
        RowType projection = table;

        PojoToRowConverter<ShortFieldPojo> converter =
                PojoToRowConverter.of(ShortFieldPojo.class, table, projection);

        ShortFieldPojo pojo = new ShortFieldPojo();
        pojo.quantity = (short) 5000;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getInt(0)).isEqualTo(5000);
    }

    @Test
    public void testByteToFloatWidening() {
        RowType table = RowType.builder().field("value", DataTypes.FLOAT()).build();
        RowType projection = table;

        PojoToRowConverter<ByteFieldPojo> converter =
                PojoToRowConverter.of(ByteFieldPojo.class, table, projection);

        ByteFieldPojo pojo = new ByteFieldPojo();
        pojo.value = (byte) 50;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getFloat(0)).isEqualTo(50.0f);
    }

    @Test
    public void testIntToDoubleWidening() {
        RowType table = RowType.builder().field("orderId", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<IntFieldPojo> converter =
                PojoToRowConverter.of(IntFieldPojo.class, table, projection);

        IntFieldPojo pojo = new IntFieldPojo();
        pojo.orderId = 987654;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getDouble(0)).isEqualTo(987654.0);
    }

    @Test
    public void testWideningWithNullValues() {
        RowType table = RowType.builder().field("orderId", DataTypes.BIGINT()).build();
        RowType projection = table;

        PojoToRowConverter<IntFieldPojo> converter =
                PojoToRowConverter.of(IntFieldPojo.class, table, projection);

        IntFieldPojo pojo = new IntFieldPojo();
        pojo.orderId = null;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.isNullAt(0)).isTrue();
    }


    /**
     * Tests that Float.NaN values are preserved through POJO-to-Row conversion without data loss.
     * This validates FR-001: System MUST preserve Float.NaN through conversion.
     *
     * <p>IEEE 754 defines NaN (Not-a-Number) as a special floating-point value representing invalid
     * or undefined calculation results. This test ensures NaN survives the conversion pipeline
     * without being corrupted or normalized.
     */
    @Test
    public void testFloatNaNPreservation() {
        RowType table = RowType.builder().field("value", DataTypes.FLOAT()).build();
        RowType projection = table;

        PojoToRowConverter<FloatFieldPojo> converter =
                PojoToRowConverter.of(FloatFieldPojo.class, table, projection);

        FloatFieldPojo pojo = new FloatFieldPojo();
        pojo.value = Float.NaN;
        GenericRow row = converter.toRow(pojo);

        assertThat(Float.isNaN(row.getFloat(0)))
                .as("Float.NaN should be preserved through POJO-to-Row conversion")
                .isTrue();
    }

    /**
     * Tests that Double.NaN values are preserved through POJO-to-Row conversion without data loss.
     * This validates FR-002: System MUST preserve Double.NaN through conversion.
     *
     * <p>IEEE 754 defines NaN (Not-a-Number) for both single and double precision. This test
     * ensures Double.NaN is correctly handled by the conversion logic.
     */
    @Test
    public void testDoubleNaNPreservation() {
        RowType table = RowType.builder().field("value", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<DoubleFieldPojo> converter =
                PojoToRowConverter.of(DoubleFieldPojo.class, table, projection);

        DoubleFieldPojo pojo = new DoubleFieldPojo();
        pojo.value = Double.NaN;
        GenericRow row = converter.toRow(pojo);

        assertThat(Double.isNaN(row.getDouble(0)))
                .as("Double.NaN should be preserved through POJO-to-Row conversion")
                .isTrue();
    }

    /**
     * Tests that Float.NaN values are correctly widened to Double.NaN when converting from Float
     * POJO field to Double table column. This validates FR-005: System MUST correctly widen Float
     * special values to Double following IEEE 754 semantics.
     *
     * <p>IEEE 754 specifies that NaN values maintain their semantic meaning during type widening
     * (Float → Double).
     */
    @Test
    public void testFloatNaNToDoubleWidening() {
        RowType table = RowType.builder().field("value", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<FloatFieldPojo> converter =
                PojoToRowConverter.of(FloatFieldPojo.class, table, projection);

        FloatFieldPojo pojo = new FloatFieldPojo();
        pojo.value = Float.NaN;
        GenericRow row = converter.toRow(pojo);

        assertThat(Double.isNaN(row.getDouble(0)))
                .as("Float.NaN should widen to Double.NaN following IEEE 754 rules")
                .isTrue();
    }

    /**
     * Tests that Float.POSITIVE_INFINITY values are preserved through POJO-to-Row conversion. This
     * validates FR-003: System MUST preserve Float.POSITIVE_INFINITY through conversion.
     *
     * <p>IEEE 754 defines positive infinity as the result of overflow in positive direction or
     * division of positive number by zero. This test ensures the sign and infinity status are
     * preserved.
     */
    @Test
    public void testFloatPositiveInfinityPreservation() {
        RowType table = RowType.builder().field("value", DataTypes.FLOAT()).build();
        RowType projection = table;

        PojoToRowConverter<FloatFieldPojo> converter =
                PojoToRowConverter.of(FloatFieldPojo.class, table, projection);

        FloatFieldPojo pojo = new FloatFieldPojo();
        pojo.value = Float.POSITIVE_INFINITY;
        GenericRow row = converter.toRow(pojo);

        float result = row.getFloat(0);
        assertThat(Float.isInfinite(result) && result > 0)
                .as("Float.POSITIVE_INFINITY should be preserved with correct sign")
                .isTrue();
    }

    /**
     * Tests that Float.NEGATIVE_INFINITY values are preserved through POJO-to-Row conversion. This
     * validates FR-003: System MUST preserve Float.NEGATIVE_INFINITY through conversion.
     *
     * <p>IEEE 754 defines negative infinity as the result of overflow in negative direction or
     * division of negative number by zero. This test ensures the sign and infinity status are
     * preserved.
     */
    @Test
    public void testFloatNegativeInfinityPreservation() {
        RowType table = RowType.builder().field("value", DataTypes.FLOAT()).build();
        RowType projection = table;

        PojoToRowConverter<FloatFieldPojo> converter =
                PojoToRowConverter.of(FloatFieldPojo.class, table, projection);

        FloatFieldPojo pojo = new FloatFieldPojo();
        pojo.value = Float.NEGATIVE_INFINITY;
        GenericRow row = converter.toRow(pojo);

        float result = row.getFloat(0);
        assertThat(Float.isInfinite(result) && result < 0)
                .as("Float.NEGATIVE_INFINITY should be preserved with correct sign")
                .isTrue();
    }

    /**
     * Tests that Double.POSITIVE_INFINITY values are preserved through POJO-to-Row conversion. This
     * validates FR-004: System MUST preserve Double.POSITIVE_INFINITY through conversion.
     */
    @Test
    public void testDoublePositiveInfinityPreservation() {
        RowType table = RowType.builder().field("value", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<DoubleFieldPojo> converter =
                PojoToRowConverter.of(DoubleFieldPojo.class, table, projection);

        DoubleFieldPojo pojo = new DoubleFieldPojo();
        pojo.value = Double.POSITIVE_INFINITY;
        GenericRow row = converter.toRow(pojo);

        double result = row.getDouble(0);
        assertThat(Double.isInfinite(result) && result > 0)
                .as("Double.POSITIVE_INFINITY should be preserved with correct sign")
                .isTrue();
    }

    /**
     * Tests that Double.NEGATIVE_INFINITY values are preserved through POJO-to-Row conversion. This
     * validates FR-004: System MUST preserve Double.NEGATIVE_INFINITY through conversion.
     */
    @Test
    public void testDoubleNegativeInfinityPreservation() {
        RowType table = RowType.builder().field("value", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<DoubleFieldPojo> converter =
                PojoToRowConverter.of(DoubleFieldPojo.class, table, projection);

        DoubleFieldPojo pojo = new DoubleFieldPojo();
        pojo.value = Double.NEGATIVE_INFINITY;
        GenericRow row = converter.toRow(pojo);

        double result = row.getDouble(0);
        assertThat(Double.isInfinite(result) && result < 0)
                .as("Double.NEGATIVE_INFINITY should be preserved with correct sign")
                .isTrue();
    }

    /**
     * Tests that Float.POSITIVE_INFINITY correctly widens to Double.POSITIVE_INFINITY when
     * converting from Float POJO field to Double table column. This validates FR-005: System MUST
     * correctly widen Float special values to Double.
     */
    @Test
    public void testFloatPositiveInfinityToDoubleWidening() {
        RowType table = RowType.builder().field("value", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<FloatFieldPojo> converter =
                PojoToRowConverter.of(FloatFieldPojo.class, table, projection);

        FloatFieldPojo pojo = new FloatFieldPojo();
        pojo.value = Float.POSITIVE_INFINITY;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getDouble(0))
                .as("Float.POSITIVE_INFINITY should widen to Double.POSITIVE_INFINITY")
                .isEqualTo(Double.POSITIVE_INFINITY);
    }

    /**
     * Tests that Float.NEGATIVE_INFINITY correctly widens to Double.NEGATIVE_INFINITY when
     * converting from Float POJO field to Double table column. This validates FR-005: System MUST
     * correctly widen Float special values to Double.
     */
    @Test
    public void testFloatNegativeInfinityToDoubleWidening() {
        RowType table = RowType.builder().field("value", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<FloatFieldPojo> converter =
                PojoToRowConverter.of(FloatFieldPojo.class, table, projection);

        FloatFieldPojo pojo = new FloatFieldPojo();
        pojo.value = Float.NEGATIVE_INFINITY;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getDouble(0))
                .as("Float.NEGATIVE_INFINITY should widen to Double.NEGATIVE_INFINITY")
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    /**
     * Tests that a POJO with mixed normal and special Float values converts correctly to a table
     * with multiple FLOAT columns. This validates FR-012: System MUST handle mixed normal and
     * special values without errors.
     *
     * <p>Real-world datasets often contain a mix of valid measurements and error conditions (NaN)
     * or overflow conditions (Infinity). This test ensures robust handling of realistic data.
     */
    @Test
    public void testMixedFloatSpecialAndNormalValues() {
        RowType table =
                RowType.builder()
                        .field("normalValue", DataTypes.FLOAT())
                        .field("nanValue", DataTypes.FLOAT())
                        .field("posInfValue", DataTypes.FLOAT())
                        .field("negInfValue", DataTypes.FLOAT())
                        .build();
        RowType projection = table;

        PojoToRowConverter<MixedFloatValuesPojo> converter =
                PojoToRowConverter.of(MixedFloatValuesPojo.class, table, projection);

        MixedFloatValuesPojo pojo = new MixedFloatValuesPojo();
        pojo.normalValue = 99.99f;
        pojo.nanValue = Float.NaN;
        pojo.posInfValue = Float.POSITIVE_INFINITY;
        pojo.negInfValue = Float.NEGATIVE_INFINITY;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getFloat(0)).as("Normal Float value should be preserved").isEqualTo(99.99f);
        assertThat(Float.isNaN(row.getFloat(1)))
                .as("Float.NaN should be preserved in mixed value row")
                .isTrue();
        assertThat(row.getFloat(2))
                .as("Float.POSITIVE_INFINITY should be preserved in mixed value row")
                .isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(row.getFloat(3))
                .as("Float.NEGATIVE_INFINITY should be preserved in mixed value row")
                .isEqualTo(Float.NEGATIVE_INFINITY);
    }

    /**
     * Tests that a POJO with mixed normal and special Double values converts correctly to a table
     * with multiple DOUBLE columns. This validates FR-012: System MUST handle mixed normal and
     * special values without errors.
     */
    @Test
    public void testMixedDoubleSpecialAndNormalValues() {
        RowType table =
                RowType.builder()
                        .field("normalValue", DataTypes.DOUBLE())
                        .field("nanValue", DataTypes.DOUBLE())
                        .field("posInfValue", DataTypes.DOUBLE())
                        .field("negInfValue", DataTypes.DOUBLE())
                        .build();
        RowType projection = table;

        PojoToRowConverter<MixedDoubleValuesPojo> converter =
                PojoToRowConverter.of(MixedDoubleValuesPojo.class, table, projection);

        MixedDoubleValuesPojo pojo = new MixedDoubleValuesPojo();
        pojo.normalValue = 123.456;
        pojo.nanValue = Double.NaN;
        pojo.posInfValue = Double.POSITIVE_INFINITY;
        pojo.negInfValue = Double.NEGATIVE_INFINITY;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getDouble(0))
                .as("Normal Double value should be preserved")
                .isEqualTo(123.456);
        assertThat(Double.isNaN(row.getDouble(1)))
                .as("Double.NaN should be preserved in mixed value row")
                .isTrue();
        assertThat(row.getDouble(2))
                .as("Double.POSITIVE_INFINITY should be preserved in mixed value row")
                .isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(row.getDouble(3))
                .as("Double.NEGATIVE_INFINITY should be preserved in mixed value row")
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    /**
     * Tests that Float special values correctly widen to Double equivalents when converting a POJO
     * with Float fields containing mixed values to a table with DOUBLE columns. This validates
     * FR-005: System MUST correctly widen Float special values to Double.
     */
    @Test
    public void testMixedFloatToDoubleWidening() {
        RowType table =
                RowType.builder()
                        .field("normalValue", DataTypes.DOUBLE())
                        .field("nanValue", DataTypes.DOUBLE())
                        .field("posInfValue", DataTypes.DOUBLE())
                        .field("negInfValue", DataTypes.DOUBLE())
                        .build();
        RowType projection = table;

        PojoToRowConverter<MixedFloatValuesPojo> converter =
                PojoToRowConverter.of(MixedFloatValuesPojo.class, table, projection);

        MixedFloatValuesPojo pojo = new MixedFloatValuesPojo();
        pojo.normalValue = 99.99f;
        pojo.nanValue = Float.NaN;
        pojo.posInfValue = Float.POSITIVE_INFINITY;
        pojo.negInfValue = Float.NEGATIVE_INFINITY;
        GenericRow row = converter.toRow(pojo);

        assertThat(row.getDouble(0))
                .as("Normal Float value should widen to Double correctly")
                .isCloseTo(99.99, within(0.01));
        assertThat(Double.isNaN(row.getDouble(1)))
                .as("Float.NaN should widen to Double.NaN")
                .isTrue();
        assertThat(row.getDouble(2))
                .as("Float.POSITIVE_INFINITY should widen to Double.POSITIVE_INFINITY")
                .isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(row.getDouble(3))
                .as("Float.NEGATIVE_INFINITY should widen to Double.NEGATIVE_INFINITY")
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    /**
     * Tests that null and Float.NaN are correctly distinguished in nullable Float columns. This
     * validates FR-011: System MUST distinguish between null and NaN.
     *
     * <p>Null represents missing/unknown data, while NaN represents invalid calculation results.
     * These are semantically different and must be preserved distinctly.
     */
    @Test
    public void testNullVsNaNDistinctionFloat() {
        RowType table = RowType.builder().field("value", DataTypes.FLOAT()).build();
        RowType projection = table;

        PojoToRowConverter<FloatFieldPojo> converter =
                PojoToRowConverter.of(FloatFieldPojo.class, table, projection);

        // Test 1: null field
        FloatFieldPojo nullPojo = new FloatFieldPojo();
        nullPojo.value = null;
        GenericRow nullRow = converter.toRow(nullPojo);

        assertThat(nullRow.isNullAt(0))
                .as("Null Float field should result in null row value")
                .isTrue();

        // Test 2: NaN field
        FloatFieldPojo nanPojo = new FloatFieldPojo();
        nanPojo.value = Float.NaN;
        GenericRow nanRow = converter.toRow(nanPojo);

        assertThat(nanRow.isNullAt(0))
                .as("Float.NaN field should NOT result in null row value")
                .isFalse();
        assertThat(Float.isNaN(nanRow.getFloat(0)))
                .as("Float.NaN should be stored as NaN, not null")
                .isTrue();
    }

    /**
     * Tests that null and Double.NaN are correctly distinguished in nullable Double columns. This
     * validates FR-011: System MUST distinguish between null and NaN.
     */
    @Test
    public void testNullVsNaNDistinctionDouble() {
        RowType table = RowType.builder().field("value", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<DoubleFieldPojo> converter =
                PojoToRowConverter.of(DoubleFieldPojo.class, table, projection);

        // Test 1: null field
        DoubleFieldPojo nullPojo = new DoubleFieldPojo();
        nullPojo.value = null;
        GenericRow nullRow = converter.toRow(nullPojo);

        assertThat(nullRow.isNullAt(0))
                .as("Null Double field should result in null row value")
                .isTrue();

        // Test 2: NaN field
        DoubleFieldPojo nanPojo = new DoubleFieldPojo();
        nanPojo.value = Double.NaN;
        GenericRow nanRow = converter.toRow(nanPojo);

        assertThat(nanRow.isNullAt(0))
                .as("Double.NaN field should NOT result in null row value")
                .isFalse();
        assertThat(Double.isNaN(nanRow.getDouble(0)))
                .as("Double.NaN should be stored as NaN, not null")
                .isTrue();
    }

    @Test
    public void testTimestampPrecision3() {
        // Test with precision 3 milliseconds
        RowType table =
                RowType.builder()
                        .field("timestampNtzField", DataTypes.TIMESTAMP(3))
                        .field("timestampLtzField", DataTypes.TIMESTAMP_LTZ(3))
                        .build();

        PojoToRowConverter<TimestampPojo> writer =
                PojoToRowConverter.of(TimestampPojo.class, table, table);

        // 123.456789
        LocalDateTime ldt = LocalDateTime.of(2025, 7, 23, 15, 1, 30, 123456789);
        Instant instant = Instant.parse("2025-07-23T15:01:30.123456789Z");

        TimestampPojo pojo = new TimestampPojo(ldt, instant);
        GenericRow row = writer.toRow(pojo);

        // truncate to 123.000000
        TimestampNtz expectedNtz = TimestampNtz.fromLocalDateTime(ldt.withNano(123000000));
        TimestampLtz expectedLtz =
                TimestampLtz.fromInstant(
                        Instant.ofEpochSecond(instant.getEpochSecond(), 123000000));

        assertThat(row.getTimestampNtz(0, 3)).isEqualTo(expectedNtz);
        assertThat(row.getTimestampLtz(1, 3)).isEqualTo(expectedLtz);
    }

    @Test
    public void testTimestampPrecision6() {
        // Test with precision 6 microseconds
        RowType table =
                RowType.builder()
                        .field("timestampNtzField", DataTypes.TIMESTAMP(6))
                        .field("timestampLtzField", DataTypes.TIMESTAMP_LTZ(6))
                        .build();

        PojoToRowConverter<TimestampPojo> writer =
                PojoToRowConverter.of(TimestampPojo.class, table, table);

        // 123.456789
        LocalDateTime ldt = LocalDateTime.of(2025, 7, 23, 15, 1, 30, 123456789);
        Instant instant = Instant.parse("2025-07-23T15:01:30.123456789Z");

        TimestampPojo pojo = new TimestampPojo(ldt, instant);
        GenericRow row = writer.toRow(pojo);

        // truncate to 123.456000
        TimestampNtz expectedNtz = TimestampNtz.fromLocalDateTime(ldt.withNano(123456000));
        TimestampLtz expectedLtz =
                TimestampLtz.fromInstant(
                        Instant.ofEpochSecond(instant.getEpochSecond(), 123456000));

        assertThat(row.getTimestampNtz(0, 6)).isEqualTo(expectedNtz);
        assertThat(row.getTimestampLtz(1, 6)).isEqualTo(expectedLtz);
    }

    @Test
    public void testTimestampPrecision9() {
        // Test with precision 9 nanoseconds
        RowType table =
                RowType.builder()
                        .field("timestampNtzField", DataTypes.TIMESTAMP(9))
                        .field("timestampLtzField", DataTypes.TIMESTAMP_LTZ(9))
                        .build();

        PojoToRowConverter<TimestampPojo> writer =
                PojoToRowConverter.of(TimestampPojo.class, table, table);

        LocalDateTime ldt = LocalDateTime.of(2025, 7, 23, 15, 1, 30, 123456789);
        Instant instant = Instant.parse("2025-07-23T15:01:30.123456789Z");

        TimestampPojo pojo = new TimestampPojo(ldt, instant);
        GenericRow row = writer.toRow(pojo);

        TimestampNtz expectedNtz = TimestampNtz.fromLocalDateTime(ldt);
        TimestampLtz expectedLtz = TimestampLtz.fromInstant(instant);

        assertThat(row.getTimestampNtz(0, 9)).isEqualTo(expectedNtz);
        assertThat(row.getTimestampLtz(1, 9)).isEqualTo(expectedLtz);
    }

    @Test
    public void testTimestampPrecisionRoundTrip() {
        testRoundTripWithPrecision(3);
        testRoundTripWithPrecision(6);
        testRoundTripWithPrecision(9);
    }

    private void testRoundTripWithPrecision(int precision) {
        RowType table =
                RowType.builder()
                        .field("timestampNtzField", DataTypes.TIMESTAMP(precision))
                        .field("timestampLtzField", DataTypes.TIMESTAMP_LTZ(precision))
                        .build();

        PojoToRowConverter<TimestampPojo> writer =
                PojoToRowConverter.of(TimestampPojo.class, table, table);
        RowToPojoConverter<TimestampPojo> reader =
                RowToPojoConverter.of(TimestampPojo.class, table, table);

        LocalDateTime originalLdt = LocalDateTime.of(2025, 7, 23, 15, 1, 30, 123456789);
        Instant originalInstant = Instant.parse("2025-07-23T15:01:30.123456789Z");

        TimestampPojo originalPojo = new TimestampPojo(originalLdt, originalInstant);

        // Convert POJO -> Row -> POJO
        GenericRow row = writer.toRow(originalPojo);
        TimestampPojo resultPojo = reader.fromRow(row);

        LocalDateTime expectedLdt = truncateLocalDateTime(originalLdt, precision);
        Instant expectedInstant = truncateInstant(originalInstant, precision);

        assertThat(resultPojo.timestampNtzField)
                .as("Round-trip LocalDateTime with precision %d", precision)
                .isEqualTo(expectedLdt);
        assertThat(resultPojo.timestampLtzField)
                .as("Round-trip Instant with precision %d", precision)
                .isEqualTo(expectedInstant);
    }

    private LocalDateTime truncateLocalDateTime(LocalDateTime ldt, int precision) {
        if (precision >= 9) {
            return ldt;
        }
        int divisor = (int) Math.pow(10, 9 - precision);
        int truncatedNanos = (ldt.getNano() / divisor) * divisor;
        return ldt.withNano(truncatedNanos);
    }

    private Instant truncateInstant(Instant instant, int precision) {
        if (precision >= 9) {
            return instant;
        }
        int divisor = (int) Math.pow(10, 9 - precision);
        int truncatedNanos = (instant.getNano() / divisor) * divisor;
        return Instant.ofEpochSecond(instant.getEpochSecond(), truncatedNanos);
    }


    /** Test POJO with Byte field. */
    public static class ByteFieldPojo {
        public Byte value;

        public ByteFieldPojo() {}
    }

    /** Test POJO with Short field. */
    public static class ShortFieldPojo {
        public Short quantity;

        public ShortFieldPojo() {}
    }

    /** Test POJO with Integer field. */
    public static class IntFieldPojo {
        public Integer orderId;

        public IntFieldPojo() {}
    }

    /** Test POJO with Long field. */
    public static class LongFieldPojo {
        public Long value;

        public LongFieldPojo() {}
    }

    /** Test POJO with Float field. */
    public static class FloatFieldPojo {
        public Float value;

        public FloatFieldPojo() {}
    }

    /** Test POJO with mixed numeric types. */
    public static class MixedNumericPojo {
        public Byte byteVal;
        public Short shortVal;
        public Integer intVal;
        public Float floatVal;

        public MixedNumericPojo() {}
    }

    /** Test POJO with Double field for special values testing. */
    public static class DoubleFieldPojo {
        public Double value;

        public DoubleFieldPojo() {}
    }

    /** Test POJO with mixed Float special and normal values. */
    public static class MixedFloatValuesPojo {
        public Float normalValue;
        public Float nanValue;
        public Float posInfValue;
        public Float negInfValue;

        public MixedFloatValuesPojo() {}
    }

    /** Test POJO with mixed Double special and normal values. */
    public static class MixedDoubleValuesPojo {
        public Double normalValue;
        public Double nanValue;
        public Double posInfValue;
        public Double negInfValue;

        public MixedDoubleValuesPojo() {}
    }

    /** POJO for testing timestamp precision. */
    public static class TimestampPojo {
        public LocalDateTime timestampNtzField;
        public Instant timestampLtzField;

        public TimestampPojo() {}

        public TimestampPojo(LocalDateTime timestampNtzField, Instant timestampLtzField) {
            this.timestampNtzField = timestampNtzField;
            this.timestampLtzField = timestampLtzField;
        }
    }
}
