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
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    // ----------------------- Numeric Type Widening Tests -----------------------

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
        RowType table = RowType.builder().field("price", DataTypes.DOUBLE()).build();
        RowType projection = table;

        PojoToRowConverter<FloatFieldPojo> converter =
                PojoToRowConverter.of(FloatFieldPojo.class, table, projection);

        FloatFieldPojo pojo = new FloatFieldPojo();
        pojo.price = 99.99f;
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

    // ----------------------- Test POJO Classes -----------------------

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
        public Float price;

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
}
