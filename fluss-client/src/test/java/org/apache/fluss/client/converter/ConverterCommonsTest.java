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

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ConverterCommons}. */
public class ConverterCommonsTest {

    // ==================== validatePojoMatchesTable Tests ====================

    @Test
    public void validatePojoMatchesTableWithExactMatch() {
        RowType table =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();

        PojoType<AllFieldsPojo> pojoType = PojoType.of(AllFieldsPojo.class);
        ConverterCommons.validatePojoMatchesTable(pojoType, table);
    }

    @Test
    public void validatePojoMatchesTableWithTypeIncompatibility() {
        RowType table =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.INT())
                        .build();

        PojoType<AllFieldsPojo> pojoType = PojoType.of(AllFieldsPojo.class);
        assertThatThrownBy(() -> ConverterCommons.validatePojoMatchesTable(pojoType, table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible with Fluss type");
    }

    @Test
    public void validatePojoMatchesTableWithMissingField() {
        RowType table =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("age", DataTypes.INT())
                        .build();

        PojoType<AllFieldsPojo> pojoType = PojoType.of(AllFieldsPojo.class);
        assertThatThrownBy(() -> ConverterCommons.validatePojoMatchesTable(pojoType, table))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must exactly match");
    }

    // ==================== validatePojoMatchesProjection Tests ====================

    @Test
    public void validatePojoMatchesProjectionWithSubset() {
        RowType projection =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();

        PojoType<AllFieldsPojo> pojoType = PojoType.of(AllFieldsPojo.class);
        ConverterCommons.validatePojoMatchesProjection(pojoType, projection);
    }

    @Test
    public void validatePojoMatchesProjectionWithSingleField() {
        RowType projection = RowType.builder().field("id", DataTypes.INT()).build();

        PojoType<AllFieldsPojo> pojoType = PojoType.of(AllFieldsPojo.class);
        ConverterCommons.validatePojoMatchesProjection(pojoType, projection);
    }

    @Test
    public void validatePojoMatchesProjectionWithMissingField() {
        RowType projection =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("missingField", DataTypes.STRING())
                        .build();

        PojoType<AllFieldsPojo> pojoType = PojoType.of(AllFieldsPojo.class);
        assertThatThrownBy(
                        () -> ConverterCommons.validatePojoMatchesProjection(pojoType, projection))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void validatePojoMatchesProjectionWithTypeIncompatibility() {
        RowType projection =
                RowType.builder()
                        .field("id", DataTypes.STRING())
                        .field("name", DataTypes.STRING())
                        .build();

        PojoType<AllFieldsPojo> pojoType = PojoType.of(AllFieldsPojo.class);
        assertThatThrownBy(
                        () -> ConverterCommons.validatePojoMatchesProjection(pojoType, projection))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ==================== validateProjectionSubset Tests ====================

    @Test
    public void validateProjectionSubsetAllFieldsInTable() {
        RowType tableSchema =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("age", DataTypes.INT())
                        .build();

        RowType projection =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();

        ConverterCommons.validateProjectionSubset(projection, tableSchema);
    }

    @Test
    public void validateProjectionSubsetSingleFieldInTable() {
        RowType tableSchema =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();

        RowType projection = RowType.builder().field("id", DataTypes.INT()).build();

        ConverterCommons.validateProjectionSubset(projection, tableSchema);
    }

    @Test
    public void validateProjectionSubsetWithFieldNotInTable() {
        RowType tableSchema =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();

        RowType projection =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("unknown", DataTypes.STRING())
                        .build();

        assertThatThrownBy(() -> ConverterCommons.validateProjectionSubset(projection, tableSchema))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ==================== validateCompatibility Tests ====================

    @Test
    public void compatibilityBooleanWithBoolean() {
        PojoType<BooleanPojo> pojoType = PojoType.of(BooleanPojo.class);
        PojoType.Property prop = pojoType.getProperty("flag");
        ConverterCommons.validateCompatibility(DataTypes.BOOLEAN(), prop);
    }

    @Test
    public void compatibilityTinyintWithByte() {
        PojoType<BytePojo> pojoType = PojoType.of(BytePojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        ConverterCommons.validateCompatibility(DataTypes.TINYINT(), prop);
    }

    @Test
    public void compatibilitySmallintWithShort() {
        PojoType<ShortPojo> pojoType = PojoType.of(ShortPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        ConverterCommons.validateCompatibility(DataTypes.SMALLINT(), prop);
    }

    @Test
    public void compatibilityIntegerWithInteger() {
        PojoType<IntPojo> pojoType = PojoType.of(IntPojo.class);
        PojoType.Property prop = pojoType.getProperty("id");
        ConverterCommons.validateCompatibility(DataTypes.INT(), prop);
    }

    @Test
    public void compatibilityBigintWithLong() {
        PojoType<LongPojo> pojoType = PojoType.of(LongPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        ConverterCommons.validateCompatibility(DataTypes.BIGINT(), prop);
    }

    @Test
    public void compatibilityFloatWithFloat() {
        PojoType<FloatPojo> pojoType = PojoType.of(FloatPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        ConverterCommons.validateCompatibility(DataTypes.FLOAT(), prop);
    }

    @Test
    public void compatibilityDoubleWithDouble() {
        PojoType<DoublePojo> pojoType = PojoType.of(DoublePojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        ConverterCommons.validateCompatibility(DataTypes.DOUBLE(), prop);
    }

    @Test
    public void compatibilityCharWithString() {
        PojoType<StringPojo> pojoType = PojoType.of(StringPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        ConverterCommons.validateCompatibility(DataTypes.CHAR(10), prop);
    }

    @Test
    public void compatibilityCharWithCharacter() {
        PojoType<CharacterPojo> pojoType = PojoType.of(CharacterPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        ConverterCommons.validateCompatibility(DataTypes.CHAR(1), prop);
    }

    @Test
    public void compatibilityStringWithString() {
        PojoType<StringPojo> pojoType = PojoType.of(StringPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        ConverterCommons.validateCompatibility(DataTypes.STRING(), prop);
    }

    @Test
    public void compatibilityStringWithCharacter() {
        PojoType<CharacterPojo> pojoType = PojoType.of(CharacterPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        ConverterCommons.validateCompatibility(DataTypes.STRING(), prop);
    }

    @Test
    public void compatibilityBinaryWithByteArray() {
        PojoType<ByteArrayPojo> pojoType = PojoType.of(ByteArrayPojo.class);
        PojoType.Property prop = pojoType.getProperty("bytes");
        ConverterCommons.validateCompatibility(DataTypes.BINARY(100), prop);
    }

    @Test
    public void compatibilityBytesWithByteArray() {
        PojoType<ByteArrayPojo> pojoType = PojoType.of(ByteArrayPojo.class);
        PojoType.Property prop = pojoType.getProperty("bytes");
        ConverterCommons.validateCompatibility(DataTypes.BYTES(), prop);
    }

    @Test
    public void compatibilityDecimalWithBigDecimal() {
        PojoType<BigDecimalPojo> pojoType = PojoType.of(BigDecimalPojo.class);
        PojoType.Property prop = pojoType.getProperty("amount");
        ConverterCommons.validateCompatibility(DataTypes.DECIMAL(10, 2), prop);
    }

    @Test
    public void compatibilityDateWithLocalDate() {
        PojoType<LocalDatePojo> pojoType = PojoType.of(LocalDatePojo.class);
        PojoType.Property prop = pojoType.getProperty("date");
        ConverterCommons.validateCompatibility(DataTypes.DATE(), prop);
    }

    @Test
    public void compatibilityTimeWithLocalTime() {
        PojoType<LocalTimePojo> pojoType = PojoType.of(LocalTimePojo.class);
        PojoType.Property prop = pojoType.getProperty("time");
        ConverterCommons.validateCompatibility(DataTypes.TIME(), prop);
    }

    @Test
    public void compatibilityTimestampNtzWithLocalDateTime() {
        PojoType<LocalDateTimePojo> pojoType = PojoType.of(LocalDateTimePojo.class);
        PojoType.Property prop = pojoType.getProperty("timestamp");
        ConverterCommons.validateCompatibility(DataTypes.TIMESTAMP(), prop);
    }

    @Test
    public void compatibilityTimestampLtzWithInstant() {
        PojoType<InstantPojo> pojoType = PojoType.of(InstantPojo.class);
        PojoType.Property prop = pojoType.getProperty("timestamp");
        ConverterCommons.validateCompatibility(DataTypes.TIMESTAMP_LTZ(), prop);
    }

    @Test
    public void compatibilityTimestampLtzWithOffsetDateTime() {
        PojoType<OffsetDateTimePojo> pojoType = PojoType.of(OffsetDateTimePojo.class);
        PojoType.Property prop = pojoType.getProperty("timestamp");
        ConverterCommons.validateCompatibility(DataTypes.TIMESTAMP_LTZ(), prop);
    }

    @Test
    public void compatibilityArrayWithArrayType() {
        PojoType<IntArrayPojo> pojoType = PojoType.of(IntArrayPojo.class);
        PojoType.Property prop = pojoType.getProperty("items");
        ConverterCommons.validateCompatibility(DataTypes.ARRAY(DataTypes.INT()), prop);
    }

    @Test
    public void compatibilityArrayWithListType() {
        PojoType<ListPojo> pojoType = PojoType.of(ListPojo.class);
        PojoType.Property prop = pojoType.getProperty("items");
        ConverterCommons.validateCompatibility(DataTypes.ARRAY(DataTypes.INT()), prop);
    }

    @Test
    public void compatibilityMapType() {
        PojoType<MapPojo> pojoType = PojoType.of(MapPojo.class);
        PojoType.Property prop = pojoType.getProperty("mapping");
        ConverterCommons.validateCompatibility(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), prop);
    }

    @Test
    public void compatibilityRowWithNestedPojo() {
        PojoType<NestedPojo> pojoType = PojoType.of(NestedPojo.class);
        PojoType.Property prop = pojoType.getProperty("nested");
        ConverterCommons.validateCompatibility(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING())),
                prop);
    }

    @Test
    public void compatibilityEnumWithString() {
        PojoType<EnumPojo> pojoType = PojoType.of(EnumPojo.class);
        PojoType.Property prop = pojoType.getProperty("status");
        ConverterCommons.validateCompatibility(DataTypes.STRING(), prop);
    }

    @Test
    public void incompatibilityBooleanWithString() {
        PojoType<StringPojo> pojoType = PojoType.of(StringPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        assertThatThrownBy(() -> ConverterCommons.validateCompatibility(DataTypes.BOOLEAN(), prop))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible");
    }

    @Test
    public void incompatibilityIntegerWithString() {
        PojoType<StringPojo> pojoType = PojoType.of(StringPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        assertThatThrownBy(() -> ConverterCommons.validateCompatibility(DataTypes.INT(), prop))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible");
    }

    @Test
    public void incompatibilityArrayWithString() {
        PojoType<StringPojo> pojoType = PojoType.of(StringPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        assertThatThrownBy(
                        () ->
                                ConverterCommons.validateCompatibility(
                                        DataTypes.ARRAY(DataTypes.INT()), prop))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be an array or Collection");
    }

    @Test
    public void incompatibilityMapWithString() {
        PojoType<StringPojo> pojoType = PojoType.of(StringPojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        assertThatThrownBy(
                        () ->
                                ConverterCommons.validateCompatibility(
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), prop))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a Map");
    }

    @Test
    public void incompatibilityRowWithArray() {
        PojoType<IntArrayPojo> pojoType = PojoType.of(IntArrayPojo.class);
        PojoType.Property prop = pojoType.getProperty("items");
        assertThatThrownBy(
                        () ->
                                ConverterCommons.validateCompatibility(
                                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())),
                                        prop))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a POJO class");
    }

    @Test
    public void incompatibilityRowWithList() {
        PojoType<ListPojo> pojoType = PojoType.of(ListPojo.class);
        PojoType.Property prop = pojoType.getProperty("items");
        assertThatThrownBy(
                        () ->
                                ConverterCommons.validateCompatibility(
                                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())),
                                        prop))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a POJO class");
    }

    @Test
    public void incompatibilityEnumWithInt() {
        PojoType<EnumPojo> pojoType = PojoType.of(EnumPojo.class);
        PojoType.Property prop = pojoType.getProperty("status");
        assertThatThrownBy(() -> ConverterCommons.validateCompatibility(DataTypes.INT(), prop))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a string type");
    }

    @Test
    public void incompatibilityBigintWithByte() {
        PojoType<BytePojo> pojoType = PojoType.of(BytePojo.class);
        PojoType.Property prop = pojoType.getProperty("value");
        assertThatThrownBy(() -> ConverterCommons.validateCompatibility(DataTypes.BIGINT(), prop))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible");
    }

    @Test
    public void convertStringToBinaryStringForCharTypeWithWrongLengthThrows() {
        assertThatThrownBy(
                        () ->
                                ConverterCommons.toBinaryStringForText(
                                        "Hello", "testField", DataTypes.CHAR(1).getTypeRoot()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void convertBooleanToBinaryString() {
        BinaryString result =
                ConverterCommons.toBinaryStringForText(
                        true, "field", DataTypes.STRING().getTypeRoot());
        assertThat(result.toString()).isEqualTo("true");
    }

    @Test
    public void convertNullToBinaryString() {
        BinaryString result =
                ConverterCommons.toBinaryStringForText(
                        null, "field", DataTypes.STRING().getTypeRoot());
        assertThat(result.toString()).isEqualTo("null");
    }

    // ==================== Test POJOs ====================

    /** Test POJO with multiple fields. */
    public static class AllFieldsPojo {
        public Integer id;
        public String name;

        public AllFieldsPojo() {}
    }

    /** Test POJO with Boolean field. */
    public static class BooleanPojo {
        public Boolean flag;

        public BooleanPojo() {}
    }

    /** Test POJO with Byte field. */
    public static class BytePojo {
        public Byte value;

        public BytePojo() {}
    }

    /** Test POJO with Short field. */
    public static class ShortPojo {
        public Short value;

        public ShortPojo() {}
    }

    /** Test POJO with Integer field. */
    public static class IntPojo {
        public Integer id;

        public IntPojo() {}
    }

    /** Test POJO with Long field. */
    public static class LongPojo {
        public Long value;

        public LongPojo() {}
    }

    /** Test POJO with Float field. */
    public static class FloatPojo {
        public Float value;

        public FloatPojo() {}
    }

    /** Test POJO with Double field. */
    public static class DoublePojo {
        public Double value;

        public DoublePojo() {}
    }

    /** Test POJO with String field. */
    public static class StringPojo {
        public String value;

        public StringPojo() {}
    }

    /** Test POJO with Character field. */
    public static class CharacterPojo {
        public Character value;

        public CharacterPojo() {}
    }

    /** Test POJO with byte[] field. */
    public static class ByteArrayPojo {
        public byte[] bytes;

        public ByteArrayPojo() {}
    }

    /** Test POJO with BigDecimal field. */
    public static class BigDecimalPojo {
        public BigDecimal amount;

        public BigDecimalPojo() {}
    }

    /** Test POJO with LocalDate field. */
    public static class LocalDatePojo {
        public LocalDate date;

        public LocalDatePojo() {}
    }

    /** Test POJO with LocalTime field. */
    public static class LocalTimePojo {
        public LocalTime time;

        public LocalTimePojo() {}
    }

    /** Test POJO with LocalDateTime field. */
    public static class LocalDateTimePojo {
        public LocalDateTime timestamp;

        public LocalDateTimePojo() {}
    }

    /** Test POJO with Instant field. */
    public static class InstantPojo {
        public Instant timestamp;

        public InstantPojo() {}
    }

    /** Test POJO with OffsetDateTime field. */
    public static class OffsetDateTimePojo {
        public OffsetDateTime timestamp;

        public OffsetDateTimePojo() {}
    }

    /** Test POJO with Integer[] field. */
    public static class IntArrayPojo {
        public Integer[] items;

        public IntArrayPojo() {}
    }

    /** Test POJO with List field. */
    public static class ListPojo {
        public List<Integer> items;

        public ListPojo() {}
    }

    /** Test POJO with Map field. */
    public static class MapPojo {
        public Map<String, Integer> mapping;

        public MapPojo() {}
    }

    /** Test POJO with nested POJO field. */
    public static class NestedPojo {
        public AllFieldsPojo nested;

        public NestedPojo() {}
    }

    /** Test POJO with Enum field. */
    public static class EnumPojo {
        public StatusEnum status;

        public EnumPojo() {}
    }

    /** Test enum for compatibility testing. */
    public enum StatusEnum {
        ACTIVE,
        INACTIVE
    }
}
