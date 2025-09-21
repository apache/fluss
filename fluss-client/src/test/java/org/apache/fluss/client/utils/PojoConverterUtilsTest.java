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

package org.apache.fluss.client.utils;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PojoConverterUtils}. */
public class PojoConverterUtilsTest {

    private TestPojo createTestPojo() {
        return new TestPojo(
                true,
                (byte) 42,
                (short) 1234,
                123456,
                9876543210L,
                3.14f,
                2.71828,
                "Hello, World!",
                new byte[] {1, 2, 3, 4, 5},
                new BigDecimal("123.45"),
                LocalDate.of(2025, 7, 23),
                LocalTime.of(15, 1, 30),
                LocalDateTime.of(2025, 7, 23, 15, 1, 30),
                Instant.parse("2025-07-23T15:01:30Z"),
                OffsetDateTime.of(2025, 7, 23, 15, 1, 30, 0, ZoneOffset.UTC));
    }

    @Test
    public void testToRow() {
        PojoConverterUtils<TestPojo> converter =
                PojoConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        TestPojo pojo = createTestPojo();

        GenericRow row = converter.toRow(pojo);

        // Verify the row
        assertThat(row).isNotNull();
        assertThat(row.getFieldCount()).isEqualTo(15);
        assertThat(row.getBoolean(0)).isEqualTo(true);
        assertThat(row.getByte(1)).isEqualTo((byte) 42);
        assertThat(row.getShort(2)).isEqualTo((short) 1234);
        assertThat(row.getInt(3)).isEqualTo(123456);
        assertThat(row.getLong(4)).isEqualTo(9876543210L);
        assertThat(row.getFloat(5)).isEqualTo(3.14f);
        assertThat(row.getDouble(6)).isEqualTo(2.71828);
        assertThat(row.getString(7).toString()).isEqualTo("Hello, World!");
        assertThat(row.getBytes(8)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(row.getDecimal(9, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("123.45"));
        assertThat(row.getInt(10)).isEqualTo(LocalDate.of(2025, 7, 23).toEpochDay());
        assertThat(row.getInt(11)).isEqualTo(LocalTime.of(15, 1, 30).toNanoOfDay() / 1_000_000);
        assertThat(row.getTimestampNtz(12, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2025, 7, 23, 15, 1, 30));
        assertThat(row.getTimestampLtz(13, 6).toInstant())
                .isEqualTo(Instant.parse("2025-07-23T15:01:30Z"));
        assertThat(row.getTimestampLtz(14, 6).toInstant())
                .isEqualTo(
                        OffsetDateTime.of(2025, 7, 23, 15, 1, 30, 0, ZoneOffset.UTC).toInstant());
    }

    @Test
    public void testFromRow() {
        PojoConverterUtils<TestPojo> converter =
                PojoConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        TestPojo originalPojo = createTestPojo();

        GenericRow row = converter.toRow(originalPojo);

        TestPojo convertedPojo = converter.fromRow(row);
        // Verify the POJO
        assertThat(convertedPojo).isNotNull();
        assertThat(convertedPojo).isEqualTo(originalPojo);
    }

    @Test
    public void testNullValues() {
        PojoConverterUtils<TestPojo> converter =
                PojoConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        assertThat(converter.toRow(null)).isNull();

        assertThat(converter.fromRow(null)).isNull();

        GenericRow row = new GenericRow(15);

        TestPojo pojo = converter.fromRow(row);

        // Verify the POJO
        assertThat(pojo).isNotNull();
        assertThat(pojo.booleanField).isNull();
        assertThat(pojo.byteField).isNull();
        assertThat(pojo.shortField).isNull();
        assertThat(pojo.intField).isNull();
        assertThat(pojo.longField).isNull();
        assertThat(pojo.floatField).isNull();
        assertThat(pojo.doubleField).isNull();
        assertThat(pojo.stringField).isNull();
        assertThat(pojo.bytesField).isNull();
        assertThat(pojo.decimalField).isNull();
        assertThat(pojo.dateField).isNull();
        assertThat(pojo.timeField).isNull();
        assertThat(pojo.timestampField).isNull();
        assertThat(pojo.timestampLtzField).isNull();
        assertThat(pojo.offsetDateTimeField).isNull();
    }

    @Test
    public void testCaching() {
        PojoConverterUtils<TestPojo> converter1 =
                PojoConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        PojoConverterUtils<TestPojo> converter2 =
                PojoConverterUtils.getConverter(TestPojo.class, createTestPojoRowType());

        // As caching is removed, subsequent calls should produce new instances
        assertThat(converter2).isNotSameAs(converter1);
    }

    @Test
    public void testPartialPojo() {
        // Create a row type with more fields than the POJO
        RowType rowType =
                RowType.builder()
                        .field("booleanField", DataTypes.BOOLEAN())
                        .field("intField", DataTypes.INT())
                        .field("stringField", DataTypes.STRING())
                        .field("extraField", DataTypes.DOUBLE()) // Not in POJO
                        .build();

        // Expect an IllegalArgumentException when creating a converter with a field not in the POJO
        assertThatThrownBy(() -> PojoConverterUtils.getConverter(PartialTestPojo.class, rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field 'extraField' not found in POJO class");
    }

    @Test
    public void testRowWithMissingFields() {
        // Create a row type with fewer fields than the POJO
        RowType rowType =
                RowType.builder()
                        .field("booleanField", DataTypes.BOOLEAN())
                        .field("intField", DataTypes.INT())
                        .field("stringField", DataTypes.STRING())
                        .build();

        PojoConverterUtils<TestPojo> converter =
                PojoConverterUtils.getConverter(TestPojo.class, rowType);

        TestPojo pojo = createTestPojo();

        GenericRow row = converter.toRow(pojo);

        assertThat(row).isNotNull();
        assertThat(row.getFieldCount()).isEqualTo(3);
        assertThat(row.getBoolean(0)).isEqualTo(true);
        assertThat(row.getInt(1)).isEqualTo(123456);
        assertThat(row.getString(2).toString()).isEqualTo("Hello, World!");

        TestPojo convertedPojo = converter.fromRow(row);

        assertThat(convertedPojo).isNotNull();
        assertThat(convertedPojo.booleanField).isEqualTo(true);
        assertThat(convertedPojo.intField).isEqualTo(123456);
        assertThat(convertedPojo.stringField).isEqualTo("Hello, World!");
        // Other fields are null
        assertThat(convertedPojo.byteField).isNull();
        assertThat(convertedPojo.shortField).isNull();
        assertThat(convertedPojo.longField).isNull();
        assertThat(convertedPojo.floatField).isNull();
        assertThat(convertedPojo.doubleField).isNull();
        assertThat(convertedPojo.bytesField).isNull();
        assertThat(convertedPojo.decimalField).isNull();
        assertThat(convertedPojo.dateField).isNull();
        assertThat(convertedPojo.timeField).isNull();
        assertThat(convertedPojo.timestampField).isNull();
        assertThat(convertedPojo.timestampLtzField).isNull();
        assertThat(convertedPojo.offsetDateTimeField).isNull();
    }

    @Test
    public void testFieldAccessFailureThrows() {
        RowType rowType = RowType.builder().field("intField", DataTypes.INT()).build();

        PojoConverterUtils<FinalFieldPojo> converter =
                PojoConverterUtils.getConverter(FinalFieldPojo.class, rowType);

        GenericRow row = new GenericRow(1);
        row.setField(0, 42);

        assertThatThrownBy(() -> converter.fromRow(row))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to set field")
                .hasMessageContaining("intField");
    }

    @Test
    public void testNoDefaultConstructorPojoThrows() {
        RowType rowType = RowType.builder().field("intField", DataTypes.INT()).build();
        assertThatThrownBy(
                        () ->
                                PojoConverterUtils.getConverter(
                                        NoDefaultConstructorPojo.class, rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must have a default constructor");
    }

    @Test
    public void testFromRowThrowsWhenDefaultConstructorThrows() {
        RowType rowType = RowType.builder().field("intField", DataTypes.INT()).build();
        PojoConverterUtils<ThrowingCtorPojo> converter =
                PojoConverterUtils.getConverter(ThrowingCtorPojo.class, rowType);
        GenericRow row = new GenericRow(1);
        row.setField(0, 1);
        assertThatThrownBy(() -> converter.fromRow(row))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to instantiate POJO class")
                .hasMessageContaining(ThrowingCtorPojo.class.getName());
    }

    // ----------------------- Exception tests for strict converter behavior -----------------------

    @Test
    public void testToRowThrowsForNonBigDecimal() {
        RowType rowType = RowType.builder().field("decimalField", DataTypes.DECIMAL(10, 2)).build();
        PojoConverterUtils<DecimalWrongTypePojo> converter =
                PojoConverterUtils.getConverter(DecimalWrongTypePojo.class, rowType);
        DecimalWrongTypePojo pojo = new DecimalWrongTypePojo("not-a-decimal");
        assertThatThrownBy(() -> converter.toRow(pojo))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a BigDecimal")
                .hasMessageContaining("decimalField");
    }

    @Test
    public void testToRowThrowsForNonLocalDate() {
        RowType rowType = RowType.builder().field("dateField", DataTypes.DATE()).build();
        PojoConverterUtils<DateWrongTypePojo> converter =
                PojoConverterUtils.getConverter(DateWrongTypePojo.class, rowType);
        DateWrongTypePojo pojo = new DateWrongTypePojo("2025-01-01");
        assertThatThrownBy(() -> converter.toRow(pojo))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a LocalDate")
                .hasMessageContaining("dateField");
    }

    @Test
    public void testToRowThrowsForNonLocalTime() {
        RowType rowType = RowType.builder().field("timeField", DataTypes.TIME()).build();
        PojoConverterUtils<TimeWrongTypePojo> converter =
                PojoConverterUtils.getConverter(TimeWrongTypePojo.class, rowType);
        TimeWrongTypePojo pojo = new TimeWrongTypePojo("15:00:00");
        assertThatThrownBy(() -> converter.toRow(pojo))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a LocalTime")
                .hasMessageContaining("timeField");
    }

    @Test
    public void testToRowThrowsForNonLocalDateTime() {
        RowType rowType = RowType.builder().field("timestampField", DataTypes.TIMESTAMP()).build();
        PojoConverterUtils<TimestampWrongTypePojo> converter =
                PojoConverterUtils.getConverter(TimestampWrongTypePojo.class, rowType);
        TimestampWrongTypePojo pojo = new TimestampWrongTypePojo("2025-01-01T12:00:00");
        assertThatThrownBy(() -> converter.toRow(pojo))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a LocalDateTime")
                .hasMessageContaining("timestampField");
    }

    @Test
    public void testToRowThrowsForNonInstantOrOffsetDateTime() {
        RowType rowType =
                RowType.builder().field("timestampLtzField", DataTypes.TIMESTAMP_LTZ()).build();
        PojoConverterUtils<TimestampLtzWrongTypePojo> converter =
                PojoConverterUtils.getConverter(TimestampLtzWrongTypePojo.class, rowType);
        TimestampLtzWrongTypePojo pojo = new TimestampLtzWrongTypePojo("2025-01-01T12:00:00Z");
        assertThatThrownBy(() -> converter.toRow(pojo))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not an Instant or OffsetDateTime")
                .hasMessageContaining("timestampLtzField");
    }

    @Test
    public void testFromRowThrowsForTimestampLtzUnsupportedTargetType() {
        RowType rowType =
                RowType.builder().field("timestampLtzField", DataTypes.TIMESTAMP_LTZ()).build();

        // Create a valid row using a POJO with Instant
        ValidTimestampLtzInstantPojo valid =
                new ValidTimestampLtzInstantPojo(Instant.parse("2025-07-23T15:01:30Z"));
        PojoConverterUtils<ValidTimestampLtzInstantPojo> validConverter =
                PojoConverterUtils.getConverter(ValidTimestampLtzInstantPojo.class, rowType);
        GenericRow row = validConverter.toRow(valid);

        // Try to read with a POJO that uses unsupported LocalDateTime for TIMESTAMP_LTZ
        PojoConverterUtils<TimestampLtzLocalDateTimePojo> invalidConverter =
                PojoConverterUtils.getConverter(TimestampLtzLocalDateTimePojo.class, rowType);

        assertThatThrownBy(() -> invalidConverter.fromRow(row))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot convert from TimestampData")
                .hasMessageContaining("timestampLtzField");
    }

    @Test
    public void testUnsupportedSchemaFieldTypeThrows() {
        RowType rowType =
                RowType.builder()
                        .field("mapField", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();
        assertThatThrownBy(() -> PojoConverterUtils.getConverter(MapPojo.class, rowType))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported field type")
                .hasMessageContaining("MAP")
                .hasMessageContaining("mapField");
    }

    // Helper POJOs for negative tests
    /** Helper POJO to trigger DECIMAL field type mismatch. */
    public static class DecimalWrongTypePojo {
        private String decimalField;

        public DecimalWrongTypePojo(String decimalField) {
            this.decimalField = decimalField;
        }
    }

    /** Helper POJO to trigger DATE field type mismatch. */
    public static class DateWrongTypePojo {
        private String dateField;

        public DateWrongTypePojo(String dateField) {
            this.dateField = dateField;
        }
    }

    /** Helper POJO to trigger TIME field type mismatch. */
    public static class TimeWrongTypePojo {
        private String timeField;

        public TimeWrongTypePojo(String timeField) {
            this.timeField = timeField;
        }
    }

    /** Helper POJO to trigger TIMESTAMP (without time zone) field type mismatch. */
    public static class TimestampWrongTypePojo {
        private String timestampField;

        public TimestampWrongTypePojo(String timestampField) {
            this.timestampField = timestampField;
        }
    }

    /** Helper POJO to trigger TIMESTAMP_LTZ field type mismatch. */
    public static class TimestampLtzWrongTypePojo {
        private String timestampLtzField;

        public TimestampLtzWrongTypePojo(String timestampLtzField) {
            this.timestampLtzField = timestampLtzField;
        }
    }

    /** Valid helper POJO using Instant for TIMESTAMP_LTZ conversions. */
    public static class ValidTimestampLtzInstantPojo {
        private Instant timestampLtzField;

        public ValidTimestampLtzInstantPojo(Instant timestampLtzField) {
            this.timestampLtzField = timestampLtzField;
        }
    }

    /** Helper POJO using unsupported LocalDateTime for TIMESTAMP_LTZ to trigger errors. */
    public static class TimestampLtzLocalDateTimePojo {
        private LocalDateTime timestampLtzField;

        public TimestampLtzLocalDateTimePojo() {}
    }

    /** Helper POJO used with an unsupported MAP schema field to assert constructor failure. */
    public static class MapPojo {
        private Map<String, Integer> mapField;

        public MapPojo() {}
    }

    /** Test POJO class with various field types. */
    public static class TestPojo {
        private Boolean booleanField;
        private Byte byteField;
        private Short shortField;
        private Integer intField;
        private Long longField;
        private Float floatField;
        private Double doubleField;
        private String stringField;
        private byte[] bytesField;
        private BigDecimal decimalField;
        private LocalDate dateField;
        private LocalTime timeField;
        private LocalDateTime timestampField;
        private Instant timestampLtzField;
        private OffsetDateTime offsetDateTimeField;

        public TestPojo() {}

        public TestPojo(
                Boolean booleanField,
                Byte byteField,
                Short shortField,
                Integer intField,
                Long longField,
                Float floatField,
                Double doubleField,
                String stringField,
                byte[] bytesField,
                BigDecimal decimalField,
                LocalDate dateField,
                LocalTime timeField,
                LocalDateTime timestampField,
                Instant timestampLtzField,
                OffsetDateTime offsetDateTimeField) {
            this.booleanField = booleanField;
            this.byteField = byteField;
            this.shortField = shortField;
            this.intField = intField;
            this.longField = longField;
            this.floatField = floatField;
            this.doubleField = doubleField;
            this.stringField = stringField;
            this.bytesField = bytesField;
            this.decimalField = decimalField;
            this.dateField = dateField;
            this.timeField = timeField;
            this.timestampField = timestampField;
            this.timestampLtzField = timestampLtzField;
            this.offsetDateTimeField = offsetDateTimeField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestPojo testPojo = (TestPojo) o;
            return Objects.equals(booleanField, testPojo.booleanField)
                    && Objects.equals(byteField, testPojo.byteField)
                    && Objects.equals(shortField, testPojo.shortField)
                    && Objects.equals(intField, testPojo.intField)
                    && Objects.equals(longField, testPojo.longField)
                    && Objects.equals(floatField, testPojo.floatField)
                    && Objects.equals(doubleField, testPojo.doubleField)
                    && Objects.equals(stringField, testPojo.stringField)
                    && Arrays.equals(bytesField, testPojo.bytesField)
                    && Objects.equals(decimalField, testPojo.decimalField)
                    && Objects.equals(dateField, testPojo.dateField)
                    && Objects.equals(timeField, testPojo.timeField)
                    && Objects.equals(timestampField, testPojo.timestampField)
                    && Objects.equals(timestampLtzField, testPojo.timestampLtzField)
                    && Objects.equals(offsetDateTimeField, testPojo.offsetDateTimeField);
        }

        @Override
        public int hashCode() {
            int result =
                    Objects.hash(
                            booleanField,
                            byteField,
                            shortField,
                            intField,
                            longField,
                            floatField,
                            doubleField,
                            stringField,
                            decimalField,
                            dateField,
                            timeField,
                            timestampField,
                            timestampLtzField,
                            offsetDateTimeField);
            result = 31 * result + Arrays.hashCode(bytesField);
            return result;
        }
    }

    /** Test POJO class with missing fields compared to the row type. */
    public static class PartialTestPojo {
        private boolean booleanField;
        private int intField;
        private String stringField;

        public PartialTestPojo() {}

        public PartialTestPojo(boolean booleanField, int intField, String stringField) {
            this.booleanField = booleanField;
            this.intField = intField;
            this.stringField = stringField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartialTestPojo that = (PartialTestPojo) o;
            return booleanField == that.booleanField
                    && intField == that.intField
                    && Objects.equals(stringField, that.stringField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(booleanField, intField, stringField);
        }
    }

    /** Creates a row type for the TestPojo class. */
    private RowType createTestPojoRowType() {
        return RowType.builder()
                .field("booleanField", DataTypes.BOOLEAN())
                .field("byteField", DataTypes.TINYINT())
                .field("shortField", DataTypes.SMALLINT())
                .field("intField", DataTypes.INT())
                .field("longField", DataTypes.BIGINT())
                .field("floatField", DataTypes.FLOAT())
                .field("doubleField", DataTypes.DOUBLE())
                .field("stringField", DataTypes.STRING())
                .field("bytesField", DataTypes.BYTES())
                .field("decimalField", DataTypes.DECIMAL(10, 2))
                .field("dateField", DataTypes.DATE())
                .field("timeField", DataTypes.TIME())
                .field("timestampField", DataTypes.TIMESTAMP())
                .field("timestampLtzField", DataTypes.TIMESTAMP_LTZ())
                .field("offsetDateTimeField", DataTypes.TIMESTAMP_LTZ())
                .build();
    }

    /** POJO with a final field to trigger IllegalAccessException on setting. */
    public static class FinalFieldPojo {
        private final int intField;

        public FinalFieldPojo() {
            this.intField = 0;
        }
    }

    /** POJO without a default constructor (illegal for ConverterUtils). */
    public static class NoDefaultConstructorPojo {
        private int intField;

        public NoDefaultConstructorPojo(int intField) {
            this.intField = intField;
        }
    }

    /** POJO whose default constructor throws to test instantiation error propagation. */
    public static class ThrowingCtorPojo {
        private Integer intField;

        public ThrowingCtorPojo() {
            throw new RuntimeException("ctor failure");
        }
    }
}
