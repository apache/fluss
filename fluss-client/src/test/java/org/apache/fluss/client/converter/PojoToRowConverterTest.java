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
