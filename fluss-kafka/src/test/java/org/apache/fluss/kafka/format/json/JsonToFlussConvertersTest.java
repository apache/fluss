/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.format.json;

import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.DecimalNode;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies strict scalar conversion without transport or Arrow dependencies. */
class JsonToFlussConvertersTest {
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @Test
    void testNumericRepresentationsAndExactDecimals() throws Exception {
        assertThat(convert(DataTypes.BOOLEAN(), "true")).isEqualTo(true);
        assertThat(convert(DataTypes.TINYINT(), "-128")).isEqualTo((byte) -128);
        assertThat(convert(DataTypes.SMALLINT(), "32767")).isEqualTo((short) 32767);
        assertThat(convert(DataTypes.INT(), "2147483647")).isEqualTo(Integer.MAX_VALUE);
        assertThat(convert(DataTypes.BIGINT(), "9223372036854775807")).isEqualTo(Long.MAX_VALUE);
        assertThat(convert(DataTypes.FLOAT(), "1.25")).isEqualTo(1.25F);
        assertThat(convert(DataTypes.DOUBLE(), "2.5")).isEqualTo(2.5D);
        Decimal decimal =
                (Decimal) convert(DataTypes.DECIMAL(30, 10), "12345678901234567890.1234567890");
        assertThat(decimal.toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345678901234567890.1234567890"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "-"})
    void testFloatRoundsDirectlyWithoutDoubleRounding(String sign) throws Exception {
        float direction = sign.isEmpty() ? 1.0F : -1.0F;
        assertThat(convert(DataTypes.FLOAT(), sign + "1.0000000596046448"))
                .isEqualTo(direction * 1.0000001F);
        assertThat(convert(DataTypes.FLOAT(), sign + "1.0000000596046447")).isEqualTo(direction);
        // The integer and decimal nodes must both round to the largest finite float.
        String belowOverflow = "340282356779733661637539395458142568447";
        assertThat(convert(DataTypes.FLOAT(), sign + belowOverflow))
                .isEqualTo(direction * Float.MAX_VALUE);
        assertThat(convert(DataTypes.FLOAT(), sign + belowOverflow + ".0"))
                .isEqualTo(direction * Float.MAX_VALUE);
        assertThatThrownBy(
                        () ->
                                convert(
                                        DataTypes.FLOAT(),
                                        sign + "340282356779733661637539395458142568448"))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("$.field")
                .hasMessageContaining("overflowing number");
    }

    @Test
    void testStringsBinaryAndTemporalRepresentations() throws Exception {
        assertThat(convert(DataTypes.CHAR(2), "\"😀a\"")).isEqualTo(BinaryString.fromString("😀a"));
        assertThat(convert(DataTypes.STRING(), "\"文本\"")).isEqualTo(BinaryString.fromString("文本"));
        assertThat((byte[]) convert(DataTypes.BINARY(2), "\"AQI=\"")).containsExactly(1, 2);
        assertThat((byte[]) convert(DataTypes.BYTES(), "\"\"")).isEmpty();
        assertThat(convert(DataTypes.DATE(), "\"1970-01-02\""))
                .isEqualTo((int) LocalDate.of(1970, 1, 2).toEpochDay());
        assertThat(convert(DataTypes.TIME(3), "\"00:00:01.123\"")).isEqualTo(1123);
        assertThat(convert(DataTypes.TIMESTAMP(3), "\"2026-09-11T12:00:00.123\""))
                .isEqualTo(
                        TimestampNtz.fromLocalDateTime(
                                LocalDateTime.of(2026, 9, 11, 12, 0, 0, 123000000)));
        assertThat(convert(DataTypes.TIMESTAMP_LTZ(3), "\"2026-09-11T12:00:00.123+08:00\""))
                .isEqualTo(TimestampLtz.fromInstant(Instant.parse("2026-09-11T04:00:00.123Z")));
    }

    @Test
    void testMissingAndNullRespectNullability() throws Exception {
        assertThat(JsonToFlussConverters.create(DataTypes.INT()).convert(null, "$.field")).isNull();
        assertThat(convert(DataTypes.INT(), "null")).isNull();
        assertThatThrownBy(
                        () ->
                                JsonToFlussConverters.create(DataTypes.INT().copy(false))
                                        .convert(null, "$.field"))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("$.field")
                .hasMessageContaining("missing or null");
        assertThatThrownBy(() -> convert(DataTypes.INT().copy(false), "null"))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("missing or null");
    }

    @Test
    void testDecimalBoundsBeforeRescaling() throws Exception {
        for (int scale : new int[] {100_000_000, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            assertThatThrownBy(
                            () ->
                                    JsonToFlussConverters.create(DataTypes.DECIMAL(5, 2))
                                            .convert(
                                                    DecimalNode.valueOf(
                                                            new BigDecimal(BigInteger.ONE, scale)),
                                                    "$.amount"))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("$.amount")
                    .hasMessageContaining(scale > 0 ? "scale" : "precision");
            Decimal zero =
                    (Decimal)
                            JsonToFlussConverters.create(DataTypes.DECIMAL(5, 2))
                                    .convert(
                                            DecimalNode.valueOf(
                                                    new BigDecimal(BigInteger.ZERO, scale)),
                                            "$.amount");
            assertThat(zero.toBigDecimal()).isEqualTo(new BigDecimal("0.00"));
        }
        for (String json : new String[] {"12.3400", "1.234e1", "-12.3400", "1e2", "1e-2"}) {
            Decimal decimal = (Decimal) convert(DataTypes.DECIMAL(5, 2), json);
            assertThat(decimal.toBigDecimal()).isEqualByComparingTo(new BigDecimal(json));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"\\uD800", "\\uDC00", "a\\uD800b", "\\uD800\\uD800", "\\uDC00\\uD800"})
    void testRejectsUnpairedSurrogates(String escaped) {
        for (DataType type : new DataType[] {DataTypes.STRING(), DataTypes.CHAR(8)}) {
            assertThatThrownBy(() -> convert(type, "\"" + escaped + "\""))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("$.field")
                    .hasMessageContaining("surrogate");
        }
    }

    @Test
    void testAcceptsPairedSurrogatesAndLiteralEscape() throws Exception {
        String escapedEmoji = "\"\\uD83D\\uDE00\"";
        assertThat(convert(DataTypes.STRING(), escapedEmoji))
                .isEqualTo(BinaryString.fromString("😀"));
        assertThat(convert(DataTypes.CHAR(1), escapedEmoji))
                .isEqualTo(BinaryString.fromString("😀"));
        assertThat(convert(DataTypes.STRING(), "\"\\\\uD800\""))
                .isEqualTo(BinaryString.fromString("\\uD800"));
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 6, 9})
    void testTimestampStorageBoundaries(int precision) throws Exception {
        long unitsPerSecond = precision == 3 ? 1_000 : precision == 6 ? 1_000_000 : 1_000_000_000;
        long nanosPerUnit = 1_000_000_000 / unitsPerSecond;
        for (long units : new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
            Instant instant =
                    Instant.ofEpochSecond(
                            Math.floorDiv(units, unitsPerSecond),
                            Math.floorMod(units, unitsPerSecond) * nanosPerUnit);
            for (boolean withZone : new boolean[] {false, true}) {
                Object actual =
                        convert(
                                timestampType(precision, withZone),
                                timestampJson(instant, withZone));
                Object expected =
                        withZone
                                ? TimestampLtz.fromEpochMillis(
                                        instant.toEpochMilli(), instant.getNano() % 1_000_000)
                                : TimestampNtz.fromMillis(
                                        instant.toEpochMilli(), instant.getNano() % 1_000_000);
                assertThat(actual).isEqualTo(expected);
                if (withZone) {
                    assertThat(((TimestampLtz) actual).toInstant()).isEqualTo(instant);
                } else {
                    assertThat(((TimestampNtz) actual).toLocalDateTime())
                            .isEqualTo(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
                }
            }
        }
        for (long limit : new long[] {Long.MIN_VALUE, Long.MAX_VALUE}) {
            Instant outside =
                    Instant.ofEpochSecond(
                                    Math.floorDiv(limit, unitsPerSecond),
                                    Math.floorMod(limit, unitsPerSecond) * nanosPerUnit)
                            .plusNanos(limit < 0 ? -nanosPerUnit : nanosPerUnit);
            for (boolean withZone : new boolean[] {false, true}) {
                assertThatThrownBy(
                                () ->
                                        convert(
                                                timestampType(precision, withZone),
                                                timestampJson(outside, withZone)))
                        .isInstanceOf(KafkaRecordEncodingException.class)
                        .hasMessageContaining("$.field")
                        .hasMessageContaining("timestamp exceeds storage range");
            }
        }
    }

    @Test
    void testRejectsTimestampOverflowAndKeepsRepresentableDates() throws Exception {
        for (boolean withZone : new boolean[] {false, true}) {
            String suffix = withZone ? "Z" : "";
            for (String date : new String[] {"3000-01-01T00:00:00", "1600-01-01T00:00:00"}) {
                assertThatThrownBy(
                                () ->
                                        convert(
                                                timestampType(9, withZone),
                                                "\"" + date + suffix + "\""))
                        .isInstanceOf(KafkaRecordEncodingException.class)
                        .hasMessageContaining("timestamp exceeds storage range");
                assertThat(convert(timestampType(6, withZone), "\"" + date + suffix + "\""))
                        .isNotNull();
            }
            assertThatThrownBy(
                            () ->
                                    convert(
                                            timestampType(3, withZone),
                                            "\"+999999999-01-01T00:00:00" + suffix + "\""))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("timestamp exceeds storage range");
        }
    }

    private static DataType timestampType(int precision, boolean withZone) {
        return withZone ? DataTypes.TIMESTAMP_LTZ(precision) : DataTypes.TIMESTAMP(precision);
    }

    private static String timestampJson(Instant instant, boolean withZone) {
        return "\""
                + (withZone
                        ? instant.toString()
                        : LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toString())
                + "\"";
    }

    @ParameterizedTest
    @MethodSource("invalidScalars")
    void testRejectsCoercionOverflowAndPrecisionLoss(DataType type, String json) {
        assertThatThrownBy(() -> convert(type, json))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("$.field");
    }

    private static Stream<Arguments> invalidScalars() {
        return Stream.of(
                Arguments.of(DataTypes.BOOLEAN(), "1"),
                Arguments.of(DataTypes.INT(), "\"1\""),
                Arguments.of(DataTypes.INT(), "1.0"),
                Arguments.of(DataTypes.INT(), "2147483648"),
                Arguments.of(DataTypes.TINYINT(), "128"),
                Arguments.of(DataTypes.SMALLINT(), "-32769"),
                Arguments.of(DataTypes.BIGINT(), "9223372036854775808"),
                Arguments.of(DataTypes.FLOAT(), "1e39"),
                Arguments.of(DataTypes.DOUBLE(), "1e309"),
                Arguments.of(DataTypes.DECIMAL(5, 2), "1.234"),
                Arguments.of(DataTypes.DECIMAL(5, 2), "1000"),
                Arguments.of(DataTypes.CHAR(1), "\"ab\""),
                Arguments.of(DataTypes.STRING(), "false"),
                Arguments.of(DataTypes.BINARY(2), "\"AQ==\""),
                Arguments.of(DataTypes.BYTES(), "\"!invalid!\""),
                Arguments.of(DataTypes.DATE(), "\"2026-02-30\""),
                Arguments.of(DataTypes.TIME(0), "\"00:00:00.001\""),
                Arguments.of(DataTypes.TIME(3), "\"00:00:00.0001\""),
                Arguments.of(DataTypes.TIMESTAMP(3), "\"2026-09-11T00:00:00.0001\""),
                Arguments.of(DataTypes.TIMESTAMP_LTZ(3), "\"2026-09-11T00:00:00\""));
    }

    @Test
    void testComplexTypesRemainUnsupportedAtThisStage() {
        assertThatThrownBy(() -> JsonToFlussConverters.create(DataTypes.ARRAY(DataTypes.INT())))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("does not support");
    }

    private static Object convert(DataType type, String json) throws Exception {
        return JsonToFlussConverters.create(type).convert(MAPPER.readTree(json), "$.field");
    }
}
