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

import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.DecimalNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.DoubleNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.FloatNode;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
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
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies scalar conversion, normalization, and storage bounds. */
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFloatingPointPreservesZeroSignIncludingUnderflow(boolean negative) {
        double zero = negative ? -0.0D : 0.0D;
        for (JsonNode node :
                new JsonNode[] {
                    DoubleNode.valueOf(zero),
                    FloatNode.valueOf((float) zero),
                    DecimalNode.valueOf(new BigDecimal(negative ? "-1e-1000" : "1e-1000"))
                }) {
            float floatValue =
                    (float) JsonToFlussConverters.create(DataTypes.FLOAT()).convert(node, "$.f");
            double doubleValue =
                    (double) JsonToFlussConverters.create(DataTypes.DOUBLE()).convert(node, "$.d");
            assertThat(Float.floatToRawIntBits(floatValue))
                    .isEqualTo(Float.floatToRawIntBits((float) zero));
            assertThat(Double.doubleToRawLongBits(doubleValue))
                    .isEqualTo(Double.doubleToRawLongBits(zero));
        }
    }

    @ParameterizedTest
    @MethodSource("nonFiniteNumbers")
    void testRejectsNonFiniteNumericNodes(JsonNode node) {
        for (DataType type :
                new DataType[] {DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.DECIMAL(38, 18)}) {
            assertThatThrownBy(() -> JsonToFlussConverters.create(type).convert(node, "$.number"))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("$.number")
                    .hasMessageContaining(type.asSummaryString());
        }
    }

    private static Stream<JsonNode> nonFiniteNumbers() {
        return Stream.of(
                DoubleNode.valueOf(Double.NaN),
                DoubleNode.valueOf(Double.NEGATIVE_INFINITY),
                DoubleNode.valueOf(Double.POSITIVE_INFINITY),
                FloatNode.valueOf(Float.NaN),
                FloatNode.valueOf(Float.NEGATIVE_INFINITY),
                FloatNode.valueOf(Float.POSITIVE_INFINITY));
    }

    @ParameterizedTest
    @ValueSource(strings = {"A", "AQ", "AQI", "AQ===", "AQ==x", "A Q==", "_w=="})
    void testBase64RejectsInvalidPaddingAlphabetAndEmbeddedWhitespace(String value) {
        for (DataType type : new DataType[] {DataTypes.BYTES(), DataTypes.BINARY(1)}) {
            assertThatThrownBy(
                            () ->
                                    JsonToFlussConverters.create(type)
                                            .convert(
                                                    MAPPER.getNodeFactory().textNode(value), "$.b"))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("$.b")
                    .hasMessageContaining("invalid Base64");
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "1969-12-31T23:59:59.999999999Z",
                "1970-01-01T05:44:59.999999999+05:45",
                "1969-12-31T20:29:59.999999999-03:30"
            })
    void testTimestampLtzNormalizesOffsetsBeforeEpoch(String value) throws Exception {
        TimestampLtz timestamp =
                (TimestampLtz) convert(DataTypes.TIMESTAMP_LTZ(9), "\"" + value + "\"");
        assertThat(timestamp.toInstant()).isEqualTo(Instant.EPOCH.minusNanos(1));
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
        assertThat(
                        JsonToFlussConverters.create(DataTypes.INT())
                                .convert(MAPPER.missingNode(), "$.field"))
                .isNull();
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
            JsonNode value = DecimalNode.valueOf(new BigDecimal(BigInteger.ONE, scale));
            JsonToFlussConverter converter = JsonToFlussConverters.create(DataTypes.DECIMAL(5, 2));
            if (scale > 0) {
                assertThat(((Decimal) converter.convert(value, "$.amount")).toBigDecimal())
                        .isEqualTo(new BigDecimal("0.00"));
            } else {
                assertThatThrownBy(() -> converter.convert(value, "$.amount"))
                        .isInstanceOf(KafkaRecordEncodingException.class)
                        .hasMessageContaining("$.amount")
                        .hasMessageContaining("precision");
            }
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
    void testUnpairedSurrogatesFollowBinaryStringEncoding(String escaped) throws Exception {
        for (DataType type : new DataType[] {DataTypes.STRING(), DataTypes.CHAR(8)}) {
            String json = "\"" + escaped + "\"";
            assertThat(convert(type, json))
                    .isEqualTo(BinaryString.fromString(MAPPER.readTree(json).textValue()));
        }
    }

    @ParameterizedTest
    @MethodSource("roundedDecimals")
    void testDecimalRoundsHalfUp(String json, String expected) throws Exception {
        assertThat(((Decimal) convert(DataTypes.DECIMAL(5, 2), json)).toBigDecimal())
                .isEqualTo(new BigDecimal(expected));
    }

    private static Stream<Arguments> roundedDecimals() {
        return Stream.of(
                Arguments.of("12.345", "12.35"),
                Arguments.of("-12.345", "-12.35"),
                Arguments.of("999.994", "999.99"),
                Arguments.of("0.005", "0.01"),
                Arguments.of("-0.005", "-0.01"),
                Arguments.of("0.0049", "0.00"),
                Arguments.of("-1e-1000", "0.00"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "123", "1.25", "[1,null,\"x\"]", "{\"a\":true}"})
    void testStringsAcceptOtherJsonTypes(String json) throws Exception {
        for (DataType type : new DataType[] {DataTypes.STRING(), DataTypes.CHAR(1)}) {
            assertThat(convert(type, json)).isEqualTo(BinaryString.fromString(json));
        }
    }

    @Test
    void testCharKeepsFullStringAndBinaryNormalizesWidth() throws Exception {
        assertThat(convert(DataTypes.CHAR(1), "\"ab😀\""))
                .isEqualTo(BinaryString.fromString("ab😀"));
        assertThat((byte[]) convert(DataTypes.BINARY(2), "\"\"")).containsExactly(0, 0);
        assertThat((byte[]) convert(DataTypes.BINARY(2), "\"AQ==\"")).containsExactly(1, 0);
        assertThat((byte[]) convert(DataTypes.BINARY(2), "\"AQID\"")).containsExactly(1, 2);
        assertThat((byte[]) convert(DataTypes.BYTES(), "\"AQID\"")).containsExactly(1, 2, 3);
    }

    @ParameterizedTest
    @ValueSource(strings = {"T", " "})
    void testTimestampSeparatorsWithAndWithoutFractions(String separator) throws Exception {
        for (String fraction : new String[] {"", ".123456"}) {
            LocalDateTime expected = LocalDateTime.parse("2026-10-25T12:34:56" + fraction);
            String local = "2026-10-25" + separator + "12:34:56" + fraction;
            assertThat(convert(DataTypes.TIMESTAMP(6), "\"" + local + "\""))
                    .isEqualTo(TimestampNtz.fromLocalDateTime(expected));
            for (String offset : new String[] {"Z", "+08:00"}) {
                assertThat(convert(DataTypes.TIMESTAMP_LTZ(6), "\"" + local + offset + "\""))
                        .isEqualTo(
                                TimestampLtz.fromInstant(
                                        expected.toInstant(ZoneOffset.of(offset))));
            }
        }
    }

    @Test
    @ResourceLock("java.util.TimeZone.default")
    void testTimestampLtzFallsBackToLocalZoneAndPreservesExplicitOffsets() throws Exception {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            for (String separator : new String[] {"T", " "}) {
                for (String fraction : new String[] {"", ".123456"}) {
                    String local = "2026-10-25" + separator + "12:34:56" + fraction;
                    TimestampLtz expected =
                            TimestampLtz.fromInstant(
                                    Instant.parse("2026-10-25T04:34:56" + fraction + "Z"));
                    assertThat(convert(DataTypes.TIMESTAMP_LTZ(6), "\"" + local + "\""))
                            .isEqualTo(expected);
                    assertThat(convert(DataTypes.TIMESTAMP_LTZ(6), "\"" + local + "+08:00\""))
                            .isEqualTo(expected);
                    assertThat(convert(DataTypes.TIMESTAMP_LTZ(6), "\"" + local + "Z\""))
                            .isEqualTo(
                                    TimestampLtz.fromInstant(
                                            Instant.parse("2026-10-25T12:34:56" + fraction + "Z")));
                }
            }
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    @ParameterizedTest
    @MethodSource("temporalPrecisions")
    void testTruncatesTemporalFractions(int precision, int nanos, int timeMillis) throws Exception {
        assertThat(convert(DataTypes.TIME(precision), "\"12:34:56.123456789\""))
                .isEqualTo(45_296_000 + timeMillis);
        for (boolean withZone : new boolean[] {false, true}) {
            for (String date : new String[] {"1969-12-31", "2026-10-25"}) {
                Instant expected = Instant.parse(date + "T12:34:56Z").plusNanos(nanos);
                Object actual =
                        convert(
                                timestampType(precision, withZone),
                                "\"" + date + "T12:34:56.123456789" + (withZone ? "Z\"" : "\""));
                assertThat(actual)
                        .isEqualTo(
                                withZone
                                        ? TimestampLtz.fromInstant(expected)
                                        : TimestampNtz.fromLocalDateTime(
                                                LocalDateTime.ofInstant(expected, ZoneOffset.UTC)));
            }
        }
    }

    private static Stream<Arguments> temporalPrecisions() {
        return Stream.of(
                Arguments.of(0, 0, 0),
                Arguments.of(1, 100_000_000, 100),
                Arguments.of(2, 120_000_000, 120),
                Arguments.of(3, 123_000_000, 123),
                Arguments.of(4, 123_400_000, 123),
                Arguments.of(5, 123_450_000, 123),
                Arguments.of(6, 123_456_000, 123),
                Arguments.of(7, 123_456_700, 123),
                Arguments.of(8, 123_456_780, 123),
                Arguments.of(9, 123_456_789, 123));
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 6})
    void testTruncatesBeforeCheckingTimestampStorageRange(int precision) throws Exception {
        long unitsPerSecond = precision == 3 ? 1_000 : 1_000_000;
        long nanosPerUnit = 1_000_000_000 / unitsPerSecond;
        Instant maximum =
                Instant.ofEpochSecond(
                        Math.floorDiv(Long.MAX_VALUE, unitsPerSecond),
                        Math.floorMod(Long.MAX_VALUE, unitsPerSecond) * nanosPerUnit);
        for (boolean withZone : new boolean[] {false, true}) {
            DataType type = timestampType(precision, withZone);
            assertThat(convert(type, timestampJson(maximum.plusNanos(nanosPerUnit - 1), withZone)))
                    .isEqualTo(convert(type, timestampJson(maximum, withZone)));
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
    void testRejectsInvalidValuesAndOverflow(DataType type, String json) {
        assertThatThrownBy(() -> convert(type, json))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("$.field");
    }

    private static Stream<Arguments> invalidScalars() {
        return Stream.of(
                Arguments.of(DataTypes.INT(), "2147483648"),
                Arguments.of(DataTypes.TINYINT(), "128"),
                Arguments.of(DataTypes.SMALLINT(), "-32769"),
                Arguments.of(DataTypes.BIGINT(), "9223372036854775808"),
                Arguments.of(DataTypes.FLOAT(), "1e39"),
                Arguments.of(DataTypes.DOUBLE(), "1e309"),
                Arguments.of(DataTypes.DECIMAL(5, 2), "999.995"),
                Arguments.of(DataTypes.DECIMAL(5, 2), "-999.995"),
                Arguments.of(DataTypes.DECIMAL(5, 2), "1000"),
                Arguments.of(DataTypes.BYTES(), "\"!invalid!\""),
                Arguments.of(DataTypes.DATE(), "\"2026-02-30\""),
                Arguments.of(DataTypes.TIME(3), "\"24:00:00\""),
                Arguments.of(DataTypes.TIMESTAMP(3), "\"2026-02-30 12:34:56\""),
                Arguments.of(DataTypes.TIMESTAMP_LTZ(3), "\"2026-10-25 12:34:56+25:00\""));
    }

    @Test
    void testComplexTypesFollowTheSharedSchemaContract() {
        for (DataType type :
                new DataType[] {
                    DataTypes.ARRAY(DataTypes.INT()),
                    DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                    DataTypes.ROW(DataTypes.INT())
                }) {
            KafkaTableSchema.validateJsonType(type);
            assertThat(JsonToFlussConverters.create(type).convert(null, "$.field")).isNull();
        }
    }

    @ParameterizedTest
    @MethodSource("invalidMapKeyTypes")
    void testInvalidMapKeysUseThePublicSchemaError(DataType type) {
        assertThatThrownBy(() -> KafkaTableSchema.validateJsonType(type))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("only supports STRING map keys");
        assertThatThrownBy(() -> JsonToFlussConverters.create(type))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("only supports STRING map keys");
    }

    private static Stream<DataType> invalidMapKeyTypes() {
        return Stream.of(
                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                DataTypes.MAP(DataTypes.CHAR(8), DataTypes.STRING()),
                DataTypes.ROW(DataTypes.ARRAY(DataTypes.MAP(DataTypes.BYTES(), DataTypes.INT()))),
                DataTypes.MAP(
                        DataTypes.STRING(), DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.INT())));
    }

    @Test
    void testSchemaDepthUsesTheSharedBusinessFieldLimit() {
        DataType type = DataTypes.INT();
        for (int depth = 0; depth < KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH; depth++) {
            switch (depth % 3) {
                case 0:
                    type = DataTypes.ROW(type);
                    break;
                case 1:
                    type = DataTypes.ARRAY(type);
                    break;
                default:
                    type = DataTypes.MAP(DataTypes.STRING(), type);
            }
        }
        // The top-level business field starts at zero; each child consumes one level.
        DataType atLimit = type;
        KafkaTableSchema.validateJsonType(atLimit);
        assertThat(JsonToFlussConverters.create(atLimit)).isNotNull();

        DataType tooDeep = DataTypes.ROW(atLimit);
        String message =
                "Kafka JSON schema exceeds the maximum nesting depth of "
                        + KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH
                        + ".";
        assertThatThrownBy(() -> KafkaTableSchema.validateJsonType(tooDeep))
                .isInstanceOf(InvalidTableException.class)
                .hasMessage(message);
        assertThatThrownBy(() -> JsonToFlussConverters.create(tooDeep))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessage(message);
    }

    private static Object convert(DataType type, String json) throws Exception {
        return JsonToFlussConverters.create(type).convert(MAPPER.readTree(json), "$.field");
    }
}
