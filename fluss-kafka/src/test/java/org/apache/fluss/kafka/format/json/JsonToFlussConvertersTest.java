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
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
