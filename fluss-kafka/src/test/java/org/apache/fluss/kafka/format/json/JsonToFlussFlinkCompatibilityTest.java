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

import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.BinaryNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.FloatNode;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Covers scalar coercion rules from Flink 1.20 JsonToRowDataConverters. */
class JsonToFlussFlinkCompatibilityTest {
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @ParameterizedTest
    @MethodSource("coercibleScalars")
    void testFlinkScalarCoercions(DataType type, String json, Object expected) throws Exception {
        assertThat(convert(type, json)).isEqualTo(expected);
    }

    private static Stream<Arguments> coercibleScalars() {
        return Stream.of(
                Arguments.of(DataTypes.BOOLEAN(), "\" TrUe \"", true),
                Arguments.of(DataTypes.BOOLEAN(), "\"false\"", false),
                // Boolean.parseBoolean returns false for every string except case-insensitive true.
                Arguments.of(DataTypes.BOOLEAN(), "\"not-a-boolean\"", false),
                Arguments.of(DataTypes.BOOLEAN(), "1", false),
                Arguments.of(DataTypes.BOOLEAN(), "{}", false),
                Arguments.of(DataTypes.TINYINT(), "\" -128 \"", Byte.MIN_VALUE),
                Arguments.of(DataTypes.TINYINT(), "\"+127\"", Byte.MAX_VALUE),
                Arguments.of(DataTypes.SMALLINT(), "\" -32768 \"", Short.MIN_VALUE),
                Arguments.of(DataTypes.SMALLINT(), "\"32767\"", Short.MAX_VALUE),
                Arguments.of(DataTypes.INT(), "\" -2147483648 \"", Integer.MIN_VALUE),
                Arguments.of(DataTypes.INT(), "\"+2147483647\"", Integer.MAX_VALUE),
                Arguments.of(DataTypes.INT(), "112.013", 112),
                Arguments.of(DataTypes.INT(), "-112.013", -112),
                Arguments.of(DataTypes.INT(), "1.0", 1),
                Arguments.of(DataTypes.BIGINT(), "\" -9223372036854775808 \"", Long.MIN_VALUE),
                Arguments.of(DataTypes.BIGINT(), "\"9223372036854775807\"", Long.MAX_VALUE),
                Arguments.of(DataTypes.BIGINT(), "112.013", 112L),
                Arguments.of(DataTypes.BIGINT(), "-112.013", -112L),
                Arguments.of(DataTypes.FLOAT(), "\" 1.25e2 \"", 125F),
                Arguments.of(DataTypes.FLOAT(), "\"-1.0000000596046448\"", -1.0000001F),
                Arguments.of(DataTypes.FLOAT(), "\"-0.0\"", -0.0F),
                Arguments.of(DataTypes.DOUBLE(), "\" -2.5e2 \"", -250D),
                Arguments.of(DataTypes.DOUBLE(), "\"-0.0\"", -0.0D));
    }

    @Test
    void testDecimalTextAndFloatingPointNodes() throws Exception {
        assertThat(((Decimal) convert(DataTypes.DECIMAL(5, 2), "\"12.345\"")).toBigDecimal())
                .isEqualByComparingTo("12.35");
        assertThat(((Decimal) convert(DataTypes.DECIMAL(5, 2), "\"1.2345e1\"")).toBigDecimal())
                .isEqualByComparingTo("12.35");
        assertThat(((Decimal) convert(DataTypes.DECIMAL(5, 2), "\"1e-2147483647\"")).toBigDecimal())
                .isEqualByComparingTo(BigDecimal.ZERO);
        JsonNode floatNode = FloatNode.valueOf(0.1F);
        Decimal decimal =
                (Decimal)
                        JsonToFlussConverters.create(DataTypes.DECIMAL(20, 10))
                                .convert(floatNode, "$.field");
        assertThat(decimal.toBigDecimal()).isEqualByComparingTo("0.1");
        assertThat(JsonToFlussConverters.create(DataTypes.DOUBLE()).convert(floatNode, "$.field"))
                .isEqualTo(0.1D);
    }

    @Test
    void testJacksonBinaryNodesAndBase64Whitespace() throws Exception {
        byte[] bytes = {1, 2, 3, 4};
        JsonNode binary = BinaryNode.valueOf(bytes);
        assertThat((byte[]) JsonToFlussConverters.create(DataTypes.BYTES()).convert(binary, "$.b"))
                .containsExactly(bytes);
        assertThat(
                        (byte[])
                                JsonToFlussConverters.create(DataTypes.BINARY(2))
                                        .convert(binary, "$.b"))
                .containsExactly((byte) 1, (byte) 2);
        assertThat((byte[]) convert(DataTypes.BYTES(), "\" AQID\\nBA== \"")).containsExactly(bytes);
        assertThat((byte[]) convert(DataTypes.BINARY(6), "\" AQID\\nBA== \""))
                .containsExactly((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 0, (byte) 0);
    }

    @Test
    void testCoercionsInsideRowsArraysAndMaps() throws Exception {
        RowType type =
                DataTypes.ROW(
                        DataTypes.FIELD("active", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("counts", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD(
                                "prices",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.DECIMAL(5, 2))));
        GenericRow row =
                (GenericRow)
                        convert(
                                type,
                                "{\"active\":\"TRUE\",\"counts\":[\"1\",-2.9,null],\"prices\":{\"a\":\"12.345\"}}");
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getArray(1).getInt(0)).isEqualTo(1);
        assertThat(row.getArray(1).getInt(1)).isEqualTo(-2);
        assertThat(row.getArray(1).isNullAt(2)).isTrue();
        assertThat(row.getMap(2).valueArray().getDecimal(0, 5, 2).toBigDecimal())
                .isEqualByComparingTo("12.35");
    }

    @ParameterizedTest
    @MethodSource("invalidScalarCoercions")
    void testRejectsInvalidTextAndOutOfRangeValues(DataType type, String json) {
        assertThatThrownBy(() -> convert(type, json))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("$.field");
    }

    private static Stream<Arguments> invalidScalarCoercions() {
        return Stream.of(
                Arguments.of(DataTypes.TINYINT(), "\"128\""),
                Arguments.of(DataTypes.TINYINT(), "1.5"),
                Arguments.of(DataTypes.SMALLINT(), "\"-32769\""),
                Arguments.of(DataTypes.SMALLINT(), "-1.5"),
                Arguments.of(DataTypes.INT(), "\"2147483648\""),
                Arguments.of(DataTypes.INT(), "\"1.0\""),
                Arguments.of(DataTypes.INT(), "2147483647.1"),
                Arguments.of(DataTypes.INT(), "true"),
                Arguments.of(DataTypes.BIGINT(), "\"9223372036854775808\""),
                Arguments.of(DataTypes.BIGINT(), "\"1.0\""),
                Arguments.of(DataTypes.BIGINT(), "9223372036854775807.1"),
                Arguments.of(DataTypes.FLOAT(), "\"not-a-number\""),
                Arguments.of(DataTypes.DOUBLE(), "[]"),
                // Retain Fluss overflow errors rather than storing non-finite values or null.
                Arguments.of(DataTypes.FLOAT(), "\"NaN\""),
                Arguments.of(DataTypes.FLOAT(), "\"1e39\""),
                Arguments.of(DataTypes.DOUBLE(), "\"Infinity\""),
                Arguments.of(DataTypes.DOUBLE(), "\"1e309\""),
                Arguments.of(DataTypes.DECIMAL(5, 2), "\"999.995\""),
                Arguments.of(DataTypes.DECIMAL(5, 2), "\"1e2147483647\""),
                Arguments.of(DataTypes.DECIMAL(5, 2), "\" 12.34 \""),
                Arguments.of(DataTypes.DECIMAL(5, 2), "true"),
                Arguments.of(DataTypes.BYTES(), "1"));
    }

    private static Object convert(DataType type, String json) throws Exception {
        return JsonToFlussConverters.create(type).convert(MAPPER.readTree(json), "$.field");
    }
}
