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
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
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

/** Tests recursive containers, nullability, and bounded conversion errors. */
class JsonToFlussComplexConvertersTest {
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @Test
    void testNestedRowsArraysAndMaps() throws Exception {
        RowType type =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "customer",
                                DataTypes.ROW(
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "address",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "city", DataTypes.STRING()))))),
                        DataTypes.FIELD(
                                "items",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("sku", DataTypes.STRING()),
                                                DataTypes.FIELD("quantity", DataTypes.INT())))),
                        DataTypes.FIELD(
                                "attributes",
                                DataTypes.MAP(
                                        DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))));
        GenericRow row =
                (GenericRow)
                        convert(
                                type,
                                "{\"customer\":{\"name\":\"Alice\",\"address\":{\"city\":\"Hangzhou\"}},"
                                        + "\"items\":[{\"sku\":\"A-100\",\"quantity\":2},null],"
                                        + "\"attributes\":{\"scores\":[7,null,9],\"empty\":[],\"missing\":null}}");
        assertThat(row.getRow(0, 2).getString(0).toString()).isEqualTo("Alice");
        assertThat(row.getRow(0, 2).getRow(1, 1).getString(0).toString()).isEqualTo("Hangzhou");
        assertThat(row.getArray(1).getRow(0, 2).getString(0).toString()).isEqualTo("A-100");
        assertThat(row.getArray(1).getRow(0, 2).getInt(1)).isEqualTo(2);
        assertThat(row.getArray(1).isNullAt(1)).isTrue();
        GenericMap attributes = (GenericMap) row.getMap(2);
        assertThat(
                        ((GenericArray) attributes.get(BinaryString.fromString("scores")))
                                .toObjectArray())
                .containsExactly(7, null, 9);
        assertThat(((GenericArray) attributes.get(BinaryString.fromString("empty"))).size())
                .isZero();
        assertThat(attributes.get(BinaryString.fromString("missing"))).isNull();
    }

    @Test
    void testArrayOfMapsOfRowsUsesScalarNormalization() throws Exception {
        DataType type =
                DataTypes.ARRAY(
                        DataTypes.MAP(
                                DataTypes.STRING(),
                                DataTypes.ROW(
                                        DataTypes.FIELD("amount", DataTypes.DECIMAL(5, 2)),
                                        DataTypes.FIELD(
                                                "labels",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.STRING())))));
        GenericArray array =
                (GenericArray)
                        convert(
                                type,
                                "[{\"entry\":{\"amount\":12.345,\"labels\":{\"boolean\":true,\"array\":[1,2]}}}]");
        GenericRow row =
                (GenericRow) ((GenericMap) array.getMap(0)).get(BinaryString.fromString("entry"));
        assertThat(row.getDecimal(0, 5, 2).toBigDecimal()).isEqualTo(new BigDecimal("12.35"));
        GenericMap labels = (GenericMap) row.getMap(1);
        assertThat(labels.get(BinaryString.fromString("boolean")))
                .isEqualTo(BinaryString.fromString("true"));
        assertThat(labels.get(BinaryString.fromString("array")))
                .isEqualTo(BinaryString.fromString("[1,2]"));
    }

    @Test
    void testEmptyContainersNullsAndMissingFields() throws Exception {
        RowType type =
                DataTypes.ROW(
                        DataTypes.FIELD("array", DataTypes.ARRAY(DataTypes.INT())),
                        DataTypes.FIELD("map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.FIELD(
                                "row", DataTypes.ROW(DataTypes.FIELD("value", DataTypes.INT()))));
        GenericRow empty = (GenericRow) convert(type, "{\"array\":[],\"map\":{},\"row\":{}}");
        assertThat(empty.getArray(0).size()).isZero();
        assertThat(empty.getMap(1).size()).isZero();
        assertThat(empty.getRow(2, 1).isNullAt(0)).isTrue();
        for (String json : new String[] {"{}", "{\"array\":null,\"map\":null,\"row\":null}"}) {
            GenericRow missing = (GenericRow) convert(type, json);
            for (int i = 0; i < 3; i++) {
                assertThat(missing.isNullAt(i)).isTrue();
            }
        }
        GenericArray arrays =
                (GenericArray)
                        convert(
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                                "[[],null,[null]]");
        assertThat(arrays.getArray(0).size()).isZero();
        assertThat(arrays.isNullAt(1)).isTrue();
        assertThat(arrays.getArray(2).isNullAt(0)).isTrue();
        assertThat(convert(type, "null")).isNull();
    }

    @Test
    void testRowsIgnoreUnknownFieldsAndMapKeysRemainDynamic() throws Exception {
        RowType type =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "nested", DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()))));
        GenericRow row =
                (GenericRow)
                        convert(type, "{\"extra\":true,\"nested\":{\"extra\":[1,2],\"id\":3}}");
        assertThat(row.getRow(0, 1).getInt(0)).isEqualTo(3);
        GenericMap map =
                (GenericMap)
                        convert(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                "{\"\":1,\"😀\":2,\"dynamic-key\":null}");
        assertThat(map.size()).isEqualTo(3);
        assertThat(map.get(BinaryString.fromString(""))).isEqualTo(1);
        assertThat(map.get(BinaryString.fromString("😀"))).isEqualTo(2);
        assertThat(map.get(BinaryString.fromString("dynamic-key"))).isNull();
    }

    @ParameterizedTest
    @MethodSource("invalidContainers")
    void testRejectsInvalidShapesAndValuesAtNestedPaths(DataType type, String json, String path)
            throws Exception {
        assertThatThrownBy(() -> convert(type, json))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining(path);
    }

    private static Stream<Arguments> invalidContainers() {
        return Stream.of(
                Arguments.of(DataTypes.ARRAY(DataTypes.INT()), "{}", "$"),
                Arguments.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), "[]", "$"),
                Arguments.of(DataTypes.ROW(DataTypes.INT()), "[]", "$"),
                Arguments.of(DataTypes.ARRAY(DataTypes.INT()), "[1,\"bad\"]", "$[1]"),
                Arguments.of(DataTypes.ARRAY(DataTypes.INT().copy(false)), "[null]", "$[0]"),
                Arguments.of(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT().copy(false)),
                        "{\"key\":null}",
                        "$[\"key\"]"),
                Arguments.of(
                        DataTypes.ROW(DataTypes.FIELD("value", DataTypes.INT().copy(false))),
                        "{}",
                        "$[\"value\"]"),
                Arguments.of(
                        DataTypes.ARRAY(
                                DataTypes.MAP(
                                        DataTypes.STRING(),
                                        DataTypes.ROW(
                                                DataTypes.FIELD("quantity", DataTypes.INT())))),
                        "[{\"bad\\\"key\":{\"quantity\":\"bad\"}}]",
                        "$[0][\"bad\\\"key\"][\"quantity\"]"));
    }

    @Test
    void testConvertsAtSchemaNestingLimit() {
        DataType type = DataTypes.INT();
        JsonNode value = MAPPER.getNodeFactory().numberNode(42);
        for (int i = 0; i < KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH; i++) {
            type = DataTypes.ARRAY(type);
            value = MAPPER.createArrayNode().add(value);
        }
        Object result = JsonToFlussConverters.create(type).convert(value, "$");
        for (int i = 0; i < KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH; i++) {
            result = ((GenericArray) result).toObjectArray()[0];
        }
        assertThat(result).isEqualTo(42);
    }

    @Test
    void testErrorPathsEscapeNamesAndStayBounded() {
        StringBuilder name = new StringBuilder("quoted\"\\\n\t\r\b\f");
        for (int i = 0; i < 1000; i++) {
            name.append('x');
        }
        ObjectNode object = MAPPER.createObjectNode().put(name.toString(), "invalid");
        JsonToFlussConverter converter =
                JsonToFlussConverters.create(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        assertThatThrownBy(() -> converter.convert(object, "$"))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("$[\"quoted\\\"\\\\\\n\\t\\r\\b\\f")
                .hasMessageContaining("...")
                .satisfies(error -> assertThat(error.getMessage().length()).isLessThan(300));
        String path = "$";
        for (int i = 0; i < 64; i++) {
            path = JsonPath.field(path, name.toString());
        }
        assertThat(path).hasSize(512).startsWith("$").endsWith("...\"]");
    }

    private static Object convert(DataType type, String json) throws Exception {
        return JsonToFlussConverters.create(type).convert(MAPPER.readTree(json), "$");
    }
}
