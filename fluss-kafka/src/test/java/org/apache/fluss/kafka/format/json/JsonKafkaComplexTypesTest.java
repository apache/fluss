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

import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaFieldProjection;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests recursive JSON conversion, nested validation and resource bounds. */
class JsonKafkaComplexTypesTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDecodesRecursiveRowArrayAndMapTypes(boolean nested) {
        RowType rowType = complexRowType();

        Object[] values =
                decodeBusinessFields(
                        rowType,
                        "{"
                                + "\"customer\":{\"name\":\"Alice\","
                                + "\"address\":{\"city\":\"Hangzhou\"}},"
                                + "\"items\":[{\"sku\":\"A-100\","
                                + "\"quantity\":2}],"
                                + "\"attributes\":{\"scores\":[7,null,9]}"
                                + "}",
                        nested);

        GenericRow customer = (GenericRow) values[0];
        assertThat(customer.getString(0).toString()).isEqualTo("Alice");
        assertThat(customer.getRow(1, 1).getString(0).toString()).isEqualTo("Hangzhou");

        GenericArray items = (GenericArray) values[1];
        assertThat(items.size()).isEqualTo(1);
        assertThat(items.getRow(0, 2).getString(0).toString()).isEqualTo("A-100");
        assertThat(items.getRow(0, 2).getInt(1)).isEqualTo(2);

        GenericMap attributes = (GenericMap) values[2];
        GenericArray scores = (GenericArray) attributes.get(BinaryString.fromString("scores"));
        assertThat(scores.toObjectArray()).containsExactly(7, null, 9);
    }

    @Test
    void testRejectsInvalidNestedValuesWithPrecisePaths() {
        assertFailure(
                complexRowType(),
                "{\"customer\":{\"address\":{\"city\":\"Hangzhou\"}},"
                        + "\"items\":[],\"attributes\":{}}",
                "$[\"customer\"][\"name\"]",
                "missing or null");
        assertFailure(
                complexRowType(),
                "{\"customer\":{\"name\":\"Alice\","
                        + "\"address\":{\"city\":\"Hangzhou\"}},"
                        + "\"items\":[{\"sku\":\"A-100\",\"quantity\":2},"
                        + "{\"sku\":\"A-200\",\"quantity\":\"bad\"}],"
                        + "\"attributes\":{}}",
                "$[\"items\"][1][\"quantity\"]",
                "expected a JSON integer");
        assertFailure(
                complexRowType(),
                "{\"customer\":{\"name\":\"Alice\","
                        + "\"address\":{\"city\":\"Hangzhou\"}},"
                        + "\"items\":[],\"attributes\":{\"bad\\\"key\":[\"bad\"]}}",
                "$[\"attributes\"][\"bad\\\"key\"][0]",
                "expected a JSON integer");
    }

    @Test
    void testRejectsUnknownRootAndNestedRowFieldsButAllowsMapKeys() {
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())),
                "{\"id\":1,\"extra\":\"unexpected\"}",
                "$[\"extra\"]",
                "unknown field");
        assertFailure(
                complexRowType(),
                "{\"customer\":{\"name\":\"Alice\","
                        + "\"address\":{\"city\":\"Hangzhou\",\"extra\":1}},"
                        + "\"items\":[],\"attributes\":{}}",
                "$[\"customer\"][\"address\"][\"extra\"]",
                "unknown field");

        Object[] values =
                decoder(
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "attributes",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.INT()))))
                        .decode(bytes("{\"attributes\":{\"dynamic-key\":1}}"));
        GenericMap attributes = (GenericMap) values[0];
        assertThat(attributes.size()).isEqualTo(1);
        assertThat(attributes.get(BinaryString.fromString("dynamic-key"))).isEqualTo(1);
    }

    @Test
    void testRejectsNonStringMapKeyBeforeDecodingRecords() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "attributes", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())));

        assertThatThrownBy(() -> decoder(rowType))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("only supports STRING map keys")
                .hasMessageContaining("MAP<INT");
    }

    @Test
    void testEnforcesSchemaJsonAndContainerResourceBounds() {
        DataType nestedType = DataTypes.STRING();
        for (int i = 0; i <= KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH; i++) {
            nestedType = DataTypes.ARRAY(nestedType);
        }
        final DataType tooDeepType = nestedType;
        assertThatThrownBy(() -> decoder(DataTypes.ROW(DataTypes.FIELD("nested", tooDeepType))))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("maximum nesting depth");

        StringBuilder nestedJson = new StringBuilder("{\"value\":");
        for (int i = 0; i < JsonKafkaFieldDecoder.MAX_JSON_NESTING_DEPTH; i++) {
            nestedJson.append("{\"nested\":");
        }
        nestedJson.append('0');
        for (int i = 0; i < JsonKafkaFieldDecoder.MAX_JSON_NESTING_DEPTH; i++) {
            nestedJson.append('}');
        }
        nestedJson.append('}');
        JsonKafkaFieldDecoder complexDecoder =
                decoder(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "value",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))));
        assertThatThrownBy(() -> complexDecoder.decode(bytes(nestedJson.toString())))
                .isInstanceOf(KafkaRecordEncodingException.class);

        StringBuilder oversizedArray = new StringBuilder("{\"values\":[");
        for (int i = 0; i <= JsonToFlussConverters.MAX_CONTAINER_ELEMENTS; i++) {
            if (i > 0) {
                oversizedArray.append(',');
            }
            oversizedArray.append('0');
        }
        oversizedArray.append("]}");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT()))),
                oversizedArray.toString(),
                "$[\"values\"]",
                "container size exceeds");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("oversizedContainers")
    void testRejectsOversizedContainersBeforeReadingExcessChild(
            String description, DataType dataType, String json, String path) {
        // The excess child is deliberately unfinished. Reading it before checking the
        // containing ARRAY/MAP/ROW limit would fail with a syntax error instead.
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("value", dataType)),
                "{\"value\":" + json,
                path,
                "container size exceeds " + JsonToFlussConverters.MAX_CONTAINER_ELEMENTS);
    }

    private static Stream<Arguments> oversizedContainers() {
        String array = containerAtLimit(false);
        String object = containerAtLimit(true);
        String oversizedArray = array.substring(0, array.length() - 1) + ",[";
        String oversizedObject = object.substring(0, object.length() - 1) + ",\"excess\":{";
        DataType arrayType = DataTypes.ARRAY(DataTypes.INT());
        DataType mapType = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
        RowType rowType = rowTypeAtLimit();
        return Stream.of(
                Arguments.of("ARRAY", arrayType, oversizedArray, "$[\"value\"]"),
                Arguments.of("MAP", mapType, oversizedObject, "$[\"value\"]"),
                Arguments.of("ROW", rowType, oversizedObject, "$[\"value\"]"),
                Arguments.of(
                        "ARRAY inside ROW",
                        DataTypes.ROW(DataTypes.FIELD("nested", arrayType)),
                        "{\"nested\":" + oversizedArray,
                        "$[\"value\"][\"nested\"]"),
                Arguments.of(
                        "MAP inside ARRAY",
                        DataTypes.ARRAY(mapType),
                        "[" + oversizedObject,
                        "$[\"value\"][0]"),
                Arguments.of(
                        "ROW inside MAP",
                        DataTypes.MAP(DataTypes.STRING(), rowType),
                        "{\"nested\":" + oversizedObject,
                        "$[\"value\"][\"nested\"]"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testAcceptsLimitForEachSiblingContainer(boolean nested) {
        String array = containerAtLimit(false);
        String object = containerAtLimit(true);
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "arrays", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))),
                        DataTypes.FIELD(
                                "maps",
                                DataTypes.ARRAY(
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))),
                        DataTypes.FIELD("rows", DataTypes.ARRAY(rowTypeAtLimit())));
        Object[] values =
                decodeBusinessFields(
                        rowType,
                        "{\"arrays\":["
                                + array
                                + ","
                                + array
                                + "],\"maps\":["
                                + object
                                + ","
                                + object
                                + "],\"rows\":["
                                + object
                                + ","
                                + object
                                + "]}",
                        nested);

        int limit = JsonToFlussConverters.MAX_CONTAINER_ELEMENTS;
        for (int i = 0; i < 2; i++) {
            GenericArray arrayValue = (GenericArray) ((GenericArray) values[0]).getArray(i);
            assertThat(arrayValue.size()).isEqualTo(limit);
            assertThat(arrayValue.getInt(limit - 1)).isZero();
            GenericMap mapValue = (GenericMap) ((GenericArray) values[1]).getMap(i);
            assertThat(mapValue.size()).isEqualTo(limit);
            assertThat(mapValue.get(BinaryString.fromString("k" + (limit - 1)))).isEqualTo(0);
            GenericRow rowValue = (GenericRow) ((GenericArray) values[2]).getRow(i, limit);
            assertThat(rowValue.getFieldCount()).isEqualTo(limit);
            assertThat(rowValue.getInt(limit - 1)).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{\"value\":[{\"a\":[],\"a\":[]}]}",
                "{\"value\":[{\"a\":[1,]}]}",
                "{\"value\":[{\"a\":[1]",
                "{\"value\":[{\"a\":[1]}]} {}",
            })
    void testRetainsStrictParsingForNestedContainers(String json) {
        JsonKafkaFieldDecoder nestedDecoder =
                decoder(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "value",
                                        DataTypes.ARRAY(
                                                DataTypes.MAP(
                                                        DataTypes.STRING(),
                                                        DataTypes.ARRAY(DataTypes.INT()))))));
        assertThatThrownBy(() -> nestedDecoder.decode(bytes(json)))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("not valid strict UTF-8 JSON");
    }

    private static RowType rowTypeAtLimit() {
        DataField[] fields = new DataField[JsonToFlussConverters.MAX_CONTAINER_ELEMENTS];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = DataTypes.FIELD("k" + i, DataTypes.INT());
        }
        return DataTypes.ROW(fields);
    }

    private static String containerAtLimit(boolean object) {
        StringBuilder json = new StringBuilder(object ? "{" : "[");
        for (int i = 0; i < JsonToFlussConverters.MAX_CONTAINER_ELEMENTS; i++) {
            if (i > 0) {
                json.append(',');
            }
            if (object) {
                json.append('"').append('k').append(i).append("\":");
            }
            json.append('0');
        }
        return json.append(object ? '}' : ']').toString();
    }

    @Test
    void testBoundsAndEscapesMapKeysInErrorPaths() {
        StringBuilder mapKey = new StringBuilder("quote\"");
        for (int i = 0; i < 300; i++) {
            mapKey.append('x');
        }
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));

        assertThatThrownBy(
                        () ->
                                decoder(rowType)
                                        .decode(
                                                bytes(
                                                        "{\"attributes\":{"
                                                                + quoteJson(mapKey.toString())
                                                                + ":\"bad\"}}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("$[\"attributes\"][\"quote\\\"")
                .hasMessageContaining("...")
                .satisfies(
                        error -> assertThat(error.getMessage()).doesNotContain(mapKey.toString()));

        StringBuilder controlCharacters = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            controlCharacters.append('\u0001');
        }
        String boundedPath = JsonPath.field(JsonPath.ROOT, controlCharacters.toString());
        assertThat(boundedPath)
                .hasSizeLessThanOrEqualTo(512)
                .endsWith("\"]")
                .doesNotContain("\\u0...");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDecodesTheSharedDepthBoundaryIncludingEmptyRows(boolean nested) {
        for (boolean emptyRow : new boolean[] {false, true}) {
            DataType type = emptyRow ? new RowType(Collections.emptyList()) : DataTypes.INT();
            String json = emptyRow ? "{}" : "7";
            Object expected = emptyRow ? new GenericRow(0) : 7;
            for (int depth = 0; depth < KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH; depth++) {
                switch (depth % 3) {
                    case 0:
                        type = DataTypes.ROW(DataTypes.FIELD("child", type));
                        json = "{\"child\":" + json + "}";
                        expected = GenericRow.of(expected);
                        break;
                    case 1:
                        type = DataTypes.ARRAY(type);
                        json = "[" + json + "]";
                        expected = new GenericArray(new Object[] {expected});
                        break;
                    default:
                        type = DataTypes.MAP(DataTypes.STRING(), type);
                        json = "{\"entry\":" + json + "}";
                        expected =
                                new GenericMap(
                                        Collections.singletonMap(
                                                BinaryString.fromString("entry"), expected));
                }
            }
            KafkaTableSchema.validateJsonType(type);
            RowType business = DataTypes.ROW(DataTypes.FIELD("value", type));
            // An empty ROW at business depth 64 is JSON container depth 66, including the root.
            assertThat(decodeBusinessFields(business, "{\"value\":" + json + "}", nested))
                    .containsExactly(expected);

            DataType tooDeep = DataTypes.ARRAY(type);
            assertThatThrownBy(() -> KafkaTableSchema.validateJsonType(tooDeep))
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessageContaining("maximum nesting depth of 64");
            assertThatThrownBy(
                            () -> decoder(DataTypes.ROW(DataTypes.FIELD("value", tooDeep)), nested))
                    .isInstanceOf(KafkaTopicSchemaException.class)
                    .hasMessageContaining("maximum nesting depth of 64");
            String invalidJson =
                    "{\"value\":"
                            + (emptyRow
                                    ? json.replace("{}", "{\"extra\":{}}")
                                    : json.replace("7", "[7]"))
                            + "}";
            assertThatThrownBy(() -> decoder(business, nested).decode(bytes(invalidJson)))
                    .isInstanceOf(KafkaRecordEncodingException.class);
        }
    }

    @ParameterizedTest
    @MethodSource("floatingPointZeroTokens")
    void testPreservesFloatingPointZeroInEveryContainer(boolean nested, String token) {
        for (Object value : numericContainerValues(DataTypes.FLOAT(), token, nested)) {
            assertThat(Float.floatToRawIntBits((Float) value))
                    .isEqualTo(Float.floatToRawIntBits(-0.0F));
        }
        for (Object value : numericContainerValues(DataTypes.DOUBLE(), token, nested)) {
            assertThat(Double.doubleToRawLongBits((Double) value))
                    .isEqualTo(Double.doubleToRawLongBits(-0.0D));
        }
    }

    private static Stream<Arguments> floatingPointZeroTokens() {
        return Stream.of(false, true)
                .flatMap(
                        nested ->
                                Stream.of("-0", "-0.0", "-0e0")
                                        .map(token -> Arguments.of(nested, token)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFloatingPointUnderflowPreservesSignAndRoundsOnceInEveryContainer(boolean nested) {
        // The last two pairs straddle half of the smallest subnormal for each target.
        // Parsing FLOAT through DOUBLE first would round both tokens to the same midpoint.
        String[][] tokens = {
            {"1e-1000", "1e-1000"},
            {"7.006492321624085e-46", "2.4703282292062327e-324"},
            {"7.006492321624086e-46", "2.4703282292062328e-324"}
        };
        for (boolean negative : new boolean[] {false, true}) {
            String sign = negative ? "-" : "";
            for (int i = 0; i < tokens.length; i++) {
                int magnitude = i == tokens.length - 1 ? 1 : 0;
                String floatToken = sign + tokens[i][0];
                for (Object value : numericContainerValues(DataTypes.FLOAT(), floatToken, nested)) {
                    assertThat(Float.floatToRawIntBits((Float) value))
                            .as("FLOAT %s, nested=%s", floatToken, nested)
                            .isEqualTo((negative ? Integer.MIN_VALUE : 0) | magnitude);
                }
                String doubleToken = sign + tokens[i][1];
                for (Object value :
                        numericContainerValues(DataTypes.DOUBLE(), doubleToken, nested)) {
                    assertThat(Double.doubleToRawLongBits((Double) value))
                            .as("DOUBLE %s, nested=%s", doubleToken, nested)
                            .isEqualTo((negative ? Long.MIN_VALUE : 0L) | magnitude);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPreservesOriginalNumericRoundingAndExactDecimalsInEveryContainer(boolean nested) {
        for (String sign : new String[] {"", "-"}) {
            float direction = sign.isEmpty() ? 1.0F : -1.0F;
            assertThat(
                            numericContainerValues(
                                    DataTypes.FLOAT(), sign + "1.0000000596046448", nested))
                    .containsOnly(direction * 1.0000001F);
            assertThat(
                            numericContainerValues(
                                    DataTypes.FLOAT(), sign + "1.0000000596046447", nested))
                    .containsOnly(direction);
            assertThat(
                            numericContainerValues(
                                    DataTypes.DOUBLE(),
                                    sign
                                            + "1.00000000000000011102230246251565404236316680908203126",
                                    nested))
                    .containsOnly(direction * 1.0000000000000002D);
            assertThat(
                            numericContainerValues(
                                    DataTypes.DOUBLE(),
                                    sign
                                            + "1.00000000000000011102230246251565404236316680908203124",
                                    nested))
                    .containsOnly((double) direction);
            String amount = sign + "12345678901234567890.123456789012345678";
            for (Object value : numericContainerValues(DataTypes.DECIMAL(38, 18), amount, nested)) {
                assertThat(((Decimal) value).toBigDecimal()).isEqualTo(new BigDecimal(amount));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMapKeysRejectUnpairedSurrogatesBeforeEncodingOrColliding(boolean nested) {
        RowType business =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "value", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        JsonKafkaFieldDecoder decoder = decoder(business, nested);
        for (String key :
                new String[] {
                    "\\ud800", "\\udc00", "a\\ud800b", "\\ud800\\ud800", "\\udc00\\ud800"
                }) {
            for (String object :
                    new String[] {"{\"?\":1,\"" + key + "\":2}", "{\"" + key + "\":2,\"?\":1}"}) {
                assertThatThrownBy(() -> decoder.decode(bytes("{\"value\":" + object + "}")))
                        .isInstanceOf(KafkaRecordEncodingException.class)
                        .hasMessageContaining("$[\"value\"]")
                        .hasMessageContaining("surrogate");
            }
        }
        GenericMap values =
                (GenericMap)
                        decodeBusinessFields(
                                business,
                                "{\"value\":{\"?\":1,\"\\ud83d\\ude00\":2,\"\\\\ud800\":3}}",
                                nested)[0];
        assertThat(values.size()).isEqualTo(3);
        assertThat(values.get(BinaryString.fromString("?"))).isEqualTo(1);
        assertThat(values.get(BinaryString.fromString("😀"))).isEqualTo(2);
        assertThat(values.get(BinaryString.fromString("\\ud800"))).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDeclaredReservedNamesAreOrdinaryFieldsBelowTheJsonRoot(boolean nested) {
        String[] names = {
            "_kafka_value", "_kafka_key", "_kafka_headers", "_kafka_metadata", "_kafka_rescue"
        };
        DataField[] fields = new DataField[names.length];
        StringBuilder object = new StringBuilder("{");
        for (int i = 0; i < names.length; i++) {
            fields[i] = DataTypes.FIELD(names[i], DataTypes.INT().copy(false));
            if (i > 0) {
                object.append(',');
            }
            object.append('"').append(names[i]).append("\":").append(i);
        }
        object.append('}');
        RowType inner = DataTypes.ROW(fields);
        RowType business =
                DataTypes.ROW(
                        DataTypes.FIELD("row", inner),
                        DataTypes.FIELD("array", DataTypes.ARRAY(inner)),
                        DataTypes.FIELD("map", DataTypes.MAP(DataTypes.STRING(), inner)));
        Object[] values =
                decodeBusinessFields(
                        business,
                        "{\"row\":"
                                + object
                                + ",\"array\":["
                                + object
                                + "],\"map\":{\"_kafka_value\":"
                                + object
                                + "}}",
                        nested);
        GenericRow expected = GenericRow.of(0, 1, 2, 3, 4);
        assertThat(values[0]).isEqualTo(expected);
        assertThat(((GenericArray) values[1]).getRow(0, names.length)).isEqualTo(expected);
        assertThat(((GenericMap) values[2]).get(BinaryString.fromString("_kafka_value")))
                .isEqualTo(expected);
    }

    @Test
    void testNestedNullabilityAppliesToRowsArrayElementsAndMapValues() {
        RowType business =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "row",
                                DataTypes.ROW(
                                        DataTypes.FIELD("required", DataTypes.INT().copy(false)))),
                        DataTypes.FIELD("array", DataTypes.ARRAY(DataTypes.INT().copy(false))),
                        DataTypes.FIELD(
                                "map",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT().copy(false))));
        assertFailure(business, "{\"row\":{}}", "$[\"row\"][\"required\"]", "missing or null");
        assertFailure(
                business,
                "{\"row\":{\"required\":null}}",
                "$[\"row\"][\"required\"]",
                "missing or null");
        assertFailure(business, "{\"array\":[1,null]}", "$[\"array\"][1]", "missing or null");
        assertFailure(
                business, "{\"map\":{\"entry\":null}}", "$[\"map\"][\"entry\"]", "missing or null");
        for (boolean nested : new boolean[] {false, true}) {
            assertThat(
                            decodeBusinessFields(
                                    business, "{\"row\":null,\"array\":null,\"map\":null}", nested))
                    .containsExactly(null, null, null);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNullValueRespectsComplexContainerAndParentNullability(boolean nested) {
        DataType[] containers = {
            DataTypes.ROW(DataTypes.FIELD("required", DataTypes.INT().copy(false))),
            DataTypes.ARRAY(DataTypes.INT().copy(false)),
            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT().copy(false))
        };
        for (DataType container : containers) {
            RowType business = DataTypes.ROW(DataTypes.FIELD("value", container));
            assertThat(decoder(business, nested).decode(null)).containsExactly((Object) null);
            assertThat(decodeBusinessFields(business, "{}", nested)).containsExactly((Object) null);
            assertThat(decodeBusinessFields(business, "{\"value\":null}", nested))
                    .containsExactly((Object) null);
            assertThatThrownBy(() -> decoder(business, nested).decode(bytes("null")))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("object root");

            RowType requiredContainer =
                    DataTypes.ROW(DataTypes.FIELD("value", container.copy(false)));
            for (String json : new String[] {"{}", "{\"value\":null}"}) {
                assertThatThrownBy(() -> decoder(requiredContainer, nested).decode(bytes(json)))
                        .isInstanceOf(KafkaRecordEncodingException.class)
                        .hasMessageContaining("$[\"value\"]")
                        .hasMessageContaining("missing or null");
            }
            if (nested) {
                // An absent parent does not require values for its non-nullable children.
                assertThat(decoder(requiredContainer, true).decode(null))
                        .containsExactly((Object) null);
                RowType requiredParent = (RowType) business.copy(false);
                assertThatThrownBy(() -> decoder(requiredParent, true).decode(null))
                        .isInstanceOf(KafkaRecordEncodingException.class)
                        .hasMessageContaining("NOT NULL field '_kafka_value'");
                assertThat(decodeBusinessFields(requiredParent, "{}", true))
                        .containsExactly((Object) null);
            } else {
                assertThatThrownBy(() -> decoder(requiredContainer, false).decode(null))
                        .isInstanceOf(KafkaRecordEncodingException.class)
                        .hasMessageContaining("NOT NULL field 'value'");
            }
        }
    }

    private static Object[] numericContainerValues(DataType type, String token, boolean nested) {
        RowType business =
                DataTypes.ROW(
                        DataTypes.FIELD("row", DataTypes.ROW(DataTypes.FIELD("value", type))),
                        DataTypes.FIELD("array", DataTypes.ARRAY(type)),
                        DataTypes.FIELD("map", DataTypes.MAP(DataTypes.STRING(), type)));
        Object[] values =
                decodeBusinessFields(
                        business,
                        "{\"row\":{\"value\":"
                                + token
                                + "},\"array\":["
                                + token
                                + "],\"map\":{\"entry\":"
                                + token
                                + "}}",
                        nested);
        return new Object[] {
            ((GenericRow) values[0]).getField(0),
            ((GenericArray) values[1]).toObjectArray()[0],
            ((GenericMap) values[2]).get(BinaryString.fromString("entry"))
        };
    }

    private static Object[] decodeBusinessFields(RowType rowType, String json, boolean nested) {
        Object[] values = decoder(rowType, nested).decode(bytes(json));
        if (!nested) {
            return values;
        }
        GenericRow row = (GenericRow) values[0];
        Object[] fields = new Object[row.getFieldCount()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = row.getField(i);
        }
        return fields;
    }

    private static void assertFailure(RowType rowType, String json, String path, String reason) {
        for (boolean nested : new boolean[] {false, true}) {
            assertThatThrownBy(() -> decoder(rowType, nested).decode(bytes(json)))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining(path)
                    .hasMessageContaining(reason);
        }
    }

    private static JsonKafkaFieldDecoder decoder(RowType rowType) {
        return decoder(rowType, false);
    }

    private static JsonKafkaFieldDecoder decoder(RowType rowType, boolean nested) {
        if (nested) {
            return new JsonKafkaFieldDecoder(
                    new KafkaFieldProjection(
                            DataTypes.ROW(DataTypes.FIELD(KafkaTableSchema.VALUE_COLUMN, rowType)),
                            Collections.singletonList(0)),
                    KafkaValueConversionLayout.NESTED);
        }
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            positions.add(i);
        }
        return new JsonKafkaFieldDecoder(new KafkaFieldProjection(rowType, positions));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static RowType complexRowType() {
        return DataTypes.ROW(
                DataTypes.FIELD(
                        "customer",
                        DataTypes.ROW(
                                DataTypes.FIELD("name", DataTypes.STRING().copy(false)),
                                DataTypes.FIELD(
                                        "address",
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "city", DataTypes.STRING().copy(false)))))),
                DataTypes.FIELD(
                        "items",
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD("sku", DataTypes.STRING().copy(false)),
                                        DataTypes.FIELD("quantity", DataTypes.INT().copy(false))))),
                DataTypes.FIELD(
                        "attributes",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))));
    }

    private static String quoteJson(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
