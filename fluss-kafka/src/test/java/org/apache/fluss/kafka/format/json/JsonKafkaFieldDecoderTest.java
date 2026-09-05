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

import org.apache.fluss.kafka.schema.KafkaFieldProjection;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests strict schema-aware JSON scalar conversion. */
public class JsonKafkaFieldDecoderTest {

    @Test
    public void testDecodesAllSupportedScalarTypes() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("flag", DataTypes.BOOLEAN().copy(false)),
                        DataTypes.FIELD("tiny", DataTypes.TINYINT()),
                        DataTypes.FIELD("small", DataTypes.SMALLINT()),
                        DataTypes.FIELD("count", DataTypes.INT()),
                        DataTypes.FIELD("total", DataTypes.BIGINT()),
                        DataTypes.FIELD("ratio", DataTypes.FLOAT()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE()),
                        DataTypes.FIELD("amount", DataTypes.DECIMAL(8, 2)),
                        DataTypes.FIELD("code", DataTypes.CHAR(4)),
                        DataTypes.FIELD("message", DataTypes.STRING()),
                        DataTypes.FIELD("fixed", DataTypes.BINARY(2)),
                        DataTypes.FIELD("payload", DataTypes.BYTES()),
                        DataTypes.FIELD("day", DataTypes.DATE()),
                        DataTypes.FIELD("clock", DataTypes.TIME(3)),
                        DataTypes.FIELD("created", DataTypes.TIMESTAMP(3)),
                        DataTypes.FIELD("observed", DataTypes.TIMESTAMP_LTZ(3)),
                        DataTypes.FIELD("optional", DataTypes.STRING()));
        JsonKafkaFieldDecoder decoder = decoder(rowType);

        Object[] values =
                decoder.decode(
                        bytes(
                                "{"
                                        + "\"flag\":true,"
                                        + "\"tiny\":12,"
                                        + "\"small\":1234,"
                                        + "\"count\":123456,"
                                        + "\"total\":1234567890123,"
                                        + "\"ratio\":1.25,"
                                        + "\"score\":2.5,"
                                        + "\"amount\":12.50,"
                                        + "\"code\":\"CNY\","
                                        + "\"message\":\"hello\","
                                        + "\"fixed\":\"AQI=\","
                                        + "\"payload\":\"a2Fma2E=\","
                                        + "\"day\":\"2026-07-27\","
                                        + "\"clock\":\"19:30:12.123\","
                                        + "\"created\":\"2026-07-27T19:30:12.123\","
                                        + "\"observed\":\"2026-07-27T19:30:12.123+08:00\","
                                        + "\"optional\":null"
                                        + "}"));

        assertThat(values[0]).isEqualTo(true);
        assertThat(values[1]).isEqualTo((byte) 12);
        assertThat(values[2]).isEqualTo((short) 1234);
        assertThat(values[3]).isEqualTo(123456);
        assertThat(values[4]).isEqualTo(1234567890123L);
        assertThat(values[5]).isEqualTo(1.25F);
        assertThat(values[6]).isEqualTo(2.5D);
        assertThat(((Decimal) values[7]).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12.50"));
        assertThat(values[8]).isEqualTo(BinaryString.fromString("CNY"));
        assertThat(values[9]).isEqualTo(BinaryString.fromString("hello"));
        assertThat((byte[]) values[10]).containsExactly(1, 2);
        assertThat((byte[]) values[11]).isEqualTo(bytes("kafka"));
        assertThat(values[12]).isEqualTo((int) LocalDate.of(2026, 7, 27).toEpochDay());
        assertThat(values[13])
                .isEqualTo((int) (LocalTime.of(19, 30, 12, 123_000_000).toNanoOfDay() / 1_000_000));
        assertThat(values[14])
                .isEqualTo(
                        TimestampNtz.fromLocalDateTime(
                                LocalDateTime.of(2026, 7, 27, 19, 30, 12, 123_000_000)));
        assertThat(values[15])
                .isEqualTo(TimestampLtz.fromInstant(Instant.parse("2026-07-27T11:30:12.123Z")));
        assertThat(values[16]).isNull();
    }

    @Test
    public void testRejectsMalformedAndAmbiguousJson() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT().copy(false)),
                        DataTypes.FIELD("optional", DataTypes.STRING()));
        JsonKafkaFieldDecoder decoder = decoder(rowType);

        assertThatThrownBy(() -> decoder.decode(bytes("[]")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("object root");
        assertThatThrownBy(() -> decoder.decode(bytes("{\"id\":1,\"id\":2}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("strict UTF-8 JSON");
        assertThatThrownBy(() -> decoder.decode(bytes("{\"id\":1} {}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("strict UTF-8 JSON");
        assertThatThrownBy(() -> decoder.decode(bytes("{")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("strict UTF-8 JSON");
    }

    @Test
    public void testRejectsMissingTypeOverflowAndPrecisionLoss() {
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT().copy(false))),
                "{}",
                "$[\"id\"]",
                "missing or null");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())),
                "{\"id\":2147483648}",
                "$[\"id\"]",
                "integer overflow");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("amount", DataTypes.DECIMAL(5, 2))),
                "{\"amount\":1.234}",
                "$[\"amount\"]",
                "scale exceeds");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3))),
                "{\"timestamp\":\"2026-07-27T19:30:12.123\"}",
                "$[\"timestamp\"]",
                "offset or Z is required");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("fixed", DataTypes.BINARY(2))),
                "{\"fixed\":\"AQ==\"}",
                "$[\"fixed\"]",
                "length must equal 2");
    }

    @Test
    public void testDecodesRecursiveRowArrayAndMapTypes() {
        RowType rowType = complexRowType();

        Object[] values =
                decoder(rowType)
                        .decode(
                                bytes(
                                        "{"
                                                + "\"customer\":{\"name\":\"Alice\","
                                                + "\"address\":{\"city\":\"Hangzhou\"}},"
                                                + "\"items\":[{\"sku\":\"A-100\","
                                                + "\"quantity\":2}],"
                                                + "\"attributes\":{\"scores\":[7,null,9]}"
                                                + "}"));

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
    public void testRejectsInvalidNestedValuesWithPrecisePaths() {
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
    public void testRejectsUnknownRootAndNestedRowFieldsButAllowsMapKeys() {
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
    public void testRescuesUnknownRootAndNestedFieldsAsJsonString() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT().copy(false)),
                        DataTypes.FIELD(
                                "customer",
                                DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING()))),
                        DataTypes.FIELD(
                                "items",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.FIELD("sku", DataTypes.STRING())))),
                        DataTypes.FIELD(
                                "attributes",
                                DataTypes.MAP(
                                        DataTypes.STRING(),
                                        DataTypes.ROW(DataTypes.FIELD("known", DataTypes.INT())))),
                        DataTypes.FIELD("kafka_rescue", DataTypes.STRING()));
        JsonKafkaFieldDecoder decoder = rescueDecoder(rowType, "kafka_rescue");

        Object[] values =
                decoder.decode(
                        bytes(
                                "{"
                                        + "\"id\":1,"
                                        + "\"customer\":{\"name\":\"Alice\",\"extra_nested\":2},"
                                        + "\"items\":[{\"sku\":\"A-1\",\"extra_item\":3}],"
                                        + "\"attributes\":{\"dynamic\":{\"known\":1,"
                                        + "\"extra_map_value\":4}},"
                                        + "\"extra\":\"unexpected\","
                                        + "\"kafka_rescue\":null"
                                        + "}"));

        assertThat(values[0]).isEqualTo(1L);
        assertThat(((GenericRow) values[1]).getString(0).toString()).isEqualTo("Alice");
        assertThat(((GenericArray) values[2]).getRow(0, 1).getString(0).toString())
                .isEqualTo("A-1");
        GenericMap attributes = (GenericMap) values[3];
        assertThat(((GenericRow) attributes.get(BinaryString.fromString("dynamic"))).getInt(0))
                .isEqualTo(1);
        assertThat(((BinaryString) values[4]).toString())
                .isEqualTo(
                        "{\"extra\":\"unexpected\","
                                + "\"customer\":{\"extra_nested\":2},"
                                + "\"items\":[{\"extra_item\":3}],"
                                + "\"attributes\":{\"dynamic\":{\"extra_map_value\":4}}}");

        assertThat(
                        decoder.decode(
                                        bytes(
                                                "{\"id\":1,\"customer\":null,\"items\":null,"
                                                        + "\"attributes\":null}"))[4])
                .isNull();
    }

    @Test
    public void testRejectsExplicitNonNullRescueColumnAndKeepsTypeChecksStrict() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("kafka_rescue", DataTypes.STRING()));
        JsonKafkaFieldDecoder decoder = rescueDecoder(rowType, "kafka_rescue");

        assertThatThrownBy(() -> decoder.decode(bytes("{\"id\":1,\"kafka_rescue\":\"supplied\"}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("configured rescue column is reserved");
        assertThatThrownBy(() -> decoder.decode(bytes("{\"id\":\"not-a-number\"}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("expected a JSON integer");
    }

    @Test
    public void testRejectsNonStringMapKeyBeforeDecodingRecords() {
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
    public void testEnforcesSchemaJsonAndContainerResourceBounds() {
        DataType nestedType = DataTypes.STRING();
        for (int i = 0; i <= JsonToFlussConverters.MAX_SCHEMA_NESTING_DEPTH; i++) {
            nestedType = DataTypes.ARRAY(nestedType);
        }
        final DataType tooDeepType = nestedType;
        assertThatThrownBy(() -> decoder(DataTypes.ROW(DataTypes.FIELD("nested", tooDeepType))))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("maximum nesting depth");

        StringBuilder nestedJson = new StringBuilder("{\"unknown\":");
        for (int i = 0; i < JsonKafkaFieldDecoder.MAX_JSON_NESTING_DEPTH; i++) {
            nestedJson.append("{\"nested\":");
        }
        nestedJson.append('0');
        for (int i = 0; i < JsonKafkaFieldDecoder.MAX_JSON_NESTING_DEPTH; i++) {
            nestedJson.append('}');
        }
        nestedJson.append('}');
        JsonKafkaFieldDecoder scalarDecoder =
                decoder(DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING())));
        assertThatThrownBy(() -> scalarDecoder.decode(bytes(nestedJson.toString())))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("not valid strict UTF-8 JSON");

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

    @Test
    public void testBoundsAndEscapesMapKeysInErrorPaths() {
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

    @Test
    public void testNullKafkaValueRequiresNullableProjection() {
        JsonKafkaFieldDecoder nullableDecoder =
                decoder(DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING())));
        assertThat(nullableDecoder.decode(null)).containsExactly((Object) null);

        JsonKafkaFieldDecoder notNullDecoder =
                decoder(DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING().copy(false))));
        assertThatThrownBy(() -> notNullDecoder.decode(null))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("NOT NULL field 'value'");
    }

    private static void assertFailure(RowType rowType, String json, String path, String reason) {
        assertThatThrownBy(() -> decoder(rowType).decode(bytes(json)))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining(path)
                .hasMessageContaining(reason);
    }

    private static JsonKafkaFieldDecoder decoder(RowType rowType) {
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            positions.add(i);
        }
        return new JsonKafkaFieldDecoder(new KafkaFieldProjection(rowType, positions));
    }

    private static JsonKafkaFieldDecoder rescueDecoder(RowType rowType, String rescueColumn) {
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            positions.add(i);
        }
        return new JsonKafkaFieldDecoder(
                new KafkaFieldProjection(rowType, positions), rescueColumn);
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
