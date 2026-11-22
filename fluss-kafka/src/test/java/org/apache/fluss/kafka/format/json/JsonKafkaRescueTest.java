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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.kafka.format.KafkaDecodedValue;
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.format.KafkaFormatFactoryRegistry;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests field-level rescue through the schema-aware decoder factory. */
class JsonKafkaRescueTest {
    private static final RowType ITEM_TYPE =
            DataTypes.ROW(DataTypes.FIELD("known", DataTypes.INT()));
    private static final RowType VALUE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT()),
                    DataTypes.FIELD(
                            "details",
                            DataTypes.ROW(
                                    DataTypes.FIELD("known", DataTypes.INT()),
                                    DataTypes.FIELD("label", DataTypes.STRING()))),
                    DataTypes.FIELD("items", DataTypes.ARRAY(ITEM_TYPE)),
                    DataTypes.FIELD("attributes", DataTypes.MAP(DataTypes.STRING(), ITEM_TYPE)),
                    DataTypes.FIELD("ratio", DataTypes.FLOAT()));

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRescuesUnknownAndInvalidFieldsAcrossNestedContainers(boolean nested) throws Exception {
        KafkaFieldDecoder decoder = decoder(nested, true);
        String extra = "{\"text\":\"\\uD83D\\uDE00\",\"number\":12345678901234567890.123456789}";
        KafkaDecodedValue decoded =
                decoder.decode(
                        bytes(
                                "{\"id\":\"bad\",\"details\":{\"known\":\"bad\",\"label\":{\"k\":1},"
                                        + "\"extra\":[1,null]},\"items\":[null,{\"known\":\"2\"},"
                                        + "{\"known\":\"bad\",\"extra\":true},\"wrong shape\"],"
                                        + "\"attributes\":{\"k\":{\"known\":\"bad\",\"extra\":null},"
                                        + "\"good\":{\"known\":4}},\"ratio\":1e40,"
                                        + "\"_kafka_key\":42,\"_kafka_rescue\":{\"origin\":true},\"extra\":"
                                        + extra
                                        + "}"));
        Object[] values = decoded.valueFields();
        assertThat(values).hasSize(nested ? 1 : VALUE_TYPE.getFieldCount());
        GenericRow row = nested ? (GenericRow) values[0] : GenericRow.of(values);
        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.getRow(1, 2).isNullAt(0)).isTrue();
        assertThat(row.getRow(1, 2).getString(1).toString()).isEqualTo("{\"k\":1}");
        InternalArray items = row.getArray(2);
        assertThat(items.size()).isEqualTo(4);
        assertThat(items.isNullAt(0)).isTrue();
        assertThat(items.getRow(1, 1).getInt(0)).isEqualTo(2);
        assertThat(items.getRow(2, 1).isNullAt(0)).isTrue();
        assertThat(items.isNullAt(3)).isTrue();
        assertThat(row.getMap(3).valueArray().getRow(0, 1).isNullAt(0)).isTrue();
        assertThat(row.getMap(3).valueArray().getRow(1, 1).getInt(0)).isEqualTo(4);
        assertThat(row.isNullAt(4)).isTrue();
        ObjectMapper mapper =
                new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        assertThat(mapper.readValue(decoded.rescue(), Map.class))
                .isEqualTo(
                        mapper.readValue(
                                "{\"_kafka_key\":42,\"_kafka_rescue\":{\"origin\":true},\"extra\":"
                                        + extra
                                        + ",\"id\":\"bad\",\"details\":{\"known\":\"bad\",\"extra\":[1,null]},"
                                        + "\"items\":[null,null,{\"known\":\"bad\",\"extra\":true},\"wrong shape\"],"
                                        + "\"attributes\":{\"k\":{\"known\":\"bad\",\"extra\":null}},\"ratio\":1e40}",
                                Map.class));
        assertThat(decoder.decode(bytes("{\"id\":9}")).rescue()).isNull();
        assertThat(decoder.decode(null).valueFields()).containsOnlyNulls();
        assertThat(decoder.decode(null).rescue()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnpairedSurrogateEscapesFollowDefaultEncoding(boolean nested) throws Exception {
        KafkaDecodedValue decoded =
                decoder(nested, true)
                        .decode(
                                bytes(
                                        "{\"details\":{\"label\":\"\\uD800\"},\"extra\":\"\\uDC00\"}"));
        Object[] values = decoded.valueFields();
        GenericRow row = nested ? (GenericRow) values[0] : GenericRow.of(values);
        assertThat(row.getRow(1, 2).getString(1).toString()).isEqualTo("?");
        ObjectMapper mapper = new ObjectMapper();
        assertThat(mapper.readTree(decoded.rescue()))
                .isEqualTo(mapper.readTree("{\"extra\":\"\\uDC00\"}"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRescueRequiresTheFixedSystemColumn(boolean nested) {
        KafkaFieldDecoder decoder = decoder(nested, false);
        assertThat(decoder.decode(bytes("{\"id\":7}")).valueFields())
                .hasSize(nested ? 1 : VALUE_TYPE.getFieldCount());
        assertThatThrownBy(() -> decoder.decode(bytes("{\"id\":7,\"extra\":1}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("unknown field");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStrictConversionsAndInvalidJsonAreStillRejected(boolean nested) {
        KafkaFieldDecoder strictDecoder = decoder(nested, false);
        String[][] invalidFields = {
            {"\"id\":\"bad\"", "$[\"id\"]"},
            {"\"details\":{\"known\":\"bad\"}", "$[\"details\"][\"known\"]"},
            {"\"items\":[{\"known\":\"bad\"}]", "$[\"items\"][0][\"known\"]"},
            {"\"attributes\":{\"k\":{\"known\":\"bad\"}}", "$[\"attributes\"][\"k\"][\"known\"]"}
        };
        for (String[] field : invalidFields) {
            assertThatThrownBy(() -> strictDecoder.decode(bytes("{" + field[0] + "}")))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining(field[1]);
        }
        KafkaFieldDecoder decoder = decoder(nested, true);
        for (String json :
                Arrays.asList(
                        "{\"extra\":[",
                        "{\"extra\":1,\"extra\":2}",
                        "[]",
                        "{} {}",
                        "{\"extra\":[{\"k\":1,\"k\":2}]}",
                        "{\"extra\":" + String.join("", Collections.nCopies(1001, "1")) + "}",
                        "{\"extra\":"
                                + String.join(
                                        "",
                                        Collections.nCopies(
                                                JsonKafkaFieldDecoder.MAX_JSON_NESTING_DEPTH, "["))
                                + "0"
                                + String.join(
                                        "",
                                        Collections.nCopies(
                                                JsonKafkaFieldDecoder.MAX_JSON_NESTING_DEPTH, "]"))
                                + "}")) {
            assertThatThrownBy(() -> decoder.decode(bytes(json)))
                    .as("invalid JSON: %s", json)
                    .isInstanceOf(KafkaRecordEncodingException.class);
        }
        assertThatThrownBy(
                        () ->
                                decoder.decode(
                                        ByteBuffer.wrap(
                                                new byte[] {
                                                    '{',
                                                    '"',
                                                    'x',
                                                    '"',
                                                    ':',
                                                    '"',
                                                    (byte) 0xff,
                                                    '"',
                                                    '}'
                                                })))
                .isInstanceOf(KafkaRecordEncodingException.class);
    }

    @Test
    void testInternalErrorsAreNotRescued() {
        IllegalStateException failure = new IllegalStateException("internal error");
        JsonNode brokenNode =
                new TextNode("value") {
                    @Override
                    public String asText() {
                        throw failure;
                    }
                };
        List<JsonNode> rescued = new ArrayList<>();
        assertThatThrownBy(
                        () ->
                                JsonToFlussConverters.create(DataTypes.INT())
                                        .convert(brokenNode, "$", rescued::add))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasCause(failure);
        assertThat(rescued).isEmpty();
    }

    private static KafkaFieldDecoder decoder(boolean nested, boolean rescue) {
        Schema.Builder schema = Schema.newBuilder();
        if (rescue) {
            schema.column(KafkaTableSchema.RESCUE_COLUMN, DataTypes.STRING());
        }
        if (nested) {
            schema.column(KafkaTableSchema.VALUE_COLUMN, VALUE_TYPE);
        } else {
            for (DataField field : VALUE_TYPE.getFields()) {
                schema.column(field.getName(), field.getType());
            }
        }
        KafkaTableSchema contract =
                KafkaTableSchema.resolve(
                        TableDescriptor.builder()
                                .schema(schema.build())
                                .distributedBy(1)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(),
                                        "parse_json")
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(),
                                        nested ? "nested" : "flat")
                                .build());
        return new KafkaFormatFactoryRegistry().createDecoder(contract);
    }

    private static ByteBuffer bytes(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
