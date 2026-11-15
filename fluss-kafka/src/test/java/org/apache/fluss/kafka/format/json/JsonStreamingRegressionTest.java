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
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regressions for lazy strings, numeric coercion and parser recovery. */
class JsonStreamingRegressionTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBinaryRescueRetainsOriginalAndSibling(boolean nested) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        for (DataType type : new DataType[] {DataTypes.BYTES(), DataTypes.BINARY(2)}) {
            for (String text : Arrays.asList("!bad", "AQID!bad", "A", "AQ=", "AQ==x")) {
                KafkaDecodedValue decoded =
                        decoder(type, true, nested)
                                .decode(bytes("{\"b\":\"" + text + "\",\"id\":7}"));
                GenericRow row =
                        nested
                                ? (GenericRow) decoded.valueFields()[0]
                                : GenericRow.of(decoded.valueFields());
                assertThat(row.isNullAt(0)).isTrue();
                assertThat(row.getInt(1)).isEqualTo(7);
                assertThat(mapper.readTree(decoded.rescue()).get("b").asText()).isEqualTo(text);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBinaryRejectsMalformedJson(boolean rescue) {
        for (boolean nested : new boolean[] {false, true}) {
            for (DataType type : new DataType[] {DataTypes.BYTES(), DataTypes.BINARY(2)}) {
                for (String text : Arrays.asList("AQID\nrest", "AQID\\qrest")) {
                    assertThatThrownBy(
                                    () ->
                                            decoder(type, rescue, nested)
                                                    .decode(
                                                            bytes(
                                                                    "{\"b\":\""
                                                                            + text
                                                                            + "\",\"id\":7}")))
                            .isInstanceOf(KafkaRecordEncodingException.class);
                }
            }
        }
    }

    @Test
    void testSmallIntegersAndOverflowRescue() {
        for (DataType type : new DataType[] {DataTypes.TINYINT(), DataTypes.SMALLINT()}) {
            for (String number : Arrays.asList("1.0", "1e0", "100e-2")) {
                assertThat(
                                ((Number)
                                                decoder(type, false, false)
                                                        .decode(bytes("{\"b\":" + number + "}"))
                                                        .valueFields()[0])
                                        .intValue())
                        .isEqualTo(1);
            }
        }
        for (DataType type :
                new DataType[] {
                    DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.INT(), DataTypes.BIGINT()
                }) {
            KafkaDecodedValue value =
                    decoder(type, true, false)
                            .decode(bytes("{\"b\":9223372036854775808,\"id\":7}"));
            assertThat(value.valueFields()).containsExactly(null, 7);
            assertThat(value.rescue()).isEqualTo("{\"b\":9223372036854775808}");
        }
    }

    @Test
    void testStringCoercionNormalizesOrdinaryNumbers() {
        for (String number : Arrays.asList("1.0", "1e0", "100e-2")) {
            assertThat(
                            decoder(DataTypes.STRING(), false, false)
                                    .decode(bytes("{\"b\":" + number + "}"))
                                    .valueFields()[0]
                                    .toString())
                    .isEqualTo("1");
        }
        assertThat(
                        decoder(DataTypes.STRING(), false, false)
                                .decode(bytes("{\"b\":{\"n\":1.0}}"))
                                .valueFields()[0]
                                .toString())
                .isEqualTo("{\"n\":1}");
    }

    @Test
    void testSkippedAndBinaryStringsEnforceConstraints() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder().maxStringLength(100).build());
        char[] chars = new char[10000];
        Arrays.fill(chars, 'A');
        for (DataType type :
                new DataType[] {
                    DataTypes.BYTES(),
                    DataTypes.BOOLEAN(),
                    DataTypes.ROW(DataTypes.FIELD("known", DataTypes.INT()))
                }) {
            String text = "\"" + new String(chars) + "\"";
            String json = type.equals(DataTypes.BYTES()) ? text : "{\"unknown\":" + text + "}";
            for (boolean rescue : new boolean[] {false, true}) {
                try (JsonParser parser = mapper.getFactory().createParser(json)) {
                    parser.nextToken();
                    assertThatThrownBy(
                                    () ->
                                            JsonParserToFlussConverters.create(type)
                                                    .convert(
                                                            parser,
                                                            "$",
                                                            rescue ? value -> {} : null))
                            .isInstanceOf(
                                    org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.exc
                                            .StreamConstraintsException.class);
                }
            }
        }
    }

    @Test
    void testDynamicRowPaths() {
        DataType row = DataTypes.ROW(DataTypes.FIELD("required", DataTypes.INT()));
        DataType array = DataTypes.ARRAY(row);
        assertThatThrownBy(
                        () ->
                                decoder(array, false, false)
                                        .decode(
                                                bytes(
                                                        "{\"b\":[{\"required\":1},{\"required\":\"bad\"}]}")))
                .hasMessageContaining("$[\"b\"][1][\"required\"]");
        DataType map = DataTypes.MAP(DataTypes.STRING(), row);
        assertThatThrownBy(
                        () ->
                                decoder(map, false, false)
                                        .decode(
                                                bytes(
                                                        "{\"b\":{\"first\":{\"required\":1},\"second\":{\"required\":\"bad\"}}}")))
                .hasMessageContaining("$[\"b\"][\"second\"][\"required\"]");
        assertThatThrownBy(
                        () ->
                                decoder(row, false, false)
                                        .decode(bytes("{\"b\":{\"required\":\"bad\"}}")))
                .hasMessageContaining("$[\"b\"][\"required\"]");
    }

    private static JsonKafkaFieldDecoder decoder(DataType type, boolean rescue, boolean nested) {
        Schema.Builder schema = Schema.newBuilder();
        if (nested) {
            schema.column(
                    KafkaTableSchema.VALUE_COLUMN,
                    DataTypes.ROW(
                            DataTypes.FIELD("b", type), DataTypes.FIELD("id", DataTypes.INT())));
        } else {
            schema.column("b", type).column("id", DataTypes.INT());
        }
        if (rescue) {
            schema.column(KafkaTableSchema.RESCUE_COLUMN, DataTypes.STRING());
        }
        return new JsonKafkaFieldDecoder(
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
                                .build()));
    }

    private static ByteBuffer bytes(String json) {
        return ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
    }
}
