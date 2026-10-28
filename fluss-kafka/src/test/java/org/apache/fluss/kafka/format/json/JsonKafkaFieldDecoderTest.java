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
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests JSON decoding, projection and failures before Arrow conversion. */
class JsonKafkaFieldDecoderTest {
    private static final RowType ROW_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT()),
                    DataTypes.FIELD("amount", DataTypes.DECIMAL(5, 2)),
                    DataTypes.FIELD("label", DataTypes.STRING()));

    private static final RowType COMPLEX_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD(
                            "items",
                            DataTypes.ARRAY(
                                    DataTypes.ROW(
                                            DataTypes.FIELD("quantity", DataTypes.INT()),
                                            DataTypes.FIELD("ratio", DataTypes.FLOAT())))),
                    DataTypes.FIELD(
                            "attributes",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))));

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testScalarProjectionAndNullValues(boolean nested) {
        JsonKafkaFieldDecoder decoder = decoder(ROW_TYPE, nested);
        Object[] values = decoder.decode(buffer("{\"amount\":1.234,\"id\":\"7\"}")).valueFields();
        GenericRow row = nested ? (GenericRow) values[0] : GenericRow.of(values);
        assertThat(row.getInt(0)).isEqualTo(7);
        assertThat(row.getDecimal(1, 5, 2).toBigDecimal()).isEqualByComparingTo("1.23");
        assertThat(row.isNullAt(2)).isTrue();
        assertThat(decoder.decode(null).valueFields())
                .containsExactly(nested ? new Object[] {null} : new Object[] {null, null, null});
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConvertsJsonTokensUsingSchema(boolean nested) {
        RowType type =
                DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()),
                        DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD("enabled", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("count", DataTypes.INT()));
        Object[] values =
                decoder(type, nested)
                        .decode(
                                buffer(
                                        "{\"label\":{\"k\":1},\"items\":[{\"k\":2},[3,null]],"
                                                + "\"enabled\":{},\"count\":1.9}"))
                        .valueFields();
        GenericRow row = nested ? (GenericRow) values[0] : GenericRow.of(values);
        assertThat(row.getString(0).toString()).isEqualTo("{\"k\":1}");
        assertThat(row.getArray(1).getString(0).toString()).isEqualTo("{\"k\":2}");
        assertThat(row.getArray(1).getString(1).toString()).isEqualTo("[3,null]");
        assertThat(row.getBoolean(2)).isFalse();
        assertThat(row.getInt(3)).isEqualTo(1);
    }

    @Test
    void testNestedBusinessFieldCanUseSystemColumnName() {
        KafkaTableSchema schema =
                KafkaTableSchema.resolve(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column(
                                                        KafkaTableSchema.KEY_COLUMN,
                                                        DataTypes.BYTES())
                                                .column(
                                                        KafkaTableSchema.VALUE_COLUMN,
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        KafkaTableSchema.KEY_COLUMN,
                                                                        DataTypes.STRING())))
                                                .build())
                                .distributedBy(1)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(),
                                        "parse_json")
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(),
                                        "nested")
                                .build());
        Object[] values =
                new JsonKafkaFieldDecoder(schema)
                        .decode(buffer("{\"_kafka_key\":\"business\"}"))
                        .valueFields();
        assertThat(((GenericRow) values[0]).getString(0).toString()).isEqualTo("business");
    }

    @Test
    void testRejectsInvalidRecords() {
        JsonKafkaFieldDecoder decoder = decoder(ROW_TYPE, false);
        for (String json : Arrays.asList("{", "[]", "{\"id\":\"bad\"}", "{\"unknown\":1}")) {
            assertThatThrownBy(() -> decoder.decode(buffer(json)))
                    .as("invalid record: %s", json)
                    .isInstanceOf(KafkaRecordEncodingException.class);
        }
    }

    @Test
    void testBorrowedBufferRangeIsUnchanged() {
        byte[] json = "{\"id\":7}".getBytes(StandardCharsets.UTF_8);
        ByteBuffer storage = ByteBuffer.allocateDirect(json.length + 2);
        storage.position(1);
        storage.put(json);
        ByteBuffer input = storage.asReadOnlyBuffer();
        input.position(1);
        input.limit(1 + json.length);
        assertThat(decoder(ROW_TYPE, false).decode(input).valueFields())
                .containsExactly(7, null, null);
        assertThat(input.position()).isEqualTo(1);
        assertThat(input.remaining()).isEqualTo(json.length);
    }

    @Test
    void testRawNumbersPreserveDecimalPrecision() {
        JsonKafkaFieldDecoder decoder =
                decoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("floatValue", DataTypes.FLOAT()),
                                DataTypes.FIELD("doubleValue", DataTypes.DOUBLE()),
                                DataTypes.FIELD("amount", DataTypes.DECIMAL(30, 10)),
                                DataTypes.FIELD("floatZero", DataTypes.FLOAT())),
                        false);
        Object[] values =
                decoder.decode(
                                buffer(
                                        "{\"floatValue\":1.0000000596046448,\"doubleValue\":-0.0,"
                                                + "\"amount\":12345678901234567890.1234567890,\"floatZero\":-0}"))
                        .valueFields();
        assertThat(values[0]).isEqualTo(1.0000001F);
        // Jackson's decimal/integer nodes currently normalize signed zero (see #4863).
        assertThat(Double.doubleToRawLongBits((Double) values[1])).isEqualTo(0L);
        assertThat(((Decimal) values[2]).toBigDecimal())
                .isEqualByComparingTo("12345678901234567890.1234567890");
        assertThat(Float.floatToRawIntBits((Float) values[3])).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDecodesComplexValues(boolean nested) {
        Object[] values =
                decoder(COMPLEX_TYPE, nested)
                        .decode(
                                buffer(
                                        "{\"items\":[{\"quantity\":\"2\",\"ratio\":1.0000000596046448,"
                                                + "\"extra\":true},null],"
                                                + "\"attributes\":{\"scores\":[7,null,9]}}"))
                        .valueFields();
        GenericRow row = nested ? (GenericRow) values[0] : GenericRow.of(values);
        InternalArray items = row.getArray(0);
        assertThat(items.size()).isEqualTo(2);
        assertThat(items.getRow(0, 2).getInt(0)).isEqualTo(2);
        assertThat(items.getRow(0, 2).getFloat(1)).isEqualTo(1.0000001F);
        assertThat(items.isNullAt(1)).isTrue();
        assertThat(row.getMap(1).keyArray().getString(0).toString()).isEqualTo("scores");
        InternalArray scores = row.getMap(1).valueArray().getArray(0);
        assertThat(scores.size()).isEqualTo(3);
        assertThat(scores.getInt(0)).isEqualTo(7);
        assertThat(scores.isNullAt(1)).isTrue();
        assertThat(scores.getInt(2)).isEqualTo(9);
    }

    @Test
    void testRejectsInvalidNestedValuesWithPaths() {
        JsonKafkaFieldDecoder decoder = decoder(COMPLEX_TYPE, false);
        String[][] invalid = {
            {"{\"items\":[{\"quantity\":\"bad\"}]}", "$[\"items\"][0][\"quantity\"]"},
            {"{\"attributes\":{\"scores\":{}}}", "$[\"attributes\"][\"scores\"]"}
        };
        for (String[] record : invalid) {
            assertThatThrownBy(() -> decoder.decode(buffer(record[0])))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining(record[1]);
        }
    }

    private static JsonKafkaFieldDecoder decoder(RowType rowType, boolean nested) {
        Schema.Builder schema = Schema.newBuilder();
        if (nested) {
            schema.column(KafkaTableSchema.VALUE_COLUMN, rowType);
        } else {
            for (DataField field : rowType.getFields()) {
                schema.column(field.getName(), field.getType());
            }
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

    private static ByteBuffer buffer(String json) {
        return ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
    }
}
