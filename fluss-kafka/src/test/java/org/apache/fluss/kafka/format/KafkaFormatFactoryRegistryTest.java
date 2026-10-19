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

package org.apache.fluss.kafka.format;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaFieldProjection;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests value decoders against the shared fixed-column table contract. */
class KafkaFormatFactoryRegistryTest {
    private final KafkaFormatFactoryRegistry formats = new KafkaFormatFactoryRegistry();

    @Test
    void testBinaryBorrowsOpaqueBytesAndPreservesNullAndEmpty() {
        KafkaFieldDecoder decoder =
                formats.createDecoder(
                        KafkaValueConversionType.BINARY, projection(DataTypes.BYTES()));
        byte[] opaque = new byte[] {(byte) 0xff, 0};
        assertThat(decoder.decode(opaque)[0]).isSameAs(opaque);
        assertThat(opaque).containsExactly((byte) 0xff, (byte) 0);
        assertThat(decoder.decode(null)).containsExactly((Object) null);
        byte[] empty = new byte[0];
        assertThat(decoder.decode(empty)[0]).isSameAs(empty);
    }

    @Test
    void testStringRejectsMalformedUtf8AndCanBeReused() {
        KafkaFieldDecoder decoder =
                formats.createDecoder(
                        KafkaValueConversionType.STRING, projection(DataTypes.STRING()));
        for (byte[] invalid :
                new byte[][] {
                    {(byte) 0xc3, 0x28},
                    {(byte) 0xc0, (byte) 0x80},
                    {(byte) 0xed, (byte) 0xa0, (byte) 0x80},
                    {(byte) 0xe2, (byte) 0x82}
                }) {
            byte[] original = invalid.clone();
            assertThatThrownBy(() -> decoder.decode(invalid))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("_kafka_value")
                    .hasMessageContaining("UTF-8");
            assertThat(invalid).isEqualTo(original);
            byte[] valid = "键".getBytes(StandardCharsets.UTF_8);
            assertThat(decoder.decode(valid)).containsExactly(BinaryString.fromString("键"));
            assertThat(valid).isEqualTo("键".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(decoder.decode(null)).containsExactly((Object) null);
        assertThat(decoder.decode(new byte[0])).containsExactly(BinaryString.fromString(""));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFactoriesUseThePublicValueProjection(boolean string) {
        DataType type = string ? DataTypes.STRING() : DataTypes.BYTES();
        TableDescriptor table =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column(KafkaTableSchema.KEY_COLUMN, DataTypes.BYTES())
                                        .column(KafkaTableSchema.VALUE_COLUMN, type)
                                        .column(
                                                KafkaTableSchema.HEADERS_COLUMN,
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.BYTES()))
                                        .column(KafkaTableSchema.RESCUE_COLUMN, DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                string
                                        ? KafkaValueConversionType.STRING
                                        : KafkaValueConversionType.BINARY)
                        .build();
        KafkaTableSchema schema = KafkaTableSchema.resolve(table);
        assertThat(schema.valueProjection().positions()).containsExactly(1);
        KafkaFieldDecoder decoder =
                formats.createDecoder(schema.valueConversionType(), schema.valueProjection());
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        assertThat(decoder.decode(payload))
                .containsExactly(string ? BinaryString.fromString("payload") : payload);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFactoriesRejectIncompatibleProjections(boolean string) {
        KafkaValueConversionType format =
                string ? KafkaValueConversionType.STRING : KafkaValueConversionType.BINARY;
        DataType type = string ? DataTypes.STRING() : DataTypes.BYTES();
        assertThatThrownBy(
                        () ->
                                formats.createDecoder(
                                        format,
                                        projection(
                                                string ? DataTypes.BYTES() : DataTypes.STRING())))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining(string ? "STRING" : "BYTES");
        KafkaFieldProjection multiple =
                new KafkaFieldProjection(
                        DataTypes.ROW(
                                DataTypes.FIELD(KafkaTableSchema.VALUE_COLUMN, type),
                                DataTypes.FIELD(KafkaTableSchema.KEY_COLUMN, DataTypes.BYTES())),
                        Arrays.asList(0, 1));
        KafkaFieldProjection empty =
                new KafkaFieldProjection(
                        DataTypes.ROW(DataTypes.FIELD(KafkaTableSchema.VALUE_COLUMN, type)),
                        Collections.emptyList());
        for (KafkaFieldProjection invalid : Arrays.asList(multiple, empty)) {
            assertThatThrownBy(() -> formats.createDecoder(format, invalid))
                    .isInstanceOf(KafkaTopicSchemaException.class)
                    .hasMessageContaining("exactly one");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFactoriesRejectKeyAndArbitraryValueColumnNames(boolean string) {
        KafkaValueConversionType format =
                string ? KafkaValueConversionType.STRING : KafkaValueConversionType.BINARY;
        DataType type = string ? DataTypes.STRING() : DataTypes.BYTES();
        for (String name : Arrays.asList(KafkaTableSchema.KEY_COLUMN, "body")) {
            KafkaFieldProjection invalid =
                    new KafkaFieldProjection(
                            DataTypes.ROW(DataTypes.FIELD(name, type)),
                            Collections.singletonList(0));
            assertThatThrownBy(() -> formats.createDecoder(format, invalid))
                    .isInstanceOf(KafkaTopicSchemaException.class)
                    .hasMessageContaining("_kafka_value");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testJsonLayoutDeterminesPhysicalValueContainer(boolean nested) {
        RowType business = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));
        KafkaFieldProjection projection =
                nested
                        ? projection(business)
                        : new KafkaFieldProjection(business, Collections.singletonList(0));
        KafkaFieldDecoder decoder =
                formats.createDecoder(
                        KafkaValueConversionType.PARSE_JSON,
                        projection,
                        nested
                                ? KafkaValueConversionLayout.NESTED
                                : KafkaValueConversionLayout.FLAT);
        Object[] values = decoder.decode("{\"id\":7}".getBytes(StandardCharsets.UTF_8));
        assertThat(values).hasSize(1);
        if (nested) {
            assertThat(((InternalRow) values[0]).getInt(0)).isEqualTo(7);
        } else {
            assertThat(values).containsExactly(7);
        }
        assertThatThrownBy(
                        () ->
                                formats.createDecoder(
                                        KafkaValueConversionType.PARSE_JSON,
                                        projection(DataTypes.STRING())))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("Reserved name");
        assertThatThrownBy(
                        () ->
                                decoder.decode(
                                        "{\"_kafka_value\":{\"id\":7}}"
                                                .getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("reserved field");
    }

    private static KafkaFieldProjection projection(DataType type) {
        return new KafkaFieldProjection(
                DataTypes.ROW(DataTypes.FIELD(KafkaTableSchema.VALUE_COLUMN, type)),
                Collections.singletonList(0));
    }
}
